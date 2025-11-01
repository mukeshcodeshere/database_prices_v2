# 2_pull_streaming.py
import datetime
import os
import io
import time
import hashlib
import signal
import sys
from typing import List, Dict, Optional, Tuple, Set
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import gc
from threading import Lock

# Import database functions
from connect_db import read_sql_with_retry, write_sql_with_retry, engine
from sqlalchemy import NVARCHAR, DATETIME, Float, BigInteger, Text, inspect, text
from sqlalchemy.exc import SQLAlchemyError

# =============================================================================
# CONFIGURATION VARIABLES - ALL DEFINED AT TOP
# =============================================================================
MAX_WORKERS = 15                    # Number of concurrent threads
BATCH_SIZE = 5                      # Symbols per batch
FULL_HISTORY_RECORDS = 7200         # Records for new tickers
INCREMENTAL_RECORDS = 100           # Records for existing tickers
STREAM_CYCLE_DELAY = 300            # Seconds between full cycles (5 minutes)
INSERT_CHUNK_SIZE = 1000            # Rows per database insert
RETRY_ATTEMPTS = 3                  # API retry attempts
API_TIMEOUT = 180                   # API timeout in seconds
RATE_LIMIT_DELAY = 0.3              # Delay between API calls
MEMORY_CLEANUP_FREQUENCY = 5        # Clean memory every N batches
LOG_FILE = 'price_streaming.log'
SCHEMA_NAME = "MV_PRICES_2"
TABLE_NAME = "MV_All_Prices"
INSTRUMENTS_TABLE = "[MV_PRICES_2].[InstrumentsExchanges]"

# =============================================================================
# LOGGING SETUP
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# ENVIRONMENT VARIABLES
# =============================================================================
load_dotenv()
GvWSUSERNAME = os.getenv("GvWSUSERNAME")
GvWSPASSWORD = os.getenv("GvWSPASSWORD")

# =============================================================================
# GRACEFUL SHUTDOWN HANDLER
# =============================================================================
class GracefulShutdown:
    """Handle graceful shutdown on SIGINT/SIGTERM."""
    def __init__(self):
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._request_shutdown)
        signal.signal(signal.SIGTERM, self._request_shutdown)
    
    def _request_shutdown(self, signum, frame):
        logger.warning("Shutdown signal received. Completing current operations...")
        self.shutdown_requested = True
    
    def should_continue(self) -> bool:
        return not self.shutdown_requested

shutdown_handler = GracefulShutdown()

# =============================================================================
# PRICE DATA EXTRACTOR
# =============================================================================
class PriceDataExtractor:
    """Handles API interactions for price data extraction."""
    BASE_URL = "https://mv-api-proxy.prod.tr.enverus.com/pythonapi/v1"

    def __init__(self, username: str, password: str, environment: str = "onboard"):
        self.username = username
        self.password = password
        self.environment = environment
        self.session = self._create_authenticated_session()
        self.lock = Lock()
        logger.info("PriceDataExtractor initialized successfully.")

    def _create_authenticated_session(self) -> requests.Session:
        """Create an authenticated session with retries."""
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.auth = (self.username, self.password)
        return session

    def _fetch_csv_data(self, params: dict) -> Optional[pd.DataFrame]:
        """Fetch CSV data from API endpoint."""
        try:
            params["output"] = "csv"
            url = f"{self.BASE_URL}/GetDaily"
            
            with self.lock:  # Thread-safe API calls
                time.sleep(RATE_LIMIT_DELAY)
                response = self.session.get(url, params=params, timeout=API_TIMEOUT)

            if response.status_code == 200:
                df = pd.read_csv(io.StringIO(response.text))
                return df
            else:
                logger.warning(f"API request failed ({response.status_code}): {response.text[:200]}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f"API request timeout after {API_TIMEOUT}s")
            return None
        except Exception as e:
            logger.error(f"Error fetching price data: {e}", exc_info=True)
            return None

    def get_daily_data(self, symbols: List[str], records_back: int, max_retries: int = RETRY_ATTEMPTS) -> Optional[pd.DataFrame]:
        """Get daily price data with retry logic."""
        fields_all = [
            "pricesymbol", "symbol", "symboldescription", "tradedatetimeutc", "open", "high", "low", "close", "last",
            "midpoint", "volume", "tradevolume", "historicvolume", "tickcount", "netchange", "percentchange",
            "openinterest", "closedate", "currency", "mostrecentvalue", "mostrecentvaluedate", "lasttradedirection",
            "prevlast", "lastopen", "lasthigh", "lastlow", "lastclose", "lastvolume", "putcallunderlier",
            "bid", "ask", "bidsize", "asksize", "biddatetimeutc", "askdatetimeutc", "optionroot", "settledate",
            "displaycontractexpdate", "market", "expirationdate", "lotunit", "strike", "tradestarttimeutc",
            "tradestoptimeutc", "sessionstarttimeutc", "sessionstoptimeutc", "blocktradedatetimeutc",
            "settleupdatetime", "prevsettleupdatetime", "exchangecode"
        ]
        
        for attempt in range(max_retries):
            try:
                symbols_str = ",".join([f'"{symbol}"' for symbol in symbols])
                fields_str = ",".join(fields_all)
                
                params = {
                    "symbols": symbols_str,
                    "fields": fields_str,
                    "recordsback": records_back,
                    "env": self.environment
                }
                
                df = self._fetch_csv_data(params)
                if df is not None and not df.empty:
                    return df
                elif df is not None and df.empty:
                    logger.debug(f"Empty data returned for symbols: {symbols[:3]}...")
                    return None
                    
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for symbols {symbols[:3]}...: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed after {max_retries} attempts for symbols: {symbols[:3]}...")
        return None

# =============================================================================
# DATABASE MANAGER
# =============================================================================
class PriceDatabaseManager:
    """Handles all database operations with duplicate prevention."""
    
    def __init__(self, schema_name: str, table_name: str):
        self.schema_name = schema_name
        self.table_name = table_name
        self.engine = engine
        self.lock = Lock()
        logger.info(f"PriceDatabaseManager initialized for {schema_name}.{table_name}")

    def ensure_schema_exists(self):
        """Create schema if it doesn't exist."""
        try:
            with self.engine.connect() as connection:
                inspector = inspect(connection)
                if self.schema_name not in inspector.get_schema_names():
                    with self.engine.begin() as conn:
                        conn.execute(text(f"CREATE SCHEMA {self.schema_name}"))
                    logger.info(f"Schema '{self.schema_name}' created successfully.")
                else:
                    logger.debug(f"Schema '{self.schema_name}' already exists.")
        except Exception as e:
            logger.error(f"Error ensuring schema exists: {e}", exc_info=True)
            raise

    def create_table_if_not_exists(self, df_sample: pd.DataFrame):
        """Create price table with proper types, indexes, and constraints."""
        try:
            with self.engine.connect() as connection:
                inspector = inspect(connection)

                if not inspector.has_table(self.table_name, schema=self.schema_name):
                    logger.info(f"Creating table {self.schema_name}.{self.table_name}...")

                    # Define explicit column types
                    dtypes = {}
                    for c in df_sample.columns:
                        if c.lower() in ('symbol', 'pricesymbol', 'currency', 'market',
                                         'optionroot', 'exchangecode', 'data_source'):
                            dtypes[c] = NVARCHAR(100)
                        elif 'time' in c.lower() or 'date' in c.lower():
                            dtypes[c] = DATETIME
                        elif c.lower() in ('volume', 'tradevolume', 'historicvolume',
                                           'tickcount', 'openinterest'):
                            dtypes[c] = BigInteger
                        elif c.lower() in ('open', 'high', 'low', 'close', 'last',
                                           'midpoint', 'bid', 'ask', 'bidsize', 'asksize',
                                           'netchange', 'percentchange', 'strike'):
                            dtypes[c] = Float
                        else:
                            dtypes[c] = Text

                    # Create empty table
                    df_sample.head(0).to_sql(
                        self.table_name,
                        self.engine,
                        schema=self.schema_name,
                        if_exists='fail',
                        index=False,
                        dtype=dtypes
                    )

                    # Add metadata columns and constraints
                    with self.engine.begin() as conn:
                        # Add metadata columns
                        conn.execute(text(f"""
                            ALTER TABLE {self.schema_name}.{self.table_name}
                            ADD price_hash VARCHAR(64) NOT NULL,
                                created_at DATETIME DEFAULT GETDATE(),
                                updated_at DATETIME DEFAULT GETDATE(),
                                data_source VARCHAR(50) DEFAULT 'MV_API';
                        """))

                        # Unique constraint on hash to prevent duplicates
                        conn.execute(text(f"""
                            ALTER TABLE {self.schema_name}.{self.table_name}
                            ADD CONSTRAINT uk_{self.table_name}_price_hash UNIQUE (price_hash);
                        """))

                        # Performance indexes
                        conn.execute(text(f"""
                            CREATE INDEX ix_{self.table_name}_symbol_date
                            ON {self.schema_name}.{self.table_name} (symbol, tradedatetimeutc);
                        """))
                        
                        conn.execute(text(f"""
                            CREATE INDEX ix_{self.table_name}_symbol
                            ON {self.schema_name}.{self.table_name} (symbol);
                        """))
                        
                        conn.execute(text(f"""
                            CREATE INDEX ix_{self.table_name}_date
                            ON {self.schema_name}.{self.table_name} (tradedatetimeutc);
                        """))

                    logger.info(f"Table {self.schema_name}.{self.table_name} created with indexes and constraints.")
                else:
                    logger.debug(f"Table {self.schema_name}.{self.table_name} already exists.")
        except Exception as e:
            logger.error(f"Error creating table: {e}", exc_info=True)
            raise

    def generate_price_hash(self, row: pd.Series) -> str:
        """Generate unique hash for price record."""
        core_fields = ['symbol', 'tradedatetimeutc', 'pricesymbol']
        available_fields = [col for col in core_fields if col in row.index and pd.notna(row[col])]
        
        if not available_fields:
            if 'symbol' in row.index and 'tradedatetimeutc' in row.index:
                available_fields = ['symbol', 'tradedatetimeutc']
            else:
                exclude_columns = ['created_at', 'updated_at', 'data_source', 'price_hash']
                available_fields = [col for col in row.index if col not in exclude_columns and pd.notna(row[col])]
        
        hash_parts = []
        for col in available_fields:
            value = row[col]
            if pd.notna(value):
                if isinstance(value, (datetime.datetime, pd.Timestamp)):
                    hash_parts.append(f"{col}:{value.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    hash_parts.append(f"{col}:{str(value).lower().strip()}")
        
        hash_str = '|'.join(sorted(hash_parts)) if hash_parts else str(row.to_dict())
        return hashlib.sha256(hash_str.encode()).hexdigest()

    def get_existing_symbols(self) -> Set[str]:
        """Get set of symbols that already exist in the database."""
        try:
            query = f"SELECT DISTINCT symbol FROM {self.schema_name}.{self.table_name}"
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                existing_symbols = {row[0] for row in result if row[0] is not None}
            logger.info(f"Found {len(existing_symbols)} existing symbols in database.")
            return existing_symbols
        except Exception as e:
            logger.warning(f"Error fetching existing symbols: {e}. Returning empty set.")
            return set()

    def get_existing_price_hashes(self, symbols: List[str] = None) -> set:
        """Get existing price hashes, optionally filtered by symbols."""
        try:
            if symbols:
                symbols_str = "', '".join(symbols)
                query = f"""
                    SELECT price_hash 
                    FROM {self.schema_name}.{self.table_name}
                    WHERE symbol IN ('{symbols_str}')
                """
            else:
                query = f"SELECT price_hash FROM {self.schema_name}.{self.table_name}"
            
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                existing_hashes = {row[0] for row in result if row[0] is not None}
            
            logger.debug(f"Retrieved {len(existing_hashes)} existing hashes.")
            return existing_hashes
        except Exception as e:
            logger.warning(f"Error fetching existing hashes: {e}. Returning empty set.")
            return set()

    def remove_duplicates_from_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicates within DataFrame."""
        if df.empty:
            return df
            
        df['price_hash'] = df.apply(self.generate_price_hash, axis=1)
        initial_count = len(df)
        df = df.drop_duplicates(subset=['price_hash'], keep='first')
        final_count = len(df)
        
        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} internal duplicates from DataFrame.")
        
        return df

    def filter_new_price_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out records that already exist in database."""
        if df.empty:
            return df
            
        # Remove internal duplicates
        df = self.remove_duplicates_from_dataframe(df)
        
        # Get existing hashes for symbols in this batch
        symbols_in_batch = df['symbol'].unique().tolist()
        existing_hashes = self.get_existing_price_hashes(symbols_in_batch)
        
        # Filter out existing records
        new_records = df[~df['price_hash'].isin(existing_hashes)]
        
        logger.info(f"Filtered to {len(new_records)} new records out of {len(df)} total.")
        return new_records

    def insert_new_price_records(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Insert new price records with proper type conversion."""
        if df.empty:
            logger.debug("No new price records to insert.")
            return 0, 0

        try:
            # Convert datetime columns
            for col in df.columns:
                if "date" in col.lower() or "time" in col.lower():
                    try:
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    except Exception as e:
                        logger.warning(f"Failed to convert column {col} to datetime: {e}")

            # Convert numeric columns
            numeric_cols = [
                'open', 'high', 'low', 'close', 'last', 'midpoint',
                'volume', 'tradevolume', 'tickcount', 'openinterest',
                'netchange', 'percentchange', 'strike', 'bidsize', 'asksize'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Add metadata
            df = df.copy()
            current_time = datetime.datetime.now()
            df['created_at'] = current_time
            df['updated_at'] = current_time
            df['data_source'] = 'MV_API'

            # Insert with thread safety
            with self.lock:
                write_sql_with_retry(
                    df,
                    self.table_name,
                    schema=self.schema_name,
                    if_exists='append',
                    index=False,
                    chunksize=INSERT_CHUNK_SIZE
                )

            logger.info(f"Successfully inserted {len(df)} new price records.")
            return len(df), 0

        except Exception as e:
            logger.error(f"Failed to insert price records: {e}", exc_info=True)
            # Try chunked insertion as fallback
            return self._insert_price_chunked(df)

    def _insert_price_chunked(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Fallback chunked insertion method."""
        chunk_size = 500
        total_inserted = 0
        total_skipped = 0
        
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size].copy()
            try:
                current_time = datetime.datetime.now()
                chunk['created_at'] = current_time
                chunk['updated_at'] = current_time
                chunk['data_source'] = 'MV_API'
                
                with self.lock:
                    write_sql_with_retry(
                        chunk,
                        self.table_name,
                        schema=self.schema_name,
                        if_exists='append',
                        index=False,
                        chunksize=len(chunk)
                    )
                total_inserted += len(chunk)
                logger.debug(f"Inserted chunk {i//chunk_size + 1}: {len(chunk)} records")
            except Exception as e:
                logger.error(f"Failed to insert chunk {i//chunk_size + 1}: {e}")
                total_skipped += len(chunk)
        
        return total_inserted, total_skipped

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def chunk_list(lst: List, chunk_size: int):
    """Split list into smaller chunks."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def get_instruments_from_db() -> List[str]:
    """Get all instruments from database."""
    try:
        query = f"""
        SELECT DISTINCT symbol 
        FROM {INSTRUMENTS_TABLE}
        WHERE symbol IS NOT NULL
        AND symbol != ''
        ORDER BY symbol
        """
        df = read_sql_with_retry(query)
        logger.info(f"Retrieved {len(df)} instruments from database.")
        return df['symbol'].unique().tolist()
    except Exception as e:
        logger.error(f"Error retrieving instruments: {e}", exc_info=True)
        return []

def extract_price_data_concurrent(
    extractor: PriceDataExtractor,
    symbols_with_records: List[Tuple[str, int]],
    max_workers: int = MAX_WORKERS,
    batch_size: int = BATCH_SIZE
) -> pd.DataFrame:
    """Extract price data concurrently with progress tracking."""
    all_dataframes = []
    
    # Group symbols by their records_back requirement
    batches = []
    current_batch = []
    current_records = None
    
    for symbol, records in symbols_with_records:
        if current_records is None:
            current_records = records
        
        if len(current_batch) >= batch_size or records != current_records:
            if current_batch:
                batches.append((current_batch, current_records))
            current_batch = [symbol]
            current_records = records
        else:
            current_batch.append(symbol)
    
    if current_batch:
        batches.append((current_batch, current_records))
    
    logger.info(f"Processing {len(symbols_with_records)} symbols in {len(batches)} batches with {max_workers} workers.")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(extractor.get_daily_data, batch_symbols, records): (batch_symbols, records)
            for batch_symbols, records in batches
        }
        
        with tqdm(total=len(future_to_batch), desc="Extracting Price Data", unit="batch", ncols=100) as pbar:
            for future in as_completed(future_to_batch):
                batch_symbols, records = future_to_batch[future]
                try:
                    df_batch = future.result(timeout=API_TIMEOUT + 30)
                    if df_batch is not None and not df_batch.empty:
                        all_dataframes.append(df_batch)
                        pbar.set_postfix({"Records": len(df_batch), "Type": f"{records}back"})
                    else:
                        logger.debug(f"No data for batch: {batch_symbols[:2]}...")
                except Exception as e:
                    logger.error(f"Error processing batch {batch_symbols[:2]}...: {e}")
                finally:
                    pbar.update(1)
    
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Combined {len(all_dataframes)} batches into {len(combined_df)} total records.")
        
        # Memory cleanup
        del all_dataframes
        gc.collect()
        
        return combined_df
    else:
        logger.warning("No price data extracted.")
        return pd.DataFrame()

# =============================================================================
# MAIN STREAMING PROCESS
# =============================================================================
def run_streaming_cycle(
    extractor: PriceDataExtractor,
    db_manager: PriceDatabaseManager,
    cycle_number: int
) -> Tuple[int, int]:
    """Run a single streaming cycle."""
    logger.info(f"\n{'='*80}")
    logger.info(f"Starting Cycle #{cycle_number} at {datetime.datetime.now()}")
    logger.info(f"{'='*80}")
    
    try:
        # Get all instruments
        all_instruments = get_instruments_from_db()
        if not all_instruments:
            logger.error("No instruments found in database.")
            return 0, 0
        
        # Get existing symbols in price table
        existing_symbols = db_manager.get_existing_symbols()
        
        # Categorize symbols
        new_symbols = [s for s in all_instruments if s not in existing_symbols]
        existing_symbols_list = [s for s in all_instruments if s in existing_symbols]
        
        logger.info(f"Found {len(new_symbols)} new symbols and {len(existing_symbols_list)} existing symbols.")
        
        # Prepare symbol-records mapping
        symbols_with_records = []
        symbols_with_records.extend([(s, FULL_HISTORY_RECORDS) for s in new_symbols])
        symbols_with_records.extend([(s, INCREMENTAL_RECORDS) for s in existing_symbols_list])
        
        if not symbols_with_records:
            logger.warning("No symbols to process in this cycle.")
            return 0, 0
        
        # Extract price data
        price_data_df = extract_price_data_concurrent(
            extractor=extractor,
            symbols_with_records=symbols_with_records,
            max_workers=MAX_WORKERS,
            batch_size=BATCH_SIZE
        )
        
        if not price_data_df.empty:
            # Create table if needed
            db_manager.create_table_if_not_exists(price_data_df)
            
            # Filter and insert new records
            new_records_df = db_manager.filter_new_price_records(price_data_df)
            
            if not new_records_df.empty:
                inserted, skipped = db_manager.insert_new_price_records(new_records_df)
                logger.info(f"Cycle #{cycle_number} complete: {inserted} inserted, {skipped} skipped.")
                
                # Cleanup
                del price_data_df, new_records_df
                gc.collect()
                
                return inserted, skipped
            else:
                logger.info(f"Cycle #{cycle_number}: No new records to insert.")
                del price_data_df, new_records_df
                gc.collect()
                return 0, 0
        else:
            logger.warning(f"Cycle #{cycle_number}: No price data extracted.")
            return 0, 0
            
    except Exception as e:
        logger.error(f"Error in cycle #{cycle_number}: {e}", exc_info=True)
        return 0, 0

def run_pull():
    """Main continuous streaming process."""
    if not GvWSUSERNAME or not GvWSPASSWORD:
        logger.error("Username or password not found in environment variables.")
        return

    logger.info(f"\n{'#'*80}")
    logger.info("PRICE DATA CONTINUOUS STREAMING SYSTEM STARTED")
    logger.info(f"Configuration:")
    logger.info(f"  - Max Workers: {MAX_WORKERS}")
    logger.info(f"  - Batch Size: {BATCH_SIZE}")
    logger.info(f"  - Full History Records: {FULL_HISTORY_RECORDS}")
    logger.info(f"  - Incremental Records: {INCREMENTAL_RECORDS}")
    logger.info(f"  - Cycle Delay: {STREAM_CYCLE_DELAY}s")
    logger.info(f"  - Schema: {SCHEMA_NAME}")
    logger.info(f"  - Table: {TABLE_NAME}")
    logger.info(f"{'#'*80}\n")

    # Initialize components
    extractor = PriceDataExtractor(GvWSUSERNAME, GvWSPASSWORD, environment="onboard")
    db_manager = PriceDatabaseManager(SCHEMA_NAME, TABLE_NAME)

    try:
        # Ensure schema exists
        db_manager.ensure_schema_exists()
        
        cycle_number = 0
        total_inserted = 0
        total_skipped = 0
        
        # Continuous streaming loop
        while shutdown_handler.should_continue():
            cycle_number += 1
            
            try:
                inserted, skipped = run_streaming_cycle(extractor, db_manager, cycle_number)
                total_inserted += inserted
                total_skipped += skipped
                
                logger.info(f"\nCumulative Stats: {total_inserted} total inserted, {total_skipped} total skipped")
                
                if shutdown_handler.should_continue():
                    logger.info(f"Waiting {STREAM_CYCLE_DELAY}s before next cycle...")
                    
                    # Sleep with interrupt checking
                    for _ in range(STREAM_CYCLE_DELAY):
                        if not shutdown_handler.should_continue():
                            break
                        time.sleep(1)
                        
            except Exception as e:
                logger.error(f"Error in cycle #{cycle_number}: {e}", exc_info=True)
                logger.info("Waiting 60s before retry...")
                time.sleep(60)
        
        logger.info(f"\nStreaming stopped gracefully after {cycle_number} cycles.")
        logger.info(f"Final Stats: {total_inserted} total inserted, {total_skipped} total skipped")
        
    except Exception as e:
        logger.error(f"Fatal error in streaming process: {e}", exc_info=True)
    finally:
        # Cleanup
        gc.collect()
        logger.info("Streaming system shutdown complete.")

# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    try:
        run_pull()
    except KeyboardInterrupt:
        logger.info("\nShutdown requested via keyboard interrupt.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)