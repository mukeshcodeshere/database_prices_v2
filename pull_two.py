import datetime
import os
import io
import time
import hashlib
from typing import List, Dict, Optional, Tuple
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import gc

# Import database functions
from connect_db import read_sql_with_retry, write_sql_with_retry, engine
from sqlalchemy import NVARCHAR, DATETIME, Float, BigInteger, Text, inspect, text
from sqlalchemy.exc import SQLAlchemyError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('price_data_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
GvWSUSERNAME = os.getenv("GvWSUSERNAME")
GvWSPASSWORD = os.getenv("GvWSPASSWORD")

# Constants
TWENTY_YEARS_DAYS = 7300  # ~20 years
TWO_MONTHS_DAYS = 60      # ~2 months

class PriceDataExtractor:
    BASE_URL = "https://mv-api-proxy.prod.tr.enverus.com/pythonapi/v1"

    def __init__(self, username: str, password: str, environment: str = "onboard"):
        """Initialize the price data extractor with authentication setup."""
        self.username = username
        self.password = password
        self.environment = environment
        self.session = self._create_authenticated_session()

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

        # MV endpoints require Basic Auth
        session.auth = (self.username, self.password)
        logger.info("Authenticated session created successfully.")
        return session

    def _fetch_csv_data(self, params: dict) -> Optional[pd.DataFrame]:
        """Helper to call GetDaily API endpoint that returns CSV output."""
        try:
            params["output"] = "csv"
            url = f"{self.BASE_URL}/GetDaily"
            
            logger.info(f"Fetching data for symbols: {params.get('symbols', '')[:100]}...")
            
            response = self.session.get(url, params=params, timeout=120)  # 2 minute timeout

            if response.status_code == 200:
                df = pd.read_csv(io.StringIO(response.text))
                logger.info(f"Successfully retrieved {len(df)} records.")
                return df
            else:
                logger.warning(f"Request failed ({response.status_code}): {response.text[:500]}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching price data: {e}")
            return None

    def get_daily_data(self, symbols: List[str], records_back: int = 7300, max_retries: int = 3) -> Optional[pd.DataFrame]:
        """Get daily price data for symbols with retry logic."""
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
                # Add small delay to avoid overwhelming the API
                time.sleep(0.5)
                
                # Format symbols as comma-separated string with quotes
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
                    logger.info(f"Retrieved {len(df)} price records for {len(symbols)} symbols.")
                    return df
                elif df is not None and df.empty:
                    logger.warning(f"No data returned for symbols: {symbols[:3]}...")
                    return None
                    
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for symbols {symbols[:3]}...: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to get price data after {max_retries} attempts.")
        return None

class PriceDatabaseManager:
    """Handles database operations for price data with duplicate prevention."""
    
    def __init__(self, schema_name: str, table_name: str):
        self.schema_name = schema_name
        self.table_name = table_name
        self.engine = engine

    def create_table_if_not_exists(self, df_sample: pd.DataFrame):
        """Create price table with proper types and indexes."""
        try:
            with self.engine.connect() as connection:
                inspector = inspect(connection)

                if not inspector.has_table(self.table_name, schema=self.schema_name):
                    logger.info(f"Table {self.schema_name}.{self.table_name} does not exist. Creating...")

                    # Build an explicit dtype map so strings are NOT MAX
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

                    # Create the empty table
                    df_sample.head(0).to_sql(
                        self.table_name,
                        self.engine,
                        schema=self.schema_name,
                        if_exists='fail',
                        index=False,
                        dtype=dtypes
                    )

                    # Add metadata columns
                    with self.engine.begin() as conn:
                        conn.execute(text(f"""
                            ALTER TABLE {self.schema_name}.{self.table_name}
                            ADD price_hash VARCHAR(64),
                                created_at DATETIME DEFAULT GETDATE(),
                                updated_at DATETIME DEFAULT GETDATE(),
                                data_source VARCHAR(50) DEFAULT 'MV_API';
                        """))

                        # Unique constraint on hash
                        conn.execute(text(f"""
                            ALTER TABLE {self.schema_name}.{self.table_name}
                            ADD CONSTRAINT uk_{self.table_name}_price_hash UNIQUE (price_hash);
                        """))

                        # Index on symbol + date
                        conn.execute(text(f"""
                            CREATE INDEX ix_{self.table_name}_symbol_date
                            ON {self.schema_name}.{self.table_name} (symbol, tradedatetimeutc);
                        """))

                    logger.info(f"Table {self.schema_name}.{self.table_name} created with indexes.")
                else:
                    logger.info(f"Table {self.schema_name}.{self.table_name} already exists.")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def generate_price_hash(self, row: pd.Series) -> str:
        """Generate a unique hash for a price record based on symbol and trade datetime."""
        # Core identifying fields for price data
        core_fields = ['symbol', 'tradedatetimeutc']
        
        # Use available core columns that exist in the row
        available_core_fields = [col for col in core_fields if col in row.index and pd.notna(row[col])]
        
        if not available_core_fields:
            # Fallback: use all non-metadata fields
            exclude_columns = ['created_at', 'updated_at', 'data_source', 'price_hash']
            available_core_fields = [col for col in row.index if col not in exclude_columns and pd.notna(row[col])]
        
        # Create hash string from core identifying fields
        hash_parts = []
        for col in available_core_fields:
            value = row[col]
            if pd.notna(value):
                # Convert datetime to string for consistent hashing
                if isinstance(value, (datetime.datetime, pd.Timestamp)):
                    hash_parts.append(f"{col}:{value.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    hash_parts.append(f"{col}:{str(value).lower().strip()}")
        
        if not hash_parts:
            # Generate hash from the entire row as last resort
            hash_str = str(row.to_dict())
        else:
            hash_str = '|'.join(sorted(hash_parts))
        
        return hashlib.sha256(hash_str.encode()).hexdigest()

    def remove_duplicates_from_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicates from the DataFrame before insertion."""
        if df.empty:
            return df
            
        # Generate hash for each row based on core price identifying fields
        df['price_hash'] = df.apply(self.generate_price_hash, axis=1)
        
        # Remove duplicates within the DataFrame itself
        initial_count = len(df)
        df = df.drop_duplicates(subset=['price_hash'], keep='first')
        final_count = len(df)
        
        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} duplicates from DataFrame.")
        
        return df

    def get_existing_price_hashes(self) -> set:
        """Get set of existing price hashes from the database."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(
                    text(f"SELECT price_hash FROM {self.schema_name}.{self.table_name}")
                )
                existing_hashes = {row[0] for row in result if row[0] is not None}
                logger.info(f"Found {len(existing_hashes)} existing price records in database.")
                return existing_hashes
        except Exception as e:
            logger.warning(f"Error fetching existing price hashes: {e}. Returning empty set.")
            return set()

    def filter_new_price_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out price records that already exist in the database."""
        if df.empty:
            return df
            
        # Remove internal duplicates first
        df = self.remove_duplicates_from_dataframe(df)
        
        # Get existing hashes from database
        existing_hashes = self.get_existing_price_hashes()
        
        # Filter out records that already exist
        new_records = df[~df['price_hash'].isin(existing_hashes)]
        
        logger.info(f"Filtered to {len(new_records)} new price records out of {len(df)} total.")
        
        return new_records

    def insert_new_price_records(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Insert new price records into the SQL Server database."""
        if df.empty:
            logger.info("No new price records to insert.")
            return 0, 0

        try:
            # Convert date/time columns
            for col in df.columns:
                if "date" in col.lower() or "time" in col.lower():
                    try:
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                    except Exception as e:
                        logger.warning(f"Failed to convert column {col} to datetime: {e}")

            # Convert numeric columns safely
            numeric_cols = [
                'open', 'high', 'low', 'close', 'last', 'midpoint',
                'volume', 'tradevolume', 'tickcount', 'openinterest',
                'netchange', 'percentchange', 'strike'
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

            # Insert into SQL with retry
            write_sql_with_retry(
                df,
                self.table_name,
                schema=self.schema_name,
                if_exists='append',
                index=False,
                chunksize=1000
            )

            logger.info(f"Successfully inserted {len(df)} new price records into {self.schema_name}.{self.table_name}")
            return len(df), 0

        except Exception as e:
            logger.exception(f"Failed to insert new price records: {e}")
            return 0, len(df)

def chunk_list(lst, chunk_size):
    """Split list into smaller chunks."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def get_instruments_from_db(limit: int = None) -> Tuple[List[str], List[str]]:
    """
    Get instruments from the database.
    Returns: (new_instruments, existing_instruments)
    """
    query = """
    SELECT symbol, last_price_pull_date 
    FROM [MV_PRICES_2].[InstrumentList] 
    WHERE symbol IS NOT NULL AND symbol != ''
    """
    
    if limit:
        query += f" ORDER BY symbol OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"
    else:
        query += " ORDER BY symbol"
    
    df = read_sql_with_retry(query)
    logger.info(f"Retrieved {len(df)} instruments from database.")
    
    # Separate new instruments (never pulled) from existing ones
    new_instruments = df[df['last_price_pull_date'].isna()]['symbol'].tolist()
    existing_instruments = df[df['last_price_pull_date'].notna()]['symbol'].tolist()
    
    logger.info(f"Found {len(new_instruments)} new instruments and {len(existing_instruments)} existing instruments.")
    return new_instruments, existing_instruments

def extract_price_data_concurrent(extractor: PriceDataExtractor, 
                                 symbols: List[str], 
                                 records_back: int = 7300,
                                 max_workers: int = 3,
                                 batch_size: int = 3) -> pd.DataFrame:
    """Extract price data for all symbols concurrently."""
    all_dataframes = []
    
    # Split symbols into batches
    batches = list(chunk_list(symbols, batch_size))
    logger.info(f"Processing {len(symbols)} symbols in {len(batches)} batches with {max_workers} workers.")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks for each batch
        future_to_batch = {
            executor.submit(extractor.get_daily_data, batch, records_back): batch
            for batch in batches
        }
        
        # Process completed tasks with progress bar
        for future in tqdm(as_completed(future_to_batch),
                          total=len(future_to_batch),
                          desc="Extracting price data"):
            batch_symbols = future_to_batch[future]
            try:
                df_batch = future.result(timeout=180)  # 3 minute timeout
                if df_batch is not None and not df_batch.empty:
                    all_dataframes.append(df_batch)
                else:
                    logger.warning(f"No data returned for batch: {batch_symbols}")
            except Exception as e:
                logger.error(f"Error processing batch {batch_symbols}: {e}")
    
    # Combine all dataframes
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Combined {len(all_dataframes)} batches into {len(combined_df)} total records.")
        return combined_df
    else:
        logger.warning("No price data extracted.")
        return pd.DataFrame()

def update_last_price_pull_date(symbols: List[str]):
    """Update last_price_pull_date for instruments that had price data pulled."""
    if not symbols:
        return
        
    try:
        with engine.begin() as connection:
            # Create a temporary table with symbols
            connection.execute(text("CREATE TABLE #temp_symbols (symbol VARCHAR(100))"))
            
            # Insert symbols in chunks
            chunk_size = 1000
            for i in range(0, len(symbols), chunk_size):
                chunk = symbols[i:i + chunk_size]
                values = ",".join([f"('{sym.replace("'", "''")}')" for sym in chunk])
                connection.execute(text(f"INSERT INTO #temp_symbols (symbol) VALUES {values}"))
            
            # Update last_price_pull_date
            result = connection.execute(text("""
                UPDATE [MV_PRICES_2].[InstrumentList] 
                SET last_price_pull_date = CURRENT_DATE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE symbol IN (SELECT symbol FROM #temp_symbols)
            """))
            
            # Drop temporary table
            connection.execute(text("DROP TABLE #temp_symbols"))
            
            logger.info(f"Updated last_price_pull_date for {result.rowcount} instruments.")
            
    except Exception as e:
        logger.error(f"Error updating last_price_pull_date: {e}")

def run_pull():
    """Main function to extract and store price data."""
    if not GvWSUSERNAME or not GvWSPASSWORD:
        logger.error("Username or password not found in environment variables.")
        return

    # Initialize components
    extractor = PriceDataExtractor(GvWSUSERNAME, GvWSPASSWORD, environment="onboard")
    
    # Define schema and table names
    schema_name = "MV_PRICES_2"
    table_name = "MV_All_Prices"
    
    # Initialize database manager
    db_manager = PriceDatabaseManager(schema_name, table_name)

    try:
        # Check if schema exists, if not, create it
        with engine.connect() as connection:
            inspector = inspect(connection)
            if schema_name not in inspector.get_schema_names():
                connection.execute(text(f"CREATE SCHEMA {schema_name}"))
                connection.commit()
                logger.info(f"Schema '{schema_name}' created successfully.")
            else:
                logger.info(f"Schema '{schema_name}' already exists.")

        # Get instruments from database
        logger.info("Retrieving instruments from database...")
        new_instruments, existing_instruments = get_instruments_from_db(limit=None)  # Remove limit for production
        
        total_inserted = 0
        processed_symbols = []

        # Process new instruments (full 20-year pull)
        if new_instruments:
            logger.info(f"Processing {len(new_instruments)} NEW instruments with full 20-year data pull...")
            new_price_data = extract_price_data_concurrent(
                extractor=extractor,
                symbols=new_instruments,
                records_back=TWENTY_YEARS_DAYS,
                max_workers=8,
                batch_size=5
            )

            if not new_price_data.empty:
                # Create table if it doesn't exist
                db_manager.create_table_if_not_exists(new_price_data)

                # Filter out duplicates and get only new records
                new_records_df = db_manager.filter_new_price_records(new_price_data)

                # Insert only new records
                if not new_records_df.empty:
                    inserted, skipped = db_manager.insert_new_price_records(new_records_df)
                    total_inserted += inserted
                    logger.info(f"New instruments: {inserted} records inserted, {skipped} skipped.")
                    
                    # Mark these instruments as processed
                    processed_symbols.extend(new_instruments)
                else:
                    logger.info("No new price records to insert for new instruments.")

                # Clean up memory
                del new_price_data, new_records_df
                gc.collect()

        # Process existing instruments (incremental 2-month pull)
        if existing_instruments:
            logger.info(f"Processing {len(existing_instruments)} EXISTING instruments with incremental 2-month data pull...")
            existing_price_data = extract_price_data_concurrent(
                extractor=extractor,
                symbols=existing_instruments,
                records_back=TWO_MONTHS_DAYS,
                max_workers=3,
                batch_size=3
            )

            if not existing_price_data.empty:
                # Filter out duplicates and get only new records
                new_records_df = db_manager.filter_new_price_records(existing_price_data)

                # Insert only new records
                if not new_records_df.empty:
                    inserted, skipped = db_manager.insert_new_price_records(new_records_df)
                    total_inserted += inserted
                    logger.info(f"Existing instruments: {inserted} records inserted, {skipped} skipped.")
                    
                    # Mark these instruments as processed
                    processed_symbols.extend(existing_instruments)
                else:
                    logger.info("No new price records to insert for existing instruments.")

                # Clean up memory
                del existing_price_data, new_records_df
                gc.collect()

        # Update last_price_pull_date for all processed symbols
        if processed_symbols:
            update_last_price_pull_date(processed_symbols)
            logger.info(f"Price data extraction complete. Total {total_inserted} records inserted for {len(processed_symbols)} instruments.")
        else:
            logger.info("No instruments processed.")

    except Exception as e:
        logger.error(f"An unexpected error occurred during execution: {e}")

if __name__ == "__main__":
    run_pull()