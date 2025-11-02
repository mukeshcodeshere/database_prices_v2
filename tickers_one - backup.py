import datetime
import os
import io
import pandas as pd
import time
import logging
import hashlib
import urllib.request
import urllib.error
import uuid
from base64 import b64encode
from typing import List, Dict, Optional, Tuple, Set
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from connect_db import engine, write_sql_with_retry
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError
import gc
from dotenv import load_dotenv
import numpy as np
import traceback
from collections import defaultdict

# ============================================================================
# CONFIGURATION PARAMETERS - MODIFY THESE AS NEEDED
# ==========================================================================
MAX_WORKERS = 1                     # Number of concurrent threads for API calls
MAX_EXCHANGES_PER_CYCLE = 6#None      # None = all exchanges, or set int for testing
BATCH_SIZE = 100                    # Records per database insert batch
SMALL_BATCH_SIZE = 10               # Smaller batch for error recovery
API_TIMEOUT = 3                    # Timeout for API requests in seconds
MAX_API_RETRIES = 3                 # Maximum retry attempts for failed API calls
MEMORY_CLEANUP_FREQUENCY = 10       # Run gc.collect() every N exchanges
SCHEMA_NAME = "MV_PRICES_2"         # Database schema name
TABLE_NAME = "InstrumentList"       # Database table name
LOG_LEVEL = logging.INFO            # Logging level (DEBUG, INFO, WARNING, ERROR)

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'instrument_pipeline_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# DEFAULT API CONFIGURATION
# ============================================================================
class DefaultConfig:
    VERSION = "MarketView PythonSDK/1.0"
    WEBSERVICE_URL = "https://mv-api-proxy.prod.tr.enverus.com/"
    API_SUFFIX = "pythonapi/v1/"
    RESPONSE_FORMAT = "csv"

# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================
class GvException(Exception):
    def __init__(self, message, inner_exception=None):
        super().__init__(message)
        self.inner_exception = inner_exception

# ============================================================================
# API CONNECTION CLASS
# ============================================================================
class MvWSConnection:
    """Thread-safe API connection handler with retry logic."""
    
    def __init__(self, username: str, password: str, config=None):
        self._config = config if config is not None else DefaultConfig
        self._version = self._config.VERSION
        self._webservice_url = self._config.WEBSERVICE_URL
        self._api_suffix = self._config.API_SUFFIX
        self._url_base = self._webservice_url + self._api_suffix
        self._response_format = self._config.RESPONSE_FORMAT
        self._lock = Lock()  # Thread safety for credentials

        user_pass = f"{username}:{password}"
        self.encoded_credentials = b64encode(user_pass.encode('ascii')).decode('ascii')
        
    def make_request(self, url: str, method: str = 'GET', data=None, 
                    content_type: Optional[str] = None, output: bool = True, 
                    timeout: int = API_TIMEOUT) -> str:
        """Make thread-safe API request with comprehensive error handling."""
        try:
            output_string = f"&output={self._response_format}" if output is True else ""
            full_url = self._url_base + url + output_string
            
            headers = {
                'User-Agent': self._version,
                'Authorization': f"Basic {self.encoded_credentials}"
            }
            
            if content_type:
                headers['Content-Type'] = content_type
                
            request = urllib.request.Request(full_url, method=method, data=data, headers=headers)
            
            with self._lock:  # Ensure thread-safe credential usage
                response = urllib.request.urlopen(request, timeout=timeout)

            response_status_code = response.getcode()
            response_text = response.read().decode('utf-8')

            if response_status_code != 200:
                if not response_text:
                    response_text = f"HTTP error, code: {response_status_code}"
                raise GvException(response_text)

            return response_text

        except urllib.error.HTTPError as e:
            error_msg = f"HTTP Error {e.code}: {e.read().decode('utf-8')}"
            logger.error(error_msg)
            raise GvException(error_msg)
        except urllib.error.URLError as e:
            if isinstance(e.reason, Exception) and "timed out" in str(e.reason).lower():
                logger.error(f"Request timeout for {url}")
                raise GvException(f"Request timeout: {e}")
            else:
                logger.error(f"URL Error for {url}: {e}")
                raise GvException(f"URL Error: {e}")
        except Exception as e:
            logger.error(f"Request failed for {url}: {e}")
            raise GvException(str(e))

# ============================================================================
# MARKET DATA EXTRACTOR
# ============================================================================
class MarketDataExtractor:
    """Extracts market instrument data from API with retry logic."""
    
    def __init__(self, username: str, password: str, environment: str = "onboard"):
        self.username = username
        self.password = password
        self.environment = environment
        self.connection = MvWSConnection(username, password)
        self._request_count = 0
        self._failed_requests = defaultdict(int)

    def _fetch_csv(self, endpoint: str, params: dict, 
                   timeout: int = API_TIMEOUT, 
                   max_retries: int = MAX_API_RETRIES) -> Optional[pd.DataFrame]:
        """Fetch CSV data with exponential backoff retry logic."""
        for attempt in range(max_retries):
            try:
                query_params = {**params, "env": self.environment}
                query_string = "&".join([f"{k}={v}" for k, v in query_params.items()])
                url = f"{endpoint}?{query_string}"
                
                self._request_count += 1
                response_text = self.connection.make_request(url, timeout=timeout)
                
                if response_text and response_text.strip():
                    df = pd.read_csv(io.StringIO(response_text))
                    if not df.empty:
                        return df
                    else:
                        logger.warning(f"Empty DataFrame for {endpoint} with params {params}")
                        return None
                else:
                    logger.warning(f"Empty response for {endpoint}")
                    return None
                    
            except GvException as e:
                self._failed_requests[endpoint] += 1
                logger.warning(f"API error fetching {endpoint} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    sleep_time = (2 ** attempt) + (time.time() % 1)  # Exponential backoff with jitter
                    time.sleep(sleep_time)
                else:
                    return None
            except Exception as e:
                self._failed_requests[endpoint] += 1
                logger.error(f"Unexpected error fetching {endpoint} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
        return None

    def get_exchange_list(self) -> Optional[pd.DataFrame]:
        """Get list of all available exchanges."""
        params = {}
        df = self._fetch_csv("getExchangeList", params, timeout=60)  # Longer timeout for exchange list
        if df is not None and not df.empty:
            logger.info(f"Retrieved {len(df)} exchanges")
            return df
        else:
            logger.error("Failed to retrieve exchange list")
            return None

    def get_exchange_codes(self) -> List[str]:
        """Get list of exchange codes for processing."""
        try:
            df = self.get_exchange_list()
            if df is not None and not df.empty and 'exchangecode' in df.columns:
                codes = df['exchangecode'].dropna().unique().tolist()
                logger.info(f"Found {len(codes)} unique exchange codes")
                return codes
            else:
                logger.error("No exchange data available or missing 'exchangecode' column")
                return []
        except Exception as e:
            logger.error(f"Error getting exchange codes: {e}")
            logger.error(traceback.format_exc())
            return []

    def get_symbols_for_exchange(self, exchange_code: str, 
                                records_back: int = 10000) -> Optional[pd.DataFrame]:
        """Pull all symbols for a specific exchange."""
        try:
            params = {
                "exchangecode": exchange_code,
                "recordsback": records_back
            }
            df = self._fetch_csv("symbolSearchByExchange", params, timeout=30)  # 30 second timeout
            if df is not None and not df.empty:
                # Add exchange_code column if not present
                if 'exchange_code' not in df.columns:
                    df['exchange_code'] = exchange_code
                logger.debug(f"Retrieved {len(df)} symbols for exchange {exchange_code}")
                return df
            else:
                logger.warning(f"No data for exchange {exchange_code}")
                return None
        except Exception as e:
            logger.error(f"Error getting symbols for {exchange_code}: {e}")
            logger.error(traceback.format_exc())
            return None

    def get_stats(self) -> dict:
        """Return statistics about API usage."""
        return {
            'total_requests': self._request_count,
            'failed_endpoints': dict(self._failed_requests)
        }

# ============================================================================
# DATABASE MANAGER
# ============================================================================
class DatabaseManager:
    """Manages database operations with deduplication and proper schema handling."""
    
    def __init__(self, schema_name: str, table_name: str):
        self.schema_name = schema_name
        self.table_name = table_name
        self.engine = engine
        self._insert_lock = Lock()  # Thread safety for insertions
        self._hash_cache: Set[str] = set()
        self._cache_valid = False
        
    def ensure_schema_exists(self) -> bool:
        """Ensure schema exists in database."""
        try:
            with self.engine.connect() as connection:
                inspector = inspect(connection)
                if self.schema_name not in inspector.get_schema_names():
                    connection.execute(text(f"CREATE SCHEMA {self.schema_name}"))
                    connection.commit()
                    logger.info(f"✓ Schema '{self.schema_name}' created")
                else:
                    logger.info(f"✓ Schema '{self.schema_name}' exists")
            return True
        except Exception as e:
            logger.error(f"✗ Error ensuring schema exists: {e}")
            logger.error(traceback.format_exc())
            return False

    def drop_and_recreate_table(self) -> bool:
        """Drop and recreate table with proper schema and indexes."""
        try:
            with self.engine.begin() as connection:
                # Drop table if exists
                connection.execute(text(f"DROP TABLE IF EXISTS {self.schema_name}.{self.table_name}"))
                logger.info(f"Dropped table {self.schema_name}.{self.table_name}")
                
                # Create table with all NVARCHAR columns
                connection.execute(text(f"""
                    CREATE TABLE {self.schema_name}.{self.table_name} (
                        symbol NVARCHAR(255),
                        exchange_code NVARCHAR(50),
                        description NVARCHAR(MAX),
                        security_type NVARCHAR(50),
                        gross_net NVARCHAR(10),
                        price_indicator NVARCHAR(50),
                        expiration_date NVARCHAR(50),
                        display_date NVARCHAR(50),
                        put_call NVARCHAR(10),
                        strike_price NVARCHAR(50),
                        option_root NVARCHAR(50),
                        session NVARCHAR(50),
                        trade_units NVARCHAR(50),
                        currency NVARCHAR(10),
                        lot_units NVARCHAR(50),
                        conversion_factor NVARCHAR(50),
                        underlier NVARCHAR(255),
                        alternate_symbol NVARCHAR(255),
                        alt_exchange NVARCHAR(50),
                        prev_settle_date NVARCHAR(50),
                        settle_date NVARCHAR(50),
                        datecreated NVARCHAR(50),
                        display_date2 NVARCHAR(50),
                        rowguid NVARCHAR(50),
                        conversion_code NVARCHAR(50),
                        source_symbol NVARCHAR(255),
                        curve_period NVARCHAR(50),
                        forward_period NVARCHAR(50),
                        category NVARCHAR(50),
                        model_code NVARCHAR(50),
                        marketbasis NVARCHAR(200),
                        product NVARCHAR(50),
                        commodity NVARCHAR(50),
                        supplier NVARCHAR(50),
                        startdate NVARCHAR(50),
                        enddate NVARCHAR(50),
                        fidgroup NVARCHAR(50),
                        rownum NVARCHAR(50),
                        -- Metadata columns
                        instrument_hash NVARCHAR(64) NOT NULL UNIQUE,
                        created_at DATETIME2 NOT NULL,
                        updated_at DATETIME2 NOT NULL,
                        last_seen_date DATE NOT NULL,
                        last_price_pull_date DATE NULL,
                        PRIMARY KEY (instrument_hash)
                    )
                """))
                
                # Create indexes for performance
                connection.execute(text(f"""
                    CREATE INDEX ix_{self.table_name}_symbol 
                    ON {self.schema_name}.{self.table_name} (symbol)
                """))
                
                connection.execute(text(f"""
                    CREATE INDEX ix_{self.table_name}_exchange_code 
                    ON {self.schema_name}.{self.table_name} (exchange_code)
                """))
                
                connection.execute(text(f"""
                    CREATE INDEX ix_{self.table_name}_last_seen 
                    ON {self.schema_name}.{self.table_name} (last_seen_date)
                """))
                
                logger.info(f"✓ Created table {self.schema_name}.{self.table_name} with indexes")
                
            self._cache_valid = False  # Invalidate cache
            return True
                
        except Exception as e:
            logger.error(f"✗ Error recreating table: {e}")
            logger.error(traceback.format_exc())
            return False

    def table_exists(self) -> bool:
        """Check if table exists in database."""
        try:
            with self.engine.connect() as connection:
                inspector = inspect(connection)
                tables = inspector.get_table_names(schema=self.schema_name)
                return self.table_name in tables
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize DataFrame data."""
        if df.empty:
            return df
            
        cleaned_df = df.copy()
        
        # Convert all columns to string and handle NaN
        for column in cleaned_df.columns:
            cleaned_df[column] = cleaned_df[column].astype(str)
            cleaned_df[column] = cleaned_df[column].replace(['nan', 'None', '<NA>', 'NaT'], None)
        
        return cleaned_df

    def generate_instrument_hash(self, row: pd.Series) -> str:
        """Generate unique hash for instrument."""
        hash_columns = ['symbol', 'exchange_code', 'security_type', 'option_root', 'source_symbol']
        
        hash_parts = []
        for col in hash_columns:
            if col in row.index and pd.notna(row[col]) and str(row[col]) != 'None':
                str_value = str(row[col]).strip().lower()
                hash_parts.append(f"{col}:{str_value}")
        
        if not hash_parts:
            # Fallback: use all non-null columns
            for col in row.index:
                if pd.notna(row[col]) and str(row[col]) != 'None' and col not in [
                    'instrument_hash', 'created_at', 'updated_at', 'last_seen_date', 'last_price_pull_date'
                ]:
                    str_value = str(row[col]).strip().lower()
                    hash_parts.append(f"{col}:{str_value}")
        
        if hash_parts:
            hash_str = '|'.join(sorted(hash_parts))
            return hashlib.sha256(hash_str.encode()).hexdigest()
        else:
            # Final fallback
            return hashlib.sha256(str(uuid.uuid4()).encode()).hexdigest()

    def prepare_data_for_insert(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame for database insertion."""
        if df.empty:
            return df
        
        # Clean the data
        prepared_df = self.clean_dataframe(df)
        
        # Generate unique hashes
        prepared_df['instrument_hash'] = prepared_df.apply(self.generate_instrument_hash, axis=1)
        
        # Remove duplicates based on hash
        initial_count = len(prepared_df)
        prepared_df = prepared_df.drop_duplicates(subset=['instrument_hash'], keep='first')
        final_count = len(prepared_df)
        
        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} duplicate hashes from batch")
        
        # Add metadata columns
        current_time = datetime.datetime.now()
        current_date = datetime.date.today()
        
        prepared_df['created_at'] = current_time
        prepared_df['updated_at'] = current_time
        prepared_df['last_seen_date'] = current_date
        prepared_df['last_price_pull_date'] = None
        
        # Ensure column structure
        prepared_df = self.ensure_column_structure(prepared_df)
        
        return prepared_df

    def ensure_column_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure DataFrame has all required columns in correct order."""
        expected_columns = [
            'symbol', 'exchange_code', 'description', 'security_type', 'gross_net',
            'price_indicator', 'expiration_date', 'display_date', 'put_call', 
            'strike_price', 'option_root', 'session', 'trade_units', 'currency',
            'lot_units', 'conversion_factor', 'underlier', 'alternate_symbol', 'alt_exchange',
            'prev_settle_date', 'settle_date', 'datecreated', 'display_date2',
            'rowguid', 'conversion_code', 'source_symbol', 'curve_period',
            'forward_period', 'category', 'model_code', 'marketbasis', 'product',
            'commodity', 'supplier', 'startdate', 'enddate', 'fidgroup', 'rownum',
            'instrument_hash', 'created_at', 'updated_at', 'last_seen_date', 'last_price_pull_date'
        ]
        
        # Add missing columns
        for column in expected_columns:
            if column not in df.columns:
                df[column] = None
        
        # Reorder columns
        df = df[expected_columns]
        
        return df

    def get_existing_hashes(self, force_refresh: bool = False) -> Set[str]:
        """Get set of existing hashes from database with caching."""
        if self._cache_valid and not force_refresh and self._hash_cache:
            return self._hash_cache
            
        try:
            with self.engine.connect() as connection:
                result = connection.execute(
                    text(f"SELECT instrument_hash FROM {self.schema_name}.{self.table_name} WHERE instrument_hash IS NOT NULL")
                )
                self._hash_cache = {row[0] for row in result}
                self._cache_valid = True
                logger.info(f"Loaded {len(self._hash_cache)} existing hashes into cache")
                return self._hash_cache
        except Exception as e:
            logger.warning(f"Error fetching existing hashes: {e}")
            return set()

    def insert_records(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Insert records with deduplication and error handling."""
        if df.empty:
            return 0, 0

        with self._insert_lock:  # Thread-safe insertion
            try:
                # Filter out existing records
                existing_hashes = self.get_existing_hashes()
                new_records = df[~df['instrument_hash'].isin(existing_hashes)]
                
                if new_records.empty:
                    logger.debug("No new records to insert (all duplicates)")
                    return 0, 0

                logger.info(f"Inserting {len(new_records)} new records (filtered {len(df) - len(new_records)} duplicates)")

                # Replace NaN with None
                new_records = new_records.replace({np.nan: None})
                
                total_inserted = 0
                total_failed = 0
                
                # Insert in batches
                for i in range(0, len(new_records), BATCH_SIZE):
                    batch = new_records.iloc[i:i + BATCH_SIZE]
                    try:
                        write_sql_with_retry(
                            batch, 
                            self.table_name, 
                            schema=self.schema_name, 
                            if_exists='append', 
                            index=False, 
                            chunksize=SMALL_BATCH_SIZE
                        )
                        total_inserted += len(batch)
                        
                        # Update hash cache
                        self._hash_cache.update(batch['instrument_hash'].tolist())
                        
                    except Exception as e:
                        logger.error(f"Failed batch insert at position {i}: {e}")
                        
                        # Try individual inserts for failed batch
                        for idx, record in batch.iterrows():
                            try:
                                single_df = pd.DataFrame([record])
                                write_sql_with_retry(
                                    single_df,
                                    self.table_name,
                                    schema=self.schema_name,
                                    if_exists='append',
                                    index=False
                                )
                                total_inserted += 1
                                self._hash_cache.add(record['instrument_hash'])
                            except Exception as single_error:
                                logger.error(f"Failed single insert for hash {record.get('instrument_hash', 'unknown')}: {single_error}")
                                total_failed += 1
                
                return total_inserted, total_failed
                
            except Exception as e:
                logger.error(f"Error during insertion: {e}")
                logger.error(traceback.format_exc())
                return 0, len(df)

    def get_record_count(self) -> int:
        """Get total record count in table."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(
                    text(f"SELECT COUNT(*) FROM {self.schema_name}.{self.table_name}")
                )
                count = result.scalar()
                return count
        except Exception as e:
            logger.error(f"Error getting record count: {e}")
            return 0

# ============================================================================
# STREAMING PIPELINE - SINGLE CYCLE WITH BATCHED PULL/PUSH
# ============================================================================
class StreamingPipeline:
    """Pipeline that pulls and pushes in batches simultaneously in ONE cycle."""
    
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.extractor = MarketDataExtractor(username, password)
        self.db_manager = DatabaseManager(SCHEMA_NAME, TABLE_NAME)
        self.total_inserted = 0
        self.total_failed = 0
        self.start_time = datetime.datetime.now()
        
    def initialize(self) -> bool:
        """Initialize database schema and table."""
        try:
            logger.info("=" * 80)
            logger.info("INITIALIZING PIPELINE")
            logger.info("=" * 80)
            
            if not self.db_manager.ensure_schema_exists():
                return False
            
            # Check if table exists, create if not
            if not self.db_manager.table_exists():
                logger.info("Table does not exist, creating...")
                if not self.db_manager.drop_and_recreate_table():
                    return False
            else:
                logger.info(f"Table exists with {self.db_manager.get_record_count()} records")
                # Load existing hashes into cache
                self.db_manager.get_existing_hashes(force_refresh=True)
            
            logger.info("✓ Initialization complete")
            return True
            
        except Exception as e:
            logger.error(f"✗ Initialization failed: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def run_single_cycle(self) -> None:
        """
        Run ONE cycle: pull all tickers from exchanges and push only new ones to DB.
        Uses concurrent extraction with batched inserts as data becomes available.
        """
        cycle_start = time.time()
        
        logger.info("=" * 80)
        logger.info("STARTING SINGLE CYCLE")
        logger.info("=" * 80)
        
        try:
            # Get exchange codes
            exchange_codes = self.extractor.get_exchange_codes()
            if not exchange_codes:
                logger.error("No exchange codes found - possible authentication issue")
                return

            if MAX_EXCHANGES_PER_CYCLE:
                exchange_codes = exchange_codes[:MAX_EXCHANGES_PER_CYCLE]
                logger.info(f"Limited to {MAX_EXCHANGES_PER_CYCLE} exchanges")

            logger.info(f"Processing {len(exchange_codes)} exchanges with {MAX_WORKERS} workers")

            # Process exchanges concurrently and insert as we go
            successful = 0
            failed = 0
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all extraction tasks
                futures = {
                    executor.submit(self.extractor.get_symbols_for_exchange, code): code
                    for code in exchange_codes
                }
                
                with tqdm(total=len(futures), desc="Extracting & Inserting", unit="exchange") as pbar:
                    for future in as_completed(futures):
                        code = futures[future]
                        try:
                            # Get data for this exchange (increased timeout)
                            df = future.result(timeout=60)  # 60 second timeout for future
                            
                            if df is not None and not df.empty:
                                # Immediately prepare and insert this exchange's data
                                prepared_df = self.db_manager.prepare_data_for_insert(df)
                                inserted, failed_records = self.db_manager.insert_records(prepared_df)
                                
                                self.total_inserted += inserted
                                self.total_failed += failed_records
                                
                                successful += 1
                                pbar.set_postfix({
                                    "✓": successful, 
                                    "✗": failed, 
                                    "inserted": self.total_inserted,
                                    "current": code
                                })
                                
                                # Clean up memory
                                del df, prepared_df
                                
                            else:
                                failed += 1
                                pbar.set_postfix({
                                    "✓": successful, 
                                    "✗": failed,
                                    "inserted": self.total_inserted,
                                    "current": code
                                })
                                
                        except Exception as e:
                            logger.error(f"Error processing {code}: {e}")
                            failed += 1
                            pbar.set_postfix({
                                "✓": successful, 
                                "✗": failed,
                                "inserted": self.total_inserted,
                                "current": code
                            })
                        finally:
                            pbar.update(1)
                            
                            # Periodic memory cleanup
                            if (successful + failed) % MEMORY_CLEANUP_FREQUENCY == 0:
                                gc.collect()
            
            # Final cleanup
            gc.collect()
            
            # Cycle summary
            cycle_duration = time.time() - cycle_start
            logger.info("=" * 80)
            logger.info("CYCLE COMPLETE")
            logger.info(f"  Duration: {cycle_duration:.2f} seconds")
            logger.info(f"  Exchanges Processed: {successful}")
            logger.info(f"  Exchanges Failed: {failed}")
            logger.info(f"  New Records Inserted: {self.total_inserted}")
            logger.info(f"  Records Failed: {self.total_failed}")
            logger.info(f"  Total DB Records: {self.db_manager.get_record_count()}")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"✗ Error in cycle: {e}")
            logger.error(traceback.format_exc())
    
    def print_final_summary(self) -> None:
        """Print final pipeline summary."""
        runtime = datetime.datetime.now() - self.start_time
        
        logger.info("=" * 80)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total Runtime: {runtime}")
        logger.info(f"Records Inserted: {self.total_inserted}")
        logger.info(f"Records Failed: {self.total_failed}")
        logger.info(f"Final DB Count: {self.db_manager.get_record_count()}")
        
        # API statistics
        api_stats = self.extractor.get_stats()
        logger.info(f"Total API Requests: {api_stats['total_requests']}")
        
        if api_stats['failed_endpoints']:
            logger.info(f"Failed Endpoints ({len(api_stats['failed_endpoints'])}):")
            for endpoint, count in sorted(api_stats['failed_endpoints'].items(), 
                                         key=lambda x: x[1], reverse=True)[:10]:
                logger.info(f"  {endpoint}: {count} failures")
        
        # Calculate rates
        if runtime.total_seconds() > 0:
            rate = self.total_inserted / runtime.total_seconds()
            logger.info(f"Average Insert Rate: {rate:.2f} records/second")
        
        logger.info("=" * 80)
        logger.info("✓ PIPELINE COMPLETE")
        logger.info("=" * 80)

# ============================================================================
# MAIN EXECUTION
# ============================================================================
def run_tickers():
    """Main entry point for the pipeline - runs ONE cycle."""
    try:
        # Load environment variables
        load_dotenv()
        username = os.getenv("GvWSUSERNAME")
        password = os.getenv("GvWSPASSWORD")

        if not username or not password:
            logger.error("=" * 80)
            logger.error("✗ AUTHENTICATION ERROR")
            logger.error("=" * 80)
            logger.error("Username or password not found in environment variables")
            logger.error("Please ensure GvWSUSERNAME and GvWSPASSWORD are set in .env file")
            logger.error("=" * 80)
            return

        # Create and initialize pipeline
        pipeline = StreamingPipeline(username, password)
        
        if not pipeline.initialize():
            logger.error("✗ Pipeline initialization failed")
            return
        
        # Run single cycle with continuous pull/push
        pipeline.run_single_cycle()
        
        # Print summary
        pipeline.print_final_summary()
        
    except KeyboardInterrupt:
        logger.info("\n" + "=" * 80)
        logger.info(" KEYBOARD INTERRUPT - SHUTTING DOWN")
        logger.info("=" * 80)
    except Exception as e:
        logger.error("=" * 80)
        logger.error("✗ FATAL ERROR")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        logger.error(traceback.format_exc())
        logger.error("=" * 80)

# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    run_tickers()