import datetime
import os
import io
import pandas as pd
import time
import logging
import hashlib
import urllib.request
import urllib.error
import urllib.parse
from base64 import b64encode
from typing import List, Dict, Optional, Tuple, Set
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from connect_db import engine, write_sql_with_retry, read_sql_with_retry
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError
import gc
from dotenv import load_dotenv
import numpy as np
import traceback
from collections import defaultdict

# ============================================================================
# CONFIGURATION PARAMETERS
# ============================================================================
MAX_WORKERS = 15                    # Number of concurrent threads for API calls
BATCH_SIZE = 5                      # Records per database insert batch
FULL_HISTORY_RECORDS = 7200         # Records to pull for NEW tickers
INCREMENTAL_RECORDS = 30            # Records to pull for EXISTING tickers
API_TIMEOUT = 2                    # Timeout for API requests in seconds
MAX_API_RETRIES = 3                 # Maximum retry attempts for failed API calls
SCHEMA_NAME = "MV_PRICES_2"         # Database schema name
INSTRUMENTS_TABLE = "InstrumentList"  # Source table for instruments
PRICES_TABLE = "MV_All_Prices"      # Target table for prices
LOG_LEVEL = logging.INFO            # Logging level

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'price_pipeline_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
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
        self._lock = Lock()

        user_pass = f"{username}:{password}"
        self.encoded_credentials = b64encode(user_pass.encode('ascii')).decode('ascii')
        
    def make_request(self, url: str, method: str = 'GET', data=None, 
                    content_type: Optional[str] = None, output: bool = True, 
                    timeout: int = API_TIMEOUT) -> str:
        """Make thread-safe API request with comprehensive error handling."""
        try:
            output_string = f"&output={self._response_format}" if output is True else ""
            # url already contains the query parameters, just add output at the end
            full_url = self._url_base + url + output_string
            
            headers = {
                'User-Agent': self._version,
                'Authorization': f"Basic {self.encoded_credentials}"
            }
            
            if content_type:
                headers['Content-Type'] = content_type
            
            logger.debug(f"Full URL: {full_url}")
                
            request = urllib.request.Request(full_url, method=method, data=data, headers=headers)
            
            with self._lock:
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
# PRICE DATA EXTRACTOR
# ============================================================================
class PriceDataExtractor:
    """Extracts price data from API with retry logic."""
    
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
                # Add env to params
                query_params = {**params, "env": self.environment}
                
                # URL encode the parameters properly
                query_string = urllib.parse.urlencode(query_params, safe='",')
                url = f"{endpoint}?{query_string}"
                
                self._request_count += 1
                # output=True tells make_request to add &output=csv
                response_text = self.connection.make_request(url, output=True, timeout=timeout)
                
                if response_text and response_text.strip():
                    # Check if response looks like an error (JSON or contains "error")
                    if response_text.strip().startswith('{') or 'error' in response_text.lower()[:100]:
                        logger.error(f"API returned error response: {response_text[:500]}")
                        return None
                    
                    try:
                        df = pd.read_csv(io.StringIO(response_text))
                        if not df.empty:
                            return df
                        else:
                            logger.warning(f"Empty DataFrame for {endpoint}")
                            return None
                    except pd.errors.ParserError as e:
                        logger.error(f"CSV parsing error for {endpoint}: {e}")
                        logger.error(f"Response text (first 500 chars): {response_text[:500]}")
                        return None
                else:
                    logger.warning(f"Empty response for {endpoint}")
                    return None
                    
            except GvException as e:
                self._failed_requests[endpoint] += 1
                logger.warning(f"API error fetching {endpoint} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    sleep_time = (2 ** attempt) + (time.time() % 1)
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

    def get_price_history(self, symbol: str, records_back: int = 100) -> Optional[pd.DataFrame]:
        """Get price history for a specific symbol using GetDaily endpoint."""
        try:
            # Small delay to avoid overwhelming the API
            time.sleep(0.3)
            
            # Format symbol with quotes as the API expects
            symbols_str = f'"{symbol}"'
            
            # Common fields for price data
            fields_all = [
                "pricesymbol", "symbol", "symboldescription", "tradedatetimeutc", "open", "high", "low", 
                "close", "last", "midpoint", "volume", "tradevolume", "historicvolume", "tickcount", 
                "netchange", "percentchange", "openinterest", "closedate", "currency", "mostrecentvalue", 
                "mostrecentvaluedate", "lasttradedirection", "prevlast", "lastopen", "lasthigh", "lastlow", 
                "lastclose", "lastvolume", "putcallunderlier", "bid", "ask", "bidsize", "asksize", 
                "biddatetimeutc", "askdatetimeutc", "optionroot", "settledate", "displaycontractexpdate", 
                "market", "expirationdate", "lotunit", "strike", "tradestarttimeutc", "tradestoptimeutc", 
                "sessionstarttimeutc", "sessionstoptimeutc", "blocktradedatetimeutc", "settleupdatetime", 
                "prevsettleupdatetime", "exchangecode"
            ]
            fields_str = ",".join(fields_all)
            
            params = {
                "symbols": symbols_str,
                "fields": fields_str,
                "recordsback": records_back
            }
            
            df = self._fetch_csv("GetDaily", params, timeout=60)
            if df is not None and not df.empty:
                # Add symbol column if not present
                if 'symbol' not in df.columns:
                    df['symbol'] = symbol
                logger.debug(f"Retrieved {len(df)} price records for {symbol}")
                return df
            else:
                logger.debug(f"No price data for {symbol}")
                return None
        except Exception as e:
            logger.error(f"Error getting price history for {symbol}: {e}")
            return None

    def get_stats(self) -> dict:
        """Return statistics about API usage."""
        return {
            'total_requests': self._request_count,
            'failed_endpoints': dict(self._failed_requests)
        }

# ============================================================================
# PRICE DATABASE MANAGER
# ============================================================================
class PriceDatabaseManager:
    """Manages price database operations with deduplication."""
    
    def __init__(self, schema_name: str, prices_table: str, instruments_table: str):
        self.schema_name = schema_name
        self.prices_table = prices_table
        self.instruments_table = instruments_table
        self.engine = engine
        self._insert_lock = Lock()
        self._existing_price_hashes: Set[str] = set()
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
            return False

    def create_prices_table(self) -> bool:
        """Create prices table if it doesn't exist."""
        try:
            with self.engine.begin() as connection:
                # Check if table exists
                inspector = inspect(connection)
                if self.prices_table in inspector.get_table_names(schema=self.schema_name):
                    logger.info(f"Table {self.schema_name}.{self.prices_table} already exists")
                    return True
                
                # Create table matching GetDaily output structure
                # Note: open, high, low, close are SQL reserved keywords - wrap in brackets
                connection.execute(text(f"""
                    CREATE TABLE {self.schema_name}.{self.prices_table} (
                        pricesymbol NVARCHAR(255),
                        symbol NVARCHAR(255) NOT NULL,
                        symboldescription NVARCHAR(500),
                        tradedatetimeutc DATETIME2 NOT NULL,
                        [open] FLOAT,
                        [high] FLOAT,
                        [low] FLOAT,
                        [close] FLOAT,
                        [last] FLOAT,
                        midpoint FLOAT,
                        volume FLOAT,
                        tradevolume FLOAT,
                        historicvolume FLOAT,
                        tickcount BIGINT,
                        netchange FLOAT,
                        percentchange FLOAT,
                        openinterest FLOAT,
                        closedate DATETIME2,
                        currency NVARCHAR(10),
                        mostrecentvalue FLOAT,
                        mostrecentvaluedate DATETIME2,
                        lasttradedirection NVARCHAR(10),
                        prevlast FLOAT,
                        lastopen FLOAT,
                        lasthigh FLOAT,
                        lastlow FLOAT,
                        lastclose FLOAT,
                        lastvolume FLOAT,
                        putcallunderlier NVARCHAR(50),
                        bid FLOAT,
                        ask FLOAT,
                        bidsize FLOAT,
                        asksize FLOAT,
                        biddatetimeutc DATETIME2,
                        askdatetimeutc DATETIME2,
                        optionroot NVARCHAR(100),
                        settledate DATETIME2,
                        displaycontractexpdate NVARCHAR(50),
                        market NVARCHAR(100),
                        expirationdate DATETIME2,
                        lotunit NVARCHAR(50),
                        strike FLOAT,
                        tradestarttimeutc DATETIME2,
                        tradestoptimeutc DATETIME2,
                        sessionstarttimeutc DATETIME2,
                        sessionstoptimeutc DATETIME2,
                        blocktradedatetimeutc DATETIME2,
                        settleupdatetime DATETIME2,
                        prevsettleupdatetime DATETIME2,
                        exchangecode NVARCHAR(50),
                        price_hash NVARCHAR(64) NOT NULL UNIQUE,
                        created_at DATETIME2 NOT NULL,
                        updated_at DATETIME2 NOT NULL,
                        data_source NVARCHAR(50) DEFAULT 'MV_API',
                        PRIMARY KEY (price_hash)
                    )
                """))
                
                # Create indexes
                connection.execute(text(f"""
                    CREATE INDEX ix_{self.prices_table}_symbol 
                    ON {self.schema_name}.{self.prices_table} (symbol)
                """))
                
                connection.execute(text(f"""
                    CREATE INDEX ix_{self.prices_table}_trade_date 
                    ON {self.schema_name}.{self.prices_table} (tradedatetimeutc)
                """))
                
                connection.execute(text(f"""
                    CREATE INDEX ix_{self.prices_table}_symbol_date 
                    ON {self.schema_name}.{self.prices_table} (symbol, tradedatetimeutc)
                """))
                
                logger.info(f"✓ Created table {self.schema_name}.{self.prices_table}")
            
            self._cache_valid = False
            return True
                
        except Exception as e:
            logger.error(f"✗ Error creating prices table: {e}")
            logger.error(traceback.format_exc())
            return False

    def get_instruments_from_db(self) -> List[str]:
        """Get list of symbols from InstrumentList table."""
        try:
            query = text(f"""
                SELECT DISTINCT symbol
                FROM [{self.schema_name}].[{self.instruments_table}]
                WHERE symbol IS NOT NULL
                AND symbol != ''
                ORDER BY symbol
            """)
            
            df = read_sql_with_retry(query)
            
            if df is not None and not df.empty:
                symbols = df['symbol'].dropna().unique().tolist()
                logger.info(f"Retrieved {len(symbols)} instruments from {self.instruments_table}")
                return symbols
            else:
                logger.warning(f"No instruments found in {self.instruments_table}")
                return []
                
        except Exception as e:
            logger.error(f"Error retrieving instruments: {e}")
            logger.error(traceback.format_exc())
            return []

    def get_existing_symbols_in_prices(self) -> Set[str]:
        """Get set of symbols that already have price data."""
        try:
            query = text(f"""
                SELECT DISTINCT symbol
                FROM [{self.schema_name}].[{self.prices_table}]
                WHERE symbol IS NOT NULL
            """)
            
            df = read_sql_with_retry(query)
            
            if df is not None and not df.empty:
                existing = set(df['symbol'].dropna().unique().tolist())
                logger.info(f"Found {len(existing)} symbols with existing price data")
                return existing
            else:
                logger.info("No existing price data found")
                return set()
                
        except Exception as e:
            logger.warning(f"Error getting existing symbols: {e}")
            return set()

    def get_existing_price_hashes(self, force_refresh: bool = False) -> Set[str]:
        """Get set of existing price hashes from database with caching."""
        if self._cache_valid and not force_refresh and self._existing_price_hashes:
            return self._existing_price_hashes
            
        try:
            query = text(f"""
                SELECT price_hash
                FROM [{self.schema_name}].[{self.prices_table}]
                WHERE price_hash IS NOT NULL
            """)
            
            df = read_sql_with_retry(query)
            
            if df is not None and not df.empty:
                self._existing_price_hashes = set(df['price_hash'].tolist())
                self._cache_valid = True
                logger.info(f"Loaded {len(self._existing_price_hashes)} existing price hashes into cache")
            else:
                self._existing_price_hashes = set()
                self._cache_valid = True
                
            return self._existing_price_hashes
            
        except Exception as e:
            logger.warning(f"Error fetching existing price hashes: {e}")
            return set()

    def generate_price_hash(self, row: pd.Series) -> str:
        """Generate unique hash for price record based on symbol and trade datetime."""
        # Use symbol and tradedatetimeutc as primary uniqueness
        hash_parts = []
        
        # Core identifying fields from GetDaily
        core_fields = ['symbol', 'tradedatetimeutc', 'pricesymbol']
        
        for col in core_fields:
            if col in row.index and pd.notna(row[col]):
                value = row[col]
                if isinstance(value, (datetime.datetime, pd.Timestamp)):
                    hash_parts.append(f"{col}:{value.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    hash_parts.append(f"{col}:{str(value).strip().lower()}")
        
        if hash_parts:
            hash_str = '|'.join(sorted(hash_parts))
            return hashlib.sha256(hash_str.encode()).hexdigest()
        else:
            # Fallback to all non-metadata columns
            all_parts = []
            for col in row.index:
                if pd.notna(row[col]) and col not in ['price_hash', 'created_at', 'updated_at', 'data_source']:
                    all_parts.append(f"{col}:{str(row[col])}")
            hash_str = '|'.join(sorted(all_parts))
            return hashlib.sha256(hash_str.encode()).hexdigest()

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize DataFrame data."""
        if df.empty:
            return df
            
        cleaned_df = df.copy()
        
        # Convert ALL datetime columns - the API returns many datetime fields
        date_cols = ['tradedatetimeutc', 'closedate', 'settledate', 'mostrecentvaluedate', 
                     'biddatetimeutc', 'askdatetimeutc', 'expirationdate', 'displaycontractexpdate',
                     'tradestarttimeutc', 'tradestoptimeutc', 'sessionstarttimeutc', 'sessionstoptimeutc',
                     'blocktradedatetimeutc', 'settleupdatetime', 'prevsettleupdatetime']
        
        for col in date_cols:
            if col in cleaned_df.columns:
                # Convert to datetime, replacing invalid/empty values with None
                cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce')
                # Replace NaT with None for SQL compatibility
                cleaned_df[col] = cleaned_df[col].where(cleaned_df[col].notna(), None)
        
        # Convert numeric columns
        numeric_cols = ['open', 'high', 'low', 'close', 'last', 'midpoint', 'volume', 
                       'tradevolume', 'historicvolume', 'tickcount', 'netchange', 'percentchange',
                       'openinterest', 'bid', 'ask', 'bidsize', 'asksize', 'strike',
                       'prevlast', 'lastopen', 'lasthigh', 'lastlow', 'lastclose', 'lastvolume',
                       'mostrecentvalue']
        
        for col in numeric_cols:
            if col in cleaned_df.columns:
                cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce')
                # Replace NaN with None for SQL compatibility
                cleaned_df[col] = cleaned_df[col].where(cleaned_df[col].notna(), None)
        
        # Ensure string columns are properly typed
        string_cols = ['pricesymbol', 'symbol', 'symboldescription', 'currency', 'lasttradedirection',
                      'putcallunderlier', 'optionroot', 'displaycontractexpdate', 'market', 'lotunit',
                      'exchangecode']
        
        for col in string_cols:
            if col in cleaned_df.columns:
                # Convert to string and replace 'nan' strings with None
                cleaned_df[col] = cleaned_df[col].astype(str)
                cleaned_df[col] = cleaned_df[col].replace(['nan', 'None', 'NaN', ''], None)
        
        return cleaned_df

    def prepare_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare price data for insertion."""
        if df.empty:
            return df
        
        # Clean the data
        prepared_df = self.clean_dataframe(df)
        
        # Generate price hashes
        prepared_df['price_hash'] = prepared_df.apply(self.generate_price_hash, axis=1)
        
        # Remove duplicates within batch
        initial_count = len(prepared_df)
        prepared_df = prepared_df.drop_duplicates(subset=['price_hash'], keep='first')
        final_count = len(prepared_df)
        
        if initial_count != final_count:
            logger.debug(f"Removed {initial_count - final_count} duplicate hashes from batch")
        
        # Add metadata
        current_time = datetime.datetime.now()
        prepared_df['created_at'] = current_time
        prepared_df['updated_at'] = current_time
        prepared_df['data_source'] = 'MV_API'
        
        return prepared_df

    def insert_price_records(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Insert price records with deduplication."""
        if df.empty:
            return 0, 0

        with self._insert_lock:
            try:
                # Filter out existing records
                existing_hashes = self.get_existing_price_hashes()
                new_records = df[~df['price_hash'].isin(existing_hashes)]
                
                if new_records.empty:
                    logger.debug("No new price records to insert (all duplicates)")
                    return 0, 0

                logger.info(f"Inserting {len(new_records)} new price records (filtered {len(df) - len(new_records)} duplicates)")

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
                            self.prices_table, 
                            schema=self.schema_name, 
                            if_exists='append', 
                            index=False,
                            chunksize=BATCH_SIZE
                        )
                        total_inserted += len(batch)
                        
                        # Update hash cache
                        self._existing_price_hashes.update(batch['price_hash'].tolist())
                        
                    except Exception as e:
                        logger.error(f"Failed batch insert at position {i}: {e}")
                        total_failed += len(batch)
                
                return total_inserted, total_failed
                
            except Exception as e:
                logger.error(f"Error during price insertion: {e}")
                logger.error(traceback.format_exc())
                return 0, len(df)

    def get_record_count(self) -> int:
        """Get total record count in prices table."""
        try:
            query = text(f"SELECT COUNT(*) FROM {self.schema_name}.{self.prices_table}")
            with self.engine.connect() as connection:
                result = connection.execute(query)
                count = result.scalar()
                return count
        except Exception as e:
            logger.error(f"Error getting record count: {e}")
            return 0

# ============================================================================
# PRICE PIPELINE - SINGLE CYCLE
# ============================================================================
class PricePipeline:
    """Pipeline that processes price data in ONE cycle with batched pull/push."""
    
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.extractor = PriceDataExtractor(username, password)
        self.db_manager = PriceDatabaseManager(SCHEMA_NAME, PRICES_TABLE, INSTRUMENTS_TABLE)
        self.total_inserted = 0
        self.total_skipped = 0
        self.total_failed = 0
        self.start_time = datetime.datetime.now()
        
    def initialize(self) -> bool:
        """Initialize database schema and table."""
        try:
            logger.info("=" * 80)
            logger.info("INITIALIZING PRICE PIPELINE")
            logger.info("=" * 80)
            
            if not self.db_manager.ensure_schema_exists():
                return False
            
            if not self.db_manager.create_prices_table():
                return False
            
            # Load existing price hashes into cache
            self.db_manager.get_existing_price_hashes(force_refresh=True)
            
            logger.info("✓ Initialization complete")
            return True
            
        except Exception as e:
            logger.error(f"✗ Initialization failed: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def run_single_cycle(self) -> None:
        """
        Run ONE cycle:
        - Get all instruments from InstrumentList
        - Identify new vs existing symbols
        - Pull full history (7200 records) for NEW symbols
        - Pull incremental (30 records) for EXISTING symbols
        - Insert with deduplication
        """
        cycle_start = time.time()
        
        logger.info("=" * 80)
        logger.info("STARTING PRICE DATA CYCLE")
        logger.info("=" * 80)
        
        try:
            # Get all instruments
            all_symbols = self.db_manager.get_instruments_from_db()
            if not all_symbols:
                logger.error("No instruments found - run ticker pipeline first")
                return
            
            logger.info(f"Found {len(all_symbols)} total instruments")
            
            # Identify new vs existing symbols
            existing_symbols = self.db_manager.get_existing_symbols_in_prices()
            new_symbols = [s for s in all_symbols if s not in existing_symbols]
            update_symbols = [s for s in all_symbols if s in existing_symbols]
            
            logger.info(f"New symbols (full history): {len(new_symbols)}")
            logger.info(f"Existing symbols (incremental): {len(update_symbols)}")
            
            # Process symbols with concurrent pull/push
            total_symbols = len(all_symbols)
            processed = 0
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all tasks
                futures = {}
                
                # New symbols: full history
                for symbol in new_symbols:
                    future = executor.submit(self.extractor.get_price_history, symbol, FULL_HISTORY_RECORDS)
                    futures[future] = ('new', symbol)
                
                # Existing symbols: incremental
                for symbol in update_symbols:
                    future = executor.submit(self.extractor.get_price_history, symbol, INCREMENTAL_RECORDS)
                    futures[future] = ('update', symbol)
                
                # Process as they complete
                with tqdm(total=total_symbols, desc=" Extracting & Inserting Prices", unit="symbol") as pbar:
                    for future in as_completed(futures):
                        symbol_type, symbol = futures[future]
                        try:
                            # Get price data
                            df = future.result(timeout=60)
                            
                            if df is not None and not df.empty:
                                # Prepare and insert immediately
                                prepared_df = self.db_manager.prepare_price_data(df)
                                inserted, failed = self.db_manager.insert_price_records(prepared_df)
                                
                                self.total_inserted += inserted
                                self.total_failed += failed
                                
                                if inserted == 0 and failed == 0:
                                    self.total_skipped += len(df)
                                
                                pbar.set_postfix({
                                    "inserted": self.total_inserted,
                                    "skipped": self.total_skipped,
                                    "type": symbol_type,
                                    "symbol": symbol[:20]
                                })
                                
                                # Clean up
                                del df, prepared_df
                                
                            else:
                                logger.debug(f"No data for {symbol}")
                                
                        except Exception as e:
                            logger.error(f"Error processing {symbol}: {e}")
                            self.total_failed += 1
                        finally:
                            processed += 1
                            pbar.update(1)
                            
                            # Periodic memory cleanup
                            if processed % 100 == 0:
                                gc.collect()
            
            # Final cleanup
            gc.collect()
            
            # Summary
            cycle_duration = time.time() - cycle_start
            logger.info("=" * 80)
            logger.info("CYCLE COMPLETE")
            logger.info(f"  Duration: {cycle_duration:.2f} seconds")
            logger.info(f"  Symbols Processed: {processed}")
            logger.info(f"  New Price Records Inserted: {self.total_inserted}")
            logger.info(f"  Duplicate Records Skipped: {self.total_skipped}")
            logger.info(f"  Failed: {self.total_failed}")
            logger.info(f"  Total DB Price Records: {self.db_manager.get_record_count()}")
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
        logger.info(f"Price Records Inserted: {self.total_inserted}")
        logger.info(f"Records Skipped (duplicates): {self.total_skipped}")
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
def main():
    """Main entry point for the price pipeline - runs ONE cycle."""
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
        pipeline = PricePipeline(username, password)
        
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
    main()