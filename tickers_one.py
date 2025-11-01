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
from typing import List, Dict, Optional, Tuple
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from connect_db import engine, write_sql_with_retry
from sqlalchemy import text, inspect, NVARCHAR, DATETIME, Float, BigInteger, DATE, INTEGER
from sqlalchemy.exc import SQLAlchemyError
import gc
from dotenv import load_dotenv
import numpy as np

# Setup logging
logger = logging.getLogger(__name__)

class DefaultConfig:
    VERSION = "MarketView PythonSDK/1.0"
    WEBSERVICE_URL = "https://mv-api-proxy.prod.tr.enverus.com/"
    API_SUFFIX = "pythonapi/v1/"
    RESPONSE_FORMAT = "csv"

class GvException(Exception):
    def __init__(self, message, inner_exception=None):
        super().__init__(message)
        self.inner_exception = inner_exception

class MvWSConnection:
    def __init__(self, username, password, config=None):
        self._config = config if config is not None else DefaultConfig
        self._version = self._config.VERSION
        self._webservice_url = self._config.WEBSERVICE_URL
        self._api_suffix = self._config.API_SUFFIX
        self._url_base = self._webservice_url + self._api_suffix
        self._response_format = self._config.RESPONSE_FORMAT

        user_pass = f"{username}:{password}"
        self.encoded_credentials = b64encode(user_pass.encode('ascii')).decode('ascii')
        
    def make_request(self, url, method='GET', data=None, content_type=None, output=True, timeout=3):
        try:
            output_string = f"&output={self._response_format}" if output is True else ""
            full_url = self._url_base + url + output_string
            
            logger.info(f"Making request to: {full_url}")
            
            headers = {
                'User-Agent': self._version,
                'Authorization': f"Basic {self.encoded_credentials}"
            }
            
            if content_type:
                headers['Content-Type'] = content_type
                
            request = urllib.request.Request(full_url, method=method, data=data, headers=headers)
            response = urllib.request.urlopen(request, timeout=timeout)

            response_status_code = response.getcode()
            response_text = response.read().decode('utf-8')
            
            logger.info(f"Response code: {response_status_code}")

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
            if isinstance(e.reason, TimeoutError):
                logger.error(f"Request timeout for {url}: {e}")
                raise GvException(f"Request timeout: {e}")
            else:
                logger.error(f"URL Error for {url}: {e}")
                raise GvException(f"URL Error: {e}")
        except Exception as e:
            logger.error(f"Request failed for {url}: {e}")
            raise GvException(str(e))

class MarketDataExtractor:
    def __init__(self, username: str, password: str, environment: str = "onboard"):
        """Initialize the market data extractor with proper MV authentication."""
        self.username = username
        self.password = password
        self.environment = environment
        self.connection = MvWSConnection(username, password)

    def _fetch_csv(self, endpoint: str, params: dict, timeout: int = 30) -> Optional[pd.DataFrame]:
        """Helper to call an API endpoint that returns CSV output."""
        for attempt in range(2):
            try:
                # Build query string
                query_params = {**params, "env": self.environment}
                query_string = "&".join([f"{k}={v}" for k, v in query_params.items()])
                url = f"{endpoint}?{query_string}"
                
                logger.info(f"Fetching: {endpoint} with params: { {k: v for k, v in params.items()} }")
                
                response_text = self.connection.make_request(url, timeout=timeout)
                
                if response_text and response_text.strip():
                    df = pd.read_csv(io.StringIO(response_text))
                    if not df.empty:
                        logger.info(f"Successfully parsed CSV with {len(df)} rows and {len(df.columns)} columns")
                        return df
                    else:
                        logger.warning(f"Empty DataFrame returned for {endpoint}")
                        return None
                else:
                    logger.warning("Empty response received")
                    return None
                    
            except GvException as e:
                logger.warning(f"API error fetching {endpoint} (attempt {attempt + 1}): {e}")
                if attempt == 1:
                    return None
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Error fetching {endpoint} (attempt {attempt + 1}): {e}")
                if attempt == 1:
                    return None
                time.sleep(2)
        return None

    def get_exchange_list(self) -> Optional[pd.DataFrame]:
        """Get list of all available exchanges."""
        params = {}
        df = self._fetch_csv("getExchangeList", params)
        if df is not None and not df.empty:
            logger.info(f"Retrieved {len(df)} exchanges.")
            return df
        else:
            logger.error("Failed to retrieve exchange list or returned empty.")
            return None

    def get_exchange_codes(self) -> List[str]:
        """Get list of exchange codes for concurrent processing."""
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
            return []

    def get_symbols_for_exchange(self, exchange_code: str, records_back: int = 10000, max_retries: int = 2) -> Optional[pd.DataFrame]:
        """Pull all symbols using symbolSearchByExchange."""
        for attempt in range(max_retries):
            try:
                params = {
                    "exchangecode": exchange_code,
                    "recordsback": records_back
                }
                df = self._fetch_csv("symbolSearchByExchange", params, timeout=3)
                if df is not None and not df.empty:
                    logger.info(f"Retrieved {len(df)} symbols for exchange {exchange_code}.")
                    return df
                else:
                    logger.warning(f"No data returned for exchange {exchange_code}, attempt {attempt + 1}")
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {exchange_code}: {e}")
            time.sleep(2 ** attempt)
        
        logger.error(f"Failed to get symbols for exchange {exchange_code} after {max_retries} attempts.")
        return None

    def extract_all_exchange_instruments(self, max_workers: int = 3, max_exchanges: int = None) -> Dict[str, pd.DataFrame]:
        """Extract instruments from all exchanges concurrently."""
        exchange_codes = self.get_exchange_codes()
        if not exchange_codes:
            logger.error("No exchange codes found. This might be due to authentication issues.")
            return {}

        if max_exchanges:
            exchange_codes = exchange_codes[:max_exchanges]
            logger.info(f"Limited to {max_exchanges} exchanges for testing")

        logger.info(f"Processing {len(exchange_codes)} exchange codes")

        results = {}
        successful_exchanges = 0
        failed_exchanges = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.get_symbols_for_exchange, code): code
                for code in exchange_codes
            }
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Extracting exchange instruments"):
                code = futures[future]
                try:
                    df = future.result(timeout=3)
                    if df is not None and not df.empty:
                        results[code] = df
                        successful_exchanges += 1
                        logger.info(f"✓ Successfully processed {code}: {len(df)} symbols")
                    else:
                        logger.warning(f"✗ No data for exchange {code}")
                        failed_exchanges += 1
                except Exception as e:
                    logger.error(f"✗ Error processing {code}: {e}")
                    failed_exchanges += 1
        
        logger.info(f"Successfully extracted data from {successful_exchanges} out of {len(exchange_codes)} exchanges. Failed: {failed_exchanges}")
        return results

class DatabaseManager:
    """Handles database operations with proper schema management."""
    
    def __init__(self, schema_name: str, table_name: str):
        self.schema_name = schema_name
        self.table_name = table_name
        self.engine = engine
        
    def ensure_schema_exists(self):
        """Ensure the schema exists in the database."""
        try:
            with self.engine.connect() as connection:
                inspector = inspect(connection)
                if self.schema_name not in inspector.get_schema_names():
                    connection.execute(text(f"CREATE SCHEMA {self.schema_name}"))
                    connection.commit()
                    logger.info(f"Schema '{self.schema_name}' created successfully.")
                else:
                    logger.info(f"Schema '{self.schema_name}' already exists.")
        except Exception as e:
            logger.error(f"Error ensuring schema exists: {e}")
            raise

    def drop_and_recreate_table(self):
        """Drop and recreate the table with correct schema and proper data types."""
        try:
            with self.engine.begin() as connection:
                # Drop table if exists
                connection.execute(text(f"DROP TABLE IF EXISTS {self.schema_name}.{self.table_name}"))
                logger.info(f"Dropped table {self.schema_name}.{self.table_name}")
                
                # Create table with ALL columns as NVARCHAR to avoid type conflicts
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
                        instrument_hash NVARCHAR(64) UNIQUE,
                        created_at NVARCHAR(50),
                        updated_at NVARCHAR(50),
                        last_seen_date NVARCHAR(50),
                        last_price_pull_date NVARCHAR(50)
                    )
                """))
                
                # Create index
                connection.execute(text(f"""
                    CREATE INDEX ix_{self.table_name}_symbol 
                    ON {self.schema_name}.{self.table_name} (symbol)
                """))
                
                logger.info(f"Created table {self.schema_name}.{self.table_name} with all NVARCHAR columns")
                
        except Exception as e:
            logger.error(f"Error recreating table: {e}")
            raise

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize DataFrame data before insertion."""
        if df.empty:
            return df
            
        cleaned_df = df.copy()
        
        # Convert ALL columns to string to avoid type conflicts
        for column in cleaned_df.columns:
            cleaned_df[column] = cleaned_df[column].astype(str)
            # Replace 'nan' strings with None
            cleaned_df[column] = cleaned_df[column].replace('nan', None)
            cleaned_df[column] = cleaned_df[column].replace('None', None)
            cleaned_df[column] = cleaned_df[column].replace('<NA>', None)
        
        return cleaned_df

    def generate_instrument_hash(self, row: pd.Series) -> str:
        """Generate a unique hash for an instrument based on its core identifying fields."""
        # Use available columns for hashing
        hash_columns = ['symbol', 'exchange_code', 'security_type', 'option_root', 'source_symbol']
        
        hash_parts = []
        for col in hash_columns:
            if col in row.index and pd.notna(row[col]):
                value = row[col]
                if pd.notna(value):
                    str_value = str(value).strip().lower()
                    hash_parts.append(f"{col}:{str_value}")
        
        if not hash_parts:
            # Fallback: use all non-null columns
            for col in row.index:
                if pd.notna(row[col]) and col not in ['instrument_hash', 'created_at', 'updated_at', 'last_seen_date', 'last_price_pull_date']:
                    str_value = str(row[col]).strip().lower()
                    hash_parts.append(f"{col}:{str_value}")
        
        if hash_parts:
            hash_str = '|'.join(sorted(hash_parts))
            return hashlib.sha256(hash_str.encode()).hexdigest()
        else:
            # Final fallback: hash the index
            return hashlib.sha256(str(row.name).encode()).hexdigest()

    def prepare_data_for_insert(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame for database insertion with proper data cleaning."""
        if df.empty:
            return df
        
        # Clean the data first
        prepared_df = self.clean_dataframe(df)
        
        # Generate unique hashes
        prepared_df['instrument_hash'] = prepared_df.apply(self.generate_instrument_hash, axis=1)
        
        # Remove duplicates based on hash
        initial_count = len(prepared_df)
        prepared_df = prepared_df.drop_duplicates(subset=['instrument_hash'], keep='first')
        final_count = len(prepared_df)
        
        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} duplicates from DataFrame.")
        
        # Add metadata columns
        current_time = datetime.datetime.now()
        current_date = datetime.date.today()
        
        prepared_df['created_at'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        prepared_df['updated_at'] = current_time.strftime('%Y-%m-%d %H:%M:%S')
        prepared_df['last_seen_date'] = current_date.strftime('%Y-%m-%d')
        prepared_df['last_price_pull_date'] = None
        
        # Ensure all columns are present and in correct order
        prepared_df = self.ensure_column_structure(prepared_df)
        
        logger.info(f"Prepared DataFrame with {len(prepared_df)} records for insertion")
        return prepared_df

    def ensure_column_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure DataFrame has all required columns in correct order."""
        # Define the expected column order based on table schema
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
        
        # Add missing columns with None values
        for column in expected_columns:
            if column not in df.columns:
                df[column] = None
        
        # Reorder columns to match expected order
        df = df[expected_columns]
        
        return df

    def get_existing_hashes(self) -> set:
        """Get set of existing hashes from the database."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(
                    text(f"SELECT instrument_hash FROM {self.schema_name}.{self.table_name} WHERE instrument_hash IS NOT NULL")
                )
                existing_hashes = {row[0] for row in result}
                logger.info(f"Found {len(existing_hashes)} existing records in database.")
                return existing_hashes
        except Exception as e:
            logger.warning(f"Error fetching existing hashes: {e}")
            return set()

    def insert_records(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Insert records into the database with proper error handling."""
        if df.empty:
            logger.info("No records to insert.")
            return 0, 0

        try:
            # Filter out existing records
            existing_hashes = self.get_existing_hashes()
            if existing_hashes:
                new_records = df[~df['instrument_hash'].isin(existing_hashes)]
                logger.info(f"Filtered to {len(new_records)} new records out of {len(df)} total.")
            else:
                new_records = df
                logger.info(f"No existing records found, inserting all {len(df)} records.")

            if new_records.empty:
                logger.info("No new records to insert.")
                return 0, 0

            # Replace NaN with None for proper NULL handling
            new_records = new_records.replace({np.nan: None})
            
            # Insert in smaller batches to identify problematic records
            batch_size = 10
            total_inserted = 0
            total_failed = 0
            
            for i in range(0, len(new_records), batch_size):
                batch = new_records.iloc[i:i + batch_size]
                try:
                    write_sql_with_retry(
                        batch, 
                        self.table_name, 
                        schema=self.schema_name, 
                        if_exists='append', 
                        index=False, 
                        chunksize=5
                    )
                    total_inserted += len(batch)
                    logger.info(f"Successfully inserted batch {i//batch_size + 1}: {len(batch)} records")
                    
                except Exception as e:
                    logger.error(f"Failed to insert batch {i//batch_size + 1}: {e}")
                    total_failed += len(batch)
                    
                    # Try inserting records one by one to identify the problematic one
                    for j, (idx, record) in enumerate(batch.iterrows()):
                        try:
                            single_record_df = pd.DataFrame([record])
                            write_sql_with_retry(
                                single_record_df,
                                self.table_name,
                                schema=self.schema_name,
                                if_exists='append',
                                index=False
                            )
                            total_inserted += 1
                            total_failed -= 1
                        except Exception as single_error:
                            logger.error(f"Failed to insert record {idx}: {single_error}")
                            logger.error(f"Problematic record: {record.to_dict()}")
            
            logger.info(f"Insertion complete: {total_inserted} inserted, {total_failed} failed")
            return total_inserted, total_failed
            
        except Exception as e:
            logger.error(f"Error during insertion process: {e}")
            return 0, len(df)

def run_tickers():
    """Main function to extract and store instrument data."""
    load_dotenv()
    username = os.getenv("GvWSUSERNAME")
    password = os.getenv("GvWSPASSWORD")

    if not username or not password:
        logger.error("Username or password not found in environment variables.")
        logger.error("Please check that GvWSUSERNAME and GvWSPASSWORD are set in your .env file")
        return

    logger.info("Initializing MarketDataExtractor...")
    extractor = MarketDataExtractor(username, password, environment="onboard")

    schema_name = "MV_PRICES_2"
    table_name = "InstrumentList"
    
    db_manager = DatabaseManager(schema_name, table_name)

    try:
        db_manager.ensure_schema_exists()

        # Recreate table to ensure schema matches API response
        logger.info("Ensuring table schema matches API response...")
        db_manager.drop_and_recreate_table()

        logger.info("Starting exchange instruments extraction...")
        # Start with just 2-3 exchanges to test
       #exchange_instruments = extractor.extract_all_exchange_instruments(max_workers=3)  #, max_exchanges=3)
        exchange_instruments = extractor.extract_all_exchange_instruments(max_workers=1,max_exchanges=3)

        if exchange_instruments:
            logger.info("Combining all exchange instruments into a single DataFrame...")
            combined_df = pd.concat(exchange_instruments.values(), ignore_index=True)
            logger.info(f"Combined DataFrame shape: {combined_df.shape}")
            logger.info(f"DataFrame columns: {list(combined_df.columns)}")
            
            # Display sample data to understand structure
            logger.info("Sample data:")
            for col in combined_df.columns[:5]:  # First 5 columns
                sample_values = combined_df[col].dropna().head(3).tolist()
                logger.info(f"  {col}: {sample_values}")

            # Prepare data for database insertion
            prepared_df = db_manager.prepare_data_for_insert(combined_df)
            
            # Insert records
            inserted, failed = db_manager.insert_records(prepared_df)
            
            if inserted > 0:
                logger.info(f"Successfully inserted {inserted} records into database.")
            else:
                logger.warning("No records were inserted.")
                
            if failed > 0:
                logger.error(f"Failed to insert {failed} records.")

        else:
            logger.warning("No exchange instruments data retrieved.")

    except Exception as e:
        logger.error(f"An unexpected error occurred during execution: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    run_tickers()