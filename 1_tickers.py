import datetime
import os,io
import pandas as pd
import time
import logging
from typing import List, Dict, Optional,Tuple
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from connect_db import engine, read_sql_with_retry, write_sql_with_retry
from sqlalchemy import text, inspect, MetaData, Table, Column, String, DateTime, UniqueConstraint
from sqlalchemy.exc import SQLAlchemyError
import gc
from dotenv import load_dotenv
# Setup logging
logger = logging.getLogger(__name__)

class MarketDataExtractor:
    BASE_URL = "https://mv-api-proxy.prod.tr.enverus.com/pythonapi/v1"

    def __init__(self, username: str, password: str, environment: str = "onboard"):
        """Initialize the market data extractor with authentication setup."""
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

        # Some MV endpoints require Basic Auth
        session.auth = (self.username, self.password)
        logger.info("Authenticated session created successfully.")
        return session

    def _fetch_csv(self, endpoint: str, params: dict) -> Optional[pd.DataFrame]:
        """Helper to call an API endpoint that returns CSV output."""
        try:
            params["output"] = "csv"
            url = f"{self.BASE_URL}/{endpoint}"
            response = self.session.get(url, params=params, timeout=3)

            logger.info(f"GET {response.url}")
            logger.info(f"Response code: {response.status_code}")

            if response.status_code == 200:
                df = pd.read_csv(io.StringIO(response.text))
                return df
            else:
                logger.warning(f"Request failed ({response.status_code}): {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error fetching {endpoint}: {e}")
            return None


    def get_exchange_list(self) -> pd.DataFrame:
        """Get list of all available exchanges."""
        params = {"env": self.environment}
        df = self._fetch_csv("getExchangeList", params)
        if df is not None:
            logger.info(f"Retrieved {len(df)} exchanges.")
            return df
        else:
            raise RuntimeError("Failed to retrieve exchange list.")

    def get_exchange_codes(self) -> List[str]:
        """Get list of exchange codes for concurrent processing."""
        try:
            df = self.get_exchange_list()
            codes = df['exchangecode'].unique().tolist()
            logger.info(f"Found {len(codes)} unique exchange codes.")
            return codes
        except Exception as e:
            logger.error(f"Error getting exchange codes: {e}")
            return []

    def get_symbols_for_exchange(self, exchange_code: str, records_back: int = 100_000_000, max_retries: int = 3) -> Optional[pd.DataFrame]:
        """Pull all symbols using symbolSearchByExchange."""
        for attempt in range(max_retries):
            try:
                params = {
                    "exchangecode": exchange_code,
                    "recordsback": records_back,
                    "env": self.environment
                }
                df = self._fetch_csv("symbolSearchByExchange", params)
                if df is not None:
                    logger.info(f"Retrieved {len(df)} symbols for exchange {exchange_code}. 🌐")
                    return df
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {exchange_code}: {e}")
            time.sleep(2 ** attempt)
        logger.error(f"Failed to get symbols for exchange {exchange_code} after {max_retries} attempts.")
        return None

    def get_instruments_for_exchange(self, exchange_code: str, max_retries: int = 3) -> Optional[pd.DataFrame]:
        """Get all instruments for a specific exchange."""
        for attempt in range(max_retries):
            try:
                params = {"exchangecode": exchange_code, "env": self.environment}
                df = self._fetch_csv("getInstrumentList", params)
                if df is not None:
                    logger.info(f"Retrieved {len(df)} instruments for exchange {exchange_code}.")
                    return df
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for exchange {exchange_code}: {e}")
            time.sleep(2 ** attempt)
        logger.error(f"Failed to get instruments for exchange {exchange_code} after {max_retries} attempts.")
        return None

    def get_chain_data(self, symbol: str, security_type: str = "fo",
                       on_date: Optional[datetime.date] = None, max_retries: int = 3) -> Optional[pd.DataFrame]:
        """Get chain data for a specific symbol."""
        for attempt in range(max_retries):
            try:
                params = {
                    "optionroot": symbol,
                    "securitytype": security_type,
                    "env": self.environment
                }
                if on_date:
                    params["ondate"] = on_date
                df = self._fetch_csv("getChain", params)
                if df is not None:
                    logger.info(f"Retrieved chain data for {symbol}: {len(df)} records. 🔗")
                    return df
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for chain {symbol}: {e}")
            time.sleep(2 ** attempt)
        logger.error(f"Failed to get chain data for {symbol} after {max_retries} attempts.")
        return None

    def get_option_analytics(self, underlier: str, strike_count: int = 10,
                              option_model: str = "bs", max_retries: int = 3) -> Optional[pd.DataFrame]:
        """Get option analytics for a specific underlier."""
        for attempt in range(max_retries):
            try:
                params = {
                    "underlier": underlier,
                    "optionmodel": option_model,
                    "strikecount": strike_count,
                    "fields": "atmindex,callput,symbol,strike,mrv,impvol,close",
                    "env": self.environment
                }
                df = self._fetch_csv("getOptionAnalytics", params)
                if df is not None:
                    logger.info(f"Retrieved option analytics for {underlier}: {len(df)} records.")
                    return df
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for {underlier}: {e}")
            time.sleep(2 ** attempt)
        logger.error(f"Failed to get option analytics for {underlier} after {max_retries} attempts.")
        return None

    def extract_all_exchange_instruments(self, max_workers: int = 5) -> Dict[str, pd.DataFrame]:
        """Extract instruments from all exchanges concurrently."""
        exchange_codes = self.get_exchange_codes()
        if not exchange_codes:
            logger.error("No exchange codes found.")
            return {}

        results = {}
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
                    else:
                        logger.warning(f"No data returned for {code}.")
                except Exception as e:
                    logger.error(f"Error processing {code}: {e}")
        logger.info(f"Successfully extracted data from {len(results)} exchanges.")
        return results


class DatabaseManager:
    """Handles database operations with duplicate prevention."""
    
    def __init__(self, schema_name: str, table_name: str):
        self.schema_name = schema_name
        self.table_name = table_name
        self.engine = engine
        
    def create_table_if_not_exists(self, df_sample: pd.DataFrame):
        """Create table with proper constraints if it doesn't exist."""
        try:
            with self.engine.connect() as connection:
                inspector = inspect(connection)
                
                # Check if table exists
                if not inspector.has_table(self.table_name, schema=self.schema_name):
                    logger.info(f"Table {self.schema_name}.{self.table_name} does not exist. Creating...")
                    
                    # Create table using pandas to_sql to infer types
                    df_sample.head(0).to_sql(
                        self.table_name, 
                        self.engine, 
                        schema=self.schema_name, 
                        if_exists='fail', 
                        index=False
                    )
                    
                    # Add unique constraint and metadata columns
                    with self.engine.begin() as conn:
                        # Add columns individually if they don’t exist
                        conn.execute(text(f"""
                            IF COL_LENGTH('{self.schema_name}.{self.table_name}', 'instrument_hash') IS NULL
                            ALTER TABLE {self.schema_name}.{self.table_name}
                            ADD instrument_hash VARCHAR(64);
                        """))

                        conn.execute(text(f"""
                            IF COL_LENGTH('{self.schema_name}.{self.table_name}', 'created_at') IS NULL
                            ALTER TABLE {self.schema_name}.{self.table_name}
                            ADD created_at DATETIME DEFAULT GETDATE();
                        """))

                        conn.execute(text(f"""
                            IF COL_LENGTH('{self.schema_name}.{self.table_name}', 'updated_at') IS NULL
                            ALTER TABLE {self.schema_name}.{self.table_name}
                            ADD updated_at DATETIME DEFAULT GETDATE();
                        """))

                        conn.execute(text(f"""
                            IF COL_LENGTH('{self.schema_name}.{self.table_name}', 'last_seen_date') IS NULL
                            ALTER TABLE {self.schema_name}.{self.table_name}
                            ADD last_seen_date DATE DEFAULT CAST(GETDATE() AS DATE);
                        """))

                        # Add unique constraint
                        conn.execute(text(f"""
                            IF NOT EXISTS (
                                SELECT * FROM sys.indexes
                                WHERE name = 'uk_{self.table_name}_hash'
                                AND object_id = OBJECT_ID('{self.schema_name}.{self.table_name}')
                            )
                            ALTER TABLE {self.schema_name}.{self.table_name}
                            ADD CONSTRAINT uk_{self.table_name}_hash UNIQUE (instrument_hash);
                        """))

                    
                    logger.info(f"Table {self.schema_name}.{self.table_name} created with constraints.")
                else:
                    logger.info(f"Table {self.schema_name}.{self.table_name} already exists.")
                    
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def generate_instrument_hash(self, row: pd.Series, exclude_columns: List[str] = None) -> str:
        """Generate a unique hash for an instrument based on its core identifying fields."""
        if exclude_columns is None:
            exclude_columns = ['created_at', 'updated_at', 'last_seen_date', 'instrument_hash']
        
        # Identify core instrument fields (you may need to adjust this based on your data)
        core_columns = [
            'symbol', 'exchangecode', 'securitytype', 'underlier', 
            'strike', 'expiration', 'optiontype', 'root', 'localcode'
        ]
        
        # Use available core columns that exist in the row
        available_core_columns = [col for col in core_columns if col in row.index and pd.notna(row[col])]
        
        if not available_core_columns:
            # Fallback: use all columns except metadata columns
            available_core_columns = [col for col in row.index if col not in exclude_columns and pd.notna(row[col])]
        
        # Create hash string from core instrument identifying fields
        hash_parts = []
        for col in available_core_columns:
            value = row[col]
            if pd.notna(value):
                hash_parts.append(f"{col}:{str(value).lower().strip()}")
        
        if not hash_parts:
            # Last resort: hash all non-metadata fields
            for col in row.index:
                if col not in exclude_columns and pd.notna(row[col]):
                    hash_parts.append(f"{col}:{str(row[col]).lower().strip()}")
        
        hash_str = '|'.join(sorted(hash_parts))
        return hashlib.sha256(hash_str.encode()).hexdigest()

    def remove_duplicates_from_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicates from the DataFrame before insertion."""
        # Generate hash for each row based on core instrument fields only
        df['instrument_hash'] = df.apply(self.generate_instrument_hash, axis=1)
        
        # Remove duplicates within the DataFrame itself
        initial_count = len(df)
        df = df.drop_duplicates(subset=['instrument_hash'], keep='first')
        final_count = len(df)
        
        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} duplicates from DataFrame.")
        
        return df

    def get_existing_hashes(self) -> set:
        """Get set of existing hashes from the database."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(
                    text(f"SELECT instrument_hash FROM {self.schema_name}.{self.table_name}")
                )
                existing_hashes = {row[0] for row in result if row[0] is not None}
                logger.info(f"Found {len(existing_hashes)} existing records in database.")
                return existing_hashes
        except Exception as e:
            logger.warning(f"Error fetching existing hashes: {e}. Returning empty set.")
            return set()

    def filter_new_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out records that already exist in the database."""
        # Remove internal duplicates first
        df = self.remove_duplicates_from_dataframe(df)
        
        # Get existing hashes from database
        existing_hashes = self.get_existing_hashes()
        
        # Filter out records that already exist
        new_records = df[~df['instrument_hash'].isin(existing_hashes)]
        
        logger.info(f"Filtered to {len(new_records)} new records out of {len(df)} total.")
        
        return new_records

    def update_existing_records_timestamp(self):
        """Update last_seen_date for existing records to today."""
        try:
            with self.engine.begin() as connection:
                result = connection.execute(
                    text(f"""
                        UPDATE {self.schema_name}.{self.table_name} 
                        SET last_seen_date = CURRENT_DATE,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE last_seen_date < CURRENT_DATE
                    """)
                )
                logger.info(f"Updated last_seen_date for {result.rowcount} existing records.")
        except Exception as e:
            logger.warning(f"Error updating existing records timestamp: {e}")

    def insert_new_records(self, df: pd.DataFrame) -> Tuple[int, int]:
        """
        Insert new records into the database.
        Returns: (records_inserted, records_skipped)
        """
        if df.empty:
            logger.info("No new records to insert.")
            return 0, 0

        try:
            # Add current timestamp for new records
            df['created_at'] = datetime.datetime.now()
            df['updated_at'] = datetime.datetime.now()
            df['last_seen_date'] = datetime.date.today()
            
            # Use write_sql_with_retry for robust insertion
            write_sql_with_retry(
                df, 
                self.table_name, 
                schema=self.schema_name, 
                if_exists='append', 
                index=False, 
                chunksize=1000
            )
            
            logger.info(f"Successfully inserted {len(df)} new records into {self.schema_name}.{self.table_name}")
            return len(df), 0
            
        except Exception as e:
            logger.error(f"Error inserting records: {e}")
            # Fallback: try individual chunk insertion
            return self._insert_chunked(df)

    def _insert_chunked(self, df: pd.DataFrame) -> Tuple[int, int]:
        """Fallback method to insert records in smaller chunks."""
        chunk_size = 500
        total_inserted = 0
        total_skipped = 0
        
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            try:
                # Add timestamps for each chunk
                chunk = chunk.copy()
                chunk['created_at'] = datetime.datetime.now()
                chunk['updated_at'] = datetime.datetime.now()
                chunk['last_seen_date'] = datetime.date.today()
                
                chunk.to_sql(
                    self.table_name,
                    self.engine,
                    schema=self.schema_name,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                total_inserted += len(chunk)
                logger.info(f"Inserted chunk {i//chunk_size + 1}: {len(chunk)} records")
            except Exception as e:
                logger.error(f"Failed to insert chunk {i//chunk_size + 1}: {e}")
                total_skipped += len(chunk)
        
        return total_inserted, total_skipped


def main():
    # Load credentials

    load_dotenv()
    username = os.getenv("GvWSUSERNAME")
    password = os.getenv("GvWSPASSWORD")

    if not username or not password:
        logger.error("Username or password not found in environment variables. Please check GvWSUSERNAME and GvWSPASSWORD. ")
        return

    extractor = MarketDataExtractor(username, password, environment="onboard")

    # Define schema and table names
    schema_name = "MV_PRICES_2"
    table_name = "InstrumentList"
    
    # Initialize database manager
    db_manager = DatabaseManager(schema_name, table_name)

    try:
        # Check if schema exists, if not, create it
        with engine.connect() as connection:
            inspector = inspect(connection)
            if schema_name not in inspector.get_schema_names():
                connection.execute(text(f"CREATE SCHEMA {schema_name}"))
                connection.commit()
                logger.info(f"Schema '{schema_name}' created successfully. ")
            else:
                logger.info(f"Schema '{schema_name}' already exists. Skipping creation. ")

        logger.info("Starting exchange instruments extraction... ")
        exchange_instruments = extractor.extract_all_exchange_instruments(max_workers=5)

        if exchange_instruments:
            logger.info("Combining all exchange instruments into a single DataFrame... ")
            combined_df = pd.concat(exchange_instruments.values(), ignore_index=True)

            # Explicitly delete the intermediate dictionary and call garbage collector
            del exchange_instruments
            gc.collect()
            logger.info("Intermediate 'exchange_instruments' dictionary deleted and garbage collection run. 🧹")

            # Create table if it doesn't exist
            db_manager.create_table_if_not_exists(combined_df)

            # Filter out duplicates and get only new records
            new_records_df = db_manager.filter_new_records(combined_df)

            # Insert only new records
            if not new_records_df.empty:
                inserted, skipped = db_manager.insert_new_records(new_records_df)
                logger.info(f"Database update complete: {inserted} new records inserted, {skipped} records skipped.")
            else:
                logger.info("No new records to insert. Database is already up to date.")

            # Update timestamps for existing records
            db_manager.update_existing_records_timestamp()

            # Explicitly delete the combined DataFrame and call garbage collector
            del combined_df, new_records_df
            gc.collect()
            logger.info("DataFrames deleted and garbage collection run.")

    except Exception as e:
        logger.error(f"An unexpected error occurred during execution: {e}")


if __name__ == "__main__":
    main()