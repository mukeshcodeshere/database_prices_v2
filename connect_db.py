#connect_db.py
import os
import time
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from urllib import parse
from dotenv import load_dotenv
import pandas as pd # Make sure pandas is imported here

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Read env vars
server_unqualified = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
server = f"tcp:{server_unqualified},1433"

# Create connection string with increased timeout and retry settings
connecting_string = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={server};"
    f"Database={database};"
    f"Uid={username};"
    f"Pwd={password};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=60;"  # Increased from 30 to 60 seconds
    f"Command Timeout=300;"    # 5 minutes for long-running queries
    f"LoginTimeout=60;"        # Login timeout
)

params = parse.quote_plus(connecting_string)

# Create engine with connection pooling and retry logic
engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}", 
    fast_executemany=True,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,  # Recycle connections every hour
    pool_pre_ping=True,  # Verify connections before use
    echo=False  # Set to True for SQL debugging
)

USERNAME = os.getenv("GvWSUSERNAME")
PASSWORD = os.getenv("GvWSPASSWORD")

def execute_with_retry(query, max_retries=3, retry_delay=5):
    """Execute query with retry logic for connection failures"""
    for attempt in range(max_retries):
        try:
            with engine.connect() as connection:
                result = connection.execute(text(query))
                return result
        except OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Database connection failed (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"Database connection failed after {max_retries} attempts: {e}")
                raise

# MODIFIED read_sql_with_retry to accept 'params'
def read_sql_with_retry(query, params=None, max_retries=3, retry_delay=5):
    """Read SQL with pandas and retry logic, supporting parameterized queries"""
    for attempt in range(max_retries):
        try:
            # Use text(query) for parameterized queries with pandas.read_sql
            if params:
                return pd.read_sql(text(query), con=engine, params=params)
            else:
                return pd.read_sql(query, con=engine)
        except OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Database query failed (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"Database query failed after {max_retries} attempts: {e}")
                raise

def write_sql_with_retry(df, table_name, schema, if_exists='append', index=False, chunksize=None, retries=5, delay=2):
    """Writes DataFrame to SQL with retry logic."""
    for i in range(retries):
        try:
            df.to_sql(table_name, con=engine, schema=schema, if_exists=if_exists, 
                      index=index, chunksize=chunksize)
            return True
        except Exception as e:
            print(f"Write error (attempt {i+1}/{retries}): {e}")
            time.sleep(delay)
    raise Exception(f"Failed to write to table {table_name} after multiple retries.")


def test_connection():
    """Test database connection"""
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1 as test"))
            test_result = result.fetchone()
            if test_result and test_result[0] == 1:
                logger.info("Database connection successful")
                return True
            else:
                logger.error("Database connection test failed")
                return False
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False

database100 = r'GCC-db-100'

# Create connection string with increased timeout and retry settings
connecting_string_100 = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={server};"
    f"Database={database100};"
    f"Uid={username};"
    f"Pwd={password};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=60;"  # Increased from 30 to 60 seconds
    f"Command Timeout=300;"    # 5 minutes for long-running queries
    f"LoginTimeout=60;"        # Login timeout
)

params100 = parse.quote_plus(connecting_string_100)

# Create engine with connection pooling and retry logic
engine100 = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params100}", 
    fast_executemany=True,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,  # Recycle connections every hour
    pool_pre_ping=True,  # Verify connections before use
    echo=False  # Set to True for SQL debugging
)


def read_sql_with_retry100(query, params=None, max_retries=3, retry_delay=5):
    """Read SQL with pandas and retry logic, supporting parameterized queries"""
    for attempt in range(max_retries):
        try:
            # Use text(query) for parameterized queries with pandas.read_sql
            if params:
                return pd.read_sql(text(query), con=engine100, params=params100)
            else:
                return pd.read_sql(query, con=engine100)
        except OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Database query failed (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logger.error(f"Database query failed after {max_retries} attempts: {e}")
                raise

def write_sql_with_retry100(df, table_name, schema, if_exists='append', index=False, chunksize=None, retries=5, delay=2):
    """Writes DataFrame to SQL with retry logic."""
    for i in range(retries):
        try:
            df.to_sql(table_name, con=engine100, schema=schema, if_exists=if_exists, 
                      index=index, chunksize=chunksize)
            return True
        except Exception as e:
            print(f"Write error (attempt {i+1}/{retries}): {e}")
            time.sleep(delay)
    raise Exception(f"Failed to write to table {table_name} after multiple retries.")

# Test connection on import
if __name__ == "__main__":
    test_connection()
else:
    try:
        test_connection()
    except Exception as e:
        logger.warning(f"Initial connection test failed: {e}")
        logger.info("Application will continue with retry logic for actual queries")