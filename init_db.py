# init_database.py
from connect_db import engine
from sqlalchemy import text

def initialize_database():
    """Initialize the database schema and table structure."""
    schema_name = "MV_PRICES_2"
    table_name = "InstrumentList"
    
    with engine.connect() as connection:
        # Create schema if not exists
        connection.execute(text(f"""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema_name}')
            BEGIN
                EXEC('CREATE SCHEMA {schema_name}')
            END
        """))
        
        # Create table with basic structure
        connection.execute(text(f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
            BEGIN
                CREATE TABLE {schema_name}.{table_name} (
                    symbol NVARCHAR(255),
                    exchangecode NVARCHAR(50),
                    securitytype NVARCHAR(50),
                    underlier NVARCHAR(255),
                    strike FLOAT,
                    expiration DATETIME2,
                    optiontype NVARCHAR(10),
                    instrument_hash NVARCHAR(64),
                    created_at DATETIME2 DEFAULT GETDATE(),
                    updated_at DATETIME2 DEFAULT GETDATE(),
                    last_seen_date DATE DEFAULT CAST(GETDATE() AS DATE),
                    last_price_pull_date DATE NULL
                )
                
                -- Add constraints and indexes
                ALTER TABLE {schema_name}.{table_name} 
                ADD CONSTRAINT uk_instrument_hash UNIQUE (instrument_hash)
                
                CREATE INDEX ix_symbol ON {schema_name}.{table_name} (symbol)
            END
        """))
        connection.commit()
        print("Database initialized successfully")

if __name__ == "__main__":
    initialize_database()