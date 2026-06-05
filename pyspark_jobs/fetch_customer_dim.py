import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Add the root directory to path to import config
sys.path.append(os.path.abspath("."))

from config.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

def fetch_customer_dim():
    print("Connecting to PostgreSQL database...")
    # Encode password to handle special characters
    encoded_password = quote_plus(DB_PASSWORD)
    db_url = f"postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    # Create database engine
    engine = create_engine(db_url)
    
    print("Querying 'customer_dim' table...")
    # Fetch customer_dim table from PostgreSQL
    df = pd.read_sql("SELECT * FROM customer_dim", engine)
    
    print(f"Successfully fetched {len(df)} records.")
    
    # Ensure processed directory exists
    os.makedirs("data/processed", exist_ok=True)
    
    # Save to processed data folder
    output_path = "data/processed/customer_dim.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved data successfully to: {output_path}")

if __name__ == "__main__":
    fetch_customer_dim()
