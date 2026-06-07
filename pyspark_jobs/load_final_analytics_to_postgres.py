import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import sys
import os
import glob

# Ensure project root is in Python path for configuration imports
sys.path.append(os.path.abspath("."))
from config.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

def main():
    # Find the processed analytics CSV file
    csv_pattern = "data/processed/final_analytics/part-*.csv"
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        print("Error: No processed final analytics CSV file found under data/processed/final_analytics/")
        sys.exit(1)
        
    csv_path = csv_files[0]
    print(f"Located processed CSV: {csv_path}")
    
    # Read CSV data into Pandas
    print("Reading CSV data...")
    df = pd.read_csv(csv_path)
    
    # Clean and rename column names to lowercase snake_case
    print("Renaming and formatting columns...")
    original_cols = df.columns.tolist()
    new_cols = [col.strip().lower().replace(" ", "_").replace("-", "_") for col in original_cols]
    
    # Update DataFrame column names
    df.columns = new_cols
    
    # Convert date columns to proper datetime types for database loading
    date_columns = ["order_date", "ship_date", "holiday_date"]
    for col in date_columns:
        if col in df.columns:
            print(f"Parsing column '{col}' as datetime...")
            df[col] = pd.to_datetime(df[col], errors="coerce")
            
    # Establish connection to PostgreSQL
    print("Connecting to PostgreSQL...")
    encoded_password = quote_plus(DB_PASSWORD) if DB_PASSWORD else ""
    db_url = f"postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)
    
    table_name = "final_analytics"
    
    # Load data
    print(f"Loading {len(df)} records into PostgreSQL table '{table_name}'...")
    df.to_sql(
        table_name,
        con=engine,
        if_exists="replace",
        index=False
    )
    
    print(f"Success! Data loaded successfully into PostgreSQL table '{table_name}'.")

if __name__ == "__main__":
    main()
