import os
import glob
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from src.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

def get_db_engine():
    """
    Creates and returns a SQLAlchemy engine for PostgreSQL using credentials from config.
    """
    encoded_password = quote_plus(DB_PASSWORD) if DB_PASSWORD else ""
    db_url = f"postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)

def extract_customers(raw_csv_path="data/raw/csv/Sample - Superstore.csv", output_csv_path="data/processed/customers.csv"):
    """
    Extracts unique customer listings from raw superstore orders CSV.
    """
    print(f"Reading raw sales records from {raw_csv_path}...")
    df = pd.read_csv(
        raw_csv_path,
        encoding="latin1"
    )

    customers = df[
        [
            "Customer ID",
            "Customer Name",
            "Segment",
            "Country",
            "City",
            "State",
            "Postal Code",
            "Region"
        ]
    ].drop_duplicates()

    print("Total Unique Customer Records Found:", len(customers))

    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    customers.to_csv(
        output_csv_path,
        index=False
    )
    print(f"Extracted customers saved successfully to {output_csv_path}")
    return len(customers)

def load_customers_to_postgres(customers_csv_path="data/processed/customers.csv", create_tables_sql_path="sql/create_tables.sql", insert_customers_sql_path="sql/insert_customers.sql"):
    """
    Loads unique customer profiles into the PostgreSQL database, executing create_tables DDL and insert_customers DML.
    """
    if not os.path.exists(customers_csv_path):
        print(f"Error: {customers_csv_path} does not exist. Please extract customers first.")
        return False
        
    print(f"Reading unique customers CSV from {customers_csv_path}...")
    df = pd.read_csv(customers_csv_path)

    # Rename columns to match PostgreSQL table
    df.columns = [
        "customer_id",
        "customer_name",
        "segment",
        "country",
        "city",
        "state",
        "postal_code",
        "region"
    ]

    engine = get_db_engine()

    # 1. Execute DDL from create_tables.sql
    if os.path.exists(create_tables_sql_path):
        print(f"Executing DDL from {create_tables_sql_path}...")
        with engine.begin() as conn:
            with open(create_tables_sql_path, "r", encoding="utf-8") as f:
                ddl_sql = f.read()
            if ddl_sql.strip():
                conn.execute(text(ddl_sql))
    else:
        print(f"Warning: DDL file {create_tables_sql_path} not found.")

    # 2. Load staging customer data
    print(f"Loading {len(df)} records into staging table 'customers'...")
    df.to_sql(
        "customers",
        engine,
        if_exists="replace",
        index=False
    )

    # 3. Execute DML from insert_customers.sql
    if os.path.exists(insert_customers_sql_path):
        print(f"Executing DML from {insert_customers_sql_path} to populate customer_dim...")
        with engine.begin() as conn:
            with open(insert_customers_sql_path, "r", encoding="utf-8") as f:
                dml_sql = f.read()
            if dml_sql.strip():
                conn.execute(text(dml_sql))
    else:
        print(f"Warning: DML file {insert_customers_sql_path} not found.")

    print("Customer loading and dimension update completed successfully!")
    return True

def load_final_analytics_to_postgres(analytics_csv_pattern="data/processed/final_analytics/part-*.csv", table_name="final_analytics"):
    """
    Reads PySpark processed CSV output files and loads them into PostgreSQL final_analytics table.
    """
    csv_files = glob.glob(analytics_csv_pattern)
    
    if not csv_files:
        print(f"Error: No processed final analytics CSV files found matching {analytics_csv_pattern}")
        return False
        
    print(f"Located {len(csv_files)} processed CSV part files.")
    
    # Read CSV data into Pandas and concatenate
    print("Reading and concatenating CSV data...")
    df_list = []
    for f in csv_files:
        print(f"Reading {f}...")
        df_list.append(pd.read_csv(f))
    df = pd.concat(df_list, ignore_index=True)
    
    # Clean and rename column names to lowercase snake_case
    print("Renaming and formatting columns...")
    original_cols = df.columns.tolist()
    new_cols = [col.strip().lower().replace(" ", "_").replace("-", "_") for col in original_cols]
    df.columns = new_cols
    
    # Convert date columns to proper datetime types for database loading
    date_columns = ["order_date", "ship_date", "holiday_date"]
    for col in date_columns:
        if col in df.columns:
            print(f"Parsing column '{col}' as datetime...")
            df[col] = pd.to_datetime(df[col], errors="coerce")
            
    engine = get_db_engine()
    
    # Load data
    print(f"Loading {len(df)} records into PostgreSQL table '{table_name}'...")
    df.to_sql(
        table_name,
        con=engine,
        if_exists="replace",
        index=False
    )
    
    print(f"Success! Data loaded successfully into PostgreSQL table '{table_name}'.")
    return True

def fetch_customer_dim(output_csv_path="data/processed/customer_dim.csv"):
    """
    Pulls customer dimension table out of PostgreSQL and saves it back to the local CSV.
    """
    engine = get_db_engine()
    print("Querying 'customer_dim' table from database...")
    df = pd.read_sql("SELECT * FROM customer_dim", engine)
    
    print(f"Successfully fetched {len(df)} records.")
    
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    df.to_csv(output_csv_path, index=False)
    print(f"Saved data successfully to: {output_csv_path}")
    return len(df)
