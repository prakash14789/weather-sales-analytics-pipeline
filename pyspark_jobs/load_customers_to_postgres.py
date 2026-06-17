import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import sys
import os

sys.path.append(os.path.abspath("."))
from config.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

# Read customer file
df = pd.read_csv(
    "data/processed/customers.csv"
)

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

# PostgreSQL connection (encode password to handle special characters like @)
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# 1. Execute DDL from create_tables.sql
print("Executing DDL from create_tables.sql...")
with engine.begin() as conn:
    with open("sql/create_tables.sql", "r", encoding="utf-8") as f:
        ddl_sql = f.read()
    if ddl_sql.strip():
        conn.execute(text(ddl_sql))

# 2. Load staging customer data
print(f"Loading {len(df)} records into staging table 'customers'...")
df.to_sql(
    "customers",
    engine,
    if_exists="replace",
    index=False
)

# 3. Execute DML from insert_customers.sql
print("Executing DML from insert_customers.sql to populate customer_dim...")
with engine.begin() as conn:
    with open("sql/insert_customers.sql", "r", encoding="utf-8") as f:
        dml_sql = f.read()
    if dml_sql.strip():
        conn.execute(text(dml_sql))

print("Customer loading and dimension update completed successfully!")