import pandas as pd
from sqlalchemy import create_engine
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

# Load into PostgreSQL
df.to_sql(
    "customers",
    engine,
    if_exists="replace",
    index=False
)

print(f"{len(df)} records loaded successfully!")