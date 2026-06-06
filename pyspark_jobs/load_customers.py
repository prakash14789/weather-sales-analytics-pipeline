import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

import os

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

from pyspark.sql import SparkSession

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

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("Holiday Analytics")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

customer_pd = pd.read_sql(
    "SELECT * FROM customers",
    engine
)

customer_df = spark.createDataFrame(customer_pd)

print("\nRow Count:")
print(customer_df.count())

print("\nSchema:")
customer_df.printSchema()

print("\nSample Data:")
customer_df.show(5)

spark.stop()