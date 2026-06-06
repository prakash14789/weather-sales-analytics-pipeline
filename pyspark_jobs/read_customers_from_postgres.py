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
    .config(
        "spark.jars",
        "drivers/postgresql-42.7.11.jar"
    )
    .getOrCreate()
)

customers_df = (
    spark.read
    .format("jdbc")
    .option(
        "url",
        f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    .option("dbtable", "customers")
    .option("user", DB_USER)
    .option("password", DB_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

print("\nRow Count:")
print(customers_df.count())

print("\nSchema:")
customers_df.printSchema()

print("\nSample Data:")
customers_df.show(5, truncate=False)

spark.stop()