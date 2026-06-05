import sys
import os

# Windows Hadoop workaround
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ["PATH"]

sys.path.append(os.path.abspath("."))

from config.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    explode,
    when,
    month,
    year,
    quarter,
    dayofweek,
    round
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

# ==================================================
# SALES DATA (CSV)
# ==================================================

sales_df = spark.read.csv(
    "data/raw/csv/Sample - Superstore.csv",
    header=True,
    inferSchema=True,
    escape='"'
)

sales_df = sales_df.withColumn(
    "Order Date",
    to_date(col("Order Date"), "M/d/yyyy")
)

# ==================================================
# CUSTOMER DIMENSION (POSTGRESQL)
# ==================================================

customers_df = (
    spark.read
    .format("jdbc")
    .option(
        "url",
        f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    .option("dbtable", "customer_dim")
    .option("user", DB_USER)
    .option("password", DB_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .load()
)

# Rename database columns to avoid collision with sales_df columns
db_customers_df = customers_df.select(
    col("customer_id"),
    col("customer_name").alias("db_customer_name"),
    col("segment").alias("db_segment"),
    col("city").alias("db_city"),
    col("state").alias("db_state"),
    col("region").alias("db_region")
)

print("\nCustomer Dimension Rows:")
print(db_customers_df.count())

# ==================================================
# SALES + CUSTOMER JOIN
# ==================================================

sales_customer_df = (
    sales_df.join(
        db_customers_df,
        sales_df["Customer ID"] ==
        db_customers_df["customer_id"],
        "left"
    ).drop("customer_id")
)


# ==================================================
# HOLIDAY DATA (API)
# ==================================================

holiday_df = (
    spark.read
    .option("multiline", "true")
    .json("data/raw/api/holidays.json")
)

holidays = holiday_df.select(
    explode("holidays").alias("holiday")
)

holidays_df = holidays.select(
    col("holiday.name").alias("holiday_name"),
    to_date(
        col("holiday.date.iso")
    ).alias("holiday_date")
)

# ==================================================
# SALES + CUSTOMERS + HOLIDAYS
# ==================================================

final_df = (
    sales_customer_df.join(
        holidays_df,
        sales_customer_df["Order Date"] ==
        holidays_df["holiday_date"],
        "left"
    )
)

# ==================================================
# HOLIDAY FLAG
# ==================================================

final_df = final_df.withColumn(
    "holiday_flag",
    when(
        col("holiday_name").isNotNull(),
        1
    ).otherwise(0)
)

# ==================================================
# FEATURE ENGINEERING
# ==================================================

final_df = final_df.withColumn(
    "month",
    month(col("Order Date"))
)

final_df = final_df.withColumn(
    "year",
    year(col("Order Date"))
)

final_df = final_df.withColumn(
    "quarter",
    quarter(col("Order Date"))
)

final_df = final_df.withColumn(
    "weekend_flag",
    when(
        dayofweek(col("Order Date")).isin([1, 7]),
        1
    ).otherwise(0)
)

final_df = final_df.withColumn(
    "profit_margin",
    round(
        (col("Profit") / col("Sales")) * 100,
        2
    )
)

# ==================================================
# VALIDATION
# ==================================================

print("\nFinal Row Count:")
print(final_df.count())

print("\nHoliday Orders:")
print(
    final_df.filter(
        col("holiday_flag") == 1
    ).count()
)

print("\nCustomer Dimension Check:")
final_df.select(
    "Customer ID",
    "db_customer_name",
    "db_segment",
    "db_region"
).show(10, truncate=False)

# ==================================================
# SAVE OUTPUT
# ==================================================

final_df.coalesce(1).write.mode("overwrite").option(
    "header",
    True
).csv(
    "data/processed/final_analytics"
)

print("\nFinal dataset saved successfully!")

spark.stop()