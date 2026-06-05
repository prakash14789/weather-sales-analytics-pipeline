from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    explode,
    when
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("Holiday Analytics")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

# ---------------------------
# SALES DATA
# ---------------------------

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

# ---------------------------
# CUSTOMER DATA
# ---------------------------

customers_df = spark.read.csv(
    "data/processed/customers.csv",
    header=True,
    inferSchema=True
)

# Rename for joining
customers_df = customers_df.withColumnRenamed(
    "Customer ID",
    "customer_id"
)

# ---------------------------
# HOLIDAY DATA
# ---------------------------

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

# ---------------------------
# SALES + HOLIDAYS
# ---------------------------

final_df = (
    sales_df.join(
        holidays_df,
        sales_df["Order Date"] ==
        holidays_df["holiday_date"],
        "left"
    )
)

final_df = final_df.withColumn(
    "holiday_flag",
    when(
        col("holiday_name").isNotNull(),
        1
    ).otherwise(0)
)

print("\nTotal Rows:")
print(final_df.count())

print("\nHoliday Orders:")
print(
    final_df.filter(
        col("holiday_flag") == 1
    ).count()
)

print("\nSample Data:")
final_df.select(
    "Order ID",
    "Order Date",
    "Sales",
    "holiday_name",
    "holiday_flag"
).show(20, truncate=False)

spark.stop()