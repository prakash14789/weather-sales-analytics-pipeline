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
    .getOrCreate()
)

# -----------------------
# SALES DATA
# -----------------------

sales_df = spark.read.csv(
    "data/raw/csv/Sample - Superstore.csv",
    header=True,
    inferSchema=True
)

sales_df = sales_df.withColumn(
    "Order Date",
    to_date(col("Order Date"), "M/d/yyyy")
)

# -----------------------
# HOLIDAY DATA
# -----------------------

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

# -----------------------
# JOIN
# -----------------------

final_df = (
    sales_df.join(
        holidays_df,
        sales_df["Order Date"] ==
        holidays_df["holiday_date"],
        "left"
    )
)

# -----------------------
# HOLIDAY FLAG
# -----------------------

final_df = final_df.withColumn(
    "holiday_flag",
    when(
        col("holiday_name").isNotNull(),
        1
    ).otherwise(0)
)

# -----------------------
# MONTH
# -----------------------

final_df = final_df.withColumn(
    "month",
    month(col("Order Date"))
)

# -----------------------
# YEAR
# -----------------------

final_df = final_df.withColumn(
    "year",
    year(col("Order Date"))
)

# -----------------------
# QUARTER
# -----------------------

final_df = final_df.withColumn(
    "quarter",
    quarter(col("Order Date"))
)

# -----------------------
# WEEKEND FLAG
# -----------------------

final_df = final_df.withColumn(
    "weekend_flag",
    when(
        dayofweek(col("Order Date")).isin([1, 7]),
        1
    ).otherwise(0)
)

# -----------------------
# PROFIT MARGIN
# -----------------------

final_df = final_df.withColumn(
    "profit_margin",
    round(
        (col("Profit") / col("Sales")) * 100,
        2
    )
)

print("\nFinal Row Count:")
print(final_df.count())

print("\nNew Columns Added:")
print(
    [
        "holiday_flag",
        "month",
        "year",
        "quarter",
        "weekend_flag",
        "profit_margin"
    ]
)

final_df.select(
    "Order Date",
    "Sales",
    "Profit",
    "holiday_flag",
    "month",
    "year",
    "quarter",
    "weekend_flag",
    "profit_margin"
).show(20)

spark.stop()