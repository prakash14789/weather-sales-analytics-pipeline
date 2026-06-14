import sys
import os
import shutil
import glob

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
    round,
    regexp_replace
)

# ==================================================
# MODULAR FUNCTIONS FOR ETL AND FEATURE ENGINEERING
# ==================================================

def clean_sales_data(sales_df):
    """
    Cleans raw superstore sales data. Converts Order Date to proper date
    and cleans up Product Name character anomalies.
    """
    return sales_df.withColumn(
        "Order Date",
        to_date(col("Order Date"), "M/d/yyyy")
    ).withColumn(
        "Product Name",
        regexp_replace(
            regexp_replace(
                regexp_replace(col("Product Name"), ",", " -"),
                "[\\u0093\\u0094]",
                "\""
            ),
            "[\\u00a0]",
            " "
        )
    )

def prepare_holidays(holiday_df):
    """
    Parses and flattens the nested raw holidays structure from API.
    """
    holidays = holiday_df.select(
        explode("holidays").alias("holiday")
    )
    return holidays.select(
        col("holiday.name").alias("holiday_name"),
        to_date(
            col("holiday.date.iso")
        ).alias("holiday_date")
    )

def join_sales_customers(sales_df, customers_df):
    """
    Left-joins cleaned Sales DataFrame with Customer Dimension from Postgres.
    """
    # Rename database columns to avoid collision with sales_df columns
    db_customers_df = customers_df.select(
        col("customer_id"),
        col("customer_name").alias("db_customer_name"),
        col("segment").alias("db_segment"),
        col("city").alias("db_city"),
        col("state").alias("db_state"),
        col("region").alias("db_region")
    )
    return (
        sales_df.join(
            db_customers_df,
            sales_df["Customer ID"] ==
            db_customers_df["customer_id"],
            "left"
        ).drop("customer_id")
    )

def join_sales_holidays(sales_customer_df, holidays_df):
    """
    Left-joins Sales+Customer DataFrame with Holidays DataFrame.
    """
    return (
        sales_customer_df.join(
            holidays_df,
            sales_customer_df["Order Date"] ==
            holidays_df["holiday_date"],
            "left"
        )
    )

def engineer_features(final_df):
    """
    Engineers retail specific analysis columns like holiday_flag,
    profit_margin, weekend_flag, month, year, and quarter.
    """
    final_df = final_df.withColumn(
        "holiday_flag",
        when(
            col("holiday_name").isNotNull(),
            1
        ).otherwise(0)
    )
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
    return final_df

# ==================================================
# MAIN ETL PIPELINE ENTRYPOINT
# ==================================================

def main():
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

    # 1. Load raw sales data
    print("\nLoading sales data...")
    sales_df = spark.read.option("encoding", "ISO-8859-1").csv(
        "data/raw/csv/Sample - Superstore.csv",
        header=True,
        inferSchema=True,
        escape='"'
    )

    # 2. Clean sales data
    sales_df = clean_sales_data(sales_df)

    # 3. Load customer dimension (PostgreSQL)
    print("Loading customer dimension from PostgreSQL...")
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

    # 4. Join Sales + Customer
    print("Joining sales and customer dimension...")
    sales_customer_df = join_sales_customers(sales_df, customers_df)
    print("Customer Dimension Rows in join check:", sales_customer_df.count())

    # 5. Load Holiday data
    print("Loading holiday data from JSON...")
    holiday_df = (
        spark.read
        .option("multiline", "true")
        .json("data/raw/api/holidays.json")
    )
    holidays_df = prepare_holidays(holiday_df)

    # 6. Join Sales + Customer + Holidays
    print("Joining holidays...")
    final_df = join_sales_holidays(sales_customer_df, holidays_df)

    # 7. Feature Engineering
    print("Performing feature engineering...")
    final_df = engineer_features(final_df)

    # 8. Validation Checks
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

    # 9. Save Output
    temp_dir = "data/processed/final_analytics_temp"
    dest_dir = "data/processed/final_analytics"

    # Write Spark output to temp directory
    final_df.coalesce(1).write.mode("overwrite").option(
        "header",
        True
    ).option(
        "nullValue",
        "null"
    ).option(
        "escape",
        "\""
    ).csv(
        temp_dir
    )

    print("\nFinal dataset generated successfully in temp folder!")

    spark.stop()

    # Ensure destination directory exists
    os.makedirs(dest_dir, exist_ok=True)

    # Remove any existing files in final directory
    for f in glob.glob(os.path.join(dest_dir, "*")):
        try:
            if os.path.isfile(f):
                os.remove(f)
            elif os.path.isdir(f):
                shutil.rmtree(f)
        except Exception as e:
            print(f"Warning: Could not remove old file {f}: {e}")

    # Move new files from temp directory to final directory
    for f in glob.glob(os.path.join(temp_dir, "*")):
        try:
            shutil.move(f, dest_dir)
        except Exception as e:
            print(f"Error: Could not move file {f} to {dest_dir}: {e}")

    # Clean up temp directory
    try:
        shutil.rmtree(temp_dir)
    except Exception as e:
        print(f"Warning: Could not remove temp directory {temp_dir}: {e}")

    print("Final dataset moved to destination and saved successfully!")

if __name__ == "__main__":
    main()