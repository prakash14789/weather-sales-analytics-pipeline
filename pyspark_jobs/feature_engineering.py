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

import datetime
import bisect
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
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
    regexp_replace,
    dayofmonth,
    sum as spark_sum,
    avg as spark_avg,
    udf,
    coalesce,
    lit
)
from pyspark.sql.types import IntegerType, DoubleType

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

def join_sales_weather(sales_df, weather_df):
    """
    Left-joins Sales+Customer+Holiday DataFrame with Weather DataFrame on Date and Region.
    """
    cleaned_weather_df = weather_df.select(
        to_date(col("weather_date")).alias("w_date"),
        col("weather_region").alias("w_region"),
        col("temp_c").cast("double").alias("temp_c"),
        col("precipitation_mm").cast("double").alias("precipitation_mm"),
        col("snowfall_cm").cast("double").alias("snowfall_cm"),
        col("wind_speed_kmh").cast("double").alias("wind_speed_kmh")
    )
    
    join_region = coalesce(col("db_region"), col("Region"))
    
    return (
        sales_df.join(
            cleaned_weather_df,
            (sales_df["Order Date"] == cleaned_weather_df["w_date"]) &
            (join_region == cleaned_weather_df["w_region"]),
            "left"
        ).drop("w_date", "w_region")
    )

def load_holiday_metadata_from_json(json_path="data/raw/api/holidays.json"):
    """
    Loads holiday dates and maps Thanksgiving dates by year from a local JSON file.
    """
    try:
        import json
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        h_dates = []
        tg_by_year = {}
        for h in data.get("holidays", []):
            iso_date = h.get("date", {}).get("iso")
            name = h.get("name")
            if iso_date:
                dt = datetime.datetime.strptime(iso_date[:10], "%Y-%m-%d").date()
                h_dates.append(dt)
                if name == "Thanksgiving Day":
                    tg_by_year[dt.year] = dt
        return sorted(list(set(h_dates))), tg_by_year
    except Exception as e:
        print(f"Warning: Could not load holiday metadata from JSON: {e}")
        return [], {}

def engineer_features(final_df, holiday_dates=None, thanksgiving_by_year=None):
    """
    Engineers retail specific analysis columns like holiday_flag,
    profit_margin, weekend_flag, month, year, quarter, days_until_next_holiday,
    days_since_last_holiday, back_to_school_flag, holiday_shopping_season_flag,
    rolling_sales_7d, and rolling_sales_30d.
    """
    # 1. Standard features
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

    # 2. Season Windows (Back to School)
    # July 15 to Aug 31
    final_df = final_df.withColumn(
        "back_to_school_flag",
        when(
            (month(col("Order Date")) == 7) & (dayofmonth(col("Order Date")) >= 15),
            1
        ).when(
            month(col("Order Date")) == 8,
            1
        ).otherwise(0)
    )

    # Load holiday metadata if not provided
    if holiday_dates is None or thanksgiving_by_year is None:
        json_holiday_dates, json_thanksgiving = load_holiday_metadata_from_json()
        if holiday_dates is None:
            holiday_dates = json_holiday_dates
        if thanksgiving_by_year is None:
            thanksgiving_by_year = json_thanksgiving

    # 3. Days Until/Since Holiday and Holiday Shopping Season (Thanksgiving to Christmas)
    spark = SparkSession.getActiveSession()
    if spark and holiday_dates:
        holiday_dates_bc = spark.sparkContext.broadcast(holiday_dates)
        thanksgiving_bc = spark.sparkContext.broadcast(thanksgiving_by_year)

        def get_days_until_holiday(order_date):
            if not order_date:
                return None
            h_dates = holiday_dates_bc.value
            idx = bisect.bisect_left(h_dates, order_date)
            if idx < len(h_dates):
                return (h_dates[idx] - order_date).days
            return None

        def get_days_since_holiday(order_date):
            if not order_date:
                return None
            h_dates = holiday_dates_bc.value
            idx = bisect.bisect_right(h_dates, order_date)
            if idx > 0:
                return (order_date - h_dates[idx - 1]).days
            return None

        def get_holiday_shopping_season(order_date):
            if not order_date:
                return 0
            yr = order_date.year
            tg_date = thanksgiving_bc.value.get(yr)
            if tg_date:
                christmas_date = datetime.date(yr, 12, 25)
                if tg_date <= order_date <= christmas_date:
                    return 1
            return 0

        days_until_udf = udf(get_days_until_holiday, IntegerType())
        days_since_udf = udf(get_days_since_holiday, IntegerType())
        holiday_season_udf = udf(get_holiday_shopping_season, IntegerType())

        final_df = final_df.withColumn("days_until_next_holiday", days_until_udf(col("Order Date")))
        final_df = final_df.withColumn("days_since_last_holiday", days_since_udf(col("Order Date")))
        final_df = final_df.withColumn("holiday_shopping_season_flag", holiday_season_udf(col("Order Date")))
    else:
        # Fallback values if Spark is not active or holiday dates are empty
        final_df = final_df.withColumn("days_until_next_holiday", when(col("Order Date").isNull(), None).otherwise(None).cast(IntegerType()))
        final_df = final_df.withColumn("days_since_last_holiday", when(col("Order Date").isNull(), None).otherwise(None).cast(IntegerType()))
        final_df = final_df.withColumn("holiday_shopping_season_flag", when(col("Order Date").isNull(), 0).otherwise(0).cast(IntegerType()))

    # 4. Rolling Daily Sales Moving Averages
    daily_sales_df = (
        final_df.groupBy("Order Date")
        .agg(spark_sum("Sales").alias("total_daily_sales"))
    )
    daily_sales_df = daily_sales_df.withColumn(
        "date_secs",
        col("Order Date").cast("timestamp").cast("long")
    )

    window_7d = Window.orderBy("date_secs").rangeBetween(-6 * 24 * 3600, 0)
    window_30d = Window.orderBy("date_secs").rangeBetween(-29 * 24 * 3600, 0)

    daily_sales_df = daily_sales_df.withColumn(
        "rolling_sales_7d",
        round(spark_avg("total_daily_sales").over(window_7d), 2)
    ).withColumn(
        "rolling_sales_30d",
        round(spark_avg("total_daily_sales").over(window_30d), 2)
    )

    final_df = final_df.join(
        daily_sales_df.select("Order Date", "rolling_sales_7d", "rolling_sales_30d"),
        on="Order Date",
        how="left"
    )

    # 5. Weather Features
    weather_cols = ["temp_c", "precipitation_mm", "snowfall_cm", "wind_speed_kmh"]
    for c in weather_cols:
        if c not in final_df.columns:
            final_df = final_df.withColumn(c, lit(None).cast(DoubleType()))
            
    final_df = final_df.withColumn(
        "is_raining",
        when(col("precipitation_mm").isNotNull() & (col("precipitation_mm") > 0.0), 1).otherwise(0)
    )
    final_df = final_df.withColumn(
        "is_snowing",
        when(col("snowfall_cm").isNotNull() & (col("snowfall_cm") > 0.0), 1).otherwise(0)
    )
    final_df = final_df.withColumn(
        "extreme_weather_flag",
        when(
            (col("precipitation_mm").isNotNull() & (col("precipitation_mm") > 25.0)) |
            (col("snowfall_cm").isNotNull() & (col("snowfall_cm") > 5.0)) |
            (col("wind_speed_kmh").isNotNull() & (col("wind_speed_kmh") > 40.0)),
            1
        ).otherwise(0)
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

    # 6.5 Load and Join Weather Data
    print("Loading weather data from JSON...")
    try:
        weather_raw_df = (
            spark.read
            .option("multiline", "true")
            .json("data/raw/api/weather.json")
        )
        final_df = join_sales_weather(final_df, weather_raw_df)
        print("Weather data joined successfully.")
    except Exception as e:
        print(f"Warning: Could not load/join weather data: {e}")

    # 7. Feature Engineering
    print("Performing feature engineering...")
    try:
        holiday_rows = holidays_df.select("holiday_date", "holiday_name").collect()
        holiday_dates = sorted(list(set([r["holiday_date"] for r in holiday_rows if r["holiday_date"]])))
        thanksgiving_by_year = {
            r["holiday_date"].year: r["holiday_date"]
            for r in holiday_rows
            if r["holiday_date"] and r["holiday_name"] == "Thanksgiving Day"
        }
    except Exception as e:
        print(f"Warning: Could not extract holiday metadata, using fallback parsing: {e}")
        holiday_dates = None
        thanksgiving_by_year = None

    final_df = engineer_features(final_df, holiday_dates, thanksgiving_by_year)

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