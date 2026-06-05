from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("Holiday Analytics")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

holiday_df = (
    spark.read
    .option("multiline", "true")
    .json("data/raw/api/holidays.json")
)

holidays = holiday_df.select(
    explode("holidays").alias("holiday")
)

clean_holidays = holidays.select(
    col("holiday.name").alias("holiday_name"),
    col("holiday.date.iso").alias("holiday_date"),
    col("holiday.primary_type").alias("holiday_type")
)

clean_holidays.show(20, truncate=False)

print("\nHoliday Count:")
print(clean_holidays.count())

spark.stop()