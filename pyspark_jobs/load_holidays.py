# from pyspark.sql import SparkSession

# spark = (
#     SparkSession.builder
#     .master("local[1]")
#     .appName("Holiday Analytics")
#     .config("spark.driver.host", "127.0.0.1")
#     .config("spark.driver.bindAddress", "127.0.0.1")
#     .getOrCreate()
# )

# holiday_df = (
#     spark.read
#     .option("multiline", "true")
#     .json("data/raw/api/holidays.json")
# )

# print("\nSchema:")
# holiday_df.printSchema()

# print("\nData:")
# holiday_df.show(truncate=False)

# spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

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

# Extract holiday array
holidays = holiday_df.select(
    explode("response.holidays").alias("holiday")
)

# Flatten fields
holidays_flat = holidays.select(
    "holiday.name",
    "holiday.date.iso"
)

print("\nHoliday Count:")
print(holidays_flat.count())

print("\nSchema:")
holidays_flat.printSchema()

print("\nSample Holidays:")
holidays_flat.show(10, truncate=False)

spark.stop()