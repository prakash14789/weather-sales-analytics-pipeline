from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Holiday Analytics Pipeline")
    .master("local[1]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

sales_df = spark.read.csv(
    "data/raw/csv/Sample - Superstore.csv",
    header=True,
    inferSchema=True
)

print("\nSchema:")
sales_df.printSchema()

print("\nRow Count:")
print(sales_df.count())

print("\nSample Data:")
sales_df.show(5)

spark.stop()