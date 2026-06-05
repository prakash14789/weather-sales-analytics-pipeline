from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("Holiday Analytics")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

sales_df = spark.read.csv(
    "data/raw/csv/Sample - Superstore.csv",
    header=True,
    inferSchema=True,
    escape='"'
)

# Convert dates
sales_df = sales_df.withColumn(
    "Order Date",
    to_date(col("Order Date"), "M/d/yyyy")
)

sales_df = sales_df.withColumn(
    "Ship Date",
    to_date(col("Ship Date"), "M/d/yyyy")
)

# Convert numeric columns
sales_df = sales_df.withColumn(
    "Sales",
    col("Sales").cast("double")
)

sales_df = sales_df.withColumn(
    "Quantity",
    col("Quantity").cast("int")
)

sales_df = sales_df.withColumn(
    "Discount",
    col("Discount").cast("double")
)

print("\nSchema After Cleaning:")
sales_df.printSchema()

print("\nRow Count:")
print(sales_df.count())
print("\nMissing Values:")

for column in sales_df.columns:
    missing_count = sales_df.filter(
        col(column).isNull()
    ).count()

    print(f"{column}: {missing_count}")

print("\nDuplicate Rows:")
print(
    sales_df.count() -
    sales_df.dropDuplicates().count()
)

spark.stop()