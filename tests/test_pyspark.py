import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    ArrayType,
    DateType
)

from pyspark_jobs.feature_engineering import (
    clean_sales_data,
    prepare_holidays,
    join_sales_customers,
    join_sales_holidays,
    engineer_features
)

@pytest.fixture(scope="session")
def spark_session():
    """Initializes a shared SparkSession for the test suite."""
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("PySpark Unit Test Suite")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_clean_sales_data(spark_session):
    """Verifies date string parsing and product name cleanup logic."""
    schema = StructType([
        StructField("Order Date", StringType(), True),
        StructField("Product Name", StringType(), True)
    ])
    
    raw_data = [
        ("11/27/2014", "Product, Name with Comma"),
        ("12/25/2017", "Product Name with \u0093Quotes\u0094"),
        ("01/01/2015", "Product Name with\u00a0NonBreakingSpace")
    ]
    
    df = spark_session.createDataFrame(raw_data, schema)
    cleaned_df = clean_sales_data(df)
    results = cleaned_df.collect()
    
    # Assert Order Date is correctly converted to date types
    assert results[0]["Order Date"] == date(2014, 11, 27)
    assert results[1]["Order Date"] == date(2017, 12, 25)
    assert results[2]["Order Date"] == date(2015, 1, 1)
    
    # Assert Product Name is cleaned up correctly
    assert results[0]["Product Name"] == "Product - Name with Comma"
    assert results[1]["Product Name"] == 'Product Name with "Quotes"'
    assert results[2]["Product Name"] == "Product Name with NonBreakingSpace"

def test_prepare_holidays(spark_session):
    """Verifies that prepare_holidays flattens and parses the API response."""
    holiday_struct_type = StructType([
        StructField("name", StringType(), True),
        StructField("date", StructType([
            StructField("iso", StringType(), True)
        ]), True)
    ])
    outer_schema = StructType([
        StructField("holidays", ArrayType(holiday_struct_type), True)
    ])
    
    raw_data = [
        ([
            {"name": "New Year's Day", "date": {"iso": "2014-01-01"}},
            {"name": "Christmas Day", "date": {"iso": "2014-12-25"}}
        ],)
    ]
    
    df = spark_session.createDataFrame(raw_data, outer_schema)
    prepared_df = prepare_holidays(df)
    results = prepared_df.orderBy("holiday_date").collect()
    
    assert len(results) == 2
    assert results[0]["holiday_name"] == "New Year's Day"
    assert results[0]["holiday_date"] == date(2014, 1, 1)
    assert results[1]["holiday_name"] == "Christmas Day"
    assert results[1]["holiday_date"] == date(2014, 12, 25)

def test_join_sales_customers(spark_session):
    """Verifies customer dimension left join and column aliasing."""
    sales_schema = StructType([
        StructField("Customer ID", StringType(), True),
        StructField("Order ID", StringType(), True)
    ])
    sales_data = [
        ("CUST-1", "ORD-1"),
        ("CUST-2", "ORD-2")
    ]
    
    cust_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("region", StringType(), True)
    ])
    cust_data = [
        ("CUST-1", "John Doe", "Corporate", "New York", "NY", "East")
    ]
    
    sales_df = spark_session.createDataFrame(sales_data, sales_schema)
    customers_df = spark_session.createDataFrame(cust_data, cust_schema)
    
    joined_df = join_sales_customers(sales_df, customers_df)
    results = joined_df.collect()
    
    # Assert CUST-1 joined correctly and columns aliased
    cust1_row = [r for r in results if r["Order ID"] == "ORD-1"][0]
    assert cust1_row["db_customer_name"] == "John Doe"
    assert cust1_row["db_segment"] == "Corporate"
    assert cust1_row["db_region"] == "East"
    
    # Assert CUST-2 joins with null values (left join)
    cust2_row = [r for r in results if r["Order ID"] == "ORD-2"][0]
    assert cust2_row["db_customer_name"] is None
    
    # Verify dropped customer_id
    assert "customer_id" not in joined_df.columns

def test_join_sales_holidays(spark_session):
    """Verifies sales and holidays join on date."""
    sales_schema = StructType([
        StructField("Order Date", DateType(), True),
        StructField("Sales", DoubleType(), True)
    ])
    sales_data = [
        (date(2014, 11, 27), 100.0),
        (date(2014, 11, 28), 150.0)
    ]
    
    holidays_schema = StructType([
        StructField("holiday_name", StringType(), True),
        StructField("holiday_date", DateType(), True)
    ])
    holidays_data = [
        ("Thanksgiving Day", date(2014, 11, 27))
    ]
    
    sales_df = spark_session.createDataFrame(sales_data, sales_schema)
    holidays_df = spark_session.createDataFrame(holidays_data, holidays_schema)
    
    joined_df = join_sales_holidays(sales_df, holidays_df)
    results = joined_df.collect()
    
    row_holiday = [r for r in results if r["Order Date"] == date(2014, 11, 27)][0]
    row_non_holiday = [r for r in results if r["Order Date"] == date(2014, 11, 28)][0]
    
    assert row_holiday["holiday_name"] == "Thanksgiving Day"
    assert row_non_holiday["holiday_name"] is None

def test_engineer_features(spark_session):
    """Verifies features calculation like profit margin, holiday flag and weekend flag."""
    schema = StructType([
        StructField("holiday_name", StringType(), True),
        StructField("Order Date", DateType(), True),
        StructField("Sales", DoubleType(), True),
        StructField("Profit", DoubleType(), True)
    ])
    
    # 2014-11-27 is a Thursday (Thanksgiving)
    # 2014-11-28 is a Friday (Regular day)
    # 2014-11-30 is a Sunday (Weekend)
    raw_data = [
        ("Thanksgiving Day", date(2014, 11, 27), 100.0, 25.0),
        (None, date(2014, 11, 28), 200.0, -10.00),
        (None, date(2014, 11, 30), 50.0, 10.15)
    ]
    
    df = spark_session.createDataFrame(raw_data, schema)
    engineered_df = engineer_features(df)
    results = engineered_df.collect()
    
    # Verify Holiday Flag
    assert results[0]["holiday_flag"] == 1
    assert results[1]["holiday_flag"] == 0
    assert results[2]["holiday_flag"] == 0
    
    # Verify Month, Year, Quarter
    assert results[0]["month"] == 11
    assert results[0]["year"] == 2014
    assert results[0]["quarter"] == 4
    
    # Verify Weekend Flag
    # Thursday is weekday -> 0
    # Friday is weekday -> 0
    # Sunday is weekend -> 1
    assert results[0]["weekend_flag"] == 0
    assert results[1]["weekend_flag"] == 0
    assert results[2]["weekend_flag"] == 1
    
    # Verify Profit Margin
    assert results[0]["profit_margin"] == 25.0
    assert results[1]["profit_margin"] == -5.0
    assert results[2]["profit_margin"] == 20.30
