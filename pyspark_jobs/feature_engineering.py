import sys
import os

# Ensure project root is in path
sys.path.append(os.path.abspath("."))

# Import all functions from src.spark to maintain complete backward-compatibility
# for existing unit tests (e.g. tests/test_pyspark.py)
from src.spark import (
    clean_sales_data,
    prepare_holidays,
    join_sales_customers,
    join_sales_holidays,
    join_sales_weather,
    load_holiday_metadata_from_json,
    engineer_features,
    run_feature_engineering
)

def main():
    print("[DEPRECATION WARNING] Running feature_engineering.py directly is deprecated.")
    print("Please use: python run_pipeline.py --step spark-etl")
    print("Delegating to src.spark.run_feature_engineering...\n")
    run_feature_engineering()

if __name__ == "__main__":
    main()