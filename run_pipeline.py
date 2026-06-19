import argparse
import sys
import os
import time

# Ensure project root is in path for imports
sys.path.append(os.path.abspath("."))

from src.config import CALENDARIFIC_API_KEY
from src.ingestion import run_holiday_ingestion, run_weather_ingestion
from src.database import (
    extract_customers,
    load_customers_to_postgres,
    load_final_analytics_to_postgres,
    fetch_customer_dim
)
from src.spark import run_feature_engineering

def print_separator(char="=", length=60):
    print(char * length)

def step_wrapper(step_name, func, *args, **kwargs):
    print_separator()
    print(f">>> Starting Pipeline Step: {step_name}")
    print_separator("-")
    start_time = time.time()
    try:
        result = func(*args, **kwargs)
        duration = time.time() - start_time
        if result:
            print_separator("-")
            print(f"SUCCESS: Step '{step_name}' completed in {duration:.2f} seconds.")
            print_separator()
            return True
        else:
            print_separator("-")
            print(f"FAILED: Step '{step_name}' completed in {duration:.2f} seconds, but returned failure.")
            print_separator()
            return False
    except Exception as e:
        duration = time.time() - start_time
        print_separator("-")
        print(f"ERROR: Step '{step_name}' encountered an exception after {duration:.2f} seconds.")
        print(f"Details: {e}")
        import traceback
        traceback.print_exc()
        print_separator()
        return False

def main():
    parser = argparse.ArgumentParser(description="Retail Holiday Sales Analytics Pipeline Orchestrator")
    parser.add_argument(
        "--step",
        choices=[
            "all",
            "ingest",
            "extract-customers",
            "load-customers",
            "spark-etl",
            "load-analytics",
            "export-customer-dim"
        ],
        default="all",
        help="Specify the pipeline step to run. Defaults to 'all' (runs everything sequentially)."
    )
    
    args = parser.parse_args()
    
    steps_to_run = []
    
    if args.step == "all":
        steps_to_run = [
            ("ingest-holidays", run_holiday_ingestion, CALENDARIFIC_API_KEY),
            ("ingest-weather", run_weather_ingestion,),
            ("extract-customers", extract_customers,),
            ("load-customers", load_customers_to_postgres,),
            ("spark-etl", run_feature_engineering,),
            ("load-analytics", load_final_analytics_to_postgres,)
        ]
    elif args.step == "ingest":
        steps_to_run = [
            ("ingest-holidays", run_holiday_ingestion, CALENDARIFIC_API_KEY),
            ("ingest-weather", run_weather_ingestion,)
        ]
    elif args.step == "extract-customers":
        steps_to_run = [("extract-customers", extract_customers,)]
    elif args.step == "load-customers":
        steps_to_run = [("load-customers", load_customers_to_postgres,)]
    elif args.step == "spark-etl":
        steps_to_run = [("spark-etl", run_feature_engineering,)]
    elif args.step == "load-analytics":
        steps_to_run = [("load-analytics", load_final_analytics_to_postgres,)]
    elif args.step == "export-customer-dim":
        steps_to_run = [("export-customer-dim", fetch_customer_dim,)]

    print_separator("#")
    print(f"Executing step(s): {[s[0] for s in steps_to_run]}")
    print_separator("#")
    
    overall_start = time.time()
    for step_name, func, *step_args in steps_to_run:
        success = step_wrapper(step_name, func, *step_args)
        if not success:
            print(f"\nPipeline execution aborted due to failure in step '{step_name}'.")
            sys.exit(1)
            
    overall_duration = time.time() - overall_start
    print_separator("#")
    print(f"Pipeline executed successfully in {overall_duration:.2f} seconds.")
    print_separator("#")

if __name__ == "__main__":
    main()
