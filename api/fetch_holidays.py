import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("CALENDARIFIC_API_KEY")
years = [2014, 2015, 2016, 2017]

# Import the actual implementations from src.ingestion to avoid duplication
# and keep backward compatibility for imports (e.g. in tests)
from src.ingestion import (
    SALES_HOLIDAYS,
    fetch_holidays_from_api,
    filter_and_deduplicate,
    run_holiday_ingestion
)

def main():
    print("[DEPRECATION WARNING] Running this script directly is deprecated.")
    print("Please use: python run_pipeline.py --step ingest")
    print("Delegating to src.ingestion.run_holiday_ingestion...\n")
    run_holiday_ingestion(API_KEY, years)

if __name__ == "__main__":
    main()