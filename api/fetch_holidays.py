import requests
import json
import os

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("CALENDARIFIC_API_KEY")

years = [2014, 2015, 2016, 2017]

# Holidays that actually impact retail sales
SALES_HOLIDAYS = [
    # Federal Holidays
    "New Year's Day",
    "Martin Luther King Jr. Day",
    "Presidents' Day",
    "Memorial Day",
    "Independence Day",
    "Labor Day",
    "Columbus Day",
    "Veterans Day",
    "Thanksgiving Day",
    "Christmas Day",

    # Key Observances that drive shopping
    "Valentine's Day",
    "Mother's Day",
    "Father's Day",
]

def fetch_holidays_from_api(api_key, year):
    """
    Fetches raw holidays from Calendarific API for the given year.
    Returns a tuple of (status_code, json_response_dict_or_none).
    """
    url = (
        f"https://calendarific.com/api/v2/holidays"
        f"?api_key={api_key}"
        f"&country=US"
        f"&year={year}"
    )
    response = requests.get(url)
    if response.status_code == 200:
        return response.status_code, response.json()
    return response.status_code, None

def filter_and_deduplicate(holidays, sales_holidays=SALES_HOLIDAYS):
    """
    Filters holidays keeping only those in sales_holidays, and removes duplicates
    based on (name, date_iso).
    """
    filtered = [
        h for h in holidays
        if h.get("name") in sales_holidays
    ]
    seen = set()
    unique = []
    for h in filtered:
        date_obj = h.get("date", {})
        iso_date = date_obj.get("iso")
        name = h.get("name")
        if not iso_date or not name:
            continue
        key = (name, iso_date)
        if key not in seen:
            seen.add(key)
            unique.append(h)
    return unique

def main():
    print("[DEPRECATION WARNING] Running this script directly is deprecated.")
    print("Please use: python run_pipeline.py --step ingest")
    print("Delegating to src.ingestion.run_holiday_ingestion...\n")
    from src.ingestion import run_holiday_ingestion
    run_holiday_ingestion(API_KEY, years)

if __name__ == "__main__":
    main()