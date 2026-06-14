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
    all_holidays = []

    for year in years:
        print(f"\nFetching holidays for {year}...")
        status_code, data = fetch_holidays_from_api(API_KEY, year)
        print(f"Status Code ({year}):", status_code)

        if status_code == 200 and data:
            holidays = data.get("response", {}).get("holidays", [])
            unique = filter_and_deduplicate(holidays)
            print(f"{year}: {len(unique)} sales holidays found")
            for h in unique:
                print(f"  - {h['date']['iso']}  {h['name']}")
            all_holidays.extend(unique)
        else:
            print(f"Failed for year {year}")
            if data:
                print(data)

    output = {
        "holidays": all_holidays
    }

    os.makedirs("data/raw/api", exist_ok=True)
    with open(
        "data/raw/api/holidays.json",
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            output,
            file,
            indent=4,
            ensure_ascii=False
        )

    print("\n===================================")
    print(f"Total Sales Holidays Saved: {len(all_holidays)}")
    print("Saved to: data/raw/api/holidays.json")
    print("===================================")

if __name__ == "__main__":
    main()