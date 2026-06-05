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

all_holidays = []

for year in years:

    print(f"\nFetching holidays for {year}...")

    url = (
        f"https://calendarific.com/api/v2/holidays"
        f"?api_key={API_KEY}"
        f"&country=US"
        f"&year={year}"
    )

    response = requests.get(url)

    print(f"Status Code ({year}):", response.status_code)

    if response.status_code == 200:

        data = response.json()

        holidays = data["response"]["holidays"]

        # Filter: keep only sales-impacting holidays
        filtered = [
            h for h in holidays
            if h["name"] in SALES_HOLIDAYS
        ]

        # Remove duplicates (same holiday appears
        # for multiple states like Veterans Day /
        # Veterans Day substitute)
        seen = set()
        unique = []
        for h in filtered:
            key = (h["name"], h["date"]["iso"])
            if key not in seen:
                seen.add(key)
                unique.append(h)

        print(f"{year}: {len(unique)} sales holidays found")

        for h in unique:
            print(f"  - {h['date']['iso']}  {h['name']}")

        all_holidays.extend(unique)

    else:

        print(f"Failed for year {year}")
        print(response.text)

output = {
    "holidays": all_holidays
}

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