import requests
import json
import os

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("CALENDARIFIC_API_KEY")

url = (
    f"https://calendarific.com/api/v2/holidays"
    f"?api_key={API_KEY}"
    f"&country=US"
    f"&year=2017"
)

response = requests.get(url)

print("Status Code:", response.status_code)

if response.status_code == 200:

    holiday_data = response.json()

    with open(
        "data/raw/api/holidays.json",
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            holiday_data,
            file,
            indent=4,
            ensure_ascii=False
        )

    print("Holiday data saved successfully!")

else:
    print("Failed to fetch data")
    print(response.text)