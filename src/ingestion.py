import os
import json
import requests

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

REGIONS = {
    "Central": {"lat": 41.8781, "lon": -87.6298, "city": "Chicago"},
    "East": {"lat": 40.7128, "lon": -74.0060, "city": "New York"},
    "South": {"lat": 33.7490, "lon": -84.3880, "city": "Atlanta"},
    "West": {"lat": 34.0522, "lon": -118.2437, "city": "Los Angeles"}
}

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
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return response.status_code, response.json()
        return response.status_code, None
    except Exception as e:
        print(f"Error fetching holidays: {e}")
        return 500, None

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

def run_holiday_ingestion(api_key, years=(2014, 2015, 2016, 2017), output_path="data/raw/api/holidays.json"):
    """
    Orchestrates the full holiday ingestion process for specified years.
    """
    if not api_key:
        print("Warning: No CALENDARIFIC_API_KEY set. Ingestion may fail.")
        
    all_holidays = []

    for year in years:
        print(f"\nFetching holidays for {year}...")
        status_code, data = fetch_holidays_from_api(api_key, year)
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

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(output, file, indent=4, ensure_ascii=False)

    print("\n===================================")
    print(f"Total Sales Holidays Saved: {len(all_holidays)}")
    print(f"Saved to: {output_path}")
    print("===================================")
    return len(all_holidays) > 0


def fetch_weather_for_region(lat, lon, start_date="2014-01-01", end_date="2017-12-31"):
    """
    Fetches raw historical daily weather from Open-Meteo Archive API.
    """
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}"
        f"&longitude={lon}"
        f"&start_date={start_date}"
        f"&end_date={end_date}"
        f"&daily=temperature_2m_mean,precipitation_sum,snowfall_sum,wind_speed_10m_max"
        f"&timezone=America%2FNew_York"
    )
    try:
        response = requests.get(url, timeout=30)
        return response.status_code, response.json() if response.status_code == 200 else None
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return 500, None

def format_weather_response(region_name, response_json):
    """
    Restructures the nested time-series daily data from Open-Meteo into
    a list of flat records.
    """
    if not response_json or "daily" not in response_json:
        return []
    
    daily = response_json["daily"]
    times = daily.get("time", [])
    temp_means = daily.get("temperature_2m_mean", [])
    precip_sums = daily.get("precipitation_sum", [])
    snow_sums = daily.get("snowfall_sum", [])
    wind_maxs = daily.get("wind_speed_10m_max", [])

    records = []
    for i in range(len(times)):
        records.append({
            "weather_region": region_name,
            "weather_date": times[i],
            "temp_c": temp_means[i] if i < len(temp_means) else None,
            "precipitation_mm": precip_sums[i] if i < len(precip_sums) else None,
            "snowfall_cm": snow_sums[i] if i < len(snow_sums) else None,
            "wind_speed_kmh": wind_maxs[i] if i < len(wind_maxs) else None
        })
    return records

def run_weather_ingestion(regions=REGIONS, start_date="2014-01-01", end_date="2017-12-31", output_path="data/raw/api/weather.json"):
    """
    Orchestrates the full weather ingestion process.
    """
    all_weather_records = []
    
    for region_name, info in regions.items():
        print(f"Fetching weather for {region_name} ({info['city']})...")
        status_code, data = fetch_weather_for_region(info["lat"], info["lon"], start_date, end_date)
        print(f"Status Code ({region_name}): {status_code}")
        
        if status_code == 200 and data:
            records = format_weather_response(region_name, data)
            print(f"Formatted {len(records)} daily records for {region_name}")
            all_weather_records.extend(records)
        else:
            print(f"Failed to fetch weather for region {region_name}")

    if all_weather_records:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(all_weather_records, f, indent=4, ensure_ascii=False)
        print(f"\nSaved {len(all_weather_records)} weather records to {output_path}")
        return True
    else:
        print("No weather records were fetched. File not written.")
        return False
