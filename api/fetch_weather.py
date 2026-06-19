import requests
import json
import os

REGIONS = {
    "Central": {"lat": 41.8781, "lon": -87.6298, "city": "Chicago"},
    "East": {"lat": 40.7128, "lon": -74.0060, "city": "New York"},
    "South": {"lat": 33.7490, "lon": -84.3880, "city": "Atlanta"},
    "West": {"lat": 34.0522, "lon": -118.2437, "city": "Los Angeles"}
}

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
        print(f"Error fetching data: {e}")
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

def main():
    print("[DEPRECATION WARNING] Running this script directly is deprecated.")
    print("Please use: python run_pipeline.py --step ingest")
    print("Delegating to src.ingestion.run_weather_ingestion...\n")
    from src.ingestion import run_weather_ingestion
    run_weather_ingestion(REGIONS)

if __name__ == "__main__":
    main()
