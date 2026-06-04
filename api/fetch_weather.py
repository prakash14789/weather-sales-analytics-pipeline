import requests
import json

url = "https://api.open-meteo.com/v1/forecast?latitude=28.61&longitude=77.23&daily=temperature_2m_max&forecast_days=7"

response = requests.get(url)

print("Status Code:", response.status_code)

weather_data = response.json()

with open(
    "data/raw/api/weather.json",
    "w",
    encoding="utf-8"
) as file:
    json.dump(weather_data, file, indent=4)

print("Weather data saved successfully!")