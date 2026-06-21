# Import the actual implementations from src.ingestion to avoid duplication
# and keep backward compatibility for imports (e.g. in tests)
from src.ingestion import (
    REGIONS,
    fetch_weather_for_region,
    format_weather_response,
    run_weather_ingestion
)

def main():
    print("[DEPRECATION WARNING] Running this script directly is deprecated.")
    print("Please use: python run_pipeline.py --step ingest")
    print("Delegating to src.ingestion.run_weather_ingestion...\n")
    run_weather_ingestion(REGIONS)

if __name__ == "__main__":
    main()
