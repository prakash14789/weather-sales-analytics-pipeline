import sys
import os

# Ensure project root is in path
sys.path.append(os.path.abspath("."))

def main():
    print("[DEPRECATION WARNING] Running load_final_analytics_to_postgres.py directly is deprecated.")
    print("Please use: python run_pipeline.py --step load-analytics")
    print("Delegating to src.database.load_final_analytics_to_postgres...\n")
    from src.database import load_final_analytics_to_postgres
    load_final_analytics_to_postgres()

if __name__ == "__main__":
    main()
