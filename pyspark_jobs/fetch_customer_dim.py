import sys
import os

# Ensure project root is in path
sys.path.append(os.path.abspath("."))

def main():
    print("[DEPRECATION WARNING] Running fetch_customer_dim.py directly is deprecated.")
    print("Please use: python run_pipeline.py --step export-customer-dim")
    print("Delegating to src.database.fetch_customer_dim...\n")
    from src.database import fetch_customer_dim
    fetch_customer_dim()

if __name__ == "__main__":
    main()
