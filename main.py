import sys
import os

# Ensure project root is in path
sys.path.append(os.path.abspath("."))

def main():
    print("[DEPRECATION WARNING] Running main.py directly is deprecated.")
    print("Please use: python run_pipeline.py --step extract-customers")
    print("Delegating to src.database.extract_customers...\n")
    from src.database import extract_customers
    extract_customers()

if __name__ == "__main__":
    main()