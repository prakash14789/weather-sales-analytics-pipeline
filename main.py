import pandas as pd

df = pd.read_csv(
    "data/raw/csv/Sample - Superstore.csv",
    encoding="latin1"
)

customers = df[
    [
        "Customer ID",
        "Customer Name",
        "Segment",
        "Country",
        "City",
        "State",
        "Postal Code",
        "Region"
    ]
].drop_duplicates()

print("Customer Records:", len(customers))

customers.to_csv(
    "data/processed/customers.csv",
    index=False
)

print("customers.csv created successfully")