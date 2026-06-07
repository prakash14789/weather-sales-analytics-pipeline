# Power BI Dashboard

This directory is dedicated to the Power BI dashboard for the **Retail Holiday Sales Analytics Pipeline**.

## Getting Started

### 1. Data Source Options
You can configure Power BI to fetch data in two ways:
* **Option A: PostgreSQL Database (Recommended for Live Queries)**
  Connect directly to the PostgreSQL `retail_dw` database to query tables like `customer_dim` and `customers`.
  * **Server**: `localhost` (or your DB host)
  * **Database**: `retail_dw`
  * **Authentication**: Database credentials configured in your `.env` file.

* **Option B: Integrated CSV Data (Recommended for Spark Output)**
  Load the integrated, feature-engineered CSV files directly from:
  `data/processed/final_analytics/`

### 2. How to Save Your Work Here
* **Power BI Project (`.pbip`)**: In Power BI Desktop, go to **File > Save As** and select **Power BI Project (*.pbip)**. Save it directly within this folder. This allows Git to track the report and semantic model metadata as text files.
* **Power BI Report Template (`.pbit`)**: You can export the template structure without embedding raw data by saving it as a `.pbit` file here.
* **Power BI Desktop Document (`.pbix`)**: Save the standard binary file here if developer mode is not preferred.

## Dashboard Metrics & Features
The feature-engineered dataset includes fields designed to highlight:
- **Holiday Impact**: `holiday_flag` and `holiday_name` to isolate holiday sales volume.
- **Seasonality & Time Analysis**: `month`, `year`, `quarter`, and `weekend_flag` to track cycles and seasonal trends.
- **Profitability**: `profit_margin` to identify shifts in profitability during holiday campaigns.
