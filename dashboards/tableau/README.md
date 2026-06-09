# Tableau Dashboard

This directory is dedicated to the Tableau dashboard for the **Retail Holiday Sales Analytics Pipeline**.

## Getting Started

### 1. Data Source Options
You can configure Tableau to fetch data in two ways:
* **Option A: PostgreSQL Database (Recommended for Live Queries)**
  Connect directly to the PostgreSQL `retail_dw` database to query tables like `customer_dim` and `customers`.
  * **Server**: `localhost` (or your DB host)
  * **Port**: `5432`
  * **Database**: `retail_dw`
  * **Authentication**: Database credentials configured in your `.env` file.

* **Option B: Integrated CSV Data (Recommended for Spark Output)**
  Load the integrated, feature-engineered CSV files directly from:
  `data/processed/final_analytics/`

### 2. How to Save Your Work Here
* **Tableau Packaged Workbook (`.twbx`)**: Save your Tableau packaged workbook directly in this directory. Packaged workbooks contain the workbook visual layout along with a snapshot of any local file data sources (like the exported CSVs).
* **Tableau Workbook (`.twb`)**: Save the standard XML-based workbook file here if you want to connect to a live PostgreSQL database and do not want to bundle the data.

## Dashboard Metrics & Features
The feature-engineered dataset includes fields designed to highlight:
- **Holiday Impact**: `holiday_flag` and `holiday_name` to isolate holiday sales volume.
- **Seasonality & Time Analysis**: `month`, `year`, `quarter`, and `weekend_flag` to track cycles and seasonal trends.
- **Profitability**: `profit_margin` to identify shifts in profitability during holiday campaigns.
