import os
import json
import glob
import datetime
import pandas as pd
import numpy as np

class DatasetValidator:
    """
    A lightweight, high-performance data validator inspired by the Great Expectations API.
    Runs validation rules on Pandas DataFrames and collects rich execution metadata.
    """
    def __init__(self, df, dataset_name):
        self.df = df
        self.dataset_name = dataset_name
        self.results = []
        self.total_checks = 0
        self.passed_checks = 0

    def _record_result(self, success, expectation_type, kwargs, result_data):
        self.total_checks += 1
        if success:
            self.passed_checks += 1
        
        self.results.append({
            "expectation_type": expectation_type,
            "kwargs": kwargs,
            "success": success,
            "result": result_data
        })
        return success

    def expect_table_columns_to_match_set(self, expected_columns, ordered=False):
        kwargs = {"expected_columns": list(expected_columns), "ordered": ordered}
        observed_cols = list(self.df.columns)
        
        if ordered:
            success = observed_cols == list(expected_columns)
        else:
            success = set(expected_columns).issubset(set(observed_cols))
            
        result_data = {
            "observed_columns": observed_cols,
            "expected_columns": list(expected_columns),
            "missing_columns": list(set(expected_columns) - set(observed_cols))
        }
        return self._record_result(success, "expect_table_columns_to_match_set", kwargs, result_data)

    def expect_column_to_exist(self, column):
        kwargs = {"column": column}
        success = column in self.df.columns
        result_data = {
            "columns": list(self.df.columns)
        }
        return self._record_result(success, "expect_column_to_exist", kwargs, result_data)

    def expect_column_values_to_not_be_null(self, column):
        kwargs = {"column": column}
        if column not in self.df.columns:
            return self._record_result(False, "expect_column_values_to_not_be_null", kwargs, {"error": "Column not found"})
        
        null_count = int(self.df[column].isnull().sum())
        total_count = len(self.df)
        success = null_count == 0
        
        result_data = {
            "element_count": total_count,
            "unexpected_count": null_count,
            "unexpected_percent": round((null_count / total_count) * 100, 2) if total_count > 0 else 0.0,
            "unexpected_values": [] # null values are inherently empty/None
        }
        return self._record_result(success, "expect_column_values_to_not_be_null", kwargs, result_data)

    def expect_column_values_to_be_of_type(self, column, expected_type):
        kwargs = {"column": column, "expected_type": expected_type}
        if column not in self.df.columns:
            return self._record_result(False, "expect_column_values_to_be_of_type", kwargs, {"error": "Column not found"})
        
        observed_dtype = str(self.df[column].dtype)
        success = False
        
        if expected_type == "numeric":
            success = pd.api.types.is_numeric_dtype(self.df[column])
        elif expected_type == "integer":
            success = pd.api.types.is_integer_dtype(self.df[column])
        elif expected_type == "string" or expected_type == "object":
            success = pd.api.types.is_object_dtype(self.df[column]) or pd.api.types.is_string_dtype(self.df[column])
        elif expected_type == "datetime":
            success = pd.api.types.is_datetime64_any_dtype(self.df[column]) or observed_dtype.startswith("datetime")
        elif expected_type == "boolean":
            success = pd.api.types.is_bool_dtype(self.df[column])
            
        result_data = {
            "observed_type": observed_dtype,
            "expected_type": expected_type
        }
        return self._record_result(success, "expect_column_values_to_be_of_type", kwargs, result_data)

    def expect_column_values_to_be_between(self, column, min_val, max_val, parse_date=False):
        kwargs = {"column": column, "min_val": str(min_val), "max_val": str(max_val)}
        if column not in self.df.columns:
            return self._record_result(False, "expect_column_values_to_be_between", kwargs, {"error": "Column not found"})
        
        series = self.df[column].dropna()
        if parse_date:
            try:
                series = pd.to_datetime(series, errors="coerce").dropna()
                min_dt = pd.to_datetime(min_val)
                max_dt = pd.to_datetime(max_val)
                out_of_bounds = series[(series < min_dt) | (series > max_dt)]
            except Exception as e:
                return self._record_result(False, "expect_column_values_to_be_between", kwargs, {"error": f"Date parsing failed: {e}"})
        else:
            out_of_bounds = series[(series < min_val) | (series > max_val)]
            
        unexpected_count = len(out_of_bounds)
        total_count = len(self.df)
        success = unexpected_count == 0
        
        # Capture up to 10 sample failures
        sample_failures = [str(x) for x in out_of_bounds.head(10).tolist()]
        
        result_data = {
            "element_count": total_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round((unexpected_count / total_count) * 100, 2) if total_count > 0 else 0.0,
            "unexpected_values": sample_failures
        }
        return self._record_result(success, "expect_column_values_to_be_between", kwargs, result_data)

    def expect_column_values_to_be_in_set(self, column, allowed_set):
        kwargs = {"column": column, "allowed_set": list(allowed_set)}
        if column not in self.df.columns:
            return self._record_result(False, "expect_column_values_to_be_in_set", kwargs, {"error": "Column not found"})
        
        series = self.df[column].dropna()
        invalid_values = series[~series.isin(allowed_set)]
        unexpected_count = len(invalid_values)
        total_count = len(self.df)
        success = unexpected_count == 0
        
        sample_failures = [str(x) for x in invalid_values.head(10).tolist()]
        
        result_data = {
            "element_count": total_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round((unexpected_count / total_count) * 100, 2) if total_count > 0 else 0.0,
            "unexpected_values": sample_failures
        }
        return self._record_result(success, "expect_column_values_to_be_in_set", kwargs, result_data)

    def expect_column_values_to_be_greater_than_or_equal_to(self, column, value):
        kwargs = {"column": column, "value": value}
        if column not in self.df.columns:
            return self._record_result(False, "expect_column_values_to_be_greater_than_or_equal_to", kwargs, {"error": "Column not found"})
        
        series = self.df[column].dropna()
        invalid_values = series[series < value]
        unexpected_count = len(invalid_values)
        total_count = len(self.df)
        success = unexpected_count == 0
        
        sample_failures = [str(x) for x in invalid_values.head(10).tolist()]
        
        result_data = {
            "element_count": total_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round((unexpected_count / total_count) * 100, 2) if total_count > 0 else 0.0,
            "unexpected_values": sample_failures
        }
        return self._record_result(success, "expect_column_values_to_be_greater_than_or_equal_to", kwargs, result_data)

    def expect_table_row_count_to_be_between(self, min_rows, max_rows):
        kwargs = {"min_rows": min_rows, "max_rows": max_rows}
        observed_count = len(self.df)
        success = min_rows <= observed_count <= max_rows
        
        result_data = {
            "observed_row_count": observed_count,
            "min_rows": min_rows,
            "max_rows": max_rows
        }
        return self._record_result(success, "expect_table_row_count_to_be_between", kwargs, result_data)


def run_validation_suite(
    raw_sales_path="data/raw/csv/Sample - Superstore.csv",
    raw_holidays_path="data/raw/api/holidays.json",
    raw_weather_path="data/raw/api/weather.json",
    final_analytics_pattern="data/processed/final_analytics/part-*.csv",
    output_report_path="data/processed/validation_report.html"
):
    """
    Orchestrates the data quality checks across all datasets and writes the HTML report.
    """
    print("\n" + "="*50)
    print("RUNNING DATA QUALITY & VALIDATION SUITE")
    print("="*50)
    
    validators = {}
    
    # 1. Validate Raw Sales Data
    print(f"Loading raw sales data from: {raw_sales_path}...")
    if os.path.exists(raw_sales_path):
        try:
            sales_df = pd.read_csv(raw_sales_path, encoding="latin1")
            v_sales = DatasetValidator(sales_df, "Raw Sales Dataset")
            
            # Check schema
            expected_cols = [
                "Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode", 
                "Customer ID", "Customer Name", "Segment", "Country", "City", 
                "State", "Postal Code", "Region", "Product ID", "Category", 
                "Sub-Category", "Product Name", "Sales", "Quantity", "Discount", "Profit"
            ]
            v_sales.expect_table_columns_to_match_set(expected_cols)
            v_sales.expect_table_row_count_to_be_between(5000, 15000)
            
            # Check crucial columns for nulls
            v_sales.expect_column_values_to_not_be_null("Order ID")
            v_sales.expect_column_values_to_not_be_null("Customer ID")
            v_sales.expect_column_values_to_not_be_null("Order Date")
            v_sales.expect_column_values_to_not_be_null("Sales")
            
            # Check boundaries and logic
            v_sales.expect_column_values_to_be_greater_than_or_equal_to("Sales", 0.0)
            v_sales.expect_column_values_to_be_greater_than_or_equal_to("Quantity", 1)
            v_sales.expect_column_values_to_be_between("Order Date", "2014-01-01", "2017-12-31", parse_date=True)
            
            validators["raw_sales"] = v_sales
            print(f"-> Checked {v_sales.total_checks} assertions. {v_sales.passed_checks} passed.")
        except Exception as e:
            print(f"Error loading/validating raw sales data: {e}")
    else:
        print(f"Warning: Raw sales data not found at {raw_sales_path}. Skipping.")

    # 2. Validate Raw Holidays JSON
    print(f"Loading raw holidays data from: {raw_holidays_path}...")
    if os.path.exists(raw_holidays_path):
        try:
            with open(raw_holidays_path, "r", encoding="utf-8") as f:
                holidays_data = json.load(f)
            
            # Flat mapping JSON array to pandas df
            holidays = holidays_data.get("holidays", [])
            flat_holidays = []
            for h in holidays:
                flat_holidays.append({
                    "holiday_name": h.get("name"),
                    "holiday_date": h.get("date", {}).get("iso")
                })
            
            holidays_df = pd.DataFrame(flat_holidays)
            v_hols = DatasetValidator(holidays_df, "Raw Holidays API JSON")
            
            v_hols.expect_table_columns_to_match_set(["holiday_name", "holiday_date"])
            v_hols.expect_column_values_to_not_be_null("holiday_name")
            v_hols.expect_column_values_to_not_be_null("holiday_date")
            v_hols.expect_column_values_to_be_between("holiday_date", "2014-01-01", "2017-12-31", parse_date=True)
            v_hols.expect_table_row_count_to_be_between(10, 150)
            
            validators["raw_holidays"] = v_hols
            print(f"-> Checked {v_hols.total_checks} assertions. {v_hols.passed_checks} passed.")
        except Exception as e:
            print(f"Error loading/validating holidays: {e}")
    else:
        print(f"Warning: Holidays JSON not found at {raw_holidays_path}. Skipping.")

    # 3. Validate Raw Weather JSON
    print(f"Loading raw weather data from: {raw_weather_path}...")
    if os.path.exists(raw_weather_path):
        try:
            weather_df = pd.read_json(raw_weather_path)
            v_weather = DatasetValidator(weather_df, "Raw Weather API JSON")
            
            v_weather.expect_table_columns_to_match_set([
                "weather_region", "weather_date", "temp_c", "precipitation_mm", "snowfall_cm", "wind_speed_kmh"
            ])
            v_weather.expect_column_values_to_not_be_null("weather_date")
            v_weather.expect_column_values_to_not_be_null("weather_region")
            v_weather.expect_column_values_to_be_in_set("weather_region", ["Central", "East", "South", "West"])
            v_weather.expect_column_values_to_be_between("temp_c", -50.0, 50.0)
            v_weather.expect_column_values_to_be_greater_than_or_equal_to("precipitation_mm", 0.0)
            v_weather.expect_column_values_to_be_greater_than_or_equal_to("snowfall_cm", 0.0)
            
            validators["raw_weather"] = v_weather
            print(f"-> Checked {v_weather.total_checks} assertions. {v_weather.passed_checks} passed.")
        except Exception as e:
            print(f"Error loading/validating weather: {e}")
    else:
        print(f"Warning: Weather JSON not found at {raw_weather_path}. Skipping.")

    # 4. Validate Final Analytics Output Data
    csv_files = glob.glob(final_analytics_pattern)
    print(f"Loading final analytics data from pattern: {final_analytics_pattern} (Found {len(csv_files)} files)...")
    if csv_files:
        try:
            df_list = []
            for f in csv_files:
                df_list.append(pd.read_csv(f))
            final_df = pd.concat(df_list, ignore_index=True)
            
            v_final = DatasetValidator(final_df, "Final Analytics Dataset")
            
            # Check schema of engineered columns
            expected_final_cols = [
                "Order Date", "Customer ID", "Sales", "Profit", "holiday_name", "holiday_flag", 
                "month", "year", "quarter", "weekend_flag", "profit_margin", "temp_c", 
                "precipitation_mm", "snowfall_cm", "wind_speed_kmh", "is_raining", "is_snowing", 
                "extreme_weather_flag", "days_until_next_holiday", "days_since_last_holiday", 
                "holiday_shopping_season_flag", "rolling_sales_7d", "rolling_sales_30d"
            ]
            v_final.expect_table_columns_to_match_set(expected_final_cols)
            
            # Numeric type check
            v_final.expect_column_values_to_be_of_type("Sales", "numeric")
            v_final.expect_column_values_to_be_of_type("Profit", "numeric")
            v_final.expect_column_values_to_be_of_type("holiday_flag", "integer")
            
            # Binary flag checks
            flags = [0, 1]
            v_final.expect_column_values_to_be_in_set("holiday_flag", flags)
            v_final.expect_column_values_to_be_in_set("weekend_flag", flags)
            v_final.expect_column_values_to_be_in_set("is_raining", flags)
            v_final.expect_column_values_to_be_in_set("is_snowing", flags)
            v_final.expect_column_values_to_be_in_set("extreme_weather_flag", flags)
            v_final.expect_column_values_to_be_in_set("holiday_shopping_season_flag", flags)
            
            # Boundary checks
            v_final.expect_column_values_to_be_between("month", 1, 12)
            v_final.expect_column_values_to_be_between("quarter", 1, 4)
            v_final.expect_column_values_to_be_between("year", 2014, 2017)
            v_final.expect_column_values_to_be_between("profit_margin", -1000.0, 100.0)
            v_final.expect_column_values_to_be_greater_than_or_equal_to("days_until_next_holiday", 0)
            v_final.expect_column_values_to_be_greater_than_or_equal_to("days_since_last_holiday", 0)
            
            # Moving averages populate checks (should not have nulls for active days)
            v_final.expect_column_values_to_not_be_null("rolling_sales_7d")
            
            validators["final_analytics"] = v_final
            print(f"-> Checked {v_final.total_checks} assertions. {v_final.passed_checks} passed.")
        except Exception as e:
            print(f"Error loading/validating final analytics: {e}")
    else:
        print(f"Warning: No processed final analytics CSV files found matching {final_analytics_pattern}. Skipping.")

    # 5. Generate HTML Report
    if validators:
        os.makedirs(os.path.dirname(output_report_path), exist_ok=True)
        generate_html_report(validators, output_report_path)
        print(f"\nSUCCESS: Validation suite finished. Report generated at: {output_report_path}")
        return True
    else:
        print("\nERROR: No datasets were validated. Report not written.")
        return False


def generate_html_report(validators, output_path):
    """
    Compiles validation metrics into a stunning, interactive HTML dashboard report
    following dark-mode, glassmorphism, and responsive card styling principles.
    """
    # Calculate overall metrics
    total_assertions = sum(v.total_checks for v in validators.values())
    total_passed = sum(v.passed_checks for v in validators.values())
    total_failed = total_assertions - total_passed
    pass_percent = round((total_passed / total_assertions) * 100, 1) if total_assertions > 0 else 0.0
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Generate SVG ring dash-offset for the premium ring widget
    dash_array = 314.159
    dash_offset = dash_array - (dash_array * (pass_percent / 100))
    
    # Theme color variables
    theme_color = "#10b981" if pass_percent >= 90.0 else ("#f59e0b" if pass_percent >= 75.0 else "#f43f5e")
    theme_glow = "rgba(16, 185, 129, 0.15)" if pass_percent >= 90.0 else ("rgba(245, 158, 11, 0.15)" if pass_percent >= 75.0 else "rgba(244, 63, 94, 0.15)")
    
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality & Validation Report</title>
    <!-- Google Fonts -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Outfit:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-dark: #0f172a;
            --bg-card: rgba(30, 41, 59, 0.7);
            --bg-card-hover: rgba(30, 41, 59, 0.95);
            --border-color: rgba(255, 255, 255, 0.08);
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --success: #34d399;
            --failure: #fb7185;
            --warning: #fbbf24;
            --info: #60a5fa;
            --accent-glow: {theme_glow};
            --accent-color: {theme_color};
        }}
        
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Inter', sans-serif;
            background-color: var(--bg-dark);
            color: var(--text-main);
            min-height: 100vh;
            padding: 2.5rem 1.5rem;
            line-height: 1.5;
            background-image: 
                radial-gradient(at 10% 20%, rgba(59, 130, 246, 0.08) 0px, transparent 50%),
                radial-gradient(at 90% 80%, rgba(16, 185, 129, 0.05) 0px, transparent 50%);
        }}

        .container {{
            max-width: 1100px;
            margin: 0 auto;
        }}

        /* Header Style */
        header {{
            display: flex;
            align-content: center;
            justify-content: space-between;
            align-items: center;
            padding: 2rem;
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            margin-bottom: 2rem;
            box-shadow: 0 10px 30px -15px rgba(0,0,0,0.3);
        }}

        .header-info h1 {{
            font-family: 'Outfit', sans-serif;
            font-weight: 700;
            font-size: 1.8rem;
            background: linear-gradient(135deg, #f8fafc 30%, var(--text-muted) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 0.5rem;
        }}

        .header-meta {{
            font-size: 0.85rem;
            color: var(--text-muted);
            display: flex;
            gap: 1.5rem;
        }}

        .header-meta span strong {{
            color: var(--text-main);
        }}

        /* Summary Widget Rings & Stats */
        .summary-dashboard {{
            display: grid;
            grid-template-columns: 320px 1fr;
            gap: 2rem;
            margin-bottom: 2rem;
        }}

        .gauge-card {{
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            backdrop-filter: blur(12px);
            padding: 2rem;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            text-align: center;
            position: relative;
            overflow: hidden;
            box-shadow: 0 10px 30px -15px rgba(0,0,0,0.3);
        }}

        .gauge-card::before {{
            content: '';
            position: absolute;
            width: 100px;
            height: 100px;
            background: var(--accent-color);
            filter: blur(60px);
            opacity: 0.15;
            top: -20px;
            right: -20px;
            border-radius: 50%;
        }}

        .progress-ring-container {{
            position: relative;
            width: 120px;
            height: 120px;
            margin-bottom: 1rem;
        }}

        .progress-ring {{
            transform: rotate(-90deg);
        }}

        .progress-ring__circle {{
            transition: stroke-dashoffset 0.35s;
            transform-origin: 50% 50%;
        }}

        .gauge-text {{
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            font-family: 'Outfit', sans-serif;
            font-size: 1.6rem;
            font-weight: 700;
            color: var(--text-main);
        }}

        .gauge-label {{
            font-family: 'Outfit', sans-serif;
            font-weight: 600;
            font-size: 1.1rem;
            color: var(--text-main);
            margin-top: 0.5rem;
        }}

        .gauge-status-desc {{
            font-size: 0.8rem;
            color: var(--text-muted);
            margin-top: 0.25rem;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 1.5rem;
        }}

        .stat-card {{
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 1.5rem;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            backdrop-filter: blur(12px);
            box-shadow: 0 10px 30px -15px rgba(0,0,0,0.3);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }}

        .stat-card:hover {{
            transform: translateY(-2px);
            border-color: rgba(255,255,255,0.15);
            background: var(--bg-card-hover);
        }}

        .stat-label {{
            font-size: 0.85rem;
            font-weight: 500;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}

        .stat-number-wrapper {{
            display: flex;
            align-items: baseline;
            gap: 0.5rem;
            margin-top: 1rem;
        }}

        .stat-number {{
            font-family: 'Outfit', sans-serif;
            font-size: 2.2rem;
            font-weight: 700;
        }}
        
        .stat-card.passed-stats .stat-number {{ color: var(--success); }}
        .stat-card.failed-stats .stat-number {{ color: var(--failure); }}
        .stat-card.total-stats .stat-number {{ color: var(--info); }}

        /* Tabs and Filters */
        .controls-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
            flex-wrap: wrap;
            gap: 1rem;
        }}

        .tabs {{
            display: flex;
            background: rgba(15, 23, 42, 0.6);
            border: 1px solid var(--border-color);
            padding: 0.3rem;
            border-radius: 12px;
            gap: 0.2rem;
        }}

        .tab-btn {{
            font-family: 'Inter', sans-serif;
            font-size: 0.85rem;
            font-weight: 500;
            color: var(--text-muted);
            background: transparent;
            border: none;
            padding: 0.5rem 1rem;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s ease;
        }}

        .tab-btn:hover {{
            color: var(--text-main);
        }}

        .tab-btn.active {{
            background: rgba(255, 255, 255, 0.07);
            color: var(--text-main);
            box-shadow: 0 4px 12px -2px rgba(0,0,0,0.2);
        }}

        .filter-status-buttons {{
            display: flex;
            gap: 0.5rem;
        }}

        .filter-status-btn {{
            font-size: 0.8rem;
            font-weight: 600;
            padding: 0.4rem 0.8rem;
            border-radius: 8px;
            cursor: pointer;
            border: 1px solid var(--border-color);
            background: rgba(30, 41, 59, 0.4);
            color: var(--text-muted);
            transition: all 0.2s ease;
        }}

        .filter-status-btn:hover {{
            color: var(--text-main);
            border-color: rgba(255,255,255,0.15);
        }}

        .filter-status-btn.active.all-btn {{ background: rgba(96, 165, 250, 0.15); color: var(--info); border-color: rgba(96, 165, 250, 0.3); }}
        .filter-status-btn.active.passed-btn {{ background: rgba(52, 211, 153, 0.15); color: var(--success); border-color: rgba(52, 211, 153, 0.3); }}
        .filter-status-btn.active.failed-btn {{ background: rgba(251, 113, 133, 0.15); color: var(--failure); border-color: rgba(251, 113, 133, 0.3); }}

        /* Expectations list */
        .expectations-list {{
            display: flex;
            flex-direction: column;
            gap: 1rem;
        }}

        .expectation-card {{
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            overflow: hidden;
            backdrop-filter: blur(12px);
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
        }}

        .expectation-card:hover {{
            border-color: rgba(255, 255, 255, 0.15);
            background: var(--bg-card-hover);
        }}

        .expectation-header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 1.25rem 1.5rem;
            cursor: pointer;
            user-select: none;
        }}

        .expectation-title-area {{
            display: flex;
            align-items: center;
            gap: 1rem;
        }}

        .status-badge {{
            font-size: 0.7rem;
            font-weight: 700;
            text-transform: uppercase;
            padding: 0.25rem 0.6rem;
            border-radius: 6px;
            letter-spacing: 0.05em;
        }}

        .status-badge.pass {{
            background: rgba(52, 211, 153, 0.15);
            color: var(--success);
            border: 1px solid rgba(52, 211, 153, 0.3);
        }}

        .status-badge.fail {{
            background: rgba(251, 113, 133, 0.15);
            color: var(--failure);
            border: 1px solid rgba(251, 113, 133, 0.3);
        }}

        .expectation-name {{
            font-family: 'Outfit', sans-serif;
            font-weight: 600;
            font-size: 0.95rem;
            color: var(--text-main);
        }}
        
        .expectation-column-tag {{
            font-family: monospace;
            font-size: 0.8rem;
            background: rgba(255, 255, 255, 0.05);
            padding: 0.15rem 0.4rem;
            border-radius: 4px;
            color: var(--text-muted);
            border: 1px solid rgba(255,255,255,0.02);
        }}

        .toggle-icon {{
            font-size: 0.8rem;
            color: var(--text-muted);
            transition: transform 0.3s ease;
        }}

        .expectation-card.open .toggle-icon {{
            transform: rotate(180deg);
        }}

        /* Expectation Details Body */
        .expectation-body {{
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            background: rgba(15, 23, 42, 0.3);
            border-top: 1px solid transparent;
        }}

        .expectation-card.open .expectation-body {{
            max-height: 1000px;
            border-top-color: var(--border-color);
        }}

        .expectation-content {{
            padding: 1.5rem;
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1.5rem;
        }}

        @media (max-width: 768px) {{
            .expectation-content {{
                grid-template-columns: 1fr;
            }}
            .summary-dashboard {{
                grid-template-columns: 1fr;
            }}
            .stats-grid {{
                grid-template-columns: 1fr;
            }}
        }}

        .info-panel h4 {{
            font-size: 0.8rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-muted);
            margin-bottom: 0.75rem;
        }}

        .details-list {{
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }}

        .details-item {{
            display: flex;
            justify-content: space-between;
            font-size: 0.85rem;
            border-bottom: 1px dashed rgba(255,255,255,0.04);
            padding-bottom: 0.25rem;
        }}

        .details-item span:first-child {{
            color: var(--text-muted);
        }}

        .details-item span:last-child {{
            font-weight: 500;
        }}

        .code-block {{
            background: rgba(15, 23, 42, 0.6);
            border: 1px solid var(--border-color);
            padding: 1rem;
            border-radius: 8px;
            font-family: monospace;
            font-size: 0.8rem;
            color: #38bdf8;
            overflow-x: auto;
            max-height: 180px;
        }}

        .sample-values-title {{
            color: var(--failure);
            font-size: 0.8rem;
            font-weight: 600;
            margin-top: 1rem;
            margin-bottom: 0.5rem;
            text-transform: uppercase;
        }}

        .sample-values {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.4rem;
        }}

        .sample-value-tag {{
            background: rgba(251, 113, 133, 0.1);
            border: 1px solid rgba(251, 113, 133, 0.2);
            color: var(--failure);
            font-size: 0.75rem;
            font-family: monospace;
            padding: 0.15rem 0.4rem;
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="container">
        
        <!-- Header -->
        <header>
            <div class="header-info">
                <h1>Data Engineering Quality Report</h1>
                <div class="header-meta">
                    <span>Generated: <strong>{timestamp}</strong></span>
                    <span>Status: <strong style="color: var(--accent-color);">{ "Healthy" if pass_percent >= 90 else ("Warning" if pass_percent >= 75 else "Unhealthy") }</strong></span>
                </div>
            </div>
            <div style="font-size: 0.75rem; text-align: right; color: var(--text-muted);">
                Retail Holiday Sales Pipeline v1.2
            </div>
        </header>

        <!-- Summary Statistics Dashboard -->
        <div class="summary-dashboard">
            <div class="gauge-card">
                <div class="progress-ring-container">
                    <svg class="progress-ring" width="120" height="120">
                        <circle class="progress-ring__background" stroke="#1e293b" stroke-width="8" fill="transparent" r="50" cx="60" cy="60"/>
                        <circle class="progress-ring__circle" stroke="var(--accent-color)" stroke-width="8" fill="transparent" r="50" cx="60" cy="60" 
                                stroke-dasharray="{dash_array}" stroke-dashoffset="{dash_offset}"/>
                    </svg>
                    <div class="gauge-text">{pass_percent}%</div>
                </div>
                <div class="gauge-label">Pass Percentage</div>
                <div class="gauge-status-desc">{total_passed} of {total_assertions} assertions successful</div>
            </div>

            <div class="stats-grid">
                <div class="stat-card total-stats">
                    <div class="stat-label">Total Checks</div>
                    <div class="stat-number-wrapper">
                        <div class="stat-number">{total_assertions}</div>
                    </div>
                </div>
                <div class="stat-card passed-stats">
                    <div class="stat-label">Passed Checks</div>
                    <div class="stat-number-wrapper">
                        <div class="stat-number">{total_passed}</div>
                    </div>
                </div>
                <div class="stat-card failed-stats">
                    <div class="stat-label">Failed Checks</div>
                    <div class="stat-number-wrapper">
                        <div class="stat-number">{total_failed}</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Controls (Tabs and filters) -->
        <div class="controls-row">
            <div class="tabs">
                <button class="tab-btn active" onclick="switchTab('all')">All Datasets</button>
"""
    
    # Generate tab buttons dynamically based on which validators were run
    for key, v in validators.items():
        html_content += f'                <button class="tab-btn" onclick="switchTab(\'{key}\')">{v.dataset_name}</button>\n'
        
    html_content += """            </div>
            
            <div class="filter-status-buttons">
                <button class="filter-status-btn active all-btn" onclick="filterStatus('all')">All Checks</button>
                <button class="filter-status-btn passed-btn" onclick="filterStatus('passed')">Passed</button>
                <button class="filter-status-btn failed-btn" onclick="filterStatus('failed')">Failed</button>
            </div>
        </div>

        <!-- Expectations List -->
        <div class="expectations-list">
"""

    card_index = 0
    # Generate list of collapsible expectation cards
    for val_key, val in validators.items():
        for res in val.results:
            success = res["success"]
            exp_type = res["expectation_type"]
            kwargs = res["kwargs"]
            result = res["result"]
            
            status_badge_class = "pass" if success else "fail"
            status_text = "PASSED" if success else "FAILED"
            column_tag_html = ""
            
            # Extract column key to print as tag
            column_name = kwargs.get("column")
            if column_name:
                column_tag_html = f'<span class="expectation-column-tag">{column_name}</span>'
                
            # Create a user friendly description
            desc = exp_type.replace("expect_", "").replace("_", " ")
            desc = desc.capitalize()
            
            card_class = f"status-{'passed' if success else 'failed'} dataset-{val_key}"
            
            html_content += f"""
            <div class="expectation-card {card_class}" id="card-{card_index}">
                <div class="expectation-header" onclick="toggleCard({card_index})">
                    <div class="expectation-title-area">
                        <span class="status-badge {status_badge_class}">{status_text}</span>
                        <span class="expectation-name">{desc}</span>
                        {column_tag_html}
                    </div>
                    <div class="toggle-icon">▼</div>
                </div>
                <div class="expectation-body" id="body-{card_index}">
                    <div class="expectation-content">
                        <!-- Details Panel -->
                        <div class="info-panel">
                            <h4>Validation Metrics</h4>
                            <div class="details-list">
                                <div class="details-item">
                                    <span>Dataset</span>
                                    <span>{val.dataset_name}</span>
                                </div>
                                <div class="details-item">
                                    <span>Assertion API Method</span>
                                    <span>{exp_type}</span>
                                </div>
            """
            
            # Print specific metrics based on output
            if "element_count" in result:
                html_content += f"""
                                <div class="details-item">
                                    <span>Total Elements Checked</span>
                                    <span>{result['element_count']}</span>
                                </div>
                """
            if "unexpected_count" in result:
                html_content += f"""
                                <div class="details-item">
                                    <span style="color: {'var(--text-muted)' if result['unexpected_count'] == 0 else 'var(--failure)'};">Unexpected Items</span>
                                    <span style="font-weight: bold; color: {'var(--text-main)' if result['unexpected_count'] == 0 else 'var(--failure)'};">{result['unexpected_count']}</span>
                                </div>
                                <div class="details-item">
                                    <span>Unexpected Percentage</span>
                                    <span>{result['unexpected_percent']}%</span>
                                </div>
                """
            if "observed_type" in result:
                 html_content += f"""
                                <div class="details-item">
                                    <span>Observed Datatype</span>
                                    <span>{result['observed_type']}</span>
                                </div>
                                <div class="details-item">
                                    <span>Expected Datatype</span>
                                    <span>{result['expected_type']}</span>
                                </div>
                """
            if "observed_row_count" in result:
                 html_content += f"""
                                <div class="details-item">
                                    <span>Observed Row Count</span>
                                    <span>{result['observed_row_count']}</span>
                                </div>
                                <div class="details-item">
                                    <span>Allowed Range</span>
                                    <span>[{result['min_rows']} - {result['max_rows']}]</span>
                                </div>
                """
                
            html_content += """
                            </div>
            """
            
            # Display sample failures if any exist
            if result.get("unexpected_values"):
                html_content += f"""
                            <div class="sample-values-title">Sample Unexpected Values</div>
                            <div class="sample-values">
                """
                for val_s in result["unexpected_values"]:
                    html_content += f'                                <span class="sample-value-tag">{val_s}</span>\n'
                html_content += """
                            </div>
                """
                
            html_content += f"""
                        </div>
                        
                        <!-- Arguments Panel -->
                        <div class="info-panel">
                            <h4>Expectation Configuration</h4>
                            <pre class="code-block">{json.dumps(kwargs, indent=2)}</pre>
                        </div>
                    </div>
                </div>
            </div>
            """
            card_index += 1
            
    html_content += """
        </div>
    </div>

    <!-- Collapsible card script and Tab filtering script -->
    <script>
        let currentTab = 'all';
        let currentStatusFilter = 'all';

        function toggleCard(index) {
            const card = document.getElementById('card-' + index);
            const body = document.getElementById('body-' + index);
            
            if (card.classList.contains('open')) {
                card.classList.remove('open');
                body.style.maxHeight = '0';
            } else {
                card.classList.add('open');
                body.style.maxHeight = body.scrollHeight + 'px';
            }
        }

        function switchTab(tabId) {
            currentTab = tabId;
            
            // Toggle active class on tab buttons
            const tabButtons = document.querySelectorAll('.tab-btn');
            tabButtons.forEach(btn => {
                if (btn.getAttribute('onclick').includes("'" + tabId + "'")) {
                    btn.classList.add('active');
                } else {
                    btn.classList.remove('active');
                }
            });
            
            applyFilters();
        }

        function filterStatus(status) {
            currentStatusFilter = status;
            
            // Toggle active class on filter buttons
            const filterButtons = document.querySelectorAll('.filter-status-btn');
            filterButtons.forEach(btn => {
                if (btn.getAttribute('onclick').includes("'" + status + "'")) {
                    btn.classList.add('active');
                } else {
                    btn.classList.remove('active');
                }
            });
            
            applyFilters();
        }

        function applyFilters() {
            const cards = document.querySelectorAll('.expectation-card');
            
            cards.forEach(card => {
                let matchesTab = false;
                let matchesStatus = false;
                
                // Check tab filter
                if (currentTab === 'all') {
                    matchesTab = true;
                } else {
                    matchesTab = card.classList.contains('dataset-' + currentTab);
                }
                
                // Check status filter
                if (currentStatusFilter === 'all') {
                    matchesStatus = true;
                } else if (currentStatusFilter === 'passed') {
                    matchesStatus = card.classList.contains('status-passed');
                } else if (currentStatusFilter === 'failed') {
                    matchesStatus = card.classList.contains('status-failed');
                }
                
                // Show or hide based on match
                if (matchesTab && matchesStatus) {
                    card.style.display = 'block';
                } else {
                    card.style.display = 'none';
                    // Make sure it collapses when hidden
                    card.classList.remove('open');
                    const body = card.querySelector('.expectation-body');
                    body.style.maxHeight = '0';
                }
            });
        }
    </script>
</body>
</html>
"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)


if __name__ == "__main__":
    # For testing execution stand-alone
    run_validation_suite()
