"""@bruin

# Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# Set the connection.
connection: duckdb-default

# Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) 
# and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization 
# and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. 
# In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # choose `table` or `view` (ingestion generally should be a table)
  type: table
  # pick a strategy.
  # suggested strategy: append
  strategy: append

# Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: VendorID
    type: integer
  - name: tpep_pickup_datetime
    type: timestamp
  - name: tpep_dropoff_datetime
    type: timestamp
  - name: passenger_count
    type: float
  - name: trip_distance
    type: float
  - name: RatecodeID
    type: float
  - name: store_and_fwd_flag
    type: string
  - name: PULocationID
    type: integer
  - name: DOLocationID
    type: integer
  - name: payment_type
    type: integer
  - name: fare_amount
    type: float
  - name: extra
    type: float
  - name: mta_tax
    type: float
  - name: tip_amount
    type: float
  - name: tolls_amount
    type: float
  - name: improvement_surcharge
    type: float
  - name: total_amount
    type: float
  - name: congestion_surcharge
    type: float
  - name: airport_fee
    type: float
  - name: extraction_timestamp
    type: timestamp

@bruin"""

# Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import pandas as pd
import requests
import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta


# Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    Implement ingestion using Bruin runtime context.
    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    
    At the end, return final_dataframe
    """

    # 1. Retrieve environment variables and pipeline variables
    start_date_str = os.environ.get("BRUIN_START_DATE")
    end_date_str = os.environ.get("BRUIN_END_DATE")
    
    # BRUIN_VARS contains the pipeline variables as a JSON string
    bruin_vars_json = os.environ.get("BRUIN_VARS", "{}")
    bruin_vars = json.loads(bruin_vars_json)
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])
    
    if not start_date_str or not end_date_str:
        print("Required BRUIN_START_DATE or BRUIN_END_DATE not set.")
        return pd.DataFrame()

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    all_dfs = []
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    # 2. Iterate through each month in the range
    current_date = start_date.replace(day=1)
    while current_date <= end_date:
        year_month = current_date.strftime("%Y-%m")
        
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year_month}.parquet"
            url = f"{base_url}{filename}"
            
            print(f"Downloading: {url}")
            try:
                response = requests.get(url)
                response.raise_for_status()
                
                # Use pyarrow (via pandas) to read the parquet content
                # We can use pd.read_parquet on the URL directly if preferred, 
                # but fetching via requests gives more control over error handling.
                df = pd.read_parquet(url)
                
                # Add extraction metadata
                df["extraction_timestamp"] = datetime.utcnow()
                
                all_dfs.append(df)
            except Exception as e:
                print(f"Failed to download {url}: {e}")
        
        current_date += relativedelta(months=1)
    
    if not all_dfs:
        print("No data found for the given range.")
        return pd.DataFrame()
        
    final_df = pd.concat(all_dfs, ignore_index=True)
    return final_df


