# NY Taxi Data Pipeline (Bruin & DuckDB)

This Bruin project implements an end-to-end data pipeline for ingesting, cleaning, and staging New York taxi trip data into a DuckDB database.

## Pipeline Architecture

The pipeline is organized into several key stages:

### 1. Ingestion
- **Assets**:
    - `ingestion.trips` ([trips.py](file:///c:/tmp/antigravity-bruin-mcp-bigquery/zoomcamp/pipeline/assets/ingestion/trips.py)): A Python asset that downloads Parquet files from the NYC Taxi & Limousine Commission (TLC) Trip Record Data CloudFront bucket.
    - `ingestion.payment_lookup` ([payment_lookup.asset.yml](file:///c:/tmp/antigravity-bruin-mcp-bigquery/zoomcamp/pipeline/assets/ingestion/payment_lookup.asset.yml)): A seed asset that loads a local CSV ([payment_lookup.csv](file:///c:/tmp/antigravity-bruin-mcp-bigquery/zoomcamp/pipeline/assets/ingestion/payment_lookup.csv)) containing payment type descriptions.

### 2. Staging
- **Asset**:
    - `staging.trips` ([trips.sql](file:///c:/tmp/antigravity-bruin-mcp-bigquery/zoomcamp/pipeline/assets/staging/trips.sql)): A DuckDB SQL asset that cleans, joins, and materializes the ingestion data.
- **Key Features**:
    - **Enrichment**: Joins raw trip records with payment names.
    - **Collision Handling**: Renames conflicting column names (e.g., `Airport_fee` to `airport_fee_legacy`) to prevent errors in case-insensitive environments.
    - **Incremental Processing**: Uses the `time_interval` strategy for efficient updates based on the run's time window.

## Setup & Configuration

### DuckDB Connection
The project is configured to use a local DuckDB database file (`duckdb.db`). The connection is defined in `.bruin.yml` as:

```yaml
connections:
    duckdb:
        - name: duckdb-default
          path: duckdb.db
```

## Running the Pipeline

### Validation
To ensure all assets are correctly configured and lineage is intact:
```bash
bruin validate
```

### Normal Run
To execute the pipeline for the current window:
```bash
bruin run
```

### Initial Run (Full Refresh)
When running the pipeline for the first time or when you need to recreate the tables from scratch in DuckDB, use the `--full-refresh` flag:
```bash
bruin run --full-refresh
```

---
Happy Building! For more information on Bruin concepts, visit the [Bruin Documentation](https://getbruin.com/docs/).