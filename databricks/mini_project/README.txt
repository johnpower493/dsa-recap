Databricks Mini Project: Auto Loader -> Bronze/Silver/Gold -> DLT -> Job

This mini-project is designed as a small, end-to-end DE workflow in Databricks.
It uses a file-based landing zone so it can be run in most workspaces.

Components
1) 00_setup_and_generate_landing_data.py
   - Creates a landing folder in DBFS and writes sample JSON batches.

2) 01_autoloader_to_bronze.py
   - Uses Auto Loader (cloudFiles) if available, otherwise falls back to file stream.
   - Writes to a Bronze Delta table (file path).

3) 02_dlt_pipeline_silver_gold.py
   - DLT pipeline template:
     - bronze_events (read stream)
     - silver_events (dedup + expectations)
     - gold_daily_revenue (aggregate)

4) 03_job_orchestration.py
   - Job-style notebook with widgets (run_date, lookback_days)
   - Shows idempotent rebuild of Gold for a rolling window.

Notes
- If Unity Catalog is required in your environment, you can adapt paths to managed tables.
- DLT notebook must be attached to a DLT pipeline to run.
