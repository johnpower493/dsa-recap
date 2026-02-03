Databricks for Data Engineering (Exercises)

This folder contains hands-on exercises for learning Databricks concepts used in data engineering.
The exercises are written as Python scripts in the "Databricks notebook source" format so you can:
- paste them into a Databricks notebook, or
- import them as source notebooks.

Recommended order (non-Unity Catalog / file-path based):
  1) 01_spark_dataframe_basics_exercise.py
  2) 02_joins_aggregations_windows_exercise.py
  3) 03_delta_lake_basics_exercise.py
  4) 04_medallion_bronze_silver_gold_exercise.py
  5) 05_structured_streaming_exercise.py
  6) 06_optimization_and_zorder_exercise.py
  7) 07_dlt_pipelines_concepts_exercise.py
  8) 08_jobs_orchestration_retries_exercise.py

Solutions:
  - 01_spark_dataframe_basics_solution.py
  - 02_joins_aggregations_windows_solution.py
  - 03_delta_lake_basics_solution.py
  - 04_medallion_bronze_silver_gold_solution.py
  - 05_structured_streaming_solution.py
  - 06_optimization_and_zorder_solution.py
  - 07_dlt_pipelines_concepts_solution.py (DLT pipeline template)
  - 08_jobs_orchestration_retries_solution.py

Unity Catalog (UC) path (managed tables):
  0) 00_unity_catalog_setup_exercise.py
  Then run UC variants:
    - 03_delta_lake_basics_uc_solution.py
    - 04_medallion_bronze_silver_gold_uc_solution.py

Prereqs
- A Databricks workspace with a cluster (DBR) and permissions to create tables.
- Unity Catalog requires a UC-enabled workspace/cluster and appropriate privileges.

Tip
- Search for "TODO" in each notebook and fill in the code.
