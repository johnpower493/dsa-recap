Data Engineering Patterns (Exercises)

This folder contains practice exercises for common data engineering patterns using SQL + Python.

Recommended order:
  1) 01_incremental_load_exercise.py
  2) 02_dedup_idempotency_exercise.py
  3) 03_scd_type2_exercise.py
  4) 04_data_quality_exercise.py
  5) 05_cdc_merge_exercise.py
  6) 06_partitioning_strategy_exercise.py
  7) 07_backfills_late_data_exercise.py
  8) 08_orchestration_retries_exercise.py

Notes:
- The SQL exercises assume the demo schema in sql/00_setup_schema_and_seed.sql (Postgres).
- These are written to be runnable with plain Python (no external deps). Where a DB is involved,
  the scripts focus on generating SQL and validating logic in-memory.
