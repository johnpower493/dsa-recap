# Databricks notebook source
# Exercise 08: Jobs orchestration, parameters, retries
#
# Goals:
# - Use notebook widgets for parameters
# - Make jobs idempotent
# - Simulate retries and checkpointing

# COMMAND ----------
dbutils.widgets.text("run_date", "2026-01-01")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------
base_path = "dbfs:/tmp/de_databricks"

# COMMAND ----------
# 1) Parameterized processing
# TODO:
# - Read the bronze events
# - Rebuild only the partition for run_date (or run_date +/- lookback)
# - Write to silver/gold idempotently (overwrite partition / MERGE)

# COMMAND ----------
# 2) Idempotency prompt
# TODO: explain (in comments) two strategies:
# - overwrite partition for run_date
# - MERGE using a primary key

# COMMAND ----------
# 3) Retry simulation
# TODO:
# - Raise an exception conditionally (e.g., if run_date ends with '02')
# - Discuss why a retry would not create duplicates if you implemented idempotent writes.
