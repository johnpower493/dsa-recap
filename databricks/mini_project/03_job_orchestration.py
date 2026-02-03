# Databricks notebook source
# Mini Project - 03: Job orchestration notebook (widgets + idempotent rebuild)
#
# Goal:
# - Parameterize run_date and lookback_days
# - Rebuild gold aggregate for a rolling window to handle late data
# - Demonstrate retry-safe idempotent writes
#
# Note:
# - If you're using DLT for gold, you usually let DLT manage the gold table.
#   This notebook demonstrates the same logic for non-DLT jobs.

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# Job parameters
try:
    dbutils.widgets.text("run_date", "2026-01-02")
    dbutils.widgets.text("lookback_days", "2")
except Exception:
    pass

run_date = dbutils.widgets.get("run_date") if "dbutils" in globals() else "2026-01-02"
lookback_days = int(dbutils.widgets.get("lookback_days")) if "dbutils" in globals() else 2

# COMMAND ----------
user = spark.sql("select current_user()").first()[0].replace("@", "_").replace(".", "_")
base_path = f"dbfs:/tmp/de_databricks/{user}/mini_project"
bronze_path = f"{base_path}/bronze/events"
silver_path = f"{base_path}/silver/events_clean"
gold_path = f"{base_path}/gold/daily_revenue"

# COMMAND ----------
bronze = spark.read.format("delta").load(bronze_path)

run_day = F.to_date(F.lit(run_date))
start_day = F.date_sub(run_day, lookback_days)

windowed = bronze.where(F.col("event_date") >= start_day).where(F.col("event_date") <= run_day)

# Silver step (idempotent): overwrite silver for window demo
# In production: MERGE by event_id or overwrite partitions by event_date.
windowed.write.mode("overwrite").format("delta").save(silver_path)

silver = spark.read.format("delta").load(silver_path)

# Gold rebuild for windowed days
agg = (
    silver.where(F.col("event_type").isin("purchase", "refund"))
    .groupBy("event_date")
    .agg(F.sum("amount_cents").alias("net_revenue_cents"))
)

# Idempotent write: overwrite for demo; in production, partition gold by event_date and overwrite window partitions.
agg.write.mode("overwrite").format("delta").save(gold_path)

spark.read.format("delta").load(gold_path).orderBy("event_date").display()

# COMMAND ----------
# Retry simulation: optionally fail to test job retries
try:
    dbutils.widgets.dropdown("fail", "false", ["false", "true"])
    fail = dbutils.widgets.get("fail")
except Exception:
    fail = "false"

if fail == "true":
    raise RuntimeError("Simulated failure: re-run should be safe because writes are idempotent.")
