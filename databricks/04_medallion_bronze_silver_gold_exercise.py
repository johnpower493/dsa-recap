# Databricks notebook source
# Exercise 04: Medallion architecture (Bronze/Silver/Gold)
#
# Goals:
# - Bronze: raw ingest (append-only)
# - Silver: cleaned/deduped/enriched
# - Gold: aggregates for BI

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
base_path = "dbfs:/tmp/de_databricks"
bronze_path = f"{base_path}/ex04/bronze/events"
silver_path = f"{base_path}/ex04/silver/events_clean"
gold_path = f"{base_path}/ex04/gold/daily_revenue"

# COMMAND ----------
# 1) Bronze: simulate raw ingest
raw = spark.createDataFrame(
    [
        ("evt-1", "2026-01-01", "c1", "purchase", 2999),
        ("evt-2", "2026-01-01", "c1", "purchase", 2999),  # duplicate
        ("evt-3", "2026-01-02", "c2", "purchase", 499),
        ("evt-4", "2026-01-02", "c2", "refund", -499),
    ],
    "event_id string, event_date string, customer_id string, event_type string, amount_cents int",
).withColumn("event_date", F.to_date("event_date"))

(raw.write.mode("overwrite").format("delta").save(bronze_path))

# COMMAND ----------
# 2) Silver: dedupe and basic validation
bronze = spark.read.format("delta").load(bronze_path)

# TODO:
# - Deduplicate by (event_id) or by business key.
# - Filter out invalid rows (e.g., purchase/refund must have non-null amount_cents)
# - Write to silver_path as Delta.

# COMMAND ----------
# 3) Gold: aggregate metrics
# TODO:
# - Read from silver
# - Compute daily net revenue (purchase + refund)
# - Write to gold_path

# COMMAND ----------
# 4) Exercise prompts
# TODO:
# - Add a "rebuild last N days" pattern for late arriving data
# - Explain why bronze is append-only and how to handle reprocessing
