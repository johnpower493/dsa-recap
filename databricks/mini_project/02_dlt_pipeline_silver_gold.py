# Databricks notebook source
# Mini Project - 02: DLT pipeline (Silver + Gold with expectations)
#
# This notebook is intended to run inside a Delta Live Tables pipeline.
# Steps:
# 1) Create a DLT pipeline in Databricks
# 2) Add this notebook as the pipeline source
# 3) Set configuration for SOURCE_PATH to your bronze path

# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
# Configure this in your DLT pipeline settings as a configuration key
# e.g., "mini_project.bronze_path": "dbfs:/tmp/.../mini_project/bronze/events"
BRONZE_PATH = spark.conf.get("mini_project.bronze_path", "dbfs:/tmp/de_databricks/UNKNOWN/mini_project/bronze/events")

# COMMAND ----------
@dlt.table(
    name="bronze_events",
    comment="Bronze events ingested by Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_events():
    return spark.readStream.format("delta").load(BRONZE_PATH)


@dlt.table(
    name="silver_events",
    comment="Validated + deduplicated events.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop(
    "monetary_amount_present",
    "(event_type NOT IN ('purchase','refund')) OR (amount_cents IS NOT NULL)",
)
def silver_events():
    b = dlt.read_stream("bronze_events")

    # De-dup by event_id with event-time watermark
    # (Assumes event_id is the event primary key)
    s = (
        b.withWatermark("event_ts", "1 day")
        .dropDuplicates(["event_id"])
    )

    return s


@dlt.table(
    name="gold_daily_revenue",
    comment="Daily net revenue from purchase/refund.",
    table_properties={"quality": "gold"},
)
def gold_daily_revenue():
    s = dlt.read("silver_events")
    return (
        s.where(F.col("event_type").isin("purchase", "refund"))
        .groupBy("event_date")
        .agg(F.sum("amount_cents").alias("net_revenue_cents"))
    )
