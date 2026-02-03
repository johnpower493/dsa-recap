# Databricks notebook source
# Solution 07: Delta Live Tables (DLT) pipeline code (intended for DLT context)
#
# This notebook is a reference template: it is meant to run as part of a DLT pipeline,
# not as a regular interactive notebook.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
# Configure your source path (e.g., cloud storage).
SOURCE_PATH = "dbfs:/tmp/de_databricks/dlt_source/events_json"

# COMMAND ----------
@dlt.table(
    name="bronze_events",
    comment="Raw events ingested from JSON (bronze).",
    table_properties={"quality": "bronze"},
)
def bronze_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(SOURCE_PATH)
    )


@dlt.table(
    name="silver_events",
    comment="Cleaned + validated events (silver).",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop(
    "monetary_amount_present",
    "(event_type NOT IN ('purchase','refund')) OR (amount_cents IS NOT NULL)",
)
def silver_events():
    b = dlt.read_stream("bronze_events")
    return (
        b.withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .dropDuplicates(["event_id"])
    )


@dlt.table(
    name="gold_daily_revenue",
    comment="Daily net revenue (gold).",
    table_properties={"quality": "gold"},
)
def gold_daily_revenue():
    s = dlt.read("silver_events")
    return (
        s.where(F.col("event_type").isin("purchase", "refund"))
        .groupBy("event_date")
        .agg(F.sum("amount_cents").alias("net_revenue_cents"))
    )
