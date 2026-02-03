# Databricks notebook source
# UC Solution: Medallion architecture using Unity Catalog managed tables
#
# Prereq:
# - Run databricks/00_unity_catalog_setup_exercise.py to select catalog/schema.
#
# This variant writes to UC tables instead of DBFS paths.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# Choose your catalog/schema
try:
    dbutils.widgets.text("catalog", "de_training")
    dbutils.widgets.text("schema", "databricks_exercises")
except Exception:
    pass

catalog = dbutils.widgets.get("catalog") if "dbutils" in globals() else "de_training"
schema = dbutils.widgets.get("schema") if "dbutils" in globals() else "databricks_exercises"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------
# Bronze table
spark.sql(
    """
    CREATE OR REPLACE TABLE bronze_events (
      event_id STRING,
      event_date DATE,
      customer_id STRING,
      event_type STRING,
      amount_cents INT,
      ingested_at TIMESTAMP
    )
    USING DELTA
    """
)

raw = (
    spark.createDataFrame(
        [
            ("evt-1", "2026-01-01", "c1", "purchase", 2999),
            ("evt-2", "2026-01-01", "c1", "purchase", 2999),
            ("evt-2", "2026-01-01", "c1", "purchase", 2999),
            ("evt-3", "2026-01-02", "c2", "purchase", 499),
            ("evt-4", "2026-01-02", "c2", "refund", -499),
        ],
        "event_id string, event_date string, customer_id string, event_type string, amount_cents int",
    )
    .withColumn("event_date", F.to_date("event_date"))
    .withColumn("ingested_at", F.current_timestamp())
)

raw.write.mode("append").saveAsTable("bronze_events")

# COMMAND ----------
# Silver table: dedupe + validate
spark.sql(
    """
    CREATE OR REPLACE TABLE silver_events (
      event_id STRING,
      event_date DATE,
      customer_id STRING,
      event_type STRING,
      amount_cents INT,
      ingested_at TIMESTAMP
    )
    USING DELTA
    """
)

bronze = spark.table("bronze_events")

w = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())

deduped = bronze.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
valid = deduped.where((~F.col("event_type").isin("purchase", "refund")) | F.col("amount_cents").isNotNull())

# Overwrite silver for demo; in production: MERGE by event_id.
valid.write.mode("overwrite").saveAsTable("silver_events")

# COMMAND ----------
# Gold table: daily revenue
spark.sql(
    """
    CREATE OR REPLACE TABLE gold_daily_revenue (
      event_date DATE,
      net_revenue_cents BIGINT
    )
    USING DELTA
    """
)

gold = (
    spark.table("silver_events")
    .where(F.col("event_type").isin("purchase", "refund"))
    .groupBy("event_date")
    .agg(F.sum("amount_cents").alias("net_revenue_cents"))
)

gold.write.mode("overwrite").saveAsTable("gold_daily_revenue")

spark.table("gold_daily_revenue").orderBy("event_date").display()
