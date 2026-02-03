# Databricks notebook source
# UC Solution: Delta Lake basics using Unity Catalog tables

# COMMAND ----------
from delta.tables import DeltaTable

# COMMAND ----------
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
# Create/overwrite a managed Delta table
spark.sql(
    """
    CREATE OR REPLACE TABLE uc_events (
      event_id STRING,
      customer_id STRING,
      event_type STRING,
      amount_cents INT
    )
    USING DELTA
    """
)

spark.createDataFrame(
    [("e1", "c1", "purchase", 2999), ("e2", "c2", "purchase", 499)],
    "event_id string, customer_id string, event_type string, amount_cents int",
).write.mode("append").saveAsTable("uc_events")

# COMMAND ----------
updates = spark.createDataFrame(
    [("e2", "c2", "refund", -499), ("e3", "c1", "purchase", 6500)],
    "event_id string, customer_id string, event_type string, amount_cents int",
)

# MERGE into UC table
# DeltaTable.forName works well with UC
uc_dt = DeltaTable.forName(spark, "uc_events")
(
    uc_dt.alias("t")
    .merge(updates.alias("s"), "t.event_id = s.event_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

spark.table("uc_events").orderBy("event_id").display()

# COMMAND ----------
# Table history (time travel works)
spark.sql("DESCRIBE HISTORY uc_events").display()
