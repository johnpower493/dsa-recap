# Databricks notebook source
# Exercise 03: Delta Lake basics
#
# Goals:
# - Write Delta tables
# - Time travel
# - Upserts with MERGE

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
base_path = "dbfs:/tmp/de_databricks"
delta_path = f"{base_path}/ex03/delta/events"

# COMMAND ----------
# 1) Create initial DataFrame and write as Delta
initial = spark.createDataFrame(
    [("e1", "c1", "purchase", 2999), ("e2", "c2", "purchase", 499)],
    "event_id string, customer_id string, event_type string, amount_cents int",
)

(initial.write.mode("overwrite").format("delta").save(delta_path))

spark.read.format("delta").load(delta_path).display()

# COMMAND ----------
# 2) Update data (simulate late update) using MERGE
updates = spark.createDataFrame(
    [("e2", "c2", "refund", -499), ("e3", "c1", "purchase", 6500)],
    "event_id string, customer_id string, event_type string, amount_cents int",
)

# TODO: implement a MERGE into the Delta table at delta_path using event_id as key.
# Hint:
#   from delta.tables import DeltaTable
#   dt = DeltaTable.forPath(spark, delta_path)
#   dt.alias("t").merge(updates.alias("s"), "t.event_id = s.event_id")...

# COMMAND ----------
# 3) Time travel
# TODO: read an older version of the Delta table (versionAsOf=0) and compare to current.

# COMMAND ----------
# 4) Exercise prompts
# TODO:
# - Add a constraint (Delta table expectations are typically DLT; for Delta use CHECK via SQL if supported)
# - Demonstrate OPTIMIZE (if enabled) and file compaction considerations
