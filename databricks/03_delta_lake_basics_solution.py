# Databricks notebook source
# Solution 03: Delta Lake basics

# COMMAND ----------
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------
user = spark.sql("select current_user()").first()[0].replace("@", "_").replace(".", "_")
base_path = f"dbfs:/tmp/de_databricks/{user}"
delta_path = f"{base_path}/ex03/delta/events"

# COMMAND ----------
initial = spark.createDataFrame(
    [("e1", "c1", "purchase", 2999), ("e2", "c2", "purchase", 499)],
    "event_id string, customer_id string, event_type string, amount_cents int",
)
(initial.write.mode("overwrite").format("delta").save(delta_path))

# COMMAND ----------
updates = spark.createDataFrame(
    [("e2", "c2", "refund", -499), ("e3", "c1", "purchase", 6500)],
    "event_id string, customer_id string, event_type string, amount_cents int",
)

dt = DeltaTable.forPath(spark, delta_path)
(
    dt.alias("t")
    .merge(updates.alias("s"), "t.event_id = s.event_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

spark.read.format("delta").load(delta_path).orderBy("event_id").display()

# COMMAND ----------
# Time travel: version 0 (initial load)
old = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
old.orderBy("event_id").display()

# COMMAND ----------
# Show history
spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`").display()
