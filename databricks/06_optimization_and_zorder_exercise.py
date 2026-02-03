# Databricks notebook source
# Exercise 06: Optimization, file sizing, partitioning, Z-ORDER
#
# Goals:
# - Understand partition pruning
# - OPTIMIZE and ZORDER (if supported)
# - AQE / broadcast joins

# COMMAND ----------
# Note: OPTIMIZE/ZORDER availability depends on Databricks runtime / workspace configuration.

# COMMAND ----------
base_path = "dbfs:/tmp/de_databricks"
delta_path = f"{base_path}/ex06/delta/fact_events"

# COMMAND ----------
from pyspark.sql import functions as F

# 1) Create a larger synthetic dataset
n = 1_000_00  # TODO: increase on a real cluster

df = (
    spark.range(0, n)
    .withColumn("event_date", F.expr("date_add('2026-01-01', cast(id % 30 as int))"))
    .withColumn("customer_id", F.expr("concat('c', cast(id % 10000 as string))"))
    .withColumn("amount_cents", (F.col("id") % 5000).cast("int"))
)

(df.write.mode("overwrite").format("delta").partitionBy("event_date").save(delta_path))

# COMMAND ----------
# 2) Partition pruning demo
spark.read.format("delta").load(delta_path).where("event_date = '2026-01-10'").explain(True)

# COMMAND ----------
# 3) OPTIMIZE + ZORDER (if available)
# TODO (Databricks SQL):
#   OPTIMIZE delta.`{delta_path}` ZORDER BY (customer_id);
# Then rerun a query filtering by customer_id and compare.

# COMMAND ----------
# 4) Exercise prompts
# TODO:
# - Show when broadcast join helps (small dim table)
# - Explain skew and techniques (salting, AQE)
