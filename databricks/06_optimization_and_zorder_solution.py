# Databricks notebook source
# Solution 06: Optimization, partitioning, Z-ORDER (where supported)

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
user = spark.sql("select current_user()").first()[0].replace("@", "_").replace(".", "_")
base_path = f"dbfs:/tmp/de_databricks/{user}"
delta_path = f"{base_path}/ex06/delta/fact_events"

# COMMAND ----------
# Generate synthetic data
n = 100_000

df = (
    spark.range(0, n)
    .withColumn("event_date", F.expr("date_add('2026-01-01', cast(id % 30 as int))"))
    .withColumn("customer_id", F.expr("concat('c', cast(id % 10000 as string))"))
    .withColumn("amount_cents", (F.col("id") % 5000).cast("int"))
)

(df.write.mode("overwrite").format("delta").partitionBy("event_date").save(delta_path))

# COMMAND ----------
# Partition pruning demo
spark.read.format("delta").load(delta_path).where("event_date = '2026-01-10'").explain(True)

# COMMAND ----------
# Broadcast join demo
small_dim = spark.createDataFrame([(f"c{i}", f"seg_{i%5}") for i in range(0, 1000)], "customer_id string, segment string")

fact = spark.read.format("delta").load(delta_path)
joined = fact.join(F.broadcast(small_dim), "customer_id", "left")
joined.explain(True)

# COMMAND ----------
# OPTIMIZE/ZORDER (Databricks SQL command, may not be available everywhere)
# If supported in your workspace, run in a SQL cell:
#   OPTIMIZE delta.`{delta_path}` ZORDER BY (customer_id);
# Then compare query performance filtering by customer_id.
