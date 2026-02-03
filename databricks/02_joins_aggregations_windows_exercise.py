# Databricks notebook source
# Exercise 02: Joins, aggregations, window functions

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------
base_path = "dbfs:/tmp/de_databricks"

# COMMAND ----------
# Create small dimension and fact DataFrames
customers = spark.createDataFrame(
    [("c1", "Ava"), ("c2", "Ben"), ("c3", "Cora")],
    "customer_id string, name string",
)

orders = spark.createDataFrame(
    [
        ("o1", "c1", "2026-01-01 00:01:00", 2999),
        ("o2", "c1", "2026-01-02 00:01:00", 499),
        ("o3", "c2", "2026-01-03 12:00:00", 6500),
    ],
    "order_id string, customer_id string, order_ts string, amount_cents int",
).withColumn("order_ts", F.to_timestamp("order_ts"))

# COMMAND ----------
# 1) Join facts to dims
joined = orders.join(customers, on="customer_id", how="left")
joined.display()

# COMMAND ----------
# 2) Aggregations
joined.groupBy("customer_id", "name").agg(F.sum("amount_cents").alias("lifetime_revenue")).display()

# COMMAND ----------
# 3) Window functions: rank orders by recency per customer
w = Window.partitionBy("customer_id").orderBy(F.col("order_ts").desc())
ranked = joined.withColumn("order_rank", F.row_number().over(w))
ranked.display()

# COMMAND ----------
# 4) Exercise prompts
# TODO:
# - Compute first_order_ts per customer
# - Compute running revenue per customer ordered by order_ts
# - Identify customers with no orders (left anti join)
