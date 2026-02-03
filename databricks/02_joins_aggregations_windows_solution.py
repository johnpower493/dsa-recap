# Databricks notebook source
# Solution 02: Joins, aggregations, window functions

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------
customers = spark.createDataFrame(
    [("c1", "Ava"), ("c2", "Ben"), ("c3", "Cora")],
    "customer_id string, name string",
)

orders = (
    spark.createDataFrame(
        [
            ("o1", "c1", "2026-01-01 00:01:00", 2999),
            ("o2", "c1", "2026-01-02 00:01:00", 499),
            ("o3", "c2", "2026-01-03 12:00:00", 6500),
        ],
        "order_id string, customer_id string, order_ts string, amount_cents int",
    )
    .withColumn("order_ts", F.to_timestamp("order_ts"))
)

# COMMAND ----------
joined = orders.join(customers, on="customer_id", how="left")
joined.display()

# COMMAND ----------
# Lifetime revenue
joined.groupBy("customer_id", "name").agg(F.sum("amount_cents").alias("lifetime_revenue")).display()

# COMMAND ----------
# First order timestamp per customer
first_order = joined.groupBy("customer_id").agg(F.min("order_ts").alias("first_order_ts"))
first_order.join(customers, "customer_id", "left").display()

# COMMAND ----------
# Running revenue per customer
w = Window.partitionBy("customer_id").orderBy("order_ts").rowsBetween(Window.unboundedPreceding, 0)
with_running = joined.withColumn("running_revenue", F.sum("amount_cents").over(w))
with_running.orderBy("customer_id", "order_ts").display()

# COMMAND ----------
# Customers with no orders (left anti)
no_orders = customers.join(orders.select("customer_id").distinct(), on="customer_id", how="left_anti")
no_orders.display()

# COMMAND ----------
# Rank orders by recency
w2 = Window.partitionBy("customer_id").orderBy(F.col("order_ts").desc())
ranked = joined.withColumn("order_rank", F.row_number().over(w2))
ranked.display()
