# Databricks notebook source
# Solution 01: Spark DataFrame basics

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# Use a per-user folder to avoid collisions in shared workspaces.
user = spark.sql("select current_user()").first()[0].replace("@", "_").replace(".", "_")
base_path = f"dbfs:/tmp/de_databricks/{user}"

# COMMAND ----------
# Create DataFrame
rows = [
    ("c1", "2026-01-01 00:00:00", "page_view", None),
    ("c1", "2026-01-01 00:01:00", "purchase", 2999),
    ("c2", "2026-01-02 10:00:00", "purchase", 499),
]

df = (
    spark.createDataFrame(rows, "customer_id string, event_ts string, event_type string, amount_cents int")
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("event_date", F.to_date("event_ts"))
)

df.display()

# COMMAND ----------
# Daily revenue (purchase only)
daily_rev = (
    df.where(F.col("event_type") == "purchase")
    .groupBy("event_date")
    .agg(F.sum("amount_cents").alias("daily_revenue_cents"))
    .orderBy("event_date")
)

daily_rev.display()

# COMMAND ----------
# Write parquet
out_path = f"{base_path}/ex01/daily_revenue_parquet"
(daily_rev.write.mode("overwrite").format("parquet").save(out_path))

spark.read.parquet(out_path).display()

# COMMAND ----------
print("df.count() =", df.count())
df.explain(True)
