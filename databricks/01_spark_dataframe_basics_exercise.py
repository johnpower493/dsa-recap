# Databricks notebook source
# Exercise 01: Spark DataFrame basics
#
# Goals:
# - Create DataFrames
# - Read/write parquet
# - Basic transformations and actions
#
# How to run:
# - Create a Databricks notebook and paste this file.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------
# TODO: set a base path in DBFS for this exercise.
# Examples:
#   base_path = "dbfs:/tmp/de_databricks"
#   base_path = f"dbfs:/tmp/{spark.sql('select current_user()').first()[0].replace('@','_')}"
base_path = "dbfs:/tmp/de_databricks"

# COMMAND ----------
# 1) Create a small DataFrame
schema = T.StructType(
    [
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("event_ts", T.TimestampType(), False),
        T.StructField("event_type", T.StringType(), False),
        T.StructField("amount_cents", T.IntegerType(), True),
    ]
)

data = [
    ("c1", "2026-01-01 00:00:00", "page_view", None),
    ("c1", "2026-01-01 00:01:00", "purchase", 2999),
    ("c2", "2026-01-02 10:00:00", "purchase", 499),
]

df = (
    spark.createDataFrame(data, schema="customer_id string, event_ts string, event_type string, amount_cents int")
    .withColumn("event_ts", F.to_timestamp("event_ts"))
)

df.display()

# COMMAND ----------
# 2) Basic transformations
purchases = df.filter(F.col("event_type") == "purchase")
agg = purchases.groupBy("customer_id").agg(F.sum("amount_cents").alias("revenue_cents"))
agg.display()

# COMMAND ----------
# 3) Write parquet and read it back
out_path = f"{base_path}/ex01/events_parquet"
(
    df.write.mode("overwrite")
    .format("parquet")
    .save(out_path)
)

read_back = spark.read.parquet(out_path)
read_back.display()

# COMMAND ----------
# 4) Common gotchas
# TODO: observe the difference between:
# - df.count() (action triggers a job)
# - df.explain(True) (query plan)
print("count:", df.count())
df.explain(True)

# COMMAND ----------
# 5) Exercise prompts
# TODO:
# - Add a column event_date = to_date(event_ts)
# - Compute daily revenue (purchase only) per day
# - Write result to parquet under base_path/ex01/daily_revenue
