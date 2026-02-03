# Databricks notebook source
# Solution 04: Medallion architecture (Bronze/Silver/Gold)

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
user = spark.sql("select current_user()").first()[0].replace("@", "_").replace(".", "_")
base_path = f"dbfs:/tmp/de_databricks/{user}"
bronze_path = f"{base_path}/ex04/bronze/events"
silver_path = f"{base_path}/ex04/silver/events_clean"
gold_path = f"{base_path}/ex04/gold/daily_revenue"

# COMMAND ----------
raw = (
    spark.createDataFrame(
        [
            ("evt-1", "2026-01-01", "c1", "purchase", 2999),
            ("evt-2", "2026-01-01", "c1", "purchase", 2999),
            ("evt-2", "2026-01-01", "c1", "purchase", 2999),  # duplicate by event_id
            ("evt-3", "2026-01-02", "c2", "purchase", 499),
            ("evt-4", "2026-01-02", "c2", "refund", -499),
        ],
        "event_id string, event_date string, customer_id string, event_type string, amount_cents int",
    )
    .withColumn("event_date", F.to_date("event_date"))
    .withColumn("ingested_at", F.current_timestamp())
)

raw.write.mode("overwrite").format("delta").save(bronze_path)

# COMMAND ----------
bronze = spark.read.format("delta").load(bronze_path)

# Dedupe by event_id: keep latest ingested_at
w = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())

deduped = bronze.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")

# Basic validation
valid = deduped.where(
    (~F.col("event_type").isin("purchase", "refund"))
    | (F.col("amount_cents").isNotNull())
)

valid.write.mode("overwrite").format("delta").save(silver_path)

# COMMAND ----------
silver = spark.read.format("delta").load(silver_path)

# Gold aggregate: daily net revenue (purchase + refund)
gold = (
    silver.where(F.col("event_type").isin("purchase", "refund"))
    .groupBy("event_date")
    .agg(F.sum("amount_cents").alias("net_revenue_cents"))
    .orderBy("event_date")
)

gold.write.mode("overwrite").format("delta").save(gold_path)

spark.read.format("delta").load(gold_path).display()

# COMMAND ----------
# Late arriving data strategy (rolling window rebuild) example:
# Recompute last N days by overwriting those partitions.
# (Here we only show the pattern; in production you would parameterize N / run_date.)
N = 3
window_start = F.date_sub(F.current_date(), N)

rebuild = (
    silver.where(F.col("event_date") >= window_start)
    .where(F.col("event_type").isin("purchase", "refund"))
    .groupBy("event_date")
    .agg(F.sum("amount_cents").alias("net_revenue_cents"))
)

# If partitioned, you'd overwrite only partitions in the window.
# For file-path Delta, simplest is overwrite entire table for demo.
# rebuild.write.mode("overwrite").format("delta").save(gold_path)
