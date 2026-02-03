# Databricks notebook source
# Solution 08: Jobs orchestration, parameters, retries

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
dbutils.widgets.text("run_date", "2026-01-02")
dbutils.widgets.text("lookback_days", "2")

run_date = dbutils.widgets.get("run_date")
lookback_days = int(dbutils.widgets.get("lookback_days"))

# COMMAND ----------
user = spark.sql("select current_user()").first()[0].replace("@", "_").replace(".", "_")
base_path = f"dbfs:/tmp/de_databricks/{user}"
bronze_path = f"{base_path}/ex08/bronze/events"
silver_path = f"{base_path}/ex08/silver/events_clean"
gold_path = f"{base_path}/ex08/gold/daily_revenue"

# COMMAND ----------
# Prepare some demo data (bronze)
raw = (
    spark.createDataFrame(
        [
            ("evt-1", "2026-01-01", "c1", "purchase", 2999),
            ("evt-2", "2026-01-02", "c1", "purchase", 499),
            ("evt-3", "2026-01-02", "c2", "refund", -499),
            ("evt-4", "2026-01-03", "c2", "purchase", 6500),
        ],
        "event_id string, event_date string, customer_id string, event_type string, amount_cents int",
    )
    .withColumn("event_date", F.to_date("event_date"))
)
raw.write.mode("overwrite").format("delta").save(bronze_path)

# COMMAND ----------
# Determine rolling window to rebuild
run_day = F.to_date(F.lit(run_date))
start_day_expr = F.date_sub(run_day, lookback_days)

bronze = spark.read.format("delta").load(bronze_path)
windowed = bronze.where(F.col("event_date") >= start_day_expr).where(F.col("event_date") <= run_day)

# Idempotent silver write (overwrite entire silver for demo; in production overwrite partitions)
windowed.write.mode("overwrite").format("delta").save(silver_path)

silver = spark.read.format("delta").load(silver_path)

# Gold rebuild for window; in real pipelines: overwrite only affected partitions
agg = (
    silver.where(F.col("event_type").isin("purchase", "refund"))
    .groupBy("event_date")
    .agg(F.sum("amount_cents").alias("net_revenue_cents"))
)
agg.write.mode("overwrite").format("delta").save(gold_path)

spark.read.format("delta").load(gold_path).orderBy("event_date").display()

# COMMAND ----------
# Retry simulation: fail on certain run_date to show idempotency
if run_date.endswith("02"):
    raise RuntimeError("Simulated failure to test retries. Re-running should not create duplicates due to overwrite semantics.")
