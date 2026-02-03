# Databricks notebook source
# Mini Project - 01: Auto Loader -> Bronze Delta
#
# Goal:
# - Read JSON from landing zone using Auto Loader (cloudFiles) if possible
# - Parse event_ts and add event_date, ingest metadata
# - Write to Bronze Delta sink with checkpointing

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
user = spark.sql("select current_user()").first()[0].replace("@", "_").replace(".", "_")
base_path = f"dbfs:/tmp/de_databricks/{user}/mini_project"
landing_path = f"{base_path}/landing/events_json"
bronze_path = f"{base_path}/bronze/events"
checkpoint_path = f"{base_path}/checkpoints/bronze_events"

print("landing_path:", landing_path)
print("bronze_path:", bronze_path)

# COMMAND ----------
# Choose Auto Loader if available; otherwise fallback.
use_autoloader = True
try:
    spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
except Exception:
    use_autoloader = False

if use_autoloader:
    raw_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(landing_path)
    )
else:
    raw_stream = spark.readStream.format("json").load(landing_path)

parsed = (
    raw_stream.withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("event_date", F.to_date("event_ts"))
    .withColumn("ingested_at", F.current_timestamp())
)

# COMMAND ----------
query = (
    parsed.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(bronze_path)
)

# Run until all available files are processed (good for labs)
query.processAllAvailable()
query.stop()

spark.read.format("delta").load(bronze_path).orderBy("event_id").display()
