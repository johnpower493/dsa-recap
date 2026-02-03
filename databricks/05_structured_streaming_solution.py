# Databricks notebook source
# Solution 05: Structured Streaming (file source -> Delta sink)

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
user = spark.sql("select current_user()").first()[0].replace("@", "_").replace(".", "_")
base_path = f"dbfs:/tmp/de_databricks/{user}"
input_path = f"{base_path}/ex05/input_json"
checkpoint_path = f"{base_path}/ex05/checkpoints/events"
out_path = f"{base_path}/ex05/delta/events"

# COMMAND ----------
# Create some example JSON files.
# In a real lab, you might upload files or land them via Auto Loader.
import json

examples = [
    {"event_id": "s1", "event_ts": "2026-01-01T00:00:00Z", "event_type": "purchase", "amount_cents": 2999},
    {"event_id": "s2", "event_ts": "2026-01-01T00:01:00Z", "event_type": "page_view", "amount_cents": None},
]

dbutils.fs.mkdirs(input_path)
for i, rec in enumerate(examples, start=1):
    dbutils.fs.put(f"{input_path}/batch_{i}.json", json.dumps(rec), overwrite=True)

# COMMAND ----------
# Read stream (file source)
stream_df = spark.readStream.format("json").load(input_path)

parsed = (
    stream_df.withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("event_date", F.to_date("event_ts"))
)

# Deduplicate using event_id with watermark (event time)
deduped = (
    parsed.withWatermark("event_ts", "1 day")
    .dropDuplicates(["event_id"])
)

# COMMAND ----------
query = (
    deduped.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(out_path)
)

# Let it run briefly for file-based example.
query.processAllAvailable()
query.stop()

spark.read.format("delta").load(out_path).display()
