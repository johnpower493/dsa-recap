# Databricks notebook source
# Exercise 05: Structured Streaming (file source -> Delta sink)
#
# Goals:
# - Use Auto Loader (cloudFiles) if available, otherwise file stream
# - Maintain checkpoints
# - Write to Delta

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
base_path = "dbfs:/tmp/de_databricks"
input_path = f"{base_path}/ex05/input_json"
checkpoint_path = f"{base_path}/ex05/checkpoints/events"
out_path = f"{base_path}/ex05/delta/events"

# COMMAND ----------
# Setup: create a folder and write a couple JSON files manually (or with dbutils.fs.put).
# TODO: create example JSON input files with fields: event_id, event_ts, event_type, amount_cents

# COMMAND ----------
# 1) Define streaming read
# Option A: Auto Loader (recommended on Databricks)
# stream_df = (spark.readStream.format("cloudFiles")
#              .option("cloudFiles.format", "json")
#              .load(input_path))
#
# Option B: file stream
stream_df = spark.readStream.format("json").load(input_path)

# TODO: parse event_ts to timestamp and add event_date.

# COMMAND ----------
# 2) Write stream to Delta with checkpointing
# TODO: write to out_path in append mode with checkpoint_path.
# query = (stream_df.writeStream
#          .format("delta")
#          .outputMode("append")
#          .option("checkpointLocation", checkpoint_path)
#          .start(out_path))

# COMMAND ----------
# 3) Exercise prompts
# TODO:
# - Add a de-duplication strategy for streaming (e.g., dropDuplicates with watermark)
# - Discuss handling late events (event-time watermarks)
