# Databricks notebook source
# Mini Project - 00: Setup + generate landing data (JSON)
#
# Goal:
# - Create a landing zone folder in DBFS
# - Write several micro-batches of JSON events
# - Include duplicates and bad rows for DLT expectations

# COMMAND ----------
from datetime import datetime, timezone
import json

# COMMAND ----------
user = spark.sql("select current_user()").first()[0].replace("@", "_").replace(".", "_")
base_path = f"dbfs:/tmp/de_databricks/{user}/mini_project"
landing_path = f"{base_path}/landing/events_json"

print("base_path:", base_path)
print("landing_path:", landing_path)

dbutils.fs.mkdirs(landing_path)

# COMMAND ----------
# Helper to write a micro-batch file

def write_batch(batch_id: str, records: list[dict]) -> None:
    content = "\n".join(json.dumps(r) for r in records)
    dbutils.fs.put(f"{landing_path}/{batch_id}.json", content, overwrite=True)


# COMMAND ----------
# Batch 1: valid + duplicates
write_batch(
    "batch_001",
    [
        {"event_id": "e1", "event_ts": "2026-01-01T00:00:00Z", "event_type": "page_view", "amount_cents": None},
        {"event_id": "e2", "event_ts": "2026-01-01T00:01:00Z", "event_type": "purchase", "amount_cents": 2999},
        {"event_id": "e2", "event_ts": "2026-01-01T00:01:00Z", "event_type": "purchase", "amount_cents": 2999},  # dup
    ],
)

# Batch 2: include a bad row (purchase missing amount)
write_batch(
    "batch_002",
    [
        {"event_id": "e3", "event_ts": "2026-01-02T12:00:00Z", "event_type": "purchase", "amount_cents": 499},
        {"event_id": "e_bad", "event_ts": "2026-01-02T12:01:00Z", "event_type": "purchase", "amount_cents": None},
    ],
)

# Batch 3: late arriving event for day 1
write_batch(
    "batch_003",
    [
        {"event_id": "e4", "event_ts": "2026-01-01T23:59:00Z", "event_type": "refund", "amount_cents": -499},
    ],
)

print("Wrote 3 batches into landing zone")

# COMMAND ----------
# Inspect landing files
display(dbutils.fs.ls(landing_path))
