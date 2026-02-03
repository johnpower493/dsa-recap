# Databricks notebook source
# Exercise 07: Delta Live Tables (DLT) concepts (conceptual notebook)
#
# Goals:
# - Understand DLT pipeline structure
# - Expectations (data quality)
# - Incremental processing semantics
#
# This notebook is mostly prompts because DLT runs in a pipeline context.

# COMMAND ----------
# Prompts:
# 1) Create a DLT pipeline with:
#    - bronze table reading raw JSON
#    - silver table with expectations (drop/ fail on bad rows)
#    - gold aggregate table
#
# 2) Add an expectation:
#    - amount_cents IS NOT NULL for purchase/refund
#
# 3) Observe lineage and automatic retries.

# Useful references:
# - dlt.table
# - dlt.expect, dlt.expect_or_drop, dlt.expect_or_fail
#
# TODO: Write the DLT python code blocks as if this notebook were in a DLT pipeline.
