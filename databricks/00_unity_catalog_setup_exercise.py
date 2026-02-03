# Databricks notebook source
# Exercise 00: Unity Catalog setup (optional)
#
# Goal:
# - Create/select a catalog + schema (database)
# - Verify Unity Catalog is enabled
# - Create a managed Delta table
#
# Notes:
# - You need privileges to CREATE CATALOG/SCHEMA.
# - If you can't create a catalog, you can usually create a schema in an existing catalog.

# COMMAND ----------
# Widgets for customization
try:
    dbutils.widgets.text("catalog", "de_training")
    dbutils.widgets.text("schema", "databricks_exercises")
except Exception:
    pass

catalog = dbutils.widgets.get("catalog") if "dbutils" in globals() else "de_training"
schema = dbutils.widgets.get("schema") if "dbutils" in globals() else "databricks_exercises"

# COMMAND ----------
# 1) Detect whether Unity Catalog is available
# If this fails, you're likely on a workspace without UC.
try:
    spark.sql("SHOW CATALOGS").display()
    uc_available = True
except Exception as e:
    uc_available = False
    print("Unity Catalog not available in this workspace/cluster:", e)

# COMMAND ----------
if uc_available:
    # 2) Create catalog/schema (best effort)
    # If CREATE CATALOG fails, choose an existing catalog and only create schema.
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    except Exception as e:
        print("CREATE CATALOG failed (using existing catalog instead). Error:", e)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"USE SCHEMA {schema}")

    # 3) Create a managed table
    spark.sql(
        """
        CREATE OR REPLACE TABLE demo_customers (
          customer_id STRING,
          name STRING
        )
        USING DELTA
        """
    )

    spark.sql("INSERT INTO demo_customers VALUES ('c1','Ava'),('c2','Ben')")
    spark.sql("SELECT * FROM demo_customers").display()

# COMMAND ----------
# Exercise prompts:
# TODO:
# - Change catalog/schema via widgets and re-run
# - Inspect table details:
#     DESCRIBE EXTENDED demo_customers;
# - If you have access, try GRANTing SELECT on the schema/table to another principal
