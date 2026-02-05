# Cost Governance (Snowflake)

## Objectives

- Isolate workloads (ETL vs BI)
- Control warehouse spend
- Provide cost transparency

## Practices

- Separate warehouses per workload
- Auto-suspend aggressively (2–5 min)
- Resource monitors + alerts
- Tagging for chargeback

## Exercises

1. Create warehouses for ETL and BI with distinct sizes
2. Apply a resource monitor to each warehouse
3. Build a cost dashboard from `warehouse_metering_history`
