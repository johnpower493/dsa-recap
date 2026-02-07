# Cost & Performance Observability (Snowflake)

This module provides **SQL queries** you can use to build dashboards for:

- Warehouse cost and credit usage
- Cost by workload/tag
- Top expensive queries and performance bottlenecks
- Concurrency and queuing issues

## Data Sources (Snowflake)

- `ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`
- `ACCOUNT_USAGE.QUERY_HISTORY`
- `ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY`
- `ACCOUNT_USAGE.TAG_REFERENCES`

## Usage

1. Run the SQL in `cost_observability_queries.sql`
2. Load results into a BI dashboard or monitoring tool
3. Use the insights to optimize warehouse sizing and query patterns
