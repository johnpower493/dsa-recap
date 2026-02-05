# Capstone Architecture (High Level)

## C4 Container View (Mermaid)

```mermaid
C4Container
title Capstone - Customer 360 Platform

Person(data_consumer, "Data Consumer")
System_Boundary(platform, "Data Platform") {
  Container(ingest_batch, "Batch Ingestion", "Snowflake + Staging", "CRM exports")
  Container(ingest_stream, "Streaming Ingestion", "Kafka/Snowpipe", "Web events")
  Container(ingest_api, "API Ingestion", "Lambda/Functions", "Support tickets")
  Container(raw, "Raw Zone", "Snowflake", "Bronze data")
  Container(transform, "dbt Transformations", "dbt", "Silver/Gold models")
  Container(curated, "Curated Marts", "Snowflake", "Customer 360 datasets")
  Container(serve, "Serving", "BI/Reverse ETL", "Dashboards + activation")
  Container(obs, "Observability", "DQ + Monitoring", "Freshness, anomalies")
  Container(sec, "Security/Governance", "Policies", "RBAC, masking")
}

Rel(ingest_batch, raw, "Loads")
Rel(ingest_stream, raw, "Streams")
Rel(ingest_api, raw, "Loads")
Rel(raw, transform, "Reads")
Rel(transform, curated, "Writes")
Rel(curated, serve, "Serves")
Rel(obs, transform, "Monitors")
Rel(sec, curated, "Controls access")
Rel(data_consumer, serve, "Consumes")
```

## Key Decisions

- Snowflake as primary store (raw → curated)
- dbt for transformations and testing
- Dedicated warehouses for ETL vs BI
- SLA-driven observability and runbooks
