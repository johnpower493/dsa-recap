# Capstone Project: End-to-End Data Product (Snowflake + dbt)

This capstone ties together **ingestion, modeling, governance, observability, and cost control** into a production-style data product.

## Scenario

You are building a **Customer 360** data product that powers analytics, marketing activation, and churn modeling.

### Data Sources

- CRM system (batch export)
- Web events (streaming)
- Support tickets (API ingestion)

### Deliverables

1. **Bronze → Silver → Gold** modeled data
2. **dbt DAG** with staging/intermediate/marts
3. **Snowflake governance** (RBAC + masking)
4. **Observability** (freshness, anomalies, SLAs)
5. **Cost governance** (warehouse isolation, monitors)
6. **Data contract** for `customer_master`

## What to Build

- Ingestion pattern for 3 sources
- dbt models for Customer 360 (facts + dimensions)
- Data contract + ownership
- CI/CD pipeline for dbt
- Observability checks + incident runbook

## Suggested Folder Structure

```
capstone_project/
  architecture.md
  data_contract.md
  cicd_pipeline.md
  incident_runbook.md
  cost_governance.md
```
