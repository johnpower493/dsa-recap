# Incident Runbook (Data Freshness Failure)

## Trigger

- Freshness SLA breach for `customer_master`
- Alert fired from observability checks

## Immediate Actions

1. **Acknowledge alert** and notify stakeholders
2. **Check ingestion logs** for upstream failures
3. **Verify latest load timestamp**
4. **Assess downstream impact** (dashboards, ML features)

## Root Cause Analysis

- Source system outage?
- Ingestion job failure?
- Schema change breaking model?

## Recovery Steps

- Re-run ingestion or backfill
- Validate row counts and DQ checks
- Rebuild impacted dbt models

## Post-Incident

- Write a postmortem
- Add automated detection or guardrails
- Update runbook if new root cause
