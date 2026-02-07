# Data Quality & Freshness Framework

This module provides **reusable patterns** for data quality checks, freshness SLAs, and anomaly detection.

## Core Checks

- **Completeness:** null checks, required fields
- **Uniqueness:** primary key uniqueness
- **Validity:** accepted value lists, ranges
- **Freshness:** last update within SLA
- **Volume:** row count variance

## Suggested Workflow

1. Define DQ rules per dataset
2. Implement checks in SQL (or dbt tests)
3. Store results in a DQ audit table
4. Alert on SLA breaches

## Files

- `dq_checks.sql` → reusable SQL patterns
- `dq_rules_template.md` → checklist for new datasets
