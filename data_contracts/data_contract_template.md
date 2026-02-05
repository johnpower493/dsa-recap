# Data Contract Template (Example)

## Dataset

- **Name:** customer_master
- **Owner:** Data Platform Team
- **Consumers:** Analytics, CRM, Marketing

## Schema

| Column | Type | Description | PII | Nullable |
|--------|------|-------------|-----|----------|
| customer_id | string | Unique customer identifier | No | No |
| email | string | Customer email | Yes | Yes |
| created_at | timestamp | Account creation time | No | No |
| status | string | Account status | No | No |

## SLA / SLO

- **Freshness:** < 2 hours latency
- **Availability:** 99.5% monthly
- **Backfill Policy:** Replay allowed up to 30 days

## Change Management

- **Schema Versioning:** Semantic versioning (MAJOR.MINOR.PATCH)
- **Breaking Change:** Removal/renaming of a column
- **Deprecation Window:** 60 days

## Data Quality Checks

- `customer_id` uniqueness
- `status` in allowed values
- Null rate thresholds on PII columns
