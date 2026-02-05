# Capstone Data Contract (Customer 360)

## Dataset

- **Name:** customer_master
- **Owner:** Customer Analytics
- **Consumers:** Marketing, Growth, Support

## Schema (v1.0.0)

| Column | Type | Description | PII | Nullable |
|--------|------|-------------|-----|----------|
| customer_id | string | Unique ID | No | No |
| email | string | Customer email | Yes | Yes |
| created_at | timestamp | Account creation time | No | No |
| status | string | Active/Inactive | No | No |
| lifetime_value | number | Total spend | No | Yes |

## SLAs

- **Freshness:** < 2 hours
- **Availability:** 99.5% monthly
- **Backfill:** 30 days

## Change Management

- Semantic versioning
- Breaking change = remove or rename a column
- Deprecation window: 60 days
