# CI/CD Pipeline (dbt + Snowflake)

## Goals

- Validate dbt models and tests on PR
- Prevent breaking changes from reaching prod
- Deploy docs + exposures

## Suggested Stages

1. **Lint & Unit Tests**
   - Run SQL linting (sqlfluff)
   - Run lightweight dbt tests

2. **Slim CI**
   - `dbt build --defer --state` for impacted models only

3. **Docs Generation**
   - `dbt docs generate`

4. **Deploy**
   - Merge to main triggers prod build

## Example Workflow (Pseudo)

```yaml
jobs:
  dbt_ci:
    steps:
      - run: dbt deps
      - run: dbt build --select state:modified+ --defer --state ./state
      - run: dbt docs generate
```
