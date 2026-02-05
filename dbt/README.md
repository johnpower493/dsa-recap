# dbt (Senior-Level Focus)

This module focuses on **advanced dbt engineering**, including DAG design, incremental strategies, CI/CD, and documentation as a product.

## Core Topics

- Layered model design (staging → intermediate → marts)
- Incremental models (merge vs append)
- Macros and reusable patterns
- CI/CD with defer/state and slim CI
- Testing strategy at scale
- Exposures + documentation

## Exercises

1. **Layered DAG Design**
   - Build staging, intermediate, and marts models
   - Enforce naming and folder conventions

2. **Incremental Strategy Comparison**
   - Build two incremental models (merge vs append)
   - Measure runtime and cost

3. **Testing at Scale**
   - Add schema tests and custom tests
   - Validate tests in CI (defer/state)

4. **Exposures + Docs**
   - Define exposures for dashboards and ML models
   - Generate docs site and review lineage
