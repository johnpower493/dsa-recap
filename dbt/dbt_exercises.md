# dbt Senior Exercises

## 1) Layered DAG Design

Create a small dbt project with:

- `staging/` models (light cleaning, type casting)
- `intermediate/` models (joins, business logic)
- `marts/` models (final facts/dims)

**Goal:** Ensure clear ownership and naming conventions.

---

## 2) Incremental Strategy Comparison

Implement two incremental models for the same dataset:

- **Merge-based**: update + insert
- **Append-only**: insert new records only

Compare **runtime** and **warehouse cost** between the two.

---

## 3) Testing at Scale

- Add schema tests (`not_null`, `unique`, `relationships`)
- Create a custom test for row count variance
- Run tests with `--defer --state` to simulate slim CI

---

## 4) Exposures + Docs

- Add exposures for dashboards and ML models
- Generate dbt docs and inspect lineage
