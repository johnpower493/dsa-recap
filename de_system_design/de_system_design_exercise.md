# Data Engineering System Design - Exercises

Use these prompts to practice designing production-ready data platforms.

## Exercise 1: CDC Pipeline (OLTP -> Lakehouse)

Design a pipeline that captures changes from a transactional Postgres DB and lands them in bronze/silver/gold tables.

Include:
- source assumptions (tables, volume, change rate)
- ingestion approach (log-based CDC vs batch extract)
- schema evolution handling
- dedup + ordering guarantees
- idempotency strategy
- late/out-of-order event handling
- monitoring + alerting

---

## Exercise 2: Batch vs Streaming Tradeoff

You need metrics for operations dashboards:
- Freshness target: under 5 minutes
- Daily cost budget: strict

Design two alternatives:
1. Micro-batch
2. True streaming

For each, define:
- architecture components
- SLA/SLO expectations
- failure modes
- operational complexity
- cost profile

Conclude with a recommendation.

---

## Exercise 3: Backfill Strategy at Scale

A bug caused incorrect transformations for 90 days of history.

Design a safe backfill plan that addresses:
- replay scope and partitioning plan
- compute scaling and throttling
- impact on downstream consumers
- reconciliation checks before/after
- rollback plan

---

## Exercise 4: Data Quality + Contracts

You own a `customer_orders` gold dataset consumed by BI + ML.

Define:
- data contract fields and constraints
- freshness, completeness, and uniqueness checks
- severity levels (warn vs fail)
- ownership + on-call model
- change management process for schema updates

---

## Exercise 5: Incident Drill

Scenario:
- Bronze ingestion lag spikes from 5 minutes to 3 hours.

Write a runbook with:
1. Detection signals
2. Immediate triage steps
3. Mitigation actions
4. Communication template
5. Post-incident follow-up actions
