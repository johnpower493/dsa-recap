# Data Engineering Learning Checklist (Snowflake + dbt Focus)

This checklist highlights **senior-level competencies** beyond fundamentals. Use it to guide learning, project work, and interview prep.

## 1) Platform Engineering (Snowflake)

- [ ] Design Snowflake account architecture (roles, databases, warehouses)
- [ ] Configure warehouse sizing and auto-suspend policies for cost control
- [ ] Implement resource monitors + credit governance
- [ ] Secure data sharing (internal/external) + reader accounts
- [ ] Query profile analysis and performance tuning
- [ ] Clustering strategies + pruning awareness
- [ ] Data retention + time travel strategies

## 2) Advanced dbt Engineering

- [ ] DAG design patterns (staging → intermediate → marts)
- [ ] Incremental models with merge strategies
- [ ] Macro development and reusable patterns
- [ ] CI/CD with defer/state and slim CI
- [ ] Exposures + documentation as product tooling
- [ ] Performance tuning (materializations, ephemeral vs incremental)

## 3) Governance & Security

- [ ] Row access policies + column masking + tag-based access
- [ ] PII tagging and retention policy enforcement
- [ ] Audit logging (access, changes, data sharing)
- [ ] Least privilege access design

## 4) Observability & Reliability

- [ ] Freshness and volume anomaly detection
- [ ] SLA/SLO definitions and alerting
- [ ] Incident response runbooks
- [ ] End-to-end lineage (dbt + Snowflake metadata)

## 5) Data Contracts & Product Thinking

- [ ] Schema versioning + breaking change strategy
- [ ] Producer/consumer contracts and ownership
- [ ] Data product SLAs + documentation
- [ ] Domain-aligned data products (data mesh thinking)

## 6) Production Engineering Practices

- [ ] Automated testing in PR pipelines
- [ ] Backfill and replay strategies
- [ ] Disaster recovery and rollback planning
- [ ] Cost/performance monitoring dashboards

---

## How to use this repository

Each folder below contains **notes + exercises** to build these senior-level skills:

- `snowflake/`
- `dbt/`
- `observability/`
- `data_contracts/`
