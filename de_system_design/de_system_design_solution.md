# Data Engineering System Design - Reference Answers

Use these as example solution outlines. In interviews, adapt depth to constraints and audience.

## 1) CDC Pipeline (OLTP -> Lakehouse)

### Proposed architecture
- Source DB (Postgres) with WAL-based CDC (e.g., Debezium)
- Kafka topics per table (keyed by PK)
- Bronze: append-only raw CDC events with metadata (`op`, `ts_ms`, `source_lsn`)
- Silver: deduplicated latest-state or change-log normalized model
- Gold: business-friendly marts (facts/dims, curated metrics)

### Key design choices
- **Ordering**: preserve per-key ordering via Kafka partitioning by PK
- **Dedup**: use `(pk, source_lsn)` or `(pk, op_ts, sequence)` as deterministic key
- **Idempotency**: MERGE into Silver on unique event keys
- **Schema evolution**: registry + compatibility checks, additive-first changes
- **Late events**: watermark and conflict-resolution rules (event time precedence)
- **Monitoring**: ingestion lag, topic backlog, null-rate drift, schema mismatch alerts

---

## 2) Batch vs Streaming Tradeoff

### A) Micro-batch (e.g., every 2-5 min)
- Lower operational complexity
- Easier replay/debugging
- Usually cheaper compute
- Meets &lt;5 min freshness with careful scheduling

### B) True streaming
- Best latency (seconds)
- More complex state, checkpoint, and exactly-once semantics
- Higher engineering/on-call overhead

### Recommendation
Given strict budget and 5-min freshness target, choose **micro-batch** first.
Move to streaming only for use-cases requiring sub-minute latency.

---

## 3) Backfill Strategy (90 days)

1. Freeze affected downstream publishes or version outputs (`v2_backfill`).
2. Recompute by partition windows (e.g., daily), oldest to newest.
3. Throttle parallelism to protect shared clusters.
4. Run reconciliation checks per partition:
   - row counts
   - key uniqueness
   - aggregate deltas within tolerance
5. Promote validated partitions, then atomically switch consumers.
6. Keep rollback path via previous table version/snapshot.

---

## 4) Data Quality + Contracts (`customer_orders`)

### Contract examples
- `order_id`: non-null, unique
- `customer_id`: non-null, valid FK domain
- `order_ts`: non-null, UTC timestamp
- `order_amount`: non-null, `>= 0`

### Checks
- Freshness: latest partition within SLA (e.g., 30 min)
- Completeness: critical columns null-rate &lt; 0.1%
- Uniqueness: duplicate PK count = 0
- Volume anomaly: day-over-day row delta threshold

### Ops model
- Severity: warn (ticket), fail (page on-call)
- Ownership: data product owner + platform on-call escalation
- Change management: PR + contract versioning + consumer notice window

---

## 5) Incident Drill (Ingestion lag 5 min -> 3 hours)

### Detection
- Lag SLO breach alert
- Consumer offset backlog spike
- Delayed partition arrivals

### Triage
1. Confirm scope (all tables vs subset)
2. Check source connectivity and CDC connector health
3. Inspect queue throughput and error logs

### Mitigation
- Restart/scale affected connectors/workers
- Temporarily increase cluster resources
- Prioritize critical topics/pipelines

### Comms template
- Impacted datasets, ETA, workaround, next update time

### Post-incident
- Root cause analysis
- Permanent fix + alert tuning
- Runbook updates and game-day follow-up
