# Snowflake (Senior-Level + Advanced)

This module focuses on **platform engineering, governance, performance, and production-grade data platform patterns** in Snowflake.

## Core Topics

- Account + database architecture
- Role hierarchy and least-privilege RBAC
- Dynamic data masking, row access policies, and tags
- Warehouse sizing, workload isolation, and credit governance
- Query profiling and performance tuning
- Clustering, search optimization, and materialized views
- Zero-copy cloning and Time Travel for safe release workflows
- Streams + Tasks for CDC-style incremental pipelines
- Dynamic tables for declarative transformations
- Secure data sharing (reader and listing patterns)

## Files

- `snowflake_exercises.sql` — foundational senior-level exercises
- `snowflake_advanced_exercises.sql` — advanced scenario-driven labs
- `snowflake_advanced_solutions.sql` — reference solutions for advanced labs

## Advanced Labs (Recommended Order)

1. **Policy-Driven Security**
   - Build role hierarchy
   - Implement masking + row access policies
   - Use tags/classification for governance metadata

2. **Cost + Performance Engineering**
   - Warehouse right-sizing and monitor triggers
   - Query profile analysis from `ACCOUNT_USAGE.QUERY_HISTORY`
   - Evaluate clustering vs search optimization vs materialized views

3. **Data Lifecycle + Release Safety**
   - Time Travel + zero-copy clone for safe testing
   - Rehearse rollback strategy with clone/swap pattern

4. **Incremental ELT Orchestration**
   - Implement stream + task DAG (bronze/silver style)
   - Add idempotency checks and task observability

5. **Near Real-Time Serving**
   - Build dynamic tables with target lag
   - Compare dynamic tables vs streams/tasks trade-offs

## Outcomes

By completing this module, you should be able to:

- Design production-ready Snowflake security and governance controls
- Build cost-aware, performant warehouse and query strategies
- Implement robust incremental pipelines using native Snowflake primitives
- Apply safe release and rollback patterns for analytics engineering
