-- Snowflake Advanced Exercises
-- ------------------------------------------------------------
-- Audience: Senior Data Engineers / Analytics Engineers
-- Goal: Practice governance, cost/performance, release safety,
--       and native Snowflake orchestration patterns.
--
-- Notes:
-- 1) Replace object names (DB/SCHEMA/ROLE/WH) to match your account.
-- 2) Some features require ACCOUNTADMIN/SECURITYADMIN privileges.
-- 3) Run in a non-production environment.

/* ============================================================
   Exercise 0: Environment bootstrap (optional)
   ============================================================
   Create a lab area and sample structures used by later exercises.

   TODO:
   - Create database + schemas: RAW, CURATED, MART, GOVERNANCE
   - Create warehouses: ETL_WH, BI_WH
   - Seed at least one source table: RAW.ORDERS_LANDING
*/


/* ============================================================
   Exercise 1: Policy-driven security (RBAC + masking + row access)
   ============================================================
   Scenario:
   You need least-privilege access for analysts while protecting PII
   and enforcing region-based row filtering.

   TODO:
   1) Create role hierarchy:
      - DATA_ENGINEER_ROLE
      - ANALYST_ROLE
      - PII_ACCESS_ROLE
   2) Grant object privileges so ANALYST_ROLE can query curated models.
   3) Add a masking policy for customer_email.
      - Show clear text only for PII_ACCESS_ROLE.
   4) Add a row access policy for region filtering.
      - NA analysts see NA rows; APAC analysts see APAC rows.
   5) Attach policies to CURATED.CUSTOMERS.
   6) Demonstrate policy behavior using role switching.
*/


/* ============================================================
   Exercise 2: Governance metadata with tags
   ============================================================
   Scenario:
   You want metadata-driven governance for sensitive columns and
   cost attribution by domain.

   TODO:
   1) Create tags:
      - DATA_CLASSIFICATION (PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED)
      - DOMAIN (SALES, FINANCE, PRODUCT)
   2) Apply tags to tables/columns in CURATED and MART schemas.
   3) Query tag references to produce a governance inventory report.
   4) Bonus: create a policy requiring RESTRICTED data to use masking.
*/


/* ============================================================
   Exercise 3: Cost governance + workload isolation
   ============================================================
   Scenario:
   BI users run bursty dashboards while ETL jobs run hourly.

   TODO:
   1) Configure ETL_WH and BI_WH with different sizing/autosuspend.
   2) Create a resource monitor with threshold triggers:
      - 75% notify
      - 90% notify
      - 100% suspend
   3) Attach monitor to BI_WH only.
   4) Build a query to report warehouse credit trends by day.
   5) Propose one optimization to reduce BI credits by >=20%.
*/


/* ============================================================
   Exercise 4: Query observability and performance tuning
   ============================================================
   Scenario:
   A dashboard query is consistently slow.

   TODO:
   1) Use ACCOUNT_USAGE.QUERY_HISTORY to find top slow queries.
   2) For one candidate query, inspect:
      - total_elapsed_time
      - bytes_scanned
      - partitions_scanned/total
      - spill metrics (if available)
   3) Apply at least 2 optimizations from:
      - clustering key
      - search optimization service
      - materialized view
      - pre-aggregation table
   4) Re-run and compare before/after metrics.
   5) Document when each optimization is most appropriate.
*/


/* ============================================================
   Exercise 5: Zero-copy clone + Time Travel release pattern
   ============================================================
   Scenario:
   You need a safe deployment strategy for schema changes.

   TODO:
   1) Clone CURATED schema to CURATED_RELEASE_CANDIDATE.
   2) Apply a breaking change in clone (e.g., column type alteration).
   3) Validate downstream model behavior.
   4) Simulate rollback using Time Travel/UNDROP or clone swap pattern.
   5) Write a short release runbook (steps + rollback decision point).
*/


/* ============================================================
   Exercise 6: Incremental ELT with Streams + Tasks
   ============================================================
   Scenario:
   RAW.ORDERS_LANDING receives upserts and deletes.
   Build an incremental pipeline to CURATED.ORDERS.

   TODO:
   1) Create stream on RAW.ORDERS_LANDING.
   2) Create MERGE logic to apply INSERT/UPDATE/DELETE into CURATED.ORDERS.
   3) Wrap MERGE in a task running every 5 minutes.
   4) Add a downstream task to refresh MART.ORDERS_DAILY.
   5) Configure task dependency graph and resume tasks.
   6) Add observability query for task history and failures.
*/


/* ============================================================
   Exercise 7: Dynamic tables for declarative transformations
   ============================================================
   Scenario:
   Replace part of streams/tasks pipeline with dynamic tables.

   TODO:
   1) Create dynamic table CURATED.DT_ORDERS_CLEAN with TARGET_LAG.
   2) Create dynamic table MART.DT_ORDERS_DAILY from curated layer.
   3) Inspect refresh history and lag behavior.
   4) Compare with exercise 6:
      - operational complexity
      - freshness control
      - cost profile
*/


/* ============================================================
   Exercise 8: Secure data sharing
   ============================================================
   Scenario:
   External partner needs read-only access to aggregated sales.

   TODO:
   1) Create secure view MART.V_PARTNER_SALES (no direct base table access).
   2) Create share and grant usage/select on required objects.
   3) Add a reader account (or document provider-consumer setup).
   4) Validate only intended columns/rows are visible.
   5) Bonus: add row-level filter for partner-specific region.
*/


/* ============================================================
   Stretch Challenge
   ============================================================
   Build an end-to-end architecture proposal that combines:
   - RBAC + masking + tags
   - warehouse isolation + monitors
   - incremental ingestion (stream/task or dynamic table)
   - secure partner sharing
   - observability dashboards (query + cost + task health)

   Deliverable:
   - 1-page architecture summary
   - SQL snippets for key controls
   - trade-off discussion (simplicity vs flexibility vs cost)
*/
