-- Exercise 04: Aggregate facts and rollups
--
-- Goal: create an aggregate fact table to speed common BI queries.
-- We'll build a daily revenue aggregate and discuss freshness/backfill concerns.

SET search_path = de_demo, public;

-- TODO 1) Create a table agg_daily_revenue with grain: one row per day.
--   Columns: date_id, orders, revenue_cents, customers, loaded_at.
--
-- TODO 2) Populate it from fact_sales.
--
-- TODO 3) Simulate an incremental refresh strategy:
--   - Recompute only the last N days (choose N=7) using INSERT ... ON CONFLICT DO UPDATE.
--
-- TODO 4) Compare query patterns:
--   a) BI query against vw_daily_revenue (from fact table)
--   b) BI query against agg_daily_revenue
