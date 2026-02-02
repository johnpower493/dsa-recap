-- SQL Exercises (Postgres)
--
-- Prereq:
--   \i sql/00_setup_schema_and_seed.sql
--
-- These are prompts; write your answers below each prompt.

SET search_path = de_demo, public;

-- 1) Incremental load (watermark)
-- Write a query to return fact_events strictly newer than a parameter :last_loaded_ts
-- and ordered by event_ts.
--
-- Answer:
-- SELECT ...


-- 2) Dedup events
-- If multiple events have the same (source_system, event_ts, event_type, customer_id, product_id, amount_cents),
-- keep the most recent by event_id.
--
-- Answer:


-- 3) Customer revenue report
-- Produce a table: email, first_purchase_ts, lifetime_net_revenue_cents
-- Net revenue includes purchase + refund.
-- Include customers with no purchases.
--
-- Answer:


-- 4) Data quality query
-- Find purchases where amount_cents is null (should be zero due to constraint).
--
-- Answer:
