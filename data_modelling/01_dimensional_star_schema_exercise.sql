-- Exercise 01: Dimensional modelling (Star schema)
--
-- Goal: build a small star schema for analytics from the existing de_demo tables.
-- You will create a date dimension, a sales fact, and a couple of useful views.
--
-- Setup: run sql/00_setup_schema_and_seed.sql first.

SET search_path = de_demo, public;

-- TODO 1) Create a dedicated schema to hold analytical objects (e.g., de_analytics).
--   - Use CREATE SCHEMA IF NOT EXISTS ...
--   - Set search_path so subsequent objects land there.

-- TODO 2) Create a dim_date table.
--   Suggested columns:
--     date_id (DATE primary key)
--     year, month, day
--     month_start_date, quarter_start_date
--     is_weekend
--
--   Populate it for the date range covering the demo data (at least 2026-01-01 to 2026-01-31).
--   Hint: generate_series.

-- TODO 3) Create a fact_sales table at the grain: one row per order line.
--   Source: de_demo.fact_events where event_type = 'purchase'.
--   Suggested columns:
--     sales_id (surrogate PK)
--     order_id (from event_payload->>'order_id')
--     order_ts
--     date_id (FK to dim_date)
--     customer_id (FK to de_demo.dim_customers)
--     product_id  (FK to de_demo.dim_products)
--     gross_amount_cents
--     source_system
--
--   Requirements:
--   - Enforce NOT NULL where it makes sense for this grain.
--   - Ensure gross_amount_cents is non-negative.

-- TODO 4) Load fact_sales from the source events.
--   - Only purchase events.
--   - Use order_id from JSON.
--   - If order_id is missing, decide how you want to handle it (reject row vs. derive).

-- TODO 5) Create a semantic view vw_daily_revenue with:
--   date_id, orders, revenue_cents, customers
--
-- TODO 6) Validate with a couple of checks:
--   - Total revenue in the view equals sum of purchase amounts in fact_events.
--   - Row counts look reasonable.
