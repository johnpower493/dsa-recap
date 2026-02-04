-- Exercise 08: SCD Type 2 dimension history + as-of joins
--
-- Goal: represent changing attributes over time and join facts "as of" the event date.
-- We'll create a customer history dimension and simulate a name change.
--
-- Prereq: 01_dimensional_star_schema_solution.sql (fact_sales exists)

SET search_path = de_demo, public;

-- TODO 1) Create de_analytics.dim_customer_scd2 with columns:
--   customer_sk (surrogate PK)
--   customer_id (natural key to de_demo.dim_customers)
--   full_name
--   valid_from (timestamptz)
--   valid_to (timestamptz)
--   is_current
--
-- TODO 2) Seed it from de_demo.dim_customers as a single current row per customer.
--
-- TODO 3) Simulate a change: for ben@example.com set name to 'Benjamin Kim'
--   effective at '2026-01-11 00:00+00' (expire old row, insert new current row).
--
-- TODO 4) Write an as-of join that returns each sale with the customer name at order_ts.
--   Condition: order_ts >= valid_from AND order_ts < valid_to
