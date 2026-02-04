-- Exercise 06: One Big Table (OBT) / Wide Table
--
-- Goal: create a denormalized, analysis-ready wide table for a specific BI use-case.
-- This is common in ELT/BI where you trade storage for simpler queries and fewer joins.
--
-- Prereq: run 01_dimensional_star_schema_solution.sql to create de_analytics.fact_sales.

SET search_path = de_demo, public;

-- TODO 1) Create de_analytics.obt_sales_lines with grain: one row per order line.
--   Include a mix of facts + denormalized dimension attributes, e.g.:
--     order_id, order_ts, date_id
--     customer_id, customer_email, customer_name
--     product_id, sku, product_name, category
--     gross_amount_cents, source_system
--
-- TODO 2) Load it from de_analytics.fact_sales joined to de_demo dims.
--
-- TODO 3) Add at least one index that matches your most common filter pattern.
--
-- TODO 4) Write a BI query using ONLY the OBT (no joins): revenue by category by day.
--
-- TODO 5) In a comment: name one downside (e.g., duplication / update complexity / slower rebuilds).
