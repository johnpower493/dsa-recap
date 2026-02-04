-- Exercise 09: Metrics / Semantic layer pattern ("metric views")
--
-- Goal: define consistent KPI logic once and reuse it.
-- We'll create a metrics view that standardizes definitions like revenue, orders, AOV.
--
-- Prereq: 01_dimensional_star_schema_solution.sql

SET search_path = de_demo, public;

-- TODO 1) Create a view de_analytics.vw_metrics_daily with:
--   date_id
--   revenue_cents
--   orders
--   customers
--   aov_cents (revenue_cents / orders, guarded for divide-by-zero)
--
-- TODO 2) Write 2 downstream queries that reuse vw_metrics_daily:
--   a) MTD revenue
--   b) Weekend vs weekday revenue
--
-- TODO 3) Add a comment explaining why centralizing metric definitions helps (governance/consistency).
