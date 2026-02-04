-- Exercise 07: Kimball bus matrix + conformed dimensions (multi-fact constellation)
--
-- Goal: model multiple facts that share conformed dimensions.
-- We'll create a second fact for engagement (page views) that conforms on dim_date and dim_customer.
--
-- Prereqs:
--   - 01_dimensional_star_schema_solution.sql (dim_date, fact_sales)
--   - 03_factless_facts_and_bridges_solution.sql (fact_page_view)

SET search_path = de_demo, public;

-- TODO 1) Create a new fact table fact_customer_daily_engagement (grain: customer-day).
--   Columns: date_id, customer_id, page_views, product_page_views
--   - product_page_views counts only paths like '/product/SKU-...'
--
-- TODO 2) Populate it from de_analytics.fact_page_view.
--
-- TODO 3) Create a "bus" style view vw_customer_day that joins:
--   dim_date + dim_customers + fact_sales (aggregated to customer-day) + fact_customer_daily_engagement
--
-- TODO 4) Query: show each customer's revenue + page_views by day.
