-- Exercise 03: Factless facts and bridge tables
--
-- Goal: model non-monetary events (page views) and a many-to-many relationship.
-- We'll build:
--   - fact_page_view (factless: measures are counts)
--   - bridge_customer_product_interest (derived interest edges from views)

SET search_path = de_demo, public;

-- TODO 1) Create fact_page_view with grain: one row per page_view event.
--   Include: event_id (lineage), view_ts, date_id, customer_id (nullable), path.
--
-- TODO 2) Load it from de_demo.fact_events where event_type='page_view'.
--
-- TODO 3) Create a bridge table bridge_customer_product_interest with grain:
--   (customer_id, product_id, first_seen_ts, last_seen_ts, view_count)
--
--   Hint: infer product_id by parsing SKU from paths like '/product/SKU-200' and joining to dim_products.
--
-- TODO 4) Write a query that lists each customer and their top "interested" product by view_count.
