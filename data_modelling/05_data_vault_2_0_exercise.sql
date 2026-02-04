-- Exercise 05: Data Vault 2.0 (Hubs, Links, Satellites)
--
-- Goal: map the same domain (customers, products, orders) into a Data Vault style model.
-- This is a conceptual exercise using Postgres tables.

SET search_path = de_demo, public;

-- TODO 1) Create a schema (e.g., de_vault).
--
-- TODO 2) Define Hubs:
--   - hub_customer (hk_customer, customer_nk=email, load_dts, record_source)
--   - hub_product  (hk_product,  product_nk=sku,   load_dts, record_source)
--   - hub_order    (hk_order,    order_nk=order_id, load_dts, record_source)
--
--   Hint: use a deterministic hash for hk_* (md5) over the natural key.
--
-- TODO 3) Define a Link:
--   - link_order_line (hk_order_line, hk_order, hk_customer, hk_product, load_dts, record_source)
--
-- TODO 4) Define Satellites:
--   - sat_customer (hk_customer, load_dts, full_name, created_at, record_source)
--   - sat_product  (hk_product,  load_dts, product_name, category, list_price_cents, record_source)
--
-- TODO 5) Load the vault from de_demo.dim_customers, de_demo.dim_products, and purchase events.
--
-- TODO 6) Write a "business vault" style query: daily revenue by category.
--   (This will require stitching hubs/links/sats together.)
