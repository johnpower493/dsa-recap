-- Exercise 02: Star vs Snowflake
--
-- Goal: explore when/why you'd normalize dimensions (snowflake) vs keep a wide dimension (star).
-- We'll model product category as its own dimension and compare query ergonomics.

SET search_path = de_demo, public;

-- TODO 1) In an analytics schema, create:
--   - dim_product (copied/derived from de_demo.dim_products but WITHOUT the category column)
--   - dim_category (category_id surrogate key, category_name natural key)
--   - Ensure dim_product has category_id FK.
--
-- TODO 2) Backfill dim_category and dim_product from de_demo.dim_products.
--
-- TODO 3) Write two queries that compute revenue by category using:
--   a) Star approach (join fact_sales -> de_demo.dim_products with category as an attribute)
--   b) Snowflake approach (join fact_sales -> dim_product -> dim_category)
--
-- TODO 4) Add a short comment in SQL explaining one tradeoff you observe.
