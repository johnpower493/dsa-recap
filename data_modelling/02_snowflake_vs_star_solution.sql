-- Solution 02: Star vs Snowflake

SET search_path = de_demo, public;

CREATE SCHEMA IF NOT EXISTS de_analytics;
SET search_path = de_analytics, de_demo, public;

-- This script assumes `de_analytics.fact_sales` exists (created in exercise 01).
-- We fail fast with a helpful message if it doesn't.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='de_analytics' AND table_name='fact_sales'
  ) THEN
    RAISE EXCEPTION 'Missing de_analytics.fact_sales. Run data_modelling/01_dimensional_star_schema_solution.sql first.';
  END IF;
END $$;

DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_category;

CREATE TABLE dim_category (
  category_id   BIGSERIAL PRIMARY KEY,
  category_name TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_product (
  product_id       BIGINT PRIMARY KEY,
  sku              TEXT NOT NULL UNIQUE,
  product_name     TEXT NOT NULL,
  list_price_cents INTEGER NOT NULL CHECK (list_price_cents >= 0),
  category_id      BIGINT NOT NULL REFERENCES dim_category(category_id)
);

-- Backfill
INSERT INTO dim_category (category_name)
SELECT DISTINCT category
FROM de_demo.dim_products
ORDER BY 1
ON CONFLICT (category_name) DO NOTHING;

INSERT INTO dim_product (product_id, sku, product_name, list_price_cents, category_id)
SELECT
  p.product_id,
  p.sku,
  p.product_name,
  p.list_price_cents,
  c.category_id
FROM de_demo.dim_products p
JOIN dim_category c ON c.category_name = p.category;

-- a) Star approach: category as a denormalized attribute
-- Ergonomic (one fewer join) and often faster for BI tools; can duplicate attributes.
SELECT
  p.category,
  SUM(s.gross_amount_cents) AS revenue_cents
FROM de_analytics.fact_sales s
JOIN de_demo.dim_products p ON p.product_id = s.product_id
GROUP BY 1
ORDER BY 2 DESC;

-- b) Snowflake approach: category normalized
-- More maintainable for shared attributes/hierarchies, but more joins and potential BI complexity.
SELECT
  c.category_name AS category,
  SUM(s.gross_amount_cents) AS revenue_cents
FROM de_analytics.fact_sales s
JOIN de_analytics.dim_product  p ON p.product_id = s.product_id
JOIN de_analytics.dim_category c ON c.category_id = p.category_id
GROUP BY 1
ORDER BY 2 DESC;
