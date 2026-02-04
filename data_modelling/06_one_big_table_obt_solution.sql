-- Solution 06: One Big Table (OBT) / Wide Table

SET search_path = de_demo, public;

CREATE SCHEMA IF NOT EXISTS de_analytics;
SET search_path = de_analytics, de_demo, public;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='de_analytics' AND table_name='fact_sales'
  ) THEN
    RAISE EXCEPTION 'Missing de_analytics.fact_sales. Run data_modelling/01_dimensional_star_schema_solution.sql first.';
  END IF;
END $$;

DROP TABLE IF EXISTS obt_sales_lines;

CREATE TABLE obt_sales_lines (
  event_id           BIGINT PRIMARY KEY,
  order_id           TEXT NOT NULL,
  order_ts           TIMESTAMPTZ NOT NULL,
  date_id            DATE NOT NULL,

  customer_id        BIGINT NOT NULL,
  customer_email     TEXT NOT NULL,
  customer_full_name TEXT NOT NULL,

  product_id         BIGINT NOT NULL,
  sku                TEXT NOT NULL,
  product_name       TEXT NOT NULL,
  category           TEXT NOT NULL,

  gross_amount_cents INTEGER NOT NULL CHECK (gross_amount_cents >= 0),
  source_system      TEXT NOT NULL
);

INSERT INTO obt_sales_lines (
  event_id, order_id, order_ts, date_id,
  customer_id, customer_email, customer_full_name,
  product_id, sku, product_name, category,
  gross_amount_cents, source_system
)
SELECT
  s.event_id,
  s.order_id,
  s.order_ts,
  s.date_id,
  c.customer_id,
  c.email,
  c.full_name,
  p.product_id,
  p.sku,
  p.product_name,
  p.category,
  s.gross_amount_cents,
  s.source_system
FROM de_analytics.fact_sales s
JOIN de_demo.dim_customers c ON c.customer_id = s.customer_id
JOIN de_demo.dim_products  p ON p.product_id  = s.product_id;

-- Index for common filters: date range + category
CREATE INDEX IF NOT EXISTS idx_obt_sales_lines_date_category
  ON obt_sales_lines (date_id, category);

-- Example BI query: revenue by category by day (no joins)
SELECT
  date_id,
  category,
  SUM(gross_amount_cents) AS revenue_cents
FROM obt_sales_lines
GROUP BY 1,2
ORDER BY 1,2;

-- Downside: dimension attributes are duplicated per line; if product/category changes you must rebuild or backfill.
