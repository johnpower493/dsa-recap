-- Solution 04: Aggregate facts and rollups

SET search_path = de_demo, public;

CREATE SCHEMA IF NOT EXISTS de_analytics;
SET search_path = de_analytics, de_demo, public;

-- This script assumes `de_analytics.fact_sales` exists (created in exercise 01).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='de_analytics' AND table_name='fact_sales'
  ) THEN
    RAISE EXCEPTION 'Missing de_analytics.fact_sales. Run data_modelling/01_dimensional_star_schema_solution.sql first.';
  END IF;
END $$;

DROP TABLE IF EXISTS agg_daily_revenue;

CREATE TABLE agg_daily_revenue (
  date_id       DATE PRIMARY KEY,
  orders        BIGINT NOT NULL,
  revenue_cents BIGINT NOT NULL,
  customers     BIGINT NOT NULL,
  loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Full backfill
INSERT INTO agg_daily_revenue (date_id, orders, revenue_cents, customers)
SELECT
  date_id,
  COUNT(DISTINCT order_id)    AS orders,
  SUM(gross_amount_cents)     AS revenue_cents,
  COUNT(DISTINCT customer_id) AS customers
FROM de_analytics.fact_sales
GROUP BY 1
ON CONFLICT (date_id) DO UPDATE
SET orders        = EXCLUDED.orders,
    revenue_cents = EXCLUDED.revenue_cents,
    customers     = EXCLUDED.customers,
    loaded_at     = now();

-- Incremental refresh: recompute last 7 days based on max date in the aggregate
-- (In production you'd use a watermark and handle late-arriving data.)
WITH bounds AS (
  SELECT COALESCE(MAX(date_id), '1900-01-01'::date) AS max_date
  FROM agg_daily_revenue
)
INSERT INTO agg_daily_revenue (date_id, orders, revenue_cents, customers)
SELECT
  s.date_id,
  COUNT(DISTINCT s.order_id)    AS orders,
  SUM(s.gross_amount_cents)     AS revenue_cents,
  COUNT(DISTINCT s.customer_id) AS customers
FROM de_analytics.fact_sales s
CROSS JOIN bounds b
WHERE s.date_id >= (b.max_date - 7)
GROUP BY 1
ON CONFLICT (date_id) DO UPDATE
SET orders        = EXCLUDED.orders,
    revenue_cents = EXCLUDED.revenue_cents,
    customers     = EXCLUDED.customers,
    loaded_at     = now();

-- Compare: query from aggregate
SELECT * FROM agg_daily_revenue ORDER BY date_id;
