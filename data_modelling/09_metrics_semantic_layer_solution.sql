-- Solution 09: Metrics / Semantic layer pattern ("metric views")

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

DROP VIEW IF EXISTS vw_metrics_daily;

CREATE VIEW vw_metrics_daily AS
SELECT
  s.date_id,
  SUM(s.gross_amount_cents)::bigint AS revenue_cents,
  COUNT(DISTINCT s.order_id)::bigint AS orders,
  COUNT(DISTINCT s.customer_id)::bigint AS customers,
  CASE WHEN COUNT(DISTINCT s.order_id) = 0 THEN NULL
       ELSE (SUM(s.gross_amount_cents) / COUNT(DISTINCT s.order_id))::bigint
  END AS aov_cents
FROM de_analytics.fact_sales s
GROUP BY 1;

-- Downstream query a) Month-to-date revenue (for Jan 2026 in demo)
SELECT
  date_trunc('month', date_id)::date AS month_start,
  SUM(revenue_cents) AS mtd_revenue_cents
FROM vw_metrics_daily
WHERE date_id >= '2026-01-01' AND date_id < '2026-02-01'
GROUP BY 1;

-- Downstream query b) Weekend vs weekday revenue
SELECT
  CASE WHEN d.is_weekend THEN 'weekend' ELSE 'weekday' END AS day_type,
  SUM(m.revenue_cents) AS revenue_cents
FROM vw_metrics_daily m
JOIN de_analytics.dim_date d ON d.date_id = m.date_id
GROUP BY 1
ORDER BY 1;

-- Centralizing metric definitions prevents "metric drift" across dashboards and makes governance easier.
