-- Solution 07: Kimball bus matrix + conformed dimensions (multi-fact constellation)

SET search_path = de_demo, public;

CREATE SCHEMA IF NOT EXISTS de_analytics;
SET search_path = de_analytics, de_demo, public;

-- Prereq checks
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='de_analytics' AND table_name='dim_date') THEN
    RAISE EXCEPTION 'Missing de_analytics.dim_date. Run data_modelling/01_dimensional_star_schema_solution.sql first.';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='de_analytics' AND table_name='fact_sales') THEN
    RAISE EXCEPTION 'Missing de_analytics.fact_sales. Run data_modelling/01_dimensional_star_schema_solution.sql first.';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='de_analytics' AND table_name='fact_page_view') THEN
    RAISE EXCEPTION 'Missing de_analytics.fact_page_view. Run data_modelling/03_factless_facts_and_bridges_solution.sql first.';
  END IF;
END $$;

DROP VIEW  IF EXISTS vw_customer_day;
DROP TABLE IF EXISTS fact_customer_daily_engagement;

CREATE TABLE fact_customer_daily_engagement (
  date_id             DATE NOT NULL REFERENCES dim_date(date_id),
  customer_id         BIGINT NOT NULL REFERENCES de_demo.dim_customers(customer_id),
  page_views          INTEGER NOT NULL CHECK (page_views >= 0),
  product_page_views  INTEGER NOT NULL CHECK (product_page_views >= 0),
  PRIMARY KEY (date_id, customer_id)
);

INSERT INTO fact_customer_daily_engagement (date_id, customer_id, page_views, product_page_views)
SELECT
  pv.date_id,
  pv.customer_id,
  COUNT(*)::int AS page_views,
  COUNT(*) FILTER (WHERE pv.path ~ '^/product/SKU-[0-9]+$')::int AS product_page_views
FROM de_analytics.fact_page_view pv
WHERE pv.customer_id IS NOT NULL
GROUP BY 1,2;

-- Bus-style view: one row per (date, customer) with sales + engagement
CREATE VIEW vw_customer_day AS
WITH sales_by_day AS (
  SELECT
    date_id,
    customer_id,
    COUNT(DISTINCT order_id) AS orders,
    SUM(gross_amount_cents)  AS revenue_cents
  FROM de_analytics.fact_sales
  GROUP BY 1,2
)
SELECT
  d.date_id,
  c.customer_id,
  c.email,
  c.full_name,
  COALESCE(s.orders, 0)        AS orders,
  COALESCE(s.revenue_cents, 0) AS revenue_cents,
  COALESCE(e.page_views, 0)    AS page_views,
  COALESCE(e.product_page_views, 0) AS product_page_views
FROM de_analytics.dim_date d
CROSS JOIN de_demo.dim_customers c
LEFT JOIN sales_by_day s
  ON s.date_id = d.date_id AND s.customer_id = c.customer_id
LEFT JOIN de_analytics.fact_customer_daily_engagement e
  ON e.date_id = d.date_id AND e.customer_id = c.customer_id
WHERE d.date_id BETWEEN '2026-01-01' AND '2026-01-31'
ORDER BY d.date_id, c.customer_id;

-- Example query: revenue + page_views by day
SELECT *
FROM vw_customer_day
WHERE orders > 0 OR page_views > 0
ORDER BY date_id, customer_id;
