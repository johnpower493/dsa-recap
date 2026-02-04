-- Solution 01: Dimensional modelling (Star schema)

SET search_path = de_demo, public;

-- 1) Analytics schema
CREATE SCHEMA IF NOT EXISTS de_analytics;
SET search_path = de_analytics, de_demo, public;

-- Clean start
DROP VIEW  IF EXISTS vw_daily_revenue;
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_date;

-- 2) Date dimension
CREATE TABLE dim_date (
  date_id            DATE PRIMARY KEY,
  year               INTEGER NOT NULL,
  month              INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
  day                INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
  month_start_date   DATE NOT NULL,
  quarter_start_date DATE NOT NULL,
  is_weekend         BOOLEAN NOT NULL
);

INSERT INTO dim_date (date_id, year, month, day, month_start_date, quarter_start_date, is_weekend)
SELECT
  d::date AS date_id,
  EXTRACT(YEAR  FROM d)::int AS year,
  EXTRACT(MONTH FROM d)::int AS month,
  EXTRACT(DAY   FROM d)::int AS day,
  DATE_TRUNC('month',   d)::date AS month_start_date,
  DATE_TRUNC('quarter', d)::date AS quarter_start_date,
  EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
FROM generate_series('2026-01-01'::date, '2026-01-31'::date, interval '1 day') AS g(d);

-- 3) Sales fact (one row per order line)
CREATE TABLE fact_sales (
  sales_id           BIGSERIAL PRIMARY KEY,
  order_id           TEXT NOT NULL,
  order_ts           TIMESTAMPTZ NOT NULL,
  date_id            DATE NOT NULL REFERENCES dim_date(date_id),
  customer_id        BIGINT NOT NULL REFERENCES de_demo.dim_customers(customer_id),
  product_id         BIGINT NOT NULL REFERENCES de_demo.dim_products(product_id),
  gross_amount_cents INTEGER NOT NULL CHECK (gross_amount_cents >= 0),
  source_system      TEXT NOT NULL,
  event_id           BIGINT NOT NULL UNIQUE  -- lineage back to the raw event
);

-- 4) Load fact_sales from raw events
INSERT INTO fact_sales (order_id, order_ts, date_id, customer_id, product_id, gross_amount_cents, source_system, event_id)
SELECT
  (e.event_payload->>'order_id') AS order_id,
  e.event_ts                     AS order_ts,
  e.event_ts::date               AS date_id,
  e.customer_id,
  e.product_id,
  e.amount_cents                 AS gross_amount_cents,
  e.source_system,
  e.event_id
FROM de_demo.fact_events e
WHERE e.event_type = 'purchase'
  AND e.customer_id IS NOT NULL
  AND e.product_id  IS NOT NULL
  AND e.amount_cents IS NOT NULL
  AND e.amount_cents >= 0
  AND (e.event_payload ? 'order_id');

-- 5) Semantic view
CREATE VIEW vw_daily_revenue AS
SELECT
  s.date_id,
  COUNT(DISTINCT s.order_id)      AS orders,
  SUM(s.gross_amount_cents)       AS revenue_cents,
  COUNT(DISTINCT s.customer_id)   AS customers
FROM fact_sales s
GROUP BY 1
ORDER BY 1;

-- 6) Validations (should return true)
-- a) revenue matches raw purchases
SELECT
  (SELECT COALESCE(SUM(revenue_cents), 0) FROM vw_daily_revenue)
  =
  (SELECT COALESCE(SUM(amount_cents), 0)
   FROM de_demo.fact_events
   WHERE event_type = 'purchase' AND amount_cents >= 0 AND (event_payload ? 'order_id'))
  AS revenue_matches;

-- b) preview
SELECT * FROM vw_daily_revenue;
