-- Solution 03: Factless facts and bridge tables

SET search_path = de_demo, public;

CREATE SCHEMA IF NOT EXISTS de_analytics;
SET search_path = de_analytics, de_demo, public;

-- Ensure dim_date exists (run exercise 01 solution first) - create minimal if missing.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='de_analytics' AND table_name='dim_date'
  ) THEN
    CREATE TABLE de_analytics.dim_date (
      date_id DATE PRIMARY KEY,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      day INTEGER NOT NULL,
      month_start_date DATE NOT NULL,
      quarter_start_date DATE NOT NULL,
      is_weekend BOOLEAN NOT NULL
    );

    INSERT INTO de_analytics.dim_date
    SELECT
      d::date,
      EXTRACT(YEAR  FROM d)::int,
      EXTRACT(MONTH FROM d)::int,
      EXTRACT(DAY   FROM d)::int,
      DATE_TRUNC('month',   d)::date,
      DATE_TRUNC('quarter', d)::date,
      EXTRACT(ISODOW FROM d) IN (6,7)
    FROM generate_series('2026-01-01'::date, '2026-01-31'::date, interval '1 day') g(d);
  END IF;
END $$;

DROP TABLE IF EXISTS bridge_customer_product_interest;
DROP TABLE IF EXISTS fact_page_view;

CREATE TABLE fact_page_view (
  event_id     BIGINT PRIMARY KEY,
  view_ts      TIMESTAMPTZ NOT NULL,
  date_id      DATE NOT NULL REFERENCES dim_date(date_id),
  customer_id  BIGINT REFERENCES de_demo.dim_customers(customer_id),
  path         TEXT NOT NULL
);

INSERT INTO fact_page_view (event_id, view_ts, date_id, customer_id, path)
SELECT
  e.event_id,
  e.event_ts,
  e.event_ts::date,
  e.customer_id,
  e.event_payload->>'path' AS path
FROM de_demo.fact_events e
WHERE e.event_type = 'page_view'
  AND (e.event_payload ? 'path');

-- Bridge: infer interest edges from product page views
CREATE TABLE bridge_customer_product_interest (
  customer_id    BIGINT NOT NULL REFERENCES de_demo.dim_customers(customer_id),
  product_id     BIGINT NOT NULL REFERENCES de_demo.dim_products(product_id),
  first_seen_ts  TIMESTAMPTZ NOT NULL,
  last_seen_ts   TIMESTAMPTZ NOT NULL,
  view_count     INTEGER NOT NULL CHECK (view_count >= 1),
  PRIMARY KEY (customer_id, product_id)
);

WITH parsed AS (
  SELECT
    pv.customer_id,
    pv.view_ts,
    -- Extract SKU from '/product/SKU-200' style paths
    NULLIF( (regexp_match(pv.path, '/product/(SKU-[0-9]+)'))[1], '' ) AS sku
  FROM fact_page_view pv
  WHERE pv.customer_id IS NOT NULL
), joined AS (
  SELECT
    p.customer_id,
    dp.product_id,
    p.view_ts
  FROM parsed p
  JOIN de_demo.dim_products dp ON dp.sku = p.sku
)
INSERT INTO bridge_customer_product_interest (customer_id, product_id, first_seen_ts, last_seen_ts, view_count)
SELECT
  customer_id,
  product_id,
  MIN(view_ts) AS first_seen_ts,
  MAX(view_ts) AS last_seen_ts,
  COUNT(*)::int AS view_count
FROM joined
GROUP BY 1,2;

-- Top interested product per customer
WITH ranked AS (
  SELECT
    b.*, 
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY view_count DESC, last_seen_ts DESC) AS rn
  FROM bridge_customer_product_interest b
)
SELECT
  c.email,
  p.sku,
  p.product_name,
  r.view_count,
  r.first_seen_ts,
  r.last_seen_ts
FROM ranked r
JOIN de_demo.dim_customers c ON c.customer_id = r.customer_id
JOIN de_demo.dim_products  p ON p.product_id  = r.product_id
WHERE r.rn = 1
ORDER BY c.email;
