-- Solution 08: SCD Type 2 dimension history + as-of joins

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

DROP TABLE IF EXISTS dim_customer_scd2;

CREATE TABLE dim_customer_scd2 (
  customer_sk  BIGSERIAL PRIMARY KEY,
  customer_id  BIGINT NOT NULL REFERENCES de_demo.dim_customers(customer_id),
  full_name    TEXT NOT NULL,
  valid_from   TIMESTAMPTZ NOT NULL,
  valid_to     TIMESTAMPTZ NOT NULL,
  is_current   BOOLEAN NOT NULL,
  UNIQUE (customer_id, valid_from),
  CHECK (valid_from < valid_to)
);

-- Seed: one current row per customer
INSERT INTO dim_customer_scd2 (customer_id, full_name, valid_from, valid_to, is_current)
SELECT
  c.customer_id,
  c.full_name,
  '1900-01-01 00:00+00'::timestamptz AS valid_from,
  '9999-12-31 00:00+00'::timestamptz AS valid_to,
  TRUE AS is_current
FROM de_demo.dim_customers c;

-- Simulate a change for Ben effective 2026-01-11
DO $$
DECLARE
  v_customer_id BIGINT;
  v_effective   TIMESTAMPTZ := '2026-01-11 00:00+00'::timestamptz;
BEGIN
  SELECT customer_id INTO v_customer_id
  FROM de_demo.dim_customers
  WHERE email = 'ben@example.com';

  -- expire current row
  UPDATE dim_customer_scd2
  SET valid_to = v_effective,
      is_current = FALSE
  WHERE customer_id = v_customer_id
    AND is_current = TRUE;

  -- insert new current row
  INSERT INTO dim_customer_scd2 (customer_id, full_name, valid_from, valid_to, is_current)
  VALUES (v_customer_id, 'Benjamin Kim', v_effective, '9999-12-31 00:00+00'::timestamptz, TRUE);
END $$;

-- As-of join: customer name at order time
SELECT
  s.order_id,
  s.order_ts,
  s.gross_amount_cents,
  c.email,
  scd.full_name AS customer_name_asof
FROM de_analytics.fact_sales s
JOIN de_demo.dim_customers c ON c.customer_id = s.customer_id
JOIN de_analytics.dim_customer_scd2 scd
  ON scd.customer_id = s.customer_id
 AND s.order_ts >= scd.valid_from
 AND s.order_ts <  scd.valid_to
ORDER BY s.order_ts, s.order_id;
