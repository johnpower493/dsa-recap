-- Postgres worked examples: schema + seed data
--
-- How to use (psql):
--   \i sql/00_setup_schema_and_seed.sql
-- Then run any example file:
--   \i sql/03_window_functions.sql

BEGIN;

-- Keep everything namespaced.
CREATE SCHEMA IF NOT EXISTS de_demo;
SET search_path = de_demo, public;

-- Clean start (idempotent for repeated practice).
DROP TABLE IF EXISTS fact_events CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS stg_events CASCADE;

-- A tiny dimensional-ish model used throughout the examples.
CREATE TABLE dim_customers (
  customer_id      BIGSERIAL PRIMARY KEY,
  email            TEXT NOT NULL UNIQUE,
  full_name        TEXT NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE dim_products (
  product_id       BIGSERIAL PRIMARY KEY,
  sku              TEXT NOT NULL UNIQUE,
  product_name     TEXT NOT NULL,
  category         TEXT NOT NULL,
  list_price_cents INTEGER NOT NULL CHECK (list_price_cents >= 0)
);

-- Facts: one row per event (purchase, refund, page_view, etc.)
CREATE TABLE fact_events (
  event_id         BIGSERIAL PRIMARY KEY,
  event_ts         TIMESTAMPTZ NOT NULL,
  event_type       TEXT NOT NULL,
  customer_id      BIGINT REFERENCES dim_customers(customer_id),
  product_id       BIGINT REFERENCES dim_products(product_id),
  amount_cents     INTEGER,
  source_system    TEXT NOT NULL,
  event_payload    JSONB NOT NULL DEFAULT '{}'::jsonb,

  -- Example of a data-quality constraint:
  CONSTRAINT chk_amount_for_monetary_events
    CHECK (
      (event_type IN ('purchase','refund') AND amount_cents IS NOT NULL)
      OR (event_type NOT IN ('purchase','refund'))
    )
);

-- Staging table to demonstrate ETL merge/upsert patterns.
CREATE TABLE stg_events (
  source_event_id  TEXT NOT NULL,
  source_system    TEXT NOT NULL,
  event_ts         TIMESTAMPTZ NOT NULL,
  event_type       TEXT NOT NULL,
  customer_email   TEXT,
  product_sku      TEXT,
  amount_cents     INTEGER,
  event_payload    JSONB NOT NULL DEFAULT '{}'::jsonb,
  loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (source_system, source_event_id)
);

-- Helpful indexes (used in indexing/explain examples).
CREATE INDEX IF NOT EXISTS idx_fact_events_event_ts ON fact_events (event_ts);
CREATE INDEX IF NOT EXISTS idx_fact_events_customer_ts ON fact_events (customer_id, event_ts);

-- Seed dims.
INSERT INTO dim_customers (email, full_name, created_at) VALUES
  ('ava@example.com',   'Ava Patel',   '2026-01-01 10:00+00'),
  ('ben@example.com',   'Ben Kim',     '2026-01-03 12:00+00'),
  ('cora@example.com',  'Cora Jones',  '2026-01-05 09:30+00'),
  ('diego@example.com', 'Diego Ruiz',  '2026-01-07 18:45+00');

INSERT INTO dim_products (sku, product_name, category, list_price_cents) VALUES
  ('SKU-100', 'Data Pipelines 101', 'books',   2999),
  ('SKU-200', 'Warehouse Hoodie',   'apparel', 6500),
  ('SKU-300', 'SQL Stickers',       'swag',     499);

-- Seed facts.
INSERT INTO fact_events (event_ts, event_type, customer_id, product_id, amount_cents, source_system, event_payload)
SELECT
  ts,
  et,
  c.customer_id,
  p.product_id,
  amt,
  src,
  payload
FROM (
  VALUES
    ('2026-01-10 08:00+00'::timestamptz, 'page_view', 'ava@example.com',  NULL,      NULL, 'web', '{"path":"/home"}'::jsonb),
    ('2026-01-10 08:05+00'::timestamptz, 'page_view', 'ava@example.com',  NULL,      NULL, 'web', '{"path":"/product/SKU-100"}'::jsonb),
    ('2026-01-10 08:10+00'::timestamptz, 'purchase',  'ava@example.com',  'SKU-100', 2999, 'web', '{"order_id":"A-1"}'::jsonb),

    ('2026-01-11 10:00+00'::timestamptz, 'page_view', 'ben@example.com',  NULL,      NULL, 'web', '{"path":"/product/SKU-200"}'::jsonb),
    ('2026-01-11 10:02+00'::timestamptz, 'purchase',  'ben@example.com',  'SKU-200', 6500, 'web', '{"order_id":"B-1"}'::jsonb),
    ('2026-01-12 11:00+00'::timestamptz, 'refund',    'ben@example.com',  'SKU-200', -6500, 'web', '{"order_id":"B-1","reason":"size"}'::jsonb),

    ('2026-01-13 07:30+00'::timestamptz, 'purchase',  'cora@example.com', 'SKU-300',  499, 'ios', '{"order_id":"C-1"}'::jsonb),
    ('2026-01-13 07:32+00'::timestamptz, 'purchase',  'cora@example.com', 'SKU-100', 2999, 'ios', '{"order_id":"C-2"}'::jsonb),

    ('2026-01-15 20:00+00'::timestamptz, 'page_view', 'diego@example.com', NULL,      NULL, 'web', '{"path":"/pricing"}'::jsonb)
) AS v(ts, et, email, sku, amt, src, payload)
LEFT JOIN dim_customers c ON c.email = v.email
LEFT JOIN dim_products  p ON p.sku   = v.sku;

COMMIT;
