-- Solution 05: Data Vault 2.0 (Hubs, Links, Satellites)

SET search_path = de_demo, public;

CREATE SCHEMA IF NOT EXISTS de_vault;
SET search_path = de_vault, de_demo, public;

-- Clean start
DROP TABLE IF EXISTS sat_product;
DROP TABLE IF EXISTS sat_customer;
DROP TABLE IF EXISTS link_order_line;
DROP TABLE IF EXISTS hub_order;
DROP TABLE IF EXISTS hub_product;
DROP TABLE IF EXISTS hub_customer;

-- Helpers: deterministic hash keys (md5) stored as text
CREATE TABLE hub_customer (
  hk_customer    TEXT PRIMARY KEY,
  customer_nk    TEXT NOT NULL UNIQUE,
  load_dts       TIMESTAMPTZ NOT NULL,
  record_source  TEXT NOT NULL
);

CREATE TABLE hub_product (
  hk_product     TEXT PRIMARY KEY,
  product_nk     TEXT NOT NULL UNIQUE,
  load_dts       TIMESTAMPTZ NOT NULL,
  record_source  TEXT NOT NULL
);

CREATE TABLE hub_order (
  hk_order       TEXT PRIMARY KEY,
  order_nk       TEXT NOT NULL UNIQUE,
  load_dts       TIMESTAMPTZ NOT NULL,
  record_source  TEXT NOT NULL
);

CREATE TABLE link_order_line (
  hk_order_line  TEXT PRIMARY KEY,
  hk_order       TEXT NOT NULL REFERENCES hub_order(hk_order),
  hk_customer    TEXT NOT NULL REFERENCES hub_customer(hk_customer),
  hk_product     TEXT NOT NULL REFERENCES hub_product(hk_product),
  load_dts       TIMESTAMPTZ NOT NULL,
  record_source  TEXT NOT NULL,
  UNIQUE (hk_order, hk_product)  -- simplified: one line per product per order
);

CREATE TABLE sat_customer (
  hk_customer    TEXT NOT NULL REFERENCES hub_customer(hk_customer),
  load_dts       TIMESTAMPTZ NOT NULL,
  full_name      TEXT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL,
  record_source  TEXT NOT NULL,
  PRIMARY KEY (hk_customer, load_dts)
);

CREATE TABLE sat_product (
  hk_product      TEXT NOT NULL REFERENCES hub_product(hk_product),
  load_dts        TIMESTAMPTZ NOT NULL,
  product_name    TEXT NOT NULL,
  category        TEXT NOT NULL,
  list_price_cents INTEGER NOT NULL,
  record_source   TEXT NOT NULL,
  PRIMARY KEY (hk_product, load_dts)
);

-- Load hubs + sats from dimensions
INSERT INTO hub_customer (hk_customer, customer_nk, load_dts, record_source)
SELECT
  md5(lower(c.email)) AS hk_customer,
  lower(c.email)      AS customer_nk,
  now()               AS load_dts,
  'dim_customers'     AS record_source
FROM de_demo.dim_customers c
ON CONFLICT (customer_nk) DO NOTHING;

INSERT INTO sat_customer (hk_customer, load_dts, full_name, created_at, record_source)
SELECT
  md5(lower(c.email)) AS hk_customer,
  now()               AS load_dts,
  c.full_name,
  c.created_at,
  'dim_customers'     AS record_source
FROM de_demo.dim_customers c;

INSERT INTO hub_product (hk_product, product_nk, load_dts, record_source)
SELECT
  md5(p.sku)        AS hk_product,
  p.sku             AS product_nk,
  now()             AS load_dts,
  'dim_products'    AS record_source
FROM de_demo.dim_products p
ON CONFLICT (product_nk) DO NOTHING;

INSERT INTO sat_product (hk_product, load_dts, product_name, category, list_price_cents, record_source)
SELECT
  md5(p.sku) AS hk_product,
  now()      AS load_dts,
  p.product_name,
  p.category,
  p.list_price_cents,
  'dim_products' AS record_source
FROM de_demo.dim_products p;

-- Load orders + links from purchase events
WITH purchases AS (
  SELECT
    e.event_id,
    e.event_ts,
    lower(c.email) AS customer_nk,
    p.sku          AS product_nk,
    (e.event_payload->>'order_id') AS order_nk,
    e.source_system
  FROM de_demo.fact_events e
  JOIN de_demo.dim_customers c ON c.customer_id = e.customer_id
  JOIN de_demo.dim_products  p ON p.product_id  = e.product_id
  WHERE e.event_type = 'purchase'
    AND e.amount_cents >= 0
    AND (e.event_payload ? 'order_id')
)
INSERT INTO hub_order (hk_order, order_nk, load_dts, record_source)
SELECT
  md5(order_nk) AS hk_order,
  order_nk,
  now(),
  'fact_events' AS record_source
FROM purchases
ON CONFLICT (order_nk) DO NOTHING;

WITH purchases AS (
  SELECT
    e.event_id,
    e.event_ts,
    lower(c.email) AS customer_nk,
    p.sku          AS product_nk,
    (e.event_payload->>'order_id') AS order_nk,
    e.source_system
  FROM de_demo.fact_events e
  JOIN de_demo.dim_customers c ON c.customer_id = e.customer_id
  JOIN de_demo.dim_products  p ON p.product_id  = e.product_id
  WHERE e.event_type = 'purchase'
    AND e.amount_cents >= 0
    AND (e.event_payload ? 'order_id')
)
INSERT INTO link_order_line (hk_order_line, hk_order, hk_customer, hk_product, load_dts, record_source)
SELECT
  md5(order_nk || '|' || product_nk) AS hk_order_line,
  md5(order_nk)                      AS hk_order,
  md5(customer_nk)                   AS hk_customer,
  md5(product_nk)                    AS hk_product,
  now()                              AS load_dts,
  'fact_events'                      AS record_source
FROM purchases
ON CONFLICT (hk_order_line) DO NOTHING;

-- Business vault style query: daily revenue by category
-- We still need a measure (amount) from the raw events; in DV this is often a "point-in-time" bridge
-- or a satellite on the link. For simplicity, we re-join to fact_events by order_id+sku.
WITH purchase_lines AS (
  SELECT
    (e.event_ts::date) AS date_id,
    (e.event_payload->>'order_id') AS order_nk,
    p.sku AS product_nk,
    e.amount_cents
  FROM de_demo.fact_events e
  JOIN de_demo.dim_products p ON p.product_id = e.product_id
  WHERE e.event_type='purchase' AND e.amount_cents >= 0 AND (e.event_payload ? 'order_id')
)
SELECT
  sp.category,
  pl.date_id,
  SUM(pl.amount_cents) AS revenue_cents
FROM purchase_lines pl
JOIN hub_order ho ON ho.order_nk = pl.order_nk
JOIN hub_product hp ON hp.product_nk = pl.product_nk
JOIN LATERAL (
  -- latest product satellite row
  SELECT s.*
  FROM sat_product s
  WHERE s.hk_product = hp.hk_product
  ORDER BY s.load_dts DESC
  LIMIT 1
) sp ON TRUE
GROUP BY 1,2
ORDER BY 2,1;
