-- Worked examples: upserts, merges, and SCD Type 2 (conceptual)
-- Postgres notes:
-- - Use INSERT ... ON CONFLICT for upserts.
-- - Postgres 15+ supports MERGE.

SET search_path = de_demo, public;

-- 1) Upsert into a dimension (e.g., customer updates from a source)
-- Suppose we received a new name for Ben.
INSERT INTO dim_customers (email, full_name)
VALUES ('ben@example.com', 'Benjamin Kim')
ON CONFLICT (email)
DO UPDATE SET
  full_name = EXCLUDED.full_name;

SELECT * FROM dim_customers ORDER BY customer_id;

-- 2) Load from staging into facts (simplified pattern)
-- Stage a couple of new events.
INSERT INTO stg_events (source_event_id, source_system, event_ts, event_type, customer_email, product_sku, amount_cents, event_payload)
VALUES
  ('evt-900', 'web', '2026-01-16 09:00+00', 'purchase', 'ava@example.com', 'SKU-300', 499, '{"order_id":"A-2"}'::jsonb),
  ('evt-901', 'web', '2026-01-16 09:05+00', 'page_view', 'ava@example.com', NULL, NULL, '{"path":"/account"}'::jsonb)
ON CONFLICT (source_system, source_event_id) DO UPDATE
SET event_payload = EXCLUDED.event_payload,
    loaded_at = now();

-- Transform + load. In a real pipeline, you would also de-dup, validate, and log rejects.
INSERT INTO fact_events (event_ts, event_type, customer_id, product_id, amount_cents, source_system, event_payload)
SELECT
  s.event_ts,
  s.event_type,
  c.customer_id,
  p.product_id,
  s.amount_cents,
  s.source_system,
  s.event_payload
FROM stg_events s
LEFT JOIN dim_customers c ON c.email = s.customer_email
LEFT JOIN dim_products  p ON p.sku   = s.product_sku
-- Simple idempotency approach: avoid inserting duplicate payloads from the same source key.
WHERE NOT EXISTS (
  SELECT 1
  FROM fact_events f
  WHERE f.source_system = s.source_system
    AND f.event_ts      = s.event_ts
    AND f.event_type    = s.event_type
    AND COALESCE(f.customer_id, -1) = COALESCE(c.customer_id, -1)
    AND COALESCE(f.product_id, -1)  = COALESCE(p.product_id, -1)
    AND COALESCE(f.amount_cents, 0) = COALESCE(s.amount_cents, 0)
);

-- 3) SCD Type 2 sketch
-- Pattern: dim_customer_history(customer_nk, valid_from, valid_to, is_current, attributes...)
-- When attributes change, expire current row and insert a new current row.
-- (Not implemented as a table here to keep the demo schema minimal.)
