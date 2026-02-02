-- Worked examples: indexes, constraints, and EXPLAIN

SET search_path = de_demo, public;

-- 1) See existing indexes
SELECT
  schemaname,
  tablename,
  indexname,
  indexdef
FROM pg_indexes
WHERE schemaname = 'de_demo'
ORDER BY tablename, indexname;

-- 2) EXPLAIN a time-bounded query (benefits from idx_fact_events_event_ts)
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM fact_events
WHERE event_ts >= '2026-01-12'::timestamptz
  AND event_ts <  '2026-01-14'::timestamptz
ORDER BY event_ts;

-- 3) Partial indexes (useful when most events are non-monetary)
CREATE INDEX IF NOT EXISTS idx_fact_events_monetary_ts
ON fact_events (event_ts)
WHERE event_type IN ('purchase','refund');

EXPLAIN (ANALYZE, BUFFERS)
SELECT
  date_trunc('day', event_ts) AS day,
  SUM(amount_cents) AS net_revenue_cents
FROM fact_events
WHERE event_type IN ('purchase','refund')
GROUP BY 1
ORDER BY 1;

-- 4) Constraints as data-quality guardrails
-- Try inserting a purchase with NULL amount (should fail due to chk_amount_for_monetary_events)
-- INSERT INTO fact_events (event_ts, event_type, customer_id, product_id, amount_cents, source_system)
-- VALUES (now(), 'purchase', 1, 1, NULL, 'web');
