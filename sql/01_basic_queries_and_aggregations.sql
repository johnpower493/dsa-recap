-- Worked examples: basic queries, filtering, grouping

SET search_path = de_demo, public;

-- 1) Simple SELECT + WHERE + ORDER BY
SELECT event_id, event_ts, event_type, source_system
FROM fact_events
WHERE event_ts >= '2026-01-11'::timestamptz
ORDER BY event_ts;

-- 2) Count events by type
SELECT event_type, COUNT(*) AS n_events
FROM fact_events
GROUP BY event_type
ORDER BY n_events DESC, event_type;

-- 3) Daily active customers (distinct customers per day)
SELECT
  date_trunc('day', event_ts) AS day,
  COUNT(DISTINCT customer_id) AS dau
FROM fact_events
WHERE customer_id IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- 4) Revenue by day (purchase + refund = net)
SELECT
  date_trunc('day', event_ts) AS day,
  SUM(amount_cents) FILTER (WHERE event_type IN ('purchase','refund')) AS net_revenue_cents
FROM fact_events
GROUP BY 1
ORDER BY 1;

-- 5) Top categories by revenue
SELECT
  p.category,
  SUM(e.amount_cents) AS revenue_cents
FROM fact_events e
JOIN dim_products p ON p.product_id = e.product_id
WHERE e.event_type IN ('purchase','refund')
GROUP BY 1
ORDER BY revenue_cents DESC;
