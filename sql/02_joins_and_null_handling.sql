-- Worked examples: joins, join pitfalls, and null handling

SET search_path = de_demo, public;

-- 1) Enrich facts with dimensions
SELECT
  e.event_id,
  e.event_ts,
  e.event_type,
  c.email,
  p.sku,
  p.category,
  e.amount_cents
FROM fact_events e
LEFT JOIN dim_customers c ON c.customer_id = e.customer_id
LEFT JOIN dim_products  p ON p.product_id  = e.product_id
ORDER BY e.event_ts;

-- 2) Customers with no purchases (anti-join)
SELECT c.customer_id, c.email
FROM dim_customers c
LEFT JOIN fact_events e
  ON e.customer_id = c.customer_id
 AND e.event_type = 'purchase'
WHERE e.event_id IS NULL
ORDER BY c.customer_id;

-- 3) Join pitfall: filters in WHERE can turn LEFT JOIN into INNER JOIN
-- Bad: this removes customers with zero purchases.
SELECT c.email, COUNT(e.event_id) AS purchases
FROM dim_customers c
LEFT JOIN fact_events e ON e.customer_id = c.customer_id
WHERE e.event_type = 'purchase'
GROUP BY c.email;

-- Good: push the filter into the JOIN or use FILTER.
SELECT c.email,
       COUNT(e.event_id) FILTER (WHERE e.event_type = 'purchase') AS purchases
FROM dim_customers c
LEFT JOIN fact_events e ON e.customer_id = c.customer_id
GROUP BY c.email
ORDER BY purchases DESC, c.email;

-- 4) COALESCE for default values
SELECT
  c.email,
  COALESCE(SUM(e.amount_cents) FILTER (WHERE e.event_type IN ('purchase','refund')), 0) AS net_revenue_cents
FROM dim_customers c
LEFT JOIN fact_events e ON e.customer_id = c.customer_id
GROUP BY c.email
ORDER BY net_revenue_cents DESC;
