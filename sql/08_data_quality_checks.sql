-- Worked examples: data quality checks (as queries)
-- In practice these can be used in dbt tests / Great Expectations / custom checks.

SET search_path = de_demo, public;

-- 1) Uniqueness checks
SELECT email, COUNT(*)
FROM dim_customers
GROUP BY email
HAVING COUNT(*) > 1;

SELECT sku, COUNT(*)
FROM dim_products
GROUP BY sku
HAVING COUNT(*) > 1;

-- 2) Referential integrity checks (should be empty unless constraints are disabled)
SELECT e.event_id
FROM fact_events e
LEFT JOIN dim_customers c ON c.customer_id = e.customer_id
WHERE e.customer_id IS NOT NULL
  AND c.customer_id IS NULL;

-- 3) Domain checks: unexpected event types
SELECT event_type, COUNT(*)
FROM fact_events
GROUP BY event_type
HAVING event_type NOT IN ('page_view','purchase','refund');

-- 4) Freshness: latest event timestamp
SELECT MAX(event_ts) AS latest_event_ts
FROM fact_events;

-- 5) Completeness: purchases missing product_id (should be zero)
SELECT COUNT(*) AS purchases_missing_product
FROM fact_events
WHERE event_type = 'purchase'
  AND product_id IS NULL;
