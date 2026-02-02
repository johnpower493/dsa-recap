-- Worked examples: window functions (analytics)

SET search_path = de_demo, public;

-- 1) Order events per customer
SELECT
  customer_id,
  event_ts,
  event_type,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_ts) AS event_number
FROM fact_events
WHERE customer_id IS NOT NULL
ORDER BY customer_id, event_ts;

-- 2) First purchase timestamp per customer
WITH purchases AS (
  SELECT *
  FROM fact_events
  WHERE event_type = 'purchase'
)
SELECT
  customer_id,
  MIN(event_ts) AS first_purchase_ts
FROM purchases
GROUP BY customer_id
ORDER BY customer_id;

-- Same idea with window functions
SELECT DISTINCT
  customer_id,
  FIRST_VALUE(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts) AS first_purchase_ts
FROM fact_events
WHERE event_type = 'purchase'
ORDER BY customer_id;

-- 3) Running net revenue over time (overall)
SELECT
  event_ts,
  event_type,
  amount_cents,
  SUM(amount_cents) FILTER (WHERE event_type IN ('purchase','refund'))
    OVER (ORDER BY event_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_net_revenue_cents
FROM fact_events
ORDER BY event_ts;

-- 4) 3-event moving average of purchase amounts per customer
SELECT
  customer_id,
  event_ts,
  amount_cents,
  AVG(amount_cents) OVER (
    PARTITION BY customer_id
    ORDER BY event_ts
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS moving_avg_last_3
FROM fact_events
WHERE event_type = 'purchase'
ORDER BY customer_id, event_ts;
