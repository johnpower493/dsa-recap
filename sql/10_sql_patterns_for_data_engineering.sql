-- Worked examples: common SQL patterns used in data engineering

SET search_path = de_demo, public;

-- 1) De-duplication: keep latest event per (customer, event_type) using QUALIFY-like pattern
-- Postgres doesn't have QUALIFY; use a subquery.
WITH ranked AS (
  SELECT
    e.*,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id, event_type
      ORDER BY event_ts DESC, event_id DESC
    ) AS rn
  FROM fact_events e
  WHERE customer_id IS NOT NULL
)
SELECT *
FROM ranked
WHERE rn = 1
ORDER BY customer_id, event_type;

-- 2) Slowly changing attribute snapshot (as-of join)
-- Example: "What was the last known customer name at event time?"
-- We don't have history table here; this is a pattern reference.
-- Typically: join fact to SCD dimension with: fact_ts >= valid_from and fact_ts < valid_to.

-- 3) JSON parsing (semi-structured data)
SELECT
  event_id,
  event_type,
  event_payload,
  event_payload->>'order_id' AS order_id,
  event_payload->>'path'     AS path
FROM fact_events
ORDER BY event_id;

-- 4) Safe casting / handling bad data
-- Use NULLIF/regexp checks before casting.
SELECT
  '123'::text AS raw,
  CASE WHEN '123' ~ '^[0-9]+$' THEN '123'::int END AS safe_int;
