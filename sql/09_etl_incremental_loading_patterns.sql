-- Worked examples: incremental loading patterns for analytics

SET search_path = de_demo, public;

-- 1) Incremental extraction watermark pattern
-- Imagine you store last successful load timestamp in an orchestration system.
-- Here we simulate it with a CTE.
WITH state AS (
  SELECT '2026-01-12 00:00+00'::timestamptz AS last_loaded_ts
)
SELECT e.*
FROM fact_events e
CROSS JOIN state s
WHERE e.event_ts > s.last_loaded_ts
ORDER BY e.event_ts;

-- 2) Build a daily aggregate table (materialization)
-- For demo we use a TEMP table so it doesn't persist.
DROP TABLE IF EXISTS tmp_daily_metrics;
CREATE TEMP TABLE tmp_daily_metrics AS
SELECT
  event_ts::date AS day,
  COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
  COUNT(DISTINCT customer_id) FILTER (WHERE customer_id IS NOT NULL) AS active_customers,
  SUM(amount_cents) FILTER (WHERE event_type IN ('purchase','refund')) AS net_revenue_cents
FROM fact_events
GROUP BY 1;

SELECT * FROM tmp_daily_metrics ORDER BY day;

-- 3) Late arriving data handling
-- When events can arrive late, rebuild a rolling window (e.g., last 3 days) instead of purely append.
WITH window_days AS (
  SELECT generate_series((current_date - 3), current_date, interval '1 day')::date AS day
)
SELECT d.day,
       COALESCE(m.net_revenue_cents, 0) AS net_revenue_cents
FROM window_days d
LEFT JOIN (
  SELECT event_ts::date AS day,
         SUM(amount_cents) FILTER (WHERE event_type IN ('purchase','refund')) AS net_revenue_cents
  FROM fact_events
  GROUP BY 1
) m USING (day)
ORDER BY d.day;
