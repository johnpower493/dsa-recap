-- Worked examples: CTEs, date series, gap filling

SET search_path = de_demo, public;

-- 1) Generate a daily series and left join facts to fill missing days
WITH days AS (
  SELECT generate_series(
           '2026-01-10'::date,
           '2026-01-16'::date,
           interval '1 day'
         )::date AS day
), daily_revenue AS (
  SELECT
    event_ts::date AS day,
    SUM(amount_cents) FILTER (WHERE event_type IN ('purchase','refund')) AS net_revenue_cents
  FROM fact_events
  GROUP BY 1
)
SELECT
  d.day,
  COALESCE(r.net_revenue_cents, 0) AS net_revenue_cents
FROM days d
LEFT JOIN daily_revenue r USING (day)
ORDER BY d.day;

-- 2) Recursive CTE: simple hierarchy expansion example
-- (Here we create an in-query hierarchy rather than extra tables.)
WITH RECURSIVE org AS (
  SELECT 1 AS emp_id, 'CTO' AS title, NULL::int AS manager_id
  UNION ALL
  SELECT 2, 'Data Eng Manager', 1
  UNION ALL
  SELECT 3, 'Senior DE', 2
  UNION ALL
  SELECT 4, 'DE', 2
), tree AS (
  SELECT emp_id, title, manager_id, 0 AS depth, emp_id::text AS path
  FROM org
  WHERE manager_id IS NULL
  UNION ALL
  SELECT o.emp_id, o.title, o.manager_id, t.depth + 1, t.path || '>' || o.emp_id
  FROM org o
  JOIN tree t ON t.emp_id = o.manager_id
)
SELECT *
FROM tree
ORDER BY path;
