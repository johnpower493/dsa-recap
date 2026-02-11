-- SQL Analytics Patterns - Solutions
-- Assumes a table: events(user_id, event_time, event_name, revenue)

-- 1) Daily Active Users (DAU)
SELECT
  CAST(event_time AS DATE) AS event_date,
  COUNT(DISTINCT user_id) AS dau
FROM events
GROUP BY 1
ORDER BY 1;


-- 2) 7-day rolling DAU (approx via DAU rolling sum)
WITH daily AS (
  SELECT CAST(event_time AS DATE) AS event_date,
         COUNT(DISTINCT user_id) AS dau
  FROM events
  GROUP BY 1
)
SELECT
  event_date,
  SUM(dau) OVER (
    ORDER BY event_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7d_dau
FROM daily
ORDER BY event_date;


-- 3) Revenue by event_name and day
SELECT
  CAST(event_time AS DATE) AS event_date,
  event_name,
  SUM(COALESCE(revenue, 0)) AS total_revenue
FROM events
GROUP BY 1, 2
ORDER BY 1, 2;


-- 4) Conversion funnel: visit -> signup -> purchase
WITH users AS (
  SELECT
    user_id,
    MAX(CASE WHEN event_name = 'visit' THEN 1 ELSE 0 END) AS did_visit,
    MAX(CASE WHEN event_name = 'signup' THEN 1 ELSE 0 END) AS did_signup,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS did_purchase
  FROM events
  GROUP BY user_id
),
counts AS (
  SELECT
    SUM(did_visit) AS visit_users,
    SUM(CASE WHEN did_visit = 1 AND did_signup = 1 THEN 1 ELSE 0 END) AS signup_users,
    SUM(CASE WHEN did_signup = 1 AND did_purchase = 1 THEN 1 ELSE 0 END) AS purchase_users
  FROM users
)
SELECT
  visit_users,
  signup_users,
  purchase_users,
  CASE WHEN visit_users = 0 THEN 0 ELSE 1.0 * signup_users / visit_users END AS visit_to_signup_rate,
  CASE WHEN signup_users = 0 THEN 0 ELSE 1.0 * purchase_users / signup_users END AS signup_to_purchase_rate
FROM counts;


-- 5) D1 retention based on signup event
WITH signups AS (
  SELECT
    user_id,
    MIN(CAST(event_time AS DATE)) AS signup_date
  FROM events
  WHERE event_name = 'signup'
  GROUP BY user_id
),
returns AS (
  SELECT DISTINCT
    s.user_id,
    s.signup_date
  FROM signups s
  JOIN events e
    ON e.user_id = s.user_id
   AND CAST(e.event_time AS DATE) = DATEADD(day, 1, s.signup_date)
)
SELECT
  s.signup_date,
  COUNT(DISTINCT s.user_id) AS signup_users,
  COUNT(DISTINCT r.user_id) AS d1_returning_users,
  CASE
    WHEN COUNT(DISTINCT s.user_id) = 0 THEN 0
    ELSE 1.0 * COUNT(DISTINCT r.user_id) / COUNT(DISTINCT s.user_id)
  END AS d1_retention_rate
FROM signups s
LEFT JOIN returns r
  ON s.user_id = r.user_id
 AND s.signup_date = r.signup_date
GROUP BY s.signup_date
ORDER BY s.signup_date;
