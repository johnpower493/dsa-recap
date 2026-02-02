-- Exercise: late-arriving data handling (SQL prompts)
--
-- Common approach: rebuild a rolling window of partitions (e.g., last 3 days) each run.
-- This trades extra compute for correctness when events arrive late.

SET search_path = de_demo, public;

-- Prompt 1:
-- Write a query that computes daily net revenue for ONLY the last 3 days relative to current_date.
--
-- Answer:


-- Prompt 2:
-- Create a table agg_daily_revenue(day, net_revenue_cents) and show an idempotent rebuild
-- strategy for the last 3 days:
--   - DELETE FROM agg_daily_revenue WHERE day in (...window...)
--   - INSERT INTO agg_daily_revenue SELECT ... for window
--
-- Answer:
