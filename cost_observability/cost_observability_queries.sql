-- Cost & Performance Observability Queries (Snowflake)

-- 1) Warehouse Credit Usage (daily)
select
  date_trunc('day', start_time) as usage_date,
  warehouse_name,
  sum(credits_used) as credits_used
from snowflake.account_usage.warehouse_metering_history
group by 1, 2
order by usage_date desc, credits_used desc;

-- 2) Cost by Warehouse (rolling 30 days)
select
  warehouse_name,
  sum(credits_used) as credits_last_30d
from snowflake.account_usage.warehouse_metering_history
where start_time >= dateadd(day, -30, current_timestamp())
group by 1
order by credits_last_30d desc;

-- 3) Top Expensive Queries (by credits)
select
  query_id,
  user_name,
  warehouse_name,
  total_elapsed_time / 1000 as seconds_elapsed,
  credits_used_cloud_services,
  (credits_used_cloud_services) as credits_used,
  query_text
from snowflake.account_usage.query_history
where start_time >= dateadd(day, -7, current_timestamp())
order by credits_used desc
limit 20;

-- 4) Concurrency & Queuing (warehouse load)
select
  start_time,
  end_time,
  warehouse_name,
  avg_running,
  avg_queued_load,
  avg_queued_provisioning
from snowflake.account_usage.warehouse_load_history
where start_time >= dateadd(day, -7, current_timestamp())
order by start_time desc;

-- 5) Tag-Based Chargeback (requires tagging)
-- Example: group cost by tag values
-- select
--   tr.tag_name,
--   tr.tag_value,
--   sum(wmh.credits_used) as credits_used
-- from snowflake.account_usage.tag_references tr
-- join snowflake.account_usage.warehouse_metering_history wmh
--   on tr.object_name = wmh.warehouse_name
-- where tr.domain = 'WAREHOUSE'
-- group by 1, 2
-- order by credits_used desc;
