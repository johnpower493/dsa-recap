-- Data Quality Checks (Reusable Patterns)

-- 1) Null Check
-- Detect required fields that are null
select
  count(*) as null_count
from analytics.customer_master
where customer_id is null;

-- 2) Uniqueness Check
-- Detect duplicate primary keys
select
  customer_id,
  count(*) as dup_count
from analytics.customer_master
group by customer_id
having count(*) > 1;

-- 3) Validity Check (Accepted Values)
select
  status,
  count(*) as status_count
from analytics.customer_master
group by status
having status not in ('ACTIVE', 'INACTIVE');

-- 4) Freshness Check
select
  max(updated_at) as last_updated_at,
  case when max(updated_at) < dateadd(hour, -2, current_timestamp())
       then 'STALE' else 'FRESH' end as freshness_status
from analytics.customer_master;

-- 5) Volume Check (Row Count Variance)
with daily_counts as (
  select load_date, count(*) as row_count
  from analytics.customer_master
  group by load_date
)
select
  load_date,
  row_count,
  avg(row_count) over (order by load_date rows between 7 preceding and 1 preceding) as rolling_avg,
  case when row_count < 0.7 * rolling_avg or row_count > 1.3 * rolling_avg
       then 'ANOMALY' else 'OK' end as status
from daily_counts;
