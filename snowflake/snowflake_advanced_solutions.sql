-- Snowflake Advanced Solutions (Reference)
-- ------------------------------------------------------------
-- IMPORTANT:
-- - Update object names to match your Snowflake environment.
-- - Run with an admin-capable role in a non-production account.
-- - Some statements are illustrative and may require adaptation.

/* ============================================================
   Solution 0: Environment bootstrap
   ============================================================ */

use role SYSADMIN;

create warehouse if not exists ETL_WH
  warehouse_size = 'MEDIUM'
  auto_suspend = 300
  auto_resume = true
  initially_suspended = true;

create warehouse if not exists BI_WH
  warehouse_size = 'SMALL'
  auto_suspend = 120
  auto_resume = true
  initially_suspended = true;

create database if not exists ADV_SNOWFLAKE_LAB;
create schema if not exists ADV_SNOWFLAKE_LAB.RAW;
create schema if not exists ADV_SNOWFLAKE_LAB.CURATED;
create schema if not exists ADV_SNOWFLAKE_LAB.MART;
create schema if not exists ADV_SNOWFLAKE_LAB.GOVERNANCE;

create or replace table ADV_SNOWFLAKE_LAB.RAW.ORDERS_LANDING (
  order_id number,
  customer_id number,
  order_ts timestamp_ntz,
  region string,
  status string,
  amount number(18,2),
  updated_at timestamp_ntz,
  op string -- I/U/D style op marker from source
);

create or replace table ADV_SNOWFLAKE_LAB.CURATED.CUSTOMERS (
  customer_id number,
  customer_name string,
  customer_email string,
  region string,
  updated_at timestamp_ntz
);

create or replace table ADV_SNOWFLAKE_LAB.CURATED.ORDERS (
  order_id number,
  customer_id number,
  order_ts timestamp_ntz,
  region string,
  status string,
  amount number(18,2),
  updated_at timestamp_ntz
);


/* ============================================================
   Solution 1: Policy-driven security
   ============================================================ */

use role SECURITYADMIN;

create role if not exists DATA_ENGINEER_ROLE;
create role if not exists ANALYST_ROLE;
create role if not exists PII_ACCESS_ROLE;

-- Example hierarchy (adjust to your org model)
grant role ANALYST_ROLE to role DATA_ENGINEER_ROLE;
grant role PII_ACCESS_ROLE to role DATA_ENGINEER_ROLE;

-- Grant roles to users (replace usernames)
-- grant role ANALYST_ROLE to user analyst_user;
-- grant role PII_ACCESS_ROLE to user governance_user;

use role SYSADMIN;

grant usage on database ADV_SNOWFLAKE_LAB to role ANALYST_ROLE;
grant usage on schema ADV_SNOWFLAKE_LAB.CURATED to role ANALYST_ROLE;
grant select on all tables in schema ADV_SNOWFLAKE_LAB.CURATED to role ANALYST_ROLE;
grant select on future tables in schema ADV_SNOWFLAKE_LAB.CURATED to role ANALYST_ROLE;

use role SECURITYADMIN;

create or replace masking policy ADV_SNOWFLAKE_LAB.GOVERNANCE.MP_MASK_EMAIL
as (val string) returns string ->
  case
    when is_role_in_session('PII_ACCESS_ROLE') then val
    else regexp_replace(val, '(^.).*(@.*$)', '\\1***\\2')
  end;

create or replace row access policy ADV_SNOWFLAKE_LAB.GOVERNANCE.RAP_REGION_FILTER
as (region_val string) returns boolean ->
  case
    when current_role() = 'DATA_ENGINEER_ROLE' then true
    when current_role() = 'ANALYST_ROLE' and region_val in ('NA','APAC') then true
    else false
  end;

alter table ADV_SNOWFLAKE_LAB.CURATED.CUSTOMERS
  modify column customer_email
  set masking policy ADV_SNOWFLAKE_LAB.GOVERNANCE.MP_MASK_EMAIL;

alter table ADV_SNOWFLAKE_LAB.CURATED.CUSTOMERS
  add row access policy ADV_SNOWFLAKE_LAB.GOVERNANCE.RAP_REGION_FILTER
  on (region);

-- Test by switching role:
-- use role ANALYST_ROLE;
-- select customer_id, customer_email, region from ADV_SNOWFLAKE_LAB.CURATED.CUSTOMERS;
-- use role PII_ACCESS_ROLE;
-- select customer_id, customer_email, region from ADV_SNOWFLAKE_LAB.CURATED.CUSTOMERS;


/* ============================================================
   Solution 2: Governance tags
   ============================================================ */

use role ACCOUNTADMIN;

create or replace tag ADV_SNOWFLAKE_LAB.GOVERNANCE.DATA_CLASSIFICATION
  allowed_values 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED';

create or replace tag ADV_SNOWFLAKE_LAB.GOVERNANCE.DOMAIN
  allowed_values 'SALES', 'FINANCE', 'PRODUCT';

alter table ADV_SNOWFLAKE_LAB.CURATED.CUSTOMERS
  set tag ADV_SNOWFLAKE_LAB.GOVERNANCE.DOMAIN = 'PRODUCT';

alter table ADV_SNOWFLAKE_LAB.CURATED.CUSTOMERS
  modify column customer_email
  set tag ADV_SNOWFLAKE_LAB.GOVERNANCE.DATA_CLASSIFICATION = 'RESTRICTED';

alter table ADV_SNOWFLAKE_LAB.CURATED.ORDERS
  set tag ADV_SNOWFLAKE_LAB.GOVERNANCE.DOMAIN = 'SALES';

select *
from snowflake.account_usage.tag_references
where object_database = 'ADV_SNOWFLAKE_LAB'
order by object_schema, object_name;


/* ============================================================
   Solution 3: Cost governance + workload isolation
   ============================================================ */

use role ACCOUNTADMIN;

create or replace resource monitor BI_MONITOR
  with credit_quota = 100
  frequency = monthly
  start_timestamp = immediately
  triggers
    on 75 percent do notify
    on 90 percent do notify
    on 100 percent do suspend;

alter warehouse BI_WH set resource_monitor = BI_MONITOR;

-- Credit trend report
select
  start_time::date as usage_date,
  warehouse_name,
  sum(credits_used_compute) as credits_compute,
  sum(credits_used_cloud_services) as credits_cloud_services,
  sum(credits_used_compute + credits_used_cloud_services) as credits_total
from snowflake.account_usage.warehouse_metering_history
where warehouse_name in ('BI_WH', 'ETL_WH')
  and start_time >= dateadd(day, -30, current_timestamp())
group by 1,2
order by 1 desc, 2;


/* ============================================================
   Solution 4: Query observability + optimization
   ============================================================ */

-- Slow query candidates
select
  query_id,
  warehouse_name,
  total_elapsed_time,
  bytes_scanned,
  rows_produced,
  start_time,
  query_text
from snowflake.account_usage.query_history
where start_time >= dateadd(day, -7, current_timestamp())
  and execution_status = 'SUCCESS'
order by total_elapsed_time desc
limit 20;

-- Example optimization candidates:
-- 1) Clustering key for range pruning on order_ts, region
alter table ADV_SNOWFLAKE_LAB.CURATED.ORDERS
  cluster by (to_date(order_ts), region);

-- 2) Materialized view for common aggregation
create or replace materialized view ADV_SNOWFLAKE_LAB.MART.MV_ORDERS_DAILY as
select
  to_date(order_ts) as order_date,
  region,
  count(*) as order_count,
  sum(amount) as gross_sales
from ADV_SNOWFLAKE_LAB.CURATED.ORDERS
group by 1,2;

-- Compare before/after by re-running target query and checking query_history metrics.


/* ============================================================
   Solution 5: Zero-copy clone + Time Travel release pattern
   ============================================================ */

use role SYSADMIN;

create or replace schema ADV_SNOWFLAKE_LAB.CURATED_RELEASE_CANDIDATE clone ADV_SNOWFLAKE_LAB.CURATED;

-- Simulated breaking change in clone
alter table ADV_SNOWFLAKE_LAB.CURATED_RELEASE_CANDIDATE.ORDERS
  alter column amount set data type number(10,0);

-- Rollback path 1: recreate clone from source
-- create or replace schema ADV_SNOWFLAKE_LAB.CURATED_RELEASE_CANDIDATE clone ADV_SNOWFLAKE_LAB.CURATED;

-- Rollback path 2: Time Travel style restore (table-level example)
-- create or replace table ADV_SNOWFLAKE_LAB.CURATED_RELEASE_CANDIDATE.ORDERS
--   clone ADV_SNOWFLAKE_LAB.CURATED_RELEASE_CANDIDATE.ORDERS before (statement => '<statement_id>');


/* ============================================================
   Solution 6: Incremental ELT with Streams + Tasks
   ============================================================ */

use role SYSADMIN;

create or replace stream ADV_SNOWFLAKE_LAB.RAW.ORDERS_LANDING_STRM
  on table ADV_SNOWFLAKE_LAB.RAW.ORDERS_LANDING
  append_only = false;

create or replace task ADV_SNOWFLAKE_LAB.CURATED.TASK_UPSERT_ORDERS
  warehouse = ETL_WH
  schedule = '5 minute'
as
merge into ADV_SNOWFLAKE_LAB.CURATED.ORDERS t
using (
  select
    order_id,
    customer_id,
    order_ts,
    region,
    status,
    amount,
    updated_at,
    op,
    metadata$action as stream_action
  from ADV_SNOWFLAKE_LAB.RAW.ORDERS_LANDING_STRM
) s
on t.order_id = s.order_id
when matched and s.op = 'D' then delete
when matched and s.op in ('U','I') and s.updated_at >= t.updated_at then
  update set
    customer_id = s.customer_id,
    order_ts = s.order_ts,
    region = s.region,
    status = s.status,
    amount = s.amount,
    updated_at = s.updated_at
when not matched and s.op in ('I','U') then
  insert (order_id, customer_id, order_ts, region, status, amount, updated_at)
  values (s.order_id, s.customer_id, s.order_ts, s.region, s.status, s.amount, s.updated_at);

create or replace task ADV_SNOWFLAKE_LAB.MART.TASK_REFRESH_ORDERS_DAILY
  warehouse = ETL_WH
  after ADV_SNOWFLAKE_LAB.CURATED.TASK_UPSERT_ORDERS
as
create or replace table ADV_SNOWFLAKE_LAB.MART.ORDERS_DAILY as
select
  to_date(order_ts) as order_date,
  region,
  count(*) as order_count,
  sum(amount) as gross_sales
from ADV_SNOWFLAKE_LAB.CURATED.ORDERS
group by 1,2;

alter task ADV_SNOWFLAKE_LAB.CURATED.TASK_UPSERT_ORDERS resume;
alter task ADV_SNOWFLAKE_LAB.MART.TASK_REFRESH_ORDERS_DAILY resume;

select *
from table(information_schema.task_history(
  scheduled_time_range_start => dateadd('day', -1, current_timestamp())
))
where name in ('TASK_UPSERT_ORDERS', 'TASK_REFRESH_ORDERS_DAILY')
order by scheduled_time desc;


/* ============================================================
   Solution 7: Dynamic tables
   ============================================================ */

use role SYSADMIN;

create or replace dynamic table ADV_SNOWFLAKE_LAB.CURATED.DT_ORDERS_CLEAN
  target_lag = '5 minutes'
  warehouse = ETL_WH
as
select
  order_id,
  customer_id,
  order_ts,
  region,
  upper(status) as status,
  amount,
  updated_at
from ADV_SNOWFLAKE_LAB.RAW.ORDERS_LANDING
qualify row_number() over (partition by order_id order by updated_at desc) = 1;

create or replace dynamic table ADV_SNOWFLAKE_LAB.MART.DT_ORDERS_DAILY
  target_lag = '10 minutes'
  warehouse = ETL_WH
as
select
  to_date(order_ts) as order_date,
  region,
  count(*) as order_count,
  sum(amount) as gross_sales
from ADV_SNOWFLAKE_LAB.CURATED.DT_ORDERS_CLEAN
group by 1,2;

-- Inspect dynamic table refresh history
select *
from table(information_schema.dynamic_table_refresh_history(
  name => 'ADV_SNOWFLAKE_LAB.CURATED.DT_ORDERS_CLEAN',
  result_limit => 50
))
order by data_timestamp desc;


/* ============================================================
   Solution 8: Secure data sharing
   ============================================================ */

use role SYSADMIN;

create or replace secure view ADV_SNOWFLAKE_LAB.MART.V_PARTNER_SALES as
select
  order_date,
  region,
  order_count,
  gross_sales
from ADV_SNOWFLAKE_LAB.MART.ORDERS_DAILY
where region in ('NA', 'APAC');

use role ACCOUNTADMIN;

create share if not exists PARTNER_SALES_SHARE;

grant usage on database ADV_SNOWFLAKE_LAB to share PARTNER_SALES_SHARE;
grant usage on schema ADV_SNOWFLAKE_LAB.MART to share PARTNER_SALES_SHARE;
grant select on view ADV_SNOWFLAKE_LAB.MART.V_PARTNER_SALES to share PARTNER_SALES_SHARE;

-- Reader account pattern (replace with your values)
-- create managed account if not exists PARTNER_READER
--   admin_name = partner_admin
--   admin_password = '<TempStrongPassword1!>'
--   type = reader;
-- alter share PARTNER_SALES_SHARE add accounts = <reader_account_locator>;


/* ============================================================
   Quick trade-off notes (for review)
   ============================================================
   - Streams/Tasks: more control + complex DAGs, but more operational overhead.
   - Dynamic tables: simpler declarative refresh, but fewer custom orchestration controls.
   - Materialized views: great for repeated aggregates, watch maintenance cost.
   - Clustering: useful for large selective scans; monitor reclustering credits.
   - Search optimization: best for point lookup / selective predicates.
*/
