-- Snowflake Senior-Level Exercises

-- 1) RBAC + Masking Policies (pseudo-structure)
-- Create a role hierarchy and mask PII fields for non-privileged roles.
-- NOTE: Replace db/schema/table names with your environment.

-- Example (structure only, not runnable here):
-- create role analyst_role;
-- create role pii_access_role;
-- grant role pii_access_role to role analyst_role;
--
-- create or replace masking policy mask_email as (val string) returns string ->
--   case when current_role() in ('PII_ACCESS_ROLE') then val else '***' end;
--
-- alter table analytics.customers modify column email set masking policy mask_email;

-- 2) Resource Monitor
-- Create a resource monitor to limit warehouse credits.
-- create or replace resource monitor dev_monitor with credit_quota=20
--  triggers on 80 percent do notify
--  triggers on 100 percent do suspend;

-- 3) Warehouse Configuration
-- Create separate warehouses for ETL and BI workloads
-- create warehouse etl_wh warehouse_size='MEDIUM' auto_suspend=300 auto_resume=true;
-- create warehouse bi_wh warehouse_size='SMALL' auto_suspend=120 auto_resume=true;

-- 4) Query Profiling
-- Use QUERY_HISTORY to identify slow queries and analyze their profile.
-- select * from table(information_schema.query_history())
-- order by total_elapsed_time desc limit 10;
