-- Snowflake Senior-Level Exercises
-- ============================================================
-- Audience: Senior Data Engineers / Analytics Engineers
-- Goal: Foundational exercises covering key Snowflake platform
--       features before advancing to complex scenarios.
--
-- Notes:
-- 1) Replace object names (DB/SCHEMA/ROLE/WH) to match your account.
-- 2) Some features require ACCOUNTADMIN/SECURITYADMIN privileges.
-- 3) Run in a non-production environment.

/* ============================================================
   Exercise 0: Environment Setup
   ============================================================
   Create a lab environment for subsequent exercises.

   TODO:
   1) Create database: SNOWFLAKE_LAB
   2) Create schemas: RAW, CURATED, MART
   3) Create warehouses: ETL_WH, BI_WH
   4) Create sample tables in RAW schema
*/

-- Create database and schemas
CREATE OR REPLACE DATABASE SNOWFLAKE_LAB;

CREATE OR REPLACE SCHEMA SNOWFLAKE_LAB.RAW;
CREATE OR REPLACE SCHEMA SNOWFLAKE_LAB.CURATED;
CREATE OR REPLACE SCHEMA SNOWFLAKE_LAB.MART;

-- Create warehouses for different workloads
CREATE OR REPLACE WAREHOUSE ETL_WH 
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 2
  SCALING_POLICY = 'STANDARD';

CREATE OR REPLACE WAREHOUSE BI_WH 
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 4
  SCALING_POLICY = 'STANDARD';

-- Sample table for testing
CREATE OR REPLACE TABLE SNOWFLAKE_LAB.RAW.ORDERS_LANDING (
  ORDER_ID NUMBER(38,0) PRIMARY KEY,
  CUSTOMER_ID NUMBER(38,0),
  ORDER_DATE TIMESTAMP_NTZ,
  ORDER_AMOUNT NUMBER(10,2),
  REGION VARCHAR(50),
  STATUS VARCHAR(20)
);

INSERT INTO SNOWFLAKE_LAB.RAW.ORDERS_LANDING VALUES
  (1, 100, '2024-01-15 10:30:00', 250.00, 'NA', 'COMPLETED'),
  (2, 101, '2024-01-15 11:45:00', 150.50, 'NA', 'PENDING'),
  (3, 102, '2024-01-16 09:20:00', 320.00, 'APAC', 'COMPLETED'),
  (4, 103, '2024-01-16 14:10:00', 89.99, 'EU', 'CANCELLED'),
  (5, 100, '2024-01-17 16:30:00', 175.00, 'NA', 'COMPLETED');

/* ============================================================
   Exercise 1: Role-Based Access Control (RBAC)
   ============================================================
   Build a basic role hierarchy with least-privilege principles.

   TODO:
   1) Create roles: DATA_ENGINEER_ROLE, ANALYST_ROLE, PII_ACCESS_ROLE
   2) Create role hierarchy (grant roles to other roles)
   3) Grant object privileges appropriately
   4) Test role switching and access
*/

-- Create roles
CREATE ROLE IF NOT EXISTS DATA_ENGINEER_ROLE;
CREATE ROLE IF NOT EXISTS ANALYST_ROLE;
CREATE ROLE IF NOT EXISTS PII_ACCESS_ROLE;

-- Grant roles to create hierarchy
GRANT ROLE PII_ACCESS_ROLE TO ROLE ANALYST_ROLE;

-- Grant system privileges to data engineer
GRANT CREATE DATABASE ON ACCOUNT TO ROLE DATA_ENGINEER_ROLE;
GRANT CREATE SCHEMA ON ACCOUNT TO ROLE DATA_ENGINEER_ROLE;
GRANT CREATE TABLE ON ACCOUNT TO ROLE DATA_ENGINEER_ROLE;
GRANT CREATE VIEW ON ACCOUNT TO ROLE DATA_ENGINEER_ROLE;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE DATA_ENGINEER_ROLE;

-- Grant usage on warehouse to roles
GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE ANALYST_ROLE;
GRANT USAGE ON WAREHOUSE BI_WH TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON WAREHOUSE BI_WH TO ROLE ANALYST_ROLE;

-- Grant access to schemas
GRANT USAGE ON DATABASE SNOWFLAKE_LAB TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA SNOWFLAKE_LAB.CURATED TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA SNOWFLAKE_LAB.MART TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA SNOWFLAKE_LAB.CURATED TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA SNOWFLAKE_LAB.MART TO ROLE ANALYST_ROLE;

-- Test query to verify role hierarchy
-- SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE();

/* ============================================================
   Exercise 2: Dynamic Data Masking
   ============================================================
   Protect sensitive data using masking policies.

   TODO:
   1) Create a table with PII columns
   2) Create masking policies for email and phone
   3) Apply policies to columns
   4) Test with different roles
*/

-- Create customer table with PII
CREATE OR REPLACE TABLE SNOWFLAKE_LAB.CURATED.CUSTOMERS (
  CUSTOMER_ID NUMBER(38,0) PRIMARY KEY,
  CUSTOMER_NAME VARCHAR(100),
  EMAIL VARCHAR(100),
  PHONE VARCHAR(20),
  REGION VARCHAR(50),
  CREDIT_SCORE NUMBER(3,0)
);

INSERT INTO SNOWFLAKE_LAB.CURATED.CUSTOMERS VALUES
  (100, 'John Doe', 'john.doe@example.com', '555-1234', 'NA', 750),
  (101, 'Jane Smith', 'jane.smith@example.com', '555-5678', 'NA', 720),
  (102, 'Bob Johnson', 'bob.j@example.com', '555-9012', 'APAC', 680),
  (103, 'Alice Brown', 'alice.b@example.com', '555-3456', 'EU', 800);

-- Create masking policy for email
CREATE OR REPLACE MASKING POLICY SNOWFLAKE_LAB.GOVERNANCE.MASK_EMAIL 
AS (val STRING) RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SECURITYADMIN', 'PII_ACCESS_ROLE') 
    THEN val
    ELSE REGEXP_REPLACE(val, '(.{2})(.*)(@.*)', '\\1***\\3')
  END;

-- Create masking policy for phone
CREATE OR REPLACE MASKING POLICY SNOWFLAKE_LAB.GOVERNANCE.MASK_PHONE 
AS (val STRING) RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SECURITYADMIN', 'PII_ACCESS_ROLE') 
    THEN val
    ELSE '***-***-' + RIGHT(val, 4)
  END;

-- Apply masking policies
ALTER TABLE SNOWFLAKE_LAB.CURATED.CUSTOMERS 
MODIFY COLUMN EMAIL SET MASKING POLICY SNOWFLAKE_LAB.GOVERNANCE.MASK_EMAIL;

ALTER TABLE SNOWFLAKE_LAB.CURATED.CUSTOMERS 
MODIFY COLUMN PHONE SET MASKING POLICY SNOWFLAKE_LAB.GOVERNANCE.MASK_PHONE;

/* ============================================================
   Exercise 3: Row Access Policies
   ============================================================
   Implement row-level security based on region.

   TODO:
   1) Create row access policy for region filtering
   2) Apply policy to table
   3) Test with different role assignments
*/

-- Create row access policy
CREATE OR REPLACE ROW ACCESS POLICY SNOWFLAKE_LAB.GOVERNANCE.REGION_FILTER
AS (region_column VARCHAR) RETURNS BOOLEAN ->
  CASE 
    -- Allow all access to admins
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SECURITYADMIN', 'DATA_ENGINEER_ROLE') 
    THEN TRUE
    -- NA analysts see only NA rows
    WHEN CURRENT_ROLE() = 'ANALYST_ROLE_NA' THEN region_column = 'NA'
    -- APAC analysts see only APAC rows
    WHEN CURRENT_ROLE() = 'ANALYST_ROLE_APAC' THEN region_column = 'APAC'
    -- EU analysts see only EU rows
    WHEN CURRENT_ROLE() = 'ANALYST_ROLE_EU' THEN region_column = 'EU'
    -- Default: no access
    ELSE FALSE
  END;

-- Apply row access policy
ALTER TABLE SNOWFLAKE_LAB.CURATED.CUSTOMERS 
ADD ROW ACCESS POLICY SNOWFLAKE_LAB.GOVERNANCE.REGION_FILTER 
ON (REGION);

ALTER TABLE SNOWFLAKE_LAB.RAW.ORDERS_LANDING 
ADD ROW ACCESS POLICY SNOWFLAKE_LAB.GOVERNANCE.REGION_FILTER 
ON (REGION);

/* ============================================================
   Exercise 4: Resource Monitors and Cost Governance
   ============================================================
   Implement credit quotas and monitoring triggers.

   TODO:
   1) Create resource monitor with multiple triggers
   2) Attach monitor to BI warehouse
   3) Query warehouse credit usage
   4) Set up alerts for credit thresholds
*/

-- Create resource monitor
CREATE OR REPLACE RESOURCE MONITOR SNOWFLAKE_LAB.GOVERNANCE.BI_WH_MONITOR
WITH CREDIT_QUOTA = 100
FREQUENCY = 'MONTHLY'
START_TIMESTAMP = IMMEDIATELY
TRIGGERS
  ON 75 PERCENT DO NOTIFY
  ON 90 PERCENT DO NOTIFY
  ON 100 PERCENT DO SUSPEND_IMMEDIATE
  ON 110 PERCENT DO SUSPEND_IMMEDIATE;

-- Attach monitor to warehouse
ALTER WAREHOUSE BI_WH SET RESOURCE_MONITOR = SNOWFLAKE_LAB.GOVERNANCE.BI_WH_MONITOR;

-- Query warehouse credit usage
SELECT 
  WAREHOUSE_NAME,
  START_TIME,
  END_TIME,
  CREDITS_USED,
  CREDITS_USED_COMPUTE,
  CREDITS_USED_CLOUD_SERVICES
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'BI_WH'
ORDER BY START_TIME DESC
LIMIT 10;

/* ============================================================
   Exercise 5: Query Profiling and Performance Analysis
   ============================================================
   Identify and analyze slow queries.

   TODO:
   1) Use QUERY_HISTORY to find slow queries
   2) Analyze query profile metrics
   3) Identify optimization opportunities
*/

-- Find slow queries in the last 7 days
SELECT 
  QUERY_ID,
  QUERY_TEXT,
  DATABASE_NAME,
  SCHEMA_NAME,
  USER_NAME,
  WAREHOUSE_NAME,
  EXECUTION_STATUS,
  TOTAL_ELAPSED_TIME/1000 AS TOTAL_ELAPSED_SECONDS,
  BYTES_SCANNED,
  PARTITIONS_SCANNED,
  PARTITIONS_TOTAL,
  ROWS_PRODUCED,
  START_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
  AND EXECUTION_STATUS = 'SUCCESS'
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 10;

-- Query warehouse performance metrics
SELECT 
  WAREHOUSE_NAME,
  AVG(TOTAL_ELAPSED_TIME)/1000 AS AVG_ELAPSED_SECONDS,
  AVG(BYTES_SCANNED) AS AVG_BYTES_SCANNED,
  COUNT(*) AS QUERY_COUNT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME IS NOT NULL
GROUP BY WAREHOUSE_NAME
ORDER BY AVG_ELAPSED_SECONDS DESC;

/* ============================================================
   Exercise 6: Clustering and Optimization
   ============================================================
   Implement clustering keys for improved query performance.

   TODO:
   1) Create a large table (if not exists)
   2) Add clustering key
   3) Monitor clustering information
   4) Compare query performance before/after
*/

-- Create orders table with sample data
CREATE OR REPLACE TABLE SNOWFLAKE_LAB.CURATED.ORDERS (
  ORDER_ID NUMBER(38,0),
  CUSTOMER_ID NUMBER(38,0),
  ORDER_DATE TIMESTAMP_NTZ,
  ORDER_AMOUNT NUMBER(10,2),
  REGION VARCHAR(50),
  STATUS VARCHAR(20)
);

-- Insert more sample data
INSERT INTO SNOWFLAKE_LAB.CURATED.ORDERS
SELECT 
  SEQ,
  MOD(SEQ, 50) + 100,
  DATEADD(HOUR, MOD(SEQ, 1000), '2024-01-01'::TIMESTAMP_NTZ),
  ROUND(RANDOM() * 500 + 50, 2),
  CASE MOD(SEQ, 3)
    WHEN 0 THEN 'NA'
    WHEN 1 THEN 'APAC'
    ELSE 'EU'
  END,
  CASE MOD(SEQ, 4)
    WHEN 0 THEN 'COMPLETED'
    WHEN 1 THEN 'PENDING'
    WHEN 2 THEN 'CANCELLED'
    ELSE 'REFUNDED'
  END
FROM TABLE(GENERATOR(ROWCOUNT => 10000)) AS T(SEQ);

-- Add clustering key on ORDER_DATE and REGION
ALTER TABLE SNOWFLAKE_LAB.CURATED.ORDERS 
CLUSTER BY (ORDER_DATE, REGION);

-- Recluster table
ALTER TABLE SNOWFLAKE_LAB.CURATED.ORDERS RECLUSTER;

-- Check clustering information
SELECT 
  TABLE_NAME,
  CLUSTERING_KEY,
  DEPTH,
  AVG_DEPTH,
  PARTITION_COUNT
FROM TABLE(INFORMATION_SCHEMA.CLUSTERING_INFORMATION(
  TABLE_NAME => 'ORDERS',
  SCHEMA_NAME => 'CURATED',
  DATABASE_NAME => 'SNOWFLAKE_LAB'
));

/* ============================================================
   Exercise 7: Search Optimization Service
   ============================================================
   Configure search optimization for selective queries.

   TODO:
   1) Add search optimization to table
   2) Test selective queries with specific filters
   3) Compare query plans and performance
*/

-- Add search optimization
ALTER TABLE SNOWFLAKE_LAB.CURATED.ORDERS ADD SEARCH OPTIMIZATION;

-- Verify search optimization is enabled
SELECT 
  TABLE_NAME,
  IS_ENABLED
FROM TABLE(INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY(
  TABLE_NAME => 'ORDERS',
  SCHEMA_NAME => 'CURATED',
  DATABASE_NAME => 'SNOWFLAKE_LAB'
));

/* ============================================================
   Exercise 8: Materialized Views
   ============================================================
   Create materialized views for pre-aggregation.

   TODO:
   1) Create materialized view for order summaries
   2) Query materialized view
   3) Check refresh status
*/

-- Create materialized view for daily order summaries
CREATE OR REPLACE MATERIALIZED VIEW SNOWFLAKE_LAB.MART.DAILY_ORDER_SUMMARY
AS
SELECT 
  DATE_TRUNC('DAY', ORDER_DATE) AS ORDER_DAY,
  REGION,
  STATUS,
  COUNT(*) AS ORDER_COUNT,
  SUM(ORDER_AMOUNT) AS TOTAL_AMOUNT,
  AVG(ORDER_AMOUNT) AS AVG_AMOUNT
FROM SNOWFLAKE_LAB.CURATED.ORDERS
GROUP BY DATE_TRUNC('DAY', ORDER_DATE), REGION, STATUS;

-- Check materialized view refresh status
SELECT 
  TABLE_NAME,
  LAST_REFRESH_TIME,
  REFRESH_STATE
FROM TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY(
  TABLE_NAME => 'DAILY_ORDER_SUMMARY',
  SCHEMA_NAME => 'MART',
  DATABASE_NAME => 'SNOWFLAKE_LAB'
));

/* ============================================================
   Exercise 9: Zero-Copy Cloning
   ============================================================
   Use cloning for safe testing and rollback.

   TODO:
   1) Clone a schema
   2) Make changes in clone
   3) Validate changes
   4) Swap schemas if needed for production
*/

-- Clone CURATED schema
CREATE OR REPLACE SCHEMA SNOWFLAKE_LAB.CURATED_CLONE CLONE SNOWFLAKE_LAB.CURATED;

-- Verify clone
SELECT 
  TABLE_NAME,
  TABLE_SCHEMA,
  ROW_COUNT,
  BYTES
FROM SNOWFLAKE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'CURATED_CLONE'
  AND TABLE_DATABASE = 'SNOWFLAKE_LAB';

/* ============================================================
   Exercise 10: Time Travel
   ============================================================
   Query historical data and restore dropped objects.

   TODO:
   1) Query table at a specific timestamp
   2) Restore a dropped table
   3) Compare current vs historical data
*/

-- Query table as of 1 hour ago
SELECT * FROM SNOWFLAKE_LAB.CURATED.ORDERS 
AT(OFFSET => -60 * 60)  -- 1 hour ago
LIMIT 10;

-- Query table at specific timestamp
SELECT * FROM SNOWFLAKE_LAB.CURATED.ORDERS 
AT(TIMESTAMP => '2024-01-15 00:00:00'::TIMESTAMP_NTZ)
LIMIT 10;

-- Show Time Travel limits
SELECT * FROM TABLE(INFORMATION_SCHEMA.DATABASE_STORAGE_USAGE_HISTORY(
  DATABASE_NAME => 'SNOWFLAKE_LAB'
));

/* ============================================================
   Exercise 11: Streams and Tasks
   ============================================================
   Build a basic incremental pipeline using streams and tasks.

   TODO:
   1) Create stream on source table
   2) Create task to process stream changes
   3) Set up task dependencies
   4) Test with data changes
*/

-- Create stream on ORDERS_LANDING
CREATE OR REPLACE STREAM SNOWFLAKE_LAB.RAW.ORDERS_LANDING_STREAM
ON TABLE SNOWFLAKE_LAB.RAW.ORDERS_LANDING;

-- Create target table for processed orders
CREATE OR REPLACE TABLE SNOWFLAKE_LAB.CURATED.ORDERS_PROCESSED (
  ORDER_ID NUMBER(38,0),
  CUSTOMER_ID NUMBER(38,0),
  ORDER_DATE TIMESTAMP_NTZ,
  ORDER_AMOUNT NUMBER(10,2),
  REGION VARCHAR(50),
  STATUS VARCHAR(20),
  PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  METADATA$ACTION VARCHAR(10),
  METADATA$ISUPDATE BOOLEAN
);

-- Create task to process stream changes
CREATE OR REPLACE TASK SNOWFLAKE_LAB.CURATED.PROCESS_ORDERS_TASK
WAREHOUSE = ETL_WH
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('SNOWFLAKE_LAB.RAW.ORDERS_LANDING_STREAM')
AS
MERGE INTO SNOWFLAKE_LAB.CURATED.ORDERS_PROCESSED AS TARGET
USING (
  SELECT 
    ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_AMOUNT, REGION, STATUS,
    METADATA$ACTION, METADATA$ISUPDATE
  FROM SNOWFLAKE_LAB.RAW.ORDERS_LANDING_STREAM
) AS SOURCE
ON TARGET.ORDER_ID = SOURCE.ORDER_ID
WHEN MATCHED AND SOURCE.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED AND SOURCE.METADATA$ISUPDATE THEN
  UPDATE SET
    CUSTOMER_ID = SOURCE.CUSTOMER_ID,
    ORDER_DATE = SOURCE.ORDER_DATE,
    ORDER_AMOUNT = SOURCE.ORDER_AMOUNT,
    REGION = SOURCE.REGION,
    STATUS = SOURCE.STATUS,
    PROCESSED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_AMOUNT, REGION, STATUS, PROCESSED_AT)
  VALUES (SOURCE.ORDER_ID, SOURCE.CUSTOMER_ID, SOURCE.ORDER_DATE, SOURCE.ORDER_AMOUNT, 
          SOURCE.REGION, SOURCE.STATUS, CURRENT_TIMESTAMP());

-- Resume task
ALTER TASK SNOWFLAKE_LAB.CURATED.PROCESS_ORDERS_TASK RESUME;

-- Check stream changes
SELECT * FROM SNOWFLAKE_LAB.RAW.ORDERS_LANDING_STREAM;

/* ============================================================
   Exercise 12: Dynamic Tables (Snowflake Feature)
   ============================================================
   Create dynamic tables for declarative transformations.

   TODO:
   1) Create dynamic table for cleaned orders
   2) Create dynamic table for aggregated metrics
   3) Monitor refresh history
*/

-- Create dynamic table for cleaned orders
CREATE OR REPLACE DYNAMIC TABLE SNOWFLAKE_LAB.CURATED.DT_ORDERS_CLEAN
TARGET_LAG = '5 MINUTES'
WAREHOUSE = ETL_WH
AS
SELECT 
  ORDER_ID,
  CUSTOMER_ID,
  ORDER_DATE,
  ORDER_AMOUNT,
  REGION,
  STATUS,
  CASE 
    WHEN STATUS IN ('COMPLETED', 'REFUNDED') THEN 'FINAL'
    ELSE 'PENDING'
  END AS ORDER_CATEGORY
FROM SNOWFLAKE_LAB.RAW.ORDERS_LANDING
WHERE STATUS != 'CANCELLED';

-- Create dynamic table for regional metrics
CREATE OR REPLACE DYNAMIC TABLE SNOWFLAKE_LAB.MART.DT_REGIONAL_METRICS
TARGET_LAG = '15 MINUTES'
WAREHOUSE = BI_WH
AS
SELECT 
  REGION,
  DATE_TRUNC('DAY', ORDER_DATE) AS ORDER_DAY,
  COUNT(*) AS ORDER_COUNT,
  SUM(ORDER_AMOUNT) AS TOTAL_REVENUE,
  AVG(ORDER_AMOUNT) AS AVG_ORDER_VALUE
FROM SNOWFLAKE_LAB.CURATED.DT_ORDERS_CLEAN
GROUP BY REGION, DATE_TRUNC('DAY', ORDER_DATE);

-- Check dynamic table refresh history
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  TABLE_NAME => 'DT_ORDERS_CLEAN',
  SCHEMA_NAME => 'CURATED',
  DATABASE_NAME => 'SNOWFLAKE_LAB'
));

/* ============================================================
   Exercise 13: Secure Data Sharing
   ============================================================
   Create secure views and set up data sharing.

   TODO:
   1) Create secure view for partner data
   2) Validate secure view behavior
   3) Document sharing pattern
*/

-- Create secure view for partner access
CREATE OR REPLACE SECURE VIEW SNOWFLAKE_LAB.MART.PARTNER_SALES_VIEW AS
SELECT 
  DATE_TRUNC('MONTH', ORDER_DATE) AS SALES_MONTH,
  REGION,
  COUNT(*) AS ORDER_COUNT,
  SUM(ORDER_AMOUNT) AS TOTAL_SALES
FROM SNOWFLAKE_LAB.CURATED.ORDERS
WHERE REGION = 'NA'  -- Restrict to specific region
  AND STATUS = 'COMPLETED'
GROUP BY DATE_TRUNC('MONTH', ORDER_DATE), REGION;

-- Test secure view
SELECT * FROM SNOWFLAKE_LAB.MART.PARTNER_SALES_VIEW;

/* ============================================================
   Exercise 14: Task Observability
   ============================================================
   Monitor task health and failures.

   TODO:
   1) Query task history
   2) Identify failed tasks
   3) Set up task error handling
*/

-- Query task history
SELECT 
  NAME AS TASK_NAME,
  SCHEDULED_TIME,
  COMPLETED_TIME,
  STATE,
  ERROR_CODE,
  ERROR_MESSAGE,
  QUERY_ID
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE DATABASE_NAME = 'SNOWFLAKE_LAB'
ORDER BY SCHEDULED_TIME DESC
LIMIT 20;

-- Query current task status
SELECT 
  NAME,
  STATE,
  SCHEDULED_FROM,
  LAST_COMPLAINED_TIME,
  NEXT_SCHEDULED_TIME
FROM SNOWFLAKE.INFORMATION_SCHEMA.TASKS
WHERE DATABASE_NAME = 'SNOWFLAKE_LAB';

/* ============================================================
   Exercise 15: Cost Analysis and Reporting
   ============================================================
   Build comprehensive cost reports.

   TODO:
   1) Report costs by warehouse
   2) Report costs by user
   3) Report costs by database/schema
*/

-- Cost by warehouse (last 30 days)
SELECT 
  WAREHOUSE_NAME,
  DATE(START_TIME) AS USAGE_DATE,
  SUM(CREDITS_USED) AS TOTAL_CREDITS,
  SUM(CREDITS_USED_COMPUTE) AS COMPUTE_CREDITS,
  SUM(CREDITS_USED_CLOUD_SERVICES) AS CLOUD_SERVICE_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME, DATE(START_TIME)
ORDER BY USAGE_DATE DESC, TOTAL_CREDITS DESC;

-- Cost by user (last 30 days)
SELECT 
  USER_NAME,
  COUNT(*) AS QUERY_COUNT,
  SUM(CREDITS_USED_COMPUTE) AS TOTAL_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND CREDITS_USED_COMPUTE > 0
GROUP BY USER_NAME
ORDER BY TOTAL_CREDITS DESC;

-- Cost by database/schema (last 30 days)
SELECT 
  DATABASE_NAME,
  SCHEMA_NAME,
  COUNT(*) AS QUERY_COUNT,
  SUM(BYTES_SCANNED) AS TOTAL_BYTES_SCANNED,
  SUM(CREDITS_USED_COMPUTE) AS TOTAL_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND CREDITS_USED_COMPUTE > 0
GROUP BY DATABASE_NAME, SCHEMA_NAME
ORDER BY TOTAL_CREDITS DESC
LIMIT 20;

/* ============================================================
   Exercise 16: Tag-Based Governance
   ============================================================
   Implement tags for metadata-driven governance.

   TODO:
   1) Create classification and domain tags
   2) Apply tags to tables/columns
   3) Query tag references for reporting
*/

-- Create tags
CREATE OR REPLACE TAG SNOWFLAKE_LAB.GOVERNANCE.DATA_CLASSIFICATION 
  ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED';

CREATE OR REPLACE TAG SNOWFLAKE_LAB.GOVERNANCE.DOMAIN 
  ALLOWED_VALUES 'SALES', 'FINANCE', 'PRODUCT', 'OPERATIONS';

CREATE OR REPLACE TAG SNOWFLAKE_LAB.GOVERNANCE.COST_CENTER;

-- Apply tags to tables
ALTER TABLE SNOWFLAKE_LAB.CURATED.CUSTOMERS 
SET TAG SNOWFLAKE_LAB.GOVERNANCE.DATA_CLASSIFICATION = 'CONFIDENTIAL';

ALTER TABLE SNOWFLAKE_LAB.CURATED.CUSTOMERS 
SET TAG SNOWFLAKE_LAB.GOVERNANCE.DOMAIN = 'SALES';

ALTER TABLE SNOWFLAKE_LAB.CURATED.ORDERS 
SET TAG SNOWFLAKE_LAB.GOVERNANCE.DATA_CLASSIFICATION = 'INTERNAL';

ALTER TABLE SNOWFLAKE_LAB.CURATED.ORDERS 
SET TAG SNOWFLAKE_LAB.GOVERNANCE.DOMAIN = 'SALES';

ALTER TABLE SNOWFLAKE_LAB.CURATED.ORDERS 
SET TAG SNOWFLAKE_LAB.GOVERNANCE.COST_CENTER = 'CC-1001';

-- Apply tags to specific columns
ALTER TABLE SNOWFLAKE_LAB.CURATED.CUSTOMERS 
MODIFY COLUMN EMAIL 
SET TAG SNOWFLAKE_LAB.GOVERNANCE.DATA_CLASSIFICATION = 'RESTRICTED';

ALTER TABLE SNOWFLAKE_LAB.CURATED.CUSTOMERS 
MODIFY COLUMN PHONE 
SET TAG SNOWFLAKE_LAB.GOVERNANCE.DATA_CLASSIFICATION = 'RESTRICTED';

-- Query tag references
SELECT 
  TAG_NAME,
  TAG_DATABASE,
  TAG_SCHEMA,
  LEVEL,
  OBJECT_DATABASE,
  OBJECT_SCHEMA,
  OBJECT_NAME,
  COLUMN_NAME,
  TAG_VALUE
FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
WHERE TAG_DATABASE = 'SNOWFLAKE_LAB'
ORDER BY TAG_NAME, OBJECT_NAME;

/* ============================================================
   Exercise 17: Query Caching and Result Reuse
   ============================================================
   Understand and leverage result caching.

   TODO:
   1) Run the same query twice
   2) Check if results were reused
   3) Understand cache invalidation
*/

-- Run query to populate cache
SELECT COUNT(*) FROM SNOWFLAKE_LAB.CURATED.ORDERS WHERE REGION = 'NA';

-- Run query again (should use cache)
SELECT COUNT(*) FROM SNOWFLAKE_LAB.CURATED.ORDERS WHERE REGION = 'NA';

-- Check if results were reused from cache
-- Note: Query after ~24 hours will not use cache due to invalidation

/* ============================================================
   Exercise 18: Micro-partitioning and Pruning
   ============================================================
   Understand micro-partitioning and query pruning.

   TODO:
   1) Check micro-partition information
   2) Analyze query plans for pruning
   3) Optimize for better pruning
*/

-- Check micro-partition information for a query
SELECT 
  PARTITION_ID,
  START_VALUE,
  END_VALUE,
  ROW_COUNT,
  DEPTH,
  MEAN_ROW_COUNT
FROM TABLE(
  RESULT_SCAN(LAST_QUERY_ID())
);

-- Explain query to see pruning information
EXPLAIN USING TABULAR
SELECT * FROM SNOWFLAKE_LAB.CURATED.ORDERS
WHERE ORDER_DATE >= '2024-01-15' AND ORDER_DATE < '2024-01-17';

/* ============================================================
   Exercise 19: Fail-safe and Data Retention
   ============================================================
   Understand Snowflake's data retention and recovery options.

   TODO:
   1) Check data retention periods
   2) Understand fail-safe vs time travel
   3) Plan retention strategies
*/

-- Check current retention settings
SELECT 
  NAME,
  TYPE,
  RETENTION_TIME
FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES
WHERE NAME = 'SNOWFLAKE_LAB';

-- Query table retention history
SELECT 
  DATABASE_NAME,
  SCHEMA_NAME,
  TABLE_NAME,
  MIN_START_TIME,
  MAX_END_TIME,
  AVG_BYTES
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS_HISTORY
WHERE TABLE_NAME = 'ORDERS'
  AND DATABASE_NAME = 'SNOWFLAKE_LAB'
GROUP BY DATABASE_NAME, SCHEMA_NAME, TABLE_NAME;

/* ============================================================
   Exercise 20: Integration and Connectivity
   ============================================================
   Explore Snowflake integration patterns.

   TODO:
   1) Create a storage integration (conceptual)
   2) Create an API integration (conceptual)
   3) Document connection patterns
*/

-- Example: Create storage integration for external stage
-- Note: Requires valid AWS credentials and ARN

/*
CREATE OR REPLACE STORAGE INTEGRATION SNOWFLAKE_LAB_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-access'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-lab-bucket/')
  STORAGE_BLOCKED_LOCATIONS = ('s3://snowflake-lab-bucket/restricted/');
*/

-- Example: Create API integration
-- Note: Requires valid API endpoint configuration

/*
CREATE OR REPLACE API INTEGRATION SNOWFLAKE_LAB_API
  API_PROVIDER = AWS_API_GATEWAY
  API_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-api-role'
  API_ALLOWED_PREFIXES = ('https://api.example.com/');
*/

/* ============================================================
   Summary
   ============================================================
   This exercise set covers:
   
   ✓ Environment setup and warehouse configuration
   ✓ Role-based access control (RBAC)
   ✓ Dynamic data masking and row access policies
   ✓ Resource monitors and cost governance
   ✓ Query profiling and performance analysis
   ✓ Clustering and search optimization
   ✓ Materialized views and zero-copy cloning
   ✓ Time Travel for data recovery
   ✓ Streams and Tasks for CDC/incremental pipelines
   ✓ Dynamic tables for declarative transformations
   ✓ Secure data sharing patterns
   ✓ Task observability and monitoring
   ✓ Cost analysis and reporting
   ✓ Tag-based governance
   ✓ Query caching and result reuse
   ✓ Micro-partitioning and pruning
   ✓ Fail-safe and data retention
   ✓ Integration and connectivity patterns
   
   Next Steps:
   - Proceed to snowflake_advanced_exercises.sql for complex scenarios
   - Build end-to-end architecture combining these patterns
   - Apply to real production use cases
*/