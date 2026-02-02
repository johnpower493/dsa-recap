-- Exercise: partitioning strategy in Postgres (prompts)
--
-- Notes:
-- - Postgres supports declarative partitioning (RANGE, LIST, HASH).
-- - Most DE workloads partition large fact tables by time.

SET search_path = de_demo, public;

-- Prompt 1: Create a partitioned fact table by event_date (derived from event_ts).
-- Hint: add a generated column event_date date GENERATED ALWAYS AS (event_ts::date) STORED
-- and PARTITION BY RANGE (event_date)
--
-- Answer:
-- CREATE TABLE ... PARTITION BY RANGE (...);


-- Prompt 2: Create 3 daily partitions and an index on each.
-- Answer:


-- Prompt 3: Demonstrate partition pruning with EXPLAIN for a date range.
-- Answer:
