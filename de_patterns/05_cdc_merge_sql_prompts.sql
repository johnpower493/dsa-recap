-- Exercise: CDC merge in SQL (Postgres)
--
-- Prereq:
--   \i sql/00_setup_schema_and_seed.sql
--
-- Scenario:
-- You receive CDC events with ops I/U/D for a dimension keyed by email.
-- Write SQL that applies CDC into the current dimension table.
--
-- Prompts:
-- 1) Create a staging table for CDC events (including op + ts).
-- 2) Load sample CDC events.
-- 3) Apply them to dim_customers using either:
--    - MERGE (Postgres 15+)
--    - or INSERT ... ON CONFLICT + DELETE
--
-- Note: In real pipelines you'll also handle ordering, de-duplication, and schema evolution.

SET search_path = de_demo, public;

-- 1) Create staging table
-- CREATE TABLE ...

-- 2) Insert CDC events
-- INSERT INTO ...

-- 3) Apply CDC
-- (Option A) MERGE ...
-- (Option B) Upsert then delete tombstones
