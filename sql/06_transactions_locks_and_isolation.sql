-- Worked examples: transactions, locks, and isolation (Postgres)
-- These are best explored in two psql sessions.

SET search_path = de_demo, public;

-- 1) Basic transaction (all-or-nothing)
BEGIN;
  INSERT INTO dim_customers (email, full_name) VALUES ('tmp_user@example.com', 'Temp User');
  -- Oops: violate UNIQUE(email) on purpose to see rollback behavior.
  -- INSERT INTO dim_customers (email, full_name) VALUES ('tmp_user@example.com', 'Temp User 2');
COMMIT;

-- If you uncomment the duplicate insert, COMMIT will fail and the transaction will abort.

-- 2) Explicit locking demo (run in session A, then query in session B)
-- Session A:
--   BEGIN;
--   SELECT * FROM dim_customers WHERE email='ava@example.com' FOR UPDATE;
--   -- keep the transaction open
-- Session B (will block if it tries to UPDATE the locked row):
--   UPDATE dim_customers SET full_name='Ava P' WHERE email='ava@example.com';

-- 3) Isolation levels
-- Postgres supports: READ COMMITTED (default), REPEATABLE READ, SERIALIZABLE.
-- Example:
--   BEGIN ISOLATION LEVEL REPEATABLE READ;
--   SELECT COUNT(*) FROM fact_events;
--   -- In another session insert rows
--   SELECT COUNT(*) FROM fact_events; -- same count under repeatable read
--   COMMIT;
