-- STEP 3/3: run after the DDL and concurrent index steps.
-- Each validation has its own bounded transaction. No ALTER ADD or index build
-- is held in the same transaction as these table scans.
-- On a large table, adjust statement_timeout only after reviewing workload.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '5min';
ALTER TABLE app_sessions VALIDATE CONSTRAINT app_sessions_purpose_check;
COMMIT;

BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '5min';
ALTER TABLE app_sessions VALIDATE CONSTRAINT app_sessions_context_check;
COMMIT;

BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '5min';
ALTER TABLE app_sessions VALIDATE CONSTRAINT app_sessions_selected_membership_fkey;
COMMIT;
