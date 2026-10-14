-- Per-recipient inbox soft deletion. Apply after 20261013_broadcast_recipients.sql.
-- The broadcast and every other recipient snapshot remain intact.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE broadcast_recipients
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

COMMIT;
