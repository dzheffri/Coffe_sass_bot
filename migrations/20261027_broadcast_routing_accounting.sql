-- New confirmations only: no historical classification, replay or delivery.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='30s';
ALTER TABLE broadcast_delivery_jobs ADD COLUMN IF NOT EXISTS route_accounting JSONB;
ALTER TABLE broadcast_recipient_deliveries ADD COLUMN IF NOT EXISTS delivery_route TEXT
    CHECK(delivery_route IN ('APP','TELEGRAM','NONE'));
COMMIT;
