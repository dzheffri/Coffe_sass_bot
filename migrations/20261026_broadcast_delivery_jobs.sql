-- NEW native confirmations only. Never backfill or replay existing broadcasts.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='30s';
CREATE TABLE IF NOT EXISTS broadcast_delivery_jobs (
    broadcast_id BIGINT PRIMARY KEY REFERENCES broadcasts(id) ON DELETE CASCADE,
    confirmation_hash VARCHAR(64) NOT NULL UNIQUE,
    requested_by_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    scope TEXT NOT NULL CHECK(scope IN ('SHOP','NETWORK')),
    display_name TEXT NOT NULL,
    target_users INTEGER NOT NULL CHECK(target_users>=0),
    status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','sending','completed','partial')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    fallback_media_kind TEXT,
    fallback_media_filename TEXT,
    fallback_media_bytes BYTEA,
    telegram_file_id TEXT
);
CREATE INDEX IF NOT EXISTS broadcast_delivery_jobs_pending_idx
    ON broadcast_delivery_jobs(created_at,broadcast_id) WHERE status IN ('queued','sending');
CREATE TABLE IF NOT EXISTS broadcast_recipient_deliveries (
    broadcast_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    telegram_user_id BIGINT,
    app_status TEXT NOT NULL DEFAULT 'pending'
        CHECK(app_status IN ('pending','sending','sent','failed','none','unknown')),
    apns_attempted INTEGER NOT NULL DEFAULT 0 CHECK(apns_attempted>=0),
    apns_success INTEGER NOT NULL DEFAULT 0 CHECK(apns_success>=0),
    apns_failed INTEGER NOT NULL DEFAULT 0 CHECK(apns_failed>=0),
    telegram_status TEXT NOT NULL DEFAULT 'pending'
        CHECK(telegram_status IN ('pending','sending','sent','failed','skipped','unknown')),
    telegram_message_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    telegram_attempted BOOLEAN NOT NULL DEFAULT FALSE,
    error_class TEXT,
    touched_at TIMESTAMPTZ,
    PRIMARY KEY(broadcast_id,user_id),
    FOREIGN KEY(broadcast_id,user_id) REFERENCES broadcast_recipients(broadcast_id,user_id)
        ON UPDATE CASCADE ON DELETE CASCADE
);
COMMIT;
