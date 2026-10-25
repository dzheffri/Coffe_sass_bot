-- Delivery state is independent of app publication. Existing posts are not sent.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE community_posts
    ADD COLUMN IF NOT EXISTS telegram_status TEXT
        CHECK (telegram_status IN ('pending','sending','published','failed','unknown')),
    ADD COLUMN IF NOT EXISTS telegram_message_id BIGINT,
    ADD COLUMN IF NOT EXISTS telegram_error_code TEXT,
    ADD COLUMN IF NOT EXISTS telegram_attempted_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS community_telegram_deliveries (
    post_id BIGINT NOT NULL REFERENCES community_posts(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    telegram_user_id BIGINT NOT NULL CHECK (telegram_user_id > 0),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','sending','sent','failed','unknown','skipped')),
    message_id BIGINT,
    error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempted_at TIMESTAMPTZ,
    PRIMARY KEY(post_id,user_id),
    UNIQUE(post_id,telegram_user_id)
);
CREATE INDEX IF NOT EXISTS idx_community_telegram_retry
    ON community_telegram_deliveries(post_id,status);
COMMIT;
