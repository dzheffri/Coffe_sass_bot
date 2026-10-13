-- Inbox schema only. Apply separately after approval; never at app startup.
-- Existing broadcasts are retained; their original audience cannot be backfilled.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE broadcasts
    -- Future media types: photo/video. Text-only and legacy broadcasts stay NULL.
    ADD COLUMN IF NOT EXISTS media_type TEXT,
    ADD COLUMN IF NOT EXISTS media_key TEXT;

CREATE TABLE IF NOT EXISTS broadcast_recipients (
    id BIGSERIAL PRIMARY KEY,
    broadcast_id BIGINT NOT NULL REFERENCES broadcasts(id) ON DELETE CASCADE,
    -- Inbox/read state is personal data, not anonymous business audit history.
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    read_at TIMESTAMPTZ,
    delivery_status TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT broadcast_recipients_broadcast_user_unique UNIQUE (broadcast_id, user_id)
);

CREATE INDEX IF NOT EXISTS broadcast_recipients_user_created_idx
    ON broadcast_recipients(user_id, created_at DESC, id DESC);

COMMIT;
