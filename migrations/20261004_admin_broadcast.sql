-- New native owner preview claims only; existing broadcasts/client data unchanged.
-- Apply separately before releasing owner tools. Never run at application startup.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';
CREATE TABLE IF NOT EXISTS admin_broadcast_previews (
    token_hash VARCHAR(64) PRIMARY KEY,
    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
    owner_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_id BIGINT NOT NULL REFERENCES app_sessions(id) ON DELETE CASCADE,
    text TEXT NOT NULL CHECK (char_length(text) BETWEEN 0 AND 4096),
    recipients_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    invalidated_at TIMESTAMPTZ,
    media_kind TEXT,
    media_filename TEXT,
    media_mime TEXT,
    media_bytes BYTEA,
    CHECK (expires_at > created_at),
    CHECK (used_at IS NULL OR invalidated_at IS NULL),
    CHECK (char_length(text)>0 OR media_kind IS NOT NULL),
    CHECK (media_kind IS NULL OR char_length(text)<=1024),
    CHECK ((media_kind IS NULL AND media_filename IS NULL AND media_mime IS NULL AND media_bytes IS NULL)
        OR (media_kind IS NOT NULL AND media_filename IS NOT NULL AND media_mime IS NOT NULL
            AND ((media_kind='photo' AND media_mime IN ('image/jpeg','image/png'))
                OR (media_kind='video' AND media_mime IN ('video/mp4','video/quicktime'))))),
    CHECK (media_bytes IS NULL
        OR (media_kind='photo' AND octet_length(media_bytes) BETWEEN 1 AND 8388608)
        OR (media_kind='video' AND octet_length(media_bytes) BETWEEN 1 AND 20971520))
);
CREATE INDEX IF NOT EXISTS admin_broadcast_previews_expiry_idx
    ON admin_broadcast_previews(expires_at);
CREATE INDEX IF NOT EXISTS admin_broadcast_previews_shop_created_idx
    ON admin_broadcast_previews(shop_id,created_at);
CREATE UNIQUE INDEX IF NOT EXISTS admin_broadcast_previews_pending_session_idx
    ON admin_broadcast_previews(session_id) WHERE used_at IS NULL AND invalidated_at IS NULL;
COMMIT;
