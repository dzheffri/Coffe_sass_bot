-- Apply separately; no startup DDL and no legacy data changes.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS admin_web_login_requests (
    id BIGSERIAL PRIMARY KEY,
    token_hash VARCHAR(64) NOT NULL UNIQUE,
    poll_hash VARCHAR(64) NOT NULL,
    code_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL,
    approved_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    approved_membership_id BIGINT REFERENCES shop_admins(id) ON DELETE SET NULL,
    approved_at TIMESTAMPTZ,
    consumed_at TIMESTAMPTZ,
    CHECK (expires_at > created_at)
);
CREATE UNIQUE INDEX IF NOT EXISTS admin_web_login_open_code_idx
    ON admin_web_login_requests(code_hash) WHERE consumed_at IS NULL;
CREATE TABLE IF NOT EXISTS admin_web_login_attempts (
    scope_hash VARCHAR(64) PRIMARY KEY,
    window_started_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0)
);
CREATE TABLE IF NOT EXISTS admin_web_sessions (
    id BIGSERIAL PRIMARY KEY,
    token_hash VARCHAR(64) NOT NULL UNIQUE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    selected_membership_id BIGINT NOT NULL REFERENCES shop_admins(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);

-- The existing moderation sender may be absent for Apple/Google owners.
-- Preserve legacy Telegram sender values, add internal identity for new requests.
DO $$ BEGIN
    IF to_regclass('shop_change_requests') IS NOT NULL THEN
        ALTER TABLE shop_change_requests ALTER COLUMN submitted_by_telegram_id DROP NOT NULL;
        ALTER TABLE shop_change_requests ADD COLUMN IF NOT EXISTS submitted_by_user_id
            BIGINT REFERENCES users(id) ON DELETE SET NULL;
    END IF;
END $$;
COMMIT;
