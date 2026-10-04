-- Local proposal only. Apply separately after approval; never run at app startup.
-- This adds invitation metadata and shared rate-limit counters only.
-- Existing users, memberships, sessions and loyalty rows are not rewritten.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS barista_admin_invites (
    id BIGSERIAL PRIMARY KEY,
    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
    role TEXT NOT NULL DEFAULT 'admin' CHECK (role = 'admin'),
    creator_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    creator_membership_id BIGINT REFERENCES shop_admins(id) ON DELETE SET NULL,
    code_hash VARCHAR(64) NOT NULL CHECK (code_hash ~ '^[a-f0-9]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    used_by_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    -- Closed expired invitations allow their short code to be reused later.
    closed_at TIMESTAMPTZ,
    CHECK (expires_at > created_at),
    CHECK (used_at IS NULL OR revoked_at IS NULL),
    CHECK ((used_at IS NULL AND revoked_at IS NULL) OR closed_at IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS barista_admin_invites_open_code_idx
    ON barista_admin_invites (code_hash) WHERE closed_at IS NULL;
CREATE INDEX IF NOT EXISTS barista_admin_invites_shop_idx
    ON barista_admin_invites (shop_id, expires_at) WHERE closed_at IS NULL;

-- Shared PostgreSQL counters, rather than process memory, prevent guesses
-- from bypassing limits by reaching another Railway worker.
CREATE TABLE IF NOT EXISTS barista_invite_attempts (
    scope_hash VARCHAR(64) PRIMARY KEY CHECK (scope_hash ~ '^[a-f0-9]{64}$'),
    window_started_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0)
);

COMMIT;
