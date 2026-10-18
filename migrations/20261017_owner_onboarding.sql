-- Manual additive migration. No existing users, memberships or credentials are rewritten.
-- Existing native staff and consumer session authorization remain purpose-separated.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE app_sessions DROP CONSTRAINT IF EXISTS app_sessions_purpose_check;
ALTER TABLE app_sessions ADD CONSTRAINT app_sessions_purpose_check
    CHECK (purpose IN ('client','barista','barista_onboarding')) NOT VALID;
ALTER TABLE app_sessions DROP CONSTRAINT IF EXISTS app_sessions_context_check;
ALTER TABLE app_sessions ADD CONSTRAINT app_sessions_context_check CHECK (
    (purpose IN ('client','barista_onboarding') AND selected_membership_id IS NULL)
    OR (purpose='barista' AND selected_membership_id IS NOT NULL)
) NOT VALID;
ALTER TABLE app_sessions VALIDATE CONSTRAINT app_sessions_purpose_check;
ALTER TABLE app_sessions VALIDATE CONSTRAINT app_sessions_context_check;

-- Reuse invitation hashes, closure semantics and shared attempt counters.
-- Super Admin owner invitations have no owner membership/user as creator.
ALTER TABLE barista_admin_invites DROP CONSTRAINT IF EXISTS barista_admin_invites_role_check;
ALTER TABLE barista_admin_invites ADD CONSTRAINT barista_admin_invites_role_check
    CHECK (role IN ('admin','owner')) NOT VALID;
ALTER TABLE barista_admin_invites ALTER COLUMN creator_user_id DROP NOT NULL;
ALTER TABLE barista_admin_invites DROP CONSTRAINT IF EXISTS barista_invites_admin_creator_check;
ALTER TABLE barista_admin_invites ADD CONSTRAINT barista_invites_admin_creator_check
    CHECK (role='owner' OR creator_user_id IS NOT NULL) NOT VALID;
ALTER TABLE barista_admin_invites VALIDATE CONSTRAINT barista_admin_invites_role_check;
ALTER TABLE barista_admin_invites VALIDATE CONSTRAINT barista_invites_admin_creator_check;
CREATE UNIQUE INDEX IF NOT EXISTS barista_owner_invites_open_shop_idx
    ON barista_admin_invites(shop_id) WHERE role='owner' AND closed_at IS NULL;

COMMIT;
