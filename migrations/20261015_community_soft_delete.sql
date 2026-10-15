-- Preserve Community content/media while hiding deleted posts from the app.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE community_posts
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

COMMIT;
