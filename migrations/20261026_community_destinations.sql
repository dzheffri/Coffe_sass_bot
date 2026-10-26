-- Existing publications keep both destinations. No content is republished.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE community_posts
    ADD COLUMN IF NOT EXISTS publish_to_app BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS publish_to_telegram BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS telegram_content_message_ids JSONB NOT NULL DEFAULT '[]'::jsonb;

COMMIT;
