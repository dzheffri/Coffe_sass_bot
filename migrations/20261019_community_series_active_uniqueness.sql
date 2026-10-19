-- Deleted series parts retain their history without reserving the active slot.
-- Keep the index name so replaying the original CREATE IF NOT EXISTS is safe.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

DROP INDEX IF EXISTS uq_community_posts_series_part;
CREATE UNIQUE INDEX uq_community_posts_series_part
    ON community_posts (LOWER(BTRIM(series_title)), series_part)
    WHERE type = 'series' AND series_title IS NOT NULL AND series_part IS NOT NULL
        AND deleted_at IS NULL;

COMMIT;
