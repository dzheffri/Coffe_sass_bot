BEGIN;

SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

-- Community is independent from shop broadcasts.  The author reference is
-- nullable so deleting an admin does not remove published community history.
CREATE TABLE IF NOT EXISTS community_posts (
    id BIGSERIAL PRIMARY KEY,
    type TEXT NOT NULL
        CHECK (type IN ('fact', 'tip', 'history', 'news', 'poll', 'series')),
    title TEXT NOT NULL,
    text TEXT NOT NULL,
    image_url TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    series_title TEXT,
    series_part INTEGER,
    series_total INTEGER,
    question TEXT,
    options JSONB NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(options) = 'array'),
    notification_attempted BOOLEAN NOT NULL DEFAULT FALSE,
    created_by_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    CONSTRAINT community_posts_series_fields_check CHECK (
        (type = 'series' AND series_title IS NOT NULL
            AND series_part IS NOT NULL AND series_part > 0
            AND series_total IS NOT NULL AND series_total >= series_part)
        OR (type <> 'series' AND series_title IS NULL
            AND series_part IS NULL AND series_total IS NULL)
    ),
    CONSTRAINT community_posts_poll_fields_check CHECK (
        (type = 'poll' AND question IS NOT NULL
            AND jsonb_array_length(options) BETWEEN 2 AND 10)
        OR (type <> 'poll' AND question IS NULL AND options = '[]'::jsonb)
    )
);

CREATE TABLE IF NOT EXISTS community_reactions (
    post_id BIGINT NOT NULL REFERENCES community_posts(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    interested BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (post_id, user_id)
);

CREATE TABLE IF NOT EXISTS community_poll_votes (
    post_id BIGINT NOT NULL REFERENCES community_posts(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    option_id INTEGER NOT NULL CHECK (option_id > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (post_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_community_posts_published
    ON community_posts (published_at DESC, id DESC)
    WHERE published_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_community_posts_series
    ON community_posts (series_title, series_part, id)
    WHERE published_at IS NOT NULL AND type = 'series';

CREATE UNIQUE INDEX IF NOT EXISTS uq_community_posts_series_part
    ON community_posts (LOWER(BTRIM(series_title)), series_part)
    WHERE type = 'series' AND series_title IS NOT NULL AND series_part IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_community_reactions_post
    ON community_reactions (post_id, interested);

CREATE INDEX IF NOT EXISTS idx_community_poll_votes_post
    ON community_poll_votes (post_id, option_id);

COMMIT;
