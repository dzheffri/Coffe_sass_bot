-- Approval outbox only. Existing products are remembered, never delivered again.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'manual';

CREATE TABLE IF NOT EXISTS shop_novelty_items (
    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
    item_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (shop_id, item_key)
);
INSERT INTO shop_novelty_items(shop_id,item_key)
SELECT DISTINCT n.shop_id, lower(regexp_replace(btrim(n.title), '\s+', ' ', 'g'))
FROM shop_news n JOIN coffee_shops s ON s.id=n.shop_id
WHERE btrim(COALESCE(n.title,''))<>''
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS shop_novelty_events (
    id BIGSERIAL PRIMARY KEY,
    moderation_request_id BIGINT UNIQUE REFERENCES shop_change_requests(id) ON DELETE SET NULL,
    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
    broadcast_id BIGINT NOT NULL UNIQUE REFERENCES broadcasts(id) ON DELETE CASCADE,
    push_title TEXT NOT NULL,
    push_body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS shop_novelty_deliveries (
    broadcast_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued','sending','sent','failed','unknown','skipped')),
    attempted INTEGER NOT NULL DEFAULT 0,
    sent INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (broadcast_id,user_id),
    FOREIGN KEY (broadcast_id,user_id) REFERENCES broadcast_recipients(broadcast_id,user_id)
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS shop_novelty_delivery_queue_idx
    ON shop_novelty_deliveries(updated_at,broadcast_id,user_id) WHERE status='queued';
COMMIT;
