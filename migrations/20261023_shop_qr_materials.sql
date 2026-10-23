BEGIN;

SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

-- One durable, opaque printed QR per physical location, independently of networks.
CREATE TABLE IF NOT EXISTS shop_qr_materials (
    id BIGSERIAL PRIMARY KEY,
    shop_id BIGINT NOT NULL UNIQUE REFERENCES coffee_shops(id) ON DELETE CASCADE,
    public_token TEXT NOT NULL UNIQUE CHECK (public_token ~ '^[A-Za-z0-9_-]{32}$'),
    design JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(design) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS shop_qr_events (
    id BIGSERIAL PRIMARY KEY,
    material_id BIGINT NOT NULL REFERENCES shop_qr_materials(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL CHECK (event_type IN
        ('QR_PAGE_OPEN', 'TELEGRAM_CLICK', 'APP_STORE_CLICK', 'GOOGLE_PLAY_CLICK')),
    platform TEXT NOT NULL CHECK (platform IN ('ios', 'android', 'desktop', 'other')),
    event_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_shop_qr_events_request
    ON shop_qr_events(material_id,event_id) WHERE event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_shop_qr_events_material_time
    ON shop_qr_events(material_id,created_at DESC);
CREATE INDEX IF NOT EXISTS idx_shop_qr_events_time ON shop_qr_events(created_at DESC);

COMMIT;
