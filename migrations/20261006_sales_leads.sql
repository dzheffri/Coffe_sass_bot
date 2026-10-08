-- Sales CRUD storage only. Apply on an isolated local/test database for this stage.
-- Never executed by application startup; production application is a separate step.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS sales_leads (
    id BIGSERIAL PRIMARY KEY,
    place_id TEXT,
    name TEXT NOT NULL CHECK (char_length(btrim(name)) > 0),
    city TEXT,
    address TEXT,
    phone TEXT,
    phone_normalized TEXT,
    email TEXT,
    instagram TEXT,
    instagram_normalized TEXT,
    telegram TEXT,
    website TEXT,
    website_normalized TEXT,
    google_maps_url TEXT,
    rating DOUBLE PRECISION CHECK (rating >= 0 AND rating <= 5),
    reviews_count INTEGER CHECK (reviews_count >= 0),
    status TEXT NOT NULL DEFAULT 'NEW' CHECK (status IN (
        'NEW', 'NOT_CONTACTED', 'CONTACTED', 'REPLIED', 'INTERESTED',
        'DEMO', 'THINKING', 'CONNECTED', 'REJECTED', 'DO_NOT_CONTACT'
    )),
    score DOUBLE PRECISION CHECK (score >= 0 AND score <= 100),
    notes TEXT,
    source TEXT,
    last_contact_at TIMESTAMPTZ,
    next_followup_at TIMESTAMPTZ,
    -- SET NULL retains the lead and timeline after a shop is deleted.
    -- Active linked-shop validation belongs to an API transition, not this FK.
    connected_shop_id BIGINT REFERENCES coffee_shops(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    updated_by_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    CHECK (place_id IS NULL OR char_length(btrim(place_id)) > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS sales_leads_place_id_unique_idx
    ON sales_leads(place_id) WHERE place_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS sales_leads_phone_normalized_idx
    ON sales_leads(phone_normalized) WHERE phone_normalized IS NOT NULL;
CREATE INDEX IF NOT EXISTS sales_leads_website_normalized_idx
    ON sales_leads(website_normalized) WHERE website_normalized IS NOT NULL;
CREATE INDEX IF NOT EXISTS sales_leads_instagram_normalized_idx
    ON sales_leads(instagram_normalized) WHERE instagram_normalized IS NOT NULL;
CREATE INDEX IF NOT EXISTS sales_leads_status_updated_idx
    ON sales_leads(status, updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS sales_leads_city_idx ON sales_leads(lower(city));
CREATE INDEX IF NOT EXISTS sales_leads_active_followup_idx
    ON sales_leads(next_followup_at, id)
    WHERE next_followup_at IS NOT NULL
      AND status NOT IN ('CONNECTED', 'REJECTED', 'DO_NOT_CONTACT');

CREATE TABLE IF NOT EXISTS sales_lead_events (
    id BIGSERIAL PRIMARY KEY,
    -- There is no lead-delete endpoint. Restrict deletion to preserve its history.
    lead_id BIGINT NOT NULL REFERENCES sales_leads(id) ON DELETE RESTRICT,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'CREATED', 'UPDATED', 'STATUS_CHANGED', 'NOTE_ADDED', 'FOLLOWUP_SET',
        'CONTACTED', 'DUPLICATE_WARNING', 'CONNECTED_TO_SHOP'
    )),
    actor_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    from_status TEXT CHECK (from_status IN (
        'NEW', 'NOT_CONTACTED', 'CONTACTED', 'REPLIED', 'INTERESTED',
        'DEMO', 'THINKING', 'CONNECTED', 'REJECTED', 'DO_NOT_CONTACT'
    )),
    to_status TEXT CHECK (to_status IN (
        'NEW', 'NOT_CONTACTED', 'CONTACTED', 'REPLIED', 'INTERESTED',
        'DEMO', 'THINKING', 'CONNECTED', 'REJECTED', 'DO_NOT_CONTACT'
    )),
    note TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS sales_lead_events_lead_created_idx
    ON sales_lead_events(lead_id, created_at DESC, id DESC);

COMMIT;
