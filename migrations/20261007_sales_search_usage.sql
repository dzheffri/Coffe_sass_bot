-- Google Places request ledger only; applied explicitly, never by app startup.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS sales_search_google_requests (
    id BIGSERIAL PRIMARY KEY,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS sales_search_google_requests_requested_at_idx
    ON sales_search_google_requests(requested_at);

COMMIT;
