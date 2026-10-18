-- Additive network model. Existing shops stay independent (network_id NULL).
-- Apply manually after reviewing this migration; application startup never runs it.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS coffee_networks (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL CHECK (length(btrim(name)) BETWEEN 1 AND 180),
    logo_url TEXT,
    loyalty_mode TEXT NOT NULL DEFAULT 'PER_SHOP'
        CHECK (loyalty_mode IN ('SHARED','PER_SHOP')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp()
);

ALTER TABLE coffee_shops ADD COLUMN IF NOT EXISTS network_id BIGINT;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='coffee_shops_network_id_fkey') THEN
        ALTER TABLE coffee_shops ADD CONSTRAINT coffee_shops_network_id_fkey
            FOREIGN KEY (network_id) REFERENCES coffee_networks(id) ON DELETE SET NULL;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS coffee_shops_network_idx ON coffee_shops(network_id, id);

-- Telegram owners with several shops need a separate selected-admin context;
-- consumer active_shop_id remains untouched.
ALTER TABLE users ADD COLUMN IF NOT EXISTS active_admin_shop_id BIGINT;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='users_active_admin_shop_id_fkey') THEN
        ALTER TABLE users ADD CONSTRAINT users_active_admin_shop_id_fkey
            FOREIGN KEY (active_admin_shop_id) REFERENCES coffee_shops(id) ON DELETE SET NULL;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS network_memberships (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    network_id BIGINT NOT NULL REFERENCES coffee_networks(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role = 'NETWORK_OWNER'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    UNIQUE (user_id, network_id)
);
CREATE INDEX IF NOT EXISTS network_memberships_network_idx
    ON network_memberships(network_id, user_id);

-- Compatibility context rows let existing shop-scoped Admin sessions, tools,
-- and web endpoints continue to authorize an individual network location.
-- Membership authority remains the network_memberships row.
ALTER TABLE shop_admins ADD COLUMN IF NOT EXISTS network_membership_id BIGINT;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='shop_admins_network_membership_fkey') THEN
        ALTER TABLE shop_admins ADD CONSTRAINT shop_admins_network_membership_fkey
            FOREIGN KEY (network_membership_id) REFERENCES network_memberships(id) ON DELETE CASCADE;
    END IF;
END $$;
CREATE UNIQUE INDEX IF NOT EXISTS shop_admins_network_location_idx
    ON shop_admins(network_membership_id, shop_id) WHERE network_membership_id IS NOT NULL;

-- Network owner codes reuse the existing hashed, rate-limited one-time invite
-- contract. A NULL network_id continues to mean a shop owner/admin invite.
ALTER TABLE barista_admin_invites ADD COLUMN IF NOT EXISTS network_id BIGINT
    REFERENCES coffee_networks(id) ON DELETE CASCADE;
DROP INDEX IF EXISTS barista_owner_invites_open_shop_idx;
CREATE UNIQUE INDEX IF NOT EXISTS barista_owner_invites_open_shop_idx
    ON barista_admin_invites(shop_id)
    WHERE role='owner' AND network_id IS NULL AND closed_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS barista_owner_invites_open_network_idx
    ON barista_admin_invites(network_id)
    WHERE role='owner' AND network_id IS NOT NULL AND closed_at IS NULL;
CREATE INDEX IF NOT EXISTS barista_owner_invites_network_idx
    ON barista_admin_invites(network_id, expires_at)
    WHERE network_id IS NOT NULL AND closed_at IS NULL;

-- The network ledger is authoritative only for SHARED networks. Individual
-- shop_clients and transaction rows remain the shop-level activity record.
CREATE TABLE IF NOT EXISTS network_client_loyalty (
    id BIGSERIAL PRIMARY KEY,
    network_id BIGINT NOT NULL REFERENCES coffee_networks(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    cups INTEGER NOT NULL DEFAULT 0 CHECK (cups BETWEEN 0 AND 6),
    free_coffee_balance INTEGER NOT NULL DEFAULT 0 CHECK (free_coffee_balance >= 0),
    total_scans INTEGER NOT NULL DEFAULT 0 CHECK (total_scans >= 0),
    total_free_coffee_earned INTEGER NOT NULL DEFAULT 0 CHECK (total_free_coffee_earned >= 0),
    total_free_coffee_redeemed INTEGER NOT NULL DEFAULT 0 CHECK (total_free_coffee_redeemed >= 0),
    last_shop_id BIGINT REFERENCES coffee_shops(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    UNIQUE (network_id, user_id)
);

-- Keep the original shop_id as the stable inbox compatibility anchor for a
-- network publication; scope/network_id identify its actual audience.
ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'SHOP';
ALTER TABLE broadcasts ADD COLUMN IF NOT EXISTS network_id BIGINT
    REFERENCES coffee_networks(id) ON DELETE SET NULL;
ALTER TABLE admin_broadcast_previews ADD COLUMN IF NOT EXISTS network_id BIGINT
    REFERENCES coffee_networks(id) ON DELETE CASCADE;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='broadcasts_scope_check') THEN
        ALTER TABLE broadcasts ADD CONSTRAINT broadcasts_scope_check
            CHECK (scope IN ('SHOP','NETWORK')) NOT VALID;
    END IF;
END $$;
ALTER TABLE broadcasts VALIDATE CONSTRAINT broadcasts_scope_check;
CREATE INDEX IF NOT EXISTS broadcasts_network_created_idx
    ON broadcasts(network_id, created_at DESC) WHERE scope='NETWORK';
CREATE INDEX IF NOT EXISTS admin_broadcast_previews_network_created_idx
    ON admin_broadcast_previews(network_id, created_at) WHERE network_id IS NOT NULL;

COMMIT;
