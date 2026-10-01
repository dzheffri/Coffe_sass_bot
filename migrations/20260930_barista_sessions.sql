-- STEP 1/3: MANUAL MIGRATION ONLY. Production execution needs approval.
-- PostgreSQL 11+ is required for a metadata-only constant DEFAULT addition.
-- Run with psql -X -v ON_ERROR_STOP=1; do not wrap all three files in one transaction.
-- Existing token_hash/expires_at/revoked_at/last_used_at values are never updated.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
    IF current_setting('server_version_num')::integer < 110000 THEN
        RAISE EXCEPTION 'This online migration requires PostgreSQL 11 or later';
    END IF;
END
$$;

ALTER TABLE app_sessions
    ADD COLUMN IF NOT EXISTS purpose TEXT NOT NULL DEFAULT 'client',
    ADD COLUMN IF NOT EXISTS selected_membership_id BIGINT NULL;

DO $$
BEGIN
    -- IF NOT EXISTS must not silently accept an incompatible previous rollout.
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute a
        JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = 'app_sessions'::regclass AND a.attname = 'purpose'
          AND a.atttypid = 'text'::regtype AND a.attnotnull AND NOT a.attisdropped
          AND pg_get_expr(d.adbin, d.adrelid) = '''client''::text'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = 'app_sessions'::regclass
          AND attname = 'selected_membership_id'
          AND atttypid = 'bigint'::regtype AND NOT attnotnull AND NOT attisdropped
    ) OR EXISTS (
        SELECT 1 FROM pg_attribute a
        JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = 'app_sessions'::regclass
          AND a.attname = 'selected_membership_id'
    ) THEN
        RAISE EXCEPTION 'app_sessions purpose/context schema differs from this migration';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'app_sessions'::regclass
          AND conname = 'app_sessions_purpose_check'
    ) THEN
        ALTER TABLE app_sessions
            ADD CONSTRAINT app_sessions_purpose_check
            CHECK (purpose IN ('client', 'barista')) NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'app_sessions'::regclass
          AND conname = 'app_sessions_selected_membership_fkey'
    ) THEN
        ALTER TABLE app_sessions
            ADD CONSTRAINT app_sessions_selected_membership_fkey
            FOREIGN KEY (selected_membership_id)
            REFERENCES shop_admins(id) ON DELETE CASCADE NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'app_sessions'::regclass
          AND conname = 'app_sessions_context_check'
    ) THEN
        ALTER TABLE app_sessions
            ADD CONSTRAINT app_sessions_context_check CHECK (
                (purpose = 'client' AND selected_membership_id IS NULL)
                OR
                (purpose = 'barista' AND selected_membership_id IS NOT NULL)
            ) NOT VALID;
    END IF;

    -- Both a prior validated migration and this NOT VALID definition are valid
    -- repeat-run states. Any differently defined named constraint is an error.
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'app_sessions'::regclass
          AND conname = 'app_sessions_purpose_check'
          AND (
              contype <> 'c'
              OR replace(pg_get_constraintdef(oid), ' NOT VALID', '') <>
                 'CHECK ((purpose = ANY (ARRAY[''client''::text, ''barista''::text])))'
          )
    ) OR EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'app_sessions'::regclass
          AND conname = 'app_sessions_selected_membership_fkey'
          AND (
              contype <> 'f' OR confrelid <> 'shop_admins'::regclass
              OR confdeltype <> 'c' OR confupdtype <> 'a' OR condeferrable
              OR conkey <> ARRAY[(SELECT attnum FROM pg_attribute
                                 WHERE attrelid = 'app_sessions'::regclass
                                   AND attname = 'selected_membership_id')]
              OR confkey <> ARRAY[(SELECT attnum FROM pg_attribute
                                  WHERE attrelid = 'shop_admins'::regclass
                                    AND attname = 'id')]
          )
    ) OR EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'app_sessions'::regclass
          AND conname = 'app_sessions_context_check'
          AND (
              contype <> 'c'
              OR replace(pg_get_constraintdef(oid), ' NOT VALID', '') <>
                 'CHECK ((((purpose = ''client''::text) AND (selected_membership_id IS NULL)) OR ((purpose = ''barista''::text) AND (selected_membership_id IS NOT NULL))))'
          )
    ) THEN
        RAISE EXCEPTION 'app_sessions has incompatible named session constraints';
    END IF;
END
$$;

COMMIT;

-- NOT VALID still checks new/changed rows. Existing rows need no rewrite:
-- their purpose is the metadata DEFAULT 'client', and membership is NULL.
-- Next run 20260930_barista_sessions_index.sql, then the validation file.
