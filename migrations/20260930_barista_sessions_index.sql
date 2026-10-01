-- STEP 2/3: run with psql -X -v ON_ERROR_STOP=1 in autocommit mode.
-- DO NOT use BEGIN, psql --single-transaction, or a transactional migrator.
-- Membership deletion cascade needs this small partial index.
SET lock_timeout = '3s';
SET statement_timeout = '15min';

DO $$
DECLARE
    index_oid OID := to_regclass('idx_app_sessions_selected_membership_id');
BEGIN
    IF index_oid IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM pg_index i
        WHERE i.indexrelid = index_oid
          AND i.indrelid = 'app_sessions'::regclass
          AND i.indisvalid AND i.indisready AND NOT i.indisunique
          AND i.indnatts = 1 AND i.indexprs IS NULL
          AND i.indkey[0] = (SELECT attnum FROM pg_attribute
                            WHERE attrelid = 'app_sessions'::regclass
                              AND attname = 'selected_membership_id')
          AND pg_get_expr(i.indpred, i.indrelid) = '(selected_membership_id IS NOT NULL)'
    ) THEN
        RAISE EXCEPTION 'Existing membership index is invalid or incompatible; inspect it before an approved DROP INDEX CONCURRENTLY and retry';
    END IF;
END
$$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_app_sessions_selected_membership_id
    ON app_sessions(selected_membership_id)
    WHERE selected_membership_id IS NOT NULL;

-- A cancelled concurrent build can leave an invalid index. Never treat its
-- existence as success: this also protects against a competing DDL invocation.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_index
        WHERE indexrelid = 'idx_app_sessions_selected_membership_id'::regclass
          AND indrelid = 'app_sessions'::regclass AND indisvalid AND indisready
    ) THEN
        RAISE EXCEPTION 'Membership index build is not valid; stop rollout and inspect';
    END IF;
END
$$;

RESET lock_timeout;
RESET statement_timeout;
