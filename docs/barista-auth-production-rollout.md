# NashiBarista auth stage 1: manual production rollout

This is an execution plan, not approval to connect to production, migrate, push
or deploy. Every production action below requires the user's separate approval.
No production connection, migration or deployment is included in this change.

## Preconditions

- Obtain an approved schema-only export for `users`, `app_sessions`,
  `shop_admins`, `coffee_shops`, their indexes/constraints and PostgreSQL version.
  Local tests reconstruct the schema from backend SQL; they do not prove the
  deployed schema has no drift. Never apply this SQL to an unknown schema.
- PostgreSQL must be version 11 or later: the constant `DEFAULT 'client'`
  addition then uses metadata instead of rewriting old rows.
- Confirm `shop_admins.id` is BIGINT and primary/unique, `user_id`/`shop_id` are
  BIGINT FK columns, and the role model is `admin`/`owner`. The backend definition
  includes `UNIQUE(shop_id,user_id)`.
- Inspect long-running transactions, session table size and write load. Have a
  database backup/restore procedure and a bounded maintenance fallback ready.
  Run migration from one operator connection, never competing migrators.
- Stage 1 has not been released, so production has no barista sessions yet.
  If a pre-release environment has plain-hashed barista tokens from the earlier
  stage 1 implementation, revoke those sessions explicitly before release.
  Migration never rewrites token hashes or silently revokes sessions.
- Keep `BARISTA_LOGIN_ENABLED=false` on all workers until the rollout is complete.
- Release the web-admin header adapter before the protected backend routes.
  The owner ID in the URL/localStorage is not authentication. The adapter uses
  signed Telegram MiniApp `initData`, or the existing verified `admin_token`
  Bearer session for browser login, and restores MiniApp SDK context on reload.
  The web release is separate from this API/bot repository; verify its active
  revision and GET/POST/DELETE authentication before releasing the backend.
  The old backend ignores these extra headers, so the web adapter can ship first.

## Migration: three independently committed steps

Use `psql -X -v ON_ERROR_STOP=1` with an approved connection. Do not use
`--single-transaction` or wrap the three files in one transaction. Each file is
run separately and must finish successfully before the next file starts.

1. Run `migrations/20260930_barista_sessions.sql`.
   A single short transaction adds `purpose TEXT NOT NULL DEFAULT 'client'` and
   nullable `selected_membership_id`. CHECK/FK constraints are added `NOT VALID`
   and enforce new writes immediately. `lock_timeout=3s` bounds each lock wait;
   `statement_timeout=30s` bounds each statement. All existing session hashes,
   expiry, revocation, creation and last-used fields remain untouched. Old rows
   become `client` with NULL membership through the constant default.
2. Run `migrations/20260930_barista_sessions_index.sql` in autocommit mode.
   `CREATE INDEX CONCURRENTLY` permits ordinary reads/writes while scanning.
   The partial membership index supports FK cascade when staff are removed.
   The file checks any existing same-name index for validity and definition;
   it never treats a failed build's invalid index as success. Lock wait is
   bounded to 3s and statement execution to 15 minutes.
3. Run `migrations/20260930_barista_sessions_validate.sql`.
   Each CHECK/FK validation has a separate transaction, `lock_timeout=3s` and
   `statement_timeout=5min`. Table scans happen after the exclusive DDL
   transaction has committed; ordinary reads/writes remain possible. Validation
   can still compete with other DDL and create I/O load. On a large table,
   adjust the timeout only after reviewing workload and obtaining approval.

The first ALTER still needs `ACCESS EXCLUSIVE` on `app_sessions`; it cannot be
made lock-free. `NOT VALID` avoids scanning all rows while holding that exclusive
lock. FK creation also briefly locks `shop_admins`. A busy database or queued
long transaction may cause a timeout; stop, inspect and retry later. Do not
remove the timeout just to force the migration through.

The steps are repeatable once their expected objects exist. The DDL accepts
both previously validated and NOT VALID versions of its constraints. It stops
on incompatible column/constraint definitions instead of silently accepting
drift. Validation is repeatable, and an already-valid matching index is reused.

An interrupted concurrent index build may leave a same-name invalid index.
Inspect `pg_index.indisvalid/indisready`, `pg_get_indexdef` and the table/index
dependency first. After separate approval, outside a transaction, drop only
that invalid index with `DROP INDEX CONCURRENTLY
idx_app_sessions_selected_membership_id`, then rerun step 2. A valid but
different index needs manual schema review; do not blindly drop it.

## Verification before backend deployment

- Verify all three constraints have `convalidated=true` and the membership
  index is valid/ready and belongs to the expected `app_sessions` table.
- Check `purpose` default and not-null status, NULL membership for old client
  rows, and no unintended session field updates. Compare an approved sample of
  active, expired and revoked rows with pre-migration values.
- Keep the old backend running and barista issuance disabled. Verify the
  current client app's existing token against `/me`, `/me/qr`, `/me/shops` and
  `/me/stats`, ordinary client login and logout. Old INSERTs omit the new columns
  and continue using the client default. Revoked/expired sessions stay denied.
- If any step fails, stop the rollout; do not proceed to deployment or login.
  Additive columns and validated constraints may stay in place while reviewed.

## Backend rollout and release switch

1. Confirm the web header adapter has shipped; apply all three migration steps
   and finish the old-client checks above.
2. Deploy the corrected backend with `BARISTA_LOGIN_ENABLED=false`.
3. Wait for every API/bot worker to run the corrected revision. Check full import
   startup, health and old client/login/logout/link flows. No old worker should
   remain able to expose the previous insecure membership/identity routes.
   The API and bot security changes must ship together: the old bot submits an
   unauthenticated HTTP confirmation, which the new API correctly rejects.
   The new bot binds/confirms through the shared server service instead. A
   pending link button created before this release has no actor binding; restart
   that link through `/start link_...` or begin a fresh link from the app.
4. Verify protection of identity linking, Telegram link confirmation, owner
   admin mutation and unlink, plus both session isolation directions. The
   release switch should still return 503 for new barista identity login.
5. Configure the registered Apple/Google audiences and enable
   `BARISTA_LOGIN_ENABLED=true` only after complete worker replacement and
   separate approval. Verify one controlled staff identity, current membership,
   context switching, logout and access removal before normal use.

The revised barista token hash is domain-separated from client hashing:
`SHA256(b"\xffnashi.barista.session.v1\0" + token.encode("utf-8"))`.
The leading binary `0xff` marker cannot be encoded by the legacy UTF-8 client
token encoder, including a token transformed by prepending the domain label.
Client tokens keep the original `SHA256(token.encode("utf-8"))`. The old backend
therefore cannot match new barista token rows through its `/me/*` lookup even
during accidental worker mixing or rollback. Runtime purpose checks provide
the new backend's additional explicit isolation. This guarantee covers tokens
issued by this corrected version, not pre-release plain-hashed barista tokens.

## Rollback

- Disable `BARISTA_LOGIN_ENABLED` first on every new-code worker to stop issuance.
  Disabling issuance alone does not revoke existing sessions.
- With separate DB approval, revoke only barista rows before reverting workers:

  ```sql
  BEGIN;
  SET LOCAL lock_timeout = '3s';
  SET LOCAL statement_timeout = '30s';
  UPDATE app_sessions
  SET revoked_at = NOW()
  WHERE purpose = 'barista' AND revoked_at IS NULL;
  COMMIT;
  ```

  Keep every client row and hash/expiry untouched. Confirm no unrevoked barista
  rows remain. Keep issuance disabled until a subsequent corrected release.
- Prefer reverting to a security-fixed compatibility build, not the previously
  audited vulnerable backend. Reverting to an old vulnerable build reopens its
  identity/membership authorization problems even though domain-separated new
  tokens are still rejected by the old client hash lookup.
- Keep the additive schema, constraints and index. Old client code is compatible
  with them; dropping columns or restoring the session table is unnecessary and
  risks client logout/data loss. Do not roll back the schema merely to roll back
  application code.
- Verify current client sessions and logout after worker replacement. Returning
  barista staff must perform Apple/Google login after a future approved release.

## Environment variables

| Variable | Required value |
| --- | --- |
| `BARISTA_LOGIN_ENABLED` | `false` throughout migration/worker rollout; `true` only for approved barista issuance. Missing means disabled. |
| `BARISTA_APPLE_CLIENT_ID` | Exact signed ID-token audience for the registered native NashiBarista App ID, currently `com.nashi.barista`. No Team ID prefix. A web flow would require its own Services ID. |
| `BARISTA_GOOGLE_CLIENT_ID` | Exact OAuth Web/server client ID ending in `.apps.googleusercontent.com`, matching `aud` and the future iOS `GIDServerClientID`. It is not the separate iOS OAuth client ID. Actual registered value must come from the project's Google configuration. |

The client application's `APPLE_CLIENT_ID` and `GOOGLE_CLIENT_ID` stay unchanged.
These IDs are public configuration, not provider secrets. No Google client
secret or Apple private key is needed by the existing ID-token verifiers.
