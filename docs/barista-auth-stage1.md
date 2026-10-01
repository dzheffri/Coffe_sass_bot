# NashiBarista: auth/session stage 1

This checkout implements barista identity login, session validation, shop
context selection and logout, together with legacy authentication hardening
and verified private-bot Telegram link confirmation. No scan, loyalty operation,
statistics, Wallet or push changes are included.

## Database migration — manual approval required

Apply the three migration files, in the order documented in
`docs/barista-auth-production-rollout.md`, before deploying this code:

1. `migrations/20260930_barista_sessions.sql` — short DDL transaction.
2. `migrations/20260930_barista_sessions_index.sql` — concurrent index, no transaction wrapper.
3. `migrations/20260930_barista_sessions_validate.sql` — separate bounded validations.

The application does **not** automatically upgrade an existing `app_sessions`
table. The existing initialization code includes the new definition only for a
fresh table.

The migration adds:

- `purpose TEXT NOT NULL DEFAULT 'client'`: all existing rows remain client
  sessions; their token hashes, expiry, last-used and revocation values are preserved.
- `selected_membership_id BIGINT`: a barista session must reference a concrete
  `shop_admins.id`; a client session must have no membership context.
- Purpose/context checks and a membership foreign key with `ON DELETE CASCADE`,
  first added `NOT VALID`; a concurrent partial index supporting that cascade;
  and validation of existing rows after the short exclusive DDL lock is released.

Repeat runs accept the expected schema/constraints and an already-valid index.
They stop on incompatible columns/constraints or an invalid existing index.
No migration performs a bulk session UPDATE. PostgreSQL 11+ is required for the
metadata-only constant default; exact production schema/version still requires
an approved schema-only export before execution.

Deleting a selected membership deletes its associated barista sessions, even
when deletion comes from the existing Telegram or owner tools. Re-adding the
employee creates a new membership and cannot reactivate an old token. Ordinary
client sessions are unaffected. Existing `revoke_all_user_sessions(user_id)`
continues to revoke both purposes for account-wide security reset; its optional
purpose filter can restrict this behavior when needed.

## Identity configuration

Configure `BARISTA_GOOGLE_CLIENT_ID` and `BARISTA_APPLE_CLIENT_ID` with the
audiences issued for NashiBarista. No fallback to the client application's
audience is allowed for barista login. A missing configuration returns 503.
Existing `GOOGLE_CLIENT_ID` / `APPLE_CLIENT_ID` behavior remains unchanged for
client login.

`BARISTA_LOGIN_ENABLED` is a release switch, defaulting to disabled. Leave it
disabled through migration and the entire worker deployment; enable it only
after every worker has the corrected code. Existing barista sessions are still
validated when issuance is disabled, so disabling the switch alone is not logout.

Only an already-linked Apple/Google identity may log in. A verified provider
subject maps to an existing `users.id`, which must have current `admin` or
`owner` membership. This endpoint neither creates users nor links accounts by
email, Telegram ID or unverified app-provided identity fields. An identity not
yet linked to the old profile returns `IDENTITY_NOT_LINKED`; account linking is
outside this stage.

## API

### POST /barista/auth/identity

```json
{"provider":"google","id_token":"<provider-issued-ID-token>","shop_id":12}
```

`provider` is `google` or `apple`; `shop_id` is optional when exactly one
membership is available. With multiple shops and no choice, the endpoint returns
409 `SHOP_CONTEXT_REQUIRED` and the currently accessible shops, without issuing
a session. The app resubmits the same still-valid identity token with its chosen
shop. The server validates that choice. No arbitrary first shop is selected.

Success contains one `access_token` (an opaque credential, not a JWT),
`token_type: "bearer"`, and the same employee/context/session fields as `/me`.
There is no refresh token or refresh endpoint. The iOS app can later store this
credential in Keychain; no iOS integration is included here.

### GET /barista/me

Use `Authorization: Bearer <barista-token>`. Example response:

```json
{
  "ok": true,
  "employee": {"user_id":123,"name":"Ольга"},
  "shops": [{"membership_id":45,"shop_id":12,"name":"Наші","role":"admin"}],
  "selected_context": {"membership_id":45,"shop_id":12,"name":"Наші","role":"admin"},
  "session": {"purpose":"barista","expires_at":"2026-10-30T10:00:00Z","idle_timeout_days":30}
}
```

Every protected endpoint rechecks the live session, its purpose, expiry and
revocation, and current membership ownership/role. Staff memberships are locked
before the session row to serialize with membership deletion and context/logout
changes. Successful authorization extends expiry to the server time plus 30
days. Expired/revoked sessions cannot be restored by touching them; denied
context changes do not extend them. There is no absolute 180-day limit.

### POST /barista/context

```json
{"shop_id":18}
```

Requires a valid barista Bearer token. The server checks current membership in
that shop, stores its membership ID, and returns updated `/me` data. Staff user
ID, membership ID and role cannot be supplied as trusted request fields.

### POST /barista/auth/logout

Requires a valid barista Bearer token. Revokes that session and returns
`{"ok":true,"revoked":true}`. It does not revoke the employee's client token.

Invalid, expired, revoked or wrong-purpose tokens return 401. Missing staff
access or an inaccessible requested shop returns 403. Client `/me/*` and
`/auth/logout` reject barista tokens; `/barista/*` rejects client tokens.
Existing client response contracts remain unchanged.

For rollback safety, barista session hashes use a separate domain:
`SHA256(b"\xffnashi.barista.session.v1\0" + token.encode("utf-8"))`.
The leading binary `0xff` byte cannot be produced by the old UTF-8 token
encoder, even by prepending the domain label to a token. Client hashes remain
the existing `SHA256(token.encode("utf-8"))`. An unmodified older backend cannot
find a newly issued barista token through its client hash lookup. This does not
change opaque token shape or add a second credential.
Before release, any experimentally issued session using the earlier plain
barista hash must be explicitly revoked; none has been issued in production.

## Local verification

Tests use a separate local PostgreSQL database named `barista_auth_tests` and
refuse a production connection configuration. Focused tests exercise the
session/router/SQL definitions. A separate full-startup audit imports actual
`run.py`, `app.db` and `app.api.main`, runs startup DDL and ASGI requests, and
checks the old backend against the migrated isolated schema. Its harness
redirects filesystem writes, disables production environment loading and blocks
external network connections. No production service is used.

The full-startup audit pins the legacy backend to commit
`50cbfc5be3af00260fe1ca888c5e805667c79b66`. Test clones must include that commit
in their local Git history; a shallow clone without it cannot run this audit.

```sh
TEST_DATABASE_URL='dbname=barista_auth_tests user=barista_test host=<local-test-socket-directory> port=55439' .venv/bin/python -m pytest -q tests
.venv/bin/python -m compileall -q app tests
git diff --check
```

Migration is exercised only on this disposable local database, including all
three steps, existing client session preservation and repeated application.
Concurrent index SQL must be executed in autocommit per statement, rather than
as one multi-statement psycopg query. No production migration or deployment is
part of this stage. See the root audit result for actual commands and results.
