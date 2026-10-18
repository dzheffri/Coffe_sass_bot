# Native Admin invitations (local implementation, not deployed)

The existing verified Apple/Google identity must already map to `users.id`.
An unknown identity still returns HTTP 403 `IDENTITY_NOT_LINKED`. A known user
without current `shop_admins` access now returns HTTP 403 `NEEDS_INVITE` and
receives no staff session. Owner/admin login otherwise keeps its existing flow,
provider audiences, Keychain token format and 30-day sliding session behavior.

## API contract

- `POST /barista/owner/invites`, body `{}`, current owner Bearer session only:
  HTTP 201 `{ "ok": true, "invite": { "id": 1, "code": "482917",
  "role": "admin", "created_at": "...", "expires_at": "...",
  "used_at": null, "revoked_at": null }, "shop": { "id": 1, "name": "..." } }`.
- `GET /barista/owner/invites`: same shop plus active `invites` array containing
  the metadata above **without** plaintext code or hash. The code is returned
  only once at creation. Codes cannot be recovered later from the database.
- `DELETE /barista/owner/invites/{id}`: HTTP 200 `{ "ok": true, "revoked": true }`.
  A missing, used or foreign-shop invitation returns 404 `INVITE_NOT_FOUND`.
- `POST /barista/auth/accept-invite`: body
  `{ "provider": "google", "id_token": "<verified-provider-token>",
  "invite_code": "482917" }`. Apple uses `provider: "apple"`.
  Success returns the existing login response: `access_token`, `token_type`,
  employee, shops, selected_context and session. The newly selected role is
  always `admin`. Unexpected body fields, including `user_id`, `shop_id`,
  `role` or `admin_user_id`, return 422.

Wrong, expired, revoked, used invitations or a creator who is no longer owner
share HTTP 400 `INVALID_INVITE`, avoiding invitation-state disclosure. Existing
membership in the invitation's shop returns 409 `MEMBERSHIP_ALREADY_EXISTS` and
leaves the invite unused. Provider verification and the existing login feature
flag are checked again for every acceptance. Disabled login returns 503
`BARISTA_LOGIN_DISABLED` before invitation or membership work.

## Storage and security

`migrations/20261004_admin_invites.sql` is a separate, **unapplied** proposal.
The existing scoped read-only production schema export was inspected first: `users`,
`coffee_shops` and `shop_admins` have bigint primary keys; `shop_admins` has the
required unique `(shop_id,user_id)` constraint and admin/owner role check. The
repository contains no suitable invitation table. The original export covers
the requested table list, not the entire production catalog, so absence of
other production invitation tables is not proven. A fresh production catalog
preflight is required before approving this proposed migration. It adds only:

1. `barista_admin_invites`: shop, admin role, original creator owner membership
   and user, keyed code hash, 24-hour TTL, used/revoked metadata and user.
2. `barista_invite_attempts`: PostgreSQL counters shared by all workers.

Existing client sessions, token hashes, expiry/revocation timestamps, users,
identities and loyalty data are not rewritten. Reapplying the migration is
tested locally. New indexes are ordinary indexes on the newly created tables;
existing large tables are not indexed or scanned. Foreign-key installation
needs brief locks on their referenced tables; the transaction has a 5-second
lock timeout and 30-second statement timeout. The file is never run at startup.

The six ASCII-digit code is generated with `secrets.randbelow`. It is stored
only as HMAC-SHA256 using a dedicated **new** `BARISTA_INVITE_PEPPER` secret. At
least 32 bytes are required; use a securely generated high-entropy secret, such
as the output of `openssl rand -hex 32`, in a later separately approved env
change. Do not reuse OAuth credentials, bot token or database password. Missing
pepper or unapplied invitation tables fail closed with 503
`INVITES_NOT_CONFIGURED`. No env was changed during implementation.

The guard allows at most five attempts per verified existing user and 100
globally during each rolling-from-first-attempt **24-hour window**, aligned with
the code TTL. Additional 15-minute burst guards use the same maxima. Waiting
15 minutes cannot bypass the daily budgets. Counters and lock order are shared by
all workers; changing processes cannot reset them. Invalid-code attempts commit
their counters without creating membership/session. Blocked per-user requests
do not spend the global budget. HTTP 429 `INVITE_RATE_LIMITED` includes
`Retry-After`. The global cap deliberately favors safety over availability and
can temporarily limit legitimate joining during abuse (up to the remaining
daily window); existing staff work is
unaffected. Creation allows at most ten active invitations per selected shop
(409 `INVITE_LIMIT_REACHED`). Expired-code cleanup affects only that shop.

Identity linkage, current creator ownership, the invite and final membership
are checked under PostgreSQL locks. Membership creation, one-use consumption,
opaque session creation and response context all use one transaction. Session
failure rolls everything back; two users cannot consume one invitation.
Creation/list/revoke use the current selected membership from Bearer auth,
never actor/shop/role data from the app. A creator's removed membership cannot
be revived by creating a later replacement membership.

Rotating the pepper invalidates remaining invitation codes and changes the
counter namespace. Plan an explicit revocation/rotation process if required;
do not silently rotate during routine deploys.

## Later rollout, after separate approval

1. Recheck the current production schema and create a backup under the agreed
   read-only process; the export used here predates later live activity.
2. Apply this new invitation migration and the separately proposed broadcast
   migration only after approval. Check new tables/indexes/constraints locally
   and in production without modifying existing client rows.
3. Add `BARISTA_INVITE_PEPPER` as the separately approved new secret. Preserve
   current Apple/Google audiences and `BARISTA_LOGIN_ENABLED` unchanged.
4. Deploy approved backend/bot code, then verify existing client/staff sessions
   and safe unauthenticated owner access. Do not create production invites or
   memberships automatically as a smoke test.
5. Release the approved iOS build after its local checks and user testing.

Rollback can leave these additive tables in place. Reverting to the previous
secure backend disables invite endpoints while existing memberships/sessions
remain legitimate. Do not drop tables, revoke sessions, remove memberships or
roll back to the historically vulnerable backend without a separate plan.
