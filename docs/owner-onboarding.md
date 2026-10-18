# Telegram-optional owner onboarding

Local implementation only. No production migration or deployment is performed.

## Data and permissions

- `shop_admins(user_id, shop_id, role)` remains the permission source.
- Owner codes reuse `barista_admin_invites`, its keyed hashes and shared guessing limits.
- Verified native identities without memberships receive an isolated
  `barista_onboarding` session. It permits `/barista/me`, logout and invite claim,
  but does not authorize shop operations, consumer APIs or Web Admin.
- Owner codes expire after seven days; regenerating revokes the preceding code.
  Claiming consumes the code and creates the membership/session atomically.
- Telegram linking uses the existing private bot confirmation. The current Admin
  `user_id` remains the target; existing Telegram data uses the existing merge logic.
- Web pairing lasts five minutes. The QR contains only an opaque request token;
  a separate browser-only poll secret is required to consume the approval.
- Web sessions have their own token namespace, expire after twelve hours and
  recheck active ownership on every request. They cannot authorize Super Admin
  or consumer routes. Existing Web permissions admit OWNER only; native `admin`
  retains staff/barista access and cannot approve browser login.

## Prepared migration order

The existing staff-session and admin-invite migrations must already be applied.
Apply the following separately using the project's established migration process:

1. `migrations/20261017_owner_onboarding.sql`
2. `migrations/20261017_admin_web_login.sql`

Both can be reapplied. Existing identities, shops, owner memberships, Telegram
credentials and business history are preserved. The existing moderation queue
accepts an internal sender ID when the owner has no Telegram profile.
The historical staff-session migration predates the new purpose and must not be
replayed after this extension; use the ordered new migrations for this release.

Reuse the existing native provider configuration and `BARISTA_INVITE_PEPPER`.
`TELEGRAM_BOT_USERNAME` optionally overrides the existing bot's public username.
No client secrets, tokens or environment files belong in Git.
