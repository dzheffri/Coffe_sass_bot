# Native broadcast delivery and approved novelties

Apply `20261026_shop_novelty_notifications.sql` and
`20261026_broadcast_delivery_jobs.sql` before deploying the backend. Both are
additive/idempotent. Neither queues any existing broadcast. Existing shop-news
titles are remembered without sending notifications.

## Native manual broadcasts

Confirmation publishes the existing inbox object and complete selected-shop
audience in the same transaction as a durable delivery job. The confirmation
token identifies one send action, including retries from a refreshed session.
The POST returns HTTP 202 with `broadcast_id`, status and channel counters.

Shop routes (network equivalents live under `/barista/owner/networks/{id}`):

- `POST /barista/owner/broadcast` — confirm/enqueue, or return that action's result.
- `POST /barista/owner/broadcast/status` — read-only reconciliation by the same token.
- `GET /barista/owner/broadcasts/{broadcast_id}` — current result.
- `GET /barista/owner/broadcasts?limit=20&offset=0` — bounded history.

Each route rechecks current authenticated owner access and tenant scope.
New confirmations persist an APP, TELEGRAM or NONE active notification route.
APP requires an enabled, nonempty registered iOS push token, matching the
existing APNs sender. Identity/session alone is never an active channel.
Without an enabled device, a Telegram address selects TELEGRAM; without either
channel, NONE. Apple/Google identity, any consumer session or a registered
device still counts the independently committed app inbox at publication.
Thus an app user without push can retain inbox and receive Telegram fallback.
Before an untouched Telegram attempt, a newly enabled push device promotes
the route to APP. At least one APNs success blocks Telegram; temporary/unknown
push failures also block fallback to avoid duplicates. No device or definitive
permanent token rejection permits Telegram fallback, or NONE without Telegram.
Tokens have no invented TTL; APNs determines permanent invalidity.
Telegram renders the
shop/network name separately; original database text is untouched. Claims and
acknowledgements are persisted per recipient/channel. A worker restart resumes
only untouched attempts. An external call with an unknown outcome is never
resent or followed by Telegram merely because its acknowledgement was lost.

Statuses: `queued`, `sending`, `completed`, `partial`; older objects are `legacy`.
For old App Store clients, the confirmation POST keeps HTTP 202 but maps an
unfinished `status` to `partial`, which their existing success guard accepts.
The additive `delivery_status` always contains the actual state. Read-only
status, token reconciliation and history continue returning actual `status`.
In an unfinished confirmation only, legacy `sent` is `target_users` and
`failed` is zero: those recipients were accepted atomically, not yet all
delivered externally. `accepted_recipients` and
`sent_semantics=accepted_recipients` make this explicit to newer clients.
A completed confirmation uses actual counts and `sent_semantics=delivered_users`.

Apply additive `20261027_broadcast_routing_accounting.sql` for this contract.
It does not classify historical recipients, queue work or replay a broadcast.
New job accounting persists independently of later account deletion/merging:
`target_users = app_routed_users + telegram_routed_users + no_delivery_channel`.
`app_inbox_created` counts app-connected recipients at publication, independently
of the active channel; a later device registration creates no additional inbox
row. The older
`app_notifications_created` remains the number of all physical recipient rows
created at publication. New jobs persist `routing_policy=push_device_priority`
and a separate `app_delivered` counter. In status/history their `sent` counts
confirmed active APP recipients plus successful Telegram recipients, never
inbox copies or APNs devices added a second time. Jobs created under the older
inbox-priority policy retain their original saved counts and are not rerouted.
`apns_users` counts users
with a known APNs attempt; `apns_devices_sent` and `apns_failed` count device
acknowledgements. Telegram success/failure counts users, with error-class
summaries and unknown outcomes recorded separately. Unrecoverable historical
route metrics remain null, not fabricated zeroes.
Legacy channel counters are null: their old mixed `recipients_count` cannot prove
how many APNs/Telegram sends occurred. Delivery success never means read.

Primary final accounting is separate from APNs transport outcomes:
`target_users = app_delivered + telegram_delivered + delivery_failed + no_route`.
For new push-priority jobs `app_delivered` counts recipients with at least one
confirmed APNs success, while `app_inbox_created` is separate and can overlap
Telegram or NONE. Older jobs retain their original inbox-based APP outcome.
`telegram_delivered` counts acknowledged Telegram recipient deliveries.
`no_route` aliases the older `no_delivery_channel`: no APP/Telegram route was
available, rather than a later delivery error. `delivery_failed` counts selected
routes with no confirmed primary delivery; uncertain Telegram outcomes are also
reported separately as its `delivery_unknown` subset. While a job is unfinished,
`delivery_pending` accounts for recipients still awaiting their outcome.
One failed device does not cancel another successful device for the same user.
If an active APP attempt remains unconfirmed, its primary outcome is failed/
unknown without a potentially duplicating Telegram retry; inbox remains intact.
These metrics derive from durable original-target accounting, surviving account
cleanup and without replay/backfill. Legacy unknown metrics remain null; an
inconsistent negative remainder sets `delivery_accounting_valid=false` instead
of inventing a successful partition.

## Approved new products

Only Super Admin approval emits automatic notifications. Submission is silent.
Profile changes, approval, durable product identities, inbox recipients and the
APNs outbox commit atomically. One approval containing several new products
creates one inbox message/push per recipient. The audience is the selected
shop's canonical `shop_clients`, including app-only and older clients.

The current editor replaces product rows and omits stable item IDs. Therefore
the durable identity is `(shop_id, normalized title)`; price/photo changes and
reapproval cannot republish that item. A different title represents a new item.
Existing automatic-push preferences are respected; inbox publication is kept.
No Telegram delivery or Android/FCM change is part of this feature.

Automatic novelty inbox objects use `origin=shop_novelty`, so they do not consume
manual weekly limits or appear as manual mailing history. They use existing
authenticated inbox APIs. APNs receives existing `type=shop_news`, `shop_id` and
`broadcast_id` keys. Unknown attempts are retained without automatic resends.

## Production checks

Never requeue a historical real broadcast. Use an explicitly named TEST shop
and isolated TEST user (or the operator's own authorized device). No migration
or startup hook sends a global announcement or mass mailing.
