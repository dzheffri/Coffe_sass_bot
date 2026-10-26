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
Delivery retains APNs priority and Telegram fallback. Telegram renders the
shop/network name separately; original database text is untouched. Claims and
acknowledgements are persisted per recipient/channel. A worker restart resumes
only untouched attempts. An external call with an unknown outcome is never
resent or followed by Telegram merely because its acknowledgement was lost.

Statuses: `queued`, `sending`, `completed`, `partial`; older objects are `legacy`.
Legacy channel counters are null: their old mixed `recipients_count` cannot prove
how many APNs/Telegram sends occurred. Delivery success never means read.

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
