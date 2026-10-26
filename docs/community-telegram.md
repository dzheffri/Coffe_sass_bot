# Community publication to Telegram

Community remains independent of coffee-shop broadcasts. Publishing a draft
commits the publication and its destinations first, then mirrors it to `@nashi_coffee_community` using the
existing `BOT_TOKEN`. The channel send uses `disable_notification=True`.

The existing **Надіслати сповіщення** option controls personal notifications:

- App devices receive the existing Community push with `community_post_id`.
- Only Telegram-only accounts enter the personal Telegram delivery snapshot.
- Apple/Google identities, consumer app sessions, or any registered app device
  (including disabled notifications) exclude an internal user from Telegram DM.
- App evidence is checked again before a queued personal send.
- Telegram is optional for app users; a linked Telegram identity never enables
  an additional personal notification for an app account.

The channel is mirrored regardless of that checkbox when **У Telegram** is selected.
The independent **У застосунку «Наші»** and **У Telegram** flags default to true.
App-only posts send no Telegram content or personal Telegram notification.
Telegram-only posts stay hidden from consumer feed/detail/reactions/votes and
never send app push. Both destinations disabled cannot be published.
Drafts, deleted posts,
and previously published legacy posts are not automatically delivered.

## Receipts and retries

Apply `migrations/20261025_community_telegram.sql` before deploying the integration.
It adds nullable Telegram fields to `community_posts` and creates the personal
delivery ledger. The additive `20261026_community_destinations.sql` migration
adds destination flags with true defaults and ordered content-message receipts.
Neither migration sends anything or backfills publication delivery.

Admin responses expose the channel status, message ID, canonical channel-post
URL, and safe retry eligibility.
`POST /community/admin/posts/{post_id}/telegram/retry` is Super Admin only and does not
invoke app publication or app push. A successful channel receipt is reused when
resuming remaining personal deliveries. Personal delivery runs after the
publication response, with throttling; pending rows can be resumed safely.

Claims are committed before external requests. Concurrent requests cannot
claim the same channel post or personal delivery twice. Confirmed deliveries
and ambiguous outcomes are not replayed. Personal receipts survive account
merges and are removed with account deletion.

Telegram has no idempotency key for these sends. An explicit Bot API rejection
or a failure before making the request can be retried. A timeout, connection
loss, 5xx, or crash after a send could mean Telegram accepted the message:
the integration keeps that outcome unknown/in flight and blocks blind retry.
Such an outcome requires checking the channel before any manual reconciliation.

## Content

All six Community types retain their readable category, title and full text.
The tip category is **☕️ Порада бариста**. Series retain their title and part.
Photos are read only from validated local Community storage.

A photo is the first channel message, with up to 1024 UTF-16 units of caption.
All remaining content follows in messages of up to 4096 UTF-16 units. Without a
photo, the complete content uses those text messages directly. Splitting never
truncates text or adds a placeholder directing Telegram-only readers to the app.

New polls use one native anonymous, single-choice regular `sendPoll` message:
the complete category/title/body goes into `description` and the photo, when
present, into `media=InputMediaPhoto` read from validated local storage. There
is no separate photo/text channel message. Telegram limits descriptions to
1024 characters; overflow fails before HTTP with
`TELEGRAM_POLL_DESCRIPTION_TOO_LONG_1024`, preserving full app content.
See the official [sendPoll](https://core.telegram.org/bots/api#sendpoll) and
[InputPollMedia](https://core.telegram.org/bots/api#inputpollmedia) contracts.
The canonical
`telegram_message_id` points to the native poll; for other types it points to
the first content message. App and Telegram votes remain independent.
Telegram poll questions are limited to 300 characters and answer labels to 100;
only the Telegram poll labels may be shortened/disambiguated, never app content.

Each acknowledged photo/text chunk is committed into
`telegram_content_message_ids` before attempting the next chunk or poll. A
confirmed rejection retries only the unfinished stage. A previously failed
legacy two-message poll with content receipts still retries only its final
poll stage, preserving the visible content. A new one-message poll records
its canonical/content receipt atomically. Timeout/5xx/crash keeps the outcome
unknown/in flight and blocks blind resend, including partial publications.
Existing published posts without content receipts are never recreated.
Personal messages still contain only a short announcement and an exact post URL.

## Bot menu and rollout

The regular user, Owner, Admin and Super Admin reply keyboards include a
separate persistent **☕️ Кавова спільнота** button. Existing menu actions retain
their order. Reply-keyboard buttons send text rather than opening URLs: tapping
this button returns an ordinary URL button opening
`https://t.me/nashi_coffee_community` directly. The common handler works during
pending scan, broadcast and support flows without changing their FSM state.
The bot no longer sends an extra inline menu message automatically after /start.

No new bot, token, webhook, scheduler, or automatic launch announcement is
introduced. The existing bot must retain permission to publish in the channel.
Deployment and real channel/push/DM smoke tests require a separate authorized
step; local tests use the guarded disposable PostgreSQL and fake senders only.
