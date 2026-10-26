# Community publication to Telegram

Community remains independent of coffee-shop broadcasts. Publishing a draft
commits the app post first, then mirrors it to `@nashi_coffee_community` using the
existing `BOT_TOKEN`. The channel send uses `disable_notification=True`.

The existing **Надіслати сповіщення** option controls personal notifications:

- App devices receive the existing Community push with `community_post_id`.
- Only Telegram-only accounts enter the personal Telegram delivery snapshot.
- Apple/Google identities, consumer app sessions, or any registered app device
  (including disabled notifications) exclude an internal user from Telegram DM.
- App evidence is checked again before a queued personal send.
- Telegram is optional for app users; a linked Telegram identity never enables
  an additional personal notification for an app account.

The channel is mirrored regardless of that checkbox. Drafts, deleted posts,
and previously published legacy posts are not automatically delivered.

## Receipts and retries

Apply `migrations/20261025_community_telegram.sql` before deploying the integration.
It adds nullable Telegram fields to `community_posts` and creates the personal
delivery ledger. It sends nothing and does not backfill old publications.

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

All six existing Community types are supported. Series retain their title and
part number. A poll is mirrored as one native anonymous, single-choice, regular
Telegram poll using its existing question and answers. Its stored Telegram
message ID and all links refer to that poll. App and Telegram votes are separate:
no Telegram vote is written to the app voting tables or API.

Telegram poll questions are limited to 300 characters and answer text to 100.
Long answers are shortened, preserving distinct labels if truncation creates a
collision. The existing app content and options remain unchanged. Additional
text and photos stay in the app; no extra channel message is created for a poll.
This keeps the same single-message delivery claim and retry protection.

Other posts retain their existing text/photo rendering. Photos are read from
the existing validated Community storage, never fetched from arbitrary URLs.

Telegram limits a message to 4096 UTF-16 units and a photo caption to 1024.
Long channel content is shortened with a note to read the full text in the app;
the app post is unchanged. Personal messages contain a short announcement and
a URL button linking to the exact channel message.

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
