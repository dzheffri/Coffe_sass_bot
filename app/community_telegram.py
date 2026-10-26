"""Community channel mirror and per-user delivery receipts.

Claims commit before the Bot API call. A transport timeout/crash is deliberately
not retried: Telegram has no idempotency key and may already have accepted it.
Only explicit Bot API rejection is safe to retry. App push is never called here.
"""

from contextlib import contextmanager, suppress
import asyncio
import logging

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramNetworkError, TelegramRetryAfter, TelegramServerError
from aiogram.types import BufferedInputFile, InlineKeyboardButton, InlineKeyboardMarkup, InputPollOption

from app.community import CommunityError
from app.community_media import CommunityMediaStorage


CHANNEL_USERNAME = "@nashi_coffee_community"
CHANNEL_URL = "https://t.me/nashi_coffee_community"
logger = logging.getLogger(__name__)

_CATEGORIES = {"fact": "Цікавий факт", "tip": "Порада", "history": "Історія кави",
               "news": "Новина", "poll": "Опитування", "series": "Серія"}


def channel_post_url(message_id):
    if isinstance(message_id, int) and not isinstance(message_id, bool) and message_id > 0:
        return f"{CHANNEL_URL}/{message_id}"
    return None


def _has_table(connection, table):
    return connection.execute("SELECT to_regclass(%s) AS name", (table,)).fetchone()["name"] is not None


def _app_evidence(connection, alias="u"):
    # Disabled/revoked devices and expired consumer sessions remain evidence of
    # app use. Permission denial must never turn an app user into a Telegram DM.
    evidence = [f"EXISTS(SELECT 1 FROM user_identities i WHERE i.user_id={alias}.id "
                "AND i.provider IN ('apple','google'))",
                f"EXISTS(SELECT 1 FROM app_sessions s WHERE s.user_id={alias}.id AND s.purpose='client')"]
    if _has_table(connection, "app_push_devices"):
        evidence.append(f"EXISTS(SELECT 1 FROM app_push_devices d WHERE d.user_id={alias}.id)")
    return " OR ".join(evidence)


def queue_telegram_only_recipients(connection, post_id):
    evidence = _app_evidence(connection)
    # Identity wins over a stale legacy column; numeric conversion is bounded
    # without ever using a provider subject or internal user_id as a chat ID.
    rows = connection.execute(f"""SELECT u.id AS user_id,
        COALESCE((SELECT CASE WHEN i.provider_user_id ~ '^[1-9][0-9]{{0,18}}$'
                         THEN i.provider_user_id::numeric END
                  FROM user_identities i WHERE i.user_id=u.id AND i.provider='telegram'),
                 u.telegram_user_id) AS telegram_user_id
        FROM users u WHERE NOT ({evidence}) ORDER BY u.id""").fetchall()
    for row in rows:
        target = row["telegram_user_id"]
        if target is None or not 0 < target <= 2**63 - 1:
            continue
        connection.execute("""INSERT INTO community_telegram_deliveries(post_id,user_id,telegram_user_id)
            VALUES(%s,%s,%s) ON CONFLICT DO NOTHING""", (post_id, row["user_id"], int(target)))


def telegram_admin_status(connection, row):
    status = row.get("telegram_status")
    failed = pending = 0
    if status is not None:
        counts = connection.execute("""SELECT COUNT(*) FILTER(WHERE status='failed') AS failed,
            COUNT(*) FILTER(WHERE status='pending') AS pending FROM community_telegram_deliveries
            WHERE post_id=%s""", (row["id"],)).fetchone()
        failed, pending = int(counts["failed"]), int(counts["pending"])
    return {
        "telegram_status": status,
        "telegram_message_id": row.get("telegram_message_id"),
        "telegram_url": channel_post_url(row.get("telegram_message_id")),
        "telegram_retry_allowed": status in {"pending", "failed"} or (status == "published" and failed + pending > 0),
        "telegram_dm_failed_count": failed,
        "telegram_dm_pending_count": pending,
    }


def channel_text(post, *, with_photo=False):
    parts = [f"☕ {_CATEGORIES[post['type']]}", post["title"]]
    if post["type"] == "series":
        parts.append(f"{post['series_title']} · Частина {post['series_part']} із {post['series_total']}")
    if post["text"]:
        parts.append(post["text"])
    value = "\n\n".join(parts)
    # Telegram limits text/captions in UTF-16 code units, not Python code points.
    limit = 1024 if with_photo else 4096
    encoded = value.encode("utf-16-le")
    if len(encoded) // 2 > limit:
        suffix = "…\n\nПовний текст — у застосунку «Наші»."
        room = limit - len(suffix.encode("utf-16-le")) // 2
        value = encoded[:room * 2].decode("utf-16-le", errors="ignore") + suffix
    return value


def _poll_text(value, limit):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Invalid Telegram poll text")
    value = value.strip()
    return value if len(value) <= limit else value[:limit - 1] + "…"


def channel_poll(post):
    """Native Telegram votes stay independent of the app's poll records."""
    options = post.get("options") or []
    if not 2 <= len(options) <= 12:
        raise ValueError("Invalid Telegram poll options")
    texts = [_poll_text(option["text"], 100) for option in options]
    # App options allow 160 characters. Preserve distinct answers when their
    # first 100 characters match, without changing the stored app options.
    used = set()
    output = []
    for index, text in enumerate(texts, 1):
        candidate = text
        suffix_index = index
        while candidate.casefold() in used:
            suffix = f"… ({suffix_index})"
            candidate = text[:100 - len(suffix)] + suffix
            suffix_index += 1
        used.add(candidate.casefold())
        output.append(InputPollOption(text=candidate, text_parse_mode=None))
    return {
        "question": _poll_text(post.get("question") or post.get("title"), 300),
        "options": output,
        "type": "regular",
        "is_anonymous": True,
        "allows_multiple_answers": False,
        "question_parse_mode": None,
    }


def _failure_status(error):
    # A parsed 4xx/429 Bot API response proves the request was rejected.
    # Network/5xx/unknown errors do not prove it and must not be blindly resent.
    if isinstance(error, TelegramAPIError) and not isinstance(error, (TelegramNetworkError, TelegramServerError)):
        return "failed"
    return "unknown"


def _message_id(message):
    value = getattr(message, "message_id", None)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError("Bot response missing message ID")
    return value


def _default_bot():
    from app.config import BOT_TOKEN
    return Bot(BOT_TOKEN)


@contextmanager
def _database(factory):
    with factory() as connection:
        with connection.transaction():
            yield connection


async def deliver_community_telegram(*, post_id, actor_user_id, connection_factory,
                                     bot_factory=None, media_storage_factory=CommunityMediaStorage,
                                     retry=False, deliver_personal=True, personal_only=False):
    # Import lazily to avoid repository/serializer dependency cycles.
    from app.community import CommunityRepository

    def current(connection):
        return CommunityRepository(connection)._post_row(post_id, published_only=True, lock=True)

    with _database(connection_factory) as connection:
        row = current(connection)
        status = row["telegram_status"]
        summary = telegram_admin_status(connection, row)
        if retry and not personal_only and not summary["telegram_retry_allowed"]:
            raise CommunityError(409, "TELEGRAM_RETRY_UNAVAILABLE",
                                 "Повторна доставка зараз недоступна. Оновіть історію публікацій.")
        send_channel = not personal_only and (status == "pending" or (retry and status == "failed"))
        if send_channel:
            row = connection.execute("""UPDATE community_posts SET telegram_status='sending',
                telegram_attempted_at=NOW(),telegram_error_code=NULL WHERE id=%s RETURNING *""",
                (post_id,)).fetchone()
        elif status != "published":
            return CommunityRepository(connection).get_post(post_id, actor_user_id, published_only=False, admin=True)
        post = CommunityRepository(connection)._serialized(row, actor_user_id, admin=True)

    bot = None
    try:
        try:
            bot = (bot_factory or _default_bot)()
        except Exception as error:
            # No HTTP request occurred: unlike a timeout, retry is safe.
            if send_channel:
                with _database(connection_factory) as connection:
                    connection.execute("""UPDATE community_posts SET telegram_status='failed',
                        telegram_error_code=%s WHERE id=%s AND telegram_status='sending'""",
                        (type(error).__name__, post_id))
            logger.warning("Community Telegram unavailable (%s)", type(error).__name__)
            return

        if send_channel:
            is_poll = post["type"] == "poll"
            image = None if is_poll else post.get("image_url")
            data = None
            poll = None
            if is_poll:
                try:
                    poll = channel_poll(post)
                except Exception as error:
                    with _database(connection_factory) as connection:
                        connection.execute("""UPDATE community_posts SET telegram_status='failed',
                            telegram_error_code=%s WHERE id=%s AND telegram_status='sending'""",
                            (type(error).__name__, post_id))
                    logger.warning("Community Telegram poll invalid (%s)", type(error).__name__)
                    return
            if image:
                try:
                    # Preparation cannot have published anything to Telegram.
                    data = media_storage_factory().read(image)
                    if data is None:
                        raise FileNotFoundError("Community image unavailable")
                except Exception as error:
                    with _database(connection_factory) as connection:
                        connection.execute("""UPDATE community_posts SET telegram_status='failed',
                            telegram_error_code=%s WHERE id=%s AND telegram_status='sending'""",
                            (type(error).__name__, post_id))
                    logger.warning("Community Telegram media unavailable (%s)", type(error).__name__)
                    return
            try:
                if is_poll:
                    message = await bot.send_poll(chat_id=CHANNEL_USERNAME, **poll,
                        disable_notification=True, request_timeout=15)
                elif image:
                    # Read only our randomized local Community storage asset.
                    message = await bot.send_photo(chat_id=CHANNEL_USERNAME,
                        photo=BufferedInputFile(data, filename=image.rsplit("/", 1)[-1]),
                        caption=channel_text(post, with_photo=True), parse_mode=None,
                        disable_notification=True, request_timeout=15)
                else:
                    message = await bot.send_message(chat_id=CHANNEL_USERNAME, text=channel_text(post),
                        parse_mode=None, disable_notification=True, request_timeout=15)
                message_id = _message_id(message)
            except Exception as error:
                state = _failure_status(error)
                with _database(connection_factory) as connection:
                    connection.execute("""UPDATE community_posts SET telegram_status=%s,
                        telegram_error_code=%s WHERE id=%s AND telegram_status='sending'""",
                        (state, type(error).__name__, post_id))
                logger.warning("Community Telegram channel %s (%s)", state, type(error).__name__)
                return
            with _database(connection_factory) as connection:
                connection.execute("""UPDATE community_posts SET telegram_status='published',
                    telegram_message_id=%s,telegram_error_code=NULL
                    WHERE id=%s AND telegram_status='sending'""", (message_id, post_id))

        if not deliver_personal:
            return

        with _database(connection_factory) as connection:
            row = current(connection)
            url = channel_post_url(row["telegram_message_id"])
            candidates = connection.execute("""SELECT user_id FROM community_telegram_deliveries
                WHERE post_id=%s AND status=ANY(%s) ORDER BY user_id""",
                (post_id, ["pending", "failed"] if retry else ["pending"])).fetchall()
        if not url:
            return
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="☕️ Відкрити публікацію", url=url)]])
        for recipient in candidates:
            with _database(connection_factory) as connection:
                current(connection)  # Stop pending delivery after soft deletion.
                claimed = connection.execute("""SELECT d.* FROM community_telegram_deliveries d
                    WHERE d.post_id=%s AND d.user_id=%s FOR UPDATE""", (post_id, recipient["user_id"])).fetchone()
                if not claimed or claimed["status"] not in (["pending", "failed"] if retry else ["pending"]):
                    continue
                app = connection.execute(f"SELECT ({_app_evidence(connection)}) AS is_app FROM users u WHERE u.id=%s",
                                         (claimed["user_id"],)).fetchone()
                state = "skipped" if not app or app["is_app"] else "sending"
                connection.execute("""UPDATE community_telegram_deliveries SET status=%s,
                    attempted_at=NOW(),error_code=NULL WHERE post_id=%s AND user_id=%s""",
                    (state, post_id, claimed["user_id"]))
            if state == "skipped":
                continue
            try:
                await asyncio.sleep(0.05)  # Stay below Telegram's bulk message rate.
                message = await bot.send_message(chat_id=claimed["telegram_user_id"],
                    text=f"☕️ Нова публікація у «Кавовій спільноті»\n\n{post['title']}",
                    parse_mode=None, reply_markup=keyboard, request_timeout=15)
                message_id = _message_id(message)
                state, error_code = "sent", None
                rate_limited = False
            except Exception as error:
                state, error_code, message_id = _failure_status(error), type(error).__name__, None
                rate_limited = isinstance(error, TelegramRetryAfter)
                logger.warning("Community Telegram personal delivery %s (%s)", state, error_code)
            with _database(connection_factory) as connection:
                connection.execute("""UPDATE community_telegram_deliveries SET status=%s,
                    message_id=%s,error_code=%s WHERE post_id=%s AND user_id=%s AND status='sending'""",
                    (state, message_id, error_code, post_id, claimed["user_id"]))
            if rate_limited:
                # Keep the rest pending for an explicit safe resume instead of
                # hammering the API during its requested backoff interval.
                break
    finally:
        if bot is not None:
            with suppress(Exception):
                await bot.session.close()
