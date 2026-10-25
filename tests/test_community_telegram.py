"""Local PostgreSQL + fake Bot API; this suite never sends Telegram/APNs."""

import asyncio
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramServerError
from aiogram.exceptions import TelegramRetryAfter
from aiogram.methods import SendMessage
from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient
from PIL import Image
import pytest

from conftest import PROJECT, bearer
from test_community import MIGRATION, SOFT_DELETE_MIGRATION, ACTIVE_SERIES_MIGRATION, TELEGRAM_MIGRATION, _draft


def bot_error(cls=TelegramBadRequest):
    return cls(method=SendMessage(chat_id="@nashi_coffee_community", text="test"), message="fake failure")


@pytest.fixture
def telegram_api(database, tmp_path):
    for migration in (MIGRATION, SOFT_DELETE_MIGRATION, ACTIVE_SERIES_MIGRATION, TELEGRAM_MIGRATION):
        database.query(migration.read_text())
    from app.api.community import build_community_router
    from app.community_media import CommunityMediaStorage
    bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=27)),
                          send_photo=AsyncMock(return_value=SimpleNamespace(message_id=28)),
                          session=SimpleNamespace(close=AsyncMock()))
    pushes = AsyncMock(return_value={"sent": 1, "failed": 0})
    storage = CommunityMediaStorage(tmp_path / "media")

    def auth(authorization: str | None = Header(default=None)):
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(401)
        session = database.db.get_app_session(authorization[7:])
        if session is None:
            raise HTTPException(401)
        return session

    def actor(authorization, init_data):
        user = auth(authorization)
        return {"user_id": user["user_id"], "telegram_id": user.get("telegram_user_id")}

    app = FastAPI()
    app.include_router(build_community_router(authenticate=auth, verify_actor=actor,
        superadmin_telegram_ids={1001}, connection_factory=database.db.get_connection,
        telegram_bot_factory=lambda: bot, media_storage_factory=lambda: storage, push_sender=pushes))
    with TestClient(app) as client:
        yield SimpleNamespace(client=client, db=database, bot=bot, pushes=pushes,
                              token=database.db.create_app_session(1), storage=storage)


def publish(harness, post, *, notify=False):
    response = harness.client.post(f"/community/admin/posts/{post['id']}/publish",
                                  headers=bearer(harness.token), json={"notify": notify})
    assert response.status_code == 200, response.text
    # TestClient waits for background delivery; refresh persisted final state.
    return harness.client.get(f"/community/admin/posts/{post['id']}", headers=bearer(harness.token)).json()


def retry(harness, post):
    response = harness.client.post(f"/community/admin/posts/{post['id']}/telegram/retry", headers=bearer(harness.token))
    if response.status_code == 200:
        return harness.client.get(f"/community/admin/posts/{post['id']}", headers=bearer(harness.token))
    return response


def add_devices(harness):
    harness.db.query("""CREATE TABLE app_push_devices(id BIGSERIAL PRIMARY KEY,user_id BIGINT REFERENCES users(id),
        device_token TEXT,platform TEXT DEFAULT 'ios',environment TEXT DEFAULT 'production',
        notifications_enabled BOOLEAN DEFAULT TRUE)""")


@pytest.mark.parametrize("kind", ["fact", "tip", "history", "news", "poll", "series"])
def test_all_types_channel_silent_once_and_consumer_unchanged(telegram_api, kind):
    h = telegram_api
    extra = {"question": "Що обираєш?", "options": ["Фільтр", "Лате"]} if kind == "poll" else {}
    if kind == "series":
        extra = {"series_title": "Кавові міфи", "series_part": 3, "series_total": 5}
    post = _draft(h.client, h.token, type=kind, title="<Публікація> ☕", **extra)
    assert h.bot.send_message.await_count == 0
    result = publish(h, post)
    assert result["telegram_status"] == "published"
    assert result["telegram_message_id"] == 27
    assert result["telegram_url"] == "https://t.me/nashi_coffee_community/27"
    assert result["telegram_retry_allowed"] is False
    publish(h, post, notify=True)
    assert h.bot.send_message.await_count == 1
    h.pushes.assert_not_awaited()
    call = h.bot.send_message.await_args.kwargs
    assert call["chat_id"] == "@nashi_coffee_community"
    assert call["disable_notification"] is True and call["parse_mode"] is None
    assert "<Публікація>" in call["text"]
    if kind == "poll":
        assert "○ Фільтр" in call["text"] and "Голосуйте у застосунку" in call["text"]
    if kind == "series":
        assert "Кавові міфи · Частина 3 із 5" in call["text"]
    public = h.client.get(f"/me/community/posts/{post['id']}", headers=bearer(h.token)).json()
    assert public["title"] == "<Публікація> ☕"
    assert "telegram_status" not in public


def test_real_app_evidence_blocks_dm_even_when_permission_disabled_or_no_token(telegram_api):
    h = telegram_api
    add_devices(h)
    for user_id in range(10, 16):
        h.db.query("INSERT INTO users(id,telegram_user_id,personal_qr_token) VALUES(%s,%s,%s)",
                   (user_id, 4000 + user_id, f"user{user_id}"))
    h.db.query("""INSERT INTO user_identities(user_id,provider,provider_user_id)
        VALUES(10,'google','google-only'),(11,'apple','apple-only'),(15,'telegram','9001')""")
    h.db.db.create_app_session(12)
    h.db.query("INSERT INTO app_push_devices(user_id,device_token,notifications_enabled) VALUES(13,'disabled',FALSE),(14,'enabled',TRUE)")
    post = _draft(h.client, h.token)
    publish(h, post, notify=True)
    h.pushes.assert_awaited_once()
    assert len(h.pushes.await_args.args[0]) == 1
    dm_targets = [call.kwargs["chat_id"] for call in h.bot.send_message.await_args_list
                  if isinstance(call.kwargs["chat_id"], int)]
    assert set(dm_targets) == {1003, 9001}
    assert not set(dm_targets) & {4010, 4011, 4012, 4013, 4014, 4015}
    for call in h.bot.send_message.await_args_list[1:]:
        assert call.kwargs["reply_markup"].inline_keyboard[0][0].url == "https://t.me/nashi_coffee_community/27"
    assert {row["user_id"] for row in h.db.query("SELECT user_id FROM community_telegram_deliveries")} == {3, 15}


def test_channel_failure_keeps_app_published_and_safe_retry_does_not_repush(telegram_api):
    h = telegram_api
    add_devices(h)
    h.db.query("INSERT INTO app_push_devices(user_id,device_token) VALUES(1,'enabled')")
    h.bot.send_message.side_effect = [bot_error(), SimpleNamespace(message_id=29), SimpleNamespace(message_id=30)]
    post = _draft(h.client, h.token)
    result = publish(h, post, notify=True)
    assert result["is_published"] and result["telegram_status"] == "failed" and result["telegram_retry_allowed"]
    assert h.db.query("SELECT status FROM community_telegram_deliveries") == [{"status": "pending"}]
    # Repeated publication itself does not retry or re-notify.
    publish(h, post, notify=True)
    assert h.bot.send_message.await_count == 1
    fixed = retry(h, post)
    assert fixed.status_code == 200, fixed.text
    assert fixed.json()["telegram_status"] == "published"
    assert h.bot.send_message.await_count == 3  # first rejection, channel, one DM
    h.pushes.assert_awaited_once()
    assert retry(h, post).status_code == 409


@pytest.mark.parametrize("error", [TimeoutError(), bot_error(TelegramNetworkError), bot_error(TelegramServerError)])
def test_ambiguous_channel_result_cannot_duplicate_on_retry(telegram_api, error):
    h = telegram_api
    h.bot.send_message.side_effect = error
    post = _draft(h.client, h.token)
    result = publish(h, post, notify=True)
    assert result["is_published"] and result["telegram_status"] == "unknown"
    assert result["telegram_retry_allowed"] is False
    assert retry(h, post).status_code == 409
    publish(h, post, notify=True)
    assert h.bot.send_message.await_count == 1
    assert h.db.query("SELECT status FROM community_telegram_deliveries") == [{"status": "pending"}]


def test_dm_failure_retries_remaining_dm_without_duplicate_channel_or_successful_dm(telegram_api):
    h = telegram_api
    h.db.query("INSERT INTO users(id,telegram_user_id,personal_qr_token) VALUES(10,4010,'u10')")
    h.bot.send_message.side_effect = [SimpleNamespace(message_id=27), SimpleNamespace(message_id=31),
                                    bot_error(), SimpleNamespace(message_id=32)]
    post = _draft(h.client, h.token)
    result = publish(h, post, notify=True)
    assert result["telegram_status"] == "published" and result["telegram_dm_failed_count"] == 1
    assert result["telegram_retry_allowed"] is True
    fixed = retry(h, post)
    assert fixed.status_code == 200 and fixed.json()["telegram_dm_failed_count"] == 0
    targets = [call.kwargs["chat_id"] for call in h.bot.send_message.await_args_list]
    assert targets == ["@nashi_coffee_community", 1003, 4010, 4010]
    assert retry(h, post).status_code == 409
    assert h.pushes.await_count == 0


def test_dm_timeout_is_never_retried_and_late_app_user_is_skipped(telegram_api):
    h = telegram_api
    h.db.query("INSERT INTO users(id,telegram_user_id,personal_qr_token) VALUES(10,4010,'u10')")
    async def send(**kwargs):
        if kwargs["chat_id"] == "@nashi_coffee_community":
            h.db.db.create_app_session(10)
            return SimpleNamespace(message_id=27)
        raise TimeoutError()
    h.bot.send_message.side_effect = send
    post = _draft(h.client, h.token)
    result = publish(h, post, notify=True)
    assert result["telegram_retry_allowed"] is False
    rows = h.db.query("SELECT user_id,status FROM community_telegram_deliveries ORDER BY user_id")
    assert rows == [{"user_id": 3, "status": "unknown"}, {"user_id": 10, "status": "skipped"}]
    assert retry(h, post).status_code == 409


def test_photo_uses_our_storage_bytes_and_limits_caption(telegram_api):
    h = telegram_api
    post = _draft(h.client, h.token, text="☕" * 10_000)
    image = BytesIO()
    Image.new("RGB", (12, 12)).save(image, format="PNG")
    response = h.client.post(f"/community/admin/posts/{post['id']}/image", headers=bearer(h.token),
                            files={"photo": ("coffee.png", image.getvalue(), "image/png")})
    assert response.status_code == 200, response.text
    result = publish(h, post)
    h.bot.send_photo.assert_awaited_once()
    h.bot.send_message.assert_not_awaited()
    call = h.bot.send_photo.await_args.kwargs
    assert call["photo"].data == image.getvalue()
    assert call["disable_notification"] is True and call["parse_mode"] is None
    assert len(call["caption"].encode("utf-16-le")) // 2 <= 1024
    assert result["telegram_message_id"] == 28


def test_draft_deleted_unauthorized_and_previous_posts_are_not_delivered(telegram_api):
    h = telegram_api
    post = _draft(h.client, h.token)
    assert retry(h, post).status_code == 404
    h.client.delete(f"/community/admin/posts/{post['id']}", headers=bearer(h.token))
    assert h.client.post(f"/community/admin/posts/{post['id']}/publish", headers=bearer(h.token), json={}).status_code == 404
    assert retry(h, post).status_code == 404
    h.bot.send_message.assert_not_awaited()
    h.db.query("INSERT INTO community_posts(type,title,text,published_at) VALUES('fact','legacy','old',NOW())")
    h.db.query(TELEGRAM_MIGRATION.read_text())
    assert h.db.query("SELECT telegram_status FROM community_posts WHERE title='legacy'") == [{"telegram_status": None}]
    ordinary = h.db.db.create_app_session(3)
    for headers in ({}, bearer(ordinary)):
        assert h.client.post(f"/community/admin/posts/{post['id']}/telegram/retry", headers=headers).status_code in (401, 403)


def test_channel_text_utf16_limit_and_safe_media_read(telegram_api):
    from app.community_telegram import channel_text
    h = telegram_api
    value = channel_text({"type": "fact", "title": "☕" * 180, "text": "☕" * 20_000})
    assert len(value.encode("utf-16-le")) // 2 <= 4096
    for url in ("https://attacker.invalid/a.png", "/uploads/community/../../secret", "/uploads/community/x.png"):
        with pytest.raises(ValueError):
            h.storage.read(url)


def test_corrupt_local_photo_is_safe_to_repair_and_retry(telegram_api):
    h = telegram_api
    post = _draft(h.client, h.token)
    image = BytesIO()
    Image.new("RGB", (12, 12)).save(image, format="PNG")
    h.client.post(f"/community/admin/posts/{post['id']}/image", headers=bearer(h.token),
                  files={"photo": ("coffee.png", image.getvalue(), "image/png")})
    original = h.storage.read
    h.storage.read = lambda _: (_ for _ in ()).throw(PermissionError("fixture-only"))
    result = publish(h, post)
    assert result["telegram_status"] == "failed" and result["telegram_retry_allowed"] is True
    h.bot.send_photo.assert_not_awaited()
    h.storage.read = original
    assert retry(h, post).json()["telegram_status"] == "published"
    h.bot.send_photo.assert_awaited_once()


def test_rate_limit_stops_bulk_delivery_and_resumes_pending_without_duplicate(telegram_api):
    h = telegram_api
    h.db.query("INSERT INTO users(id,telegram_user_id,personal_qr_token) VALUES(10,4010,'u10')")
    limit = TelegramRetryAfter(method=SendMessage(chat_id=1003, text="test"), message="rate", retry_after=1)
    h.bot.send_message.side_effect = [SimpleNamespace(message_id=27), limit,
                                    SimpleNamespace(message_id=31), SimpleNamespace(message_id=32)]
    post = _draft(h.client, h.token)
    result = publish(h, post, notify=True)
    assert result["telegram_status"] == "published" and result["telegram_retry_allowed"]
    assert result["telegram_dm_pending_count"] == 1 and result["telegram_dm_failed_count"] == 1
    assert h.bot.send_message.await_count == 2
    final = retry(h, post).json()
    assert final["telegram_dm_pending_count"] == final["telegram_dm_failed_count"] == 0
    assert [call.kwargs["chat_id"] for call in h.bot.send_message.await_args_list] == [
        "@nashi_coffee_community", 1003, 1003, 4010]


def test_persisted_pending_dm_can_resume_after_worker_interruption(telegram_api):
    h = telegram_api
    post = _draft(h.client, h.token)
    publish(h, post)
    h.db.query("""INSERT INTO community_telegram_deliveries(post_id,user_id,telegram_user_id,status)
        VALUES(%s,3,1003,'pending')""", (post["id"],))
    before = h.client.get(f"/community/admin/posts/{post['id']}", headers=bearer(h.token)).json()
    assert before["telegram_retry_allowed"] and before["telegram_dm_pending_count"] == 1
    assert retry(h, post).json()["telegram_dm_pending_count"] == 0
    targets = [call.kwargs["chat_id"] for call in h.bot.send_message.await_args_list]
    assert targets == ["@nashi_coffee_community", 1003]


def test_inflight_channel_and_dm_claims_never_replay(telegram_api):
    h = telegram_api
    post = _draft(h.client, h.token)
    h.db.query("""UPDATE community_posts SET published_at=NOW(),telegram_status='sending' WHERE id=%s""", (post["id"],))
    publish(h, post, notify=True)
    assert retry(h, post).status_code == 409
    h.bot.send_message.assert_not_awaited()
    h.db.query("""UPDATE community_posts SET telegram_status='published',telegram_message_id=27 WHERE id=%s""", (post["id"],))
    h.db.query("""INSERT INTO community_telegram_deliveries(post_id,user_id,telegram_user_id,status)
        VALUES(%s,3,1003,'sending')""", (post["id"],))
    assert retry(h, post).status_code == 409
    h.bot.send_message.assert_not_awaited()


def test_concurrent_channel_workers_claim_exactly_once(telegram_api):
    from app.community import CommunityRepository
    from app.community_telegram import deliver_community_telegram
    h = telegram_api
    post = _draft(h.client, h.token)
    with h.db.db.get_connection() as connection:
        with connection.transaction():
            CommunityRepository(connection).claim_publication(post["id"], 1, True)

    async def run():
        started, release = asyncio.Event(), asyncio.Event()
        async def send(**kwargs):
            if kwargs["chat_id"] == "@nashi_coffee_community":
                started.set()
                await release.wait()
            return SimpleNamespace(message_id=27)
        h.bot.send_message.side_effect = send
        options = dict(post_id=post["id"], actor_user_id=1, connection_factory=h.db.db.get_connection,
                       bot_factory=lambda: h.bot, media_storage_factory=lambda: h.storage)
        first = asyncio.create_task(deliver_community_telegram(**options))
        await started.wait()
        # Second worker sees the committed sending claim, never a retryable pending.
        await deliver_community_telegram(**options)
        release.set()
        await first
    asyncio.run(run())
    assert [call.kwargs["chat_id"] for call in h.bot.send_message.await_args_list] == [
        "@nashi_coffee_community", 1003]
    assert h.db.query("SELECT status FROM community_telegram_deliveries") == [{"status": "sent"}]


def test_concurrent_personal_workers_claim_each_recipient_once(telegram_api):
    from app.community_telegram import deliver_community_telegram
    h = telegram_api
    post = _draft(h.client, h.token)
    publish(h, post)
    h.db.query("""INSERT INTO community_telegram_deliveries(post_id,user_id,telegram_user_id,status)
        VALUES(%s,3,1003,'pending')""", (post["id"],))
    async def run():
        options = dict(post_id=post["id"], actor_user_id=1, connection_factory=h.db.db.get_connection,
                       bot_factory=lambda: h.bot, media_storage_factory=lambda: h.storage)
        await asyncio.gather(deliver_community_telegram(**options), deliver_community_telegram(**options))
    asyncio.run(run())
    assert [call.kwargs["chat_id"] for call in h.bot.send_message.await_args_list] == [
        "@nashi_coffee_community", 1003]


def test_pending_channel_before_worker_start_can_be_safely_resumed(telegram_api):
    h = telegram_api
    post = _draft(h.client, h.token)
    h.db.query("UPDATE community_posts SET published_at=NOW(),telegram_status='pending' WHERE id=%s", (post["id"],))
    assert retry(h, post).json()["telegram_status"] == "published"
    h.bot.send_message.assert_awaited_once()


def test_rejected_channel_retry_makes_only_one_attempt_not_background_retry(telegram_api):
    h = telegram_api
    h.bot.send_message.side_effect = bot_error()
    post = _draft(h.client, h.token)
    assert publish(h, post, notify=True)["telegram_status"] == "failed"
    assert h.bot.send_message.await_count == 1
    assert retry(h, post).json()["telegram_status"] == "failed"
    assert h.bot.send_message.await_count == 2
    assert h.db.query("SELECT status FROM community_telegram_deliveries") == [{"status": "pending"}]
