"""Publication destinations against local PostgreSQL and mocked delivery clients."""

from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock
import sys

import pytest

from conftest import PROJECT, bearer
from test_community import (
    ACTIVE_SERIES_MIGRATION, MIGRATION, SOFT_DELETE_MIGRATION, TELEGRAM_MIGRATION,
    _draft, _tokens, community_api,
)


DESTINATIONS_MIGRATION = PROJECT / "migrations/20261026_community_destinations.sql"


@pytest.fixture
def delivery_clients(monkeypatch):
    """Run the real delivery logic while replacing both external transports."""
    from app import community_telegram

    bot = SimpleNamespace(
        send_message=AsyncMock(return_value=SimpleNamespace(message_id=71)),
        send_photo=AsyncMock(return_value=SimpleNamespace(message_id=72)),
        send_poll=AsyncMock(return_value=SimpleNamespace(message_id=73)),
        session=SimpleNamespace(close=AsyncMock()),
    )
    real_deliver = community_telegram.deliver_community_telegram

    async def deliver(**kwargs):
        kwargs["bot_factory"] = lambda: bot
        return await real_deliver(**kwargs)

    monkeypatch.setattr(community_telegram, "deliver_community_telegram", deliver)
    pushes = ModuleType("app.app_push")
    pushes.send_app_pushes = AsyncMock(return_value={"sent": 1, "failed": 0, "errors": []})
    monkeypatch.setitem(sys.modules, "app.app_push", pushes)
    return SimpleNamespace(bot=bot, pushes=pushes.send_app_pushes)


def _add_app_device(db):
    db.query("""CREATE TABLE app_push_devices (
        id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id),
        device_token TEXT NOT NULL UNIQUE, platform TEXT NOT NULL DEFAULT 'ios',
        environment TEXT NOT NULL DEFAULT 'production',
        notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE
    )""")
    db.query("INSERT INTO app_push_devices(user_id,device_token) VALUES(2,'app-device')")


def _publish(client, token, post, *, notify=False):
    response = client.post(f"/community/admin/posts/{post['id']}/publish",
                           headers=bearer(token), json={"notify": notify})
    assert response.status_code == 200, response.text
    return client.get(f"/community/admin/posts/{post['id']}", headers=bearer(token)).json()


def test_destination_migration_preserves_existing_rows_and_survives_replay(database):
    for migration in (MIGRATION, SOFT_DELETE_MIGRATION, ACTIVE_SERIES_MIGRATION, TELEGRAM_MIGRATION):
        database.query(migration.read_text())
    database.query("""INSERT INTO community_posts(type,title,text,published_at)
        VALUES('news','Чернетка','Текст',NULL),('news','Опубліковано','Текст',NOW())""")
    before = database.query("SELECT * FROM community_posts ORDER BY id")
    users_before = database.query("SELECT * FROM users ORDER BY id")

    database.query(DESTINATIONS_MIGRATION.read_text())
    database.query(DESTINATIONS_MIGRATION.read_text())
    after = database.query("SELECT * FROM community_posts ORDER BY id")
    assert [{key: row[key] for key in before[0]} for row in after] == before
    assert all(row["publish_to_app"] is True and row["publish_to_telegram"] is True for row in after)
    assert all(row["telegram_content_message_ids"] == [] for row in after)
    assert all(row["telegram_status"] is None for row in after)
    assert database.query("SELECT * FROM community_telegram_deliveries") == []
    assert database.query("SELECT * FROM users ORDER BY id") == users_before

    database.query("""UPDATE community_posts SET publish_to_app=FALSE,publish_to_telegram=FALSE,
        telegram_content_message_ids='[71,72]'::jsonb WHERE id=%s""", (after[0]["id"],))
    changed = database.query("SELECT * FROM community_posts ORDER BY id")
    database.query(DESTINATIONS_MIGRATION.read_text())
    assert database.query("SELECT * FROM community_posts ORDER BY id") == changed


def test_legacy_admin_payload_defaults_both_destinations_and_exposes_them(community_api):
    client, db = community_api
    admin, _ = _tokens(db)
    post = _draft(client, admin)
    assert post["publish_to_app"] is True and post["publish_to_telegram"] is True
    updated = client.put(f"/community/admin/posts/{post['id']}", headers=bearer(admin),
                         json={"type": "news", "title": "Оновлено", "text": "Текст"})
    assert updated.status_code == 200, updated.text
    for result in (updated.json(),
                   client.get(f"/community/admin/posts/{post['id']}", headers=bearer(admin)).json(),
                   client.get("/community/admin/posts", headers=bearer(admin)).json()["items"][0]):
        assert result["publish_to_app"] is True and result["publish_to_telegram"] is True

    telegram_only = _draft(client, admin, publish_to_app=False, publish_to_telegram=True)
    legacy_update = client.put(f"/community/admin/posts/{telegram_only['id']}", headers=bearer(admin),
                               json={"type": "news", "title": "Старий клієнт", "text": "Оновлено"})
    assert legacy_update.status_code == 200, legacy_update.text
    assert legacy_update.json()["publish_to_app"] is False
    assert legacy_update.json()["publish_to_telegram"] is True


@pytest.mark.parametrize("app_destination,telegram_destination", [(True, False), (False, True), (True, True)])
def test_modes_gate_app_push_telegram_channel_and_telegram_only_dm(
        community_api, delivery_clients, app_destination, telegram_destination):
    client, db = community_api
    admin, customer = _tokens(db)
    _add_app_device(db)
    post = _draft(client, admin, publish_to_app=app_destination,
                  publish_to_telegram=telegram_destination)
    result = _publish(client, admin, post, notify=True)
    _publish(client, admin, post, notify=True)

    assert result["is_published"] is True
    assert result["publish_to_app"] is app_destination
    assert result["publish_to_telegram"] is telegram_destination
    assert result["notification_attempted"] is app_destination
    assert delivery_clients.pushes.await_count == int(app_destination)
    channel_calls = [call.kwargs for call in delivery_clients.bot.send_message.await_args_list
                     if call.kwargs["chat_id"] == "@nashi_coffee_community"]
    dm_calls = [call.kwargs for call in delivery_clients.bot.send_message.await_args_list
                if call.kwargs["chat_id"] != "@nashi_coffee_community"]
    assert len(channel_calls) == int(telegram_destination)
    assert [call["chat_id"] for call in dm_calls] == ([1003] if telegram_destination else [])
    assert len(db.query("SELECT * FROM community_telegram_deliveries")) == int(telegram_destination)
    assert result["telegram_status"] == ("published" if telegram_destination else None)
    public = client.get("/me/community/posts", headers=bearer(customer)).json()
    assert public["total"] == int(app_destination)
    assert [item["id"] for item in public["items"]] == ([post["id"]] if app_destination else [])
    delivery_clients.bot.send_photo.assert_not_awaited()
    delivery_clients.bot.send_poll.assert_not_awaited()


@pytest.mark.parametrize("app_destination,telegram_destination", [(True, False), (False, True), (True, True)])
def test_notify_false_only_publishes_selected_content_and_never_replays_notifications(
        community_api, delivery_clients, app_destination, telegram_destination):
    client, db = community_api
    admin, _ = _tokens(db)
    _add_app_device(db)
    post = _draft(client, admin, publish_to_app=app_destination,
                  publish_to_telegram=telegram_destination)
    first = _publish(client, admin, post)
    second = _publish(client, admin, post, notify=True)
    assert first["notification_attempted"] is False
    assert second["notification_attempted"] is False
    delivery_clients.pushes.assert_not_awaited()
    assert db.query("SELECT * FROM community_telegram_deliveries") == []
    assert delivery_clients.bot.send_message.await_count == int(telegram_destination)
    if telegram_destination:
        assert delivery_clients.bot.send_message.await_args.kwargs["chat_id"] == "@nashi_coffee_community"
        assert delivery_clients.bot.send_message.await_args.kwargs["disable_notification"] is True


def test_draft_can_have_no_destination_but_publication_requires_one(community_api, delivery_clients):
    client, db = community_api
    admin, _ = _tokens(db)
    post = _draft(client, admin, publish_to_app=False, publish_to_telegram=False)
    body = {"type": "news", "title": "Без каналу", "text": "Чернетка",
            "publish_to_app": False, "publish_to_telegram": False}
    updated = client.put(f"/community/admin/posts/{post['id']}", headers=bearer(admin), json=body)
    assert updated.status_code == 200, updated.text
    response = client.post(f"/community/admin/posts/{post['id']}/publish", headers=bearer(admin),
                           json={"notify": True})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "COMMUNITY_DESTINATION_REQUIRED"
    stored = db.query("SELECT published_at,notification_attempted,telegram_status FROM community_posts WHERE id=%s",
                      (post["id"],))[0]
    assert stored == {"published_at": None, "notification_attempted": False, "telegram_status": None}
    assert db.query("SELECT * FROM community_telegram_deliveries") == []
    delivery_clients.pushes.assert_not_awaited()
    delivery_clients.bot.send_message.assert_not_awaited()

    body["publish_to_app"] = True
    assert client.put(f"/community/admin/posts/{post['id']}", headers=bearer(admin), json=body).status_code == 200
    assert _publish(client, admin, post)["is_published"] is True


def test_telegram_only_posts_are_hidden_from_every_consumer_path_and_series(community_api, delivery_clients):
    client, db = community_api
    admin, customer = _tokens(db)
    news = _draft(client, admin, publish_to_app=False, publish_to_telegram=True)
    poll = _draft(client, admin, type="poll", text="", question="Кава?", options=["Так", "Ні"],
                  publish_to_app=False, publish_to_telegram=True)
    hidden_part = _draft(client, admin, type="series", series_title="Історія", series_part=1,
                         series_total=2, publish_to_app=False, publish_to_telegram=True)
    visible_part = _draft(client, admin, type="series", series_title="Історія", series_part=2,
                          series_total=2, publish_to_app=True, publish_to_telegram=False)
    for post in (news, poll, hidden_part, visible_part):
        _publish(client, admin, post)
    for post in (news, poll, hidden_part):
        assert client.get(f"/me/community/posts/{post['id']}", headers=bearer(customer)).status_code == 404
        assert client.put(f"/me/community/posts/{post['id']}/reaction", headers=bearer(customer),
                          json={"interested": True}).status_code == 404
        assert client.get(f"/community/admin/posts/{post['id']}", headers=bearer(admin)).status_code == 200
    assert client.post(f"/me/community/posts/{poll['id']}/vote", headers=bearer(customer),
                       json={"option_id": 1}).status_code == 404
    assert db.query("SELECT * FROM community_reactions") == []
    assert db.query("SELECT * FROM community_poll_votes") == []
    feed = client.get("/me/community/posts?limit=1", headers=bearer(customer)).json()
    assert feed["total"] == 1 and [post["id"] for post in feed["items"]] == [visible_part["id"]]
    assert client.get("/me/community/posts?limit=1&offset=1", headers=bearer(customer)).json()["items"] == []
    detail = client.get(f"/me/community/posts/{visible_part['id']}", headers=bearer(customer)).json()
    assert [post["id"] for post in detail["series_posts"]] == [visible_part["id"]]
    assert "publish_to_app" not in detail and "publish_to_telegram" not in detail
    assert client.get("/community/admin/posts", headers=bearer(admin)).json()["total"] == 4


@pytest.mark.parametrize("app_destination,telegram_destination", [(True, False), (False, True)])
def test_published_destination_flags_are_immutable(
        community_api, delivery_clients, app_destination, telegram_destination):
    client, db = community_api
    admin, _ = _tokens(db)
    post = _draft(client, admin, publish_to_app=app_destination,
                  publish_to_telegram=telegram_destination)
    _publish(client, admin, post)
    response = client.put(f"/community/admin/posts/{post['id']}", headers=bearer(admin), json={
        "type": "news", "title": "Зміна напрямку", "text": "Текст",
        "publish_to_app": not app_destination, "publish_to_telegram": not telegram_destination,
    })
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "COMMUNITY_POST_ALREADY_PUBLISHED"
    stored = db.query("SELECT publish_to_app,publish_to_telegram FROM community_posts WHERE id=%s",
                      (post["id"],))[0]
    assert stored == {"publish_to_app": app_destination, "publish_to_telegram": telegram_destination}
