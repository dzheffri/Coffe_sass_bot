"""Approval/inbox/APNs isolation on disposable PostgreSQL. No live deliveries."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock

from psycopg.types.json import Jsonb
import pytest

from conftest import PROJECT, bearer
from test_barista_operations import operations_database
from test_barista_owner_tools import owner_database
from test_consumer_broadcast_inbox import inbox_api
from app import shop_novelty_notifications as novelty


@pytest.fixture
def novelty_db(owner_database):
    db = owner_database
    db.query("""ALTER TABLE coffee_shops ADD COLUMN subtitle TEXT, ADD COLUMN address TEXT,
        ADD COLUMN work_from TEXT, ADD COLUMN work_to TEXT, ADD COLUMN instagram TEXT,
        ADD COLUMN description TEXT, ADD COLUMN logo_url TEXT, ADD COLUMN cover_url TEXT;
        CREATE TABLE shop_news(id BIGSERIAL PRIMARY KEY,shop_id BIGINT REFERENCES coffee_shops(id),
            title TEXT,price TEXT,image_url TEXT,sort_order INT,updated_at TIMESTAMPTZ);
        CREATE TABLE shop_change_requests(id BIGSERIAL PRIMARY KEY,shop_id BIGINT REFERENCES coffee_shops(id),
            payload JSONB,status TEXT DEFAULT 'pending',reviewed_at TIMESTAMPTZ,
            reviewed_by_telegram_id BIGINT,reject_reason TEXT);
        CREATE TABLE shop_reminder_settings(shop_id BIGINT PRIMARY KEY,shop_news_enabled BOOLEAN DEFAULT TRUE);
        INSERT INTO shop_news(shop_id,title,price,image_url,sort_order)
            VALUES(1,'Старий десерт','95','old.jpg',0);
        INSERT INTO shop_clients(shop_id,user_id,last_activity_at) VALUES(1,2,NOW());
    """)
    db.query((PROJECT / "migrations/20261026_shop_novelty_notifications.sql").read_text())
    return db


def request(db, items, shop_id=1):
    return db.query("""INSERT INTO shop_change_requests(shop_id,payload) VALUES(%s,%s) RETURNING id""",
        (shop_id, Jsonb({"name": "Наші", "news": items})))[0]["id"]


def approve(db, items):
    request_id = request(db, items)
    assert novelty.approve_shop_change(request_id, 9001) == {
        "ok": True, "status": "approved", "request_id": request_id, "shop_id": 1}
    return request_id


def test_submission_silent_approval_batches_new_items_and_exact_shop_inbox(novelty_db, inbox_api):
    db = novelty_db
    request_id = request(db, [{"title": "Новий десерт", "price": "160", "image_url": "new.jpg"},
                              {"title": "Новий напій", "price": "80 грн"}])
    assert db.query("SELECT * FROM broadcasts") == []
    assert db.query("SELECT * FROM shop_novelty_deliveries") == []
    novelty.approve_shop_change(request_id, 9001)
    event = db.query("SELECT * FROM shop_novelty_events")[0]
    recipients = db.query("SELECT user_id FROM broadcast_recipients ORDER BY user_id")
    assert recipients == [{"user_id": user} for user in (2,4,6)]  # app-only and >60 days included
    assert db.query("SELECT COUNT(*) AS n FROM broadcasts")[0]["n"] == 1
    assert event["shop_id"] == 1 and "160 грн" in event["push_body"] and "80 грн" in event["push_body"]
    assert db.query("SELECT origin FROM broadcasts") == [{"origin": "shop_novelty"}]
    token = db.db.create_app_session(2)
    inbox = inbox_api.client.get("/me/broadcasts", headers=bearer(token))
    assert inbox.status_code == 200 and inbox.json()["unread_count"] == 1
    assert inbox.json()["items"][0]["broadcast_id"] == event["broadcast_id"]
    other = db.db.create_app_session(5)
    assert inbox_api.client.get("/me/broadcasts", headers=bearer(other)).json()["items"] == []


def test_price_photo_reapprove_and_normalized_title_never_repush(novelty_db):
    db = novelty_db
    first = approve(db, [{"title": "Новий десерт", "price": "160", "image_url": "a.jpg"}])
    novelty.approve_shop_change(first, 9001)
    approve(db, [{"title": "  НОВИЙ   десерт  ", "price": "180", "image_url": "replacement.jpg"}])
    approve(db, [{"title": "Новий десерт", "price": "200"}])
    assert db.query("SELECT COUNT(*) AS n FROM broadcasts")[0]["n"] == 1
    assert db.query("SELECT COUNT(*) AS n FROM shop_novelty_events")[0]["n"] == 1
    assert db.query("SELECT price FROM shop_news") == [{"price": "200"}]


def test_migration_idempotent_existing_products_not_announced(novelty_db):
    db = novelty_db
    db.query((PROJECT / "migrations/20261026_shop_novelty_notifications.sql").read_text())
    approve(db, [{"title": "Старий десерт", "price": "115", "image_url": "replacement.jpg"},
                 {"title": "", "price": "", "image_url": ""}])
    assert db.query("SELECT * FROM broadcasts") == []


def test_concurrent_duplicate_approval_creates_one_batch(novelty_db):
    db = novelty_db
    request_id = request(db, [{"title": "Лате"}])
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: novelty.approve_shop_change(request_id, 9001), range(2)))
    assert all(result["ok"] for result in results)
    assert db.query("SELECT COUNT(*) AS n FROM broadcasts")[0]["n"] == 1


def test_atomic_approval_rolls_back_profile_and_ledger(novelty_db, monkeypatch):
    db = novelty_db
    request_id = request(db, [{"title": "Не збережено"}])
    monkeypatch.setattr(novelty, "record_approved_novelties", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError()))
    with pytest.raises(RuntimeError):
        novelty.approve_shop_change(request_id, 9001)
    assert db.query("SELECT title FROM shop_news") == [{"title": "Старий десерт"}]
    assert db.query("SELECT status FROM shop_change_requests") == [{"status": "pending"}]


def test_apns_one_per_user_batch_only_enabled_ios_and_no_repeat(novelty_db, monkeypatch):
    db = novelty_db
    approve(db, [{"title": "Лате"}, {"title": "Макарунс"}])
    devices = lambda user_id: [
        {"device_token": "a" * 64, "platform": "ios", "notifications_enabled": True},
        {"device_token": "a" * 64, "platform": "ios", "notifications_enabled": True},
        {"device_token": "b" * 64, "platform": "ios", "notifications_enabled": False},
        {"device_token": "not-fcm-in-this-release", "platform": "android"},
    ]
    monkeypatch.setattr(db.db, "get_app_push_devices_for_user", devices, raising=False)
    sender = AsyncMock(return_value={"sent": 1, "failed": 0})

    async def drain():
        while await novelty.deliver_one(sender=sender):
            pass
    asyncio.run(drain())
    assert sender.await_count == 3
    for call in sender.await_args_list:
        assert len(call.kwargs["devices"]) == 1
        assert call.kwargs["data"]["shop_id"] == 1 and call.kwargs["data"]["broadcast_id"] > 0
        assert "Наші" in call.kwargs["title"]
    asyncio.run(drain())
    assert sender.await_count == 3
    assert db.query("SELECT DISTINCT status FROM shop_novelty_deliveries") == [{"status": "sent"}]


def test_uncertain_push_not_retried_and_no_telegram(novelty_db, monkeypatch):
    db = novelty_db
    approve(db, [{"title": "Лате"}])
    monkeypatch.setattr(db.db, "get_app_push_devices_for_user",
                        lambda _: [{"device_token": "a" * 64}], raising=False)
    sender = AsyncMock(side_effect=TimeoutError())

    async def drain():
        while await novelty.deliver_one(sender=sender):
            pass
    asyncio.run(drain()); asyncio.run(drain())
    assert sender.await_count == 3
    assert db.query("SELECT DISTINCT status FROM shop_novelty_deliveries") == [{"status": "unknown"}]


def test_novelty_does_not_consume_manual_weekly_quota(novelty_db):
    db = novelty_db
    for index in range(9):
        approve(db, [{"title": f"Новинка {index}"}])
    assert db.db.can_send_broadcast(1) is True
    assert db.query("SELECT COUNT(*) AS n FROM broadcasts")[0]["n"] == 9


def test_disabled_automatic_push_keeps_inbox(novelty_db):
    db = novelty_db
    db.query("INSERT INTO shop_reminder_settings VALUES(1,FALSE)")
    approve(db, [{"title": "Новий десерт"}])
    assert db.query("SELECT COUNT(*) AS n FROM broadcast_recipients")[0]["n"] == 3
    assert db.query("SELECT * FROM shop_novelty_deliveries") == []


def test_merge_preserves_sent_receipt_and_crash_is_not_replayed(novelty_db, monkeypatch):
    db = novelty_db
    approve(db, [{"title": "Новий десерт"}])
    db.query("UPDATE shop_novelty_deliveries SET status='sent',attempted=1,sent=1 WHERE user_id=4")
    with db.db.get_connection() as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                novelty.merge_novelty_receipts(cursor, 4, 2)
                cursor.execute("DELETE FROM broadcast_recipients WHERE user_id=4")
    assert db.query("SELECT status,sent FROM shop_novelty_deliveries WHERE user_id=2") == [{"status": "sent", "sent": 1}]
    db.query("UPDATE shop_novelty_deliveries SET status='sending',updated_at=NOW()-INTERVAL '11 minutes' WHERE user_id=6")
    sender = AsyncMock()
    assert asyncio.run(novelty.deliver_one(sender=sender)) is False
    sender.assert_not_called()
    assert db.query("SELECT status FROM shop_novelty_deliveries WHERE user_id=6") == [{"status": "unknown"}]
