"""Native owner tools against guarded local PostgreSQL, with Telegram mocked."""

import importlib
import asyncio
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import AsyncMock
from types import SimpleNamespace

import pytest

from conftest import bearer
from test_barista_operations import operations_database, seed_balance


@pytest.fixture
def owner_database(operations_database):
    database = operations_database
    database.query("""
        CREATE TABLE broadcasts (
            id BIGSERIAL PRIMARY KEY,
            shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
            sender_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            text TEXT NOT NULL, recipients_count INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        INSERT INTO shop_admins(id,shop_id,user_id,role) VALUES(3,1,2,'admin');
        SELECT setval(pg_get_serial_sequence('shop_admins','id'),3);
        UPDATE shop_admins SET role='owner' WHERE id=2;
        INSERT INTO users(id,telegram_user_id,full_name,personal_qr_token)
            VALUES(4,1004,'Наш клієнт','our-customer'),
                  (5,1005,'Чужий клієнт','other-customer'),
                  (6,1006,'Неактивний','inactive-customer');
        INSERT INTO shop_clients(shop_id,user_id,last_activity_at)
            VALUES(1,4,NOW()),(2,5,NOW()),(1,6,NOW()-INTERVAL '61 days');
    """)
    database.query((Path(__file__).parents[1] / "migrations/20261004_admin_broadcast.sql").read_text())
    return database


@pytest.fixture
def owner_api(owner_database, api, monkeypatch):
    module = importlib.import_module("app.api.barista_owner")

    async def deliver(broadcast):
        return [row["user_id"] for row in broadcast["recipients"]], 0

    delivery = AsyncMock(side_effect=deliver)
    delivery.original = module.deliver_owner_broadcast
    monkeypatch.setattr(module, "deliver_owner_broadcast", delivery)
    return api[0], delivery


def preview(client, token, text="Привіт від нашої кав’ярні"):
    return client.post("/barista/owner/broadcast/preview", headers=bearer(token), json={"text": text})


def send(client, token, confirmation):
    return client.post("/barista/owner/broadcast", headers=bearer(token),
                       json={"confirmation_token": confirmation})


def test_owner_lists_only_selected_shop_and_localized_safe_member_data(owner_api, owner_database):
    client, _ = owner_api
    response = client.get("/barista/owner/members", headers=bearer(owner_database.staff_token()))
    assert response.status_code == 200
    data = response.json()
    assert data["shop"] == {"id": 1, "name": "Наші"}
    assert {item["membership_id"] for item in data["members"]} == {1, 3}
    assert {item["role"] for item in data["members"]} == {"owner", "admin"}
    assert all(set(item) == {"membership_id", "user_id", "name", "avatar", "role"}
               for item in data["members"])


@pytest.mark.parametrize("path", ["/barista/owner/members", "/barista/statistics"])
def test_client_tokens_and_missing_tokens_are_rejected(owner_api, path):
    client, _ = owner_api
    assert client.get(path).status_code == 401
    assert client.get(path, headers=bearer("legacy-active")).status_code == 401


def test_owner_removes_only_admin_and_preserves_shared_client_profile_sessions_balance(owner_api, owner_database):
    client, _ = owner_api
    database = owner_database
    seed_balance(database, cups=6, free=2)
    staff = database.staff_token(user_id=2, membership_id=3)
    customer = database.db.create_app_session(2)
    before = database.query("SELECT * FROM shop_clients WHERE shop_id=1 AND user_id=2")
    response = client.delete("/barista/owner/members/3", headers=bearer(database.staff_token()))
    assert response.status_code == 200
    assert response.json() == {"ok": True, "removed_membership_id": 3}
    assert database.query("SELECT id FROM shop_admins WHERE id=3") == []
    assert client.get("/barista/me", headers=bearer(staff)).status_code == 401
    assert database.db.get_app_session(customer)["user_id"] == 2
    assert database.query("SELECT * FROM shop_clients WHERE shop_id=1 AND user_id=2") == before
    assert database.query("SELECT id FROM users WHERE id=2")


@pytest.mark.parametrize("target,expected", [(1,409), (2,404), (900,404)])
def test_owner_cannot_remove_owner_cross_shop_or_missing_member(owner_api, owner_database, target, expected):
    client, _ = owner_api
    before = owner_database.query("SELECT * FROM shop_admins ORDER BY id")
    response = client.delete(f"/barista/owner/members/{target}", headers=bearer(owner_database.staff_token()))
    assert response.status_code == expected
    assert owner_database.query("SELECT * FROM shop_admins ORDER BY id") == before


def test_admin_cannot_use_owner_member_or_broadcast_endpoints(owner_api, owner_database):
    client, delivery = owner_api
    token = owner_database.staff_token(user_id=2, membership_id=3)
    assert client.get("/barista/owner/members", headers=bearer(token)).status_code == 403
    assert client.delete("/barista/owner/members/1", headers=bearer(token)).status_code == 403
    assert preview(client, token).status_code == 403
    assert send(client, token, "a" * 43).status_code == 403
    delivery.assert_not_awaited()


@pytest.mark.parametrize("days", [7,30])
def test_owner_real_statistics_are_scoped_and_do_not_invent_scan_or_period_gift_counts(owner_api, owner_database, days):
    client, _ = owner_api
    database = owner_database
    seed_balance(database, cups=3, free=1)
    database.query("""
        INSERT INTO transactions(shop_id,user_id,admin_user_id,type,cups_added,free_redeemed,created_at)
            VALUES(1,2,1,'add_cups',3,0,NOW()-INTERVAL '1 day'),
                  (1,2,1,'redeem_free',0,1,NOW()),
                  (2,5,3,'add_cups',999,0,NOW());
    """)
    response = client.get(f"/barista/statistics?days={days}", headers=bearer(database.staff_token()))
    assert response.status_code == 200
    data = response.json()
    assert (data["shop"]["id"], data["role"], data["days"]) == (1, "owner", days)
    assert data["metrics"]["cups_added"] == 3
    assert data["metrics"]["free_redeemed"] == 1
    assert data["metrics"]["add_operations"] == 1
    assert data["metrics"]["redeem_operations"] == 1
    assert data["metrics"]["scans"] is None and data["metrics"]["free_earned"] is None
    assert set(data["unavailable_metrics"]) == {"scans", "free_earned"}
    assert len(data["activity_by_weekday"]) == 7
    assert sum(item["cups_added"] for item in data["activity_by_weekday"]) == 3
    assert len(data["recent_actions"]) == 2


def test_admin_can_read_only_operational_seven_day_stats(owner_api, owner_database):
    client, _ = owner_api
    token = owner_database.staff_token(user_id=2, membership_id=3)
    response = client.get("/barista/statistics?days=7", headers=bearer(token))
    assert response.status_code == 200
    metrics = response.json()["metrics"]
    assert metrics["active_clients"] is None and metrics["returns_after_broadcast"] is None
    assert metrics["cups_added"] == 0
    assert client.get("/barista/statistics?days=30", headers=bearer(token)).status_code == 403
    assert client.get("/barista/statistics?days=8", headers=bearer(token)).status_code == 422
    assert client.get("/barista/statistics?days=7&shop_id=2", headers=bearer(token)).status_code == 422


def test_role_downgrade_and_deleted_membership_apply_to_every_owner_request(owner_api, owner_database):
    client, delivery = owner_api
    database = owner_database
    token = database.staff_token()
    database.query("UPDATE shop_admins SET role='admin' WHERE id=1")
    assert client.get("/barista/owner/members", headers=bearer(token)).status_code == 403
    assert preview(client, token).status_code == 403
    database.query("DELETE FROM shop_admins WHERE id=1")
    assert client.get("/barista/owner/members", headers=bearer(token)).status_code == 401
    delivery.assert_not_awaited()


def test_broadcast_preview_and_send_reuse_own_shop_active_recipient_list(owner_api, owner_database):
    client, delivery = owner_api
    database = owner_database
    token = database.staff_token()
    response = preview(client, token)
    assert response.status_code == 200
    body = response.json()
    assert body["recipients_count"] == 1
    assert body["shop"]["id"] == 1
    stored = database.query("SELECT * FROM admin_broadcast_previews")[0]
    assert stored["token_hash"] != body["confirmation_token"]
    assert stored["used_at"] is None
    response = send(client, token, body["confirmation_token"])
    assert response.status_code == 200
    assert response.json()["sent"] == 1
    assert delivery.await_args.args[0]["recipients"] == [{"telegram_user_id":1004,"user_id":4}]
    assert database.query("SELECT user_id,shop_id,type FROM touch_logs") == [
        {"user_id":4,"shop_id":1,"type":"broadcast"}]
    assert database.query("SELECT sender_user_id,shop_id,recipients_count FROM broadcasts") == [
        {"sender_user_id":1,"shop_id":1,"recipients_count":1}]
    assert send(client, token, body["confirmation_token"]).status_code == 409
    assert delivery.await_count == 1


def test_other_shop_other_session_changed_audience_or_expired_preview_never_send(owner_api, owner_database):
    client, delivery = owner_api
    database = owner_database
    token = database.staff_token()
    body = preview(client, token).json()
    assert send(client, database.staff_token(user_id=3,membership_id=2), body["confirmation_token"]).status_code == 403
    assert send(client, database.staff_token(), body["confirmation_token"]).status_code == 403
    database.query("UPDATE shop_clients SET last_activity_at=NOW() WHERE shop_id=1 AND user_id=6")
    response = send(client, token, body["confirmation_token"])
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "PREVIEW_STALE"
    fresh = preview(client, token).json()
    database.query("UPDATE admin_broadcast_previews SET created_at=NOW()-INTERVAL '20 minutes',expires_at=NOW()-INTERVAL '1 minute'")
    response = send(client, token, fresh["confirmation_token"])
    assert response.status_code == 409 and response.json()["detail"]["code"] == "PREVIEW_EXPIRED"
    delivery.assert_not_awaited()


def test_broadcast_request_cannot_inject_recipient_shop_identity_or_role(owner_api, owner_database):
    client, delivery = owner_api
    token = owner_database.staff_token()
    for field in ("shop_id", "user_id", "role", "recipients", "admin_user_id"):
        response = client.post("/barista/owner/broadcast/preview", headers=bearer(token),
                               json={"text":"Hello",field:2})
        assert response.status_code == 422
    assert client.post("/barista/owner/broadcast", headers=bearer(token),
                       json={"confirmation_token":"a"*43,"text":"override"}).status_code == 422
    assert preview(client, token, "   ").status_code == 422
    delivery.assert_not_awaited()


def test_two_concurrent_confirmations_attempt_delivery_only_once(owner_api, owner_database):
    client, delivery = owner_api
    token = owner_database.staff_token()
    confirmation = preview(client, token).json()["confirmation_token"]
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: send(client, token, confirmation), range(2)))
    assert sorted(result.status_code for result in results) == [200,409]
    assert delivery.await_count == 1
    assert len(owner_database.query("SELECT * FROM broadcasts")) == 1


def test_preview_expiring_while_confirmation_waits_for_row_lock_cannot_send(owner_api, owner_database):
    client, delivery = owner_api
    database = owner_database
    token = database.staff_token()
    confirmation = preview(client, token).json()["confirmation_token"]
    database.query("""UPDATE admin_broadcast_previews
        SET created_at=clock_timestamp()-INTERVAL '1 minute',
            expires_at=clock_timestamp()+INTERVAL '300 milliseconds'""")
    with ThreadPoolExecutor(max_workers=1) as executor:
        with database.db.get_connection() as locker:
            with locker.transaction():
                locker.execute("SELECT token_hash FROM admin_broadcast_previews FOR UPDATE")
                pending = executor.submit(send, client, token, confirmation)
                deadline = time.monotonic() + 3
                while True:
                    assert not pending.done(), "Confirmation did not wait for the preview lock"
                    waiting = database.query("""SELECT pid FROM pg_stat_activity
                        WHERE datname=current_database() AND pid<>pg_backend_pid()
                          AND wait_event_type='Lock'
                          AND query LIKE '%%SELECT *,media_bytes FROM admin_broadcast_previews%%'""")
                    if waiting:
                        break
                    assert time.monotonic() < deadline, "Confirmation never acquired the expected lock wait"
                    time.sleep(0.01)
                time.sleep(0.4)
        result = pending.result(timeout=5)
    assert result.status_code == 409
    assert result.json()["detail"]["code"] == "PREVIEW_EXPIRED"
    assert database.query("SELECT used_at FROM admin_broadcast_previews")[0]["used_at"] is None
    assert database.query("SELECT * FROM broadcasts") == []
    delivery.assert_not_awaited()


def test_native_broadcast_preserves_eight_per_week_quota(owner_api, owner_database):
    client, delivery = owner_api
    database = owner_database
    database.query("INSERT INTO broadcasts(shop_id,sender_user_id,text) SELECT 1,1,'prior' FROM generate_series(1,8)")
    token = database.staff_token()
    confirmation = preview(client, token).json()["confirmation_token"]
    response = send(client, token, confirmation)
    assert response.status_code == 429
    assert response.json()["detail"]["code"] == "BROADCAST_LIMIT_REACHED"
    delivery.assert_not_awaited()


def test_preview_expiring_while_audit_insert_waits_cannot_send(owner_api, owner_database):
    client, delivery = owner_api
    database = owner_database
    token = database.staff_token()
    confirmation = preview(client, token).json()["confirmation_token"]
    database.query("""UPDATE admin_broadcast_previews
        SET created_at=clock_timestamp()-INTERVAL '1 minute',
            expires_at=clock_timestamp()+INTERVAL '500 milliseconds'""")
    with ThreadPoolExecutor(max_workers=1) as executor:
        with database.db.get_connection() as locker:
            with locker.transaction():
                locker.execute("LOCK TABLE broadcasts IN SHARE MODE")
                pending = executor.submit(send, client, token, confirmation)
                deadline = time.monotonic() + 3
                while True:
                    assert not pending.done(), "Confirmation did not wait for the audit INSERT"
                    waiting = database.query("""SELECT pid FROM pg_stat_activity
                        WHERE datname=current_database() AND pid<>pg_backend_pid()
                          AND wait_event_type='Lock'
                          AND query LIKE '%%INSERT INTO broadcasts%%'""")
                    if waiting:
                        break
                    assert time.monotonic() < deadline, "Audit INSERT never reached the expected lock wait"
                    time.sleep(0.01)
                time.sleep(0.6)
        result = pending.result(timeout=5)
    assert result.status_code == 409
    assert result.json()["detail"]["code"] == "PREVIEW_EXPIRED"
    assert database.query("SELECT used_at FROM admin_broadcast_previews")[0]["used_at"] is None
    assert database.query("SELECT * FROM broadcasts") == []
    delivery.assert_not_awaited()


def test_plain_text_delivery_uses_only_saved_recipient_ids_and_closes_bot_session(owner_api, monkeypatch):
    module = importlib.import_module("app.api.barista_owner")
    # Call the actual adapter, replacing only Telegram/config dependencies.
    bot = SimpleNamespace(send_message=AsyncMock(side_effect=[None, RuntimeError("offline failure")]),
                          session=SimpleNamespace(close=AsyncMock()))
    monkeypatch.setitem(sys.modules, "app.config", SimpleNamespace(BOT_TOKEN="offline-token"))
    monkeypatch.setattr(module, "Bot", lambda token:bot)
    touched, failed = asyncio.run(owner_api[1].original({
        "text":"<b>Ordinary text</b>",
        "recipients":[{"user_id":4,"telegram_user_id":1004},{"user_id":6,"telegram_user_id":1006}],
    }))
    assert touched == [4] and failed == 1
    assert [call.kwargs for call in bot.send_message.await_args_list] == [
        {"chat_id":1004,"text":"<b>Ordinary text</b>"},
        {"chat_id":1006,"text":"<b>Ordinary text</b>"},
    ]
    bot.session.close.assert_awaited_once()
