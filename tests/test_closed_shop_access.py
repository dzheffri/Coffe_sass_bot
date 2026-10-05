"""Closed shops cannot retain staff operations, invitations or broadcasts."""

import asyncio
import importlib
import sys
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from conftest import bearer
from test_admin_invites import accept, invites_api, issue
from test_barista_operations import operations_api, operations_database, seed_balance
from test_barista_owner_tools import owner_api, owner_database, preview, send
from test_barista_session_races import wait_for_authorization_lock


def close_shop(database, shop_id=1):
    database.query("UPDATE coffee_shops SET is_active=FALSE WHERE id=%s", (shop_id,))


def test_staff_login_and_context_do_not_include_closed_shops(api, database):
    client, _ = api
    database.add_second_membership()
    close_shop(database)
    memberships = importlib.import_module("app.barista_sessions").get_barista_memberships(1)
    assert [item["shop_id"] for item in memberships] == [2]
    login = client.post("/barista/auth/identity", json={"provider":"google", "id_token":"valid-staff"})
    assert login.status_code == 200
    assert login.json()["selected_context"]["shop_id"] == 2
    assert [shop["shop_id"] for shop in login.json()["shops"]] == [2]
    token = login.json()["access_token"]
    denied = client.post("/barista/context", headers=bearer(token), json={"shop_id":1})
    assert denied.status_code == 403
    assert denied.json()["detail"]["code"] == "SHOP_ACCESS_DENIED"


@pytest.mark.parametrize("path", ["/barista/scan", "/barista/add-cup", "/barista/redeem"])
def test_closed_shop_operations_denied_without_balance_or_session_changes(operations_api, operations_database, path):
    client, added, redeemed = operations_api
    database = operations_database
    seed_balance(database, cups=6, free=1)
    token = database.staff_token()
    before = database.session(token)
    balances = database.query("SELECT * FROM shop_clients ORDER BY id")
    close_shop(database)
    result = client.post(path, headers=bearer(token), json={"qr_token":"coffee:customer-qr"})
    assert result.status_code == 403
    assert database.session(token) == before
    assert database.query("SELECT * FROM shop_clients ORDER BY id") == balances
    assert database.query("SELECT * FROM transactions") == []
    added.assert_not_awaited()
    redeemed.assert_not_awaited()


def test_closed_shop_members_and_broadcast_are_forbidden(owner_api, owner_database):
    client, delivery = owner_api
    database = owner_database
    token = database.staff_token()
    confirmation = preview(client, token).json()["confirmation_token"]
    close_shop(database)
    assert client.get("/barista/owner/members", headers=bearer(token)).status_code == 403
    assert client.delete("/barista/owner/members/3", headers=bearer(token)).status_code == 403
    assert client.get("/barista/statistics", headers=bearer(token)).status_code == 403
    assert preview(client, token).status_code == 403
    assert send(client, token, confirmation).status_code == 403
    assert database.query("SELECT * FROM broadcasts") == []
    assert database.query("SELECT used_at FROM admin_broadcast_previews")[0]["used_at"] is None
    assert database.query("SELECT id FROM shop_admins WHERE id=3")
    delivery.assert_not_awaited()


def test_closed_shop_cannot_create_or_accept_existing_invitation(invites_api, database):
    client, _ = invites_api
    token, invitation = issue(client, database)
    before = database.query("SELECT * FROM app_sessions ORDER BY id")
    close_shop(database)
    assert client.post("/barista/owner/invites", headers=bearer(token), json={}).status_code == 403
    assert client.get("/barista/owner/invites", headers=bearer(token)).status_code == 403
    assert client.delete(f"/barista/owner/invites/{invitation['id']}", headers=bearer(token)).status_code == 403
    result = accept(client, invitation["code"])
    assert result.status_code == 400
    assert result.json()["detail"]["code"] == "INVALID_INVITE"
    assert database.query("SELECT * FROM shop_admins WHERE shop_id=1 AND user_id=2") == []
    assert database.query("SELECT * FROM app_sessions ORDER BY id") == before
    assert database.query("SELECT used_at FROM barista_admin_invites")[0]["used_at"] is None


def test_shop_closure_wins_over_waiting_authorization(database):
    sessions = importlib.import_module("app.barista_sessions")
    token = database.staff_token()
    before = database.session(token)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with database.db.get_connection() as locker:
            with locker.transaction():
                locker.execute("UPDATE coffee_shops SET is_active=FALSE WHERE id=1")
                pending = executor.submit(sessions.resolve_barista_session, token)
                wait_for_authorization_lock(database, "FOR NO KEY UPDATE OF cs", pending)
        with pytest.raises(sessions.BaristaSessionError, match="STAFF_ACCESS_REQUIRED"):
            pending.result(timeout=3)
    assert database.session(token) == before


def test_already_claimed_broadcast_stops_pending_delivery_on_shop_closure(owner_api, owner_database, monkeypatch):
    module = importlib.import_module("app.api.barista_owner")

    async def first_delivery(**kwargs):
        close_shop(owner_database)

    bot = SimpleNamespace(send_message=AsyncMock(side_effect=first_delivery),
                          session=SimpleNamespace(close=AsyncMock()))
    monkeypatch.setitem(sys.modules, "app.config", SimpleNamespace(BOT_TOKEN="offline-token"))
    monkeypatch.setattr(module, "Bot", lambda token: bot)
    touched, failed = asyncio.run(owner_api[1].original({
        "shop_id":1, "text":"Shop-only message",
        "recipients":[{"user_id":4,"telegram_user_id":1004}, {"user_id":6,"telegram_user_id":1006}],
    }))
    assert touched == [4] and failed == 1
    bot.send_message.assert_awaited_once_with(chat_id=1004, text="Shop-only message")
    bot.session.close.assert_awaited_once()
