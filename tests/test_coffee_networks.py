"""Targeted local PostgreSQL tests for coffee-network ownership and data isolation."""

import hashlib
import importlib

import psycopg
import pytest
from fastapi import Header, HTTPException
from pydantic import BaseModel
from fastapi.testclient import TestClient

from conftest import PROJECT, bearer

pytest_plugins = ("test_owner_onboarding",)


@pytest.fixture
def network_api(onboarding_api, database, monkeypatch):
    client, _ = onboarding_api
    monkeypatch.setenv("BARISTA_INVITE_PEPPER", "local-network-invite-pepper-for-tests-only-32-bytes")
    database.query("""
        CREATE TABLE shop_clients (
          id BIGSERIAL PRIMARY KEY, shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
          user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          cups INTEGER NOT NULL DEFAULT 0, free_coffee_balance INTEGER NOT NULL DEFAULT 0,
          total_scans INTEGER NOT NULL DEFAULT 0, total_free_coffee_earned INTEGER NOT NULL DEFAULT 0,
          total_free_coffee_redeemed INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), UNIQUE(shop_id,user_id));
        CREATE TABLE transactions (
          id BIGSERIAL PRIMARY KEY, shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
          user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          admin_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          type TEXT NOT NULL CHECK(type IN ('add_cups','redeem_free')),
          cups_added INTEGER NOT NULL DEFAULT 0, free_redeemed INTEGER NOT NULL DEFAULT 0,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
        CREATE TABLE touch_logs (
          id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
          type TEXT NOT NULL CHECK(type IN ('auto','broadcast','service')), sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
        CREATE TABLE return_logs (
          id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
          touch_log_id BIGINT NOT NULL REFERENCES touch_logs(id) ON DELETE CASCADE,
          touch_type TEXT NOT NULL CHECK(touch_type IN ('auto','broadcast')), returned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE(shop_id,user_id,touch_log_id));
        CREATE TABLE broadcasts (
          id BIGSERIAL PRIMARY KEY, shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
          sender_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE, text TEXT NOT NULL,
          recipients_count INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
        CREATE TABLE admin_broadcast_previews (
          token_hash VARCHAR(64) PRIMARY KEY, shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
          owner_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          session_id BIGINT NOT NULL REFERENCES app_sessions(id) ON DELETE CASCADE,
          text TEXT NOT NULL, recipients_hash VARCHAR(64) NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), expires_at TIMESTAMPTZ NOT NULL,
          used_at TIMESTAMPTZ, invalidated_at TIMESTAMPTZ,
          media_kind TEXT, media_filename TEXT, media_mime TEXT, media_bytes BYTEA);
        SELECT setval('users_id_seq',(SELECT MAX(id) FROM users));
        SELECT setval('coffee_shops_id_seq',(SELECT MAX(id) FROM coffee_shops));
    """)
    database.query((PROJECT / "migrations/20261004_admin_invites.sql").read_text())
    database.query((PROJECT / "migrations/20261017_owner_onboarding.sql").read_text())
    database.query((PROJECT / "migrations/20261017_admin_web_login.sql").read_text())
    database.query((PROJECT / "migrations/20261013_broadcast_recipients.sql").read_text())
    migration = (PROJECT / "migrations/20261018_coffee_networks.sql").read_text()
    database.query(migration)
    network_router = importlib.import_module("app.api.networks")

    def parse_bearer(value):
        if not value or not value.startswith("Bearer "):
            raise HTTPException(401)
        return value[7:]

    def require_superadmin(authorization: str | None = Header(default=None)):
        if authorization != "Bearer local-superadmin":
            raise HTTPException(403)
        return {"telegram_user_id": 9001}

    client.app.include_router(network_router.build_superadmin_network_router(require_superadmin=require_superadmin))
    from app.barista_sessions import resolve_barista_session
    client.app.include_router(network_router.build_barista_network_router(
        authorize=resolve_barista_session, parse_bearer=parse_bearer))

    admin_web = importlib.import_module("app.api.admin_web")
    class ShopModel(BaseModel):
        pass
    class ReminderModel(BaseModel):
        pass
    client.app.include_router(admin_web.build_admin_web_router(
        parse_bearer=parse_bearer, bot_username=lambda: "nashi_test_bot",
        shop_model=ShopModel, reminder_model=ReminderModel, analytics_functions={}))
    yield client, database


def make_network(database, name="CoffeeIn", mode="PER_SHOP"):
    module = importlib.import_module("app.network_management")
    with database.db.get_connection() as connection:
        with connection.transaction():
            network = module.create_network(connection, name, loyalty_mode=mode)
    return network


def add_shop(database, name, network_id=None):
    row = database.query("""INSERT INTO coffee_shops(name,network_id) VALUES(%s,%s) RETURNING id""",
                         (name, network_id))[0]
    return row["id"]


def attach(database, network_id, shop_id):
    module = importlib.import_module("app.network_management")
    with database.db.get_connection() as connection:
        with connection.transaction():
            return module.attach_shop(connection, network_id, shop_id)


def test_additive_migration_is_idempotent_and_old_shops_stay_independent(network_api, database):
    _, _ = network_api
    database.query((PROJECT / "migrations/20261018_coffee_networks.sql").read_text())
    assert database.query("SELECT id,network_id FROM coffee_shops ORDER BY id") == [
        {"id": 1, "network_id": None}, {"id": 2, "network_id": None}
    ]
    assert database.query("SELECT loyalty_mode FROM coffee_networks") == []


def test_network_invite_claim_grants_all_points_but_shop_owner_stays_scoped(network_api, database):
    client, _ = network_api
    network = make_network(database)
    first, second, third = (add_shop(database, f"Point {index}") for index in range(1, 4))
    for point in (first, second, third):
        attach(database, network["id"], point)

    invitation = client.post(f"/superadmin/networks/{network['id']}/owner-invite",
                             headers=bearer("local-superadmin"), json={})
    assert invitation.status_code == 201, invitation.text
    invite = invitation.json()["invite"]
    onboarding = client.post("/barista/auth/identity", json={"provider": "google", "id_token": "new"}).json()
    from app.owner_onboarding import claim_invite
    access_token, session = claim_invite(token=onboarding["access_token"], code=invite["code"])
    assert session["networks"][0]["network_id"] == network["id"]
    assert {item["shop_id"] for item in session["memberships"]} >= {first, second, third}
    from app.barista_sessions import BaristaSessionError, resolve_barista_session
    for point in (first, second, third):
        assert resolve_barista_session(access_token, shop_id=point)["selected_membership_id"] > 0
    stats_response = client.get(f"/barista/owner/networks/{network['id']}/statistics",
                                headers=bearer(access_token))
    assert stats_response.status_code == 200 and stats_response.json()["network"]["network_id"] == network["id"]
    # An existing shop owner remains scoped to that one shop and does not inherit network access.
    owner_token = database.db.create_app_session(1, purpose="barista", selected_membership_id=1)
    owner_session = resolve_barista_session(owner_token)
    assert owner_session["networks"] == []
    with pytest.raises(BaristaSessionError):
        resolve_barista_session(owner_token, shop_id=second)
    # Owner-code is single-use: claiming the same plaintext code again fails.
    repeated_token = database.db.create_app_session(onboarding["employee"]["user_id"], purpose="barista_onboarding")
    from app.admin_invites import InviteError
    with pytest.raises(InviteError):
        claim_invite(token=repeated_token, code=invite["code"])

    # Browser session is still owner-only and returns only this user's networks.
    web_token = "local-network-web-session-token"
    database.query("""INSERT INTO admin_web_sessions(user_id,selected_membership_id,token_hash,expires_at)
        VALUES(%s,%s,%s,NOW()+INTERVAL '1 hour')""",
        (session["user_id"], session["selected_membership_id"],
         hashlib.sha256(("admin-web-session:" + web_token).encode()).hexdigest()))
    web_headers = bearer(web_token)
    web_networks = client.get("/admin-web/networks", headers=web_headers)
    assert web_networks.status_code == 200
    assert [item["network_id"] for item in web_networks.json()["networks"]] == [network["id"]]
    web_stats = client.get(f"/admin-web/networks/{network['id']}/statistics", headers=web_headers)
    assert web_stats.status_code == 200 and len(web_stats.json()["shops"]) == 3


def test_shared_loyalty_accumulates_across_points_and_redeems_at_other_point(network_api, database):
    network = make_network(database, mode="SHARED")
    first, second = add_shop(database, "Shared A"), add_shop(database, "Shared B")
    attach(database, network["id"], first); attach(database, network["id"], second)
    database.db.add_cups_for_shop_client(first, 2, 1, 3)
    earned = database.db.add_cups_for_shop_client(second, 2, 1, 4)
    assert earned["earned_free"] == 1
    assert database.db.get_shop_client_balance_by_user_id(first, 2)["free_coffee_balance"] == 1
    assert database.db.get_shop_client_balance_by_user_id(second, 2)["cups"] == 0
    redeemed = database.db.redeem_free_for_shop_client(second, 2, 1)
    assert redeemed["free_coffee_balance"] == 0
    assert database.query("SELECT shop_id,type,free_redeemed FROM transactions ORDER BY id")[-1] == {
        "shop_id": second, "type": "redeem_free", "free_redeemed": 1
    }
    assert database.query("SELECT free_coffee_balance FROM shop_clients WHERE shop_id=%s AND user_id=2", (first,))[0]["free_coffee_balance"] == 0


def test_per_shop_loyalty_remains_independent_and_cross_shop_redeem_is_denied(network_api, database):
    network = make_network(database, mode="PER_SHOP")
    first, second = add_shop(database, "Separate A"), add_shop(database, "Separate B")
    attach(database, network["id"], first); attach(database, network["id"], second)
    database.db.add_cups_for_shop_client(first, 2, 1, 3)
    assert database.db.redeem_free_for_shop_client(second, 2, 1) == "NOT_FOUND"
    assert database.db.get_shop_client_balance_by_user_id(first, 2)["cups"] == 3
    assert database.db.get_shop_client_balance_by_user_id(second, 2) is None


def test_network_stats_are_aggregated_with_shop_breakdown_and_recipient_dedup(network_api, database):
    network = make_network(database)
    first, second, outside = add_shop(database, "Stats A"), add_shop(database, "Stats B"), add_shop(database, "Outside")
    attach(database, network["id"], first); attach(database, network["id"], second)
    database.query("INSERT INTO shop_clients(shop_id,user_id,cups) VALUES(%s,2,2),(%s,2,1),(%s,3,5)",
                   (first, second, outside))
    database.query("INSERT INTO transactions(shop_id,user_id,admin_user_id,type,cups_added) VALUES(%s,2,1,'add_cups',3),(%s,2,1,'add_cups',4),(%s,3,1,'add_cups',9)",
                   (first, second, outside))
    from app.network_management import network_dashboard
    with database.db.get_connection() as connection:
        with connection.transaction():
            dashboard = network_dashboard(connection, network["id"])
    assert dashboard["stats"]["clients"] == 1
    assert dashboard["stats"]["scans_today"] == 7
    assert len(dashboard["shops"]) == 2 and [shop["shop_id"] for shop in dashboard["shops"]] == [first, second]
    recipients = database.db.get_network_broadcast_recipients(network["id"])
    assert [row["user_id"] for row in recipients] == [2]
    from app.broadcast_publication import BroadcastPublication
    with database.db.get_connection() as connection:
        with connection.transaction():
            broadcast = BroadcastPublication().publish(connection, first, 1, "Network test", recipients,
                                                        scope="NETWORK", network_id=network["id"])
    assert broadcast["scope"] == "NETWORK" and broadcast["network_id"] == network["id"]
    assert database.query("SELECT user_id FROM broadcast_recipients WHERE broadcast_id=%s", (broadcast["id"],)) == [{"user_id": 2}]
    assert [row["user_id"] for row in database.db.get_broadcast_recipients(first)] == [2]


def test_network_owner_telegram_identity_is_recognized_by_internal_user_membership(network_api, database):
    _, _ = network_api
    network = make_network(database)
    first, second = add_shop(database, "Telegram A"), add_shop(database, "Telegram B")
    attach(database, network["id"], first); attach(database, network["id"], second)
    from app.network_management import create_network_owner_invite
    with database.db.get_connection() as connection:
        with connection.transaction():
            invite = create_network_owner_invite(connection, network["id"])
    from app.owner_onboarding import create_onboarding_identity, claim_invite
    user = create_onboarding_identity("google", "network-telegram-owner")
    session_token = database.db.create_app_session(user["id"], purpose="barista_onboarding")
    token, _ = claim_invite(token=session_token, code=invite["code"])
    from app.barista_sessions import resolve_barista_session
    session = resolve_barista_session(token)
    telegram_id = 991234
    database.query("UPDATE users SET telegram_user_id=%s WHERE id=%s", (telegram_id, user["id"]))
    from app.db import get_admin_shop_and_role
    owner = get_admin_shop_and_role(telegram_id)
    assert owner["role"] == "owner" and owner["id"] in (first, second)
    assert database.db.set_admin_active_shop(telegram_id, second) is True
    assert database.db.get_admin_shop_and_role(telegram_id)["id"] == second
    assert database.db.set_admin_active_shop(telegram_id, 999999) is False
    assert database.db.get_admin_shop_and_role(telegram_id)["id"] == second
    assert session["networks"][0]["network_id"] == network["id"]
