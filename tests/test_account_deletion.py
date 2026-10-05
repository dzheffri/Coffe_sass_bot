"""Shared-account deletion against the guarded local PostgreSQL schema only."""

import importlib
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import HTTPException

from conftest import PROJECT, bearer
from test_barista_operations import operations_database


@pytest.fixture
def deletion_database(operations_database):
    db = operations_database
    db.query("""
        INSERT INTO shop_admins(id,shop_id,user_id,role) VALUES(3,1,2,'admin');
        SELECT setval(pg_get_serial_sequence('shop_admins','id'),3);
        SELECT setval(pg_get_serial_sequence('users','id'),3);
        CREATE TABLE wallet_passes(user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
            serial_number TEXT UNIQUE NOT NULL);
        CREATE TABLE wallet_device_registrations(serial_number TEXT NOT NULL,push_token TEXT NOT NULL);
        CREATE TABLE app_push_devices(user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,device_token TEXT);
        CREATE TABLE telegram_link_sessions(token_hash TEXT PRIMARY KEY,user_id BIGINT,current_user_id BIGINT,
            telegram_user_id BIGINT,provider TEXT,provider_user_id TEXT);
        CREATE TABLE admin_login_tickets(telegram_user_id BIGINT,ticket TEXT);
        CREATE TABLE broadcasts(id BIGSERIAL PRIMARY KEY,shop_id BIGINT REFERENCES coffee_shops(id),
            sender_user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,text TEXT);
        CREATE TABLE reminder_logs(id BIGSERIAL PRIMARY KEY,shop_id BIGINT REFERENCES coffee_shops(id),
            user_id BIGINT REFERENCES users(id) ON DELETE CASCADE);
        INSERT INTO users(telegram_user_id,full_name,personal_qr_token)
            VALUES(1004,'Інший клієнт','other-client-qr');
        INSERT INTO user_identities(user_id,provider,provider_user_id)
            VALUES(4,'apple','other-client-apple');
        INSERT INTO shop_clients(shop_id,user_id,cups,free_coffee_balance)
            VALUES(1,2,6,2),(1,4,4,1);
        INSERT INTO wallet_passes VALUES(2,'deleted-wallet'),(4,'other-wallet');
        INSERT INTO wallet_device_registrations VALUES('deleted-wallet','deleted-push'),('other-wallet','other-push');
        INSERT INTO app_push_devices VALUES(2,'deleted-device'),(4,'other-device');
        INSERT INTO telegram_link_sessions VALUES
            ('own-user-link',2,NULL,NULL,'google','google-client'),
            ('own-current-link',NULL,2,NULL,'apple','pending-apple'),
            ('own-identity-link',NULL,NULL,NULL,'google','google-client'),
            ('other-link',4,NULL,1004,'google','other-client');
        INSERT INTO transactions(shop_id,user_id,admin_user_id,type,cups_added)
            VALUES(1,2,1,'add_cups',2),(1,4,2,'add_cups',3);
        INSERT INTO touch_logs(shop_id,user_id,type) VALUES(1,2,'auto');
        INSERT INTO return_logs(shop_id,user_id,touch_log_id,touch_type) VALUES(1,2,1,'auto');
        INSERT INTO broadcasts(shop_id,sender_user_id,text) VALUES(1,2,'Текст кав’ярні');
        INSERT INTO reminder_logs(shop_id,user_id) VALUES(1,2);
    """)
    return db


def module():
    return importlib.import_module("app.account_deletion")


def delete(client, token, body=None):
    return client.request("DELETE", "/barista/account", headers=bearer(token),
                          json={} if body is None else body)


def test_delete_requires_actual_staff_session_and_does_not_accept_client_token(deletion_database, api):
    client, _ = api
    before = deletion_database.query("SELECT * FROM users ORDER BY id")
    for headers in ({}, bearer("invalid"), bearer("legacy-active")):
        assert client.get("/barista/account/deletion", headers=headers).status_code == 401
        assert client.request("DELETE", "/barista/account", headers=headers, json={}).status_code == 401
    assert deletion_database.query("SELECT * FROM users ORDER BY id") == before


@pytest.mark.parametrize("key", ["user_id", "admin_user_id", "employee_id", "shop_id", "role", "owner_id"])
def test_request_cannot_delete_another_user_or_supply_authority(deletion_database, api, key):
    client, _ = api
    response = delete(client, deletion_database.staff_token(2, 3), {key: 1})
    assert response.status_code == 422
    assert deletion_database.query("SELECT id FROM users ORDER BY id") == [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]


def test_admin_deletion_removes_shared_profile_balances_identities_all_access_but_preserves_business(deletion_database, api):
    db = deletion_database
    client, _ = api
    staff = db.staff_token(2, 3)
    another_staff = db.staff_token(2, 3)
    customer = db.db.create_app_session(2)
    foreign_staff = db.staff_token()
    unrelated = db.query("SELECT * FROM shop_clients WHERE user_id=4")
    old_transactions = db.query("SELECT id,shop_id,type,cups_added,free_redeemed,created_at FROM transactions ORDER BY id")
    old_marketing = db.db.get_shop_marketing_efficiency(1, days=30)
    response = delete(client, staff)
    assert response.status_code == 200, response.text
    assert response.json() == {"ok": True, "status": "deleted", "apple_revocation": "not_applicable"}
    for table in ("users", "user_identities", "app_sessions", "shop_admins", "shop_clients",
                  "wallet_passes", "app_push_devices"):
        key = "id" if table == "users" else "user_id"
        assert db.query(f"SELECT * FROM {table} WHERE {key}=2") == []
    assert db.db.get_app_session(customer) is None
    assert client.get("/barista/me", headers=bearer(staff)).status_code == 401
    assert client.get("/barista/me", headers=bearer(another_staff)).status_code == 401
    assert client.get("/barista/me", headers=bearer(foreign_staff)).status_code == 200
    assert db.query("SELECT * FROM shop_clients WHERE user_id=4") == unrelated
    assert db.query("SELECT id,shop_id,type,cups_added,free_redeemed,created_at FROM transactions ORDER BY id") == old_transactions
    anonymous = db.query("SELECT * FROM users WHERE full_name='Видалений користувач'")[0]
    assert anonymous["id"] != 2 and anonymous["telegram_user_id"] is None and anonymous["username"] is None
    assert anonymous["personal_qr_token"] != "customer-qr"
    for table in ("shop_admins", "shop_clients", "user_identities", "app_sessions"):
        assert db.query(f"SELECT * FROM {table} WHERE user_id=%s", (anonymous["id"],)) == []
    assert db.query("SELECT user_id FROM transactions WHERE id=1")[0]["user_id"] == anonymous["id"]
    assert db.query("SELECT admin_user_id FROM transactions WHERE id=2")[0]["admin_user_id"] == anonymous["id"]
    assert db.query("SELECT user_id FROM return_logs")[0]["user_id"] == anonymous["id"]
    assert db.query("SELECT COUNT(*) AS count FROM return_logs")[0]["count"] == 1
    assert db.query("SELECT sender_user_id FROM broadcasts")[0]["sender_user_id"] == anonymous["id"]
    assert db.query("SELECT * FROM wallet_device_registrations WHERE serial_number='deleted-wallet'") == []
    assert db.query("SELECT token_hash FROM telegram_link_sessions") == [{"token_hash": "other-link"}]
    assert db.db.get_shop_marketing_efficiency(1, days=30) == old_marketing
    # A login that had looked up the old identity before deletion cannot issue
    # an opaque session referencing the removed users.id after commit.
    from psycopg.errors import ForeignKeyViolation
    with pytest.raises(ForeignKeyViolation):
        db.db.create_app_session(2)


def test_sole_owner_delete_closes_shop_without_removing_clients_or_history(deletion_database, api):
    db = deletion_database
    client, _ = api
    token = db.staff_token()
    staff = db.staff_token(2, 3)
    ordinary_client = db.db.create_app_session(2)
    db.query("INSERT INTO shop_clients(shop_id,user_id,cups,free_coffee_balance) VALUES(2,2,5,3)")
    clients_before = db.query("SELECT * FROM users WHERE id IN(2,4) ORDER BY id")
    identities_before = db.query("SELECT * FROM user_identities WHERE user_id IN(2,4) ORDER BY id")
    balances_before = db.query("SELECT * FROM shop_clients WHERE user_id IN(2,4) ORDER BY id")
    ledger_before = db.query("SELECT id,shop_id,type,cups_added,free_redeemed,created_at FROM transactions ORDER BY id")
    state = client.get("/barista/account/deletion", headers=bearer(token))
    assert state.status_code == 200
    assert state.json() == {"ok": True, "can_delete": True, "blocking_shops": [],
                            "closing_shops": [{"id": 1, "name": "Наші"}],
                            "apple_revocation": "manual_required"}
    response = delete(client, token)
    assert response.status_code == 200, response.text
    assert db.query("SELECT id,is_active FROM coffee_shops ORDER BY id") == [
        {"id": 1, "is_active": False}, {"id": 2, "is_active": True}]
    assert db.query("SELECT * FROM users WHERE id IN(2,4) ORDER BY id") == clients_before
    assert db.query("SELECT * FROM user_identities WHERE user_id IN(2,4) ORDER BY id") == identities_before
    assert db.query("SELECT * FROM shop_clients WHERE user_id IN(2,4) ORDER BY id") == balances_before
    assert db.query("SELECT id,shop_id,type,cups_added,free_redeemed,created_at FROM transactions ORDER BY id") == ledger_before
    assert db.query("SELECT * FROM shop_admins WHERE shop_id=1") == []
    assert db.query("SELECT * FROM users WHERE id=1") == []
    assert db.query("SELECT * FROM user_identities WHERE user_id=1") == []
    assert db.query("SELECT * FROM app_sessions WHERE user_id=1") == []
    assert db.db.get_app_session(ordinary_client) is not None
    assert client.get("/barista/me", headers=bearer(staff)).status_code == 401
    for path in ("/barista/scan", "/barista/add-cup", "/barista/redeem"):
        assert client.post(path, headers=bearer(staff), json={"qr_token": "coffee:customer-qr"}).status_code == 401
    assert db.query("SELECT * FROM wallet_passes WHERE user_id=2")
    assert db.query("SELECT * FROM wallet_device_registrations WHERE serial_number='other-wallet'")


def test_sole_owner_without_other_staff_still_can_delete(deletion_database, api):
    db = deletion_database
    db.query("DELETE FROM shop_admins WHERE id=3")
    response = api[0].get("/barista/account/deletion", headers=bearer(db.staff_token()))
    assert response.json()["can_delete"] is True
    assert response.json()["closing_shops"] == [{"id": 1, "name": "Наші"}]


def test_coowner_delete_keeps_shop_and_other_owner_active(deletion_database, api):
    db = deletion_database
    db.query("UPDATE shop_admins SET role='owner' WHERE id=3")
    other_token = db.staff_token(2, 3)
    token = db.staff_token()
    state = api[0].get("/barista/account/deletion", headers=bearer(token)).json()
    assert state["closing_shops"] == []
    assert delete(api[0], token).status_code == 200
    assert db.query("SELECT is_active FROM coffee_shops WHERE id=1") == [{"is_active": True}]
    assert db.query("SELECT user_id,role FROM shop_admins WHERE shop_id=1") == [{"user_id": 2, "role": "owner"}]
    assert api[0].get("/barista/me", headers=bearer(other_token)).status_code == 200


def test_global_owner_deletion_closes_all_sole_owned_shops_not_only_selected_context(deletion_database, api):
    db = deletion_database
    db.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(2,1,'owner')")
    state = api[0].get("/barista/account/deletion", headers=bearer(db.staff_token())).json()
    assert state["closing_shops"] == [{"id": 1, "name": "Наші"}, {"id": 2, "name": "Інша кав’ярня"}]
    response = delete(api[0], db.staff_token())
    assert response.status_code == 200
    assert db.query("SELECT is_active FROM coffee_shops ORDER BY id") == [{"is_active": False}, {"is_active": False}]
    assert db.query("SELECT * FROM shop_admins") == []
    assert db.query("SELECT id FROM users WHERE id IN(2,3,4) ORDER BY id") == [{"id": 2}, {"id": 3}, {"id": 4}]


def test_multishop_closure_preserves_coowned_and_unrelated_shop_memberships_sessions(deletion_database, api):
    db = deletion_database
    db.query("UPDATE shop_admins SET role='owner' WHERE id=3")
    db.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(2,1,'owner')")
    db.query("INSERT INTO coffee_shops(id,name) VALUES(3,'Чужа кав’ярня')")
    membership = db.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(3,3,'owner') RETURNING id")[0]["id"]
    other_coowner = db.staff_token(2, 3)
    other_staff = db.staff_token(3, 2)
    unrelated_staff_session = db.staff_token(3, membership)
    foreign_membership = db.query("SELECT * FROM shop_admins WHERE shop_id=3")
    foreign_user = db.query("SELECT * FROM users WHERE id=3")
    before_sessions = db.query("SELECT * FROM app_sessions WHERE user_id=2")
    state = api[0].get("/barista/account/deletion", headers=bearer(db.staff_token())).json()
    assert state["closing_shops"] == [{"id": 2, "name": "Інша кав’ярня"}]
    assert delete(api[0], db.staff_token()).status_code == 200
    assert db.query("SELECT id,is_active FROM coffee_shops ORDER BY id") == [
        {"id": 1, "is_active": True}, {"id": 2, "is_active": False}, {"id": 3, "is_active": True}]
    assert db.query("SELECT * FROM shop_admins WHERE shop_id=3") == foreign_membership
    assert db.query("SELECT * FROM users WHERE id=3") == foreign_user
    assert db.query("SELECT * FROM app_sessions WHERE user_id=2") == before_sessions
    assert api[0].get("/barista/me", headers=bearer(other_coowner)).status_code == 200
    assert api[0].get("/barista/me", headers=bearer(other_staff)).status_code == 401
    assert api[0].get("/barista/me", headers=bearer(unrelated_staff_session)).status_code == 200


def test_closed_shop_revokes_pending_invites_previews_and_only_scoped_staff_access(deletion_database, api):
    db = deletion_database
    db.query((PROJECT / "migrations/20261004_admin_invites.sql").read_text())
    db.query((PROJECT / "migrations/20261004_admin_broadcast.sql").read_text())
    db.query("UPDATE users SET telegram_user_id=2002 WHERE id=2")
    extra_membership = db.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(2,2,'admin') RETURNING id")[0]["id"]
    db.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(1,4,'admin')")
    db.query("INSERT INTO admin_login_tickets VALUES(2002,'multi-shop-ticket'),(1004,'closed-only-ticket')")
    staff_closed = db.staff_token(2, 3)
    staff_other = db.staff_token(2, extra_membership)
    retained_preview_session = db.session(staff_other)["id"]
    db.query("""
        INSERT INTO barista_admin_invites(shop_id,creator_user_id,creator_membership_id,code_hash,expires_at)
        VALUES(1,2,3,%s,NOW()+INTERVAL '1 day'),(2,2,%s,%s,NOW()+INTERVAL '1 day')
    """, ("a" * 64, extra_membership, "b" * 64))
    # A preview may outlive a context change. Closure must invalidate it even
    # when its session is currently selected to a different, still-active shop.
    db.query("""
        INSERT INTO admin_broadcast_previews(token_hash,shop_id,owner_user_id,session_id,text,
            recipients_hash,expires_at,media_kind,media_filename,media_mime,media_bytes)
        VALUES(%s,1,2,%s,'Фото',%s,NOW()+INTERVAL '1 hour','photo','photo.jpg','image/jpeg',%s)
    """, ("c" * 64, retained_preview_session, "d" * 64, b"photo"))
    client_sessions = db.query("SELECT * FROM app_sessions WHERE purpose='client' AND user_id<>1 ORDER BY id")
    assert delete(api[0], db.staff_token()).status_code == 200
    closed_invite = db.query("SELECT * FROM barista_admin_invites WHERE shop_id=1")[0]
    assert closed_invite["revoked_at"] is not None and closed_invite["closed_at"] is not None
    assert closed_invite["creator_membership_id"] is None
    assert db.query("SELECT revoked_at,closed_at FROM barista_admin_invites WHERE shop_id=2") == [
        {"revoked_at": None, "closed_at": None}]
    preview = db.query("SELECT invalidated_at,media_bytes FROM admin_broadcast_previews")[0]
    assert preview["invalidated_at"] is not None and preview["media_bytes"] is None
    assert db.query("SELECT ticket FROM admin_login_tickets") == [{"ticket": "multi-shop-ticket"}]
    assert db.query("SELECT * FROM app_sessions WHERE purpose='client' AND user_id<>1 ORDER BY id") == client_sessions
    assert api[0].get("/barista/me", headers=bearer(staff_closed)).status_code == 401
    assert api[0].get("/barista/me", headers=bearer(staff_other)).status_code == 200

def test_apple_linked_account_deleted_with_honest_manual_revocation_result(deletion_database, api, monkeypatch):
    db = deletion_database
    db.query("INSERT INTO user_identities(user_id,provider,provider_user_id) VALUES(2,'apple','own-apple')")
    # Existing Apple auth keeps no revocable tokens/code. Do not invent an Apple
    # request or claim revoked; TN3194 manual fallback applies to this account.
    response = delete(api[0], db.staff_token(2, 3))
    assert response.status_code == 200
    assert response.json()["apple_revocation"] == "manual_required"
    assert db.query("SELECT * FROM user_identities WHERE user_id=2") == []


def test_removed_or_revoked_membership_cannot_delete_account(deletion_database, api):
    db = deletion_database
    staff = db.staff_token(2, 3)
    db.query("UPDATE app_sessions SET revoked_at=NOW() WHERE token_hash=%s", (db.db._hash_barista_session_token(staff),))
    assert delete(api[0], staff).status_code == 401
    fresh = db.staff_token(2, 3)
    db.query("DELETE FROM shop_admins WHERE id=3")
    assert delete(api[0], fresh).status_code == 401
    assert db.query("SELECT id FROM users WHERE id=2")


def test_legacy_client_helper_reuses_owner_closure_and_business_anonymization(deletion_database):
    db = deletion_database
    assert db.db.delete_user_account(1)["status"] == "deleted"
    assert db.query("SELECT is_active FROM coffee_shops WHERE id=1") == [{"is_active": False}]
    assert db.query("SELECT * FROM users WHERE id=2")
    assert db.db.delete_user_account(2)["status"] == "deleted"
    assert db.query("SELECT COUNT(*) AS count FROM transactions")[0]["count"] == 2
    assert db.db.delete_user_account(2) == {"status": "user_not_found"}


def test_owner_closure_and_personal_deletion_roll_back_atomically_on_failure(deletion_database, monkeypatch):
    from psycopg.errors import UniqueViolation

    db = deletion_database
    db.staff_token()
    db.staff_token(2, 3)
    before = {table: db.query(f"SELECT * FROM {table} ORDER BY id")
              for table in ("coffee_shops", "users", "shop_admins", "user_identities",
                            "app_sessions", "shop_clients", "transactions")}
    monkeypatch.setattr(module().secrets, "token_urlsafe", lambda length: "customer-qr")
    with pytest.raises(UniqueViolation):
        module().delete_personal_account(1)
    assert {table: db.query(f"SELECT * FROM {table} ORDER BY id") for table in before} == before

def test_deletion_clears_only_own_non_fk_pending_owner_reference(deletion_database, api):
    db = deletion_database
    # Production's legacy registration field is deliberately not a users FK.
    db.query("ALTER TABLE coffee_shops ADD COLUMN pending_owner_telegram_id BIGINT")
    db.query("UPDATE users SET telegram_user_id=2002 WHERE id=2")
    db.query("UPDATE coffee_shops SET pending_owner_telegram_id=2002 WHERE id=1")
    db.query("UPDATE coffee_shops SET pending_owner_telegram_id=1003 WHERE id=2")
    db.query("INSERT INTO admin_login_tickets VALUES(2002,'own-ticket'),(1003,'other-ticket')")
    before = db.query("SELECT id,name FROM coffee_shops ORDER BY id")
    response = delete(api[0], db.staff_token(2, 3))
    assert response.status_code == 200
    assert db.query("SELECT id,pending_owner_telegram_id FROM coffee_shops ORDER BY id") == [
        {"id": 1, "pending_owner_telegram_id": None},
        {"id": 2, "pending_owner_telegram_id": 1003},
    ]
    assert db.query("SELECT id,name FROM coffee_shops ORDER BY id") == before
    assert db.query("SELECT ticket FROM admin_login_tickets") == [{"ticket": "other-ticket"}]


def test_two_coowners_deleting_simultaneously_close_shop_when_final_owner_leaves(deletion_database):
    db = deletion_database
    db.query("UPDATE shop_admins SET role='owner' WHERE id=3")
    tokens = [db.staff_token(), db.staff_token(2, 3)]
    start = threading.Barrier(2)

    def remove(user_id, token):
        start.wait(timeout=5)
        try:
            result = module().delete_personal_account(user_id, barista_token=token)
            return result["status"]
        except HTTPException as error:
            return error.detail["code"]

    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(lambda pair: remove(*pair), zip([1, 2], tokens)))
    assert results == ["deleted", "deleted"]
    assert db.query("SELECT * FROM shop_admins WHERE shop_id=1") == []
    assert db.query("SELECT is_active FROM coffee_shops WHERE id=1") == [{"is_active": False}]
    assert db.query("SELECT id FROM users WHERE id=4") == [{"id": 4}]
