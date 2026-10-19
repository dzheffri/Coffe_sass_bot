"""Local PostgreSQL owner onboarding, bearer isolation and one-use invite regression."""

import importlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient

from conftest import PROJECT, bearer, extract_definitions


@pytest.fixture
def onboarding_api(database, monkeypatch):
    monkeypatch.setenv("BARISTA_LOGIN_ENABLED", "true")
    monkeypatch.setenv("BARISTA_GOOGLE_CLIENT_ID", "nashi-google-ios")
    monkeypatch.setenv("BARISTA_APPLE_CLIENT_ID", "com.nashi.barista")
    monkeypatch.setenv("BARISTA_INVITE_PEPPER", "local-owner-onboarding-pepper-at-least-32-bytes")
    database.query((PROJECT / "migrations/20261004_admin_invites.sql").read_text())
    database.query((PROJECT / "migrations/20261017_owner_onboarding.sql").read_text())
    database.query("""ALTER TABLE coffee_shops ADD COLUMN pending_owner_telegram_id BIGINT;
        ALTER TABLE coffee_shops ADD COLUMN city TEXT;
        ALTER TABLE coffee_shops ADD COLUMN subtitle TEXT;
        ALTER TABLE coffee_shops ADD COLUMN address TEXT;
        ALTER TABLE coffee_shops ADD COLUMN work_from TEXT;
        ALTER TABLE coffee_shops ADD COLUMN work_to TEXT;
        ALTER TABLE coffee_shops ADD COLUMN instagram TEXT;
        ALTER TABLE coffee_shops ADD COLUMN description TEXT;
        ALTER TABLE coffee_shops ADD COLUMN logo_url TEXT;
        ALTER TABLE coffee_shops ADD COLUMN cover_url TEXT;
        CREATE TABLE subscriptions(id BIGSERIAL PRIMARY KEY,shop_id BIGINT REFERENCES coffee_shops(id),
            plan TEXT,status TEXT,expires_at TIMESTAMPTZ);
        CREATE TABLE shop_news(id BIGSERIAL PRIMARY KEY,shop_id BIGINT,title TEXT,price TEXT,image_url TEXT,sort_order INT);
        SELECT setval('users_id_seq',(SELECT MAX(id) FROM users));
        SELECT setval('coffee_shops_id_seq',(SELECT MAX(id) FROM coffee_shops));""")
    calls = []

    def verifier(provider):
        def verify(token, *, audience):
            calls.append((provider, token, audience))
            subject = {"google": {"owner": "google-staff", "known": "google-client",
                                  "new": "new-google-sub", "other": "other-google-sub"},
                       "apple": {"new": "new-apple-sub"}}[provider].get(token)
            return {"sub": subject} if subject else None
        return verify

    def actor(authorization: str | None = Header(default=None)):
        if authorization == "Bearer local-superadmin":
            return {"telegram_user_id": 9001}
        if authorization in ("Bearer local-owner", "Bearer local-admin"):
            return {"telegram_user_id": 1001}
        raise HTTPException(401)

    app = FastAPI()
    shop_module = importlib.import_module("app.api.shop_onboarding")
    namespace = {"app": app, "HTTPException": HTTPException, "Depends": importlib.import_module("fastapi").Depends,
                 "get_current_user": actor, "SUPERADMIN_TELEGRAM_IDS": {9001},
                 "get_connection": database.db.get_connection, "owner_metadata": shop_module.owner_metadata}
    extract_definitions(PROJECT / "app/api/main.py", {"_get_bearer_token_or_401", "require_superadmin"}, namespace)
    app.include_router(shop_module.build_shop_onboarding_router(require_superadmin=namespace["require_superadmin"]))
    extract_definitions(PROJECT / "app/api/main.py", {"superadmin_get_shops", "superadmin_get_shop"}, namespace)
    app.include_router(importlib.import_module("app.api.barista").build_barista_router(
        verify_google=verifier("google"), verify_apple=verifier("apple"),
        find_user=database.db.get_user_by_identity, parse_bearer=namespace["_get_bearer_token_or_401"],
    ))
    with TestClient(app) as client:
        yield client, calls


SUPERADMIN = bearer("local-superadmin")


def new_shop(client):
    response = client.post("/superadmin/shops", headers=SUPERADMIN, json={"name": "Нова кава", "city": "Київ"})
    assert response.status_code == 201, response.text
    return response.json()["shop"]


def new_invite(client, shop_id):
    response = client.post(f"/superadmin/shops/{shop_id}/owner-invite", headers=SUPERADMIN, json={})
    assert response.status_code == 201, response.text
    return response.json()["invite"]


def login(client, provider="google", token="new"):
    response = client.post("/barista/auth/identity", json={"provider": provider, "id_token": token})
    assert response.status_code == 200, response.text
    return response.json()


def claim(client, token, code, **extra):
    return client.post("/barista/auth/claim-owner", headers=bearer(token), json={"invite_code": code, **extra})


def test_shop_create_has_no_owner_ids_and_matches_legacy_trial(onboarding_api, database):
    client, _ = onboarding_api
    shop = new_shop(client)
    assert shop["owner_connected"] is shop["telegram_connected"] is False
    assert shop["owner_user_id"] is shop["owner_name"] is shop["owner_telegram_id"] is None
    assert shop["city"] == "Київ" and shop["is_active"] is True
    assert database.query("SELECT * FROM shop_admins WHERE shop_id=%s", (shop["shop_id"],)) == []
    assert database.query("SELECT pending_owner_telegram_id FROM coffee_shops WHERE id=%s", (shop["shop_id"],))[0]["pending_owner_telegram_id"] is None
    assert database.query("SELECT plan,status,expires_at>NOW() AS active FROM subscriptions WHERE shop_id=%s", (shop["shop_id"],)) == [{"plan": "trial", "status": "active", "active": True}]
    assert client.get(f"/superadmin/shops/{shop['shop_id']}", headers=SUPERADMIN).json()["shop"]["owner_connected"] is False


@pytest.mark.parametrize("token", [None, "local-owner", "local-admin"])
def test_shop_creation_and_owner_invitation_are_superadmin_only(onboarding_api, database, token):
    client, _ = onboarding_api
    headers = bearer(token) if token else {}
    for path, body in (("/superadmin/shops", {"name": "Denied"}), ("/superadmin/shops/1/owner-invite", {})):
        assert client.post(path, headers=headers, json=body).status_code == (403 if token else 401)
    assert len(database.query("SELECT * FROM coffee_shops")) == 2


@pytest.mark.parametrize("body", [{"name": " "}, {"name": "x", "user_id": 2},
                                  {"name": "x", "pending_owner_telegram_id": 1002}, {"name": "x", "role": "owner"}])
def test_create_rejects_blank_names_and_authorization_fields(onboarding_api, body):
    client, _ = onboarding_api
    assert client.post("/superadmin/shops", headers=SUPERADMIN, json=body).status_code == 422


def test_owner_code_seven_days_hash_only_and_regeneration_closes_previous(onboarding_api, database):
    client, _ = onboarding_api
    shop = new_shop(client)
    first = new_invite(client, shop["shop_id"])
    second = new_invite(client, shop["shop_id"])
    assert len(second["code"]) == 6 and second["code"].isascii() and second["code"].isdigit()
    rows = database.query("SELECT * FROM barista_admin_invites ORDER BY id")
    assert rows[0]["revoked_at"] is not None and rows[0]["closed_at"] is not None
    assert rows[1]["role"] == second["role"] == "owner"
    assert rows[1]["creator_user_id"] is rows[1]["creator_membership_id"] is None
    assert (rows[1]["expires_at"] - rows[1]["created_at"]).total_seconds() == 7 * 86400
    assert len(rows[1]["code_hash"]) == 64 and rows[1]["code_hash"] != second["code"]
    token = login(client)["access_token"]
    assert claim(client, token, first["code"]).status_code == 400
    assert claim(client, token, second["code"]).status_code == 200


@pytest.mark.parametrize("provider", ["google", "apple"])
def test_unknown_verified_identity_creates_only_internal_user_and_onboarding_session(onboarding_api, database, provider):
    client, calls = onboarding_api
    result = login(client, provider)
    assert result["onboarding_required"] is result["needs_owner_claim"] is True
    assert result["selected_context"] is None and result["shops"] == []
    assert result["session"]["purpose"] == "barista_onboarding"
    assert result["employee"]["telegram_user_id"] is None
    user_id = result["employee"]["user_id"]
    assert user_id > 3 and database.query("SELECT * FROM shop_admins WHERE user_id=%s", (user_id,)) == []
    assert database.query("SELECT provider FROM user_identities WHERE user_id=%s", (user_id,)) == [{"provider": provider}]
    token = result["access_token"]
    assert database.db.get_app_session(token) is None
    assert client.get("/barista/me", headers=bearer(token)).json()["needs_owner_claim"] is True
    assert client.post("/barista/context", headers=bearer(token), json={"shop_id": 1}).status_code == 401
    assert client.post("/barista/scan", headers=bearer(token), json={"qr_token": "owner-qr"}).status_code == 401
    assert client.post("/barista/owner/invites", headers=bearer(token), json={}).status_code == 401
    assert calls[-1][2] == ("nashi-google-ios" if provider == "google" else "com.nashi.barista")
    assert login(client, provider)["employee"]["user_id"] == user_id
    assert len(database.query("SELECT * FROM users")) == 4


def test_known_no_shop_identity_reuses_user_and_onboarding_logout_isolated(onboarding_api, database):
    client, _ = onboarding_api
    result = login(client, token="known")
    assert result["employee"]["user_id"] == 2 and result["needs_owner_claim"] is True
    assert len(database.query("SELECT * FROM users")) == 3
    assert client.post("/barista/auth/logout", headers=bearer(result["access_token"])).json() == {"ok": True, "revoked": True}
    assert client.get("/barista/me", headers=bearer(result["access_token"])).status_code == 401
    assert database.db.get_app_session("legacy-active") is not None


def test_claim_is_atomic_owner_membership_new_staff_session_and_repeat_login(onboarding_api, database):
    client, _ = onboarding_api
    shop = new_shop(client)
    invite = new_invite(client, shop["shop_id"])
    onboarding = login(client)
    result = claim(client, onboarding["access_token"], invite["code"])
    assert result.status_code == 200, result.text
    body = result.json()
    assert body["needs_owner_claim"] is body["onboarding_required"] is False
    assert body["selected_context"]["role"] == "owner" and body["selected_context"]["shop_id"] == shop["shop_id"]
    assert body["session"]["purpose"] == "barista"
    assert client.get("/barista/me", headers=bearer(onboarding["access_token"])).status_code == 401
    assert client.get("/barista/me", headers=bearer(body["access_token"])).status_code == 200
    row = database.query("SELECT * FROM barista_admin_invites WHERE id=%s", (invite["id"],))[0]
    assert row["used_by_user_id"] == onboarding["employee"]["user_id"] and row["used_at"] is not None
    assert row["closed_at"] is not None
    repeat = login(client)
    assert repeat["needs_owner_claim"] is False and repeat["selected_context"]["role"] == "owner"
    detail = client.get(f"/superadmin/shops/{shop['shop_id']}", headers=SUPERADMIN).json()["shop"]
    assert detail["owner_connected"] is True and detail["owner_user_id"] == onboarding["employee"]["user_id"]
    assert detail["telegram_connected"] is False and detail["owner_telegram_id"] is None
    listed = client.get("/superadmin/shops", headers=SUPERADMIN).json()["shops"]
    assert next(item for item in listed if item["shop_id"] == shop["shop_id"])["owner_connected"] is True
    assert client.post(f"/superadmin/shops/{shop['shop_id']}/owner-invite", headers=SUPERADMIN, json={}).status_code == 409
    pending = client.get(f"/superadmin/shops/{shop['shop_id']}", headers=SUPERADMIN).json()["shop"]
    # The owner code cannot be re-issued after claim; the connected owner stays
    # connected and Telegram remains an optional separate link.
    assert pending["owner_connected"] is True and pending["telegram_connected"] is False
    assert pending["owner_telegram_id"] is None
    other = login(client, token="other")
    assert claim(client, other["access_token"], invite["code"]).status_code == 400


@pytest.mark.parametrize("extra", [{"user_id": 1}, {"shop_id": 1}, {"role": "admin"}, {"telegram_id": 1001}])
def test_claim_rejects_caller_supplied_identity_shop_or_role(onboarding_api, extra):
    client, _ = onboarding_api
    result = login(client)
    assert claim(client, result["access_token"], "123456", **extra).status_code == 422


@pytest.mark.parametrize("code", ["12345", "1234567", "１２３４５６", 123456, True])
def test_claim_requires_six_ascii_digits(onboarding_api, code):
    client, _ = onboarding_api
    assert claim(client, login(client)["access_token"], code).status_code == 422


def test_owner_code_cannot_replace_legacy_owner_or_pending_telegram_assignment(onboarding_api, database):
    client, _ = onboarding_api
    assert client.post("/superadmin/shops/1/owner-invite", headers=SUPERADMIN, json={}).status_code == 409
    shop = new_shop(client)
    invitation = new_invite(client, shop["shop_id"])
    database.query("UPDATE coffee_shops SET pending_owner_telegram_id=123456 WHERE id=%s", (shop["shop_id"],))
    assert claim(client, login(client)["access_token"], invitation["code"]).status_code == 409
    assert client.post(f"/superadmin/shops/{shop['shop_id']}/owner-invite", headers=SUPERADMIN, json={}).status_code == 409
    assert database.query("SELECT * FROM shop_admins WHERE shop_id=%s", (shop["shop_id"],)) == []
    assert login(client, token="owner")["selected_context"]["role"] == "owner"


@pytest.mark.parametrize("state", ["expired", "closed", "inactive"])
def test_unavailable_codes_do_not_grant_access_or_consume_onboarding_session(onboarding_api, database, state):
    client, _ = onboarding_api
    shop = new_shop(client)
    invitation = new_invite(client, shop["shop_id"])
    if state == "expired":
        database.query("UPDATE barista_admin_invites SET created_at=NOW()-INTERVAL '8 days',expires_at=NOW()-INTERVAL '1 second'")
    elif state == "closed":
        database.query("UPDATE barista_admin_invites SET revoked_at=NOW(),closed_at=NOW()")
    else:
        database.query("UPDATE coffee_shops SET is_active=FALSE WHERE id=%s", (shop["shop_id"],))
    token = login(client)["access_token"]
    assert claim(client, token, invitation["code"]).status_code == 400
    assert client.get("/barista/me", headers=bearer(token)).status_code == 200
    assert database.query("SELECT * FROM shop_admins WHERE shop_id=%s", (shop["shop_id"],)) == []


def test_authenticated_claim_rate_limits_wrong_codes_and_preserves_session(onboarding_api, database):
    client, _ = onboarding_api
    token = login(client)["access_token"]
    for _ in range(5):
        assert claim(client, token, "999999").status_code == 400
    blocked = claim(client, token, "999999")
    assert blocked.status_code == 429 and int(blocked.headers["Retry-After"]) > 0
    assert client.get("/barista/me", headers=bearer(token)).status_code == 200
    assert len(database.query("SELECT * FROM shop_admins")) == 2


def test_existing_admin_invite_can_be_claimed_with_new_onboarding_session(onboarding_api, database):
    client, _ = onboarding_api
    invitation = client.post("/barista/owner/invites", headers=bearer(database.staff_token()), json={}).json()["invite"]
    response = claim(client, login(client)["access_token"], invitation["code"])
    assert response.status_code == 200
    assert response.json()["selected_context"]["role"] == "admin"
    assert database.query("SELECT role FROM barista_admin_invites WHERE id=%s", (invitation["id"],))[0]["role"] == "admin"


def test_two_claimants_get_exactly_one_owner_membership_and_one_consumption(onboarding_api, database):
    client, _ = onboarding_api
    shop = new_shop(client)
    invitation = new_invite(client, shop["shop_id"])
    tokens = [login(client)["access_token"], login(client, token="other")["access_token"]]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda token: claim(client, token, invitation["code"]).status_code, tokens))
    assert sorted(results) in ([200, 400], [200, 409])
    assert len(database.query("SELECT * FROM shop_admins WHERE shop_id=%s AND role='owner'", (shop["shop_id"],))) == 1
    assert len(database.query("SELECT * FROM app_sessions WHERE purpose='barista' AND user_id>3")) == 1


def test_migration_is_rerunnable_and_preserves_all_existing_credentials_and_memberships(onboarding_api, database):
    before = {table: database.query(f"SELECT * FROM {table} ORDER BY id") for table in ("users", "shop_admins", "app_sessions")}
    database.query((PROJECT / "migrations/20261017_owner_onboarding.sql").read_text())
    assert {table: database.query(f"SELECT * FROM {table} ORDER BY id") for table in before} == before


def test_regeneration_cannot_reactivate_a_previous_still_unexpired_code(onboarding_api, database, monkeypatch):
    client, _ = onboarding_api
    module = importlib.import_module("app.owner_onboarding")
    numbers = iter([123456, 123456, 654321])
    monkeypatch.setattr(module.secrets, "randbelow", lambda maximum: next(numbers))
    shop = new_shop(client)
    first = new_invite(client, shop["shop_id"])
    second = new_invite(client, shop["shop_id"])
    assert first["code"] == "123456" and second["code"] == "654321"
    token = login(client)["access_token"]
    assert claim(client, token, first["code"]).status_code == 400
    assert claim(client, token, second["code"]).status_code == 200


@pytest.mark.parametrize("scope", ["consumer", "staff", "revoked", "expired"])
def test_claim_rejects_wrong_purpose_or_dead_onboarding_bearer(onboarding_api, database, scope):
    client, _ = onboarding_api
    shop = new_shop(client)
    invitation = new_invite(client, shop["shop_id"])
    if scope == "consumer":
        token = "legacy-active"
    elif scope == "staff":
        token = database.staff_token()
    else:
        token = login(client)["access_token"]
        change = "revoked_at=NOW()" if scope == "revoked" else "expires_at=NOW()-INTERVAL '1 second'"
        database.query(f"UPDATE app_sessions SET {change} WHERE token_hash=%s", (database.db._hash_barista_session_token(token),))
    assert claim(client, token, invitation["code"]).status_code == 401
    assert database.query("SELECT * FROM shop_admins WHERE shop_id=%s", (shop["shop_id"],)) == []
    assert database.query("SELECT used_at FROM barista_admin_invites WHERE id=%s", (invitation["id"],))[0]["used_at"] is None


def test_claim_session_creation_failure_rolls_back_membership_code_and_revocation(onboarding_api, database, monkeypatch):
    client, _ = onboarding_api
    shop = new_shop(client)
    invitation = new_invite(client, shop["shop_id"])
    token = login(client)["access_token"]
    module = importlib.import_module("app.owner_onboarding")

    def unavailable(*args, **kwargs):
        raise RuntimeError("Local fixture session failure")

    monkeypatch.setattr(module, "create_app_session", unavailable)
    with pytest.raises(RuntimeError, match="Local fixture session failure"):
        claim(client, token, invitation["code"])
    assert database.query("SELECT * FROM shop_admins WHERE shop_id=%s", (shop["shop_id"],)) == []
    row = database.query("SELECT * FROM barista_admin_invites WHERE id=%s", (invitation["id"],))[0]
    assert row["used_at"] is row["closed_at"] is row["used_by_user_id"] is None
    assert database.session(token)["revoked_at"] is None


def test_onboarding_expiring_during_session_lock_wait_cannot_claim_owner(onboarding_api, database):
    client, _ = onboarding_api
    shop = new_shop(client)
    invitation = new_invite(client, shop["shop_id"])
    token = login(client)["access_token"]
    digest = database.db._hash_barista_session_token(token)
    database.query("UPDATE app_sessions SET expires_at=clock_timestamp()+INTERVAL '0.5 seconds' WHERE token_hash=%s", (digest,))
    started = threading.Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with database.db.get_connection() as blocker:
            with blocker.transaction():
                blocker.execute("SELECT id FROM app_sessions WHERE token_hash=%s FOR UPDATE", (digest,))
                def attempt():
                    started.set()
                    return claim(client, token, invitation["code"])
                future = pool.submit(attempt)
                assert started.wait(2)
                time.sleep(0.1)
                assert not future.done()
                # Hold the row past expiry, then release it before joining the worker.
                time.sleep(0.65)
        response = future.result(timeout=5)
    assert response.status_code == 401
    assert database.query("SELECT * FROM shop_admins WHERE shop_id=%s", (shop["shop_id"],)) == []
    assert database.query("SELECT used_at FROM barista_admin_invites WHERE id=%s", (invitation["id"],))[0]["used_at"] is None
