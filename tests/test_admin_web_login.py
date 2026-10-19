"""Local PostgreSQL pairing, live owner permissions and optional Telegram linking."""

import importlib
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import HTTPException
from pydantic import BaseModel

from conftest import PROJECT, bearer, extract_definitions


@pytest.fixture
def web_api(api, database, monkeypatch):
    client, _ = api
    monkeypatch.setenv("BARISTA_INVITE_PEPPER", "local-test-only-pepper-" * 3)
    database.query((PROJECT / "migrations/20261017_admin_web_login.sql").read_text())
    database.query("""CREATE TABLE telegram_link_sessions(
        token_hash VARCHAR(64) PRIMARY KEY,provider TEXT NOT NULL,provider_user_id TEXT NOT NULL,
        mode TEXT NOT NULL DEFAULT 'link',current_user_id BIGINT,status TEXT DEFAULT 'pending',
        telegram_user_id BIGINT,user_id INTEGER,personal_qr_token TEXT,created_at TIMESTAMPTZ DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL,confirmed_at TIMESTAMPTZ)""")
    module = importlib.import_module("app.api.admin_web")
    # Never import production config/db initialization in this fixture.
    monkeypatch.setattr(module, "db", database.db)
    monkeypatch.setattr(importlib.import_module("app.admin_telegram_link"), "db", database.db)
    def parse(value):
        if not value or not value.startswith("Bearer "):
            raise HTTPException(401)
        return value.removeprefix("Bearer ")
    client.app.include_router(module.build_admin_web_router(
        parse_bearer=parse, bot_username=lambda: "test_nashi_bot",
        shop_model=BaseModel, reminder_model=BaseModel, analytics_functions={},
    ))
    return client


def start(client):
    response = client.post("/auth/admin-web-login/start", json={})
    assert response.status_code == 201
    return response.json()


def poll(client, request):
    return client.post("/auth/admin-web-login/poll", json={
        "request_token": request["request_token"], "poll_token": request["poll_token"]})


def confirm(client, request, token, *, selector="code"):
    return client.post("/barista/web-login/confirm", headers=bearer(token),
                       json={"code_or_token": request[selector]})


def test_pairing_qr_code_are_short_lived_opaque_and_stored_as_hashes(web_api, database):
    request = start(web_api)
    row = database.query("SELECT *,expires_at-created_at AS ttl FROM admin_web_login_requests")[0]
    assert len(request["request_token"]) >= 40
    assert len(request["code"]) == 6 and request["code"].isdigit()
    assert request["request_token"] != request["poll_token"]
    assert row["ttl"].total_seconds() == 300
    assert row["token_hash"] != request["request_token"]
    assert request["code"] not in str(row)
    assert poll(web_api, request).json()["status"] == "pending"


@pytest.mark.parametrize("selector", ["code", "request_token"])
def test_owner_inspects_confirms_and_browser_consumes_once_without_telegram(web_api, database, selector):
    database.query("UPDATE users SET telegram_user_id=NULL WHERE id=1")
    native = database.staff_token()
    request = start(web_api)
    response = web_api.post("/barista/web-login/inspect", headers=bearer(native),
                            json={"code_or_token": request[selector]})
    assert response.status_code == 200
    assert response.json()["shop"] == {"shop_id": 1, "name": "Наші"}
    assert poll(web_api, request).json()["status"] == "pending"  # inspect != consent
    assert confirm(web_api, request, native, selector=selector).status_code == 200
    result = poll(web_api, request).json()
    assert result["status"] == "approved"
    assert result["actor"] == {"user_id": 1, "role": "owner"}
    assert result["selected_shop_id"] == 1
    headers = bearer(result["access_token"])
    assert web_api.get("/admin-web/shops", headers=headers).status_code == 200
    assert web_api.get("/me", headers=headers).status_code == 401
    assert web_api.get("/barista/me", headers=headers).status_code == 401
    assert poll(web_api, request).json()["status"] == "expired"
    assert confirm(web_api, request, native, selector=selector).status_code == 404


def test_qr_raw_token_and_short_code_resolve_exact_same_pending_request(web_api, database):
    native = database.staff_token()
    request = start(web_api)
    other = start(web_api)
    module = importlib.import_module("app.admin_web_login")
    before = database.query("SELECT * FROM admin_web_login_requests ORDER BY id")
    assert len(request["request_token"]) == 43
    assert (before[0]["expires_at"] - before[0]["created_at"]).total_seconds() == 300
    with database.db.get_connection() as connection:
        with connection.transaction():
            raw = module.find_request(connection, request["request_token"])
            short = module.find_request(connection, request["code"])
            different = module.find_request(connection, other["request_token"])
    assert raw["id"] == short["id"] == before[0]["id"]
    assert raw["id"] != different["id"]
    assert raw["token_hash"] == module.digest(request["request_token"])
    assert raw["code_hash"] == module.code_digest(request["code"])
    for selector in ("request_token", "code"):
        inspected = web_api.post("/barista/web-login/inspect", headers=bearer(native),
                                  json={"code_or_token": request[selector]})
        assert inspected.status_code == 200 and inspected.json()["status"] == "pending"
        assert poll(web_api, request).json()["status"] == "pending"
        assert poll(web_api, other).json()["status"] == "pending"
        assert database.query("SELECT * FROM admin_web_login_requests ORDER BY id") == before
        assert database.query("SELECT * FROM admin_web_sessions") == []
    assert confirm(web_api, request, native, selector="request_token").status_code == 200
    approved = poll(web_api, request)
    assert approved.status_code == 200 and approved.json()["status"] == "approved"
    assert poll(web_api, other).json()["status"] == "pending"
    rows = database.query("SELECT * FROM admin_web_login_requests ORDER BY id")
    assert rows[0]["approved_user_id"] == 1 and rows[0]["consumed_at"] is not None
    assert rows[1] == before[1]
    assert len(database.query("SELECT * FROM admin_web_sessions")) == 1


@pytest.mark.parametrize("state", ["expired", "approved", "consumed", "malformed", "unknown"])
def test_qr_raw_token_rejects_expired_used_and_invalid_requests_without_mutation(web_api, database, state):
    native = database.staff_token()
    request = start(web_api)
    selector = request["request_token"]
    if state == "expired":
        database.query("""UPDATE admin_web_login_requests SET expires_at=NOW()-INTERVAL '1 second',
            created_at=NOW()-INTERVAL '6 minutes'""")
    elif state in ("approved", "consumed"):
        assert confirm(web_api, request, native, selector="request_token").status_code == 200
        if state == "consumed":
            assert poll(web_api, request).json()["status"] == "approved"
    elif state == "malformed":
        selector += "!"
    elif state == "unknown":
        selector = "x" * 43
    before = database.query("SELECT * FROM admin_web_login_requests ORDER BY id")
    sessions = database.query("SELECT * FROM admin_web_sessions ORDER BY id")
    for action in ("inspect", "confirm"):
        response = web_api.post(f"/barista/web-login/{action}", headers=bearer(native),
                                json={"code_or_token": selector})
        assert response.status_code == 404
        assert response.json()["detail"]["code"] == "LOGIN_NOT_AVAILABLE"
    assert database.query("SELECT * FROM admin_web_login_requests ORDER BY id") == before
    assert database.query("SELECT * FROM admin_web_sessions ORDER BY id") == sessions


def test_poll_requires_browser_only_secret_not_just_qr(web_api, database):
    request = start(web_api)
    confirm(web_api, request, database.staff_token())
    other = {**request, "poll_token": "x" * 43}
    assert poll(web_api, other).status_code == 404
    assert poll(web_api, request).json()["status"] == "approved"


def test_simultaneous_browser_polls_issue_only_one_session(web_api, database):
    request = start(web_api)
    assert confirm(web_api, request, database.staff_token()).status_code == 200
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda _: poll(web_api, request).json()["status"], range(2)))
    assert sorted(responses) == ["approved", "expired"]
    assert database.query("SELECT COUNT(*) AS n FROM admin_web_sessions")[0]["n"] == 1


def test_current_barista_role_cannot_approve_or_invent_owner_role(web_api, database):
    request = start(web_api)
    admin = database.staff_token(user_id=3, membership_id=2)
    assert confirm(web_api, request, admin).status_code == 403
    response = web_api.post("/barista/web-login/confirm", headers=bearer(admin),
                            json={"code_or_token": request["code"], "role": "owner", "user_id": 1})
    assert response.status_code == 422
    assert poll(web_api, request).json()["status"] == "pending"


def test_expired_request_and_wrong_code_are_unavailable(web_api, database):
    request = start(web_api)
    database.query("UPDATE admin_web_login_requests SET expires_at=NOW()-INTERVAL '1 second',created_at=NOW()-INTERVAL '6 minutes'")
    assert confirm(web_api, request, database.staff_token()).status_code == 404
    assert poll(web_api, request).json()["status"] == "expired"


def test_guess_rate_limit_persists_on_invalid_code(web_api, database):
    native = database.staff_token()
    for _ in range(5):
        response = web_api.post("/barista/web-login/inspect", headers=bearer(native), json={"code_or_token": "000000"})
        assert response.status_code == 404
    assert web_api.post("/barista/web-login/inspect", headers=bearer(native),
                        json={"code_or_token": "000000"}).status_code == 429


def test_request_creation_rate_limited(web_api):
    for _ in range(10):
        start(web_api)
    assert web_api.post("/auth/admin-web-login/start", json={}).status_code == 429


def test_revoked_owner_access_and_shop_isolation_are_live(web_api, database):
    request = start(web_api)
    assert confirm(web_api, request, database.staff_token()).status_code == 200
    token = poll(web_api, request).json()["access_token"]
    assert web_api.post("/admin-web/context", headers=bearer(token), json={"shop_id": 2}).status_code == 403
    database.query("UPDATE shop_admins SET role='admin' WHERE id=1")
    assert web_api.get("/admin-web/shops", headers=bearer(token)).status_code == 403


def test_owner_removed_after_confirmation_cannot_finish_login(web_api, database):
    request = start(web_api)
    confirm(web_api, request, database.staff_token())
    database.query("UPDATE coffee_shops SET is_active=FALSE WHERE id=1")
    assert poll(web_api, request).status_code == 403


def test_migration_idempotent_and_new_tables_cascade_with_account(web_api, database):
    request = start(web_api)
    confirm(web_api, request, database.staff_token())
    poll(web_api, request)
    before = database.query("SELECT * FROM app_sessions ORDER BY id")
    database.query((PROJECT / "migrations/20261017_admin_web_login.sql").read_text())
    assert database.query("SELECT * FROM app_sessions ORDER BY id") == before
    database.query("DELETE FROM users WHERE id=1")
    assert database.query("SELECT * FROM admin_web_sessions") == []
    row = database.query("SELECT * FROM admin_web_login_requests")[0]
    assert row["approved_user_id"] is None


def test_telegram_link_requires_session_and_is_scoped_to_current_user(web_api, database):
    assert web_api.post("/barista/telegram-link/start", json={}).status_code == 401
    database.query("UPDATE users SET telegram_user_id=NULL WHERE id=1")
    token = database.staff_token()
    response = web_api.post("/barista/telegram-link/start", headers=bearer(token), json={})
    assert response.status_code == 201
    link = response.json()
    assert link["deep_link"].endswith("?start=link_" + link["token"])
    assert web_api.get("/barista/telegram-link/status/" + link["token"], headers=bearer(token)).json()["status"] == "pending"
    other = database.staff_token(user_id=3, membership_id=2)
    assert web_api.get("/barista/telegram-link/status/" + link["token"], headers=bearer(other)).status_code == 404
    # New link expires the previous one.
    assert web_api.post("/barista/telegram-link/start", headers=bearer(token), json={}).status_code == 201
    assert web_api.get("/barista/telegram-link/status/" + link["token"], headers=bearer(token)).json()["status"] == "expired"


def test_trusted_bot_confirmation_links_same_internal_owner_and_is_idempotent(web_api, database, monkeypatch):
    database.query("UPDATE users SET telegram_user_id=NULL WHERE id=1")
    native = database.staff_token()
    link = web_api.post("/barista/telegram-link/start", headers=bearer(native), json={}).json()
    extract_definitions(PROJECT / "app/db.py", {"link_user_identity", "merge_users"}, database.db.__dict__)
    module = importlib.import_module("app.telegram_link")
    monkeypatch.setattr(module, "get_connection", database.db.get_connection)
    monkeypatch.setattr(module, "get_user_by_identity", database.db.get_user_by_identity)
    assert module.confirm_telegram_link_session(link["token"], 12345)["ok"] is False  # not bound
    assert module.bind_telegram_link_session(link["token"], 12345)["ok"] is True
    assert module.confirm_telegram_link_session(link["token"], 54321)["ok"] is False
    result = module.confirm_telegram_link_session(link["token"], 12345)
    assert result["ok"] is True and result["user_id"] == 1
    assert module.confirm_telegram_link_session(link["token"], 12345)["ok"] is True
    assert database.query("SELECT user_id,role FROM shop_admins WHERE shop_id=1") == [{"user_id": 1, "role": "owner"}]
    assert database.db.get_admin_shop_and_role(12345)["role"] == "owner"
    assert web_api.get("/barista/telegram-link/status/" + link["token"], headers=bearer(native)).json()["status"] == "linked"


def test_bot_resolves_verified_telegram_identity_before_legacy_column(web_api, database):
    database.query("INSERT INTO user_identities(user_id,provider,provider_user_id) VALUES (1,'telegram','1003')")
    # Legacy column1003 belongs to user3; identity establishes user1 owner.
    assert database.db.get_admin_shop_and_role(1003)["role"] == "owner"
    assert database.db.get_admin_shop_and_role(1001)["role"] == "owner"  # legacy fallback
