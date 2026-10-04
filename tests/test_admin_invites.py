"""Real isolated PostgreSQL invitation/auth/security/concurrency regression."""

import importlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from conftest import PROJECT, bearer, extract_definitions


@pytest.fixture
def invites_api(database, monkeypatch):
    monkeypatch.setenv("BARISTA_LOGIN_ENABLED", "true")
    monkeypatch.setenv("BARISTA_GOOGLE_CLIENT_ID", "nashi-google-ios")
    monkeypatch.setenv("BARISTA_APPLE_CLIENT_ID", "com.nashi.barista")
    monkeypatch.setenv("BARISTA_INVITE_PEPPER", "isolated-tests-only-pepper-32-bytes-minimum")
    database.query((PROJECT / "migrations/20261004_admin_invites.sql").read_text())
    database.query("INSERT INTO user_identities(user_id,provider,provider_user_id) VALUES(3,'google','google-other')")
    database.query("INSERT INTO user_identities(user_id,provider,provider_user_id) VALUES(2,'apple','apple-client')")
    calls = []

    def verifier(provider):
        def verify(token, *, audience):
            calls.append((provider, token, audience))
            subjects = {"google": {"owner": "google-staff", "client": "google-client",
                                   "other": "google-other", "unknown": "not-linked"},
                        "apple": {"owner": "apple-staff", "client": "apple-client"}}
            subject = subjects[provider].get(token)
            return {"sub": subject} if subject else None
        return verify

    namespace = {"HTTPException": HTTPException}
    extract_definitions(PROJECT / "app/api/main.py", {"_get_bearer_token_or_401"}, namespace)
    app = FastAPI()
    router = importlib.import_module("app.api.barista")
    app.include_router(router.build_barista_router(
        verify_google=verifier("google"), verify_apple=verifier("apple"),
        find_user=database.db.get_user_by_identity,
        parse_bearer=namespace["_get_bearer_token_or_401"],
    ))
    with TestClient(app) as client:
        yield client, calls


def issue(client, database):
    token = database.staff_token()
    response = client.post("/barista/owner/invites", headers=bearer(token), json={})
    assert response.status_code == 201, response.text
    return token, response.json()["invite"]


def accept(client, code, *, provider="google", identity="client", **extra):
    return client.post("/barista/auth/accept-invite", json={
        "provider": provider, "id_token": identity, "invite_code": code, **extra,
    })


def snapshots(database):
    return {table: database.query(f"SELECT * FROM {table} ORDER BY id")
            for table in ("users", "user_identities", "shop_admins", "app_sessions", "barista_admin_invites")}


def test_owner_creates_admin_only_one_use_24_hour_invite_without_plaintext(invites_api, database):
    client, _ = invites_api
    before = database.query("SELECT * FROM app_sessions WHERE purpose='client' ORDER BY id")
    token, invitation = issue(client, database)
    assert len(invitation["code"]) == 6 and invitation["code"].isascii() and invitation["code"].isdigit()
    row = database.query("SELECT * FROM barista_admin_invites")[0]
    assert row["role"] == invitation["role"] == "admin"
    assert row["creator_user_id"] == row["creator_membership_id"] == row["shop_id"] == 1
    assert row["code_hash"] != invitation["code"] and len(row["code_hash"]) == 64
    assert (row["expires_at"] - row["created_at"]).total_seconds() == 24 * 3600
    assert row["used_at"] is row["revoked_at"] is row["used_by_user_id"] is None
    listed = client.get("/barista/owner/invites", headers=bearer(token))
    assert listed.status_code == 200
    assert listed.json()["invites"] == [{key: value for key, value in invitation.items() if key != "code"}]
    assert "code_hash" not in listed.text and invitation["code"] not in listed.text
    assert database.query("SELECT * FROM app_sessions WHERE purpose='client' ORDER BY id") == before


@pytest.mark.parametrize("provider", ["google", "apple"])
def test_accept_reverifies_existing_identity_creates_admin_and_session_atomically(invites_api, database, provider):
    client, calls = invites_api
    _, invitation = issue(client, database)
    original = snapshots(database)
    result = accept(client, invitation["code"], provider=provider)
    assert result.status_code == 200
    assert calls[-1] == (provider, "client", "nashi-google-ios" if provider == "google" else "com.nashi.barista")
    body = result.json()
    assert body["employee"] == {"user_id": 2, "name": "Іван"}
    assert body["selected_context"]["role"] == "admin"
    assert body["selected_context"]["shop_id"] == 1
    session = database.session(body["access_token"])
    assert session["purpose"] == "barista" and session["user_id"] == 2
    membership = database.query("SELECT * FROM shop_admins WHERE id=%s", (session["selected_membership_id"],))[0]
    assert membership["user_id"] == 2 and membership["shop_id"] == 1 and membership["role"] == "admin"
    invite = database.query("SELECT * FROM barista_admin_invites WHERE id=%s", (invitation["id"],))[0]
    assert invite["used_at"] is not None and invite["used_by_user_id"] == 2
    assert invite["closed_at"] is not None and invite["revoked_at"] is None
    after = snapshots(database)
    assert after["users"] == original["users"] and after["user_identities"] == original["user_identities"]
    assert client.get("/barista/me", headers=bearer(body["access_token"])).status_code == 200
    assert database.db.get_app_session(body["access_token"]) is None


@pytest.mark.parametrize("state", ["wrong", "expired", "revoked", "used", "creator_removed", "creator_downgraded"])
def test_invalid_invites_never_create_membership_or_privileged_session(invites_api, database, state):
    client, _ = invites_api
    _, invitation = issue(client, database)
    code = invitation["code"]
    if state == "wrong":
        code = "999999" if code != "999999" else "999998"
    elif state == "expired":
        database.query("UPDATE barista_admin_invites SET created_at=NOW()-INTERVAL '25 hours',expires_at=NOW()-INTERVAL '1 hour'")
    elif state == "revoked":
        database.query("UPDATE barista_admin_invites SET revoked_at=NOW(),closed_at=NOW()")
    elif state == "used":
        database.query("UPDATE barista_admin_invites SET used_at=NOW(),closed_at=NOW(),used_by_user_id=3")
    elif state == "creator_removed":
        database.query("DELETE FROM shop_admins WHERE id=1")
    else:
        database.query("UPDATE shop_admins SET role='admin' WHERE id=1")
    before = snapshots(database)
    result = accept(client, code)
    assert result.status_code == 400 and result.json()["detail"]["code"] == "INVALID_INVITE"
    assert snapshots(database) == before


@pytest.mark.parametrize("extra", [{"user_id": 1}, {"shop_id": 2}, {"role": "owner"}, {"admin_user_id": 1}])
def test_accept_rejects_app_supplied_identity_shop_and_role(invites_api, database, extra):
    client, calls = invites_api
    _, invitation = issue(client, database)
    before = snapshots(database)
    assert accept(client, invitation["code"], **extra).status_code == 422
    assert snapshots(database) == before and calls == []


@pytest.mark.parametrize("code", ["12345", "1234567", "abcdef", "１２３４５６", 123456, True])
def test_accept_requires_exact_six_ascii_digits(invites_api, database, code):
    client, calls = invites_api
    before = snapshots(database)
    assert accept(client, code).status_code == 422
    assert snapshots(database) == before and calls == []


def test_unknown_identity_cannot_create_user_membership_session_or_attempt_record(invites_api, database):
    client, _ = invites_api
    _, invitation = issue(client, database)
    before = snapshots(database)
    result = accept(client, invitation["code"], identity="unknown")
    assert result.status_code == 403 and result.json()["detail"]["code"] == "IDENTITY_NOT_LINKED"
    assert snapshots(database) == before
    assert database.query("SELECT * FROM barista_invite_attempts") == []


def test_known_user_without_membership_needs_invite_but_existing_admin_and_owner_login(invites_api, database):
    client, _ = invites_api
    before = database.query("SELECT * FROM app_sessions ORDER BY id")
    result = client.post("/barista/auth/identity", json={"provider": "google", "id_token": "client"})
    assert result.status_code == 403 and result.json()["detail"]["code"] == "NEEDS_INVITE"
    assert database.query("SELECT * FROM app_sessions ORDER BY id") == before
    for identity, role in (("owner", "owner"), ("other", "admin")):
        response = client.post("/barista/auth/identity", json={"provider": "google", "id_token": identity})
        assert response.status_code == 200 and response.json()["selected_context"]["role"] == role


@pytest.mark.parametrize("method,path", [("post", "/barista/owner/invites"),
                                         ("get", "/barista/owner/invites"),
                                         ("delete", "/barista/owner/invites/1")])
def test_admin_and_client_tokens_cannot_manage_owner_invites(invites_api, database, method, path):
    client, _ = invites_api
    admin_token = database.staff_token(3, 2)
    request = getattr(client, method)
    kwargs = {"json": {}} if method == "post" else {}
    assert request(path, headers=bearer(admin_token), **kwargs).status_code == 403
    assert request(path, headers=bearer("legacy-active"), **kwargs).status_code == 401
    assert database.query("SELECT * FROM barista_admin_invites") == []


@pytest.mark.parametrize("extra", [{"shop_id": 2}, {"role": "owner"}, {"user_id": 2}])
def test_create_cannot_override_selected_shop_or_admin_role(invites_api, database, extra):
    client, _ = invites_api
    response = client.post("/barista/owner/invites", headers=bearer(database.staff_token()), json=extra)
    assert response.status_code == 422 and database.query("SELECT * FROM barista_admin_invites") == []


def test_only_current_shop_owner_can_list_and_revoke_invite(invites_api, database):
    client, _ = invites_api
    token, invitation = issue(client, database)
    database.query("UPDATE shop_admins SET role='owner' WHERE id=2")
    second_owner = database.staff_token(3, 2)
    listed = client.get("/barista/owner/invites", headers=bearer(second_owner))
    assert listed.status_code == 200 and listed.json()["invites"] == []
    assert client.delete(f"/barista/owner/invites/{invitation['id']}", headers=bearer(second_owner)).status_code == 404
    assert client.delete(f"/barista/owner/invites/{invitation['id']}", headers=bearer(token)).status_code == 200
    assert accept(client, invitation["code"]).status_code == 400


def test_one_invite_concurrently_accepted_by_two_users_only_issues_one_session(invites_api, database, monkeypatch):
    client, _ = invites_api
    _, invitation = issue(client, database)
    module = importlib.import_module("app.api.barista_invites")
    original = module.accept_invite
    entered = threading.Barrier(2)

    def synchronized(**kwargs):
        entered.wait(timeout=5)
        return original(**kwargs)

    monkeypatch.setattr(module, "accept_invite", synchronized)
    before = database.query("SELECT COUNT(*) AS n FROM app_sessions")[0]["n"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(accept, client, invitation["code"], identity=identity)
                   for identity in ("client", "other")]
        responses = [future.result(timeout=10) for future in futures]
    assert sorted(response.status_code for response in responses) == [200, 400]
    assert database.query("SELECT COUNT(*) AS n FROM app_sessions")[0]["n"] == before + 1
    assert database.query("SELECT COUNT(*) AS n FROM shop_admins WHERE shop_id=1 AND role='admin'")[0]["n"] == 1
    winner = next(response for response in responses if response.status_code == 200)
    assert database.query("SELECT used_by_user_id FROM barista_admin_invites")[0]["used_by_user_id"] == winner.json()["employee"]["user_id"]


def test_invite_expiring_during_row_lock_wait_never_creates_membership_or_session(invites_api, database):
    client, _ = invites_api
    _, invitation = issue(client, database)
    session_count = database.query("SELECT COUNT(*) AS n FROM app_sessions")[0]["n"]
    with ThreadPoolExecutor(max_workers=1) as pool:
        with database.db.get_connection() as locker:
            with locker.transaction():
                expires = locker.execute(
                    """UPDATE barista_admin_invites
                       SET expires_at=clock_timestamp()+INTERVAL '2 seconds'
                       WHERE id=%s RETURNING expires_at""", (invitation["id"],)
                ).fetchone()["expires_at"]
                future = pool.submit(accept, client, invitation["code"])
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    waiting = database.query(
                        """SELECT query_start FROM pg_stat_activity
                           WHERE datname=current_database() AND pid<>pg_backend_pid()
                             AND wait_event_type='Lock'
                             AND query LIKE '%%barista_admin_invites%%FOR UPDATE%%'"""
                    )
                    if waiting:
                        assert waiting[0]["query_start"] < expires
                        break
                    if future.done():
                        pytest.fail("Acceptance completed without waiting on the invitation lock")
                    time.sleep(0.01)
                else:
                    pytest.fail("Acceptance did not reach the invitation row lock")
                while not locker.execute("SELECT clock_timestamp()>%s AS expired", (expires,)).fetchone()["expired"]:
                    time.sleep(0.02)
        result = future.result(timeout=5)
    assert result.status_code == 400 and result.json()["detail"]["code"] == "INVALID_INVITE"
    assert database.query("SELECT id FROM shop_admins WHERE shop_id=1 AND user_id=2") == []
    assert database.query("SELECT COUNT(*) AS n FROM app_sessions")[0]["n"] == session_count
    row = database.query("SELECT used_at FROM barista_admin_invites WHERE id=%s", (invitation["id"],))[0]
    assert row["used_at"] is None


def test_session_creation_failure_rolls_back_membership_and_invite_consumption(invites_api, database, monkeypatch):
    client, _ = invites_api
    _, invitation = issue(client, database)
    before = snapshots(database)
    module = importlib.import_module("app.admin_invites")

    def fail(*args, **kwargs):
        raise RuntimeError("isolated session insertion failure")

    monkeypatch.setattr(module, "create_app_session", fail)
    with pytest.raises(RuntimeError, match="isolated session insertion failure"):
        accept(client, invitation["code"])
    assert snapshots(database) == before


def test_invite_expiring_during_membership_unique_wait_rolls_back_insert_but_keeps_attempts(invites_api, database):
    client, _ = invites_api
    _, invitation = issue(client, database)
    session_count = database.query("SELECT COUNT(*) AS n FROM app_sessions")[0]["n"]
    expires = database.query(
        """UPDATE barista_admin_invites SET expires_at=clock_timestamp()+INTERVAL '2 seconds'
           WHERE id=%s RETURNING expires_at""", (invitation["id"],)
    )[0]["expires_at"]
    with ThreadPoolExecutor(max_workers=1) as pool:
        with database.db.get_connection() as locker:
            # Roll back the contending insert after TTL. That permits acceptance
            # to insert its own membership, testing the final consumption gate.
            with locker.transaction(force_rollback=True):
                locker.execute("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(1,2,'admin')")
                future = pool.submit(accept, client, invitation["code"])
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    waiting = database.query(
                        """SELECT query_start FROM pg_stat_activity
                           WHERE datname=current_database() AND pid<>pg_backend_pid()
                             AND wait_event_type='Lock'
                             AND query LIKE '%%INSERT INTO shop_admins%%'"""
                    )
                    if waiting:
                        assert waiting[0]["query_start"] < expires
                        break
                    if future.done():
                        pytest.fail("Acceptance completed without waiting on the membership unique key")
                    time.sleep(0.01)
                else:
                    pytest.fail("Acceptance did not reach the membership insert wait")
                while not locker.execute("SELECT clock_timestamp()>%s AS expired", (expires,)).fetchone()["expired"]:
                    time.sleep(0.02)
        result = future.result(timeout=5)
    assert result.status_code == 400 and result.json()["detail"]["code"] == "INVALID_INVITE"
    assert database.query("SELECT id FROM shop_admins WHERE shop_id=1 AND user_id=2") == []
    assert database.query("SELECT COUNT(*) AS n FROM app_sessions")[0]["n"] == session_count
    assert database.query("SELECT used_at FROM barista_admin_invites")[0]["used_at"] is None
    assert sorted(row["attempts"] for row in database.query("SELECT * FROM barista_invite_attempts")) == [1, 1, 1, 1]


def test_guess_limit_is_persistent_shared_and_independent_per_user(invites_api, database):
    client, _ = invites_api
    for _ in range(5):
        assert accept(client, "999999").status_code == 400
    result = accept(client, "999999")
    assert result.status_code == 429 and result.json()["detail"]["code"] == "INVITE_RATE_LIMITED"
    assert 1 <= int(result.headers["Retry-After"]) <= 86400
    assert sorted(row["attempts"] for row in database.query("SELECT * FROM barista_invite_attempts")) == [5, 5, 5, 5]
    # Another verified user has an independent budget; repeated blocked-user
    # requests do not increment the shared global budget.
    assert accept(client, "999999", identity="other").status_code == 400
    assert sorted(row["attempts"] for row in database.query("SELECT * FROM barista_invite_attempts")) == [1, 1, 5, 5, 6, 6]
    database.query("UPDATE barista_invite_attempts SET window_started_at=NOW()-INTERVAL '16 minutes'")
    assert accept(client, "999999").status_code == 429  # Daily cap outlives the burst window.
    database.query("UPDATE barista_invite_attempts SET window_started_at=NOW()-INTERVAL '25 hours'")
    assert accept(client, "999999").status_code == 400


def test_concurrent_wrong_guesses_cannot_exceed_shared_user_budget(invites_api, database):
    client, _ = invites_api
    before = snapshots(database)
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(accept, client, "999999") for _ in range(8)]
        results = [future.result(timeout=10) for future in futures]
    assert sorted(result.status_code for result in results) == [400] * 5 + [429] * 3
    assert sorted(row["attempts"] for row in database.query("SELECT * FROM barista_invite_attempts")) == [5, 5, 5, 5]
    assert snapshots(database) == before


def test_missing_migration_returns_safe_unavailable_response(invites_api, database):
    client, _ = invites_api
    token = database.staff_token()
    database.query("DROP TABLE barista_invite_attempts,barista_admin_invites")
    for result in (
        client.get("/barista/owner/invites", headers=bearer(token)),
        client.post("/barista/owner/invites", headers=bearer(token), json={}),
        client.delete("/barista/owner/invites/1", headers=bearer(token)),
        accept(client, "123456"),
    ):
        assert result.status_code == 503
        assert result.json() == {"detail": {"code": "INVITES_NOT_CONFIGURED"}}


@pytest.mark.parametrize("scope", ["global", "global:day"])
def test_global_guard_and_missing_pepper_fail_closed(invites_api, database, monkeypatch, scope):
    client, _ = invites_api
    assert accept(client, "999999").status_code == 400
    module = importlib.import_module("app.admin_invites")
    digest = module._digest(module._pepper(), "invite-attempt:" + scope)
    database.query("UPDATE barista_invite_attempts SET attempts=100 WHERE scope_hash=%s", (digest,))
    assert accept(client, "999999", identity="other").status_code == 429
    monkeypatch.delenv("BARISTA_INVITE_PEPPER")
    before = snapshots(database)
    assert accept(client, "999999").status_code == 503
    assert client.post("/barista/owner/invites", headers=bearer(database.staff_token()), json={}).status_code == 503
    after = snapshots(database)
    assert after["users"] == before["users"] and after["shop_admins"] == before["shop_admins"]


def test_invite_migration_is_repeatable_without_rewriting_client_or_loyalty_data(invites_api, database):
    client, _ = invites_api
    _, invitation = issue(client, database)
    before = snapshots(database)
    database.query((PROJECT / "migrations/20261004_admin_invites.sql").read_text())
    assert snapshots(database) == before
