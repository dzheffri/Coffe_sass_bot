"""Auth/session isolation, migration compatibility and live staff access."""

from datetime import datetime, timedelta, timezone
import hashlib

import pytest
from psycopg.errors import CheckViolation

from conftest import PROJECT, bearer


def login(client, provider="google", **extra):
    return client.post("/barista/auth/identity", json={
        "provider": provider, "id_token": "valid-staff", **extra,
    })


def test_migration_preserves_every_legacy_credential_and_state(database):
    after = database.query("SELECT * FROM app_sessions ORDER BY id")
    for old, migrated in zip(database.legacy_before, after, strict=True):
        assert {key: migrated[key] for key in old} == old
        assert migrated["purpose"] == "client"
        assert migrated["selected_membership_id"] is None
    # Existing client code that omits purpose must remain valid.
    database.query("INSERT INTO app_sessions(user_id,token_hash,expires_at) VALUES (1,%s,NOW()+INTERVAL '1 day')",
                   (database.db._hash_app_session_token("old-client-writer"),))
    assert database.session("old-client-writer")["purpose"] == "client"
    with database.db.get_connection() as connection:
        connection.execute((PROJECT / "migrations/20260930_barista_sessions.sql").read_text())
    assert database.session("legacy-active")["purpose"] == "client"


@pytest.mark.parametrize("path", ["/me", "/me/qr", "/me/shops", "/me/stats"])
def test_actual_client_routes_keep_working_after_migration(api, path):
    client, _ = api
    response = client.get(path, headers=bearer("legacy-active"))
    assert response.status_code == 200
    assert response.json()["user_id"] == 1


@pytest.mark.parametrize("provider,audience", [
    ("google", "nashi-google-ios"), ("apple", "com.nashi.barista"),
])
def test_identity_uses_verified_subject_existing_user_and_native_audience(api, database, provider, audience):
    client, calls = api
    response = login(client, provider)
    assert response.status_code == 200
    result = response.json()
    assert calls == [(provider, "valid-staff", audience)]
    assert result["employee"] == {"user_id": 1, "name": "Ольга"}
    assert result["selected_context"] == {"membership_id": 1, "shop_id": 1, "name": "Наші", "role": "owner"}
    assert result["token_type"] == "bearer"
    token = result["access_token"]
    session = database.session(token)
    assert len(token) >= 40
    assert session["token_hash"] != token
    assert session["purpose"] == "barista"
    assert session["selected_membership_id"] == 1
    assert result["session"]["idle_timeout_days"] == 30


@pytest.mark.parametrize("token,expected,status", [
    ("forged-subject", "INVALID_IDENTITY", 401),
    ("valid-unknown", "IDENTITY_NOT_LINKED", 403),
    ("valid-client", "STAFF_ACCESS_REQUIRED", 403),
])
def test_no_auto_creation_or_staff_elevation(api, database, token, expected, status):
    client, _ = api
    before = database.query("SELECT COUNT(*) AS n FROM app_sessions")[0]["n"]
    result = client.post("/barista/auth/identity", json={"provider": "google", "id_token": token})
    assert result.status_code == status
    assert result.json()["detail"]["code"] == expected
    assert database.query("SELECT COUNT(*) AS n FROM app_sessions")[0]["n"] == before
    assert database.query("SELECT COUNT(*) AS n FROM users")[0]["n"] == 3


@pytest.mark.parametrize("extra", [{"role": "owner"}, {"admin_user_id": 3},
                                   {"user_id": 1}, {"provider_user_id": "google-staff"}])
def test_identity_rejects_forged_authorization_fields(api, extra):
    client, calls = api
    response = login(client, **extra)
    assert response.status_code == 422
    assert calls == []


def test_provider_must_be_configured_not_fall_back_to_client_audience(api, monkeypatch):
    client, calls = api
    monkeypatch.delenv("BARISTA_GOOGLE_CLIENT_ID")
    response = login(client)
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "IDENTITY_PROVIDER_NOT_CONFIGURED"
    assert calls == []


def test_barista_issuance_is_disabled_until_all_workers_have_switched(api, database, monkeypatch):
    client, calls = api
    existing = database.staff_token()
    monkeypatch.delenv("BARISTA_LOGIN_ENABLED")
    response = login(client)
    assert response.status_code == 503
    assert calls == []
    # The rollout flag controls issuance; already valid sessions remain usable
    # and can be logged out while an operator prepares a rollback.
    assert client.get("/barista/me", headers=bearer(existing)).status_code == 200
    assert client.post("/barista/auth/logout", headers=bearer(existing)).status_code == 200


def test_legacy_hash_algorithm_is_unchanged_and_cannot_find_new_barista_tokens(database):
    client_token = database.db.create_app_session(1)
    barista_token = database.staff_token()
    legacy_digest = lambda token: hashlib.sha256(token.strip().encode("utf-8")).hexdigest()
    assert database.session(client_token)["token_hash"] == legacy_digest(client_token)
    assert database.session(barista_token)["token_hash"] != legacy_digest(barista_token)
    assert database.query("SELECT id FROM app_sessions WHERE token_hash=%s",
                          (legacy_digest(barista_token),)) == []
    assert database.query("SELECT id FROM app_sessions WHERE token_hash=%s",
                          (legacy_digest("nashi.barista.session.v1\0" + barista_token),)) == []


def test_multiple_shops_require_explicit_server_checked_context(api, database):
    client, _ = api
    second_membership = database.add_second_membership()
    response = login(client)
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "SHOP_CONTEXT_REQUIRED"
    assert {shop["shop_id"] for shop in response.json()["detail"]["shops"]} == {1, 2}
    assert login(client, shop_id=99).status_code == 403
    chosen = login(client, shop_id=2)
    assert chosen.status_code == 200
    assert chosen.json()["selected_context"]["membership_id"] == second_membership


def test_context_selection_is_persisted_and_returns_available_shops(api, database):
    client, _ = api
    second_membership = database.add_second_membership()
    token = database.staff_token()
    headers = bearer(token)
    response = client.post("/barista/context", headers=headers, json={"shop_id": 2})
    assert response.status_code == 200
    assert response.json()["selected_context"]["role"] == "admin"
    assert database.session(token)["selected_membership_id"] == second_membership
    me = client.get("/barista/me", headers=headers)
    assert me.json()["selected_context"]["shop_id"] == 2
    assert len(me.json()["shops"]) == 2


def test_forbidden_context_does_not_extend_or_change_session(api, database):
    client, _ = api
    token = database.staff_token()
    database.query("UPDATE app_sessions SET expires_at=NOW()+INTERVAL '2 days' WHERE token_hash=%s",
                   (database.db._hash_barista_session_token(token),))
    before = database.session(token)
    response = client.post("/barista/context", headers=bearer(token), json={"shop_id": 2})
    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "SHOP_ACCESS_DENIED"
    assert database.session(token) == before


def test_purpose_isolation_both_directions_on_all_routes(api, database):
    client, _ = api
    barista = database.staff_token()
    for path in ("/me", "/me/qr", "/me/shops", "/me/stats"):
        assert client.get(path, headers=bearer(barista)).status_code == 401
    assert client.post("/auth/logout", headers=bearer(barista)).status_code == 401
    assert client.get("/barista/me", headers=bearer("legacy-active")).status_code == 401
    assert client.post("/barista/context", headers=bearer("legacy-active"), json={"shop_id": 1}).status_code == 401
    assert client.post("/barista/auth/logout", headers=bearer("legacy-active")).status_code == 401
    assert database.db.get_app_session(barista) is None
    assert database.db.get_app_session("legacy-active") is not None
    assert database.session(barista)["revoked_at"] is None


def test_successful_activity_extends_same_token_to_thirty_days(api, database):
    client, _ = api
    token = database.staff_token()
    database.query("UPDATE app_sessions SET expires_at=NOW()+INTERVAL '1 day' WHERE token_hash=%s",
                   (database.db._hash_barista_session_token(token),))
    before = datetime.now(timezone.utc)
    response = client.get("/barista/me", headers=bearer(token))
    after = datetime.now(timezone.utc)
    assert response.status_code == 200
    session = database.session(token)
    assert before + timedelta(days=30) <= session["expires_at"] <= after + timedelta(days=30)
    assert before <= session["last_used_at"] <= after
    rendered_expiry = datetime.fromisoformat(response.json()["session"]["expires_at"].replace("Z", "+00:00"))
    assert rendered_expiry == session["expires_at"]


@pytest.mark.parametrize("condition", ["expired", "revoked"])
def test_dead_sessions_never_revive(api, database, condition):
    client, _ = api
    token = database.staff_token()
    update = "expires_at=NOW()-INTERVAL '1 second'" if condition == "expired" else "revoked_at=NOW()"
    database.query(f"UPDATE app_sessions SET {update} WHERE token_hash=%s",
                   (database.db._hash_barista_session_token(token),))
    before = database.session(token)
    for path, method, body in (("/barista/me", "get", None),
                               ("/barista/context", "post", {"shop_id": 1}),
                               ("/barista/auth/logout", "post", None)):
        kwargs = {"headers": bearer(token)}
        if body is not None:
            kwargs["json"] = body
        assert getattr(client, method)(path, **kwargs).status_code == 401
    assert database.session(token) == before


@pytest.mark.parametrize("token", ["legacy-expired", "legacy-revoked"])
def test_legacy_expiration_and_revocation_remain_enforced(api, token):
    client, _ = api
    assert client.get("/me", headers=bearer(token)).status_code == 401


def test_membership_deletion_removes_scoped_session_without_touching_client(api, database):
    client, _ = api
    database.add_second_membership()
    token = database.staff_token()
    client_token = database.db.create_app_session(1)
    database.query("DELETE FROM shop_admins WHERE id=1")
    # Another current staff membership cannot rescue the removed shop session.
    assert client.get("/barista/me", headers=bearer(token)).status_code == 401
    database.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES (1,1,'owner')")
    assert client.get("/barista/me", headers=bearer(token)).status_code == 401
    assert client.post("/barista/context", headers=bearer(token), json={"shop_id": 2}).status_code == 401
    assert client.get("/me", headers=bearer(client_token)).status_code == 200


def test_role_is_rechecked_on_every_request_and_failure_does_not_extend(api, database):
    client, _ = api
    token = database.staff_token()
    assert client.get("/barista/me", headers=bearer(token)).status_code == 200
    database.query("UPDATE shop_admins SET role='inactive' WHERE id=1")
    before = database.session(token)
    assert client.get("/barista/me", headers=bearer(token)).status_code == 403
    assert client.post("/barista/context", headers=bearer(token), json={"shop_id": 1}).status_code == 403
    assert database.session(token) == before


def test_logout_revokes_only_current_barista_session(api, database):
    client, _ = api
    token = database.staff_token()
    other_barista = database.staff_token()
    client_token = database.db.create_app_session(1)
    response = client.post("/barista/auth/logout", headers=bearer(token))
    assert response.json() == {"ok": True, "revoked": True}
    assert database.session(token)["revoked_at"] is not None
    assert client.get("/barista/me", headers=bearer(token)).status_code == 401
    assert client.get("/barista/me", headers=bearer(other_barista)).status_code == 200
    assert client.get("/me", headers=bearer(client_token)).status_code == 200


def test_direct_session_creation_cannot_use_another_employees_membership(database):
    with pytest.raises(ValueError, match="STAFF_ACCESS_REQUIRED"):
        database.staff_token(user_id=1, membership_id=2)
    with pytest.raises(ValueError):
        database.db.create_app_session(1, purpose="barista")
    with pytest.raises(ValueError):
        database.db.create_app_session(1, selected_membership_id=1)
    with pytest.raises(ValueError):
        database.db.create_app_session(1, purpose="admin")


def test_schema_enforces_purpose_and_context_constraints(database):
    with pytest.raises(CheckViolation):
        database.query("INSERT INTO app_sessions(user_id,token_hash,expires_at,purpose) VALUES (1,'invalid',NOW()+INTERVAL '1 day','barista')")
    with pytest.raises(CheckViolation):
        database.query("INSERT INTO app_sessions(user_id,token_hash,expires_at,purpose,selected_membership_id) VALUES (1,'invalid2',NOW()+INTERVAL '1 day','client',1)")


def test_revoke_helpers_preserve_default_client_scope_and_allow_explicit_staff_scope(database):
    client = database.db.create_app_session(1)
    barista = database.staff_token()
    assert database.db.revoke_app_session(barista) is False
    assert database.db.revoke_app_session(client) is True
    assert database.session(barista)["revoked_at"] is None
    assert database.db.revoke_all_user_sessions(1, purpose="barista") == 1
    assert database.session(barista)["revoked_at"] is not None


@pytest.mark.parametrize("header", [None, "Basic x", "Bearer", "Bearer ", "not-a-token"])
def test_bad_authorization_headers_are_rejected(api, header):
    client, _ = api
    response = client.get("/barista/me", headers={"Authorization": header} if header else {})
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
