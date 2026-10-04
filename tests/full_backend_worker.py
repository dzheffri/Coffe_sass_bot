"""Real backend import/security worker, executed only by guarded local pytest.

No backend function extraction and no production environment loading. Imports
execute the actual DB/startup code against a disposable PostgreSQL schema;
external Python sockets and production filesystem locations are blocked.
"""

import base64
import asyncio
import hashlib
import hmac
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import time
from types import SimpleNamespace
from urllib.parse import urlencode
from unittest.mock import AsyncMock

from psycopg.conninfo import conninfo_to_dict


def _guard_environment(source, phase, schema, audit_root):
    dsn = os.environ["TEST_DATABASE_URL"]
    values = conninfo_to_dict(dsn)
    expected_socket = audit_root.parent / "socket"
    assert values.get("dbname") == "barista_auth_tests"
    assert values.get("user") == "barista_test"
    assert values.get("host") == str(expected_socket)
    assert values.get("port") == "55439"
    assert schema.startswith("full_security_") and schema.isidentifier()
    os.environ.update({
        "DATABASE_URL": dsn + f" options='-c search_path={schema}'",
        "BOT_TOKEN": "123456789:" + "A" * 35,
        "SCANNER_URL": "https://invalid.example/local-only",
        "ADMIN_PANEL_URL": "https://invalid.example/local-only",
        "GOOGLE_CLIENT_ID": "offline-client.apps.googleusercontent.com",
        "BARISTA_GOOGLE_CLIENT_ID": "offline-server.apps.googleusercontent.com",
        "APPLE_CLIENT_ID": "offline.client.apple",
        "BARISTA_APPLE_CLIENT_ID": "com.nashi.barista",
        "PYTHON_DOTENV_DISABLED": "1",
    })
    import dotenv
    dotenv.load_dotenv = lambda *args, **kwargs: False
    original_connect = socket.socket.connect

    def local_only(sock, address):
        if sock.family != socket.AF_UNIX or not str(address).startswith(str(expected_socket)):
            raise RuntimeError("External network disabled in full-backend security tests")
        return original_connect(sock, address)

    socket.socket.connect = local_only
    uploads = audit_root / "uploads"
    original_makedirs = os.makedirs

    def redirected_makedirs(path, *args, **kwargs):
        return original_makedirs(uploads if str(path) == "/data/uploads" else path,
                                 *args, **kwargs)

    os.makedirs = redirected_makedirs
    from fastapi.staticfiles import StaticFiles
    original_static_init = StaticFiles.__init__

    def redirected_static(self, *args, **kwargs):
        if str(kwargs.get("directory")) == "/data/uploads":
            kwargs["directory"] = uploads
        original_static_init(self, *args, **kwargs)

    StaticFiles.__init__ = redirected_static
    original_sqlite = sqlite3.connect
    sqlite3.connect = lambda path, *args, **kwargs: original_sqlite(
        audit_root / f"{phase}.sqlite" if str(path).endswith("web_panel.db") else path,
        *args, **kwargs)
    sys.path.insert(0, str(source))


def worker(source, phase, schema, audit_root):
    _guard_environment(source, phase, schema, audit_root)
    # Import order is intentional: run imports bot and then the actual API.
    import run
    from app.api import main
    from app import db
    from fastapi.testclient import TestClient

    assert run.app is main.app
    for name in ("verify_google_id_token", "verify_apple_id_token",
                 "get_user_by_identity", "_get_bearer_token_or_401"):
        assert callable(getattr(main, name)), name
    report = {"full_import_run_and_main": True}
    state_path = audit_root / "state.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {}

    def query(statement, values=()):
        with db.get_connection() as conn:
            cur = conn.execute(statement, values)
            return cur.fetchall() if cur.description else []

    def auth(token):
        return {"Authorization": "Bearer " + token}

    if phase == "legacy_setup":
        # The real baseline startup omits profile fields/table used by /me/shops.
        # These assumptions reproduce its query-compatible schema; they are not
        # a claim about live production and are not migration under test.
        for column in ("subtitle", "work_from", "work_to", "instagram",
                       "description", "logo_url", "cover_url"):
            query(f"ALTER TABLE coffee_shops ADD COLUMN IF NOT EXISTS {column} TEXT DEFAULT ''")
        query("""CREATE TABLE IF NOT EXISTS shop_news (
            id BIGSERIAL PRIMARY KEY, shop_id BIGINT REFERENCES coffee_shops(id) ON DELETE CASCADE,
            title TEXT DEFAULT '', price TEXT DEFAULT '', image_url TEXT DEFAULT '',
            sort_order INTEGER DEFAULT 0)""")
        query("""INSERT INTO users(id,telegram_user_id,username,full_name,personal_qr_token)
            VALUES (1,71001,'owner','Local owner','local-owner-qr'),
                   (2,71002,'attacker','Local attacker','local-attacker-qr'),
                   (3,71003,'admin','Local admin','local-admin-qr'),
                   (4,71004,'otherowner','Other owner','local-other-owner-qr'),
                   (32650,NULL,'guest','Guest','guest-qr')""")
        query("SELECT setval('users_id_seq',32650)")
        query("INSERT INTO coffee_shops(id,name) VALUES(1,'Local cafe'),(2,'Other cafe')")
        query("SELECT setval('coffee_shops_id_seq',2)")
        query("""INSERT INTO shop_admins(shop_id,user_id,role)
            VALUES(1,1,'owner'),(1,3,'admin'),(2,4,'owner')""")
        query("""INSERT INTO user_identities(user_id,provider,provider_user_id)
            SELECT id,'telegram',telegram_user_id::text FROM users
            WHERE telegram_user_id IS NOT NULL""")
        query("""INSERT INTO user_identities(user_id,provider,provider_user_id)
            VALUES(1,'google','honest-owner'),(2,'google','registered-attacker'),
                  (3,'google','honest-admin'),(4,'google','other-owner')""")
        query("INSERT INTO shop_clients(shop_id,user_id,cups,free_coffee_balance) VALUES(1,1,5,1)")
        query("UPDATE users SET active_shop_id=1 WHERE id=1")
        for token, days, revoked in (("legacy-active", 9, False),
                                     ("legacy-expired", -1, False),
                                     ("legacy-revoked", 9, True)):
            query("""INSERT INTO app_sessions(user_id,token_hash,expires_at,revoked_at)
                VALUES(1,%s,NOW()+(%s*INTERVAL '1 day'),
                       CASE WHEN %s THEN NOW() ELSE NULL END)""",
                  (db._hash_app_session_token(token), days, revoked))
        state["legacy_created_token"] = db.create_app_session(1)
        state["before"] = query("SELECT * FROM app_sessions ORDER BY id")
        report["legacy_real_startup_schema"] = True
    else:
        with TestClient(main.app) as client:
            for path in ("/me", "/me/qr", "/me/shops", "/me/stats"):
                response = client.get(path, headers=auth("legacy-active"))
                assert response.status_code == 200, (phase, path, response.text)
            for token in ("legacy-expired", "legacy-revoked"):
                assert client.get("/me", headers=auth(token)).status_code == 401
            assert db.get_app_session(state["legacy_created_token"])["user_id"] == 1
            report["legacy_client_routes_and_dead_sessions"] = True
            if phase == "current_security":
                _security_scenarios(main, db, client, query, auth, state, report)
            elif phase == "legacy_after_issuance":
                for path in ("/me", "/me/qr", "/me/shops", "/me/stats"):
                    response = client.get(path, headers=auth(state["barista_token"]))
                    assert response.status_code == 401, (path, response.text)
                assert db.get_app_session(state["barista_token"]) is None
                # An ASCII domain prefix must not transform a staff token into
                # an old plain-SHA256 client credential either.
                assert db.get_app_session(
                    "nashi.barista.session.v1\0" + state["barista_token"]
                ) is None
                report["unmodified_old_backend_rejects_barista"] = True
    state_path.write_text(json.dumps(state, default=str))
    print(json.dumps(report))


def _security_scenarios(main, db, client, query, auth, state, report):
    """Use real verifier signatures and HTTP dependencies; offline certificates."""
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding, rsa
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.public_key().public_bytes(serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    main.google_requests.Request = lambda: lambda *args, **kwargs: SimpleNamespace(
        status=200, data=json.dumps({"local-key": pem}).encode())

    def jwt(subject, *, barista=False):
        encode = lambda data: base64.urlsafe_b64encode(data).rstrip(b"=").decode()
        payload = {"iss": "https://accounts.google.com", "sub": subject,
            "aud": os.environ["BARISTA_GOOGLE_CLIENT_ID" if barista else "GOOGLE_CLIENT_ID"],
            "iat": int(time.time()) - 5, "exp": int(time.time()) + 600}
        signing = encode(json.dumps({"alg": "RS256", "kid": "local-key"}).encode())
        signing += "." + encode(json.dumps(payload).encode())
        return signing + "." + encode(key.sign(signing.encode(), padding.PKCS1v15(), hashes.SHA256()))

    def telegram_init_data(actor, *, forged=False):
        data = {"auth_date": str(int(time.time())),
                "user": json.dumps({"id": actor, "first_name": "Offline"})}
        check_string = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
        secret = hmac.new(b"WebAppData", os.environ["BOT_TOKEN"].encode(), hashlib.sha256).digest()
        data["hash"] = ("0" * 64 if forged else
            hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest())
        return urlencode(data)

    report["offline_google_jwt_verified"] = main.verify_google_id_token(jwt("honest-owner"))["sub"] == "honest-owner"
    assert main.validate_telegram_init_data(telegram_init_data(71001))["telegram_id"] == 71001
    assert main.validate_telegram_init_data(telegram_init_data(71001, forged=True)) is None

    owner_token = db.create_app_session(1)
    attacker_token = db.create_app_session(2)
    admin_token = db.create_app_session(3)

    _real_owner_analytics_security(
        main, db, client, query, auth, telegram_init_data,
        owner_token, attacker_token, admin_token, report,
    )

    # Normal check/login/create keep their established response contract.
    response = client.post("/auth/test-identity", json={
        "provider": "google", "id_token": jwt("honest-owner"), "action": "check"})
    assert response.status_code == 200 and response.json()["status"] == "existing", response.text
    assert client.get("/me", headers=auth(response.json()["access_token"])).status_code == 200
    response = client.post("/auth/test-identity", json={
        "provider": "google", "id_token": jwt("new-check-only"), "action": "check"})
    assert response.json()["status"] == "needs_onboarding", response.text
    assert db.get_user_by_identity("google", "new-check-only") is None
    response = client.post("/auth/test-identity", json={
        "provider": "google", "id_token": jwt("safe-created-client"), "action": "create"})
    assert response.json()["ok"] is True, response.text
    assert client.get("/me", headers=auth(response.json()["access_token"])).status_code == 200
    report["ordinary_identity_check_login_create_preserved"] = True

    # Previously an arbitrary Telegram ID attached the attacker's verified
    # provider identity to the owner. No proof of target ownership was supplied.
    response = client.post("/auth/test-identity", json={
        "provider": "google", "id_token": jwt("attacker-direct"),
        "action": "link_telegram", "telegram_id": 71001})
    assert response.status_code in (200, 400, 403), response.text
    assert response.json().get("ok") is not True, response.text
    assert db.get_user_by_identity("google", "attacker-direct") is None
    report["cannot_link_identity_to_foreign_telegram"] = True

    from app.telegram_link import bind_telegram_link_session, confirm_telegram_link_session
    response = client.post("/auth/unlink-identity", headers=auth(owner_token),
        json={"user_id": 1, "provider": "google"})
    assert response.json()["status"] == "unlinked", response.text
    response = client.post("/auth/telegram-link/start", json={
        "provider": "google", "id_token": jwt("safe-deeplink")})
    assert response.json()["status"] == "pending", response.text
    link_token = response.json()["token"]
    response = client.post("/auth/telegram-link/confirm", headers=auth(owner_token),
        json={"token": link_token})
    assert response.status_code == 403, response.text
    assert db.get_user_by_identity("google", "safe-deeplink") is None
    bound = bind_telegram_link_session(link_token, 71001)
    assert bound["ok"] is True, bound
    # Neither the raw target ID, a different client's valid session, nor
    # signed initData from a different Telegram actor can confirm that link.
    for headers, body in (
        ({}, {"token": link_token, "telegram_id": 71001}),
        (auth(attacker_token), {"token": link_token, "telegram_id": 71001}),
        ({}, {"token": link_token, "telegram_id": 71001,
              "init_data": telegram_init_data(71002)}),
        ({}, {"token": link_token, "telegram_id": 71001,
              "init_data": telegram_init_data(71001, forged=True)}),
        ({}, {"token": link_token, "init_data": telegram_init_data(71002)}),
    ):
        response = client.post("/auth/telegram-link/confirm", headers=headers, json=body)
        assert response.status_code in (401, 403), (body, response.text)
        assert db.get_user_by_identity("google", "safe-deeplink") is None
    # A confirmed native client session proves the Telegram owner as well.
    response = client.post("/auth/telegram-link/confirm", headers=auth(owner_token),
        json={"token": link_token, "telegram_id": 71001})
    assert response.json()["status"] == "confirmed", response.text
    assert db.get_user_by_identity("google", "safe-deeplink")["id"] == 1
    response = client.post("/auth/telegram-link/confirm", headers=auth(attacker_token),
        json={"token": link_token})
    assert response.status_code == 403, response.text
    assert confirm_telegram_link_session(link_token, 71002)["ok"] is False
    assert confirm_telegram_link_session(link_token, 71001)["ok"] is True
    report["cannot_confirm_foreign_telegram_link_and_valid_flow_works"] = True

    # Free the Google slot through an authorized own-account unlink before
    # testing signed Telegram confirmation for a different one-time token.
    response = client.post("/auth/unlink-identity", headers=auth(owner_token),
        json={"user_id": 1, "provider": "google"})
    assert response.json()["status"] == "unlinked", response.text
    response = client.post("/auth/telegram-link/start", json={
        "provider": "google", "id_token": jwt("honest-owner")})
    second_link = response.json()["token"]
    assert bind_telegram_link_session(second_link, 71001)["ok"] is True
    response = client.post("/auth/telegram-link/confirm", json={"token": second_link,
        "telegram_id": 71001, "init_data": telegram_init_data(71001)})
    assert response.json()["status"] == "confirmed", response.text
    report["verified_telegram_http_confirmation_works"] = True

    # Owner URL identifiers, ordinary client identity and admin membership are
    # not equivalent to an authenticated current owner of that shop.
    base = "/owner/settings/71001/admins"
    for headers in ({}, auth(attacker_token), auth(admin_token),
                    {"X-Telegram-Init-Data": telegram_init_data(71002)}):
        response = client.post(base, headers=headers, json={"telegram_id": 71002})
        assert response.status_code in (401, 403), response.text
        assert query("SELECT id FROM shop_admins WHERE shop_id=1 AND user_id=2") == []
        response = client.delete(base + "/71003", headers=headers)
        assert response.status_code in (401, 403), response.text
        assert query("SELECT id FROM shop_admins WHERE shop_id=1 AND user_id=3")
    assert client.post("/owner/settings/71004/admins", headers=auth(owner_token),
        json={"telegram_id": 71002}).status_code == 403
    response = client.get(base, headers=auth(owner_token))
    assert response.status_code == 200 and response.json()["ok"] is True, response.text
    response = client.post(base, headers=auth(owner_token), json={"telegram_id": 71002})
    assert response.json()["ok"] is True, response.text
    assert query("SELECT role FROM shop_admins WHERE shop_id=1 AND user_id=2")[0]["role"] == "admin"
    response = client.delete(base + "/71002", headers=auth(owner_token))
    assert response.json()["ok"] is True, response.text
    assert query("SELECT id FROM shop_admins WHERE shop_id=1 AND user_id=2") == []
    verified_owner = {"X-Telegram-Init-Data": telegram_init_data(71001)}
    response = client.post(base, headers=verified_owner, json={"telegram_id": 71002})
    assert response.json()["ok"] is True, response.text
    assert client.delete(base + "/71002", headers=verified_owner).json()["ok"] is True
    report["cannot_self_grant_admin_and_authorized_owner_flows_work"] = True

    # The legacy route now authenticates ownership; the existing /me route
    # retains its response shape and last-login-identity safeguard.
    response = client.post("/auth/unlink-identity", json={"user_id": 1, "provider": "google"})
    assert response.status_code == 401, response.text
    response = client.post("/auth/unlink-identity", headers=auth(attacker_token),
        json={"user_id": 1, "provider": "google"})
    assert response.status_code == 403, response.text
    assert db.get_user_by_identity("google", "honest-owner")["id"] == 1
    response = client.post("/auth/unlink-identity", headers=auth(attacker_token),
        json={"user_id": 2, "provider": "google"})
    assert response.json()["status"] == "unlinked", response.text
    assert db.get_user_by_identity("google", "registered-attacker") is None
    assert client.post("/me/unlink-identity", headers=auth(owner_token),
        json={"provider": "google"}).json()["status"] == "unlinked"
    assert db.link_user_identity(1, "google", "honest-owner")["status"] == "linked"
    report["cannot_unlink_foreign_identity_and_own_unlink_works"] = True

    # Rollout issuance is explicitly closed by default; both provider verifier
    # and membership checks are real after operators enable the flag locally.
    os.environ.pop("BARISTA_LOGIN_ENABLED", None)
    body = {"provider": "google", "id_token": jwt("honest-owner", barista=True)}
    assert client.post("/barista/auth/identity", json=body).status_code == 503
    os.environ["BARISTA_LOGIN_ENABLED"] = "true"
    response = client.post("/barista/auth/identity", json=body)
    assert response.status_code == 200, response.text
    barista = response.json()["access_token"]
    state["barista_token"] = barista
    for path in ("/me", "/me/qr", "/me/shops", "/me/stats"):
        assert client.get(path, headers=auth(barista)).status_code == 401, path
    assert client.get("/barista/me", headers=auth("legacy-active")).status_code == 401
    assert client.post("/barista/context", headers=auth("legacy-active"),
        json={"shop_id": 1}).status_code == 401
    assert client.post("/barista/auth/logout", headers=auth("legacy-active")).status_code == 401
    assert client.get("/barista/me", headers=auth(barista)).status_code == 200
    assert client.post("/barista/context", headers=auth(barista),
        json={"shop_id": 1}).status_code == 200
    legacy_hash = hashlib.sha256(barista.encode()).hexdigest()
    assert query("SELECT id FROM app_sessions WHERE token_hash=%s", (legacy_hash,)) == []
    assert query("SELECT purpose FROM app_sessions WHERE token_hash=%s",
                 (db._hash_barista_session_token(barista),))[0]["purpose"] == "barista"
    os.environ["BARISTA_LOGIN_ENABLED"] = "false"
    assert client.get("/barista/me", headers=auth(barista)).status_code == 200
    other_barista = db.create_app_session(1, purpose="barista", selected_membership_id=1)
    assert client.post("/barista/auth/logout", headers=auth(other_barista)).json()["revoked"] is True
    assert client.get("/barista/me", headers=auth(other_barista)).status_code == 401
    report["session_purpose_isolation_real_routes_and_issuance_gate"] = True

    _real_barista_operations(db, client, query, auth, report)
    _real_native_admin_tools(db, client, query, auth, jwt, report)
    _real_bot_regressions(main, db, client, query, auth, jwt, report)


def _real_owner_analytics_security(
    main, db, client, query, auth, telegram_init_data,
    owner_token, attacker_token, admin_token, report,
):
    """All actual legacy routes authenticate actor and lock current owner scope."""
    suffixes = ("overview", "activity", "clients", "details")
    expected_routes = {
        f"/owner/analytics/{{owner_telegram_id}}/{suffix}" for suffix in suffixes
    }
    actual_routes = {
        route.path: route.methods for route in main.app.routes
        if getattr(route, "path", "").startswith("/owner/analytics/")
    }
    assert set(actual_routes) == expected_routes, actual_routes
    assert all(methods == {"GET"} for methods in actual_routes.values())
    report["owner_analytics_complete_real_route_inventory"] = True

    other_token = db.create_app_session(4)
    barista_token = db.create_app_session(1, purpose="barista", selected_membership_id=1)
    query("""INSERT INTO users(id,telegram_user_id,full_name,personal_qr_token)
        VALUES(8,71008,'Mixed role owner','mixed-owner-qr'),
              (9,566408696,'Local superadmin only','local-superadmin-qr')""")
    query("""INSERT INTO user_identities(user_id,provider,provider_user_id)
        VALUES(8,'telegram','71008'),(9,'telegram','566408696')""")
    # Earlier admin membership used to select a foreign shop in all four helpers.
    query("""INSERT INTO shop_admins(shop_id,user_id,role)
        VALUES(2,8,'admin'),(1,8,'owner')""")
    mixed_token = db.create_app_session(8)
    superadmin_token = db.create_app_session(9)
    query("""INSERT INTO shop_clients(shop_id,user_id,cups,free_coffee_balance,total_scans)
        VALUES(2,4,444,444,444)""")
    query("""INSERT INTO transactions(shop_id,user_id,admin_user_id,type,cups_added)
        VALUES(2,4,4,'add_cups',444)""")
    other_membership = query("SELECT * FROM shop_admins WHERE user_id=4 AND shop_id=2")[0]

    for suffix in suffixes:
        own_path = f"/owner/analytics/71001/{suffix}"
        foreign_path = f"/owner/analytics/71004/{suffix}"
        for headers in (
            {}, auth("garbage"), {"Authorization": "Basic legacy-active"},
            auth("legacy-expired"), auth("legacy-revoked"), auth(barista_token),
            {"X-Telegram-Init-Data": "unverified-data"},
            {"X-Telegram-Init-Data": telegram_init_data(71001, forged=True)},
            {**auth("garbage"), "X-Telegram-Init-Data": telegram_init_data(71001)},
        ):
            response = client.get(own_path, headers=headers)
            assert response.status_code == 401, (suffix, response.status_code, response.text)
        for headers in (auth(attacker_token), auth(admin_token), auth(other_token),
                        {"X-Telegram-Init-Data": telegram_init_data(71002)},
                        {"X-Telegram-Init-Data": telegram_init_data(71003)}):
            response = client.get(own_path, headers=headers)
            assert response.status_code == 403, (suffix, response.text)
        assert client.get(foreign_path, headers=auth(owner_token)).status_code == 403
        assert client.get(f"/owner/analytics/566408696/{suffix}",
                          headers=auth(superadmin_token)).status_code == 403

        expected = client.get(own_path, headers=auth(owner_token))
        assert expected.status_code == 200 and expected.json()["ok"] is True, expected.text
        assert expected.json()["shop_id"] == 1
        for headers in (
            auth("legacy-active"), {"X-Telegram-Init-Data": telegram_init_data(71001)},
            {**auth(owner_token), "X-Telegram-Init-Data": "invalid-ignored-secondary-proof"},
        ):
            response = client.get(own_path, headers=headers)
            assert response.status_code == 200 and response.json() == expected.json(), response.text
        for selector, value in (
            ("shop_id", 2), ("owner_id", 4), ("owner_telegram_id", 71004),
            ("telegram_id", 71004), ("admin_user_id", 4), ("user_id", 4), ("role", "owner"),
        ):
            response = client.get(own_path, params={selector: value}, headers=auth(owner_token))
            assert response.status_code == 403, (suffix, selector, response.text)
        # GET bodies are never consumed as a context. No body field selects a shop.
        response = client.request("GET", own_path, headers=auth(owner_token),
            json={"shop_id": 2, "owner_id": 4, "telegram_id": 71004, "role": "owner"})
        assert response.status_code == 200 and response.json() == expected.json(), response.text

        mixed = client.get(f"/owner/analytics/71008/{suffix}", headers=auth(mixed_token))
        assert mixed.status_code == 200 and mixed.json() == expected.json(), mixed.text
        foreign = client.get(foreign_path, headers=auth(other_token))
        assert foreign.status_code == 200 and foreign.json()["shop_id"] == 2, foreign.text

        query("UPDATE shop_admins SET role='admin' WHERE id=%s", (other_membership["id"],))
        assert client.get(foreign_path, headers=auth(other_token)).status_code == 403
        query("UPDATE shop_admins SET role='owner' WHERE id=%s", (other_membership["id"],))
        query("DELETE FROM shop_admins WHERE id=%s", (other_membership["id"],))
        assert client.get(foreign_path, headers=auth(other_token)).status_code == 403
        assert client.get(foreign_path, headers={
            "X-Telegram-Init-Data": telegram_init_data(71004)}).status_code == 403
        query("""INSERT INTO shop_admins(id,shop_id,user_id,role,created_at)
            VALUES(%s,%s,%s,%s,%s)""", tuple(other_membership[key] for key in
                ("id", "shop_id", "user_id", "role", "created_at")))
        report[f"real_owner_analytics_{suffix}_authentication_and_shop_isolation"] = True

    assert client.get("/owner/analytics/71001/overview", headers=auth(owner_token)).json()["stats"]["free_coffees_now"] == 1
    assert client.get("/owner/analytics/71001/activity", headers=auth(owner_token)).json()["scans_today"] == 0
    own_clients = client.get("/owner/analytics/71001/clients", headers=auth(owner_token)).json()["clients"]
    assert [item["name"] for item in own_clients] == ["Local owner"]
    assert client.get("/owner/analytics/71001/details", headers=auth(owner_token)).json()["loyalty"]["free_now"] == 1
    # Restore the fixture's original memberships/data for subsequent real flows.
    query("DELETE FROM transactions WHERE shop_id=2 AND user_id=4 AND cups_added=444")
    query("DELETE FROM shop_clients WHERE shop_id=2 AND user_id=4")
    query("DELETE FROM users WHERE id IN (8,9)")


def _real_barista_operations(db, client, query, auth, report):
    """Exercise the actual Stage 2 router/imports with only delivery mocked."""
    from app.api import barista
    from unittest.mock import patch

    os.environ["BARISTA_LOGIN_ENABLED"] = "false"
    token = db.create_app_session(1, purpose="barista", selected_membership_id=1)
    headers = auth(token)
    body = {"qr_token": "coffee:guest-qr"}
    paths = ("/barista/scan", "/barista/add-cup", "/barista/redeem")
    for path in paths:
        assert client.post(path, json=body).status_code == 401
        assert client.post(path, headers=auth("legacy-active"), json=body).status_code == 401
        assert client.post(path, headers=headers, json={**body, "shop_id": 2}).status_code == 422
        assert client.post(path, headers=headers,
            json={"qr_token": "coffee:unknown-qr"}).status_code == 404

    session_before = query("SELECT * FROM app_sessions WHERE token_hash=%s",
                          (db._hash_barista_session_token(token),))
    balance_before = query("SELECT * FROM shop_clients WHERE user_id=32650")
    scan = client.post("/barista/scan", headers=headers, json=body)
    assert scan.status_code == 200, scan.text
    assert scan.json()["client"]["cups"] == 0
    assert scan.json()["shop"] == {"id": 1, "name": "Local cafe"}
    assert query("SELECT * FROM shop_clients WHERE user_id=32650") == balance_before == []
    assert query("SELECT * FROM app_sessions WHERE token_hash=%s",
                 (db._hash_barista_session_token(token),)) == session_before

    query("""INSERT INTO subscriptions(shop_id,plan,status,expires_at)
        VALUES(1,'basic','active',NOW()+INTERVAL '30 days')
        ON CONFLICT(shop_id) DO UPDATE SET status='active',expires_at=EXCLUDED.expires_at""")
    query("INSERT INTO shop_clients(shop_id,user_id,cups) VALUES(1,32650,6)")
    with patch.object(barista, "notify_cups_added", AsyncMock()) as added, \
         patch.object(barista, "notify_free_redeemed", AsyncMock()) as redeemed:
        response = client.post("/barista/add-cup", headers=headers, json=body)
        assert response.status_code == 200, response.text
        assert response.json()["client"]["cups"] == 0
        assert response.json()["client"]["free_coffee_balance"] == 1
        assert response.json()["operation"]["free_coffee_earned"] == 1
        added.assert_awaited_once()
        response = client.post("/barista/redeem", headers=headers, json=body)
        assert response.status_code == 200, response.text
        assert response.json()["client"]["free_coffee_balance"] == 0
        redeemed.assert_awaited_once()
        response = client.post("/barista/redeem", headers=headers, json=body)
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "NO_FREE_COFFEE"
        # A delivery failure happens after the shared loyalty transaction commits.
        added.side_effect = RuntimeError("offline delivery unavailable")
        response = client.post("/barista/add-cup", headers=headers, json=body)
        assert response.status_code == 200
        assert query("SELECT cups FROM shop_clients WHERE shop_id=1 AND user_id=32650")[0]["cups"] == 1
    assert query("SELECT COUNT(*) AS n FROM transactions WHERE shop_id=1 AND user_id=32650")[0]["n"] == 3
    assert os.environ["BARISTA_LOGIN_ENABLED"] == "false"
    report["real_barista_qr_operations_readonly_atomic_and_delivery_isolated"] = True


def _real_native_admin_tools(db, client, query, auth, jwt, report):
    """Execute complete imported invite/owner modules and actual DB helpers."""
    from app.api import barista, barista_owner
    from unittest.mock import patch

    client_before = query("SELECT * FROM app_sessions WHERE purpose='client' ORDER BY id")
    owner = db.create_app_session(1, purpose="barista", selected_membership_id=1)
    admin_membership = query("SELECT id FROM shop_admins WHERE user_id=3 AND shop_id=1")[0]["id"]
    admin = db.create_app_session(3, purpose="barista", selected_membership_id=admin_membership)
    other_membership = query("SELECT id FROM shop_admins WHERE user_id=4 AND shop_id=2")[0]["id"]
    other_owner = db.create_app_session(4, purpose="barista", selected_membership_id=other_membership)
    os.environ["BARISTA_LOGIN_ENABLED"] = "true"
    os.environ["BARISTA_INVITE_PEPPER"] = "offline-isolated-invites-only-secret-32bytes-minimum"
    assert db.link_user_identity(32650, "google", "native-guest")["status"] == "linked"
    memberless_body = {"provider": "google", "id_token": jwt("native-guest", barista=True)}
    before = query("SELECT COUNT(*) AS n FROM app_sessions")[0]["n"]
    response = client.post("/barista/auth/identity", json=memberless_body)
    assert response.status_code == 403 and response.json()["detail"]["code"] == "NEEDS_INVITE"
    assert query("SELECT COUNT(*) AS n FROM app_sessions")[0]["n"] == before
    unknown = client.post("/barista/auth/identity", json={
        "provider": "google", "id_token": jwt("native-not-linked", barista=True)})
    assert unknown.status_code == 403 and unknown.json()["detail"]["code"] == "IDENTITY_NOT_LINKED"
    assert client.post("/barista/owner/invites", headers=auth(admin), json={}).status_code == 403
    created = client.post("/barista/owner/invites", headers=auth(owner), json={})
    assert created.status_code == 201, created.text
    invitation = created.json()["invite"]
    listed = client.get("/barista/owner/invites", headers=auth(owner))
    assert listed.status_code == 200 and len(listed.json()["invites"]) == 1
    assert "code" not in listed.json()["invites"][0]
    assert client.delete(f"/barista/owner/invites/{invitation['id']}",
                         headers=auth(other_owner)).status_code == 404
    response = client.post("/barista/auth/accept-invite", json={
        **memberless_body, "invite_code": invitation["code"]})
    assert response.status_code == 200, response.text
    accepted = response.json()
    assert accepted["employee"]["user_id"] == 32650
    assert accepted["selected_context"]["role"] == "admin" and accepted["selected_context"]["shop_id"] == 1
    assert client.get("/barista/me", headers=auth(accepted["access_token"])).status_code == 200
    assert client.get("/me", headers=auth(accepted["access_token"])).status_code == 401
    consumed = query("SELECT used_at,used_by_user_id FROM barista_admin_invites WHERE id=%s",
                     (invitation["id"],))[0]
    assert consumed["used_at"] is not None and consumed["used_by_user_id"] == 32650
    assert client.post("/barista/auth/accept-invite", json={
        **memberless_body, "invite_code": invitation["code"]}).status_code == 400
    report["real_admin_invites_verified_existing_identity_and_atomic_session"] = True

    members = client.get("/barista/owner/members", headers=auth(owner))
    assert members.status_code == 200 and members.json()["shop"]["id"] == 1
    assert all(item["user_id"] != 4 for item in members.json()["members"])
    assert client.get("/barista/owner/members", headers=auth(admin)).status_code == 403
    assert client.delete("/barista/owner/members/1", headers=auth(owner)).status_code == 409
    assert client.get("/barista/statistics?days=30", headers=auth(admin)).status_code == 403
    stats = client.get("/barista/statistics?days=7", headers=auth(admin))
    assert stats.status_code == 200 and stats.json()["metrics"]["active_clients"] is None
    with patch.object(barista, "notify_cups_added", AsyncMock()):
        result = client.post("/barista/add-cup", headers=auth(owner),
                             json={"qr_token": "coffee:guest-qr", "count": 3})
        assert result.status_code == 200 and result.json()["operation"]["cups_added"] == 3
    assert client.post("/barista/owner/broadcast/preview", headers=auth(admin), json={"text": "local"}).status_code == 403
    preview = client.post("/barista/owner/broadcast/preview", headers=auth(owner), json={"text": "Offline only"})
    assert preview.status_code == 200, preview.text
    assert preview.json()["recipients_count"] == 1
    confirmation = preview.json()["confirmation_token"]
    assert client.post("/barista/owner/broadcast", headers=auth(other_owner),
                       json={"confirmation_token": confirmation}).status_code == 403

    async def offline_delivery(broadcast):
        assert broadcast["recipients"] == [{"telegram_user_id": 71001, "user_id": 1}]
        return [1], 0

    with patch.object(barista_owner, "deliver_owner_broadcast", AsyncMock(side_effect=offline_delivery)) as delivery:
        sent = client.post("/barista/owner/broadcast", headers=auth(owner), json={"confirmation_token": confirmation})
        assert sent.status_code == 200 and sent.json()["sent"] == 1, sent.text
        assert client.post("/barista/owner/broadcast", headers=auth(owner),
                           json={"confirmation_token": confirmation}).status_code == 409
        delivery.assert_awaited_once()
    assert query("SELECT * FROM app_sessions WHERE purpose='client' ORDER BY id") == client_before
    os.environ["BARISTA_LOGIN_ENABLED"] = "false"
    report["real_native_owner_tools_shop_scope_role_and_broadcast_isolation"] = True


def _real_bot_regressions(main, db, client, query, auth, jwt, report):
    """Execute real trusted Telegram entry handlers; mock only outbound replies."""
    from app.handlers import common

    def message(actor, *, chat_id=None, chat_type="private"):
        return SimpleNamespace(
            from_user=SimpleNamespace(id=actor, username="offline", full_name="Offline actor"),
            chat=SimpleNamespace(id=actor if chat_id is None else chat_id, type=chat_type),
            answer=AsyncMock(), edit_text=AsyncMock(),
        )

    def callback(token, actor, *, chat_id=None, chat_type="private"):
        return SimpleNamespace(
            data="tg_link_confirm:" + token,
            from_user=SimpleNamespace(id=actor),
            message=message(actor, chat_id=chat_id, chat_type=chat_type),
            answer=AsyncMock(),
        )

    def start(token, actor, *, chat_id=None, chat_type="private"):
        incoming = message(actor, chat_id=chat_id, chat_type=chat_type)
        asyncio.run(common.start_handler(
            incoming, SimpleNamespace(args="link_" + token),
            SimpleNamespace(set_state=AsyncMock()),
        ))
        return incoming

    def confirm(token, actor, *, chat_id=None, chat_type="private"):
        incoming = callback(token, actor, chat_id=chat_id, chat_type=chat_type)
        asyncio.run(common.confirm_telegram_link(incoming))
        return incoming

    def link_state(token):
        return query("SELECT * FROM telegram_link_sessions WHERE token_hash=%s",
            (hashlib.sha256(token.encode()).hexdigest(),))[0]

    query("""INSERT INTO users(id,telegram_user_id,full_name,personal_qr_token)
        VALUES(6,71006,'Bot link target','bot-link-target-qr'),
              (7,71007,'Bot merge target','bot-merge-target-qr')""")
    query("""INSERT INTO user_identities(user_id,provider,provider_user_id)
        VALUES(6,'telegram','71006'),(7,'telegram','71007')""")
    response = client.post("/auth/telegram-link/start", json={
        "provider": "google", "id_token": jwt("bot-linked-target")})
    token = response.json()["token"]
    for incoming in (start(token, 71006, chat_type="group"),
                     start(token, 71006, chat_id=71002)):
        incoming.answer.assert_awaited_once()
        assert link_state(token)["telegram_user_id"] is None
    legitimate = start(token, 71006)
    legitimate.answer.assert_awaited_once()
    assert link_state(token)["telegram_user_id"] == 71006
    assert "reply_markup" in legitimate.answer.await_args.kwargs
    start(token, 71002)  # An unrelated private actor cannot rebind it.
    assert link_state(token)["telegram_user_id"] == 71006
    foreign = confirm(token, 71002)
    foreign.message.edit_text.assert_awaited_once()
    assert link_state(token)["status"] == "pending"
    for incoming in (confirm(token, 71006, chat_type="group"),
                     confirm(token, 71006, chat_id=71002)):
        incoming.answer.assert_not_awaited()
        incoming.message.edit_text.assert_not_awaited()
        assert link_state(token)["status"] == "pending"
    matching = confirm(token, 71006)
    matching.message.edit_text.assert_awaited_once()
    assert link_state(token)["status"] == "confirmed"
    assert db.get_user_by_identity("google", "bot-linked-target")["id"] == 6
    assert client.post("/auth/test-identity", json={"provider": "google",
        "id_token": jwt("bot-linked-target")}).json()["status"] == "existing"
    confirm(token, 71002)  # Foreign replay cannot acquire its confirmed receipt.
    assert link_state(token)["telegram_user_id"] == 71006
    report["real_bot_private_actor_binding_and_confirmation"] = True

    # The old merge flow still requires a verified source identity, then the
    # existing user's actual private Telegram confirmation; its balance and
    # source-session behavior remain unchanged.
    created = client.post("/auth/test-identity", json={"provider": "google",
        "id_token": jwt("bot-merge-source"), "action": "create"}).json()
    assert created["ok"] is True, created
    source = created["user_id"]
    source_token = created["access_token"]
    query("""INSERT INTO shop_clients(shop_id,user_id,cups,free_coffee_balance)
        VALUES(1,%s,2,0),(1,7,5,1)""", (source,))
    response = client.post("/auth/telegram-merge/start", json={
        "provider": "google", "id_token": jwt("bot-merge-source"),
        "current_user_id": source})
    assert response.json()["status"] == "pending", response.text
    merge_token = response.json()["token"]
    start(merge_token, 71007)
    confirm(merge_token, 71002)
    assert db.get_user_by_identity("google", "bot-merge-source")["id"] == source
    matching = confirm(merge_token, 71007)
    matching.message.edit_text.assert_awaited_once()
    assert link_state(merge_token)["status"] == "confirmed"
    assert db.get_user_by_identity("google", "bot-merge-source")["id"] == 7
    assert db.get_app_session(source_token) is None
    balance = query("SELECT cups,free_coffee_balance FROM shop_clients WHERE shop_id=1 AND user_id=7")[0]
    assert balance == {"cups": 0, "free_coffee_balance": 2}
    assert query("SELECT id FROM users WHERE id=%s", (source,)) == []
    confirm(merge_token, 71007)
    assert query("SELECT cups,free_coffee_balance FROM shop_clients WHERE shop_id=1 AND user_id=7")[0] == balance
    report["real_bot_existing_merge_preserves_balance_and_revokes_source"] = True


if __name__ == "__main__":
    assert len(sys.argv) == 5, "worker is run only by test_full_backend_security.py"
    worker(Path(sys.argv[1]), sys.argv[2], sys.argv[3], Path(sys.argv[4]))
