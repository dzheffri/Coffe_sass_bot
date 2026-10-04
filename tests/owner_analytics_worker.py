"""Real startup analytics regression worker, restricted to a disposable local DB.

Independent of the uncommitted native invites/broadcast/count test extensions;
the security-only release can run this worker against its own clean checkout.
"""

import hashlib
import hmac
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys
import time
from urllib.parse import urlencode

from psycopg.conninfo import conninfo_to_dict


def guard_environment(source, schema, audit_root):
    dsn = os.environ["TEST_DATABASE_URL"]
    values = conninfo_to_dict(dsn)
    expected_socket = audit_root.parent / "socket"
    assert values.get("dbname") == "barista_auth_tests"
    assert values.get("user") == "barista_test"
    assert values.get("host") == str(expected_socket)
    assert values.get("port") == "55439"
    assert schema.startswith("owner_analytics_") and schema.isidentifier()
    os.environ.update({
        "DATABASE_URL": dsn + f" options='-c search_path={schema}'",
        "BOT_TOKEN": "123456789:" + "A" * 35,
        "SCANNER_URL": "https://invalid.example/local-only",
        "ADMIN_PANEL_URL": "https://invalid.example/local-only",
        "BARISTA_LOGIN_ENABLED": "false",
        "PYTHON_DOTENV_DISABLED": "1",
    })
    import dotenv
    dotenv.load_dotenv = lambda *args, **kwargs: False
    original_connect = socket.socket.connect

    def local_only(sock, address):
        if sock.family != socket.AF_UNIX or not str(address).startswith(str(expected_socket)):
            raise RuntimeError("External network disabled in owner analytics tests")
        return original_connect(sock, address)

    socket.socket.connect = local_only
    uploads = audit_root / "uploads"
    original_makedirs = os.makedirs
    os.makedirs = lambda path, *args, **kwargs: original_makedirs(
        uploads if str(path) == "/data/uploads" else path, *args, **kwargs)
    from fastapi.staticfiles import StaticFiles
    original_static = StaticFiles.__init__

    def local_static(self, *args, **kwargs):
        if str(kwargs.get("directory")) == "/data/uploads":
            kwargs["directory"] = uploads
        return original_static(self, *args, **kwargs)

    StaticFiles.__init__ = local_static
    original_sqlite = sqlite3.connect
    sqlite3.connect = lambda path, *args, **kwargs: original_sqlite(
        audit_root / "web_panel.sqlite" if str(path).endswith("web_panel.db") else path,
        *args, **kwargs)
    sys.path.insert(0, str(source))


def worker(source, schema, audit_root):
    guard_environment(source, schema, audit_root)
    import run
    from app.api import main
    from app import db
    from fastapi.testclient import TestClient

    assert run.app is main.app
    report = {"real_backend_and_bot_startup": True}

    def query(statement, values=()):
        with db.get_connection() as conn:
            cur = conn.execute(statement, values)
            return cur.fetchall() if cur.description else []

    def auth(token):
        return {"Authorization": "Bearer " + token}

    def miniapp(actor, *, forged=False):
        data = {"auth_date": str(int(time.time())), "user": json.dumps({"id": actor})}
        checked = "\n".join(f"{key}={value}" for key, value in sorted(data.items()))
        key = hmac.new(b"WebAppData", os.environ["BOT_TOKEN"].encode(), hashlib.sha256).digest()
        data["hash"] = "0" * 64 if forged else hmac.new(key, checked.encode(), hashlib.sha256).hexdigest()
        return {"X-Telegram-Init-Data": urlencode(data)}

    query("""INSERT INTO users(id,telegram_user_id,full_name,personal_qr_token)
        VALUES(1,81001,'A/B owner','owner-ab'),(2,81002,'C owner','owner-c'),
              (3,81003,'Admin A','admin-a'),(4,81004,'Non-member','non-member'),
              (5,81005,'Customer A','customer-a'),(6,81006,'Customer B','customer-b'),
              (7,81007,'Customer C','customer-c'),(8,81008,'Mixed owner','mixed-owner'),
              (9,566408696,'Superadmin only','superadmin-only'),
              (10,81010,'Single owner','single-owner')""")
    query("SELECT setval('users_id_seq',10)")
    query("""INSERT INTO user_identities(user_id,provider,provider_user_id)
        SELECT id,'telegram',telegram_user_id::text FROM users""")
    query("INSERT INTO coffee_shops(id,name) VALUES(1,'Cafe A'),(2,'Cafe B'),(3,'Cafe C')")
    query("SELECT setval('coffee_shops_id_seq',3)")
    query("""INSERT INTO shop_admins(id,shop_id,user_id,role)
        VALUES(1,1,1,'owner'),(2,2,1,'owner'),(3,3,2,'owner'),(4,1,3,'admin'),
              (5,3,8,'admin'),(6,1,8,'owner'),(7,1,10,'owner')""")
    query("SELECT setval('shop_admins_id_seq',7)")
    query("""INSERT INTO shop_clients(shop_id,user_id,cups,free_coffee_balance,total_scans)
        VALUES(1,5,2,1,10),(2,6,6,2,20),(3,7,6,99,999)""")
    query("""INSERT INTO transactions(shop_id,user_id,admin_user_id,type,cups_added)
        VALUES(1,5,1,'add_cups',2),(2,6,1,'add_cups',4),(3,7,2,'add_cups',99)""")
    tokens = {user_id: db.create_app_session(user_id) for user_id in (1,2,3,4,8,9,10)}
    staff = db.create_app_session(1, purpose="barista", selected_membership_id=1)
    expired = db.create_app_session(1)
    revoked = db.create_app_session(1)
    query("UPDATE app_sessions SET expires_at=NOW()-INTERVAL '1 day' WHERE token_hash=%s",
          (db._hash_app_session_token(expired),))
    assert db.revoke_app_session(revoked) is True
    session_sql = "SELECT id,user_id,token_hash,revoked_at,purpose,selected_membership_id FROM app_sessions ORDER BY id"
    sessions_before = query(session_sql)
    suffixes = ("overview", "activity", "clients", "details")

    with TestClient(main.app) as client:
        routes = {route.path: route.methods for route in main.app.routes
                  if getattr(route, "path", "").startswith("/owner/analytics/")}
        assert set(routes) == {f"/owner/analytics/{{owner_telegram_id}}/{suffix}" for suffix in suffixes}
        assert all(methods == {"GET"} for methods in routes.values())
        report["complete_legacy_analytics_inventory"] = True
        shops_path = "/owner/shops/81001"
        for headers in ({}, auth("garbage"), auth(expired), auth(revoked), auth(staff), miniapp(81001, forged=True)):
            assert client.get(shops_path, headers=headers).status_code == 401
        for headers in (auth(tokens[2]), auth(tokens[3]), auth(tokens[4]), miniapp(81002)):
            assert client.get(shops_path, headers=headers).status_code == 403
        expected_list = {"ok": True, "shops": [{"shop_id": 1, "name": "Cafe A"},
            {"shop_id": 2, "name": "Cafe B"}], "selected_shop_id": None}
        assert client.get(shops_path, headers=auth(tokens[1])).json() == expected_list
        assert client.get(shops_path, headers=miniapp(81001)).json() == expected_list
        assert client.get("/owner/shops/81010", headers=auth(tokens[10])).json() == {
            "ok": True, "shops": [{"shop_id": 1, "name": "Cafe A"}], "selected_shop_id": 1}
        assert client.get("/owner/shops/566408696", headers=auth(tokens[9])).status_code == 403
        report["owned_shop_list_authentication_and_isolation"] = True

        for suffix in suffixes:
            path = f"/owner/analytics/81001/{suffix}"
            for headers in ({}, auth("garbage"), auth(expired), auth(revoked), auth(staff),
                            miniapp(81001, forged=True), {"X-Telegram-Init-Data": "garbage"}):
                assert client.get(path, params={"shop_id": 1}, headers=headers).status_code == 401
            for headers in (auth(tokens[2]), auth(tokens[3]), auth(tokens[4]), miniapp(81002)):
                assert client.get(path, params={"shop_id": 1}, headers=headers).status_code == 403
            assert client.get(path, headers=auth(tokens[1])).status_code == 409
            assert client.get(path, headers=auth(tokens[1])).json()["detail"]["code"] == "SHOP_CONTEXT_REQUIRED"
            missing_miniapp_context = client.get(path, headers=miniapp(81001))
            assert missing_miniapp_context.status_code == 409
            assert missing_miniapp_context.json()["detail"]["code"] == "SHOP_CONTEXT_REQUIRED"
            assert client.get(path, params={"shop_id": 3}, headers=auth(tokens[1])).status_code == 403
            assert client.get(f"/owner/analytics/81002/{suffix}", params={"shop_id": 3},
                              headers=auth(tokens[1])).status_code == 403
            for key in ("owner_id", "user_id", "admin_user_id", "telegram_id", "owner_telegram_id", "role"):
                assert client.get(path, params={"shop_id": 1, key: "3"}, headers=auth(tokens[1])).status_code == 403
            for value in ("", "0", "-1", "word", "1.0", "9223372036854775808", "1" * 20):
                assert client.get(path, params={"shop_id": value}, headers=auth(tokens[1])).status_code == 422
            assert client.get(path + "?shop_id=1&shop_id=2", headers=auth(tokens[1])).status_code == 422

            selected = {}
            for shop_id in (1,2):
                response = client.get(path, params={"shop_id": shop_id}, headers=auth(tokens[1]))
                assert response.status_code == 200 and response.json()["shop_id"] == shop_id, response.text
                selected[shop_id] = response.json()
                assert client.get(path, params={"shop_id": shop_id}, headers=miniapp(81001)).json() == selected[shop_id]
            assert selected[1] != selected[2]
            # Own sole-shop legacy URL remains compatible without a selector.
            legacy = client.get(f"/owner/analytics/81010/{suffix}", headers=auth(tokens[10]))
            assert legacy.status_code == 200 and legacy.json() == selected[1]
            legacy_miniapp = client.get(f"/owner/analytics/81010/{suffix}", headers=miniapp(81010))
            assert legacy_miniapp.status_code == 200 and legacy_miniapp.json() == selected[1]
            mixed = client.get(f"/owner/analytics/81008/{suffix}", headers=auth(tokens[8]))
            assert mixed.status_code == 200 and mixed.json() == selected[1]
            # Forged GET body cannot choose C or override the selected A.
            body = client.request("GET", path, params={"shop_id": 1}, headers=auth(tokens[1]),
                                  json={"shop_id": 3, "owner_id": 2, "role": "owner"})
            assert body.status_code == 200 and body.json() == selected[1]
            assert client.get(f"/owner/analytics/566408696/{suffix}", params={"shop_id": 1},
                              headers=auth(tokens[9])).status_code == 403
            query("UPDATE shop_admins SET role='admin' WHERE id=2")
            assert client.get(path, params={"shop_id": 2}, headers=auth(tokens[1])).status_code == 403
            query("UPDATE shop_admins SET role='owner' WHERE id=2")
            query("DELETE FROM shop_admins WHERE id=2")
            assert client.get(path, params={"shop_id": 2}, headers=auth(tokens[1])).status_code == 403
            assert client.get(path, params={"shop_id": 2}, headers=miniapp(81001)).status_code == 403
            query("INSERT INTO shop_admins(id,shop_id,user_id,role) VALUES(2,2,1,'owner')")
            report[f"{suffix}_authentication_explicit_AB_context_C_denied"] = True

        assert client.get("/owner/analytics/81001/overview?shop_id=1", headers=auth(tokens[1])).json()["stats"]["free_coffees_now"] == 1
        assert client.get("/owner/analytics/81001/overview?shop_id=2", headers=auth(tokens[1])).json()["stats"]["free_coffees_now"] == 2
        assert client.get("/owner/analytics/81001/activity?shop_id=1", headers=auth(tokens[1])).json()["scans_today"] == 2
        assert client.get("/owner/analytics/81001/activity?shop_id=2", headers=auth(tokens[1])).json()["scans_today"] == 4
        assert client.get("/owner/analytics/81001/clients?shop_id=1", headers=auth(tokens[1])).json()["clients"][0]["name"] == "Customer A"
        assert client.get("/owner/analytics/81001/clients?shop_id=2", headers=auth(tokens[1])).json()["clients"][0]["name"] == "Customer B"
        assert client.get("/owner/analytics/81001/details?shop_id=1", headers=auth(tokens[1])).json()["loyalty"]["total_scans"] == 10
        assert client.get("/owner/analytics/81001/details?shop_id=2", headers=auth(tokens[1])).json()["loyalty"]["total_scans"] == 20
        assert query(session_sql) == sessions_before
        assert client.get("/me", headers=auth(tokens[1])).status_code == 200
        assert client.get("/me", headers=auth(staff)).status_code == 401
        assert client.get("/barista/me", headers=auth(tokens[1])).status_code == 401
        assert client.post("/barista/auth/identity", json={"provider": "google", "id_token": "unused"}).status_code == 503
        report["existing_sessions_preserved_and_client_staff_purpose_isolation"] = True
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    worker(Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]))
