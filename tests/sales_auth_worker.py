"""Actual backend startup and sales auth under the existing local-only guard."""

import hashlib
import hmac
import json
import os
from pathlib import Path
import sys
import time
from urllib.parse import urlencode

from full_backend_worker import _guard_environment


def worker(source, schema, audit_root):
    # Blocks external sockets, disables dotenv, redirects SQLite/uploads, and
    # permits only the known disposable PostgreSQL socket before backend import.
    _guard_environment(source, "sales_auth", schema, audit_root)
    import run
    from app.api import main
    from app import db
    from fastapi.testclient import TestClient

    assert run.app is main.app
    superadmin_id = next(iter(main.SUPERADMIN_TELEGRAM_IDS))
    assert 81001 not in main.SUPERADMIN_TELEGRAM_IDS

    def query(statement, values=()):
        with db.get_connection() as conn:
            cursor = conn.execute(statement, values)
            return cursor.fetchall() if cursor.description else []

    def bearer(token):
        return {"Authorization": "Bearer " + token}

    def miniapp(actor, *, age=0, forged=False):
        data = {"auth_date": str(int(time.time()) - age), "user": json.dumps({"id": actor})}
        checked = "\n".join(f"{key}={value}" for key, value in sorted(data.items()))
        key = hmac.new(b"WebAppData", os.environ["BOT_TOKEN"].encode(), hashlib.sha256).digest()
        data["hash"] = "0" * 64 if forged else hmac.new(key, checked.encode(), hashlib.sha256).hexdigest()
        return {"X-Telegram-Init-Data": urlencode(data)}

    query("""INSERT INTO users(id,telegram_user_id,full_name,personal_qr_token)
        VALUES(1,%s,'API superadmin','sales-superadmin'),
              (2,81001,'Ordinary bot admin','sales-ordinary'),
              (3,NULL,'Unlinked client','sales-unlinked')""", (superadmin_id,))
    query("""INSERT INTO user_identities(user_id,provider,provider_user_id)
        SELECT id,'telegram',telegram_user_id::text FROM users WHERE telegram_user_id IS NOT NULL""")
    query("INSERT INTO coffee_shops(id,name) VALUES(1,'Sales test cafe')")
    query("INSERT INTO shop_admins(id,shop_id,user_id,role) VALUES(1,1,1,'owner'),(2,1,2,'owner')")
    query("INSERT INTO shop_clients(shop_id,user_id,cups) VALUES(1,3,5)")
    superadmin = db.create_app_session(1)
    ordinary = db.create_app_session(2)
    unlinked = db.create_app_session(3)
    expired = db.create_app_session(1)
    revoked = db.create_app_session(1)
    barista = db.create_app_session(1, purpose="barista", selected_membership_id=1)
    query("UPDATE app_sessions SET expires_at=NOW()-INTERVAL '1 day' WHERE token_hash=%s",
          (db._hash_app_session_token(expired),))
    assert db.revoke_app_session(revoked)
    business_before = {table: query(f"SELECT * FROM {table} ORDER BY id")
                       for table in ("users", "coffee_shops", "shop_admins", "shop_clients")}
    table_sql = "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename"
    tables_before = query(table_sql)
    report = {"real_startup_and_sales_router": True}

    with TestClient(main.app) as client:
        routes = {path: set(operations) for path, operations in main.app.openapi()["paths"].items()
                  if path.startswith("/sales")}
        assert routes == {
            "/sales/search": {"get"},
            "/sales/search/usage": {"get"},
            "/sales/me": {"get"}, "/sales/leads": {"get", "post"},
            "/sales/leads/{lead_id}": {"get", "patch"},
            "/sales/leads/{lead_id}/events": {"post"},
            "/sales/followups": {"get"}, "/sales/stats": {"get"},
        }, routes
        expected = {"allowed": True, "user_id": 1, "role": "superadmin"}
        for headers in (bearer(superadmin), miniapp(superadmin_id)):
            response = client.get("/sales/me", headers=headers)
            assert response.status_code == 200, response.text
            assert response.json() == expected
            assert response.headers["Cache-Control"] == "no-store"
            vary = {name.strip().lower() for name in response.headers["Vary"].split(",")}
            assert {"authorization", "x-telegram-init-data"}.issubset(vary), response.headers["Vary"]
        report["both_verified_auth_mechanisms"] = True

        for headers in (bearer(ordinary), miniapp(81001), bearer(unlinked), miniapp(89999)):
            response = client.request(
                "GET", "/sales/me?role=superadmin&is_superadmin=true&user_id=1",
                headers={**headers, "X-Role": "superadmin", "X-Is-Superadmin": "true"},
                json={"telegram_id": superadmin_id, "user_id": 1, "role": "superadmin"},
            )
            assert response.status_code == 403, response.text
        assert client.get("/sales/me?user_id=2", headers=bearer(superadmin)).status_code == 422
        report["ordinary_and_forged_claims_denied"] = True

        for headers in ({}, bearer("unknown"), bearer(expired), bearer(revoked), bearer(barista),
                        miniapp(superadmin_id, forged=True), miniapp(superadmin_id, age=86401)):
            assert client.get("/sales/me", headers=headers).status_code == 401
        assert client.get("/sales/me", headers={
            **miniapp(superadmin_id), **bearer("unknown"),
        }).status_code == 401
        assert client.get("/sales/me", headers={
            **miniapp(superadmin_id), **bearer(ordinary),
        }).status_code == 403
        report["dead_and_barista_sessions_denied"] = True

    assert query(table_sql) == tables_before
    assert not any(item["tablename"].startswith("sales_") for item in tables_before)
    assert {table: query(f"SELECT * FROM {table} ORDER BY id") for table in business_before} == business_before
    report["no_sales_storage_or_business_changes"] = True
    print(json.dumps(report))


if __name__ == "__main__":
    worker(Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]))
