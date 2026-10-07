"""Actual main/router/DB wiring under the established local-only startup guard."""

import json
from pathlib import Path
import sys

from full_backend_worker import _guard_environment


def worker(source, schema, audit_root):
    _guard_environment(source, "sales_leads", schema, audit_root)
    import run
    from app.api import main
    from app import db
    from fastapi.testclient import TestClient

    assert run.app is main.app
    def query(statement, values=()):
        with db.get_connection() as connection:
            cursor = connection.execute(statement, values)
            return cursor.fetchall() if cursor.description else []

    assert query("SELECT tablename FROM pg_tables WHERE schemaname=current_schema() AND tablename IN ('sales_leads','sales_lead_events')") == []
    report = {"runtime_did_not_create_sales_tables": True}
    superadmin = next(iter(main.SUPERADMIN_TELEGRAM_IDS))
    query("""INSERT INTO users(id,telegram_user_id,full_name,personal_qr_token)
        VALUES(1,%s,'Local Sales superadmin','sales-crud-superadmin'),
              (2,81001,'Ordinary bot admin','sales-crud-ordinary')""", (superadmin,))
    query("""INSERT INTO user_identities(user_id,provider,provider_user_id)
        SELECT id,'telegram',telegram_user_id::text FROM users""")
    query("INSERT INTO coffee_shops(id,name) VALUES(1,'Sales existing cafe')")
    superadmin_token = db.create_app_session(1)
    ordinary_token = db.create_app_session(2)
    auth = {"Authorization": "Bearer " + superadmin_token}
    ordinary = {"Authorization": "Bearer " + ordinary_token}
    before = {table: query(f"SELECT * FROM {table} ORDER BY id")
              for table in ("users", "coffee_shops", "shop_admins", "shop_clients")}

    with TestClient(main.app) as client:
        missing = client.get("/sales/leads", headers=auth)
        assert missing.status_code == 503, missing.text
        assert missing.json()["detail"]["code"] == "SALES_SCHEMA_NOT_READY"
        assert query("SELECT tablename FROM pg_tables WHERE schemaname=current_schema() AND tablename IN ('sales_leads','sales_lead_events')") == []
        query((source / "migrations/20261006_sales_leads.sql").read_text(encoding="utf-8"))
        query((source / "migrations/20261006_sales_leads.sql").read_text(encoding="utf-8"))
        created = client.post("/sales/leads", headers=auth,
                              json={"name": "Real wired local Sales cafe", "city": "Київ"})
        assert created.status_code == 201, created.text
        lead_id = created.json()["lead"]["id"]
        assert created.json()["lead"]["created_by_user_id"] == 1
        updated = client.patch(f"/sales/leads/{lead_id}", headers=auth,
                               json={"status": "INTERESTED", "notes": "Validated through main"})
        assert updated.status_code == 200, updated.text
        assert any(event["event_type"] == "STATUS_CHANGED" for event in updated.json()["events"])
        detail = client.get(f"/sales/leads/{lead_id}", headers=auth)
        assert detail.status_code == 200 and detail.json()["lead"]["status"] == "INTERESTED"
        assert client.get("/sales/leads", headers=auth).json()["total"] == 1
        assert client.get("/sales/stats", headers=auth).json()["interested"] == 1
        for method, path, body in (
            ("GET", "/sales/leads", None), ("GET", f"/sales/leads/{lead_id}", None),
            ("POST", "/sales/leads", {"name": "Unauthorized"}),
            ("PATCH", f"/sales/leads/{lead_id}", {"status": "CONTACTED"}),
            ("POST", f"/sales/leads/{lead_id}/events", {"event_type": "CONTACTED"}),
            ("GET", "/sales/stats", None), ("GET", "/sales/followups", None),
        ):
            assert client.request(method, path, json=body).status_code == 401
            assert client.request(method, path, headers=ordinary, json=body).status_code == 403
        assert query("SELECT COUNT(*) AS n FROM sales_leads")[0]["n"] == 1
    report["real_startup_crud_and_guards"] = True
    assert {table: query(f"SELECT * FROM {table} ORDER BY id") for table in before} == before
    report["business_tables_preserved"] = True
    print(json.dumps(report))


if __name__ == "__main__":
    worker(Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]))
