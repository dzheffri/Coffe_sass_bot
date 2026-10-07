"""Real Sales router/repository tests against guarded disposable PostgreSQL.

The fixture never imports app.db or configuration startup. Every SQL operation,
including the explicit migration and failure triggers, uses the conftest guard.
"""

import ast
import hashlib
import hmac
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from threading import Barrier
from urllib.parse import parse_qsl
import uuid
from zoneinfo import ZoneInfo

import psycopg
from psycopg import sql
import pytest
from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient

from conftest import PROJECT, TEST_ROOT, extract_definitions
from test_sales_auth import TEST_BOT_TOKEN, signed_init_data


MIGRATION = PROJECT / "migrations/20261006_sales_leads.sql"
AUTH = {"Authorization": "Bearer sales-superadmin"}
STATUSES = ("NEW", "NOT_CONTACTED", "CONTACTED", "REPLIED", "INTERESTED",
            "DEMO", "THINKING", "CONNECTED", "REJECTED", "DO_NOT_CONTACT")
BASE_SCHEMA = """
CREATE TABLE users (
 id BIGSERIAL PRIMARY KEY, telegram_user_id BIGINT UNIQUE,
 username TEXT, full_name TEXT, personal_qr_token TEXT UNIQUE NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE coffee_shops (
 id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, city TEXT, address TEXT, instagram TEXT DEFAULT '',
 is_active BOOLEAN NOT NULL DEFAULT TRUE,
 pending_owner_telegram_id BIGINT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO users(id,telegram_user_id,personal_qr_token)
 VALUES (1,1001,'sales-actor'),(2,1002,'other-actor');
INSERT INTO coffee_shops(id,name,city,address)
 VALUES (1,'Підключена кав’ярня','Київ','Хрещатик 1'),
        (2,'Closed cafe','Львів','Вулиця 2');
UPDATE coffee_shops SET is_active=FALSE WHERE id=2;
"""


class SalesDatabase:
    def __init__(self, connect):
        self.connect = connect

    def query(self, statement, values=()):
        with self.connect() as connection:
            cursor = connection.execute(statement, values)
            return cursor.fetchall() if cursor.description else []


@pytest.fixture
def sales_database(local_connection):
    dsn, state, connect = local_connection
    schema = "sales_crud_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    state["schema"] = schema
    db = SalesDatabase(connect)
    try:
        db.query(BASE_SCHEMA)
        db.query(MIGRATION.read_text(encoding="utf-8"))
        yield db
    finally:
        state["schema"] = None
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def sales_api(sales_database):
    from app.api.sales import build_sales_router

    source = ast.parse((PROJECT / "app/api/main.py").read_text(encoding="utf-8"))
    allowlist = next(ast.literal_eval(node.value) for node in source.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "SUPERADMIN_TELEGRAM_IDS"
                for target in node.targets))
    superadmin = next(iter(allowlist))
    ordinary = next(value for value in range(81001, 81100) if value not in allowlist)
    namespace = {
        "Header": Header, "HTTPException": HTTPException,
        "get_app_session": {"sales-superadmin": {"user_id": 1, "telegram_user_id": superadmin},
                            "sales-ordinary": {"user_id": 2, "telegram_user_id": ordinary},
                            "sales-unlinked": {"user_id": 2, "telegram_user_id": None}}.get,
        "get_user_by_identity": lambda provider, subject: {"id": 1} if subject == str(superadmin)
            else ({"id": 2} if subject == str(ordinary) else None),
        "BOT_TOKEN": TEST_BOT_TOKEN, "datetime": datetime, "timezone": timezone,
        "parse_qsl": parse_qsl, "json": json, "hmac": hmac, "hashlib": hashlib,
    }
    extract_definitions(PROJECT / "app/api/main.py", {
        "_get_bearer_token_or_401", "get_current_user", "get_verified_telegram_actor",
        "validate_telegram_init_data",
    }, namespace)
    app = FastAPI()
    app.include_router(build_sales_router(
        verify_actor=namespace["get_verified_telegram_actor"],
        superadmin_telegram_ids=allowlist, connection_factory=sales_database.connect))
    with TestClient(app) as client:
        yield client, sales_database, superadmin, ordinary


def create(client, **values):
    response = client.post("/sales/leads", headers=AUTH,
                           json={"name": "Local sales cafe", **values})
    assert response.status_code == 201, response.text
    return response.json()


def test_create_list_detail_and_update_real_router(sales_api):
    client, db, *_ = sales_api
    created = create(client, name="  Кава для вас  ", city="Київ", address="Вулиця 4",
                     phone="(067) 123-45-67", instagram="https://instagram.com/for_you_coffee/",
                     website="https://www.example.com/", rating=4.8, reviews_count=23,
                     notes="Початкова нотатка")
    lead = created["lead"]
    assert lead["name"] == "Кава для вас"
    assert lead["phone"] == "(067) 123-45-67"
    assert lead["phone_normalized"] == "+380671234567"
    assert lead["website_normalized"] == "example.com"
    assert lead["instagram_normalized"] == "for_you_coffee"
    assert lead["status"] == "NEW"
    assert lead["created_by_user_id"] == lead["updated_by_user_id"] == 1
    assert created["duplicate_warnings"] == []
    assert any(event["event_type"] == "CREATED" and event["actor_user_id"] == 1
               for event in created["events"])
    listing = client.get("/sales/leads", headers=AUTH).json()
    assert listing["total"] == 1 and listing["limit"] == 20 and listing["offset"] == 0
    assert listing["items"][0]["id"] == lead["id"]
    detail = client.get(f"/sales/leads/{lead['id']}", headers=AUTH)
    assert detail.status_code == 200
    assert detail.json()["lead"] == lead
    assert detail.json()["events"] == created["events"]
    patched = client.patch(f"/sales/leads/{lead['id']}", headers=AUTH,
                           json={"name": "Кава оновлена", "city": "Львів", "score": 81})
    assert patched.status_code == 200, patched.text
    after = patched.json()
    assert after["lead"]["name"] == "Кава оновлена" and after["lead"]["score"] == 81
    assert after["lead"]["id"] == lead["id"]
    assert after["lead"]["created_at"] == lead["created_at"]
    assert any(event["event_type"] == "UPDATED" for event in after["events"])
    assert db.query("SELECT COUNT(*) AS n FROM sales_leads")[0]["n"] == 1
    for response in (detail, patched):
        assert response.headers["Cache-Control"] == "no-store"


def test_list_filters_and_pagination(sales_api):
    client, *_ = sales_api
    first = create(client, name="Кава Alpha", city="Київ", status="INTERESTED")
    create(client, name="Кава Бета", city="Київ", status="NEW")
    create(client, name="Кавова Гама", city="Львів", status="INTERESTED")
    filtered = client.get("/sales/leads", headers=AUTH,
                          params={"q": "alpha", "city": "Київ", "status": "INTERESTED"})
    assert filtered.status_code == 200, filtered.text
    assert filtered.json()["total"] == 1
    assert filtered.json()["items"][0]["id"] == first["lead"]["id"]
    paged = client.get("/sales/leads?limit=1&offset=1", headers=AUTH).json()
    assert paged["total"] == 3 and len(paged["items"]) == 1
    assert paged["limit"] == paged["offset"] == 1


def test_status_notes_followup_and_manual_events_keep_verified_actor(sales_api):
    client, db, *_ = sales_api
    lead_id = create(client)["lead"]["id"]
    followup = "2026-10-10T12:30:00+03:00"
    response = client.patch(f"/sales/leads/{lead_id}", headers=AUTH,
                            json={"status": "INTERESTED", "notes": "Нові нотатки",
                                  "next_followup_at": followup})
    assert response.status_code == 200, response.text
    events = response.json()["events"]
    changed = next(event for event in events if event["event_type"] == "STATUS_CHANGED")
    assert changed["from_status"] == "NEW" and changed["to_status"] == "INTERESTED"
    assert any(event["event_type"] == "FOLLOWUP_SET" for event in events)
    for body in ({"event_type": "NOTE_ADDED", "note": "Ручна нотатка"},
                 {"event_type": "CONTACTED", "note": "Зателефонували вручну"}):
        response = client.post(f"/sales/leads/{lead_id}/events", headers=AUTH, json=body)
        assert response.status_code in (200, 201), response.text
        assert any(event["event_type"] == body["event_type"] and event["note"] == body["note"]
                   for event in response.json()["events"])
    detail = client.get(f"/sales/leads/{lead_id}", headers=AUTH).json()
    assert detail["lead"]["last_contact_at"] is not None
    assert all(event["actor_user_id"] == 1 for event in detail["events"])
    assert db.query("SELECT COUNT(*) AS n FROM sales_leads")[0]["n"] == 1


@pytest.mark.parametrize("field,value", [("id", 500), ("created_at", "2026-01-01T00:00:00Z"),
    ("created_by_user_id", 2), ("updated_by_user_id", 2), ("actor_user_id", 2),
    ("user_id", 2), ("role", "superadmin"), ("is_superadmin", True)])
def test_client_cannot_set_actor_or_immutable_fields(sales_api, field, value):
    client, *_ = sales_api
    lead_id = create(client)["lead"]["id"]
    for method, path, payload in (
        ("POST", "/sales/leads", {"name": "Forged create", field: value}),
        ("PATCH", f"/sales/leads/{lead_id}", {"name": "Forged update", field: value}),
        ("POST", f"/sales/leads/{lead_id}/events",
         {"event_type": "NOTE_ADDED", "note": "Forged event", field: value}),
    ):
        assert client.request(method, path, headers=AUTH, json=payload).status_code == 422
    detail = client.get(f"/sales/leads/{lead_id}", headers=AUTH).json()
    assert detail["lead"]["name"] == "Local sales cafe"
    assert len(detail["events"]) == 1


ROUTES = (("GET", "/sales/leads", None), ("GET", "/sales/leads/999999", None),
          ("POST", "/sales/leads", {"name": "Unauthorized"}),
          ("PATCH", "/sales/leads/999999", {"status": "INTERESTED"}),
          ("POST", "/sales/leads/999999/events", {"event_type": "CONTACTED"}),
          ("GET", "/sales/followups", None), ("GET", "/sales/stats", None))


@pytest.mark.parametrize("method,path,body", ROUTES)
def test_all_crud_endpoints_require_verified_superadmin(sales_api, method, path, body):
    client, db, superadmin, ordinary = sales_api
    for headers, expected in (
        ({}, 401), ({"Authorization": "Bearer unknown"}, 401),
        ({"X-Telegram-Id": str(superadmin), "X-Role": "superadmin"}, 401),
        ({"Authorization": "Bearer sales-ordinary", "X-Is-Superadmin": "true"}, 403),
        ({"X-Telegram-Init-Data": signed_init_data(ordinary)}, 403),
        ({"Authorization": "Bearer sales-unlinked"}, 403),
    ):
        response = client.request(method, path, headers=headers, json=body)
        assert response.status_code == expected, (method, path, expected, response.text)
    assert db.query("SELECT COUNT(*) AS n FROM sales_leads")[0]["n"] == 0


@pytest.mark.parametrize("method,path,body", ROUTES)
def test_authorized_actor_query_spoofing_is_rejected(sales_api, method, path, body):
    client, *_ = sales_api
    for headers, expected in ((AUTH, 422), ({}, 401),
                              ({"Authorization": "Bearer sales-ordinary"}, 403)):
        response = client.request(method, path + "?actor_user_id=2&role=superadmin",
                                  headers=headers, json=body)
        assert response.status_code == expected, response.text


def test_signed_miniapp_can_create_lead_with_backend_actor(sales_api):
    client, _, superadmin, _ = sales_api
    response = client.post("/sales/leads", json={"name": "Signed actor"},
        headers={"X-Telegram-Init-Data": signed_init_data(superadmin)})
    assert response.status_code == 201, response.text
    assert response.json()["lead"]["created_by_user_id"] == 1


def test_same_place_id_conflicts_while_null_place_ids_are_independent(sales_api):
    client, db, *_ = sales_api
    first = create(client, place_id="local-place-unique")["lead"]
    duplicate = client.post("/sales/leads", headers=AUTH,
                            json={"name": "Conflicting place", "place_id": "local-place-unique"})
    assert duplicate.status_code == 409, duplicate.text
    assert db.query("SELECT id FROM sales_leads") == [{"id": first["id"]}]
    create(client, place_id=None)
    create(client, place_id=None)
    assert db.query("SELECT COUNT(*) AS n FROM sales_leads")[0]["n"] == 3


@pytest.mark.parametrize("field,first_value,second_value", [
    ("phone", "+380 (67) 123-45-67", "0671234567"),
    ("website", "https://www.example.com/", "example.com"),
    ("instagram", "https://instagram.com/for_you_coffee/", "@for_you_coffee"),
])
def test_contact_matches_warn_without_merging(sales_api, field, first_value, second_value):
    client, db, *_ = sales_api
    first = create(client, name="Original network cafe", **{field: first_value})["lead"]
    second = create(client, name="New branch", **{field: second_value})
    assert second["lead"]["id"] != first["id"]
    assert any(warning["code"] == "POSSIBLE_DUPLICATE" and warning["field"] == field
               and warning["lead_id"] == first["id"] and warning["lead_name"] == first["name"]
               for warning in second["duplicate_warnings"])
    assert any(event["event_type"] == "DUPLICATE_WARNING" for event in second["events"])
    assert db.query("SELECT COUNT(*) AS n FROM sales_leads")[0]["n"] == 2
    assert client.get(f"/sales/leads/{first['id']}", headers=AUTH).json()["lead"] == first


def test_preexisting_connected_shop_match_warns_without_automatic_link(sales_api):
    client, *_ = sales_api
    created = create(client, name="Підключена кав’ярня", city="Київ", address="Хрещатик 1")
    assert any(warning["code"] == "CONNECTED_SHOP_MATCH" and warning["shop_id"] == 1
               for warning in created["duplicate_warnings"])
    assert created["lead"]["connected_shop_id"] is None
    assert created["lead"]["status"] == "NEW"


def test_contact_update_works_when_core_shop_has_no_instagram_column(sales_api):
    client, db, *_ = sales_api
    lead_id = create(client, name="Core schema cafe")["lead"]["id"]
    db.query("ALTER TABLE coffee_shops DROP COLUMN instagram")
    response = client.patch(f"/sales/leads/{lead_id}", headers=AUTH,
                            json={"instagram": "@local_sales_test"})
    assert response.status_code == 200, response.text
    assert response.json()["lead"]["instagram"] == "@local_sales_test"


def test_connected_requires_existing_active_shop(sales_api):
    client, *_ = sales_api
    lead_id = create(client)["lead"]["id"]
    for patch in ({"status": "CONNECTED"}, {"status": "CONNECTED", "connected_shop_id": 9999},
                  {"status": "CONNECTED", "connected_shop_id": 2}):
        response = client.patch(f"/sales/leads/{lead_id}", headers=AUTH, json=patch)
        assert response.status_code in (409, 422), response.text
    connected = client.patch(f"/sales/leads/{lead_id}", headers=AUTH,
                             json={"status": "CONNECTED", "connected_shop_id": 1})
    assert connected.status_code == 200, connected.text
    assert connected.json()["lead"]["connected_shop_id"] == 1
    assert connected.json()["lead"]["status"] == "CONNECTED"
    assert any(event["event_type"] == "CONNECTED_TO_SHOP" for event in connected.json()["events"])


def test_do_not_contact_blocks_contact_event_and_contacted_status(sales_api):
    client, *_ = sales_api
    lead_id = create(client, status="DO_NOT_CONTACT")["lead"]["id"]
    before = client.get(f"/sales/leads/{lead_id}", headers=AUTH).json()
    for method, path, body in (
        ("POST", f"/sales/leads/{lead_id}/events", {"event_type": "CONTACTED"}),
        ("PATCH", f"/sales/leads/{lead_id}", {"status": "CONTACTED"}),
    ):
        response = client.request(method, path, headers=AUTH, json=body)
        assert response.status_code == 409, response.text
        assert response.json()["detail"]["code"] == "CONTACT_FORBIDDEN"
    assert client.get(f"/sales/leads/{lead_id}", headers=AUTH).json() == before
    note = client.post(f"/sales/leads/{lead_id}/events", headers=AUTH,
                       json={"event_type": "NOTE_ADDED", "note": "Причина заборони"})
    assert note.status_code in (200, 201), note.text


def test_followup_buckets_use_kyiv_dates_and_exclude_terminal_leads(sales_api):
    client, db, *_ = sales_api
    today = db.query("SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Kyiv')::date AS day")[0]["day"]
    zone = ZoneInfo("Europe/Kyiv")
    dates = {"overdue": datetime.combine(today - timedelta(days=1), datetime.min.time(), zone),
             "today": datetime.combine(today, datetime.min.time(), zone) + timedelta(minutes=1),
             "upcoming": datetime.combine(today + timedelta(days=1), datetime.min.time(), zone)}
    active_ids = {bucket: create(client, name=bucket, next_followup_at=date.isoformat())["lead"]["id"]
                  for bucket, date in dates.items()}
    for status in ("CONNECTED", "REJECTED", "DO_NOT_CONTACT"):
        create(client, name=status, status=status, next_followup_at=dates["today"].isoformat(),
               **({"connected_shop_id": 1} if status == "CONNECTED" else {}))
    all_active = client.get("/sales/followups", headers=AUTH).json()
    assert all_active["total"] == 3
    assert {lead["id"] for lead in all_active["items"]} == set(active_ids.values())
    for bucket, lead_id in active_ids.items():
        response = client.get("/sales/followups", headers=AUTH, params={bucket: "true"})
        assert response.status_code == 200, response.text
        assert response.json()["total"] == 1
        assert response.json()["items"][0]["id"] == lead_id
        listing = client.get("/sales/leads", headers=AUTH, params={"followup": bucket})
        assert listing.status_code == 200, listing.text
        assert {lead["id"] for lead in listing.json()["items"]} == {lead_id}
    assert client.get("/sales/followups?today=true&overdue=true", headers=AUTH).status_code == 422
    stats = client.get("/sales/stats", headers=AUTH).json()
    assert stats["followups_today"] == 1 and stats["followups_overdue"] == 1
    # A timestamp just after Kyiv midnight falls on the previous UTC date.
    assert dates["today"].astimezone(timezone.utc).date() == today - timedelta(days=1)


def test_empty_stats_then_real_status_counts(sales_api):
    client, *_ = sales_api
    empty = client.get("/sales/stats", headers=AUTH)
    assert empty.status_code == 200 and all(value == 0 for value in empty.json().values())
    for status in STATUSES:
        create(client, status=status, **({"connected_shop_id": 1} if status == "CONNECTED" else {}))
    stats = client.get("/sales/stats", headers=AUTH).json()
    assert stats["total"] == len(STATUSES)
    for status in STATUSES:
        assert stats[status.lower()] == 1


@pytest.mark.parametrize("method,path,payload", [
    ("POST", "/sales/leads", {"name": "  "}),
    ("POST", "/sales/leads", {"name": "Bad", "status": "UNKNOWN"}),
    ("POST", "/sales/leads", {"name": "Bad", "rating": 5.1}),
    ("POST", "/sales/leads", {"name": "Bad", "reviews_count": -1}),
    ("POST", "/sales/leads/1/events", {"event_type": "CREATED"}),
    ("GET", "/sales/leads?limit=0", None),
    ("GET", "/sales/leads?limit=1001", None),
    ("GET", "/sales/leads?offset=-1", None),
    ("GET", "/sales/leads?status=UNKNOWN", None),
    ("GET", "/sales/leads?followup=UNKNOWN", None),
])
def test_actual_endpoint_request_validation(sales_api, method, path, payload):
    client, *_ = sales_api
    assert client.request(method, path, headers=AUTH, json=payload).status_code == 422


def test_missing_lead_is_404_for_detail_update_and_event(sales_api):
    client, *_ = sales_api
    for method, path, body in (("GET", "/sales/leads/999999", None),
        ("PATCH", "/sales/leads/999999", {"name": "Absent"}),
        ("POST", "/sales/leads/999999/events", {"event_type": "NOTE_ADDED", "note": "Absent"})):
        response = client.request(method, path, headers=AUTH, json=body)
        assert response.status_code == 404, response.text


def test_migration_idempotence_indexes_and_history_foreign_keys(sales_api):
    client, db, *_ = sales_api
    lead_id = create(client, status="CONNECTED", connected_shop_id=1)["lead"]["id"]
    before = db.query("SELECT * FROM sales_leads")
    db.query(MIGRATION.read_text(encoding="utf-8"))
    assert db.query("SELECT * FROM sales_leads") == before
    indexes = db.query("""SELECT indexdef,indisunique,indisvalid,indisready
        FROM pg_indexes p JOIN pg_class c ON c.relname=p.indexname
        JOIN pg_namespace ns ON ns.oid=c.relnamespace AND ns.nspname=current_schema()
        JOIN pg_index i ON i.indexrelid=c.oid
        WHERE p.schemaname=current_schema() AND p.tablename='sales_leads'""")
    assert all(index["indisvalid"] and index["indisready"] for index in indexes)
    assert any(index["indisunique"] and "place_id" in index["indexdef"]
               and "IS NOT NULL" in index["indexdef"] for index in indexes)
    for column in ("phone_normalized", "website_normalized", "instagram_normalized"):
        assert any(column in index["indexdef"] for index in indexes)
    event_count = db.query("SELECT COUNT(*) AS n FROM sales_lead_events")[0]["n"]
    db.query("DELETE FROM users WHERE id=1")
    assert db.query("SELECT created_by_user_id,updated_by_user_id FROM sales_leads WHERE id=%s",
                    (lead_id,))[0] == {"created_by_user_id": None, "updated_by_user_id": None}
    assert all(event["actor_user_id"] is None for event in db.query("SELECT actor_user_id FROM sales_lead_events"))
    assert db.query("SELECT COUNT(*) AS n FROM sales_lead_events")[0]["n"] == event_count
    db.query("DELETE FROM coffee_shops WHERE id=1")
    assert db.query("SELECT id,connected_shop_id FROM sales_leads WHERE id=%s", (lead_id,)) == [
        {"id": lead_id, "connected_shop_id": None}]
    assert db.query("SELECT COUNT(*) AS n FROM sales_lead_events")[0]["n"] == event_count


def install_event_failure(db):
    db.query("""CREATE FUNCTION sales_test_fail_event() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'isolated event failure'; END $$;
        CREATE TRIGGER sales_test_event_failure BEFORE INSERT ON sales_lead_events
        FOR EACH ROW EXECUTE FUNCTION sales_test_fail_event();""")


def test_failed_event_rolls_back_create_and_update(sales_database):
    from app.sales_db import SalesRepository

    db = sales_database
    repository = SalesRepository(db.connect)
    lead_id = repository.create_lead({"name": "Existing lead"}, 1)["lead"]["id"]
    before_leads = db.query("SELECT * FROM sales_leads ORDER BY id")
    before_events = db.query("SELECT * FROM sales_lead_events ORDER BY id")
    install_event_failure(db)
    with pytest.raises(psycopg.Error, match="isolated event failure"):
        repository.create_lead({"name": "Rolled back lead"}, 1)
    with pytest.raises(psycopg.Error, match="isolated event failure"):
        repository.update_lead(lead_id, {"status": "INTERESTED", "notes": "Rollback"}, 1)
    with pytest.raises(psycopg.Error, match="isolated event failure"):
        repository.add_event(lead_id, {"event_type": "CONTACTED"}, 1)
    assert db.query("SELECT * FROM sales_leads ORDER BY id") == before_leads
    assert db.query("SELECT * FROM sales_lead_events ORDER BY id") == before_events


def test_concurrent_place_id_create_has_one_lead_and_one_created_event(sales_database):
    from app.sales_db import SalesError, SalesRepository

    db = sales_database
    barrier = Barrier(2)
    def create_concurrently(index):
        barrier.wait(timeout=5)
        try:
            result = SalesRepository(db.connect).create_lead(
                {"name": f"Concurrent branch {index}", "place_id": "same-concurrent-place"}, 1)
            return "created", result["lead"]["id"]
        except SalesError as error:
            return "conflict", error.status_code
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(create_concurrently, (1, 2)))
    assert [item[0] for item in results].count("created") == 1
    assert ("conflict", 409) in results
    assert db.query("SELECT COUNT(*) AS n FROM sales_leads")[0]["n"] == 1
    assert db.query("SELECT COUNT(*) AS n FROM sales_lead_events WHERE event_type='CREATED'")[0]["n"] == 1


@pytest.mark.parametrize("function,value,expected", [
    ("normalize_phone", "067 123-45-67", "+380671234567"),
    ("normalize_phone", "+380 (67) 123-45-67", "+380671234567"),
    ("normalize_phone", "380671234567", "+380671234567"),
    ("normalize_phone", "12345", None),
    ("normalize_phone", "call 0671234567", None),
    ("normalize_phone", "+380671234567 ext 1", None),
    ("normalize_phone", None, None),
    ("normalize_website", "https://example.com/", "example.com"),
    ("normalize_website", "https://www.example.com", "example.com"),
    ("normalize_website", "example.com", "example.com"),
    ("normalize_website", "https://EXAMPLE.com/path?q=1", "example.com"),
    ("normalize_website", "javascript:alert(1)", None),
    ("normalize_website", "https://user:password@example.com", None),
    ("normalize_instagram", "https://instagram.com/for_you_coffee/", "for_you_coffee"),
    ("normalize_instagram", "@for_you_coffee", "for_you_coffee"),
    ("normalize_instagram", "for_you_coffee", "for_you_coffee"),
    ("normalize_instagram", "https://evil.example/for_you_coffee/", None),
    ("normalize_instagram", "https://instagram.com/p/post-id/", None),
])
def test_contact_normalization_is_conservative(function, value, expected):
    from app import sales_contacts

    assert getattr(sales_contacts, function)(value) == expected


def test_real_backend_startup_wiring_and_explicit_local_migration(local_connection):
    dsn, _, _ = local_connection
    schema = "full_security_sales_crud_" + uuid.uuid4().hex
    audit_root = Path(tempfile.mkdtemp(prefix="sales_crud_", dir=TEST_ROOT))
    environment = {
        "PATH": os.environ.get("PATH", ""), "TEST_DATABASE_URL": dsn,
        "PYTHONDONTWRITEBYTECODE": "1", "PYTHON_DOTENV_DISABLED": "1",
        "BARISTA_LOGIN_ENABLED": "false", "SUPER_ADMIN_IDS": "81001",
    }
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        result = subprocess.run([
            sys.executable, str(PROJECT / "tests/sales_leads_worker.py"),
            str(PROJECT), schema, str(audit_root),
        ], cwd=audit_root, env=environment, capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stdout + result.stderr
        report = json.loads(result.stdout.strip().splitlines()[-1])
        assert report == {"runtime_did_not_create_sales_tables": True,
                          "real_startup_crud_and_guards": True,
                          "business_tables_preserved": True}
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        shutil.rmtree(audit_root)
