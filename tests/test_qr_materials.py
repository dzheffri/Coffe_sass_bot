"""QR designer tests use the guarded disposable local PostgreSQL schema only."""

from datetime import datetime, timedelta, timezone
from io import BytesIO
import importlib
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from PIL import Image
from psycopg.errors import ForeignKeyViolation
import pytest

from conftest import PROJECT, bearer
from app.qr_materials import destinations, platform_from_user_agent, public_base, QrError


MIGRATION = PROJECT / "migrations/20261023_shop_qr_materials.sql"
ADMIN = bearer("qr-superadmin")


@pytest.fixture
def qr_api(database, monkeypatch, tmp_path):
    monkeypatch.setenv("QR_PUBLIC_BASE_URL", "https://admin.example.com")
    monkeypatch.setenv("QR_APP_STORE_URL", "https://apps.apple.com/ua/app/nashi/id123456")
    monkeypatch.setenv("TELEGRAM_BOT_USERNAME", "nashi_test_bot")
    monkeypatch.delenv("QR_GOOGLE_PLAY_URL", raising=False)
    database.query("""ALTER TABLE coffee_shops ADD COLUMN city TEXT;
        ALTER TABLE coffee_shops ADD COLUMN logo_url TEXT;
        ALTER TABLE coffee_shops ADD COLUMN cover_url TEXT;
        ALTER TABLE coffee_shops ADD COLUMN network_id BIGINT;
        ALTER TABLE coffee_shops ADD COLUMN deleted_at TIMESTAMPTZ;
        UPDATE coffee_shops SET logo_url='/uploads/shop-logo.png',cover_url='/uploads/shop-cover.jpg',network_id=7;
    """)
    database.query(MIGRATION.read_text())
    module = importlib.import_module("app.api.qr_materials")

    def actor(authorization, init_data):
        if authorization == "Bearer qr-superadmin" or init_data == "signed-superadmin-proof":
            return {"user_id": 1, "telegram_id": 9001}
        if authorization in ("Bearer owner", "Bearer admin", "Bearer customer"):
            return {"user_id": 3, "telegram_id": 1003}
        raise HTTPException(401)

    app = FastAPI()
    app.include_router(module.build_qr_materials_router(
        verify_actor=actor, superadmin_telegram_ids={9001}, connection_factory=database.db.get_connection,
        media_storage_factory=lambda: module.QrMediaStorage(tmp_path / "qr-images"),
    ))
    with TestClient(app) as client:
        yield client, database, tmp_path


def material(client, shop_id=1):
    response = client.get(f"/superadmin/shops/{shop_id}/qr-material", headers=ADMIN)
    assert response.status_code == 200, response.text
    return response.json()["material"]


def open_page(client, token, **overrides):
    body = {"event_type": "QR_PAGE_OPEN", "event_id": str(uuid4()), **overrides}
    return client.post(f"/public/qr/{token}/events", json=body)


def test_migration_idempotent_preserves_shops_and_memberships(qr_api):
    _, db, _ = qr_api
    before_shops = db.query("SELECT * FROM coffee_shops ORDER BY id")
    before_members = db.query("SELECT * FROM shop_admins ORDER BY id")
    db.query(MIGRATION.read_text())
    assert db.query("SELECT * FROM coffee_shops ORDER BY id") == before_shops
    assert db.query("SELECT * FROM shop_admins ORDER BY id") == before_members
    assert db.query("SELECT * FROM shop_qr_materials") == []


def test_create_opaque_token_is_stable_unique_and_not_shop_id(qr_api):
    client, db, _ = qr_api
    first, second = material(client), material(client, 2)
    assert len(first["public_token"]) == 32
    assert first["public_token"] != second["public_token"]
    assert first["public_url"] == "https://admin.example.com/go/" + first["public_token"]
    assert material(client)["public_url"] == first["public_url"]
    assert len(db.query("SELECT * FROM shop_qr_materials")) == 2


def test_save_reload_design_does_not_mutate_shop_profile_or_qr(qr_api):
    client, db, _ = qr_api
    first = material(client)
    shop_before = db.query("SELECT * FROM coffee_shops WHERE id=1")[0]
    design = {**first["design"], "template": "photo", "title": "Мій заголовок", "display_name": "Моя назва",
              "logo_url": "/uploads/qr/custom-logo.png", "background_mode": "image",
              "background_image_url": "/uploads/shop-cover.jpg", "background_dim": 0.4,
              "background_blur": 3, "background_scale": 1.5, "logo_position": "above_qr"}
    response = client.put("/superadmin/shops/1/qr-material", headers=ADMIN, json=design)
    assert response.status_code == 200, response.text
    reloaded = material(client)
    assert reloaded["design"] == design
    assert reloaded["public_url"] == first["public_url"]
    assert db.query("SELECT * FROM coffee_shops WHERE id=1")[0] == shop_before


@pytest.mark.parametrize("template", ["minimal", "light", "dark", "photo"])
def test_all_templates_are_saved(qr_api, template):
    client, _, _ = qr_api
    response = client.put("/superadmin/shops/1/qr-material", headers=ADMIN, json={"template": template})
    assert response.status_code == 200
    assert material(client)["design"]["template"] == template


def test_public_landing_active_shop_has_no_internal_or_private_data(qr_api):
    client, _, _ = qr_api
    first = material(client)
    response = client.get("/public/qr/" + first["public_token"])
    assert response.status_code == 200
    assert response.json() == {"name": "Наші", "logo_url": "/uploads/shop-logo.png", "brand": "Наші",
        "destinations": [{"kind": kind, "url": f"/public/qr/{first['public_token']}/click/{kind}"}
                         for kind in ("telegram", "app_store")]}
    assert response.headers["cache-control"] == "no-store"
    for key in ("shop_id", "owner", "network_id", "stats", "design", "user_id"):
        assert key not in response.json()


@pytest.mark.parametrize("state", ["inactive", "deleted", "missing", "malformed"])
def test_unavailable_public_shops_reject_landing_and_analytics(qr_api, state):
    client, db, _ = qr_api
    token = material(client)["public_token"]
    if state == "inactive":
        db.query("UPDATE coffee_shops SET is_active=FALSE WHERE id=1")
    elif state == "deleted":
        db.query("UPDATE coffee_shops SET deleted_at=NOW() WHERE id=1")
    else:
        token = "z" * 32 if state == "missing" else "bad-token"
    assert client.get("/public/qr/" + token).status_code == 404
    assert open_page(client, token).status_code == 404
    assert client.get(f"/public/qr/{token}/click/telegram", follow_redirects=False).status_code == 404


@pytest.mark.parametrize("kind,target", [
    ("telegram", "https://t.me/nashi_test_bot"),
    ("app_store", "https://apps.apple.com/ua/app/nashi/id123456"),
])
def test_click_counts_and_redirects_to_trusted_server_destination(qr_api, kind, target):
    client, db, _ = qr_api
    token = material(client)["public_token"]
    response = client.get(f"/public/qr/{token}/click/{kind}", follow_redirects=False,
                          headers={"user-agent": "Mozilla iPhone"})
    assert response.status_code == 302
    assert response.headers["location"] == target
    row = db.query("SELECT * FROM shop_qr_events")[0]
    assert row["event_type"] == kind.upper() + "_CLICK"
    assert row["platform"] == "ios"
    assert "ip" not in row and "user_id" not in row and "user_agent" not in row


def test_google_play_hidden_when_missing_and_enabled_only_by_server(qr_api, monkeypatch):
    client, _, _ = qr_api
    token = material(client)["public_token"]
    assert "google_play" not in [item["kind"] for item in client.get("/public/qr/" + token).json()["destinations"]]
    assert client.get(f"/public/qr/{token}/click/google_play").status_code == 404
    monkeypatch.setenv("QR_GOOGLE_PLAY_URL", "https://play.google.com/store/apps/details?id=com.nashi.app")
    response = client.get(f"/public/qr/{token}/click/google_play", follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"].startswith("https://play.google.com/")


def test_event_page_open_is_deduplicated_by_request_id(qr_api):
    client, db, _ = qr_api
    token = material(client)["public_token"]
    identifier = str(uuid4())
    assert open_page(client, token, event_id=identifier).json() == {"ok": True, "recorded": True}
    assert open_page(client, token, event_id=identifier).json() == {"ok": True, "recorded": False}
    assert len(db.query("SELECT * FROM shop_qr_events")) == 1


@pytest.mark.parametrize("agent,platform", [
    ("Mozilla iPhone", "ios"), ("Mozilla iPad", "ios"), ("Macintosh Mobile Safari", "ios"),
    ("Mozilla Android Linux", "android"), ("Mozilla Windows NT", "desktop"),
    ("Mozilla Macintosh", "desktop"), ("X11 Linux", "desktop"), ("", "other"), ("bot", "other"),
])
def test_platform_detection(agent, platform):
    assert platform_from_user_agent(agent) == platform


@pytest.mark.parametrize("period,scans", [("7d", 1), ("30d", 2), ("all", 3)])
def test_stats_periods_counts_conversion_and_daily(qr_api, period, scans):
    client, db, _ = qr_api
    first = material(client)
    identifier = db.query("SELECT id FROM shop_qr_materials WHERE shop_id=1")[0]["id"]
    for age in (1, 12, 45):
        db.query("""INSERT INTO shop_qr_events(material_id,event_type,platform,created_at)
                    VALUES(%s,'QR_PAGE_OPEN','ios',%s)""",
                 (identifier, datetime.now(timezone.utc) - timedelta(days=age)))
    client.get(f"/public/qr/{first['public_token']}/click/telegram", follow_redirects=False)
    response = client.get(f"/superadmin/shops/1/qr-material/stats?period={period}", headers=ADMIN)
    assert response.status_code == 200, response.text
    stats = response.json()
    assert stats["totals"]["scans"] == scans
    assert stats["totals"]["telegram"] == 1
    assert stats["totals"]["conversion"]["telegram"] == 1 / scans
    assert sum(item["scans"] for item in stats["daily"]) == scans


def test_same_network_physical_shops_stats_are_isolated_and_global_aggregates(qr_api):
    client, _, _ = qr_api
    first, second = material(client), material(client, 2)
    open_page(client, first["public_token"])
    open_page(client, second["public_token"])
    open_page(client, second["public_token"])
    stats_a = client.get("/superadmin/shops/1/qr-material/stats?period=all", headers=ADMIN).json()
    stats_b = client.get("/superadmin/shops/2/qr-material/stats?period=all", headers=ADMIN).json()
    stats_all = client.get("/superadmin/qr-materials/stats?period=all", headers=ADMIN).json()
    assert stats_a["totals"]["scans"] == 1
    assert stats_b["totals"]["scans"] == 2
    assert stats_all["totals"]["scans"] == 3
    assert {row["shop_id"] for row in stats_all["shops"]} == {1, 2}
    assert {row["network_id"] for row in stats_all["shops"]} == {7}


@pytest.mark.parametrize("identity", [None, "owner", "admin", "customer"])
def test_editor_and_statistics_block_non_superadmin(qr_api, identity):
    client, _, _ = qr_api
    headers = bearer(identity) if identity else {}
    for path, method, body in (
        ("/qr/admin/me", "GET", None), ("/superadmin/qr-materials", "GET", None),
        ("/superadmin/qr-materials/stats", "GET", None),
        ("/superadmin/shops/1/qr-material", "GET", None),
        ("/superadmin/shops/1/qr-material", "PUT", {"title": "No"}),
        ("/superadmin/shops/1/qr-material/stats", "GET", None),
        ("/superadmin/shops/1/qr-material/image", "POST", None),
    ):
        response = client.request(method, path, headers=headers, json=body)
        assert response.status_code == (401 if identity is None else 403), (method, path, response.text)


def test_superadmin_verified_telegram_proof_and_shop_listing(qr_api):
    client, _, _ = qr_api
    assert client.get("/qr/admin/me", headers={"X-Telegram-Init-Data": "signed-superadmin-proof"}).status_code == 200
    before = client.get("/superadmin/qr-materials", headers=ADMIN).json()["shops"]
    assert len(before) == 2 and not any(item["has_material"] for item in before)
    material(client)
    after = client.get("/superadmin/qr-materials", headers=ADMIN).json()["shops"]
    assert next(item for item in after if item["shop_id"] == 1)["has_material"]


@pytest.mark.parametrize("url", ["https://evil.example/a", "javascript:alert(1)",
    "https://apps.apple.com.evil.example/id123", "https://apps.apple.com@evil.example/id123", "//evil.example/a"])
def test_server_store_config_rejects_unsafe_destinations(qr_api, monkeypatch, url):
    monkeypatch.setenv("QR_APP_STORE_URL", url)
    assert "app_store" not in destinations()


def test_click_query_cannot_override_destination(qr_api):
    client, db, _ = qr_api
    token = material(client)["public_token"]
    response = client.get(f"/public/qr/{token}/click/telegram?url=https://evil.example", follow_redirects=False)
    assert response.status_code == 422 and "location" not in response.headers
    assert db.query("SELECT * FROM shop_qr_events") == []


@pytest.mark.parametrize("body", [
    {"event_type": "APP_STORE_CLICK"}, {"event_type": "QR_PAGE_OPEN", "shop_id": 2},
    {"event_type": "QR_PAGE_OPEN", "event_id": "bad"}, {"platform": "ios"},
])
def test_public_analytics_reject_unexpected_or_malformed_data(qr_api, body):
    client, db, _ = qr_api
    token = material(client)["public_token"]
    assert client.post(f"/public/qr/{token}/events", json=body).status_code == 422
    assert db.query("SELECT * FROM shop_qr_events") == []


def test_rate_limit_is_cross_request_and_no_personal_data_is_saved(qr_api):
    client, db, _ = qr_api
    token = material(client)["public_token"]
    identifier = db.query("SELECT id FROM shop_qr_materials WHERE shop_id=1")[0]["id"]
    db.query("""INSERT INTO shop_qr_events(material_id,event_type,platform)
                SELECT %s,'QR_PAGE_OPEN','other' FROM generate_series(1,120)""", (identifier,))
    response = open_page(client, token)
    assert response.status_code == 429
    assert len(db.query("SELECT * FROM shop_qr_events")) == 120
    click = client.get(f"/public/qr/{token}/click/telegram", follow_redirects=False)
    assert click.status_code == 302
    assert click.headers["location"] == "https://t.me/nashi_test_bot"
    assert len(db.query("SELECT * FROM shop_qr_events")) == 120


@pytest.mark.parametrize("body", [
    {"logo_url": "https://evil.example/photo.png"}, {"background_image_url": "/uploads/../secret.png"},
    {"background_color": "red"}, {"background_dim": 2}, {"logo_size": 600},
    {"title": "x" * 181}, {"background_scale": 0}, {"unknown": "ignored?"},
])
def test_design_validation_rejects_unsafe_or_unbounded_values(qr_api, body):
    client, _, _ = qr_api
    assert client.put("/superadmin/shops/1/qr-material", headers=ADMIN, json=body).status_code == 422


@pytest.mark.parametrize("kind", ["PNG", "JPEG"])
def test_custom_image_upload_uses_existing_validation_and_is_independent(qr_api, kind):
    client, db, root = qr_api
    photo = BytesIO()
    Image.new("RGB", (20, 20), "#ab9e88").save(photo, format=kind)
    before = db.query("SELECT * FROM coffee_shops WHERE id=1")
    response = client.post("/superadmin/shops/1/qr-material/image", headers=ADMIN,
        files={"file": ("original-name.jpg", photo.getvalue(), "image/jpeg")})
    assert response.status_code == 201, response.text
    url = response.json()["url"]
    assert url.startswith("/uploads/qr/") and "original-name" not in url
    assert (root / "qr-images" / url.split("/")[-1]).exists()
    assert db.query("SELECT * FROM coffee_shops WHERE id=1") == before


def test_image_rejects_unsupported_and_oversized_media(qr_api):
    client, _, _ = qr_api
    for content, status in ((b"<svg></svg>", 415), (b"x" * (8 * 1024 * 1024 + 1), 413)):
        response = client.post("/superadmin/shops/1/qr-material/image", headers=ADMIN,
            files={"file": ("photo.png", content, "image/png")})
        assert response.status_code == status, response.text


def test_fk_cascade_deletes_only_matching_shop_material_and_events(qr_api):
    client, db, _ = qr_api
    first, second = material(client), material(client, 2)
    open_page(client, first["public_token"])
    open_page(client, second["public_token"])
    with pytest.raises(ForeignKeyViolation):
        db.query("INSERT INTO shop_qr_materials(shop_id,public_token) VALUES(999,%s)", ("a" * 32,))
    db.query("DELETE FROM coffee_shops WHERE id=1")
    assert len(db.query("SELECT * FROM shop_qr_events")) == 1
    assert db.query("SELECT shop_id FROM shop_qr_materials") == [{"shop_id": 2}]


def test_public_base_rejects_request_host_style_injection(monkeypatch):
    monkeypatch.setenv("QR_PUBLIC_BASE_URL", "https://evil.example/path?redirect=https://elsewhere.example")
    with pytest.raises(QrError):
        public_base()
