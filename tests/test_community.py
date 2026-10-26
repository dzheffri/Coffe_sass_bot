"""Community MVP API tests against the guarded disposable PostgreSQL fixture."""

from io import BytesIO
import importlib
import sys
from types import ModuleType
from unittest.mock import AsyncMock
from types import SimpleNamespace

from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient
from PIL import Image
from psycopg.errors import UniqueViolation
import pytest

from conftest import PROJECT, bearer


MIGRATION = PROJECT / "migrations/20261015_community_posts.sql"
SOFT_DELETE_MIGRATION = PROJECT / "migrations/20261015_community_soft_delete.sql"
ACTIVE_SERIES_MIGRATION = PROJECT / "migrations/20261019_community_series_active_uniqueness.sql"
TELEGRAM_MIGRATION = PROJECT / "migrations/20261025_community_telegram.sql"
DESTINATIONS_MIGRATION = PROJECT / "migrations/20261026_community_destinations.sql"


def _photo_bytes(kind="PNG"):
    output = BytesIO()
    Image.new("RGB", (16, 16), "#f4d58c").save(output, format=kind)
    return output.getvalue()


@pytest.fixture
def community_api(database, tmp_path):
    database.query(MIGRATION.read_text(encoding="utf-8"))
    database.query(SOFT_DELETE_MIGRATION.read_text(encoding="utf-8"))
    database.query(ACTIVE_SERIES_MIGRATION.read_text(encoding="utf-8"))
    database.query(TELEGRAM_MIGRATION.read_text(encoding="utf-8"))
    database.query(DESTINATIONS_MIGRATION.read_text(encoding="utf-8"))
    module = importlib.import_module("app.api.community")

    def auth(authorization: str | None = Header(default=None)):
        if not authorization or not authorization.lower().startswith("bearer "):
            raise HTTPException(401)
        session = database.db.get_app_session(authorization.split(" ", 1)[1])
        if session is None:
            raise HTTPException(401)
        return session

    def actor(authorization: str | None, init_data: str | None):
        current = auth(authorization)
        return {"user_id": current["user_id"], "telegram_id": current.get("telegram_user_id")}

    app = FastAPI()
    app.include_router(module.build_community_router(
        authenticate=auth,
        verify_actor=actor,
        superadmin_telegram_ids={1001},
        connection_factory=database.db.get_connection,
        media_storage_factory=lambda: module.CommunityMediaStorage(tmp_path / "community-upload"),
        telegram_bot_factory=lambda: SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=1)),
            send_photo=AsyncMock(return_value=SimpleNamespace(message_id=1)),
            send_poll=AsyncMock(return_value=SimpleNamespace(message_id=1)), session=SimpleNamespace(close=AsyncMock())),
    ))
    with TestClient(app) as client:
        yield client, database


def _tokens(db):
    return db.db.create_app_session(1), db.db.create_app_session(2)


def _third_token(db):
    return db.db.create_app_session(3)


def _draft(client, token, **overrides):
    body = {"type": "news", "title": "Новина", "text": "Текст"}
    body.update(overrides)
    response = client.post("/community/admin/posts", headers=bearer(token), json=body)
    assert response.status_code == 201, response.text
    return response.json()


def test_migration_is_idempotent_and_admin_is_allowlisted(community_api):
    client, db = community_api
    admin, ordinary = _tokens(db)
    db.query(MIGRATION.read_text(encoding="utf-8"))
    assert client.get("/community/admin/me", headers=bearer(admin)).json()["role"] == "superadmin"
    assert client.get("/community/admin/me", headers=bearer(ordinary)).status_code == 403
    assert client.get("/me/community/posts", headers=bearer(ordinary)).json()["total"] == 0


def test_every_admin_route_rejects_anonymous_and_non_allowlisted_actor(community_api):
    client, db = community_api
    admin, ordinary = _tokens(db)
    owner_or_admin = _third_token(db)
    draft = _draft(client, admin)
    paths = [
        ("GET", "/community/admin/me", None),
        ("GET", "/community/admin/posts", None),
        ("POST", "/community/admin/posts", {"type": "news", "title": "x", "text": "y"}),
        ("GET", f"/community/admin/posts/{draft['id']}", None),
        ("DELETE", f"/community/admin/posts/{draft['id']}", None),
        ("PUT", f"/community/admin/posts/{draft['id']}", {"type": "news", "title": "x", "text": "y"}),
        ("POST", f"/community/admin/posts/{draft['id']}/image", None),
        ("POST", f"/community/admin/posts/{draft['id']}/publish", {"notify": False}),
    ]
    for method, path, body in paths:
        for headers in ({}, bearer(ordinary), bearer(owner_or_admin)):
            response = client.request(method, path, headers=headers, json=body)
            assert response.status_code in {401, 403}, (method, path, response.status_code, response.text)


def test_unpublished_posts_are_hidden_and_publish_is_idempotent(community_api, monkeypatch):
    client, db = community_api
    admin, customer = _tokens(db)
    draft = _draft(client, admin)
    assert client.get(f"/me/community/posts/{draft['id']}", headers=bearer(customer)).status_code == 404
    assert client.get("/me/community/posts", headers=bearer(customer)).json()["total"] == 0

    pushes = ModuleType("app.app_push")
    pushes.send_app_pushes = AsyncMock()
    monkeypatch.setitem(sys.modules, "app.app_push", pushes)
    published = client.post(f"/community/admin/posts/{draft['id']}/publish",
                            headers=bearer(admin), json={"notify": True})
    assert published.status_code == 200, published.text
    assert published.json()["is_published"] is True
    again = client.post(f"/community/admin/posts/{draft['id']}/publish",
                        headers=bearer(admin), json={"notify": True})
    assert again.status_code == 200 and again.json()["id"] == draft["id"]
    # There are no app_push_devices in the base fixture, so no external send is attempted.
    pushes.send_app_pushes.assert_not_awaited()
    assert client.get(f"/me/community/posts/{draft['id']}", headers=bearer(customer)).status_code == 200


def test_unpublished_detail_reaction_and_vote_are_not_public(community_api):
    client, db = community_api
    admin, customer = _tokens(db)
    news = _draft(client, admin)
    poll = _draft(client, admin, type="poll", title="Питання", text="",
                  question="Q", options=["A", "B"])
    for post in (news, poll):
        assert client.get(f"/me/community/posts/{post['id']}", headers=bearer(customer)).status_code == 404
        assert client.put(f"/me/community/posts/{post['id']}/reaction", headers=bearer(customer),
                          json={"interested": True}).status_code == 404
    assert client.post(f"/me/community/posts/{poll['id']}/vote", headers=bearer(customer),
                       json={"option_id": 1}).status_code == 404


def test_publish_without_notification_keeps_claim_false_and_never_replays(community_api, monkeypatch):
    client, db = community_api
    admin, customer = _tokens(db)
    pushes = ModuleType("app.app_push")
    pushes.send_app_pushes = AsyncMock()
    monkeypatch.setitem(sys.modules, "app.app_push", pushes)
    draft = _draft(client, admin, title="Без push")
    first = client.post(f"/community/admin/posts/{draft['id']}/publish", headers=bearer(admin), json={})
    repeat = client.post(f"/community/admin/posts/{draft['id']}/publish", headers=bearer(admin),
                         json={"notify": True})
    assert first.status_code == repeat.status_code == 200
    assert first.json()["notification_attempted"] is False
    assert repeat.json()["notification_attempted"] is False
    assert pushes.send_app_pushes.assert_not_awaited() is None
    assert client.get(f"/me/community/posts/{draft['id']}", headers=bearer(customer)).status_code == 200


def test_first_publish_claims_before_one_deduplicated_app_push(community_api, monkeypatch):
    client, db = community_api
    admin, _ = _tokens(db)
    db.query("""CREATE TABLE app_push_devices (
        id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        device_token TEXT NOT NULL UNIQUE, platform TEXT NOT NULL DEFAULT 'ios',
        environment TEXT NOT NULL DEFAULT 'production', notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE
    )""")
    db.query("INSERT INTO app_push_devices(user_id,device_token) VALUES(1,'one'),(2,'two')")
    pushes = ModuleType("app.app_push")
    pushes.send_app_pushes = AsyncMock()
    monkeypatch.setitem(sys.modules, "app.app_push", pushes)
    draft = _draft(client, admin, title="Головна новина")
    first = client.post(f"/community/admin/posts/{draft['id']}/publish", headers=bearer(admin),
                        json={"notify": True})
    second = client.post(f"/community/admin/posts/{draft['id']}/publish", headers=bearer(admin),
                         json={"notify": True})
    assert first.status_code == second.status_code == 200
    pushes.send_app_pushes.assert_awaited_once()
    args, kwargs = pushes.send_app_pushes.await_args
    assert len(args[0]) == 2
    assert kwargs == {"title": "Наші ☕️", "body": "Головна новина",
                      "data": {"community_post_id": draft["id"]}}
    assert first.json()["notification_attempted"] is True


def test_push_audience_ignores_disabled_non_ios_and_duplicate_normalized_tokens(community_api, monkeypatch):
    client, db = community_api
    admin, _ = _tokens(db)
    db.query("""CREATE TABLE app_push_devices (
        id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        device_token TEXT NOT NULL UNIQUE, platform TEXT NOT NULL DEFAULT 'ios',
        environment TEXT NOT NULL DEFAULT 'production', notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE
    )""")
    db.query("""INSERT INTO app_push_devices(user_id,device_token,platform,notifications_enabled)
                VALUES(1,' AA ','ios',TRUE),(2,'aa','ios',TRUE),
                      (3,'non-ios','android',TRUE),(1,'disabled','ios',FALSE)""")
    pushes = ModuleType("app.app_push")
    pushes.send_app_pushes = AsyncMock(return_value={"sent": 1, "failed": 0, "errors": []})
    monkeypatch.setitem(sys.modules, "app.app_push", pushes)
    draft = _draft(client, admin, title="Фільтр push")
    response = client.post(f"/community/admin/posts/{draft['id']}/publish", headers=bearer(admin),
                           json={"notify": True})
    assert response.status_code == 200
    args, _ = pushes.send_app_pushes.await_args
    assert [item["device_token"] for item in args[0]] == ["aa"]


def test_sender_failure_does_not_unpublish_or_repeat_push(community_api, monkeypatch):
    client, db = community_api
    admin, customer = _tokens(db)
    db.query("""CREATE TABLE app_push_devices (
        id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        device_token TEXT NOT NULL UNIQUE, platform TEXT NOT NULL DEFAULT 'ios',
        environment TEXT NOT NULL DEFAULT 'production', notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE
    )""")
    db.query("INSERT INTO app_push_devices(user_id,device_token) VALUES(2,'device')")
    pushes = ModuleType("app.app_push")
    pushes.send_app_pushes = AsyncMock(side_effect=RuntimeError("transport unavailable"))
    monkeypatch.setitem(sys.modules, "app.app_push", pushes)
    draft = _draft(client, admin, title="Падіння APNs")
    first = client.post(f"/community/admin/posts/{draft['id']}/publish", headers=bearer(admin),
                        json={"notify": True})
    repeat = client.post(f"/community/admin/posts/{draft['id']}/publish", headers=bearer(admin),
                         json={"notify": True})
    assert first.status_code == repeat.status_code == 200
    assert first.json()["is_published"] is True and first.json()["notification_attempted"] is True
    assert repeat.json()["is_published"] is True
    pushes.send_app_pushes.assert_awaited_once()
    assert client.get(f"/me/community/posts/{draft['id']}", headers=bearer(customer)).status_code == 200


def test_reaction_is_idempotent_and_vote_is_immutable(community_api):
    client, db = community_api
    admin, customer = _tokens(db)
    news = _draft(client, admin)
    assert client.post(f"/community/admin/posts/{news['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    first = client.put(f"/me/community/posts/{news['id']}/reaction", headers=bearer(customer),
                       json={"interested": True})
    second = client.put(f"/me/community/posts/{news['id']}/reaction", headers=bearer(customer),
                        json={"interested": True})
    assert first.status_code == second.status_code == 200
    assert second.json()["reaction_count"] == 1 and second.json()["interested"] is True

    poll = _draft(client, admin, type="poll", title="Опитування", text="",
                  question="Що обираєте?", options=["Еспресо", "Фільтр"])
    assert client.post(f"/community/admin/posts/{poll['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    vote = client.post(f"/me/community/posts/{poll['id']}/vote", headers=bearer(customer), json={"option_id": 1})
    repeat = client.post(f"/me/community/posts/{poll['id']}/vote", headers=bearer(customer), json={"option_id": 2})
    assert vote.status_code == 200 and vote.json()["vote_option_id"] == 1
    assert repeat.status_code == 409 and repeat.json()["detail"]["code"] == "POLL_ALREADY_VOTED"


def test_reaction_count_is_per_user_and_remove_does_not_leave_duplicate(community_api):
    client, db = community_api
    admin, customer = _tokens(db)
    other = _third_token(db)
    post = _draft(client, admin)
    assert client.post(f"/community/admin/posts/{post['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    path = f"/me/community/posts/{post['id']}/reaction"
    assert client.put(path, headers=bearer(customer), json={"interested": True}).json()["reaction_count"] == 1
    assert client.put(path, headers=bearer(customer), json={"interested": True}).json()["reaction_count"] == 1
    assert client.put(path, headers=bearer(other), json={"interested": True}).json()["reaction_count"] == 2
    removed = client.put(path, headers=bearer(customer), json={"interested": False})
    assert removed.status_code == 200 and removed.json()["interested"] is False
    assert removed.json()["reaction_count"] == 1
    assert client.get(f"/me/community/posts/{post['id']}", headers=bearer(other)).json()["interested"] is True


def test_consumer_mutations_reject_forged_user_and_invalid_poll_option(community_api):
    client, db = community_api
    admin, customer = _tokens(db)
    post = _draft(client, admin, type="poll", title="Питання", text="",
                  question="Q", options=["A", "B"])
    assert client.post(f"/community/admin/posts/{post['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    vote_path = f"/me/community/posts/{post['id']}/vote"
    reaction_path = f"/me/community/posts/{post['id']}/reaction"
    assert client.post(vote_path, headers=bearer(customer), json={"option_id": 3}).status_code == 422
    assert client.post(vote_path, headers=bearer(customer),
                       json={"option_id": 1, "user_id": 1}).status_code == 422
    assert client.put(reaction_path, headers=bearer(customer),
                      json={"interested": True, "user_id": 1}).status_code == 422


def test_public_feed_paginates_newest_first(community_api):
    client, db = community_api
    admin, customer = _tokens(db)
    posts = [_draft(client, admin, title=f"Новина {index}") for index in range(3)]
    for post in posts:
        assert client.post(f"/community/admin/posts/{post['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    first = client.get("/me/community/posts?limit=2&offset=0", headers=bearer(customer)).json()
    second = client.get("/me/community/posts?limit=2&offset=2", headers=bearer(customer)).json()
    assert first["total"] == 3 and first["limit"] == 2 and first["offset"] == 0
    assert [item["id"] for item in first["items"]] == [posts[2]["id"], posts[1]["id"]]
    assert [item["id"] for item in second["items"]] == [posts[0]["id"]]


def test_series_detail_returns_published_siblings_only(community_api):
    client, db = community_api
    admin, customer = _tokens(db)
    first = _draft(client, admin, type="series", title="Частина 1", text="A",
                   series_title="Історія", series_part=1, series_total=2)
    second = _draft(client, admin, type="series", title="Частина 2", text="B",
                    series_title="Історія", series_part=2, series_total=2)
    assert client.post(f"/community/admin/posts/{first['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    detail = client.get(f"/me/community/posts/{first['id']}", headers=bearer(customer))
    assert detail.status_code == 200
    assert [item["id"] for item in detail.json()["series_posts"]] == [first["id"]]
    assert client.post(f"/community/admin/posts/{second['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    detail = client.get(f"/me/community/posts/{first['id']}", headers=bearer(customer)).json()
    assert [item["id"] for item in detail["series_posts"]] == [first["id"], second["id"]]
    duplicate = _draft(client, admin, type="series", title="Дубль", text="C",
                       series_title="ІСТОРІЯ", series_part=3, series_total=3)
    assert duplicate["id"]
    # The unique expression is case-insensitive; the duplicate should be rejected on update/publish path.
    assert client.put(f"/community/admin/posts/{duplicate['id']}", headers=bearer(admin), json={
        "type": "series", "title": "Дубль", "text": "C",
        "series_title": "Історія", "series_part": 1, "series_total": 2,
    }).status_code == 409


@pytest.mark.parametrize("published", [False, True])
def test_active_series_part_is_unique_for_drafts_and_published_posts(community_api, published):
    client, db = community_api
    admin, _ = _tokens(db)
    post = _draft(client, admin, type="series", series_title="Espresso", series_part=1, series_total=3)
    if published:
        assert client.post(f"/community/admin/posts/{post['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    duplicate = client.post("/community/admin/posts", headers=bearer(admin), json={
        "type": "series", "title": "Дублікат", "text": "Текст", "series_title": "  ESPRESSO  ",
        "series_part": 1, "series_total": 3,
    })
    assert duplicate.status_code == 409
    assert duplicate.json()["detail"]["code"] == "SERIES_PART_ALREADY_EXISTS"
    assert len(db.query("SELECT * FROM community_posts")) == 1


def test_deleted_published_series_part_can_be_recreated_without_losing_history(community_api):
    client, db = community_api
    admin, customer = _tokens(db)
    original = _draft(client, admin, type="series", title="Стара частина", text="Історичний текст",
                      series_title="Еспресо", series_part=1, series_total=3)
    assert client.post(f"/community/admin/posts/{original['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    assert client.delete(f"/community/admin/posts/{original['id']}", headers=bearer(admin)).status_code == 200
    historical = db.query("SELECT * FROM community_posts WHERE id=%s", (original["id"],))[0]
    replacement = _draft(client, admin, type="series", title="Нова частина", text="Актуальний текст",
                         series_title=" Еспресо ", series_part=1, series_total=3)
    assert replacement["id"] != original["id"]
    assert replacement["is_published"] is False
    assert client.get("/me/community/posts", headers=bearer(customer)).json()["total"] == 0
    assert client.post(f"/community/admin/posts/{replacement['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    feed = client.get("/me/community/posts", headers=bearer(customer)).json()
    assert [item["id"] for item in feed["items"]] == [replacement["id"]]
    assert client.get(f"/me/community/posts/{original['id']}", headers=bearer(customer)).status_code == 404
    detail = client.get(f"/me/community/posts/{replacement['id']}", headers=bearer(customer))
    assert detail.status_code == 200
    assert [item["id"] for item in detail.json()["series_posts"]] == [replacement["id"]]
    assert db.query("SELECT * FROM community_posts WHERE id=%s", (original["id"],))[0] == historical
    assert historical["deleted_at"] is not None


def test_series_draft_update_conflicts_only_with_active_parts(community_api):
    client, db = community_api
    admin, _ = _tokens(db)
    occupied = _draft(client, admin, type="series", series_title="Espresso", series_part=1, series_total=3)
    draft = _draft(client, admin, type="series", series_title="Espresso", series_part=2, series_total=3)
    body = {"type": "series", "title": "Оновлено", "text": "Текст", "series_title": " ESPRESSO ",
            "series_part": 1, "series_total": 3}
    conflict = client.put(f"/community/admin/posts/{draft['id']}", headers=bearer(admin), json=body)
    assert conflict.status_code == 409 and conflict.json()["detail"]["code"] == "SERIES_PART_ALREADY_EXISTS"
    assert db.query("SELECT series_part FROM community_posts WHERE id=%s", (draft["id"],)) == [{"series_part": 2}]
    assert client.delete(f"/community/admin/posts/{occupied['id']}", headers=bearer(admin)).status_code == 200
    updated = client.put(f"/community/admin/posts/{draft['id']}", headers=bearer(admin), json=body)
    assert updated.status_code == 200 and updated.json()["series_part"] == 1


def test_active_series_migration_preserves_history_and_survives_full_replay(database):
    database.query(MIGRATION.read_text(encoding="utf-8"))
    database.query(SOFT_DELETE_MIGRATION.read_text(encoding="utf-8"))
    old = database.query("""INSERT INTO community_posts
        (type,title,text,series_title,series_part,series_total,published_at,deleted_at)
        VALUES('series','Стара частина','Історія',' Espresso ',1,3,NOW(),NOW()) RETURNING *""")[0]
    active = database.query("""INSERT INTO community_posts(type,title,text,series_title,series_part,series_total)
        VALUES('series','Чинна частина','Текст','Espresso',2,3) RETURNING *""")[0]
    insert = """INSERT INTO community_posts(type,title,text,series_title,series_part,series_total)
        VALUES('series','Заміна','Текст','ESPRESSO',1,3) RETURNING *"""
    # This is the legacy bug: the deleted row still reserves its normalized slot.
    with pytest.raises(UniqueViolation):
        database.query(insert)
    for _ in range(2):
        database.query(ACTIVE_SERIES_MIGRATION.read_text(encoding="utf-8"))
    replacement = database.query(insert)[0]
    before = database.query("SELECT * FROM community_posts ORDER BY id")
    for _ in range(2):
        for migration in (MIGRATION, SOFT_DELETE_MIGRATION, ACTIVE_SERIES_MIGRATION):
            database.query(migration.read_text(encoding="utf-8"))
    assert database.query("SELECT * FROM community_posts ORDER BY id") == before
    assert next(row for row in before if row["id"] == old["id"]) == old
    assert next(row for row in before if row["id"] == active["id"]) == active
    assert replacement["deleted_at"] is None
    with pytest.raises(UniqueViolation):
        database.query(insert)
    predicate = database.query("""SELECT pg_get_expr(indpred,indrelid) AS predicate FROM pg_index
        WHERE indexrelid='uq_community_posts_series_part'::regclass""")[0]["predicate"]
    assert "deleted_at IS NULL" in predicate


def test_photo_is_validated_randomized_and_published_image_is_immutable(community_api):
    client, db = community_api
    admin, _ = _tokens(db)
    draft = _draft(client, admin)
    response = client.post(f"/community/admin/posts/{draft['id']}/image", headers=bearer(admin),
                            files={"photo": ("../../escape.jpg", _photo_bytes("JPEG"), "image/jpeg")})
    assert response.status_code == 200, response.text
    image_url = response.json()["image_url"]
    assert image_url.startswith("/uploads/community/") and ".." not in image_url
    assert client.post(f"/community/admin/posts/{draft['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    assert client.post(f"/community/admin/posts/{draft['id']}/image", headers=bearer(admin),
                       files={"photo": ("x.png", _photo_bytes(), "image/png")}).status_code == 409
    bad = _draft(client, admin)
    assert client.post(f"/community/admin/posts/{bad['id']}/image", headers=bearer(admin),
                       files={"photo": ("x.jpg", b"not-an-image", "image/jpeg")}).status_code in {415, 422}


def test_community_media_delete_rejects_traversal_before_filesystem_access(tmp_path):
    from app.community_media import CommunityMediaStorage

    root = tmp_path / "community"
    storage = CommunityMediaStorage(root)
    outside = tmp_path / ("x" * 29 + ".jpg")
    outside.write_bytes(b"must remain outside the community directory")
    traversal = "../" + outside.name
    assert len(traversal) == 36  # Previously passed a length-only check.
    for key in [traversal, "%2e%2e/" + outside.name, "/" + outside.name,
                "a" * 32 + ".jpg.tmp", "b" * 32 + ".mp4", "A" * 32 + ".png"]:
        assert storage.delete("/uploads/community/" + key) is False
    assert outside.read_bytes() == b"must remain outside the community directory"

    for kind in ["PNG", "JPEG"]:
        url = storage.save({"bytes": _photo_bytes(kind), "filename": "../photo"})
        file = root / url.rsplit("/", 1)[1]
        assert file.is_file()
        assert storage.delete(url) is True
        assert not file.exists()
        assert storage.delete(url) is False


def test_user_foreign_keys_cascade_reactions_but_preserve_post_history(community_api):
    client, db = community_api
    admin, customer = _tokens(db)
    post = _draft(client, admin)
    poll = _draft(client, admin, type="poll", title="Голос", text="",
                  question="Q", options=["A", "B"])
    for item in (post, poll):
        assert client.post(f"/community/admin/posts/{item['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    assert client.put(f"/me/community/posts/{post['id']}/reaction", headers=bearer(customer),
                      json={"interested": True}).status_code == 200
    assert client.post(f"/me/community/posts/{poll['id']}/vote", headers=bearer(customer),
                       json={"option_id": 1}).status_code == 200
    db.query("DELETE FROM users WHERE id=2")
    assert db.query("SELECT COUNT(*) AS n FROM community_posts")[0]["n"] == 2
    assert db.query("SELECT COUNT(*) AS n FROM community_reactions")[0]["n"] == 0
    assert db.query("SELECT COUNT(*) AS n FROM community_poll_votes")[0]["n"] == 0
    db.query("DELETE FROM users WHERE id=1")
    assert db.query("SELECT COUNT(*) AS n FROM community_posts")[0]["n"] == 2
    assert all(row["created_by_user_id"] is None
               for row in db.query("SELECT created_by_user_id FROM community_posts"))


def test_soft_delete_migration_preserves_existing_community_rows_and_is_idempotent(database):
    database.query(MIGRATION.read_text(encoding="utf-8"))
    post = database.query("""INSERT INTO community_posts(type,title,text,published_at,image_url)
        VALUES('news','До міграції','Існуючий текст',NOW(),'/uploads/community/kept.jpg')
        RETURNING *""")[0]
    for _ in range(2):
        database.query(SOFT_DELETE_MIGRATION.read_text(encoding="utf-8"))
    after = database.query("SELECT * FROM community_posts WHERE id=%s", (post["id"],))[0]
    assert {key: after[key] for key in post} == post
    assert after["deleted_at"] is None


@pytest.mark.parametrize("published", [False, True])
def test_superadmin_soft_delete_hides_draft_and_published_and_preserves_media(community_api, tmp_path, published):
    client, db = community_api
    admin, customer = _tokens(db)
    post = _draft(client, admin, type="poll", title="Видалити", text="Зберегти текст",
                  question="Q", options=["A", "B"])
    image = client.post(f"/community/admin/posts/{post['id']}/image", headers=bearer(admin),
                        files={"photo": ("photo.jpg", _photo_bytes("JPEG"), "image/jpeg")})
    assert image.status_code == 200
    media_file = tmp_path / "community-upload" / image.json()["image_url"].rsplit("/", 1)[1]
    media_before = media_file.read_bytes()
    if published:
        assert client.post(f"/community/admin/posts/{post['id']}/publish", headers=bearer(admin), json={}).status_code == 200
        assert client.put(f"/me/community/posts/{post['id']}/reaction", headers=bearer(customer),
                          json={"interested": True}).status_code == 200
        assert client.post(f"/me/community/posts/{post['id']}/vote", headers=bearer(customer),
                           json={"option_id": 1}).status_code == 200
    before = db.query("SELECT * FROM community_posts WHERE id=%s", (post["id"],))[0]
    path = f"/community/admin/posts/{post['id']}"
    deleted = client.delete(path, headers=bearer(admin))
    assert deleted.status_code == 200 and deleted.json() == {"ok": True}
    assert "no-store" in deleted.headers["cache-control"]
    after = db.query("SELECT * FROM community_posts WHERE id=%s", (post["id"],))[0]
    assert after["deleted_at"] is not None
    assert {key: value for key, value in after.items() if key != "deleted_at"} == {
        key: value for key, value in before.items() if key != "deleted_at"}
    assert media_file.read_bytes() == media_before
    assert db.query("SELECT COUNT(*) AS n FROM community_reactions WHERE post_id=%s", (post["id"],))[0]["n"] == int(published)
    assert db.query("SELECT COUNT(*) AS n FROM community_poll_votes WHERE post_id=%s", (post["id"],))[0]["n"] == int(published)
    for list_path, token in [("/me/community/posts", customer), ("/community/admin/posts", admin)]:
        page = client.get(list_path, headers=bearer(token)).json()
        assert page["items"] == [] and page["total"] == 0
    assert client.get(path, headers=bearer(admin)).status_code == 404
    assert client.get(f"/me/community/posts/{post['id']}", headers=bearer(customer)).status_code == 404
    assert client.put(f"/me/community/posts/{post['id']}/reaction", headers=bearer(customer),
                      json={"interested": False}).status_code == 404
    assert client.post(f"/me/community/posts/{post['id']}/vote", headers=bearer(customer),
                       json={"option_id": 2}).status_code == 404
    assert client.put(path, headers=bearer(admin), json={"type": "news", "title": "x", "text": "y"}).status_code == 404
    assert client.post(path + "/publish", headers=bearer(admin), json={"notify": True}).status_code == 404
    assert client.post(path + "/image", headers=bearer(admin),
                       files={"photo": ("x.png", _photo_bytes(), "image/png")}).status_code == 404
    assert len(list(media_file.parent.iterdir())) == 1
    assert client.delete(path, headers=bearer(admin)).json() == {"ok": True}
    assert db.query("SELECT deleted_at FROM community_posts WHERE id=%s", (post["id"],))[0]["deleted_at"] == after["deleted_at"]
    assert client.delete("/community/admin/posts/999999", headers=bearer(admin)).status_code == 404


@pytest.mark.parametrize("role", ["owner", "admin"])
def test_shop_owner_and_admin_cannot_delete_posts(community_api, role):
    client, db = community_api
    admin, _ = _tokens(db)
    db.query("UPDATE shop_admins SET role=%s WHERE user_id=3", (role,))
    other = _third_token(db)
    post = _draft(client, admin)
    path = f"/community/admin/posts/{post['id']}"
    assert client.delete(path, headers=bearer(other)).status_code == 403
    assert db.query("SELECT deleted_at FROM community_posts WHERE id=%s", (post["id"],))[0]["deleted_at"] is None


def test_deletion_keeps_other_posts_votes_reactions_and_pagination(community_api):
    client, db = community_api
    admin, customer = _tokens(db)
    posts = [_draft(client, admin, type="poll", title=f"Poll {i}", text="", question="Q", options=["A", "B"])
             for i in range(3)]
    for post in posts:
        path = f"/community/admin/posts/{post['id']}"
        assert client.post(path + "/publish", headers=bearer(admin), json={}).status_code == 200
    keep, remove, newest = posts
    assert client.put(f"/me/community/posts/{keep['id']}/reaction", headers=bearer(customer),
                      json={"interested": True}).status_code == 200
    assert client.post(f"/me/community/posts/{keep['id']}/vote", headers=bearer(customer),
                       json={"option_id": 1}).status_code == 200
    before = client.get(f"/me/community/posts/{keep['id']}", headers=bearer(customer)).json()
    assert client.delete(f"/community/admin/posts/{remove['id']}", headers=bearer(admin)).status_code == 200
    assert client.get(f"/me/community/posts/{keep['id']}", headers=bearer(customer)).json() == before
    for path, token in [("/me/community/posts", customer), ("/community/admin/posts", admin)]:
        first = client.get(path + "?limit=1&offset=0", headers=bearer(token)).json()
        second = client.get(path + "?limit=1&offset=1", headers=bearer(token)).json()
        assert first["total"] == second["total"] == 2
        assert [item["id"] for item in first["items"] + second["items"]] == [newest["id"], keep["id"]]
    assert client.put(f"/me/community/posts/{newest['id']}/reaction", headers=bearer(customer),
                      json={"interested": True}).status_code == 200
    assert client.post(f"/me/community/posts/{newest['id']}/vote", headers=bearer(customer),
                       json={"option_id": 2}).status_code == 200


def test_deleted_series_part_is_excluded_from_other_part_details(community_api):
    client, db = community_api
    admin, customer = _tokens(db)
    posts = [_draft(client, admin, type="series", title=f"Частина {i}", text="Кава",
                    series_title="Серія", series_part=i, series_total=2) for i in [1, 2]]
    for post in posts:
        assert client.post(f"/community/admin/posts/{post['id']}/publish", headers=bearer(admin), json={}).status_code == 200
    assert client.delete(f"/community/admin/posts/{posts[0]['id']}", headers=bearer(admin)).status_code == 200
    detail = client.get(f"/me/community/posts/{posts[1]['id']}", headers=bearer(customer)).json()
    assert [item["id"] for item in detail["series_posts"]] == [posts[1]["id"]]
