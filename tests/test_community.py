"""Community MVP API tests against the guarded disposable PostgreSQL fixture."""

from io import BytesIO
import importlib
import sys
from types import ModuleType
from unittest.mock import AsyncMock

from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient
from PIL import Image
import pytest

from conftest import PROJECT, bearer


MIGRATION = PROJECT / "migrations/20261015_community_posts.sql"


def _photo_bytes(kind="PNG"):
    output = BytesIO()
    Image.new("RGB", (16, 16), "#f4d58c").save(output, format=kind)
    return output.getvalue()


@pytest.fixture
def community_api(database, tmp_path):
    database.query(MIGRATION.read_text(encoding="utf-8"))
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
