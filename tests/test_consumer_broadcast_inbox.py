"""Consumer inbox against guarded local PostgreSQL and private temporary media.

Use real app-session authentication and recipient snapshots without importing
backend configuration/startup. No Telegram/APNs delivery or external requests.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import importlib
from io import BytesIO
import sys
import threading
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient
from PIL import Image
import pytest

from app.broadcast_publication import BroadcastPublication
from conftest import PROJECT, bearer, extract_definitions
from test_barista_operations import operations_database
from test_barista_owner_tools import owner_database


ITEM_FIELDS = {"broadcast_id", "shop_id", "shop_name", "text", "media_type", "created_at", "read_at"}
FIXED_CREATED = datetime(2026, 10, 10, 9, tzinfo=timezone.utc)
FIXED_READ = datetime(2026, 10, 11, 9, tzinfo=timezone.utc)


class _CaptureMediaChunks:
    def __init__(self, app):
        self.app = app
        self.chunks = []

    async def __call__(self, scope, receive, send):
        async def capture(message):
            if (scope["type"] == "http" and scope["path"].endswith("/media")
                    and message["type"] == "http.response.body" and message.get("body")):
                self.chunks.append(len(message["body"]))
            await send(message)

        await self.app(scope, receive, capture)


@pytest.fixture
def inbox_api(owner_database, monkeypatch):
    db = owner_database
    pushes = ModuleType("app.app_push")
    pushes.send_app_push = AsyncMock(side_effect=AssertionError("Inbox called APNs"))
    pushes.send_app_pushes = AsyncMock(side_effect=AssertionError("Inbox called APNs"))
    monkeypatch.setitem(sys.modules, "app.app_push", pushes)
    namespace = {"Header": Header, "HTTPException": HTTPException,
                 "get_app_session": db.db.get_app_session}
    extract_definitions(PROJECT / "app/api/main.py",
                        {"_get_bearer_token_or_401", "get_current_user"}, namespace)
    module = importlib.import_module("app.api.broadcasts")
    media_get = Mock(wraps=db.broadcast_storage.get)
    monkeypatch.setattr(db.broadcast_storage, "get", media_get)
    storage_factory = Mock(return_value=db.broadcast_storage)
    app = FastAPI()
    app.include_router(module.build_broadcast_inbox_router(
        authenticate=namespace["get_current_user"], connection_factory=db.db.get_connection,
        media_storage_factory=storage_factory,
    ))
    capture = _CaptureMediaChunks(app)
    with TestClient(capture) as client:
        yield SimpleNamespace(client=client, db=db, storage_factory=storage_factory,
                              media_get=media_get, pushes=pushes, chunks=capture.chunks)
    pushes.send_app_push.assert_not_called()
    pushes.send_app_pushes.assert_not_called()


def publish(db, *, shop_id=1, users=(4,), text="Повідомлення для клієнтів", media=None, created_at=None):
    sender = 1 if shop_id == 1 else 3
    with BroadcastPublication(db.broadcast_storage) as publication:
        with db.db.get_connection() as connection:
            with connection.transaction():
                row = publication.publish(connection, shop_id, sender, text,
                                          [{"user_id": user_id} for user_id in users], media)
                if created_at is not None:
                    connection.execute("UPDATE broadcasts SET created_at=%s WHERE id=%s", (created_at, row["id"]))
    return db.query("SELECT * FROM broadcasts WHERE id=%s", (row["id"],))[0]


def detail_path(row, shop_id=None):
    return f"/me/shops/{row['shop_id'] if shop_id is None else shop_id}/broadcasts/{row['id']}"


def parsed_timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def photo_bytes(image_format="JPEG"):
    output = BytesIO()
    Image.new("RGB", (32, 32), "brown").save(output, format=image_format)
    return output.getvalue()


def video_bytes(brand=b"isom"):
    def box(kind, data):
        return (8 + len(data)).to_bytes(4, "big") + kind + data

    return box(b"ftyp", brand + b"\0" * 4 + brand) + box(b"moov", b"") + box(b"mdat", b"video")


def media_data(extension):
    if extension == "jpg":
        return "photo", photo_bytes(), "image/jpeg"
    if extension == "png":
        return "photo", photo_bytes("PNG"), "image/png"
    return "video", video_bytes(b"qt  " if extension == "mov" else b"isom"), (
        "video/quicktime" if extension == "mov" else "video/mp4")


def with_media(db, extension="jpg", *, users=(4,), text="Оригінальний підпис"):
    kind, data, mime = media_data(extension)
    row = publish(db, users=users, text=text,
                  media={"kind": kind, "filename": "attachment", "bytes": data})
    return row, data, mime


def assert_private_headers(response):
    cache = response.headers.get("cache-control", "").lower()
    assert "private" in cache and "no-store" in cache
    assert "authorization" in {value.strip().lower() for value in response.headers.get("vary", "").split(",")}
    assert response.headers.get("x-content-type-options") == "nosniff"


def test_global_inbox_is_recipient_only_newest_first_and_unread_count_is_independent_of_page(inbox_api):
    api, db = inbox_api, inbox_api.db
    token = db.db.create_app_session(4)
    rows = []
    for index in range(25):
        row = publish(db, text=f"Повідомлення {index}", created_at=FIXED_CREATED + timedelta(seconds=index // 2))
        rows.append(row)
        if index % 3 == 0:
            db.query("UPDATE broadcast_recipients SET read_at=%s WHERE broadcast_id=%s AND user_id=4",
                     (FIXED_READ, row["id"]))
    # A newer foreign snapshot and a legacy broadcast without recipients are hidden.
    publish(db, users=(5,), text="Чуже", created_at=FIXED_CREATED + timedelta(days=1))
    db.query("INSERT INTO broadcasts(shop_id,sender_user_id,text,created_at) VALUES(1,1,'Legacy',%s)",
             (FIXED_CREATED + timedelta(days=2),))
    expected = sorted(rows, key=lambda row: (row["created_at"], row["id"]), reverse=True)
    unread = sum(index % 3 != 0 for index in range(25))
    first = api.client.get("/me/broadcasts", headers=bearer(token))
    assert first.status_code == 200
    body = first.json()
    assert set(body) == {"items", "total", "limit", "offset", "unread_count"}
    assert body["total"] == 25 and body["limit"] == 20 and body["offset"] == 0
    assert body["unread_count"] == unread
    assert [item["broadcast_id"] for item in body["items"]] == [row["id"] for row in expected[:20]]
    assert all(set(item) == ITEM_FIELDS for item in body["items"])
    assert_private_headers(first)
    page = api.client.get("/me/broadcasts?limit=3&offset=20", headers=bearer(token)).json()
    assert [item["broadcast_id"] for item in page["items"]] == [row["id"] for row in expected[20:23]]
    assert page["total"] == 25 and page["unread_count"] == unread
    assert page["limit"] == 3 and page["offset"] == 20
    beyond = api.client.get("/me/broadcasts?limit=100&offset=1000000", headers=bearer(token)).json()
    assert beyond == {"items": [], "total": 25, "unread_count": unread, "limit": 100, "offset": 1000000}
    api.storage_factory.assert_not_called()


def test_shop_filter_has_own_counts_and_unknown_or_foreign_shops_return_same_empty_list(inbox_api):
    api, db = inbox_api, inbox_api.db
    token = db.db.create_app_session(4)
    one = publish(db, shop_id=1)
    two = publish(db, shop_id=2)
    db.query("UPDATE broadcast_recipients SET read_at=%s WHERE broadcast_id=%s AND user_id=4", (FIXED_READ, two["id"]))
    first = api.client.get("/me/shops/1/broadcasts", headers=bearer(token)).json()
    second = api.client.get("/me/shops/2/broadcasts", headers=bearer(token)).json()
    assert [item["broadcast_id"] for item in first["items"]] == [one["id"]]
    assert first["total"] == 1 and first["unread_count"] == 1
    assert [item["broadcast_id"] for item in second["items"]] == [two["id"]]
    assert second["total"] == 1 and second["unread_count"] == 0
    other = db.db.create_app_session(5)
    empty = {"items": [], "total": 0, "unread_count": 0, "limit": 20, "offset": 0}
    for shop_id in (1, 2, 999):
        response = api.client.get(f"/me/shops/{shop_id}/broadcasts", headers=bearer(other))
        assert response.status_code == 200 and response.json() == empty


@pytest.mark.parametrize("query", ["limit=0", "limit=-1", "limit=101", "limit=no",
                                  "offset=-1", "offset=1000001", "offset=no"])
def test_pagination_rejects_invalid_or_unbounded_parameters(inbox_api, query):
    token = inbox_api.db.db.create_app_session(4)
    for path in ("/me/broadcasts", "/me/shops/1/broadcasts"):
        assert inbox_api.client.get(path + "?" + query, headers=bearer(token)).status_code == 422


def test_detail_preserves_full_text_and_exposes_only_relative_protected_media_endpoint(inbox_api):
    api, db = inbox_api, inbox_api.db
    text = "Оригінальний текст ☕\n" + "Усі подробиці повідомлення. " * 100
    row, _, _ = with_media(db, text=text)
    token = db.db.create_app_session(4)
    response = api.client.get(detail_path(row), headers=bearer(token))
    assert response.status_code == 200
    body = response.json()
    assert set(body) == ITEM_FIELDS | {"media_endpoint"}
    assert body["broadcast_id"] == row["id"] and body["shop_id"] == 1
    assert body["shop_name"] == "Наші" and body["text"] == text
    assert body["media_type"] == "photo" and body["read_at"] is None
    assert parsed_timestamp(body["created_at"]) == row["created_at"]
    assert body["media_endpoint"] == detail_path(row) + "/media"
    assert row["media_key"] not in response.text and str(db.broadcast_storage.root) not in response.text
    assert_private_headers(response)
    api.storage_factory.assert_not_called()
    text_only = publish(db, text="Без вкладення")
    assert api.client.get(detail_path(text_only), headers=bearer(token)).json()["media_endpoint"] is None


def test_get_requests_do_not_mark_read_and_post_preserves_first_timestamp_without_touching_other_users(inbox_api):
    api, db = inbox_api, inbox_api.db
    row, _, _ = with_media(db, users=(4, 5))
    token, other = db.db.create_app_session(4), db.db.create_app_session(5)
    db.query("UPDATE broadcast_recipients SET read_at=%s WHERE broadcast_id=%s AND user_id=5", (FIXED_READ, row["id"]))
    other_before = db.query("SELECT * FROM broadcast_recipients WHERE broadcast_id=%s AND user_id=5", (row["id"],))
    for path in ("/me/broadcasts", detail_path(row), detail_path(row) + "/media"):
        assert api.client.get(path, headers=bearer(token)).status_code == 200
    assert db.query("SELECT read_at FROM broadcast_recipients WHERE broadcast_id=%s AND user_id=4", (row["id"],)) == [{"read_at": None}]
    first = api.client.post(detail_path(row) + "/read", headers=bearer(token),
                            json={"user_id": 5, "read_at": "2000-01-01T00:00:00Z"})
    assert first.status_code == 200
    body = first.json()
    assert set(body) == {"ok", "broadcast_id", "shop_id", "read_at"}
    assert body["ok"] is True and body["broadcast_id"] == row["id"] and body["shop_id"] == 1
    timestamp = parsed_timestamp(body["read_at"])
    assert timestamp.year != 2000
    repeat = api.client.post(detail_path(row) + "/read", headers=bearer(token))
    assert repeat.status_code == 200 and repeat.json() == body
    assert db.query("SELECT read_at FROM broadcast_recipients WHERE broadcast_id=%s AND user_id=4", (row["id"],)) == [{"read_at": timestamp}]
    assert db.query("SELECT * FROM broadcast_recipients WHERE broadcast_id=%s AND user_id=5", (row["id"],)) == other_before
    assert api.client.get("/me/broadcasts", headers=bearer(token)).json()["unread_count"] == 0
    assert parsed_timestamp(api.client.get(detail_path(row), headers=bearer(other)).json()["read_at"]) == FIXED_READ
    assert_private_headers(first)


def test_concurrent_read_requests_return_one_stable_timestamp(inbox_api):
    api, db = inbox_api, inbox_api.db
    row = publish(db, users=(4, 5))
    token = db.db.create_app_session(4)
    ready = threading.Barrier(4)

    def read(_):
        ready.wait(timeout=5)
        return api.client.post(detail_path(row) + "/read", headers=bearer(token))

    with ThreadPoolExecutor(max_workers=4) as workers:
        responses = list(workers.map(read, range(4)))
    assert all(response.status_code == 200 for response in responses)
    timestamps = {response.json()["read_at"] for response in responses}
    assert len(timestamps) == 1
    stored = db.query("SELECT user_id,read_at FROM broadcast_recipients WHERE broadcast_id=%s ORDER BY user_id", (row["id"],))
    assert stored == [{"user_id": 4, "read_at": parsed_timestamp(timestamps.pop())},
                      {"user_id": 5, "read_at": None}]


@pytest.mark.parametrize("rejection", ["unknown", "foreign-user", "wrong-shop", "owner-without-recipient"])
def test_detail_read_and_media_use_identical_not_found_for_nonrecipients_before_storage_or_range(inbox_api, rejection):
    api, db = inbox_api, inbox_api.db
    row, _, _ = with_media(db)
    user_id = 5 if rejection == "foreign-user" else 1 if rejection == "owner-without-recipient" else 4
    token = db.db.create_app_session(user_id)
    path = detail_path(row, 2 if rejection == "wrong-shop" else None)
    if rejection == "unknown":
        path = f"/me/shops/1/broadcasts/{row['id'] + 100000}"
    recipients_before = db.query("SELECT * FROM broadcast_recipients ORDER BY id")
    for method, endpoint in (("GET", path), ("POST", path + "/read"),
                             ("DELETE", path), ("GET", path + "/media")):
        response = api.client.request(method, endpoint, headers={**bearer(token), "Range": "bytes=invalid"})
        assert response.status_code == 404
        assert response.json() == {"detail": {"code": "BROADCAST_NOT_FOUND"}}
        assert_private_headers(response)
    assert db.query("SELECT * FROM broadcast_recipients ORDER BY id") == recipients_before
    api.storage_factory.assert_not_called()
    api.media_get.assert_not_called()


def test_recipient_can_soft_delete_only_own_copy_idempotently_and_keep_audit_and_media(inbox_api):
    api, db = inbox_api, inbox_api.db
    row, data, _ = with_media(db, users=(4, 5))
    token = db.db.create_app_session(4)
    other_token = db.db.create_app_session(5)
    audit_before = db.query("SELECT id,shop_id,sender_user_id,text,media_type,media_key FROM broadcasts")

    first = api.client.delete(detail_path(row), headers=bearer(token))
    assert first.status_code == 200
    first_body = first.json()
    assert set(first_body) == {"ok", "broadcast_id", "shop_id", "deleted_at"}
    assert first_body["ok"] is True
    assert first_body["broadcast_id"] == row["id"] and first_body["shop_id"] == row["shop_id"]
    assert_private_headers(first)
    deleted_at = first_body["deleted_at"]
    assert db.query("SELECT deleted_at FROM broadcast_recipients WHERE broadcast_id=%s AND user_id=4",
                    (row["id"],)) == [{"deleted_at": parsed_timestamp(deleted_at)}]

    second = api.client.delete(detail_path(row), headers=bearer(token))
    assert second.status_code == 200 and second.json() == first_body

    for path in ("/me/broadcasts", "/me/shops/1/broadcasts"):
        response = api.client.get(path, headers=bearer(token))
        assert response.status_code == 200
        assert response.json() == {"items": [], "total": 0, "unread_count": 0, "limit": 20, "offset": 0}
        assert_private_headers(response)

    for method, path in (
        ("GET", detail_path(row)),
        ("POST", detail_path(row) + "/read"),
        ("GET", detail_path(row) + "/media"),
    ):
        response = api.client.request(method, path, headers=bearer(token))
        assert response.status_code == 404
        assert response.json() == {"detail": {"code": "BROADCAST_NOT_FOUND"}}
        assert_private_headers(response)
    api.storage_factory.assert_not_called()
    api.media_get.assert_not_called()

    # The publication and media remain audit data, and another recipient still
    # owns an independent visible copy.
    assert db.query("SELECT id,shop_id,sender_user_id,text,media_type,media_key FROM broadcasts") == audit_before
    other_detail = api.client.get(detail_path(row), headers=bearer(other_token))
    assert other_detail.status_code == 200
    assert api.client.get(detail_path(row) + "/media", headers=bearer(other_token)).content == data
    assert db.query("SELECT deleted_at FROM broadcast_recipients WHERE broadcast_id=%s AND user_id=5",
                    (row["id"],)) == [{"deleted_at": None}]


def test_delete_removes_message_from_totals_unread_and_pages(inbox_api):
    api, db = inbox_api, inbox_api.db
    rows = [publish(db, users=(4,), text=f"Повідомлення {index}") for index in range(3)]
    token = db.db.create_app_session(4)
    before = api.client.get("/me/broadcasts?limit=1&offset=0", headers=bearer(token)).json()
    assert before["total"] == 3 and before["unread_count"] == 3

    deleted = api.client.delete(detail_path(rows[1]), headers=bearer(token))
    assert deleted.status_code == 200

    after = api.client.get("/me/broadcasts?limit=1&offset=0", headers=bearer(token)).json()
    assert after["total"] == 2 and after["unread_count"] == 2
    assert rows[1]["id"] not in [item["broadcast_id"] for item in after["items"]]
    page = api.client.get("/me/broadcasts?limit=2&offset=1", headers=bearer(token)).json()
    assert page["total"] == 2 and page["unread_count"] == 2
    assert rows[1]["id"] not in [item["broadcast_id"] for item in page["items"]]


def test_legacy_broadcast_without_snapshot_is_hidden_despite_current_shop_membership(inbox_api):
    api, db = inbox_api, inbox_api.db
    row = db.query("INSERT INTO broadcasts(shop_id,sender_user_id,text) VALUES(1,1,'Legacy audience unknown') RETURNING *")[0]
    token = db.db.create_app_session(4)
    assert db.query("SELECT * FROM shop_clients WHERE user_id=4 AND shop_id=1")
    for path in ("/me/broadcasts", "/me/shops/1/broadcasts"):
        body = api.client.get(path, headers=bearer(token)).json()
        assert body["items"] == [] and body["total"] == 0 and body["unread_count"] == 0
    for method, suffix in (("GET", ""), ("POST", "/read"), ("GET", "/media")):
        response = api.client.request(method, detail_path(row) + suffix, headers=bearer(token))
        assert response.status_code == 404 and response.json()["detail"]["code"] == "BROADCAST_NOT_FOUND"
    api.storage_factory.assert_not_called()


@pytest.mark.parametrize("authorization", ["missing", "invalid", "expired", "revoked", "barista", "malformed"])
def test_all_inbox_endpoints_require_active_consumer_sessions_before_storage(inbox_api, authorization):
    api, db = inbox_api, inbox_api.db
    row, _, _ = with_media(db)
    if authorization in {"expired", "revoked"}:
        token = db.db.create_app_session(4)
        session_id = db.session(token)["id"]
        column = "expires_at" if authorization == "expired" else "revoked_at"
        db.query(f"UPDATE app_sessions SET {column}=NOW()-INTERVAL '1 day' WHERE id=%s", (session_id,))
        headers = bearer(token)
    elif authorization == "barista":
        headers = bearer(db.staff_token())
    elif authorization == "malformed":
        headers = {"Authorization": "Basic offline"}
    else:
        headers = {} if authorization == "missing" else bearer("invalid-offline-session")
    endpoints = [("GET", "/me/broadcasts"), ("GET", "/me/shops/1/broadcasts"),
                 ("GET", detail_path(row)), ("POST", detail_path(row) + "/read"),
                 ("DELETE", detail_path(row)), ("GET", detail_path(row) + "/media")]
    for method, path in endpoints:
        response = api.client.request(method, path, headers={**headers, "Range": "bytes=invalid"})
        assert response.status_code == 401 and response.headers.get("www-authenticate") == "Bearer"
    api.storage_factory.assert_not_called()
    api.media_get.assert_not_called()
    assert db.query("SELECT read_at FROM broadcast_recipients") == [{"read_at": None}]


def test_old_snapshot_survives_closed_shop_and_removed_or_inactive_loyalty_membership(inbox_api):
    api, db = inbox_api, inbox_api.db
    row, data, _ = with_media(db)
    db.query("UPDATE broadcasts SET created_at=NOW()-INTERVAL '180 days' WHERE id=%s", (row["id"],))
    db.query("UPDATE coffee_shops SET is_active=FALSE WHERE id=1")
    db.query("UPDATE shop_clients SET last_activity_at=NOW()-INTERVAL '180 days' WHERE user_id=4")
    db.query("DELETE FROM shop_clients WHERE shop_id=1 AND user_id=4")
    token = db.db.create_app_session(4)
    for path in ("/me/broadcasts", "/me/shops/1/broadcasts"):
        response = api.client.get(path, headers=bearer(token))
        assert response.status_code == 200 and response.json()["total"] == 1
        assert response.json()["items"][0]["broadcast_id"] == row["id"]
    assert api.client.get(detail_path(row), headers=bearer(token)).status_code == 200
    assert api.client.get(detail_path(row) + "/media", headers=bearer(token)).content == data
    assert api.client.post(detail_path(row) + "/read", headers=bearer(token)).status_code == 200


@pytest.mark.parametrize("extension", ["jpg", "png", "mp4", "mov"])
def test_private_media_streams_original_bytes_with_correct_mime_length_and_headers(inbox_api, extension):
    api, db = inbox_api, inbox_api.db
    row, data, mime = with_media(db, extension)
    token = db.db.create_app_session(4)
    response = api.client.get(detail_path(row) + "/media", headers=bearer(token))
    assert response.status_code == 200 and response.content == data
    assert response.headers["content-type"] == mime
    assert response.headers["content-length"] == str(len(data))
    assert response.headers["accept-ranges"] == "bytes" and "content-range" not in response.headers
    assert_private_headers(response)
    api.media_get.assert_called_once_with(row["media_key"])
    assert db.query("SELECT read_at FROM broadcast_recipients") == [{"read_at": None}]


@pytest.mark.parametrize("extension", ["jpg", "mp4"])
@pytest.mark.parametrize("range_value,range_kind", [
    ("bytes=0-3", "first"), ("bytes=5-", "open"), ("bytes=-5", "suffix"),
    ("bytes=0-999999", "whole"), ("bytes=-999999", "whole"), ("bytes=0-0", "single"),
])
def test_authorized_single_ranges_return_exact_bytes_and_content_range(inbox_api, extension, range_value, range_kind):
    api, db = inbox_api, inbox_api.db
    row, data, mime = with_media(db, extension)
    token = db.db.create_app_session(4)
    start, end = {"first": (0, 3), "open": (5, len(data) - 1),
                  "suffix": (len(data) - 5, len(data) - 1), "whole": (0, len(data) - 1),
                  "single": (0, 0)}[range_kind]
    response = api.client.get(detail_path(row) + "/media", headers={**bearer(token), "Range": range_value})
    assert response.status_code == 206 and response.content == data[start:end + 1]
    assert response.headers["content-range"] == f"bytes {start}-{end}/{len(data)}"
    assert response.headers["content-length"] == str(end - start + 1)
    assert response.headers["content-type"] == mime
    assert_private_headers(response)


@pytest.mark.parametrize("range_value", ["bytes=999999-", "bytes=9-3", "bytes=-0", "bytes=", "bytes=x-y",
                                        "bytes=0-3,5-8", "items=0-3", "bytes=-", "bytes=" + "9" * 129 + "-"])
def test_invalid_or_unsatisfiable_ranges_fail_with_416_only_after_recipient_authorization(inbox_api, range_value):
    api, db = inbox_api, inbox_api.db
    row, data, _ = with_media(db, "mp4")
    token = db.db.create_app_session(4)
    response = api.client.get(detail_path(row) + "/media", headers={**bearer(token), "Range": range_value})
    assert response.status_code == 416 and response.json()["detail"]["code"] == "INVALID_MEDIA_RANGE"
    assert response.headers["content-range"] == f"bytes */{len(data)}"
    assert_private_headers(response)


def test_media_response_uses_chunks_no_larger_than_64_kib(inbox_api):
    api, db = inbox_api, inbox_api.db
    jpeg = photo_bytes()
    data = jpeg + b"\0" * (200000 - len(jpeg))
    row = publish(db, media={"kind": "photo", "filename": "large.jpg", "bytes": data})
    token = db.db.create_app_session(4)
    for range_header, expected in (({}, data), ({"Range": "bytes=1-140000"}, data[1:140001])):
        api.chunks.clear()
        response = api.client.get(detail_path(row) + "/media", headers={**bearer(token), **range_header})
        assert response.status_code in {200, 206} and response.content == expected
        assert len(api.chunks) > 1 and max(api.chunks) <= 65536 and sum(api.chunks) == len(expected)


@pytest.mark.parametrize("failure", ["no-attachment", "missing-file", "corrupt-file", "traversal-key", "type-mismatch"])
def test_missing_or_invalid_media_fails_closed_without_exposing_private_paths(inbox_api, failure):
    api, db = inbox_api, inbox_api.db
    outside = db.broadcast_storage.root.parent / "outside.jpg"
    outside.write_bytes(b"unrelated private file")
    if failure == "no-attachment":
        row = publish(db)
    else:
        row, _, _ = with_media(db)
        path = db.broadcast_storage.root / row["media_key"]
        if failure == "missing-file":
            path.unlink()
        elif failure == "corrupt-file":
            path.write_bytes(b"\xff\xd8\xffcorrupt")
        elif failure == "traversal-key":
            db.query("UPDATE broadcasts SET media_key='../outside.jpg' WHERE id=%s", (row["id"],))
        else:
            db.query("UPDATE broadcasts SET media_type='video' WHERE id=%s", (row["id"],))
    token = db.db.create_app_session(4)
    response = api.client.get(detail_path(row) + "/media", headers=bearer(token))
    assert response.status_code == 404 and response.json() == {"detail": {"code": "BROADCAST_MEDIA_NOT_FOUND"}}
    assert str(db.broadcast_storage.root) not in response.text
    assert "../outside.jpg" not in response.text
    if row.get("media_key"):
        assert row["media_key"] not in response.text
    assert outside.read_bytes() == b"unrelated private file"
    assert_private_headers(response)
