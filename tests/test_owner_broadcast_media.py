"""Native photo/video preview, media retention and shop-isolated mocked sends."""

import asyncio
from io import BytesIO
import importlib
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

from PIL import Image
import pytest
from fastapi import HTTPException
from starlette.requests import Request

from conftest import bearer
from test_barista_operations import operations_database
from test_barista_owner_tools import owner_api, owner_database, preview, send


def photo_data(format="JPEG", size=(32,32)):
    data = BytesIO()
    Image.new("RGB", size, "brown").save(data, format=format)
    return data.getvalue()


def video_data(brand=b"isom"):
    def box(kind, data):
        return (8+len(data)).to_bytes(4,"big")+kind+data
    return box(b"ftyp",brand+b"\x00"*4+brand)+box(b"moov",b"")+box(b"mdat",b"test")


def media_preview(client, token, *, kind="photo", data=None, filename="attachment.jpg", text="Запрошуємо на каву", **extra):
    return client.post("/barista/owner/broadcast/preview", headers=bearer(token),
                       data={"media_kind":kind,"text":text,**extra},
                       files={"media":(filename,photo_data() if data is None else data,"application/octet-stream")})


@pytest.mark.parametrize("kind,data,name,mime", [
    ("photo",photo_data(),"picture.jpg","image/jpeg"),
    ("photo",photo_data("PNG"),"picture.png","image/png"),
    ("video",video_data(),"video.mp4","video/mp4"),
    ("video",video_data(b"qt  "),"video.mov","video/quicktime"),
], ids=["jpeg", "png", "mp4", "mov"])
def test_valid_native_media_is_pinned_to_shop_preview_and_cleared_before_delivery(
    owner_api,owner_database,kind,data,name,mime,
):
    client,delivery = owner_api
    token = owner_database.staff_token()
    result = media_preview(client,token,kind=kind,data=data,filename="../../"+name)
    assert result.status_code == 200
    body = result.json()
    assert body["media"] == {"kind":kind,"filename":name,"size_bytes":len(data),"mime_type":mime}
    assert body["shop"]["id"] == 1 and body["recipients_count"] == 1
    stored = owner_database.query("SELECT * FROM admin_broadcast_previews")[0]
    assert bytes(stored["media_bytes"]) == data
    result = send(client,token,body["confirmation_token"])
    assert result.status_code == 200
    captured = delivery.await_args.args[0]
    assert captured["media"]["bytes"] == data and captured["media"]["kind"] == kind
    assert captured["recipients"] == [{"user_id":4,"telegram_user_id":1004}]
    assert captured["text"] == "Запрошуємо на каву"
    stored = owner_database.query("SELECT * FROM admin_broadcast_previews")[0]
    assert stored["media_bytes"] is None and stored["used_at"] is not None
    assert send(client,token,body["confirmation_token"]).status_code == 409
    assert delivery.await_count == 1


def test_media_only_and_json_text_backwards_compatibility(owner_api,owner_database):
    client,_ = owner_api
    token = owner_database.staff_token()
    media = media_preview(client,token,text="")
    assert media.status_code == 200 and media.json()["text"] == ""
    assert send(client,token,media.json()["confirmation_token"]).status_code == 200
    text = preview(client,token)
    assert text.status_code == 200 and text.json()["media"] is None


@pytest.mark.parametrize("kind,data,status", [
    ("photo",b"GIF89a",415),
    ("photo",b"\xff\xd8\xffinvalid",422),
    ("photo",photo_data("PNG",(10001,1)),422),
    ("video",b"not a video",415),
    ("video",b"\x00\x00\x00\x10ftypisom\x00\x00\x00\x00",415),
    ("video",video_data()[:-2],422),
    ("animation",photo_data(),422),
    ("photo",b"",422),
], ids=["gif", "corrupt-jpeg", "bad-dimensions", "unknown-video", "short-ftyp", "truncated-video", "unknown-kind", "empty"])
def test_media_magic_container_and_photo_dimensions_fail_closed(owner_api,owner_database,kind,data,status):
    client,delivery = owner_api
    response = media_preview(client,owner_database.staff_token(),kind=kind,data=data)
    assert response.status_code == status
    assert owner_database.query("SELECT * FROM admin_broadcast_previews") == []
    delivery.assert_not_awaited()


def test_caption_and_extra_fields_do_not_override_context_or_media(owner_api,owner_database):
    client,_ = owner_api
    token = owner_database.staff_token()
    assert media_preview(client,token,text="x"*1025).status_code == 422
    for field in ("shop_id","user_id","role","admin_user_id","media_url","file_id"):
        assert media_preview(client,token,**{field:"2"}).status_code in (400,422)
    assert owner_database.query("SELECT * FROM admin_broadcast_previews") == []


def test_admin_and_client_cannot_upload_or_create_media_preview(owner_api,owner_database):
    client,_ = owner_api
    assert media_preview(client,"legacy-active").status_code == 401
    token = owner_database.staff_token(user_id=2,membership_id=3)
    assert media_preview(client,token).status_code == 403
    assert owner_database.query("SELECT * FROM admin_broadcast_previews") == []


def test_utf16_caption_and_text_boundaries_match_native_client(owner_api, owner_database):
    client, _ = owner_api
    token = owner_database.staff_token()
    assert media_preview(client, token, text="🎁" * 512).status_code == 200
    assert media_preview(client, token, text="🎁" * 513).status_code == 422
    assert preview(client, token, "🎁" * 2048).status_code == 200
    assert preview(client, token, "🎁" * 2049).status_code == 422


@pytest.mark.parametrize("kind,limit", [("photo", 8 * 1024 * 1024), ("video", 20 * 1024 * 1024)])
def test_binary_limit_rejects_oversize_before_media_decoder(kind, limit):
    module = importlib.import_module("app.broadcast_media")
    with pytest.raises(HTTPException) as error:
        module.validate_media(kind, "attachment", b"x" * (limit + 1))
    assert error.value.status_code == 413
    assert error.value.detail["code"] == "MEDIA_TOO_LARGE"


def test_media_upload_is_not_consumed_before_owner_authorization(owner_api, owner_database, monkeypatch):
    client, _ = owner_api
    module = importlib.import_module("app.api.barista_owner")
    parse = AsyncMock()
    monkeypatch.setattr(module, "multipart_media", parse)
    token = owner_database.staff_token(user_id=2, membership_id=3)
    assert media_preview(client, token).status_code == 403
    assert media_preview(client, "legacy-active").status_code == 401
    parse.assert_not_awaited()


def test_foreign_shop_or_other_session_cannot_send_saved_media(owner_api, owner_database):
    client, delivery = owner_api
    token = owner_database.staff_token()
    body = media_preview(client, token).json()
    foreign = owner_database.staff_token(user_id=3, membership_id=2)
    assert send(client, foreign, body["confirmation_token"]).status_code == 403
    assert send(client, owner_database.staff_token(), body["confirmation_token"]).status_code == 403
    stored = owner_database.query("SELECT * FROM admin_broadcast_previews")[0]
    assert stored["media_bytes"] is not None and stored["used_at"] is None
    delivery.assert_not_awaited()


def test_unknown_media_preview_cannot_bypass_raw_stream_size_cap():
    module = importlib.import_module("app.broadcast_media")
    consumed = []

    async def receive():
        consumed.append(True)
        return {"type":"http.request","body":b"123456","more_body":False}

    request = Request({"type":"http","method":"POST","path":"/","headers":[],"query_string":b""},receive)
    with pytest.raises(HTTPException) as error:
        asyncio.run(module.bounded_body(request,5))
    assert error.value.status_code == 413 and len(consumed) == 1
    consumed.clear()
    request = Request({"type":"http","method":"POST","path":"/",
                       "headers":[(b"content-length",b"6")],"query_string":b""},receive)
    with pytest.raises(HTTPException) as error:
        asyncio.run(module.bounded_body(request,5))
    assert error.value.status_code == 413 and consumed == []


def test_owner_role_is_rechecked_after_media_upload_before_storage(owner_api,owner_database,monkeypatch):
    client,_ = owner_api
    module = importlib.import_module("app.api.barista_owner")
    original = module.multipart_media

    async def revoke_while_uploading(request):
        result = await original(request)
        owner_database.query("UPDATE shop_admins SET role='admin' WHERE id=1")
        return result

    monkeypatch.setattr(module,"multipart_media",revoke_while_uploading)
    response = media_preview(client,owner_database.staff_token())
    assert response.status_code == 403
    assert owner_database.query("SELECT * FROM admin_broadcast_previews") == []


def test_selected_shop_change_during_upload_requires_new_preview(owner_api,owner_database,monkeypatch):
    client,_ = owner_api
    database = owner_database
    second = database.add_second_membership()
    database.query("UPDATE shop_admins SET role='owner' WHERE id=%s",(second,))
    token = database.staff_token()
    module = importlib.import_module("app.api.barista_owner")
    original = module.multipart_media

    async def switch_context(request):
        result = await original(request)
        database.query("UPDATE app_sessions SET selected_membership_id=%s WHERE token_hash=%s",
                       (second,database.db._hash_barista_session_token(token)))
        return result

    monkeypatch.setattr(module,"multipart_media",switch_context)
    response = media_preview(client,token)
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "PREVIEW_CONTEXT_MISMATCH"
    assert database.query("SELECT * FROM admin_broadcast_previews") == []


def test_replacement_expiry_cleanup_and_creation_budget_bound_storage(owner_api,owner_database):
    client,_ = owner_api
    database = owner_database
    token = database.staff_token()
    first = media_preview(client,token).json()
    second = media_preview(client,token).json()
    rows = database.query("SELECT * FROM admin_broadcast_previews ORDER BY created_at")
    assert rows[0]["invalidated_at"] is not None and rows[0]["media_bytes"] is None
    assert rows[1]["invalidated_at"] is None and rows[1]["media_bytes"] is not None
    assert send(client,token,first["confirmation_token"]).status_code == 409
    database.query("UPDATE admin_broadcast_previews SET created_at=NOW()-INTERVAL '20 minutes',expires_at=NOW()-INTERVAL '1 minute'")
    module = importlib.import_module("app.admin_tools")
    with database.db.get_connection() as connection:
        with connection.transaction():
            module.cleanup_broadcast_previews(connection)
    assert all(row["media_bytes"] is None for row in database.query("SELECT * FROM admin_broadcast_previews"))
    assert send(client,token,second["confirmation_token"]).status_code == 409
    for _ in range(8):
        assert media_preview(client,token).status_code == 200
    assert media_preview(client,token).status_code == 429
    assert sum(row["media_bytes"] is not None for row in database.query("SELECT * FROM admin_broadcast_previews")) == 1
    database.query("UPDATE admin_broadcast_previews SET created_at=NOW()-INTERVAL '25 hours',expires_at=NOW()-INTERVAL '24 hours'")
    with database.db.get_connection() as connection:
        with connection.transaction():
            module.cleanup_broadcast_previews(connection)
    assert database.query("SELECT * FROM admin_broadcast_previews") == []


def test_pending_media_limit_applies_across_owner_sessions(owner_api,owner_database):
    client,_ = owner_api
    database = owner_database
    for _ in range(5):
        assert media_preview(client,database.staff_token()).status_code == 200
    assert media_preview(client,database.staff_token()).status_code == 429
    assert len(database.query("SELECT * FROM admin_broadcast_previews WHERE invalidated_at IS NULL")) == 5


@pytest.mark.parametrize("kind,method",[("photo","send_photo"),("video","send_video")])
def test_native_delivery_passes_buffered_media_and_caption_only_to_verified_recipients(owner_api,monkeypatch,kind,method):
    module = importlib.import_module("app.api.barista_owner")
    bot = SimpleNamespace(send_photo=AsyncMock(),send_video=AsyncMock(),send_message=AsyncMock(),
                          session=SimpleNamespace(close=AsyncMock()))
    monkeypatch.setitem(sys.modules,"app.config",SimpleNamespace(BOT_TOKEN="offline-token"))
    monkeypatch.setattr(module,"Bot",lambda token:bot)
    data = photo_data() if kind == "photo" else video_data()
    touched,failed = asyncio.run(owner_api[1].original({
        "shop_id": 1,
        "text":"Caption", "recipients":[{"user_id":4,"telegram_user_id":1004}],
        "media":{"kind":kind,"filename":"attachment","bytes":data},
    }))
    assert touched == [4] and failed == 0
    call = getattr(bot,method).await_args.kwargs
    assert call["chat_id"] == 1004 and call["caption"] == "Caption"
    assert call[kind].data == data
    bot.send_message.assert_not_awaited()
    bot.session.close.assert_awaited_once()


@pytest.mark.parametrize("kind,method", [("photo", "send_photo"), ("video", "send_video")])
def test_native_delivery_reuses_only_telegram_returned_media_id(owner_api, monkeypatch, kind, method):
    module = importlib.import_module("app.api.barista_owner")
    response = (SimpleNamespace(photo=[SimpleNamespace(file_id="bot-uploaded-photo")]) if kind == "photo"
                else SimpleNamespace(video=SimpleNamespace(file_id="bot-uploaded-video")))
    bot = SimpleNamespace(send_photo=AsyncMock(return_value=response), send_video=AsyncMock(return_value=response),
                          send_message=AsyncMock(), session=SimpleNamespace(close=AsyncMock()))
    monkeypatch.setitem(sys.modules, "app.config", SimpleNamespace(BOT_TOKEN="offline-token"))
    monkeypatch.setattr(module, "Bot", lambda token: bot)
    data = photo_data() if kind == "photo" else video_data()
    touched, failed = asyncio.run(owner_api[1].original({
        "shop_id": 1,
        "text": "Caption", "recipients": [{"user_id": 4, "telegram_user_id": 1004},
                                          {"user_id": 5, "telegram_user_id": 1005}],
        "media": {"kind": kind, "filename": "attachment", "bytes": data},
    }))
    calls = getattr(bot, method).await_args_list
    assert touched == [4, 5] and failed == 0
    assert calls[0].kwargs[kind].data == data
    assert calls[1].kwargs[kind] == "bot-uploaded-" + kind
    assert [call.kwargs["chat_id"] for call in calls] == [1004, 1005]
    assert all(call.kwargs["caption"] == "Caption" for call in calls)
    bot.send_message.assert_not_awaited()
    bot.session.close.assert_awaited_once()


@pytest.mark.parametrize("column", ["invalidated_at", "media_bytes"])
def test_missing_media_migration_returns_503_without_consuming_upload(owner_api,owner_database,monkeypatch,column):
    client,_ = owner_api
    token = owner_database.staff_token()
    # This DDL is only within the guarded local temporary test schema.
    owner_database.query(f"ALTER TABLE admin_broadcast_previews DROP COLUMN {column} CASCADE")
    module = importlib.import_module("app.api.barista_owner")
    parse = AsyncMock()
    monkeypatch.setattr(module, "multipart_media", parse)
    response = media_preview(client,token)
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "BROADCAST_NOT_CONFIGURED"
    parse.assert_not_awaited()


def test_cleanup_startup_is_safe_before_new_feature_migration(owner_api, owner_database):
    client, _ = owner_api
    owner_database.query("DROP TABLE admin_broadcast_previews")
    # Start/shutdown the actual included router handlers: missing migration
    # disables the new feature, while startup and existing client routes survive.
    with client:
        assert client.get("/barista/me").status_code == 401
