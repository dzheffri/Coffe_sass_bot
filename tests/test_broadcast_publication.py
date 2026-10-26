"""Confirmed publication on guarded local PostgreSQL and private temporary media.

Actual native routes/transactions run; Telegram/APNs delivery is mocked.
"""

from concurrent.futures import ThreadPoolExecutor
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fastapi import HTTPException
from psycopg import InterfaceError, OperationalError
import pytest

from app.broadcast_publication import BroadcastPublication, publish_bot_broadcast, record_broadcast_delivery
from test_barista_operations import operations_database
from test_barista_owner_tools import owner_api, owner_database, preview, send
from test_owner_broadcast_media import media_preview, photo_data, video_data
from test_bot_broadcast_publication import owner, _callback, _saved_preview, _seed_state, _state


@pytest.fixture
def audience(owner_database):
    db = owner_database
    db.query("""
        INSERT INTO shop_clients(shop_id,user_id,last_activity_at) VALUES(1,2,NOW());
        INSERT INTO users(id,telegram_user_id,personal_qr_token) VALUES(7,1007,'recent-publication-user');
        INSERT INTO shop_clients(shop_id,user_id,last_activity_at)
            VALUES(1,7,NOW()-INTERVAL '59 days');
    """)
    return db


def snapshot(db):
    return db.query("""SELECT broadcast_id,user_id,read_at,delivery_status
        FROM broadcast_recipients ORDER BY broadcast_id,user_id""")


def test_native_text_confirmation_snapshots_all_shop_clients_without_changing_telegram(audience, owner_api):
    db = audience
    client, delivery = owner_api
    token = db.staff_token()
    body = preview(client, token, "Уся наша аудиторія").json()
    # Native owner broadcasts include old/inactive clients as well as users
    # without Telegram. The selected shop still excludes user 5 from shop 2.
    assert body["recipients_count"] == 4
    assert db.query("SELECT * FROM broadcasts") == []
    accepted = send(client, token, body["confirmation_token"])
    assert accepted.status_code == 202 and accepted.json()["status"] == "queued"
    assert db.query("SELECT * FROM touch_logs") == []
    delivery.drain()
    row = db.query("SELECT * FROM broadcasts")[0]
    assert row["text"] == "Уся наша аудиторія" and row["shop_id"] == 1 and row["sender_user_id"] == 1
    assert row["media_type"] is None and row["media_key"] is None
    assert snapshot(db) == [
        {"broadcast_id": row["id"], "user_id": user, "read_at": None, "delivery_status": None}
        for user in (2, 4, 6, 7)]
    # Publication delivery receives the complete inbox snapshot, including
    # users without Telegram.  The APNs-first adapter decides per recipient
    # whether Telegram fallback is needed after the claim.
    assert {call.args[1]["user_id"] for call in delivery.worker_push.await_args_list} == {2, 4, 6, 7}
    assert delivery.worker_push.await_args.args[0]["shop_name"] == "Наші"
    assert row["recipients_count"] == 3
    assert {row["user_id"] for row in db.query("SELECT user_id FROM touch_logs")} == {4, 6, 7}
    repeated = send(client, token, body["confirmation_token"])
    assert repeated.status_code == 202 and repeated.json()["broadcast_id"] == accepted.json()["broadcast_id"]
    assert len(db.query("SELECT * FROM broadcasts")) == 1 and len(snapshot(db)) == 4
    delivery.drain()
    assert delivery.worker_bot.send_message.await_count == 3


@pytest.mark.parametrize("kind,data,filename,text", [
    ("photo", photo_data(), "picture.jpg", "Фото"),
    ("photo", photo_data("PNG"), "picture.png", "PNG"),
    ("video", video_data(), "clip.mp4", "Відео"),
    ("video", video_data(b"qt  "), "clip.mov", "MOV"),
    ("photo", photo_data(), "picture.jpg", ""),
    ("video", video_data(), "clip.mp4", ""),
], ids=["jpeg", "png", "mp4", "mov", "photo-only", "video-only"])
def test_native_media_persists_original_content_and_private_key(audience, owner_api, kind, data, filename, text):
    db = audience
    client, delivery = owner_api
    token = db.staff_token()
    body = media_preview(client, token, kind=kind, data=data, filename=filename, text=text).json()
    assert not db.broadcast_storage.root.exists()
    assert send(client, token, body["confirmation_token"]).status_code == 202
    delivery.drain()
    row = db.query("SELECT * FROM broadcasts")[0]
    assert row["media_type"] == kind and row["media_key"]
    assert row["text"] == text
    assert db.broadcast_storage.get(row["media_key"])["bytes"] == data
    assert [path.name for path in db.broadcast_storage.root.iterdir()] == [row["media_key"]]
    assert len(snapshot(db)) == 4
    payload = delivery.worker_push.await_args.args[0]
    assert db.broadcast_storage.get(payload["media_key"])["bytes"] == data and payload["text"] == text
    assert payload["shop_name"] == "Наші"
    assert {call.args[1]["user_id"] for call in delivery.worker_push.await_args_list} == {2, 4, 6, 7}
    stored = db.query("SELECT media_bytes,used_at FROM admin_broadcast_previews")[0]
    assert stored["media_bytes"] is None and stored["used_at"] is not None


@pytest.mark.parametrize("text", ["Текст залишається", ""], ids=["caption", "media-only"])
def test_native_storage_failure_does_not_break_original_delivery_or_create_broken_key(
    audience, owner_api, monkeypatch, caplog, text,
):
    db = audience
    client, delivery = owner_api
    monkeypatch.setattr(db.broadcast_storage, "save", lambda media: (_ for _ in ()).throw(OSError("private failure")))
    token = db.staff_token()
    data = photo_data()
    body = media_preview(client, token, data=data, text=text).json()
    assert send(client, token, body["confirmation_token"]).status_code == 202
    fallback = db.query("SELECT fallback_media_bytes FROM broadcast_delivery_jobs")[0]
    assert bytes(fallback["fallback_media_bytes"]) == data
    delivery.drain()
    row = db.query("SELECT * FROM broadcasts")[0]
    assert row["text"] == text and row["media_type"] is None and row["media_key"] is None
    assert len(snapshot(db)) == 4
    assert delivery.worker_bot.send_photo.await_args.kwargs["photo"].data == data
    assert delivery.worker_bot.send_photo.await_count == 3
    assert "Broadcast media persistence failed (OSError)" in caplog.text
    assert "private failure" not in caplog.text


def test_app_only_audience_can_publish_without_telegram_devices(owner_database, owner_api):
    db = owner_database
    db.query("DELETE FROM shop_clients WHERE shop_id=1")
    db.query("INSERT INTO shop_clients(shop_id,user_id,last_activity_at) VALUES(1,2,NOW())")
    client, delivery = owner_api
    token = db.staff_token()
    body = preview(client, token).json()
    assert body["recipients_count"] == 1
    result = send(client, token, body["confirmation_token"])
    assert result.status_code == 202 and result.json()["sent"] == 0
    delivery.drain()
    assert [row["user_id"] for row in snapshot(db)] == [2]
    recipient = delivery.worker_push.await_args.args[1]
    assert recipient["telegram_user_id"] is None and recipient["user_id"] == 2
    delivery.worker_bot.send_message.assert_not_awaited()


def test_native_snapshot_includes_null_activity_client_and_excludes_other_shop(owner_database, owner_api):
    db = owner_database
    db.query("ALTER TABLE shop_clients ALTER COLUMN last_activity_at DROP NOT NULL")
    db.query("""
        INSERT INTO users(id,telegram_user_id,personal_qr_token)
        VALUES (8,NULL,'null-activity-client');
        INSERT INTO shop_clients(shop_id,user_id,last_activity_at)
        VALUES (1,8,NULL)
    """)
    client, delivery = owner_api
    token = db.staff_token()
    body = preview(client, token, "Для всіх клієнтів").json()

    assert body["recipients_count"] == 3
    assert send(client, token, body["confirmation_token"]).status_code == 202
    delivery.drain()
    recipients = [call.args[1] for call in delivery.worker_push.await_args_list]
    assert {row["user_id"] for row in recipients} == {4, 6, 8}
    assert {row["user_id"] for row in snapshot(db)} == {4, 6, 8}
    assert 5 not in {row["user_id"] for row in recipients}


def test_native_preview_becomes_stale_when_old_client_is_added(owner_database, owner_api):
    db = owner_database
    client, delivery = owner_api
    token = db.staff_token()
    body = preview(client, token).json()
    db.query("""
        INSERT INTO users(id,telegram_user_id,personal_qr_token)
        VALUES (8,1008,'old-after-preview');
        INSERT INTO shop_clients(shop_id,user_id,last_activity_at)
        VALUES (1,8,NOW()-INTERVAL '365 days')
    """)

    response = send(client, token, body["confirmation_token"])
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "PREVIEW_STALE"
    assert db.query("SELECT * FROM broadcasts") == []
    assert snapshot(db) == []
    delivery.assert_not_awaited()


def test_native_preview_is_stable_when_only_client_activity_changes(owner_database, owner_api):
    db = owner_database
    client, delivery = owner_api
    token = db.staff_token()
    body = preview(client, token).json()
    db.query("UPDATE shop_clients SET last_activity_at=NOW() WHERE shop_id=1 AND user_id=6")

    response = send(client, token, body["confirmation_token"])
    assert response.status_code == 202
    delivery.drain()
    assert delivery.worker_bot.send_message.await_count == 2


def test_new_app_only_audience_member_invalidates_native_preview(owner_database, owner_api):
    db = owner_database
    client, delivery = owner_api
    token = db.staff_token()
    body = preview(client, token).json()
    db.query("INSERT INTO shop_clients(shop_id,user_id,last_activity_at) VALUES(1,2,NOW())")
    response = send(client, token, body["confirmation_token"])
    assert response.status_code == 409 and response.json()["detail"]["code"] == "PREVIEW_STALE"
    assert db.query("SELECT * FROM broadcasts") == [] and snapshot(db) == []
    delivery.assert_not_awaited()


def test_native_concurrent_confirmation_creates_one_broadcast_snapshot_and_file(audience, owner_api):
    db = audience
    client, delivery = owner_api
    token = db.staff_token()
    body = media_preview(client, token).json()
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: send(client, token, body["confirmation_token"]), range(2)))
    assert sorted(response.status_code for response in results) == [202, 202]
    assert results[0].json()["broadcast_id"] == results[1].json()["broadcast_id"]
    assert len(db.query("SELECT * FROM broadcasts")) == 1 and len(snapshot(db)) == 4
    assert len(list(db.broadcast_storage.root.iterdir())) == 1
    delivery.drain()
    assert delivery.worker_bot.send_photo.await_count == 3


def test_recipient_insert_failure_rolls_back_broadcast_preview_and_private_file(audience, owner_api):
    db = audience
    db.query("""ALTER TABLE broadcast_recipients
        ADD CONSTRAINT forced_local_failure CHECK(user_id<>2)""")
    client, delivery = owner_api
    token = db.staff_token()
    body = media_preview(client, token).json()
    with pytest.raises(Exception, match="forced_local_failure"):
        send(client, token, body["confirmation_token"])
    assert db.query("SELECT * FROM broadcasts") == [] and snapshot(db) == []
    assert list(db.broadcast_storage.root.iterdir()) == []
    pending = db.query("SELECT used_at,media_bytes FROM admin_broadcast_previews")[0]
    assert pending["used_at"] is None and pending["media_bytes"] is not None
    delivery.assert_not_awaited()


def test_expiry_after_media_save_rolls_back_and_removes_unreferenced_file(audience, owner_api, monkeypatch):
    db = audience
    from app import broadcast_publication

    original = broadcast_publication.BroadcastPublication.publish

    def expire_after_insert(self, connection, *args, **kwargs):
        row = original(self, connection, *args, **kwargs)
        connection.execute("""UPDATE admin_broadcast_previews
            SET created_at=NOW()-INTERVAL '2 minutes',expires_at=clock_timestamp()-INTERVAL '1 minute'""")
        return row

    monkeypatch.setattr(broadcast_publication.BroadcastPublication, "publish", expire_after_insert)
    client, delivery = owner_api
    token = db.staff_token()
    body = media_preview(client, token).json()
    response = send(client, token, body["confirmation_token"])
    assert response.status_code == 409 and response.json()["detail"]["code"] == "PREVIEW_EXPIRED"
    assert db.query("SELECT * FROM broadcasts") == [] and snapshot(db) == []
    assert list(db.broadcast_storage.root.iterdir()) == []
    assert db.query("SELECT used_at FROM admin_broadcast_previews") == [{"used_at": None}]
    delivery.assert_not_awaited()


def test_shared_recipient_batch_deduplicates_user_ids(audience):
    db = audience
    recipients = [{"user_id": 2}, {"user_id": 4}, {"user_id": 2}]
    with BroadcastPublication(db.broadcast_storage) as publication:
        with db.db.get_connection() as connection:
            with connection.transaction():
                row = publication.publish(connection, 1, 1, "Batch", recipients)
    assert snapshot(db) == [
        {"broadcast_id": row["id"], "user_id": user, "read_at": None, "delivery_status": None}
        for user in (2, 4)]


def test_bot_shared_publication_uses_same_full_snapshot_and_existing_sent_accounting(audience):
    db = audience
    result = publish_bot_broadcast(1, 1001, "Bot content")
    assert {row["user_id"] for row in result["recipients"]} == {2, 4, 7}
    assert len(snapshot(db)) == 3
    record_broadcast_delivery(1, result["broadcast_id"], [4, 7])
    rows = db.query("SELECT * FROM broadcasts")
    assert len(rows) == 1 and rows[0]["recipients_count"] == 2
    assert rows[0]["sender_user_id"] == 1 and rows[0]["text"] == "Bot content"
    assert len(snapshot(db)) == 3


@pytest.mark.parametrize("shop_id,actor,status", [(1, 1004, 403), (2, 1001, 403), (999, 1001, 403)])
def test_bot_publication_rechecks_owner_and_shop_scope(owner_database, shop_id, actor, status):
    with pytest.raises(HTTPException) as error:
        publish_bot_broadcast(shop_id, actor, "Wrong scope")
    assert error.value.status_code == status
    assert owner_database.query("SELECT * FROM broadcasts") == [] and snapshot(owner_database) == []


def test_bot_quota_is_checked_with_publication_and_existing_explicit_exemption(audience):
    db = audience
    db.query("INSERT INTO broadcasts(shop_id,sender_user_id,text) SELECT 1,1,'old' FROM generate_series(1,8)")
    with pytest.raises(HTTPException) as error:
        publish_bot_broadcast(1, 1001, "Over quota")
    assert error.value.status_code == 429 and snapshot(db) == []
    result = publish_bot_broadcast(1, 1001, "Existing exempt shop", quota_exempt=True)
    assert len(db.query("SELECT * FROM broadcasts")) == 9
    assert {row["broadcast_id"] for row in snapshot(db)} == {result["broadcast_id"]}


def test_commit_response_loss_never_deletes_referenced_media(audience):
    db = audience
    with pytest.raises(RuntimeError, match="simulated lost commit response"):
        with BroadcastPublication(db.broadcast_storage) as publication:
            with db.db.get_connection() as connection:
                with connection.transaction():
                    row = publication.publish(connection, 1, 1, "Committed", [{"user_id": 2}],
                                              {"kind": "photo", "filename": "image.jpg", "bytes": photo_data()})
            # Commit succeeded; caller can still lose acknowledgement afterward.
            raise RuntimeError("simulated lost commit response")
    assert db.broadcast_storage.get(row["media_key"])["bytes"] == photo_data()
    assert len(snapshot(db)) == 1


@pytest.mark.parametrize("failure", [InterfaceError, OperationalError])
def test_unknown_commit_outcome_retains_media_instead_of_racing_commit(audience, failure, caplog):
    db = audience
    with pytest.raises(failure):
        with BroadcastPublication(db.broadcast_storage) as publication:
            with db.db.get_connection() as connection:
                with connection.transaction():
                    row = publication.publish(connection, 1, 1, "Uncertain", [{"user_id": 2}],
                                              {"kind": "photo", "filename": "image.jpg", "bytes": photo_data()})
                    raise failure("simulated uncertain connection")
    assert db.query("SELECT * FROM broadcasts") == [] and snapshot(db) == []
    assert db.broadcast_storage.get(row["media_key"])["bytes"] == photo_data()
    assert "Broadcast media cleanup deferred" in caplog.text


@pytest.mark.parametrize("kind,data", [(None, None), ("photo", photo_data()), ("video", video_data())],
                         ids=["text", "photo", "video"])
def test_bot_confirmation_downloads_publishes_and_delivers_through_real_shared_storage(audience, owner, kind, data):
    db = audience
    from app.telegram_broadcast_media import download_telegram_broadcast_media

    owner.namespace.update({
        "publish_bot_broadcast": publish_bot_broadcast,
        "record_broadcast_delivery": record_broadcast_delivery,
        "download_telegram_broadcast_media": download_telegram_broadcast_media,
        "get_admin_shop_and_role": Mock(return_value={"id": 1, "name": "Наші", "role": "owner"}),
        "get_shop": Mock(return_value={"id": 1, "is_active": True}),
    })

    async def scenario():
        state, callback = _state(actor=1001), _callback()
        callback.from_user.id = 1001
        callback.bot.get_file = AsyncMock(return_value=SimpleNamespace(
            file_size=len(data) if data else 0, file_path="offline/private-media"))

        async def download(path, *, destination, **kwargs):
            destination.write(data)
            destination.seek(0)

        callback.bot.download_file = AsyncMock(side_effect=download)
        reference = {"kind": kind, "file_id": "offline-id", "filename": "attachment"} if kind else None
        await _seed_state(state, _saved_preview(broadcast_shop_id=1, broadcast_media_reference=reference))
        await owner.broadcast_confirm_callback(callback, state)
        row = db.query("SELECT * FROM broadcasts")[0]
        assert row["shop_id"] == 1 and row["sender_user_id"] == 1 and row["text"] == "Original caption"
        assert row["recipients_count"] == 2 and len(snapshot(db)) == 3
        if kind:
            assert row["media_type"] == kind
            assert db.broadcast_storage.get(row["media_key"])["bytes"] == data
            callback.bot.get_file.assert_awaited_once_with("offline-id")
        else:
            assert row["media_type"] is None and row["media_key"] is None
            callback.bot.get_file.assert_not_awaited()
        if kind:
            sender = callback.bot.copy_message
            assert [call.kwargs for call in sender.await_args_list] == [
                {"chat_id": user, "from_chat_id": 888, "message_id": 55,
                 "caption": "☕️ Наші\n\nOriginal caption", "parse_mode": None} for user in (1004, 1007)]
        else:
            sender = callback.bot.send_message
            assert [call.kwargs for call in sender.await_args_list] == [
                {"chat_id": user, "text": "☕️ Наші\n\nOriginal caption", "parse_mode": None} for user in (1004, 1007)]
            callback.bot.copy_message.assert_not_awaited()
        await owner.broadcast_confirm_callback(callback, state)
        assert len(db.query("SELECT * FROM broadcasts")) == 1
        assert len(snapshot(db)) == 3 and sender.await_count == 2

    asyncio.run(scenario())
