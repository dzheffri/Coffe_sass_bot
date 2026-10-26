"""Offline bot publication tests; no backend startup, database or network.

Only function definitions are extracted from owner.py. Aiogram's real memory
FSM storage and event isolation exercise claim races, while all external work
is mocked. Telegram media is downloaded only into an in-memory test buffer.
"""

import ast
import asyncio
import copy
import logging
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import uuid

from aiogram import types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage, SimpleEventIsolation
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from fastapi import HTTPException
from PIL import Image
import pytest


PROJECT = Path(__file__).resolve().parents[1]
ACTOR = 9002
SHOP_ID = 73
PREVIEW_TOKEN = "1" * 32
BROADCAST_ID = 405
WAITING_STATE = State("waiting_broadcast_text", group_name="OwnerStates")


def _extract_owner(namespace):
    """Compile definitions without decorators, imports, or startup statements."""
    path = PROJECT / "app/handlers/owner.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    nodes = []
    for original in tree.body:
        if (isinstance(original, (ast.FunctionDef, ast.AsyncFunctionDef))
                and original.name.startswith(("broadcast_", "_broadcast_"))):
            node = copy.deepcopy(original)
            node.decorator_list = []
            nodes.append(node)
        elif (isinstance(original, ast.Assign)
              and isinstance(original.value, ast.Call)
              and isinstance(original.value.func, ast.Name)
              and original.value.func.id == "SimpleEventIsolation"):
            # This one constructor is safe and gives each harness a real lock.
            nodes.append(copy.deepcopy(original))
    module = ast.Module(body=[
        ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
        *nodes,
    ], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), "exec"), namespace)


def _state(*, actor=ACTOR, storage=None):
    return FSMContext(
        storage=storage or MemoryStorage(),
        key=StorageKey(bot_id=9001, chat_id=777, user_id=actor),
    )


class _YieldingMemoryStorage(MemoryStorage):
    """Keep real storage semantics while exposing read/clear interleavings."""

    async def get_data(self, key):
        snapshot = await super().get_data(key)
        await asyncio.sleep(0)
        return snapshot

    async def get_state(self, key):
        snapshot = await super().get_state(key)
        await asyncio.sleep(0)
        return snapshot


async def _seed_state(state, data):
    await state.set_state(WAITING_STATE)
    await state.set_data(data)


def _saved_preview(**overrides):
    return {
        "broadcast_text": "Original caption",
        "broadcast_from_chat_id": 888,
        "broadcast_message_id": 55,
        "broadcast_shop_id": SHOP_ID,
        "broadcast_preview_token": PREVIEW_TOKEN,
        "broadcast_media_reference": None,
        "broadcast_has_attachment": bool(overrides.get('broadcast_media_reference')),
        **overrides,
    }


def _message(kind="text", *, actor=ACTOR, caption="Original caption"):
    message = SimpleNamespace(
        from_user=SimpleNamespace(id=actor), chat=SimpleNamespace(id=777),
        message_id=44, text="Original text" if kind == "text" else None,
        caption=None if kind == "text" else caption,
        photo=None, video=None, animation=None, document=None,
        bot=SimpleNamespace(copy_message=AsyncMock(), send_message=AsyncMock()),
        answer=AsyncMock(), edit_reply_markup=AsyncMock(),
    )
    if kind == "photo":
        message.photo = [
            SimpleNamespace(file_id="small-photo", file_size=20),
            SimpleNamespace(file_id="largest-photo", file_size=200),
        ]
    elif kind in {"video", "animation", "document"}:
        setattr(message, kind, SimpleNamespace(
            file_id=f"{kind}-id", file_size=200,
            file_name=f"original.{ 'mov' if kind == 'video' else 'gif' }",
        ))
    return message


def _callback(action="confirm", *, token=PREVIEW_TOKEN, actor=ACTOR):
    return SimpleNamespace(
        data=f"broadcast_{action}:{token}",
        from_user=SimpleNamespace(id=actor), answer=AsyncMock(),
        message=_message(actor=actor),
        bot=SimpleNamespace(copy_message=AsyncMock(), send_message=AsyncMock()),
    )


@pytest.fixture
def owner():
    namespace = {
        "__name__": "offline_bot_broadcast_owner",
        "types": types, "FSMContext": FSMContext,
        "InlineKeyboardButton": InlineKeyboardButton,
        "InlineKeyboardMarkup": InlineKeyboardMarkup,
        "KeyboardButton": KeyboardButton, "ReplyKeyboardMarkup": ReplyKeyboardMarkup,
        "SimpleEventIsolation": SimpleEventIsolation,
        "uuid": uuid, "uuid4": uuid.uuid4, "asyncio": asyncio,
        "logging": logging, "logger": logging.getLogger("offline_bot_broadcast_owner"),
        "HTTPException": HTTPException, "MY_SHOP_ID": 1,
        "OwnerStates": SimpleNamespace(waiting_broadcast_text=WAITING_STATE),
        "is_owner": Mock(return_value=True),
        "get_admin_shop_and_role": Mock(return_value={
            "id": SHOP_ID, "name": "Offline shop", "role": "owner",
        }),
        "get_shop": Mock(return_value={"id": SHOP_ID, "is_active": True}),
        "can_send_broadcast": Mock(return_value=True),
        "get_broadcast_recipients": Mock(side_effect=AssertionError("Audience belongs to publication")),
        "save_broadcast": Mock(side_effect=AssertionError("Legacy history must not be written")),
        "log_broadcast_touches": Mock(side_effect=AssertionError("Legacy touch helper must not be called")),
        "download_telegram_broadcast_media": AsyncMock(return_value=None),
        "publish_bot_broadcast": Mock(return_value={
            "broadcast_id": BROADCAST_ID, "shop_id": SHOP_ID, "shop_name": "Offline shop",
            "text": "Original caption", "recipients": [
                {"user_id": 11, "telegram_user_id": 201},
                {"user_id": 12, "telegram_user_id": None},
            ],
        }),
        # App push is attempted for the complete publication recipient
        # snapshot.  The default emulates a user without an enabled device so
        # the existing Telegram fallback assertions remain deterministic.
        "send_broadcast_app_push": AsyncMock(return_value=None),
        "record_broadcast_delivery": Mock(),
    }

    async def threadpool(function, *args, **kwargs):
        # Preserve the async boundary without starting threads or external work.
        return function(*args, **kwargs)

    namespace["run_in_threadpool"] = AsyncMock(side_effect=threadpool)
    _extract_owner(namespace)
    namespace["owner_main_keyboard_for_user"] = Mock(return_value=None)
    return SimpleNamespace(namespace=namespace, **namespace)


def test_preview_keyboard_binds_both_actions_to_the_preview_token(owner):
    keyboard = owner.broadcast_preview_keyboard(PREVIEW_TOKEN)
    callbacks = {button.callback_data for row in keyboard.inline_keyboard for button in row}
    assert callbacks == {
        f"broadcast_confirm:{PREVIEW_TOKEN}", f"broadcast_cancel:{PREVIEW_TOKEN}",
    }
    assert all(len(value.encode("utf-8")) <= 64 for value in callbacks)


@pytest.mark.parametrize("kind", ["text", "photo", "video", "animation", "document"])
def test_preview_saves_shop_token_and_optional_reference_but_copies_original(owner, kind):
    async def scenario():
        state, message = _state(), _message(kind)
        await _seed_state(state, {})
        await owner.broadcast_preview_handler(message, state)
        data = await state.get_data()
        assert data["broadcast_shop_id"] == SHOP_ID
        assert data["broadcast_from_chat_id"] == 777
        assert data["broadcast_message_id"] == 44
        assert data["broadcast_text"] == ("Original text" if kind == "text" else "Original caption")
        token = data["broadcast_preview_token"]
        assert uuid.UUID(hex=token).hex == token and len(token) == 32
        reference = data["broadcast_media_reference"]
        if kind in {"photo", "video"}:
            assert set(reference) == {"kind", "file_id", "filename"}
            assert reference["kind"] == kind
            assert reference["file_id"] == ("largest-photo" if kind == "photo" else "video-id")
            assert isinstance(reference["filename"], str) and reference["filename"]
        else:
            assert reference is None
        message.bot.copy_message.assert_awaited_once()
        copied = message.bot.copy_message.await_args.kwargs
        assert {key: copied[key] for key in ("chat_id", "from_chat_id", "message_id")} == {
            "chat_id": 777, "from_chat_id": 777, "message_id": 44,
        }
        assert "text" not in copied and "caption" not in copied
        assert {button.callback_data for row in copied["reply_markup"].inline_keyboard for button in row} == {
            f"broadcast_confirm:{token}", f"broadcast_cancel:{token}",
        }
        owner.download_telegram_broadcast_media.assert_not_awaited()
        owner.publish_bot_broadcast.assert_not_called()
        message.bot.send_message.assert_not_awaited()

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["text", "photo", "video", "animation", "document"])
def test_confirmation_publishes_then_copies_original_only_to_telegram_audience(owner, kind):
    async def scenario():
        reference = ({"kind": kind, "file_id": f"{kind}-id", "filename": f"original.{kind}"}
                     if kind in {"photo", "video"} else None)
        media = ({"kind": kind, "filename": f"attachment.{kind}", "bytes": b"validated"}
                 if reference else None)
        owner.download_telegram_broadcast_media.return_value = media
        state, callback = _state(), _callback()
        audience = copy.deepcopy(owner.publish_bot_broadcast.return_value["recipients"])
        await _seed_state(state, _saved_preview(broadcast_media_reference=reference,
                                               broadcast_has_attachment=kind != 'text'))
        await owner.broadcast_confirm_callback(callback, state)
        owner.publish_bot_broadcast.assert_called_once_with(
            SHOP_ID, ACTOR, "Original caption", media, quota_exempt=False,
        )
        owner.download_telegram_broadcast_media.assert_awaited_once_with(callback.bot, reference)
        if kind == 'text':
            callback.bot.send_message.assert_awaited_once_with(
                chat_id=201,text='☕️ Offline shop\n\nOriginal caption',parse_mode=None)
            callback.bot.copy_message.assert_not_awaited()
        else:
            callback.bot.copy_message.assert_awaited_once_with(
                chat_id=201,from_chat_id=888,message_id=55,
                caption='☕️ Offline shop\n\nOriginal caption',parse_mode=None)
            callback.bot.send_message.assert_not_awaited()
        owner.record_broadcast_delivery.assert_called_once_with(SHOP_ID, BROADCAST_ID, [11])
        assert owner.publish_bot_broadcast.return_value["recipients"] == audience
        assert await state.get_data() == {} and await state.get_state() is None
        called_functions = [call.args[0] for call in owner.run_in_threadpool.await_args_list]
        assert owner.publish_bot_broadcast in called_functions
        assert owner.record_broadcast_delivery in called_functions
        owner.get_broadcast_recipients.assert_not_called()
        owner.save_broadcast.assert_not_called()
        owner.log_broadcast_touches.assert_not_called()

    asyncio.run(scenario())


def test_confirmation_prefers_app_push_and_keeps_telegram_as_fallback(owner):
    async def scenario():
        owner.send_broadcast_app_push.side_effect = [True, None]
        owner.publish_bot_broadcast.return_value["recipients"] = [
            {"user_id": 11, "telegram_user_id": 201},
            {"user_id": 12, "telegram_user_id": None},
        ]
        state, callback = _state(), _callback()
        await _seed_state(state, _saved_preview())

        await owner.broadcast_confirm_callback(callback, state)

        owner.send_broadcast_app_push.assert_awaited()
        assert callback.bot.copy_message.await_count == 0
        owner.record_broadcast_delivery.assert_called_once_with(SHOP_ID, BROADCAST_ID, [11])

    asyncio.run(scenario())


@pytest.mark.parametrize("shop_id,exempt", [(1, True), (73, False)])
def test_confirmation_passes_exact_shop_one_quota_exception(owner, shop_id, exempt):
    async def scenario():
        owner.get_admin_shop_and_role.return_value["id"] = shop_id
        owner.get_shop.return_value["id"] = shop_id
        owner.publish_bot_broadcast.return_value["shop_id"] = shop_id
        state = _state()
        await _seed_state(state, _saved_preview(broadcast_shop_id=shop_id))
        await owner.broadcast_confirm_callback(_callback(), state)
        owner.publish_bot_broadcast.assert_called_once_with(
            shop_id, ACTOR, "Original caption", None, quota_exempt=exempt,
        )

    asyncio.run(scenario())


@pytest.mark.parametrize("action", ["confirm", "cancel"])
def test_stale_callback_does_not_clear_a_newer_preview(owner, action):
    async def scenario():
        state, callback = _state(), _callback(action, token="2" * 32)
        saved = _saved_preview()
        await _seed_state(state, saved)
        await getattr(owner, f"broadcast_{action}_callback")(callback, state)
        assert await state.get_data() == saved
        assert await state.get_state() == WAITING_STATE.state
        owner.publish_bot_broadcast.assert_not_called()
        owner.download_telegram_broadcast_media.assert_not_awaited()
        callback.bot.copy_message.assert_not_awaited()

    asyncio.run(scenario())


def test_matching_cancel_clears_preview_without_publication(owner):
    async def scenario():
        state, callback = _state(), _callback("cancel")
        await _seed_state(state, _saved_preview())
        await owner.broadcast_cancel_callback(callback, state)
        assert await state.get_data() == {}
        owner.publish_bot_broadcast.assert_not_called()
        owner.download_telegram_broadcast_media.assert_not_awaited()
        callback.bot.copy_message.assert_not_awaited()

    asyncio.run(scenario())


def test_photo_without_caption_publishes_empty_text_and_copies_original(owner):
    async def scenario():
        state, message = _state(), _message("photo", caption=None)
        await _seed_state(state, {})
        await owner.broadcast_preview_handler(message, state)
        data = await state.get_data()
        assert data["broadcast_text"] == ""
        media = {"kind": "photo", "filename": "photo.jpg", "bytes": b"validated"}
        owner.download_telegram_broadcast_media.return_value = media
        owner.publish_bot_broadcast.return_value['text'] = ''
        callback = _callback(token=data["broadcast_preview_token"])
        await owner.broadcast_confirm_callback(callback, state)
        owner.publish_bot_broadcast.assert_called_once_with(
            SHOP_ID, ACTOR, "", media, quota_exempt=False,
        )
        callback.bot.copy_message.assert_awaited_once_with(
            chat_id=201, from_chat_id=777, message_id=44,caption='☕️ Offline shop',parse_mode=None,
        )

    asyncio.run(scenario())


def test_repeated_confirmation_publishes_and_delivers_once(owner):
    async def scenario():
        state = _state()
        await _seed_state(state, _saved_preview())
        first, second = _callback(), _callback()
        await owner.broadcast_confirm_callback(first, state)
        await owner.broadcast_confirm_callback(second, state)
        owner.publish_bot_broadcast.assert_called_once()
        owner.record_broadcast_delivery.assert_called_once()
        first.bot.send_message.assert_awaited_once()
        second.bot.send_message.assert_not_awaited()
        first.bot.copy_message.assert_not_awaited()

    asyncio.run(scenario())


def test_concurrent_confirmation_claims_shared_fsm_before_download_or_delivery(owner):
    async def scenario():
        storage = _YieldingMemoryStorage()
        first_state, second_state = _state(storage=storage), _state(storage=storage)
        first, second = _callback(), _callback()
        entered, release = asyncio.Event(), asyncio.Event()
        await _seed_state(first_state, _saved_preview())

        async def blocked_download(*_):
            assert await first_state.get_data() == {}
            entered.set()
            await release.wait()
            return None

        owner.download_telegram_broadcast_media.side_effect = blocked_download
        task = asyncio.create_task(owner.broadcast_confirm_callback(first, first_state))
        second_task = asyncio.create_task(owner.broadcast_confirm_callback(second, second_state))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            await asyncio.wait_for(second_task, 5)
            owner.publish_bot_broadcast.assert_not_called()
            first.bot.copy_message.assert_not_awaited()
            second.bot.copy_message.assert_not_awaited()
        finally:
            release.set()
            await asyncio.wait_for(task, 5)
            if not second_task.done():
                await asyncio.wait_for(second_task, 5)
        owner.download_telegram_broadcast_media.assert_awaited_once()
        owner.publish_bot_broadcast.assert_called_once()
        owner.record_broadcast_delivery.assert_called_once()
        first.bot.send_message.assert_awaited_once()
        second.bot.send_message.assert_not_awaited()
        first.bot.copy_message.assert_not_awaited()

    asyncio.run(scenario())


@pytest.mark.parametrize("missing", ["broadcast_from_chat_id", "broadcast_message_id"])
def test_confirmation_without_original_identifiers_cannot_publish_or_send(owner, missing):
    async def scenario():
        data = _saved_preview()
        data.pop(missing)
        state, callback = _state(), _callback()
        await _seed_state(state, data)
        await owner.broadcast_confirm_callback(callback, state)
        owner.publish_bot_broadcast.assert_not_called()
        owner.download_telegram_broadcast_media.assert_not_awaited()
        callback.bot.copy_message.assert_not_awaited()

    asyncio.run(scenario())


@pytest.mark.parametrize("rejection", ["not-owner", "no-membership", "different-shop", "closed-shop", "missing-shop"])
def test_context_or_role_rejection_prevents_publication_and_delivery(owner, rejection):
    async def scenario():
        if rejection == "not-owner":
            owner.is_owner.return_value = False
        elif rejection == "no-membership":
            owner.get_admin_shop_and_role.return_value = None
        elif rejection == "different-shop":
            owner.get_admin_shop_and_role.return_value["id"] = 74
        elif rejection == "closed-shop":
            owner.get_shop.return_value["is_active"] = False
        else:
            owner.get_shop.return_value = None
        state, callback = _state(), _callback()
        await _seed_state(state, _saved_preview())
        await owner.broadcast_confirm_callback(callback, state)
        owner.publish_bot_broadcast.assert_not_called()
        callback.bot.copy_message.assert_not_awaited()
        owner.record_broadcast_delivery.assert_not_called()
        if rejection == "not-owner":
            owner.get_admin_shop_and_role.assert_not_called()

    asyncio.run(scenario())


@pytest.mark.parametrize("status", [403, 409, 429, 503])
def test_publication_error_blocks_all_telegram_delivery(owner, status):
    async def scenario():
        owner.publish_bot_broadcast.side_effect = HTTPException(status, {"code": "OFFLINE_REJECTION"})
        state, callback = _state(), _callback()
        await _seed_state(state, _saved_preview())
        await owner.broadcast_confirm_callback(callback, state)
        owner.publish_bot_broadcast.assert_called_once()
        callback.bot.copy_message.assert_not_awaited()
        owner.record_broadcast_delivery.assert_not_called()
        assert await state.get_data() == {}

    asyncio.run(scenario())


def test_download_failure_publishes_without_media_and_still_copies_original(owner, caplog, capsys):
    async def scenario():
        reference = {"kind": "photo", "file_id": "private-file-id", "filename": "photo.jpg"}
        owner.download_telegram_broadcast_media.side_effect = RuntimeError("private-path-and-token")
        state, callback = _state(), _callback()
        await _seed_state(state, _saved_preview(broadcast_media_reference=reference))
        with caplog.at_level(logging.DEBUG):
            await owner.broadcast_confirm_callback(callback, state)
        owner.publish_bot_broadcast.assert_called_once_with(
            SHOP_ID, ACTOR, "Original caption", None, quota_exempt=False,
        )
        callback.bot.copy_message.assert_awaited_once_with(
            chat_id=201, from_chat_id=888, message_id=55,
            caption='☕️ Offline shop\n\nOriginal caption',parse_mode=None,
        )
        output = caplog.text + capsys.readouterr().out
        assert "private-path-and-token" not in output
        assert "private-file-id" not in output
        assert "RuntimeError" in output

    asyncio.run(scenario())


def test_delivery_records_only_successful_user_ids_from_published_audience(owner):
    async def scenario():
        owner.publish_bot_broadcast.return_value["recipients"] = [
            {"user_id": 11, "telegram_user_id": 201},
            {"user_id": 12, "telegram_user_id": None},
            {"user_id": 13, "telegram_user_id": 203},
        ]
        callback, state = _callback(), _state()
        callback.bot.send_message.side_effect = [None, RuntimeError("offline Telegram failure")]
        await _seed_state(state, _saved_preview())
        await owner.broadcast_confirm_callback(callback, state)
        assert [call.kwargs["chat_id"] for call in callback.bot.send_message.await_args_list] == [201, 203]
        owner.record_broadcast_delivery.assert_called_once_with(SHOP_ID, BROADCAST_ID, [11])

    asyncio.run(scenario())


def test_photo_sender_name_and_long_unicode_caption_preserve_all_owner_text(owner):
    async def scenario():
        text = '🥐' * 800
        reference = {'kind':'photo','file_id':'local-test-photo','filename':'coffee.jpg'}
        owner.publish_bot_broadcast.return_value.update(text=text,shop_name='Інша кав’ярня')
        state,callback = _state(),_callback()
        await _seed_state(state,_saved_preview(broadcast_text=text,broadcast_media_reference=reference))
        await owner.broadcast_confirm_callback(callback,state)
        photo = callback.bot.copy_message.await_args.kwargs
        assert photo['parse_mode'] is None
        assert len(photo['caption'].encode('utf-16-le')) // 2 <= 1024
        sent = photo['caption'] + ''.join(call.kwargs['text'] for call in callback.bot.send_message.await_args_list)
        assert sent == '☕️ Інша кав’ярня\n\n' + text
        assert owner.publish_bot_broadcast.call_args.args[2] == text
    asyncio.run(scenario())


def test_native_only_audience_is_published_without_a_telegram_send(owner):
    async def scenario():
        owner.publish_bot_broadcast.return_value["recipients"] = [
            {"user_id": 12, "telegram_user_id": None},
        ]
        callback, state = _callback(), _state()
        await _seed_state(state, _saved_preview())
        await owner.broadcast_confirm_callback(callback, state)
        owner.publish_bot_broadcast.assert_called_once()
        callback.bot.copy_message.assert_not_awaited()
        assert owner.publish_bot_broadcast.return_value["recipients"] == [
            {"user_id": 12, "telegram_user_id": None},
        ]

    asyncio.run(scenario())


def test_finishing_an_old_delivery_keeps_a_newly_created_preview(owner):
    async def scenario():
        state, callback = _state(), _callback()
        entered, release = asyncio.Event(), asyncio.Event()
        await _seed_state(state, _saved_preview())

        async def blocked_download(*_):
            entered.set()
            await release.wait()
            return None

        owner.download_telegram_broadcast_media.side_effect = blocked_download
        task = asyncio.create_task(owner.broadcast_confirm_callback(callback, state))
        newer = _saved_preview(broadcast_preview_token="3" * 32, broadcast_message_id=99)
        try:
            await asyncio.wait_for(entered.wait(), 1)
            await _seed_state(state, newer)
        finally:
            release.set()
            await asyncio.wait_for(task, 1)
        assert await state.get_data() == newer
        assert await state.get_state() == WAITING_STATE.state
        owner.publish_bot_broadcast.assert_called_once()
        callback.bot.send_message.assert_awaited_once_with(
            chat_id=201,text='☕️ Offline shop\n\nOriginal caption',parse_mode=None)

    asyncio.run(scenario())


@pytest.mark.parametrize("handler", ["broadcast_preview_handler", "broadcast_cancel_handler"])
def test_stale_router_state_cannot_mutate_a_different_current_fsm_state(owner, handler):
    async def scenario():
        state, message = _state(), _message("photo")
        current_data = {"pending_admin_id": 42}
        await state.set_data(current_data)
        await state.set_state("OwnerStates:waiting_add_admin_id")
        # Aiogram may have selected a handler using raw_state before waiting for
        # its key lock. The handler must check the current state again inside it.
        await getattr(owner, handler)(message, state)
        assert await state.get_data() == current_data
        assert await state.get_state() == "OwnerStates:waiting_add_admin_id"
        message.bot.copy_message.assert_not_awaited()
        message.answer.assert_not_awaited()
        owner.publish_bot_broadcast.assert_not_called()

    asyncio.run(scenario())


@pytest.mark.parametrize("action", ["confirm", "cancel"])
def test_legacy_untokened_callbacks_cannot_consume_a_preview(owner, action):
    async def scenario():
        state, callback = _state(), _callback(action)
        callback.data = f"broadcast_{action}"
        saved = _saved_preview()
        await _seed_state(state, saved)
        await getattr(owner, f"broadcast_{action}_callback")(callback, state)
        assert await state.get_data() == saved
        owner.publish_bot_broadcast.assert_not_called()
        callback.bot.copy_message.assert_not_awaited()

    asyncio.run(scenario())


@pytest.fixture
def downloader():
    # broadcast_media has no config/database imports; use its actual validator.
    from app.broadcast_media import PHOTO_LIMIT, VIDEO_LIMIT, validate_media

    path = PROJECT / "app/telegram_broadcast_media.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    definitions = [node for node in tree.body
                   if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))]
    namespace = {
        "__name__": "offline_telegram_broadcast_media",
        "BytesIO": BytesIO, "HTTPException": HTTPException,
        "PHOTO_LIMIT": PHOTO_LIMIT, "VIDEO_LIMIT": VIDEO_LIMIT,
        "validate_media": validate_media,
    }
    module = ast.Module(body=[
        ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0),
        *definitions,
    ], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), "exec"), namespace)
    return SimpleNamespace(namespace=namespace, **namespace)


def _photo_bytes(image_format):
    output = BytesIO()
    Image.new("RGB", (8, 8), "brown").save(output, format=image_format)
    return output.getvalue()


def _video_bytes(brand):
    def box(kind, payload):
        return (8 + len(payload)).to_bytes(4, "big") + kind + payload

    return (box(b"ftyp", brand + b"\0" * 4 + brand)
            + box(b"moov", b"") + box(b"mdat", b"offline"))


@pytest.mark.parametrize("kind,data,mime,extension", [
    ("photo", _photo_bytes("JPEG"), "image/jpeg", ".jpg"),
    ("photo", _photo_bytes("PNG"), "image/png", ".png"),
    ("video", _video_bytes(b"isom"), "video/mp4", ".mp4"),
    ("video", _video_bytes(b"qt  "), "video/quicktime", ".mov"),
], ids=["jpeg", "png", "mp4", "mov"])
def test_download_uses_memory_buffer_and_returns_only_validated_persistable_media(
    downloader, kind, data, mime, extension,
):
    async def scenario():
        buffers = []

        async def download(file_path, *, destination, chunk_size, seek):
            assert file_path == "telegram/private/path"
            assert isinstance(destination, BytesIO)
            assert chunk_size == 65536 and seek is True
            buffers.append(destination)
            for start in range(0, len(data), 17):
                destination.write(data[start:start + 17])
            destination.seek(0)

        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=SimpleNamespace(
                file_size=len(data), file_path="telegram/private/path",
            )),
            download_file=AsyncMock(side_effect=download),
        )
        result = await downloader.download_telegram_broadcast_media(bot, {
            "kind": kind, "file_id": "telegram-file-id", "filename": "../../original.wrong",
        })
        assert result == {
            "kind": kind, "filename": "original" + extension,
            "mime_type": mime, "size_bytes": len(data), "bytes": data,
        }
        bot.get_file.assert_awaited_once_with("telegram-file-id")
        bot.download_file.assert_awaited_once()
        assert buffers[0].closed

    asyncio.run(scenario())


@pytest.mark.parametrize("reference", [
    None,
    {"kind": "animation", "file_id": "gif-id"},
    {"kind": "document", "file_id": "document-id"},
])
def test_unsupported_or_absent_reference_skips_all_download_work(downloader, reference):
    bot = SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock())
    assert asyncio.run(downloader.download_telegram_broadcast_media(bot, reference)) is None
    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()


@pytest.mark.parametrize("file_id", [None, "", 12])
def test_supported_reference_requires_a_nonempty_file_id(downloader, file_id):
    bot = SimpleNamespace(get_file=AsyncMock(), download_file=AsyncMock())
    with pytest.raises(HTTPException) as error:
        asyncio.run(downloader.download_telegram_broadcast_media(bot, {
            "kind": "photo", "file_id": file_id, "filename": "photo.jpg",
        }))
    assert error.value.status_code == 422
    bot.get_file.assert_not_awaited()
    bot.download_file.assert_not_awaited()


@pytest.mark.parametrize("kind,limit", [("photo", 8 * 1024 * 1024), ("video", 20 * 1024 * 1024)])
def test_remote_size_metadata_rejects_oversize_before_downloading(downloader, kind, limit):
    bot = SimpleNamespace(
        get_file=AsyncMock(return_value=SimpleNamespace(file_size=limit + 1, file_path="remote/path")),
        download_file=AsyncMock(),
    )
    with pytest.raises(HTTPException) as error:
        asyncio.run(downloader.download_telegram_broadcast_media(bot, {
            "kind": kind, "file_id": "file-id", "filename": "attachment",
        }))
    assert error.value.status_code == 413
    bot.get_file.assert_awaited_once_with("file-id")
    bot.download_file.assert_not_awaited()


def test_missing_remote_path_cannot_trigger_download(downloader):
    bot = SimpleNamespace(
        get_file=AsyncMock(return_value=SimpleNamespace(file_size=10, file_path=None)),
        download_file=AsyncMock(),
    )
    with pytest.raises(HTTPException) as error:
        asyncio.run(downloader.download_telegram_broadcast_media(bot, {
            "kind": "photo", "file_id": "file-id", "filename": "photo.jpg",
        }))
    assert error.value.status_code == 422
    bot.download_file.assert_not_awaited()


@pytest.mark.parametrize("kind,metadata_size", [("photo", None), ("photo", 1), ("video", None), ("video", 1)])
def test_stream_limit_cannot_be_bypassed_by_missing_or_underreported_metadata(
    downloader, kind, metadata_size,
):
    async def scenario():
        buffers = []
        downloader.namespace["PHOTO_LIMIT"] = 4
        downloader.namespace["VIDEO_LIMIT"] = 4
        validator = Mock(side_effect=AssertionError("Oversize stream must never reach validation"))
        downloader.namespace["validate_media"] = validator

        async def download(_path, *, destination, **_):
            buffers.append(destination)
            destination.write(b"1234")
            destination.write(b"5")

        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=SimpleNamespace(file_size=metadata_size, file_path="remote/path")),
            download_file=AsyncMock(side_effect=download),
        )
        with pytest.raises(HTTPException) as error:
            await downloader.download_telegram_broadcast_media(bot, {
                "kind": kind, "file_id": "file-id", "filename": "attachment",
            })
        assert error.value.status_code == 413
        validator.assert_not_called()
        assert buffers[0].closed

    asyncio.run(scenario())


def test_bounded_buffer_allows_exact_limit_and_rejects_growth_without_writing(downloader):
    buffer = downloader._BoundedMediaBuffer(4)
    try:
        assert buffer.write(b"12") == 2
        assert buffer.write(b"34") == 2
        with pytest.raises(HTTPException) as error:
            buffer.write(b"5")
        assert error.value.status_code == 413
        assert buffer.getvalue() == b"1234" and buffer.tell() == 4
        buffer.seek(0)
        assert buffer.write(b"ab") == 2
        assert buffer.getvalue() == b"ab34"
    finally:
        buffer.close()


@pytest.mark.parametrize("stage", ["download", "validation"])
def test_download_buffer_is_closed_even_when_download_or_validation_fails(downloader, stage):
    async def scenario():
        buffers = []

        async def download(_path, *, destination, **_):
            buffers.append(destination)
            destination.write(b"invalid-media")
            if stage == "download":
                raise OSError("mocked download failure")

        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=SimpleNamespace(file_size=13, file_path="remote/path")),
            download_file=AsyncMock(side_effect=download),
        )
        expected_error = OSError if stage == "download" else HTTPException
        with pytest.raises(expected_error):
            await downloader.download_telegram_broadcast_media(bot, {
                "kind": "photo", "file_id": "file-id", "filename": "photo.jpg",
            })
        assert buffers[0].closed

    asyncio.run(scenario())


@pytest.mark.parametrize("kind,data", [
    ("photo", b"GIF89a"), ("photo", b"\xff\xd8\xffinvalid"),
    ("video", b"not-an-mp4"), ("video", _video_bytes(b"xxxx")),
])
def test_telegram_attachment_bytes_must_pass_shared_media_validation(downloader, kind, data):
    async def scenario():
        async def download(_path, *, destination, **_):
            destination.write(data)

        bot = SimpleNamespace(
            get_file=AsyncMock(return_value=SimpleNamespace(file_size=len(data), file_path="remote/path")),
            download_file=AsyncMock(side_effect=download),
        )
        with pytest.raises(HTTPException) as error:
            await downloader.download_telegram_broadcast_media(bot, {
                "kind": kind, "file_id": "file-id", "filename": "attachment",
            })
        assert error.value.status_code in {415, 422}

    asyncio.run(scenario())
