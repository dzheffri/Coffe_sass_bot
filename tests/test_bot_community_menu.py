"""Offline menu and real Aiogram routing tests; no DB or network startup."""

import ast
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, ReplyKeyboardMarkup
import pytest

from app.keyboards import (
    COMMUNITY_CHANNEL_URL,
    COMMUNITY_BUTTON_TEXT,
    admin_main_keyboard,
    community_channel_keyboard,
    user_main_keyboard,
)


def _send_panel(**overrides):
    """Load the actual panel function without importing bot/DB configuration."""
    path = Path(__file__).resolve().parents[1] / "app/handlers/common.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    node = next(node for node in tree.body
                if isinstance(node, ast.AsyncFunctionDef)
                and node.name == "send_correct_panel")
    namespace = {
        "types": types,
        "is_any_shop_admin": Mock(return_value=False),
        "is_owner": Mock(return_value=False),
        "SUPER_ADMIN_IDS": set(),
        "user_main_keyboard": user_main_keyboard,
        "admin_main_keyboard": admin_main_keyboard,
        "community_channel_keyboard": community_channel_keyboard,
        "get_panel_mode": Mock(return_value=None),
        "SCANNER_URL": "https://example.test/scanner",
        "get_personal_admin_panel_url": Mock(return_value="https://example.test/admin"),
        "get_admin_shops_for_telegram": Mock(return_value=[{"id": 1}, {"id": 2}]),
    }
    namespace.update(overrides)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["send_correct_panel"], namespace


def test_community_button_opens_public_channel_directly():
    keyboard = community_channel_keyboard()
    assert isinstance(keyboard, InlineKeyboardMarkup)
    assert len(keyboard.inline_keyboard) == 1
    assert len(keyboard.inline_keyboard[0]) == 1
    button = keyboard.inline_keyboard[0][0]
    assert button.text == "☕️ Кавова спільнота" == COMMUNITY_BUTTON_TEXT
    assert button.url == "https://t.me/nashi_coffee_community" == COMMUNITY_CHANNEL_URL
    assert button.callback_data is None
    assert button.web_app is None
    assert button.login_url is None


def test_existing_loyalty_menu_rows_and_order_are_preserved():
    keyboard = user_main_keyboard()
    assert isinstance(keyboard, ReplyKeyboardMarkup)
    assert keyboard.resize_keyboard is True
    assert keyboard.is_persistent is True
    assert [[button.text for button in row] for row in keyboard.keyboard] == [
        ["📱 Мій QR-код"],
        ["☕ Мої чашки", "🎁 Мої безкоштовні кави"],
        ["🏪 Мої кав’ярні"],
        [COMMUNITY_BUTTON_TEXT],
        ["🛟 Служба підтримки"],
    ]


def test_regular_user_panel_exposes_persistent_button_without_automatic_extra_message():
    send_panel, namespace = _send_panel()
    message = SimpleNamespace(from_user=SimpleNamespace(id=1001), answer=AsyncMock())
    asyncio.run(send_panel(message))

    message.answer.assert_awaited_once()
    menu = message.answer.await_args
    assert "покажи його баристі" in menu.args[0]
    assert isinstance(menu.kwargs["reply_markup"], ReplyKeyboardMarkup)
    assert menu.kwargs["reply_markup"].is_persistent is True
    assert [COMMUNITY_BUTTON_TEXT] in [[button.text for button in row]
                                     for row in menu.kwargs["reply_markup"].keyboard]
    namespace["get_panel_mode"].assert_not_called()


def test_admin_panel_does_not_change_or_send_user_announcement():
    send_panel, _ = _send_panel(is_any_shop_admin=Mock(return_value=True))
    message = SimpleNamespace(from_user=SimpleNamespace(id=1001), answer=AsyncMock())
    asyncio.run(send_panel(message))
    message.answer.assert_awaited_once()
    assert message.answer.await_args.args[0] == "✅ Адмін-панель кав’ярні активна."
    keyboard = message.answer.await_args.kwargs["reply_markup"]
    assert [[button.text for button in row] for row in keyboard.keyboard] == [
        ["☕ Режим: нарахування", "✅ Режим: списання"],
        ["📷 Відкрити сканер"],
        [COMMUNITY_BUTTON_TEXT],
    ]


@pytest.mark.parametrize("role,kwargs,existing_rows", [
    ("admin", {}, [
        ["☕ Режим: нарахування", "✅ Режим: списання"], ["📷 Відкрити сканер"],
    ]),
    ("owner", {"is_owner": True, "can_select_shop": True,
               "can_switch_to_super_admin": True, "admin_panel_url": "https://example.test/admin"}, [
        ["☕ Режим: нарахування", "✅ Режим: списання"], ["📷 Відкрити сканер"],
        ["🏪 Обрати кав’ярню"], ["📊 Статистика кав’ярні"],
        ["➕ Додати адміністратора", "➖ Видалити адміністратора"],
        ["👤 Список адміністраторів", "📣 Зробити розсилку"], ["💳 Підписка"],
        ["🌐 Відкрити адмін-панель"], ["🛠 Режим super admin"],
    ]),
    ("superadmin", {"is_super_admin": True, "can_switch_to_owner": True}, [
        ["☕ Режим: нарахування", "✅ Режим: списання"], ["📷 Відкрити сканер"],
        ["📊 Загальна статистика"], ["🏪 Список кав’ярень"],
        ["➕ Додати кав’ярню", "🗑 Видалити кав’ярню"], ["💾 Backup системи"],
        ["📢 Розсилка всім користувачам"], ["💳 Продовжити підписку"], ["👑 Режим owner"],
    ]),
])
def test_staff_menus_append_community_without_reordering_existing_buttons(role, kwargs, existing_rows):
    keyboard = admin_main_keyboard(scanner_url="https://example.test/scanner", **kwargs)
    rows = [[button.text for button in row] for row in keyboard.keyboard]
    assert rows == [*existing_rows, [COMMUNITY_BUTTON_TEXT]], role
    assert keyboard.is_persistent is True
    assert keyboard.resize_keyboard is True
    assert keyboard.keyboard[1][0].web_app.url == "https://example.test/scanner"
    community = keyboard.keyboard[-1][0]
    assert community.web_app is None and community.request_contact is None
    if kwargs.get("admin_panel_url"):
        web = next(button for row in keyboard.keyboard for button in row
                   if button.text == "🌐 Відкрити адмін-панель")
        assert web.web_app.url == kwargs["admin_panel_url"]


@pytest.mark.parametrize("admin,owner,superadmin,mode", [
    (False, False, False, None), (True, False, False, None), (True, True, False, None),
    (False, False, True, None), (True, True, True, "owner"), (True, True, True, "super_admin"),
])
def test_every_panel_role_and_mode_contains_one_permanent_community_button(admin, owner, superadmin, mode):
    send_panel, _ = _send_panel(is_any_shop_admin=Mock(return_value=admin),
                              is_owner=Mock(return_value=owner),
                              SUPER_ADMIN_IDS={1001} if superadmin else set(),
                              get_panel_mode=Mock(return_value=mode))
    message = SimpleNamespace(from_user=SimpleNamespace(id=1001), answer=AsyncMock())
    asyncio.run(send_panel(message))
    message.answer.assert_awaited_once()
    keyboard = message.answer.await_args.kwargs["reply_markup"]
    assert isinstance(keyboard, ReplyKeyboardMarkup) and keyboard.is_persistent is True
    assert sum(button.text == COMMUNITY_BUTTON_TEXT for row in keyboard.keyboard for button in row) == 1


def _registered_community_router():
    """Compile the actual decorator and handler without importing DB/config."""
    path = Path(__file__).resolve().parents[1] / "app/handlers/common.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    node = next(node for node in tree.body
                if isinstance(node, ast.AsyncFunctionDef) and node.name == "community_open")
    router = Router()
    namespace = {"router": router, "types": types, "F": F,
                 "COMMUNITY_BUTTON_TEXT": COMMUNITY_BUTTON_TEXT,
                 "community_channel_keyboard": community_channel_keyboard}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return router


@pytest.mark.parametrize("active_state", [
    None, "OwnerStates:waiting_broadcast_text", "AdminScanStates:waiting_cups_count",
    "SupportStates:waiting_for_message", "SupportStates:waiting_for_admin_reply",
    "SuperAdminStates:waiting_global_broadcast_text",
])
def test_registered_community_handler_handles_tap_before_fsm_catchalls_without_mutating_state(active_state):
    async def run():
        common = _registered_community_router()
        dispatcher = Dispatcher(storage=MemoryStorage())
        dispatcher.include_router(common)
        later_router = Router()
        later_handler = AsyncMock()
        async def stateful_fallback(message):
            await later_handler(message)
        later_router.message.register(stateful_fallback)
        dispatcher.include_router(later_router)
        # Mock the transport itself: any real request would fail this test.
        transport = AsyncMock()
        bot = Bot(token="123456:" + "A" * 35, session=transport)
        state = dispatcher.fsm.get_context(bot=bot, chat_id=42, user_id=42)
        data = {"broadcast_preview_token": "unchanged", "shop_id": 7,
                "pending_scan": {"client_user_id": 11, "cups": 2}}
        await state.set_state(active_state)
        await state.set_data(data)
        message = types.Message(message_id=1, date=datetime.now(timezone.utc),
                                chat=types.Chat(id=42, type="private"),
                                from_user=types.User(id=42, is_bot=False, first_name="Test"),
                                text=COMMUNITY_BUTTON_TEXT)
        await dispatcher.feed_update(bot, types.Update(update_id=1, message=message))
        later_handler.assert_not_awaited()
        transport.assert_awaited_once()
        sent = transport.await_args.args[1]
        assert sent.chat_id == 42
        assert sent.reply_markup == community_channel_keyboard()
        assert sent.disable_notification is True
        assert sent.reply_markup.inline_keyboard[0][0].url == COMMUNITY_CHANNEL_URL
        assert await state.get_state() == active_state
        assert await state.get_data() == data
        await dispatcher.fsm.close()
    asyncio.run(run())


def test_registered_community_handler_does_not_intercept_unrelated_menu_text():
    async def run():
        dispatcher = Dispatcher(storage=MemoryStorage())
        dispatcher.include_router(_registered_community_router())
        next_router = Router()
        handler = AsyncMock()
        async def unrelated_handler(message):
            await handler(message)
        next_router.message.register(unrelated_handler)
        dispatcher.include_router(next_router)
        transport = AsyncMock()
        bot = Bot(token="123456:" + "A" * 35, session=transport)
        message = types.Message(message_id=1, date=datetime.now(timezone.utc),
                                chat=types.Chat(id=42, type="private"),
                                from_user=types.User(id=42, is_bot=False, first_name="Test"),
                                text="☕ Мої чашки")
        await dispatcher.feed_update(bot, types.Update(update_id=1, message=message))
        handler.assert_awaited_once()
        transport.assert_not_awaited()
        await dispatcher.fsm.close()
    asyncio.run(run())


def test_common_router_precedes_stateful_menu_routers_in_bot_registration():
    path = Path(__file__).resolve().parents[1] / "bot.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    main = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "main")
    registered = [node.value.args[0].id for node in main.body
                  if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
                  and isinstance(node.value.func, ast.Attribute) and node.value.func.attr == "include_router"]
    assert registered[0] == "common_router"
    assert {"user_router", "owner_router", "admin_router", "super_admin_router", "support_router"} <= set(registered[1:])
