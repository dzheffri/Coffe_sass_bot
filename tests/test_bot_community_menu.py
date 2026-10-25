"""Offline regular-user menu tests; no bot, database or network startup."""

import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from aiogram import types
from aiogram.types import InlineKeyboardMarkup, ReplyKeyboardMarkup

from app.keyboards import (
    COMMUNITY_CHANNEL_URL,
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
    assert button.text == "☕️ Кавова спільнота"
    assert button.url == "https://t.me/nashi_coffee_community" == COMMUNITY_CHANNEL_URL
    assert button.callback_data is None
    assert button.web_app is None
    assert button.login_url is None


def test_existing_loyalty_menu_rows_and_order_are_preserved():
    keyboard = user_main_keyboard()
    assert isinstance(keyboard, ReplyKeyboardMarkup)
    assert keyboard.resize_keyboard is True
    assert [[button.text for button in row] for row in keyboard.keyboard] == [
        ["📱 Мій QR-код"],
        ["☕ Мої чашки", "🎁 Мої безкоштовні кави"],
        ["🏪 Мої кав’ярні"],
        ["🛟 Служба підтримки"],
    ]


def test_regular_user_panel_exposes_channel_url_without_intermediate_action():
    send_panel, namespace = _send_panel()
    message = SimpleNamespace(from_user=SimpleNamespace(id=1001), answer=AsyncMock())
    asyncio.run(send_panel(message))

    assert message.answer.await_count == 2
    menu, channel = message.answer.await_args_list
    assert "покажи його баристі" in menu.args[0]
    assert isinstance(menu.kwargs["reply_markup"], ReplyKeyboardMarkup)
    assert channel.kwargs["reply_markup"] == community_channel_keyboard()
    assert channel.kwargs["disable_notification"] is True
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
    ]
