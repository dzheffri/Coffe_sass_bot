"""Isolated offline worker through real Telegram handlers; no production access."""

import importlib
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch


class BroadcastMediaTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        # app.db invokes init_db at import. Its connection is inert here;
        # even startup SQL reaches only a MagicMock, never PostgreSQL.
        environment = {
            "PYTHON_DOTENV_DISABLED": "1",
            "BOT_TOKEN": "9001:offline-test-token",
            "DATABASE_URL": "postgresql://unused.invalid/offline",
            "SUPER_ADMIN_IDS": "9001",
            "SCANNER_URL": "https://example.invalid/scanner",
            "ADMIN_PANEL_URL": "https://example.invalid/admin",
        }
        with patch.dict(os.environ, environment, clear=True), patch(
            "psycopg.connect", return_value=MagicMock()
        ):
            cls.owner = importlib.import_module("app.handlers.owner")
            cls.super_admin = importlib.import_module("app.handlers.super_admin")
            cls.database = importlib.import_module("app.db")

    def setUp(self):
        self.addCleanup(patch.stopall)
        patch.object(
            self.database, "connect",
            side_effect=AssertionError("No database connection is allowed"),
        ).start()
        patch.object(self.owner, "owner_main_keyboard_for_user", return_value=None).start()
        self.owner_allowed = patch.object(self.owner, "is_owner", return_value=True).start()
        self.shop = patch.object(
            self.owner, "get_admin_shop_and_role",
            return_value={"id": 73, "name": "Offline shop", "role": "owner"},
        ).start()
        self.recipients = patch.object(
            self.owner, "get_broadcast_recipients",
            return_value=[{"telegram_user_id": 201, "user_id": 11}],
        ).start()
        self.save = patch.object(self.owner, "save_broadcast").start()
        self.touches = patch.object(self.owner, "log_broadcast_touches").start()
        self.all_users = patch.object(
            self.super_admin, "get_all_users", return_value=[]
        ).start()
        patch.object(self.super_admin.asyncio, "sleep", new=AsyncMock()).start()

    def message(self, kind="text", *, actor=9002, caption="Original caption"):
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=actor), chat=SimpleNamespace(id=777),
            message_id=44, text="Original text" if kind == "text" else None,
            caption=None if kind == "text" else caption,
            photo=None, video=None, animation=None, document=None,
            bot=SimpleNamespace(copy_message=AsyncMock(), send_message=AsyncMock()),
            answer=AsyncMock(), edit_reply_markup=AsyncMock(),
        )
        if kind == "photo":
            message.photo = [SimpleNamespace(file_id="photo-id")]
        elif kind in ("video", "animation", "document"):
            setattr(message, kind, SimpleNamespace(file_id=f"{kind}-id"))
        return message

    def state(self, data=None):
        return SimpleNamespace(
            clear=AsyncMock(), set_state=AsyncMock(), update_data=AsyncMock(),
            get_data=AsyncMock(return_value=data or {}),
        )

    def callback(self):
        return SimpleNamespace(
            from_user=SimpleNamespace(id=9002), answer=AsyncMock(),
            message=self.message(), bot=SimpleNamespace(copy_message=AsyncMock()),
        )

    async def test_ordinary_owner_cannot_start_or_send_global_broadcast(self):
        message, state = self.message(actor=9002), self.state()
        self.assertFalse(self.super_admin.is_super_admin(9002))
        await self.super_admin.global_broadcast_start(message, state)
        state.set_state.assert_not_awaited()
        await self.super_admin.global_broadcast_send(message, state)
        self.all_users.assert_not_called()
        message.bot.copy_message.assert_not_awaited()
        state.clear.assert_awaited_once()

    async def test_global_media_preserves_original_and_existing_recipient_dedup(self):
        self.all_users.return_value = [
            {"telegram_user_id": 201}, {"telegram_user_id": 201},
            {"telegram_user_id": None}, {"telegram_user_id": 202},
        ]
        self.assertTrue(self.super_admin.is_super_admin(9001))
        for kind in ("text", "photo", "video", "animation", "document"):
            with self.subTest(kind=kind):
                message, state = self.message(kind, actor=9001), self.state()
                await self.super_admin.global_broadcast_send(message, state)
                self.assertEqual(message.bot.copy_message.await_count, 2)
                self.assertEqual(
                    [call.kwargs for call in message.bot.copy_message.await_args_list],
                    [dict(chat_id=201, from_chat_id=777, message_id=44),
                     dict(chat_id=202, from_chat_id=777, message_id=44)],
                )
                message.bot.send_message.assert_not_awaited()
        self.recipients.assert_not_called()

    async def test_owner_preview_preserves_media_and_caption_without_overrides(self):
        for kind in ("text", "photo", "video", "animation", "document"):
            for caption in (None, "Original caption"):
                with self.subTest(kind=kind, caption=caption):
                    message, state = self.message(kind, caption=caption), self.state()
                    await self.owner.broadcast_preview_handler(message, state)
                    state.update_data.assert_awaited_once_with(
                        broadcast_text="Original text" if kind == "text" else caption or "",
                        broadcast_from_chat_id=777, broadcast_message_id=44,
                    )
                    copied = message.bot.copy_message.await_args.kwargs
                    self.assertEqual(copied["chat_id"], 777)
                    self.assertEqual(copied["from_chat_id"], 777)
                    self.assertEqual(copied["message_id"], 44)
                    self.assertIn("reply_markup", copied)
                    self.assertNotIn("text", copied)
                    self.assertNotIn("caption", copied)
        self.recipients.assert_not_called()
        self.all_users.assert_not_called()

    async def test_owner_confirmation_uses_only_server_shop_and_saved_original(self):
        callback = self.callback()
        state = self.state({
            "broadcast_text": "Original caption", "broadcast_from_chat_id": 888,
            "broadcast_message_id": 55,
        })
        await self.owner.broadcast_confirm_callback(callback, state)
        self.shop.assert_called_once_with(9002)
        self.recipients.assert_called_once_with(73)
        self.all_users.assert_not_called()
        callback.bot.copy_message.assert_awaited_once_with(
            chat_id=201, from_chat_id=888, message_id=55,
        )
        self.save.assert_called_once_with(73, 9002, "Original caption", 1)
        self.touches.assert_called_once_with(73, [11])
        state.clear.assert_awaited_once()

    async def test_owner_confirmation_without_original_ids_cannot_send(self):
        callback, state = self.callback(), self.state({"broadcast_text": "Old state"})
        await self.owner.broadcast_confirm_callback(callback, state)
        self.recipients.assert_not_called()
        self.all_users.assert_not_called()
        callback.bot.copy_message.assert_not_awaited()
        self.save.assert_not_called()
        self.touches.assert_not_called()

    async def test_owner_confirmation_rechecks_role_before_recipient_query(self):
        self.owner_allowed.return_value = False
        callback = self.callback()
        state = self.state({"broadcast_from_chat_id": 888, "broadcast_message_id": 55})
        await self.owner.broadcast_confirm_callback(callback, state)
        self.shop.assert_not_called()
        self.recipients.assert_not_called()
        self.all_users.assert_not_called()
        callback.bot.copy_message.assert_not_awaited()
        state.clear.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
