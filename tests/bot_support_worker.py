"""Run real bot handlers offline in a fresh process; no DB or Telegram access."""

import importlib
import os
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch


class BotSupportTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        environment = {
            "PYTHON_DOTENV_DISABLED": "1",
            "BOT_TOKEN": "9001:offline-test-token",
            "DATABASE_URL": "postgresql://unused.invalid/offline",
            "SUPER_ADMIN_IDS": "9001",
            "SCANNER_URL": "https://example.invalid/scanner",
            "ADMIN_PANEL_URL": "https://example.invalid/admin",
        }
        # Actual imports include init_db; every connection is inert, not PostgreSQL.
        with patch.dict(os.environ, environment, clear=True), patch(
            "psycopg.connect", return_value=MagicMock()
        ):
            cls.common = importlib.import_module("app.handlers.common")
            cls.support = importlib.import_module("app.handlers.support")
            cls.database = importlib.import_module("app.db")

    def setUp(self):
        self.addCleanup(patch.stopall)
        patch.object(self.database, "connect", side_effect=AssertionError(
            "No database access is allowed in support tests"
        )).start()
        self.ensure_user = patch.object(self.common, "ensure_user").start()
        self.assign_owner = patch.object(self.common, "assign_pending_owner_if_exists").start()
        self.panel = patch.object(self.common, "send_correct_panel", new=AsyncMock()).start()
        self.bind = patch.object(self.common, "bind_telegram_link_session", return_value={"ok": True}).start()

    def message(self, actor=9002, text="Потрібна допомога"):
        return SimpleNamespace(
            from_user=SimpleNamespace(id=actor, username="offline", full_name="Ольга"),
            chat=SimpleNamespace(id=actor, type="private"), text=text,
            answer=AsyncMock(), bot=SimpleNamespace(send_message=AsyncMock()),
        )

    def state(self, data=None):
        return SimpleNamespace(
            set_state=AsyncMock(), clear=AsyncMock(), update_data=AsyncMock(),
            get_data=AsyncMock(return_value=data or {}),
        )

    async def test_deep_link_calls_existing_flow_with_identical_state_and_prompt(self):
        incoming, state = self.message(), self.state()
        existing = self.support.support_start
        with patch.object(self.common, "support_start", wraps=existing) as support_start:
            await self.common.start_handler(incoming, SimpleNamespace(args="barista_support"), state)
            support_start.assert_awaited_once_with(incoming, state)
        state.set_state.assert_awaited_once_with(self.support.SupportStates.waiting_for_message)
        ordinary, ordinary_state = self.message(), self.state()
        await existing(ordinary, ordinary_state)
        self.assertEqual(incoming.answer.await_args, ordinary.answer.await_args)
        self.ensure_user.assert_not_called()
        self.assign_owner.assert_not_called()
        self.panel.assert_not_awaited()
        self.bind.assert_not_called()

    async def test_real_start_router_and_fsm_accept_barista_support(self):
        from aiogram import Bot, Dispatcher, types
        from datetime import datetime, timezone

        bot = Bot("9001:offline-test-token")
        dispatcher = Dispatcher()
        dispatcher.include_router(self.common.router)
        incoming = types.Message(
            message_id=1, date=datetime.now(timezone.utc),
            chat=types.Chat(id=9002, type="private"),
            from_user=types.User(id=9002, first_name="Ольга", is_bot=False),
            text="/start barista_support",
        )
        reply = incoming.model_copy(update={"message_id": 2})
        with patch.object(Bot, "__call__", new=AsyncMock(return_value=reply)) as api:
            await dispatcher.feed_update(bot, types.Update(update_id=1, message=incoming))
            self.assertEqual(api.await_count, 1)
            self.assertTrue(api.await_args.args[0].text.startswith("🛟 Служба підтримки"))
        state = dispatcher.fsm.get_context(bot=bot, chat_id=9002, user_id=9002)
        self.assertEqual(await state.get_state(), self.support.SupportStates.waiting_for_message.state)
        self.ensure_user.assert_not_called()
        self.panel.assert_not_awaited()
        await bot.session.close()

    async def test_ordinary_and_unrecognized_start_keep_existing_user_panel_flow(self):
        for args in (None, "", "unknown", "barista_support_extra"):
            with self.subTest(args=args):
                incoming, state = self.message(), self.state()
                self.ensure_user.reset_mock()
                self.assign_owner.reset_mock()
                self.panel.reset_mock()
                await self.common.start_handler(incoming, SimpleNamespace(args=args), state)
                self.ensure_user.assert_called_once_with(
                    telegram_user_id=9002, username="offline", full_name="Ольга",
                )
                self.assign_owner.assert_called_once_with(9002)
                self.panel.assert_awaited_once_with(incoming)
                state.set_state.assert_not_awaited()

    async def test_verified_telegram_link_start_is_preserved(self):
        incoming, state = self.message(), self.state()
        await self.common.start_handler(incoming, SimpleNamespace(args="link_offline-token"), state)
        self.bind.assert_called_once_with("offline-token", 9002)
        self.ensure_user.assert_not_called()
        self.panel.assert_not_awaited()
        state.set_state.assert_not_awaited()
        self.assertEqual(
            incoming.answer.await_args.kwargs["reply_markup"].inline_keyboard[0][0].callback_data,
            "tg_link_confirm:offline-token",
        )

    async def test_existing_support_delivery_and_state_clear_are_preserved(self):
        incoming, state = self.message(), self.state()
        await self.support.support_message(incoming, state)
        sent = incoming.bot.send_message.await_args
        self.assertEqual(sent.args[0], self.support.SUPPORT_ADMIN_ID)
        self.assertIn("💬 Повідомлення:\nПотрібна допомога", sent.args[1])
        self.assertEqual(sent.kwargs["reply_markup"].inline_keyboard[0][0].callback_data, "support_reply:9002")
        self.assertTrue(incoming.answer.await_args.args[0].startswith("🛟 Дякуємо за звернення!"))
        state.clear.assert_awaited_once()

    async def test_non_support_admin_cannot_reply(self):
        incoming, state = self.message(actor=9002), self.state({"reply_user_id": 9003})
        callback = SimpleNamespace(
            from_user=incoming.from_user, data="support_reply:9003",
            answer=AsyncMock(), message=incoming,
        )
        await self.support.support_reply_start(callback, state)
        callback.answer.assert_awaited_once_with("Немає доступу", show_alert=True)
        state.set_state.assert_not_awaited()
        state.update_data.assert_not_awaited()
        await self.support.support_send_reply(incoming, state)
        incoming.bot.send_message.assert_not_awaited()

    async def test_support_admin_reply_and_cancel_remain_unchanged(self):
        incoming = self.message(actor=self.support.SUPPORT_ADMIN_ID, text="Допоможемо")
        state = self.state({"reply_user_id": 9002})
        await self.support.support_send_reply(incoming, state)
        incoming.bot.send_message.assert_awaited_once_with(
            chat_id=9002, text="🛟 Відповідь служби підтримки\n\nДопоможемо",
        )
        state.clear.assert_awaited_once()
        incoming = self.message(actor=self.support.SUPPORT_ADMIN_ID, text="скасувати")
        state = self.state({"reply_user_id": 9002})
        await self.support.support_send_reply(incoming, state)
        incoming.bot.send_message.assert_not_awaited()
        state.clear.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
