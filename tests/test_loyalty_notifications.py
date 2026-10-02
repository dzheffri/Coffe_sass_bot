"""Real shared-helper checks with inert delivery/DB modules, never production."""

import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace
import sys
import unittest
from unittest.mock import AsyncMock, MagicMock, patch


PROJECT = Path(__file__).resolve().parents[1]


def load_notifications():
    # Load the complete real module under a separate name. Do not collide with
    # session-scoped router fixtures, or run app.db/config import startup.
    database = ModuleType("app.db")
    for name in (
        "touch_wallet_pass", "get_wallet_push_tokens",
        "get_app_push_devices_for_user",
    ):
        setattr(database, name, MagicMock())
    config = ModuleType("app.config")
    config.BOT_TOKEN = "9001:offline-test-token"
    wallet = ModuleType("app.wallet_push")
    wallet.send_wallet_pushes = AsyncMock()
    push = ModuleType("app.app_push")
    push.send_app_pushes = AsyncMock()
    spec = importlib.util.spec_from_file_location(
        "offline_loyalty_notifications", PROJECT / "app/loyalty_notifications.py"
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {
        "app.db": database, "app.config": config,
        "app.wallet_push": wallet, "app.app_push": push,
    }):
        spec.loader.exec_module(module)
    return module


class LoyaltyNotificationsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.notifications = load_notifications()
        self.bot = SimpleNamespace(send_message=AsyncMock())
        self.common = dict(
            bot=self.bot, user_id=22, telegram_user_id=202,
            shop_id=3, shop_name="Наші",
        )

    async def test_add_scanner_payloads_are_unchanged_for_one_two_and_twenty(self):
        for count in (1, 2, 20):
            with self.subTest(count=count):
                push = AsyncMock()
                self.bot.send_message.reset_mock()
                with patch.object(
                    self.notifications, "refresh_wallet_and_send_app_push", push
                ):
                    await self.notifications.notify_cups_added(
                        **self.common, count=count, earned_free=0,
                        shop_client={"cups": 5, "free_coffee_balance": 1},
                    )
                word = "чашку" if count == 1 else "чашки"
                push.assert_awaited_once_with(
                    user_id=22, title=f"☕ Нараховано {count} {word}",
                    body="Наші. Зараз у вас 5/7 чашок",
                    event_type="cups_added", shop_id=3,
                )
                self.bot.send_message.assert_awaited_once_with(
                    202,
                    f"☕ Тобі нарахували {count} чашок\n"
                    "🏪 Наші\n☕ Зараз: 5/7\n🎁 Безкоштовних кав: 1",
                )

    async def test_earned_gift_keeps_scanner_title_body_event_and_message(self):
        for count, earned, cups, balance in ((1, 1, 0, 1), (20, 2, 4, 4)):
            with self.subTest(count=count, earned=earned):
                push = AsyncMock()
                self.bot.send_message.reset_mock()
                with patch.object(
                    self.notifications, "refresh_wallet_and_send_app_push", push
                ):
                    await self.notifications.notify_cups_added(
                        **self.common, count=count, earned_free=earned,
                        shop_client={"cups": cups, "free_coffee_balance": balance},
                    )
                push.assert_awaited_once_with(
                    user_id=22, title="🎁 Безкоштовна кава вже ваша",
                    body=(f"Наші. Нараховано {count} чашок. "
                          f"Безкоштовних кав: {balance}"),
                    event_type="free_coffee_earned", shop_id=3,
                )
                self.bot.send_message.assert_awaited_once_with(
                    202,
                    f"☕ Тобі нарахували {count} чашок\n🏪 Наші\n"
                    f"☕ Зараз: {cups}/7\n🎁 Безкоштовних кав: {balance}\n"
                    f"✨ Додано безкоштовних кав: {earned}",
                )

    async def test_redeem_scanner_payloads_are_unchanged(self):
        push = AsyncMock()
        with patch.object(
            self.notifications, "refresh_wallet_and_send_app_push", push
        ):
            await self.notifications.notify_free_redeemed(
                **self.common,
                shop_client={"cups": 5, "free_coffee_balance": 0},
            )
        push.assert_awaited_once_with(
            user_id=22, title="🎁 Безкоштовну каву використано",
            body="Наші. Залишок безкоштовних кав: 0",
            event_type="free_coffee_redeemed", shop_id=3,
        )
        self.bot.send_message.assert_awaited_once_with(
            202,
            "✅ У тебе списали 1 безкоштовну каву\n🏪 Наші\n"
            "☕ Чашок: 5/7\n🎁 Залишок безкоштовних кав: 0",
        )

    async def test_wallet_failure_does_not_skip_app_push_or_telegram(self):
        self.notifications.touch_wallet_pass.side_effect = RuntimeError("offline Wallet")
        self.notifications.get_app_push_devices_for_user.return_value = [{"token": "offline"}]
        self.notifications.send_app_pushes.return_value = {"sent": 1, "failed": 0}
        with patch("builtins.print"):
            await self.notifications.notify_cups_added(
                **self.common, count=1, earned_free=0,
                shop_client={"cups": 5, "free_coffee_balance": 0},
            )
        self.notifications.send_app_pushes.assert_awaited_once_with(
            devices=[{"token": "offline"}], title="☕ Нараховано 1 чашку",
            body="Наші. Зараз у вас 5/7 чашок",
            data={"type": "cups_added", "shop_id": 3},
        )
        self.bot.send_message.assert_awaited_once()

    async def test_wallet_push_app_push_and_telegram_errors_are_isolated(self):
        self.notifications.touch_wallet_pass.return_value = {"serial_number": "offline-serial"}
        self.notifications.get_wallet_push_tokens.return_value = ["offline-wallet"]
        self.notifications.send_wallet_pushes.side_effect = RuntimeError("offline Wallet push")
        self.notifications.get_app_push_devices_for_user.return_value = [{"token": "offline"}]
        self.notifications.send_app_pushes.side_effect = RuntimeError("offline app push")
        self.bot.send_message.side_effect = RuntimeError("offline Telegram")
        with patch("builtins.print"):
            await self.notifications.notify_free_redeemed(
                **self.common, shop_client={"cups": 5, "free_coffee_balance": 0}
            )
        self.notifications.send_wallet_pushes.assert_awaited_once_with(["offline-wallet"])
        self.notifications.send_app_pushes.assert_awaited_once()
        self.bot.send_message.assert_awaited_once()

    async def test_unexpected_push_helper_failure_does_not_skip_telegram(self):
        for function, extra in (
            (self.notifications.notify_cups_added, {"count": 1, "earned_free": 0}),
            (self.notifications.notify_free_redeemed, {}),
        ):
            with self.subTest(function=function.__name__):
                self.bot.send_message.reset_mock()
                with patch.object(
                    self.notifications, "refresh_wallet_and_send_app_push",
                    AsyncMock(side_effect=RuntimeError("offline unexpected failure")),
                ):
                    await function(
                        **self.common, **extra,
                        shop_client={"cups": 5, "free_coffee_balance": 0},
                    )
                self.bot.send_message.assert_awaited_once()

    async def test_api_sender_uses_existing_token_and_closes_owned_bot(self):
        sender = SimpleNamespace(
            send_message=AsyncMock(), session=SimpleNamespace(close=AsyncMock())
        )
        with patch.object(self.notifications, "Bot", return_value=sender) as factory:
            await self.notifications._send_telegram_notification(
                bot=None, telegram_user_id=202, text="Original text"
            )
        self.assertEqual(factory.call_args.kwargs["token"], "9001:offline-test-token")
        self.assertEqual(factory.call_args.kwargs["default"].parse_mode, "HTML")
        sender.send_message.assert_awaited_once_with(202, "Original text")
        sender.session.close.assert_awaited_once()

    async def test_api_sender_creation_send_and_close_failure_do_not_raise(self):
        with patch.object(self.notifications, "Bot", side_effect=RuntimeError("offline create")):
            await self.notifications._send_telegram_notification(
                bot=None, telegram_user_id=202, text="Original text"
            )
        sender = SimpleNamespace(
            send_message=AsyncMock(side_effect=RuntimeError("offline send")),
            session=SimpleNamespace(close=AsyncMock(side_effect=RuntimeError("offline close"))),
        )
        with patch.object(self.notifications, "Bot", return_value=sender):
            await self.notifications._send_telegram_notification(
                bot=None, telegram_user_id=202, text="Original text"
            )
        sender.session.close.assert_awaited_once()

    async def test_supplied_scanner_bot_is_not_closed_or_replaced(self):
        self.bot.session = SimpleNamespace(close=AsyncMock())
        with patch.object(self.notifications, "Bot") as factory:
            await self.notifications._send_telegram_notification(
                bot=self.bot, telegram_user_id=202, text="Original text"
            )
        factory.assert_not_called()
        self.bot.session.close.assert_not_awaited()
        self.bot.send_message.assert_awaited_once_with(202, "Original text")

    async def test_no_telegram_profile_does_not_create_bot(self):
        with patch.object(self.notifications, "Bot") as factory:
            await self.notifications._send_telegram_notification(
                bot=None, telegram_user_id=None, text="Original text"
            )
        factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
