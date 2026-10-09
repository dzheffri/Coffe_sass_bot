"""Exercise real reminder jobs with inert DB and delivery dependencies."""

import importlib.util
import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch


PROJECT = Path(__file__).resolve().parents[1]
SCENARIOS = (
    "subscription_last_day", "one_left", "inactive_5_7", "inactive_14_30",
    "free_coffee",
)
CLIENT_SCENARIOS = SCENARIOS[1:]
ROW = {
    "shop_id": 3, "user_id": 22, "telegram_user_id": 202,
    "shop_name": "Наші", "cups": 6, "free_coffee_balance": 2,
}
DEVICE = {"device_token": "offline-device", "environment": "production"}
SETTINGS = {
    "one_left_enabled": True, "one_left_days": 3,
    "inactive_5_7_enabled": True, "inactive_5_7_days": 8,
    "inactive_14_30_enabled": True, "inactive_14_30_days": 21,
    "free_coffee_enabled": True, "free_coffee_days": 5,
}
TELEGRAM_TEXTS = {
    "subscription_last_day": (
        "⚠️ У вас залишився останній день підписки\n\n"
        "🏪 Кав’ярня: Наші\n\n"
        "Після завершення підписки доступ до системи буде заблоковано.\n\n"
        "Щоб продовжити роботу, зв’яжіться з адміністратором сервісу."
    ),
    "one_left": (
        "☕ У тебе залишилась лише 1 чашка до безкоштовної кави!\n\n"
        "🏪 Наші\n☕ Зараз: 6/7\n\n"
        "Заходь найближчим часом і забирай свій бонус 🎁"
    ),
    "inactive_5_7": (
        "👋 Ми скучили за тобою\n\n🏪 Наші\n"
        "Ти давно не заходив до нас.\n\n"
        "☕ Зараз у тебе: 6/7\n🎁 Безкоштовних кав: 2\n\n"
        "Заходь на каву найближчим часом 💛"
    ),
    "inactive_14_30": (
        "☕ Давно не бачилися\n\n🏪 Наші\nТи давно не був у нас.\n\n"
        "☕ Зараз у тебе: 6/7\n🎁 Безкоштовних кав: 2\n\n"
        "Будемо раді бачити тебе знову 💛"
    ),
    "free_coffee": (
        "🎁 У тебе вже є безкоштовна кава!\n\n🏪 Наші\n"
        "🎁 Безкоштовних кав: 2\n☕ Поточні чашки: 6/7\n\n"
        "Заходь та забирай свій бонус ☕"
    ),
}
PUSH_TEXTS = {
    "subscription_last_day": (
        "⚠️ Останній день підписки",
        "Наші · Після завершення підписки доступ буде заблоковано. "
        "Зв’яжіться з адміністратором для продовження.",
    ),
    "one_left": (
        "☕ Ще одна кава — і подарунок!",
        "Наші · Зараз: 6/7 · Заходь і забирай свій бонус 🎁",
    ),
    "inactive_5_7": (
        "👋 Ми скучили за тобою",
        "Наші · Заходь на каву 💛 · Чашки: 6/7 · Безкоштовних кав: 2",
    ),
    "inactive_14_30": (
        "☕ Давно не бачилися",
        "Наші · Будемо раді бачити тебе знову 💛 · Чашки: 6/7 · Безкоштовних кав: 2",
    ),
    "free_coffee": (
        "🎁 У тебе є безкоштовна кава!",
        "Наші · Безкоштовних кав: 2 · Поточні чашки: 6/7",
    ),
}


def load_reminders(rows_by_scenario=None):
    """Import the complete production module without DB/config/APNs startup."""
    rows_by_scenario = rows_by_scenario or {}
    database = ModuleType("app.db")
    for name in (
        "get_clients_for_one_left_reminder", "get_clients_for_inactive_reminder",
        "get_clients_with_free_coffee", "get_owners_for_subscription_last_day",
        "was_reminder_sent_recently", "save_reminder_log", "save_auto_touch",
        "get_shop_reminder_settings", "get_app_push_devices_for_user",
        "touch_wallet_pass", "get_wallet_push_tokens",
    ):
        setattr(database, name, MagicMock())
    database.get_owners_for_subscription_last_day.return_value = rows_by_scenario.get(
        "subscription_last_day", []
    )
    database.get_clients_for_one_left_reminder.return_value = rows_by_scenario.get(
        "one_left", []
    )
    database.get_clients_with_free_coffee.return_value = rows_by_scenario.get(
        "free_coffee", []
    )
    database.get_clients_for_inactive_reminder.side_effect = (
        lambda *, days_from, days_to: rows_by_scenario.get(
            {(5, 7): "inactive_5_7", (14, 30): "inactive_14_30"}[
                (days_from, days_to)
            ], []
        )
    )
    database.get_shop_reminder_settings.return_value = dict(SETTINGS)
    database.was_reminder_sent_recently.return_value = False
    database.get_app_push_devices_for_user.return_value = [dict(DEVICE)]

    push = ModuleType("app.app_push")
    push.send_app_pushes = AsyncMock(return_value={"sent": 1, "failed": 0, "errors": []})
    wallet = ModuleType("app.wallet_push")
    wallet.send_wallet_pushes = AsyncMock()
    loyalty = ModuleType("app.loyalty_notifications")
    loyalty.refresh_wallet_and_send_app_push = AsyncMock()
    config = ModuleType("app.config")
    config.BOT_TOKEN = "9001:offline-test-token"
    aiogram = ModuleType("aiogram")
    aiogram.Bot = MagicMock()

    spec = importlib.util.spec_from_file_location(
        "offline_reminders", PROJECT / "app/reminders.py"
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {
        "app.db": database, "app.app_push": push, "app.wallet_push": wallet,
        "app.loyalty_notifications": loyalty, "app.config": config,
        "aiogram": aiogram,
    }):
        spec.loader.exec_module(module)
    return SimpleNamespace(
        module=module, database=database, push=push, wallet=wallet, loyalty=loyalty,
        bot=SimpleNamespace(send_message=AsyncMock()),
    )


def job(module, scenario):
    return getattr(module, f"send_{scenario}_reminders")


class RemindersPushTests(unittest.IsolatedAsyncioTestCase):
    def assert_no_wallet(self, harness):
        harness.database.touch_wallet_pass.assert_not_called()
        harness.database.get_wallet_push_tokens.assert_not_called()
        harness.wallet.send_wallet_pushes.assert_not_awaited()
        harness.loyalty.refresh_wallet_and_send_app_push.assert_not_awaited()

    def assert_logged(self, harness, scenario, rows):
        self.assertEqual(harness.module.save_reminder_log.call_args_list, [
            call(row["shop_id"], row["user_id"], scenario) for row in rows
        ])
        self.assertEqual(harness.module.save_auto_touch.call_args_list, [
            call(row["shop_id"], row["user_id"]) for row in rows
        ] if scenario in CLIENT_SCENARIOS else [])

    async def test_all_five_jobs_keep_telegram_text_and_logs_and_add_app_push(self):
        for scenario in SCENARIOS:
            with self.subTest(scenario=scenario):
                harness = load_reminders({scenario: [dict(ROW)]})
                with patch("builtins.print"):
                    await job(harness.module, scenario)(harness.bot)
                harness.bot.send_message.assert_awaited_once_with(
                    202, TELEGRAM_TEXTS[scenario]
                )
                self.assert_logged(harness, scenario, [ROW])
                harness.module.get_app_push_devices_for_user.assert_called_once_with(22)
                harness.push.send_app_pushes.assert_awaited_once()
                payload = harness.push.send_app_pushes.await_args.kwargs
                self.assertEqual(payload["devices"], [DEVICE])
                self.assertEqual(payload["data"], {
                    "type": "reminder", "reminder_type": scenario, "shop_id": 3,
                })
                self.assertEqual((payload["title"], payload["body"]), PUSH_TEXTS[scenario])
                self.assert_no_wallet(harness)

    async def test_all_five_jobs_send_push_when_telegram_fails(self):
        for scenario in SCENARIOS:
            with self.subTest(scenario=scenario):
                harness = load_reminders({scenario: [dict(ROW)]})
                harness.bot.send_message.side_effect = RuntimeError("offline Telegram")
                with patch("builtins.print"):
                    await job(harness.module, scenario)(harness.bot)
                harness.bot.send_message.assert_awaited_once_with(
                    202, TELEGRAM_TEXTS[scenario]
                )
                harness.push.send_app_pushes.assert_awaited_once()
                harness.module.save_reminder_log.assert_not_called()
                harness.module.save_auto_touch.assert_not_called()
                self.assert_no_wallet(harness)

    async def test_push_lookup_or_send_errors_do_not_stop_telegram_next_rows_or_jobs(self):
        rows = [dict(ROW), dict(ROW, user_id=23, telegram_user_id=203)]
        for failing_channel in ("lookup", "send"):
            with self.subTest(failing_channel=failing_channel):
                harness = load_reminders({scenario: rows for scenario in SCENARIOS})
                secret = "PRIVATE_APNS_TOKEN_AND_KEY_MUST_NOT_BE_LOGGED"
                if failing_channel == "lookup":
                    harness.module.get_app_push_devices_for_user.side_effect = RuntimeError(secret)
                else:
                    harness.push.send_app_pushes.side_effect = RuntimeError(secret)
                with patch("builtins.print") as printer:
                    await harness.module.run_reminders_once(harness.bot)
                self.assertEqual(harness.bot.send_message.await_count, 10)
                self.assertEqual(harness.module.get_app_push_devices_for_user.call_count, 10)
                self.assertEqual(harness.module.save_reminder_log.call_args_list, [
                    call(3, row["user_id"], scenario)
                    for scenario in SCENARIOS for row in rows
                ])
                self.assertEqual(harness.module.save_auto_touch.call_args_list, [
                    call(3, row["user_id"])
                    for scenario in CLIENT_SCENARIOS for row in rows
                ])
                self.assertEqual(harness.push.send_app_pushes.await_count,
                                 0 if failing_channel == "lookup" else 10)
                printed = " ".join(str(value) for item in printer.call_args_list
                                   for value in item.args)
                self.assertNotIn(secret, printed)
                self.assertNotIn(DEVICE["device_token"], printed)
                self.assert_no_wallet(harness)

    async def test_failed_push_results_log_only_safe_counters(self):
        harness = load_reminders({"free_coffee": [dict(ROW)]})
        harness.push.send_app_pushes.return_value = {
            "sent": 1, "failed": 2,
            "errors": [{"device_token": "PRIVATE_DEVICE_TOKEN",
                        "reason": "PRIVATE_APNS_CREDENTIAL"}],
        }
        with patch("builtins.print") as printer:
            await harness.module.send_free_coffee_reminders(harness.bot)
        printed = " ".join(str(value) for item in printer.call_args_list
                           for value in item.args)
        self.assertIn("sent=1", printed)
        self.assertIn("failed=2", printed)
        self.assertNotIn("PRIVATE_DEVICE_TOKEN", printed)
        self.assertNotIn("PRIVATE_APNS_CREDENTIAL", printed)
        self.assert_logged(harness, "free_coffee", [ROW])

    async def test_no_registered_devices_preserves_all_telegram_jobs_and_logs(self):
        for scenario in SCENARIOS:
            with self.subTest(scenario=scenario):
                harness = load_reminders({scenario: [dict(ROW)]})
                harness.module.get_app_push_devices_for_user.return_value = []
                await job(harness.module, scenario)(harness.bot)
                harness.bot.send_message.assert_awaited_once_with(
                    202, TELEGRAM_TEXTS[scenario]
                )
                harness.push.send_app_pushes.assert_not_awaited()
                self.assert_logged(harness, scenario, [ROW])

    async def test_missing_user_and_unknown_user_are_safe_push_noops(self):
        for user_id, devices in ((None, []), (0, []), (999, []), (999, None)):
            with self.subTest(user_id=user_id, devices=devices):
                harness = load_reminders()
                harness.module.get_app_push_devices_for_user.return_value = devices
                await harness.module._send_reminder_app_push(
                    user_id=user_id, shop_id=3, reminder_type="free_coffee",
                    title="Gift", body="Our shop",
                )
                harness.push.send_app_pushes.assert_not_awaited()
                if user_id:
                    harness.module.get_app_push_devices_for_user.assert_called_once_with(user_id)
                else:
                    harness.module.get_app_push_devices_for_user.assert_not_called()

    async def test_devices_are_deduplicated_by_normalized_token_per_invocation(self):
        harness = load_reminders()
        harness.module.get_app_push_devices_for_user.return_value = [
            {"device_token": " ABC ", "environment": "production"},
            {"device_token": "abc", "environment": "sandbox"},
            {"device_token": "DEF", "environment": "sandbox"},
            {"device_token": " def ", "environment": "production"},
            {"device_token": "", "environment": "production"},
            {"device_token": None, "environment": "production"},
        ]
        with patch("builtins.print"):
            for scenario in ("one_left", "free_coffee"):
                await harness.module._send_reminder_app_push(
                    user_id=22, shop_id=3, reminder_type=scenario,
                    title="Gift", body="Our shop",
                )
        self.assertEqual(harness.push.send_app_pushes.await_count, 2)
        self.assertEqual(harness.module.get_app_push_devices_for_user.call_count, 2)
        for invocation, scenario in zip(harness.push.send_app_pushes.await_args_list,
                                        ("one_left", "free_coffee")):
            devices = invocation.kwargs["devices"]
            self.assertEqual([device["device_token"].strip().lower() for device in devices],
                             ["abc", "def"])
            self.assertEqual(invocation.kwargs["data"], {
                "type": "reminder", "reminder_type": scenario, "shop_id": 3,
            })

    async def test_disabled_client_settings_skip_both_channels_and_logs(self):
        for scenario in CLIENT_SCENARIOS:
            with self.subTest(scenario=scenario):
                harness = load_reminders({scenario: [dict(ROW)]})
                harness.module.get_shop_reminder_settings.return_value[
                    f"{scenario}_enabled"
                ] = False
                await job(harness.module, scenario)(harness.bot)
                harness.module.was_reminder_sent_recently.assert_not_called()
                harness.bot.send_message.assert_not_awaited()
                harness.module.get_app_push_devices_for_user.assert_not_called()
                harness.push.send_app_pushes.assert_not_awaited()
                harness.module.save_reminder_log.assert_not_called()
                harness.module.save_auto_touch.assert_not_called()

    async def test_recent_client_reminders_skip_both_channels_using_configured_repeat_days(self):
        for scenario in CLIENT_SCENARIOS:
            with self.subTest(scenario=scenario):
                harness = load_reminders({scenario: [dict(ROW)]})
                harness.module.was_reminder_sent_recently.return_value = True
                await job(harness.module, scenario)(harness.bot)
                harness.module.was_reminder_sent_recently.assert_called_once_with(
                    shop_id=3, user_id=22, reminder_type=scenario,
                    days=SETTINGS[f"{scenario}_days"],
                )
                harness.bot.send_message.assert_not_awaited()
                harness.module.get_app_push_devices_for_user.assert_not_called()
                harness.push.send_app_pushes.assert_not_awaited()
                harness.module.save_reminder_log.assert_not_called()
                harness.module.save_auto_touch.assert_not_called()

    async def test_client_repeat_days_settings_cache_and_inactivity_ranges_stay_unchanged(self):
        rows = [dict(ROW), dict(ROW, user_id=23, telegram_user_id=203)]
        for scenario in CLIENT_SCENARIOS:
            with self.subTest(scenario=scenario):
                harness = load_reminders({scenario: rows})
                with patch("builtins.print"):
                    await job(harness.module, scenario)(harness.bot)
                harness.module.get_shop_reminder_settings.assert_called_once_with(3)
                self.assertEqual(harness.module.was_reminder_sent_recently.call_args_list, [
                    call(shop_id=3, user_id=row["user_id"], reminder_type=scenario,
                         days=SETTINGS[f"{scenario}_days"]) for row in rows
                ])
                if scenario.startswith("inactive_"):
                    days_from, days_to = (5, 7) if scenario == "inactive_5_7" else (14, 30)
                    harness.module.get_clients_for_inactive_reminder.assert_called_once_with(
                        days_from=days_from, days_to=days_to
                    )
                self.assert_logged(harness, scenario, rows)

    async def test_push_success_does_not_add_telegram_log_after_existing_log_failure(self):
        harness = load_reminders({"free_coffee": [dict(ROW)]})
        harness.module.save_reminder_log.side_effect = RuntimeError("offline logging")
        with patch("builtins.print"):
            await harness.module.send_free_coffee_reminders(harness.bot)
        harness.bot.send_message.assert_awaited_once()
        harness.module.save_reminder_log.assert_called_once_with(3, 22, "free_coffee")
        harness.module.save_auto_touch.assert_not_called()
        harness.push.send_app_pushes.assert_awaited_once()

    async def test_morning_schedule_still_runs_at_nine_once_per_kyiv_date(self):
        harness = load_reminders()

        class StopLoop(Exception):
            pass

        moments = [
            datetime(2026, 10, 9, 8, 59, tzinfo=harness.module.KYIV_TZ),
            datetime(2026, 10, 9, 9, 0, tzinfo=harness.module.KYIV_TZ),
            datetime(2026, 10, 9, 9, 5, tzinfo=harness.module.KYIV_TZ),
            datetime(2026, 10, 10, 9, 0, tzinfo=harness.module.KYIV_TZ),
        ]
        run = AsyncMock()
        sleeper = AsyncMock(side_effect=[None, None, None, StopLoop()])
        with patch.object(harness.module, "kyiv_now", side_effect=moments), \
                patch.object(harness.module, "run_reminders_once", run), \
                patch.object(harness.module.asyncio, "sleep", sleeper), \
                patch("builtins.print"):
            with self.assertRaises(StopLoop):
                await harness.module.reminders_loop(harness.bot)
        self.assertEqual(run.await_args_list, [call(harness.bot), call(harness.bot)])
        self.assertEqual(sleeper.await_args_list, [call(300)] * 4)
        self.assertEqual(harness.module.KYIV_TZ.key, "Europe/Kyiv")


if __name__ == "__main__":
    unittest.main()
