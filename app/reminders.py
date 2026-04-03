import asyncio
from datetime import datetime, timezone

from aiogram import Bot

from app.db import (
    get_clients_for_one_left_reminder,
    get_clients_for_inactive_reminder,
    get_clients_with_free_coffee,
    was_reminder_sent_recently,
    save_reminder_log,
    save_auto_touch,
)


REMINDER_ONE_LEFT = "one_left"
REMINDER_INACTIVE_5_7 = "inactive_5_7"
REMINDER_INACTIVE_14_30 = "inactive_14_30"
REMINDER_FREE_COFFEE = "free_coffee"

CHECK_INTERVAL_SECONDS = 60 * 30  # раз в 30 минут


def utc_now():
    return datetime.now(timezone.utc)


def _safe_last_activity(row):
    value = row.get("last_activity_at")
    if not value:
        return None

    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def _days_since_last_activity(row):
    last_activity = _safe_last_activity(row)
    if not last_activity:
        return None

    delta = utc_now() - last_activity
    return delta.total_seconds() / 86400


async def send_one_left_reminders(bot: Bot):
    clients = get_clients_for_one_left_reminder()

    for row in clients:
        shop_id = row["shop_id"]
        user_id = row["user_id"]
        telegram_user_id = row["telegram_user_id"]

        # чтобы не спамить: не чаще 1 раза в 3 дня
        if was_reminder_sent_recently(
            shop_id=shop_id,
            user_id=user_id,
            reminder_type=REMINDER_ONE_LEFT,
            days=3,
        ):
            continue

        text = (
            f"☕ У тебе залишилась лише 1 чашка до безкоштовної кави!\n\n"
            f"🏪 {row['shop_name']}\n"
            f"☕ Зараз: {row['cups']}/7\n\n"
            f"Заходь найближчим часом і забирай свій бонус 🎁"
        )

        try:
            await bot.send_message(telegram_user_id, text)
            save_reminder_log(shop_id, user_id, REMINDER_ONE_LEFT)
            save_auto_touch(shop_id, user_id)
        except Exception as e:
            print(f"[reminders][one_left] failed for {telegram_user_id}: {e}")


async def send_inactive_5_7_reminders(bot: Bot):
    clients = get_clients_for_inactive_reminder(days_from=5, days_to=7)

    for row in clients:
        shop_id = row["shop_id"]
        user_id = row["user_id"]
        telegram_user_id = row["telegram_user_id"]

        # не чаще 1 раза в 5 дней
        if was_reminder_sent_recently(
            shop_id=shop_id,
            user_id=user_id,
            reminder_type=REMINDER_INACTIVE_5_7,
            days=5,
        ):
            continue

        days_ago = _days_since_last_activity(row)
        days_text = f"{int(days_ago)}" if days_ago is not None else "декілька"

        text = (
            f"👋 Ми скучили за тобою\n\n"
            f"🏪 {row['shop_name']}\n"
            f"Ти не заходив приблизно {days_text} днів.\n\n"
            f"☕ Зараз у тебе: {row['cups']}/7\n"
            f"🎁 Безкоштовних кав: {row['free_coffee_balance']}\n\n"
            f"Заходь на каву найближчим часом 💛"
        )

        try:
            await bot.send_message(telegram_user_id, text)
            save_reminder_log(shop_id, user_id, REMINDER_INACTIVE_5_7)
            save_auto_touch(shop_id, user_id)
        except Exception as e:
            print(f"[reminders][inactive_5_7] failed for {telegram_user_id}: {e}")


async def send_inactive_14_30_reminders(bot: Bot):
    clients = get_clients_for_inactive_reminder(days_from=14, days_to=30)

    for row in clients:
        shop_id = row["shop_id"]
        user_id = row["user_id"]
        telegram_user_id = row["telegram_user_id"]

        # не чаще 1 раза в 10 дней
        if was_reminder_sent_recently(
            shop_id=shop_id,
            user_id=user_id,
            reminder_type=REMINDER_INACTIVE_14_30,
            days=10,
        ):
            continue

        days_ago = _days_since_last_activity(row)
        days_text = f"{int(days_ago)}" if days_ago is not None else "деякий час"

        text = (
            f"☕ Давно не бачилися\n\n"
            f"🏪 {row['shop_name']}\n"
            f"Ти не був у нас вже приблизно {days_text} днів.\n\n"
            f"☕ Зараз у тебе: {row['cups']}/7\n"
            f"🎁 Безкоштовних кав: {row['free_coffee_balance']}\n\n"
            f"Будемо раді бачити тебе знову 💛"
        )

        try:
            await bot.send_message(telegram_user_id, text)
            save_reminder_log(shop_id, user_id, REMINDER_INACTIVE_14_30)
            save_auto_touch(shop_id, user_id)
        except Exception as e:
            print(f"[reminders][inactive_14_30] failed for {telegram_user_id}: {e}")


async def send_free_coffee_reminders(bot: Bot):
    clients = get_clients_with_free_coffee()

    for row in clients:
        shop_id = row["shop_id"]
        user_id = row["user_id"]
        telegram_user_id = row["telegram_user_id"]

        # не чаще 1 раза в 3 дня
        if was_reminder_sent_recently(
            shop_id=shop_id,
            user_id=user_id,
            reminder_type=REMINDER_FREE_COFFEE,
            days=3,
        ):
            continue

        text = (
            f"🎁 У тебе вже є безкоштовна кава!\n\n"
            f"🏪 {row['shop_name']}\n"
            f"🎁 Безкоштовних кав: {row['free_coffee_balance']}\n"
            f"☕ Поточні чашки: {row['cups']}/7\n\n"
            f"Заходь та забирай свій бонус ☕"
        )

        try:
            await bot.send_message(telegram_user_id, text)
            save_reminder_log(shop_id, user_id, REMINDER_FREE_COFFEE)
            save_auto_touch(shop_id, user_id)
        except Exception as e:
            print(f"[reminders][free_coffee] failed for {telegram_user_id}: {e}")


async def run_reminders_once(bot: Bot):
    await send_one_left_reminders(bot)
    await send_inactive_5_7_reminders(bot)
    await send_inactive_14_30_reminders(bot)
    await send_free_coffee_reminders(bot)


async def reminders_loop(bot: Bot):
    print("REMINDERS LOOP STARTED 🔁")

    while True:
        try:
            await run_reminders_once(bot)
            print("REMINDERS: ✅ done")
        except Exception as e:
            print(f"REMINDERS ERROR: {e}")

        await asyncio.sleep(CHECK_INTERVAL_SECONDS)
