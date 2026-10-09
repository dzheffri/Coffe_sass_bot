import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot

from app.db import (
    get_clients_for_one_left_reminder,
    get_clients_for_inactive_reminder,
    get_clients_with_free_coffee,
    get_owners_for_subscription_last_day,
    was_reminder_sent_recently,
    save_reminder_log,
    save_auto_touch,
    get_shop_reminder_settings,
    get_app_push_devices_for_user,
)
from app.app_push import send_app_pushes


KYIV_TZ = ZoneInfo("Europe/Kyiv")

REMINDER_ONE_LEFT = "one_left"
REMINDER_INACTIVE_5_7 = "inactive_5_7"
REMINDER_INACTIVE_14_30 = "inactive_14_30"
REMINDER_FREE_COFFEE = "free_coffee"
REMINDER_SUBSCRIPTION_LAST_DAY = "subscription_last_day"


def kyiv_now():
    return datetime.now(KYIV_TZ)


async def _send_reminder_app_push(
    *, user_id, shop_id, reminder_type, title, body
):
    if not user_id:
        return

    try:
        devices = get_app_push_devices_for_user(user_id)
        unique_devices = []
        seen_tokens = set()
        for device in devices:
            token = (device.get("device_token") or "").strip().lower()
            if token and token not in seen_tokens:
                seen_tokens.add(token)
                unique_devices.append(device)

        if not unique_devices:
            return

        result = await send_app_pushes(
            devices=unique_devices,
            title=title,
            body=body,
            data={
                "type": "reminder",
                "reminder_type": reminder_type,
                "shop_id": shop_id,
            },
        )
        print(
            f"[reminders][APP PUSH] type={reminder_type} "
            f"devices={len(unique_devices)} "
            f"sent={result.get('sent', 0)} failed={result.get('failed', 0)}"
        )
    except Exception as exc:
        print(
            f"[reminders][APP PUSH] type={reminder_type} "
            f"error={type(exc).__name__}"
        )


async def send_subscription_last_day_reminders(bot: Bot):
    owners = get_owners_for_subscription_last_day()

    for row in owners:
        shop_id = row["shop_id"]
        user_id = row["user_id"]
        telegram_user_id = row["telegram_user_id"]

        text = (
            f"⚠️ У вас залишився останній день підписки\n\n"
            f"🏪 Кав’ярня: {row['shop_name']}\n\n"
            f"Після завершення підписки доступ до системи буде заблоковано.\n\n"
            f"Щоб продовжити роботу, зв’яжіться з адміністратором сервісу."
        )

        try:
            await bot.send_message(telegram_user_id, text)

            save_reminder_log(
                shop_id,
                user_id,
                REMINDER_SUBSCRIPTION_LAST_DAY
            )

        except Exception as e:
            print(
                f"[reminders][subscription_last_day] "
                f"failed for owner {telegram_user_id}: {e}"
            )

        await _send_reminder_app_push(
            user_id=user_id,
            shop_id=shop_id,
            reminder_type=REMINDER_SUBSCRIPTION_LAST_DAY,
            title="⚠️ Останній день підписки",
            body=(
                f"{row['shop_name']} · Після завершення підписки доступ "
                "буде заблоковано. Зв’яжіться з адміністратором для продовження."
            ),
        )


async def send_one_left_reminders(bot: Bot):
    clients = get_clients_for_one_left_reminder()

    settings_cache = {}

    for row in clients:
        shop_id = row["shop_id"]
        user_id = row["user_id"]
        telegram_user_id = row["telegram_user_id"]

        if shop_id not in settings_cache:
            settings_cache[shop_id] = get_shop_reminder_settings(shop_id)

        settings = settings_cache[shop_id]

        if not settings["one_left_enabled"]:
            continue

        repeat_days = settings["one_left_days"]

        if was_reminder_sent_recently(
            shop_id=shop_id,
            user_id=user_id,
            reminder_type=REMINDER_ONE_LEFT,
            days=repeat_days,
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

            save_reminder_log(
                shop_id,
                user_id,
                REMINDER_ONE_LEFT
            )

            save_auto_touch(shop_id, user_id)

        except Exception as e:
            print(
                f"[reminders][one_left] "
                f"failed for {telegram_user_id}: {e}"
            )

        await _send_reminder_app_push(
            user_id=user_id,
            shop_id=shop_id,
            reminder_type=REMINDER_ONE_LEFT,
            title="☕ Ще одна кава — і подарунок!",
            body=(
                f"{row['shop_name']} · Зараз: {row['cups']}/7 · "
                "Заходь і забирай свій бонус 🎁"
            ),
        )


async def send_inactive_5_7_reminders(bot: Bot):
    clients = get_clients_for_inactive_reminder(
        days_from=5,
        days_to=7
    )

    settings_cache = {}

    for row in clients:
        shop_id = row["shop_id"]
        user_id = row["user_id"]
        telegram_user_id = row["telegram_user_id"]

        if shop_id not in settings_cache:
            settings_cache[shop_id] = get_shop_reminder_settings(shop_id)

        settings = settings_cache[shop_id]

        if not settings["inactive_5_7_enabled"]:
            continue

        repeat_days = settings["inactive_5_7_days"]

        if was_reminder_sent_recently(
            shop_id=shop_id,
            user_id=user_id,
            reminder_type=REMINDER_INACTIVE_5_7,
            days=repeat_days,
        ):
            continue

        text = (
            f"👋 Ми скучили за тобою\n\n"
            f"🏪 {row['shop_name']}\n"
            f"Ти давно не заходив до нас.\n\n"
            f"☕ Зараз у тебе: {row['cups']}/7\n"
            f"🎁 Безкоштовних кав: {row['free_coffee_balance']}\n\n"
            f"Заходь на каву найближчим часом 💛"
        )

        try:
            await bot.send_message(telegram_user_id, text)

            save_reminder_log(
                shop_id,
                user_id,
                REMINDER_INACTIVE_5_7
            )

            save_auto_touch(shop_id, user_id)

        except Exception as e:
            print(
                f"[reminders][inactive_5_7] "
                f"failed for {telegram_user_id}: {e}"
            )

        await _send_reminder_app_push(
            user_id=user_id,
            shop_id=shop_id,
            reminder_type=REMINDER_INACTIVE_5_7,
            title="👋 Ми скучили за тобою",
            body=(
                f"{row['shop_name']} · Заходь на каву 💛 · "
                f"Чашки: {row['cups']}/7 · "
                f"Безкоштовних кав: {row['free_coffee_balance']}"
            ),
        )


async def send_inactive_14_30_reminders(bot: Bot):
    clients = get_clients_for_inactive_reminder(
        days_from=14,
        days_to=30
    )

    settings_cache = {}

    for row in clients:
        shop_id = row["shop_id"]
        user_id = row["user_id"]
        telegram_user_id = row["telegram_user_id"]

        if shop_id not in settings_cache:
            settings_cache[shop_id] = get_shop_reminder_settings(shop_id)

        settings = settings_cache[shop_id]

        if not settings["inactive_14_30_enabled"]:
            continue

        repeat_days = settings["inactive_14_30_days"]

        if was_reminder_sent_recently(
            shop_id=shop_id,
            user_id=user_id,
            reminder_type=REMINDER_INACTIVE_14_30,
            days=repeat_days,
        ):
            continue

        text = (
            f"☕ Давно не бачилися\n\n"
            f"🏪 {row['shop_name']}\n"
            f"Ти давно не був у нас.\n\n"
            f"☕ Зараз у тебе: {row['cups']}/7\n"
            f"🎁 Безкоштовних кав: {row['free_coffee_balance']}\n\n"
            f"Будемо раді бачити тебе знову 💛"
        )

        try:
            await bot.send_message(telegram_user_id, text)

            save_reminder_log(
                shop_id,
                user_id,
                REMINDER_INACTIVE_14_30
            )

            save_auto_touch(shop_id, user_id)

        except Exception as e:
            print(
                f"[reminders][inactive_14_30] "
                f"failed for {telegram_user_id}: {e}"
            )

        await _send_reminder_app_push(
            user_id=user_id,
            shop_id=shop_id,
            reminder_type=REMINDER_INACTIVE_14_30,
            title="☕ Давно не бачилися",
            body=(
                f"{row['shop_name']} · Будемо раді бачити тебе знову 💛 · "
                f"Чашки: {row['cups']}/7 · "
                f"Безкоштовних кав: {row['free_coffee_balance']}"
            ),
        )


async def send_free_coffee_reminders(bot: Bot):
    clients = get_clients_with_free_coffee()

    settings_cache = {}

    for row in clients:
        shop_id = row["shop_id"]
        user_id = row["user_id"]
        telegram_user_id = row["telegram_user_id"]

        if shop_id not in settings_cache:
            settings_cache[shop_id] = get_shop_reminder_settings(shop_id)

        settings = settings_cache[shop_id]

        if not settings["free_coffee_enabled"]:
            continue

        repeat_days = settings["free_coffee_days"]

        if was_reminder_sent_recently(
            shop_id=shop_id,
            user_id=user_id,
            reminder_type=REMINDER_FREE_COFFEE,
            days=repeat_days,
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

            save_reminder_log(
                shop_id,
                user_id,
                REMINDER_FREE_COFFEE
            )

            save_auto_touch(shop_id, user_id)

        except Exception as e:
            print(
                f"[reminders][free_coffee] "
                f"failed for {telegram_user_id}: {e}"
            )

        await _send_reminder_app_push(
            user_id=user_id,
            shop_id=shop_id,
            reminder_type=REMINDER_FREE_COFFEE,
            title="🎁 У тебе є безкоштовна кава!",
            body=(
                f"{row['shop_name']} · "
                f"Безкоштовних кав: {row['free_coffee_balance']} · "
                f"Поточні чашки: {row['cups']}/7"
            ),
        )


async def run_reminders_once(bot: Bot):
    await send_subscription_last_day_reminders(bot)

    await send_one_left_reminders(bot)
    await send_inactive_5_7_reminders(bot)
    await send_inactive_14_30_reminders(bot)
    await send_free_coffee_reminders(bot)


async def reminders_loop(bot: Bot):
    print("REMINDERS LOOP STARTED 🔁")

    last_run_date = None

    while True:
        try:
            now = kyiv_now()
            today = now.date()

            if now.hour == 9 and last_run_date != today:
                await run_reminders_once(bot)

                print("REMINDERS: ✅ morning run done")

                last_run_date = today

        except Exception as e:
            print(f"REMINDERS ERROR: {e}")

        await asyncio.sleep(300)
