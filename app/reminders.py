import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from app.db import (
    get_clients_for_one_left_reminder,
    get_clients_for_inactive_reminder,
    was_reminder_sent_recently,
    save_reminder_log,
)

logger = logging.getLogger(__name__)
KYIV_TZ = ZoneInfo("Europe/Kyiv")


def kyiv_now():
    return datetime.now(KYIV_TZ)


async def send_morning_reminders(bot):
    sent_total = 0

    # 1) Осталась 1 чашка до бесплатной
    one_left_clients = get_clients_for_one_left_reminder()
    for row in one_left_clients:
        already_sent = was_reminder_sent_recently(
            shop_id=row["shop_id"],
            user_id=row["user_id"],
            reminder_type="one_left",
            days=7,
        )
        if already_sent:
            continue

        text = (
            f"☕ У тебе залишилася лише 1 чашка до безкоштовної кави\n"
            f"{row['shop_name']}\n\n"
            f"Будемо раді бачити тебе сьогодні 💛"
        )

        try:
            await bot.send_message(row["telegram_user_id"], text)
            save_reminder_log(row["shop_id"], row["user_id"], "one_left")
            sent_total += 1
        except Exception as e:
            logger.warning(f"[one_left reminder] failed for {row['telegram_user_id']}: {e}")

    # 2) Не был 7 дней
    inactive_7 = get_clients_for_inactive_reminder(days_from=7, days_to=30)
    for row in inactive_7:
        already_sent = was_reminder_sent_recently(
            shop_id=row["shop_id"],
            user_id=row["user_id"],
            reminder_type="inactive_7d",
            days=7,
        )
        if already_sent:
            continue

        text = (
            f"☕ Давно не бачилися в {row['shop_name']}\n\n"
            f"Заходь на каву — будемо раді тебе бачити 💛"
        )

        try:
            await bot.send_message(row["telegram_user_id"], text)
            save_reminder_log(row["shop_id"], row["user_id"], "inactive_7d")
            sent_total += 1
        except Exception as e:
            logger.warning(f"[inactive_7d reminder] failed for {row['telegram_user_id']}: {e}")

    # 3) Не был 30 дней
    inactive_30 = get_clients_for_inactive_reminder(days_from=30, days_to=None)
    for row in inactive_30:
        already_sent = was_reminder_sent_recently(
            shop_id=row["shop_id"],
            user_id=row["user_id"],
            reminder_type="inactive_30d",
            days=30,
        )
        if already_sent:
            continue

        text = (
            f"☕ Тебе давно не було в {row['shop_name']}\n\n"
            f"Можливо, саме час знову забігти на улюблену каву?"
        )

        try:
            await bot.send_message(row["telegram_user_id"], text)
            save_reminder_log(row["shop_id"], row["user_id"], "inactive_30d")
            sent_total += 1
        except Exception as e:
            logger.warning(f"[inactive_30d reminder] failed for {row['telegram_user_id']}: {e}")

    return sent_total


async def reminders_loop(bot):
    logger.info("[reminders] loop started")
    last_run_date = None

    while True:
        try:
            now = kyiv_now()
            today = now.date()

            # запускаем один раз утром в промежутке 09:00–09:59 по Киеву
            if now.hour == 9 and last_run_date != today:
                sent = await send_morning_reminders(bot)
                logger.info(f"[reminders] morning run complete, sent={sent}")
                last_run_date = today

        except Exception as e:
            logger.exception(f"[reminders] loop error: {e}")

        await asyncio.sleep(300)
