import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from app.config import BOT_TOKEN
from app.handlers import (
    common_router,
    user_router,
    admin_router,
    owner_router,
    super_admin_router,
)
from app.reminders import reminders_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)


async def main():
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    dp = Dispatcher()

    dp.include_router(common_router)
    dp.include_router(user_router)
    dp.include_router(admin_router)
    dp.include_router(owner_router)
    dp.include_router(super_admin_router)

    await bot.delete_webhook(drop_pending_updates=True)

    reminder_task = asyncio.create_task(reminders_loop(bot))

    try:
        await dp.start_polling(bot)
    finally:
        reminder_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await reminder_task


if __name__ == "__main__":
    import contextlib
    asyncio.run(main())
