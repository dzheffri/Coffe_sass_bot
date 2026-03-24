from aiogram import Router, types
from aiogram.filters import Command

from app.db import (
    ensure_user,
    is_any_shop_admin,
    is_owner,
    assign_pending_owner_if_exists,
)
from app.keyboards import user_main_keyboard, admin_main_keyboard
from app.config import SUPER_ADMIN_IDS, SCANNER_URL

router = Router()


@router.message(Command("start"))
async def start_handler(message: types.Message):
    ensure_user(
        telegram_user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name
    )

    assign_pending_owner_if_exists(message.from_user.id)

    user_id = message.from_user.id
    admin_flag = is_any_shop_admin(user_id)
    owner_flag = is_owner(user_id)
    super_admin_flag = user_id in SUPER_ADMIN_IDS

    if admin_flag or super_admin_flag:
        kb = admin_main_keyboard(
            scanner_url=SCANNER_URL,
            is_owner=owner_flag,
            is_super_admin=super_admin_flag
        )
        await message.answer(
            "👋 Вітаю. Адмін-панель активна.",
            reply_markup=kb
        )
        return

    await message.answer(
        "👋 Вітаємо у бонусній системі.\n\n"
        "У тебе один QR-код для всіх кав’ярень.\n"
        "На касі нічого обирати не потрібно — бариста просто сканує код.",
        reply_markup=user_main_keyboard()
    )