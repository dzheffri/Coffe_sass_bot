from aiogram import Router, types, F
from aiogram.filters import Command

from app.db import (
    ensure_user,
    is_any_shop_admin,
    is_owner,
    assign_pending_owner_if_exists,
    get_panel_mode,
    set_panel_mode,
)
from app.keyboards import user_main_keyboard, admin_main_keyboard
from app.config import SUPER_ADMIN_IDS, SCANNER_URL


router = Router()


async def send_correct_panel(message: types.Message):
    user_id = message.from_user.id
    admin_flag = is_any_shop_admin(user_id)
    owner_flag = is_owner(user_id)
    super_admin_flag = user_id in SUPER_ADMIN_IDS

    if not admin_flag and not super_admin_flag:
        await message.answer(
            "👋 Вітаємо у програмі лояльності Наші Coffee Club Pass ☕\n\n"
            "✨ Ваша кава — тепер ще вигідніша\n\n"
            "☕ За кожну покупку ти отримуєш чашки\n"
            "🎁 Збери 7 — і отримай 8-му каву безкоштовно\n\n"
            "📱 Просто натисни «Мій QR-код»\n"
            "та покажи його баристі при замовленні\n\n"
            "🏪 Один QR-код працює у всіх кав’ярнях,\n"
            "які підключені до програми «Наші»",
            reply_markup=user_main_keyboard()
        )
        return

    panel_mode = get_panel_mode(user_id)

    if super_admin_flag and admin_flag:
        if panel_mode == "owner":
            await message.answer(
                "✅ Увімкнено режим owner.\n"
                "Тут ти бачиш інтерфейс власника кав’ярні.",
                reply_markup=admin_main_keyboard(
                    scanner_url=SCANNER_URL,
                    is_owner=owner_flag,
                    is_super_admin=False,
                    can_switch_to_owner=False,
                    can_switch_to_super_admin=True,
                )
            )
            return

        await message.answer(
            "✅ Увімкнено режим super admin.\n"
            "Тут ти керуєш усією системою.",
            reply_markup=admin_main_keyboard(
                scanner_url=SCANNER_URL,
                is_owner=False,
                is_super_admin=True,
                can_switch_to_owner=True,
                can_switch_to_super_admin=False,
            )
        )
        return

    if admin_flag:
        await message.answer(
            "✅ Адмін-панель кав’ярні активна.",
            reply_markup=admin_main_keyboard(
                scanner_url=SCANNER_URL,
                is_owner=owner_flag,
                is_super_admin=False,
                can_switch_to_owner=False,
                can_switch_to_super_admin=False,
            )
        )
        return

    await message.answer(
        "✅ Адмін-панель super admin активна.",
        reply_markup=admin_main_keyboard(
            scanner_url=SCANNER_URL,
            is_owner=False,
            is_super_admin=True,
            can_switch_to_owner=False,
            can_switch_to_super_admin=False,
        )
    )


@router.message(Command("start"))
async def start_handler(message: types.Message):
    ensure_user(
        telegram_user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name
    )

    assign_pending_owner_if_exists(message.from_user.id)
    await send_correct_panel(message)


@router.message(Command("id"))
async def my_id_command_handler(message: types.Message):
    await message.answer(
        f"Твій Telegram ID:\n`{message.from_user.id}`",
        parse_mode="Markdown"
    )


@router.message(F.text == "👑 Режим owner")
async def switch_to_owner_mode(message: types.Message):
    user_id = message.from_user.id

    if user_id not in SUPER_ADMIN_IDS:
        return

    if not is_any_shop_admin(user_id):
        return

    set_panel_mode(user_id, "owner")
    await send_correct_panel(message)


@router.message(F.text == "🛠 Режим super admin")
async def switch_to_super_admin_mode(message: types.Message):
    user_id = message.from_user.id

    if user_id not in SUPER_ADMIN_IDS:
        return

    set_panel_mode(user_id, "super_admin")
    await send_correct_panel(message)
