from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.filters.command import CommandObject
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import asyncio

from app.db import (
    ensure_user,
    is_any_shop_admin,
    is_owner,
    assign_pending_owner_if_exists,
    get_panel_mode,
    set_panel_mode,
    create_admin_login_ticket,
)
from app.keyboards import user_main_keyboard, admin_main_keyboard
from app.config import SUPER_ADMIN_IDS, SCANNER_URL, ADMIN_PANEL_URL
from app.telegram_link import bind_telegram_link_session, confirm_telegram_link_session


router = Router()

def get_personal_admin_panel_url(user_id: int) -> str:
    ticket = create_admin_login_ticket(user_id)
    base_url = ADMIN_PANEL_URL.rstrip("/")
    return f"{base_url}/?ticket={ticket}"


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
                    admin_panel_url=(
                        get_personal_admin_panel_url(user_id)
                        if owner_flag
                        else ""
                    ),
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
                admin_panel_url="",
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
                admin_panel_url=(
                    get_personal_admin_panel_url(user_id)
                    if owner_flag
                    else ""
                ),
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
            admin_panel_url="",
        )
    )


def telegram_link_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Підтвердити підключення",
                    callback_data=f"tg_link_confirm:{token}",
                )
            ]
        ]
    )


@router.message(Command("start"))
async def start_handler(
    message: types.Message,
    command: CommandObject,
):
    args = (command.args or "").strip()

    # Новый сценарий:
    # /start link_<одноразовый_token>
    if args.startswith("link_"):
        token = args.removeprefix("link_").strip()

        if (not token or not message.from_user
                or message.chat.type != "private"
                or message.chat.id != message.from_user.id):
            await message.answer(
                "❌ Посилання для підключення некоректне.\n"
                "Поверніться в застосунок «Наші» та спробуйте ще раз."
            )
            return

        try:
            binding = await asyncio.to_thread(
                bind_telegram_link_session, token, message.from_user.id
            )
        except Exception as error:
            print("TELEGRAM LINK BIND ERROR:", repr(error))
            binding = {"ok": False}

        if not binding.get("ok"):
            await message.answer(
                "❌ Посилання для підключення недійсне.\n"
                "Поверніться в застосунок «Наші» та спробуйте ще раз."
            )
            return

        await message.answer(
            "☕ <b>Підключити бонуси до «Наші»?</b>\n\n"
            "Після підтвердження ваші чашки, подарунки та історія "
            "будуть доступні у профілі застосунку.\n\n"
            "Якщо ви не починали це підключення — нічого не натискайте.",
            reply_markup=telegram_link_keyboard(token),
        )
        return

    # Обычный /start оставляем без изменений.
    ensure_user(
        telegram_user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name
    )

    assign_pending_owner_if_exists(message.from_user.id)
    await send_correct_panel(message)


@router.callback_query(F.data.startswith("tg_link_confirm:"))
async def confirm_telegram_link(callback: types.CallbackQuery):
    if (not callback.data or not callback.from_user
            or not callback.message
            or callback.message.chat.type != "private"
            or callback.message.chat.id != callback.from_user.id):
        return

    token = callback.data.split(":", 1)[1].strip()

    if not token:
        await callback.answer(
            "Некоректне посилання",
            show_alert=True,
        )
        return

    await callback.answer("Підтверджуємо...")

    try:
        data = await asyncio.to_thread(
            confirm_telegram_link_session, token, callback.from_user.id
        )

    except Exception as error:
        print("TELEGRAM LINK CONFIRM ERROR:", repr(error))
        await callback.message.edit_text(
            "❌ Не вдалося підтвердити підключення.\n\n"
            "Спробуйте ще раз із застосунку «Наші»."
        )
        return

    if not data.get("ok"):
        status = data.get("status")
        message = data.get("message") or "Не вдалося підключити профіль"

        if status == "telegram_not_found":
            await callback.message.edit_text(
                "🤔 Ми не знайшли старий профіль «Наші» для цього Telegram.\n\n"
                "Якщо ви раніше не користувалися програмою через цього бота, "
                "поверніться в застосунок і виберіть «Створити новий профіль»."
            )
            return

        if status == "expired":
            await callback.message.edit_text(
                "⌛ Посилання вже не дійсне.\n\n"
                "Поверніться в застосунок «Наші» та почніть підключення ще раз."
            )
            return

        await callback.message.edit_text(
            f"❌ {message}"
        )
        return

    await callback.message.edit_text(
        "✅ <b>Вхід підтверджено!</b>\n\n"
        "Ваші бонуси підключено до профілю «Наші».\n"
        "Тепер поверніться в застосунок — він завершить вхід автоматично."
    )


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
