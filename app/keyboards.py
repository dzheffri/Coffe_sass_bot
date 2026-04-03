from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
)


def user_main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Мій QR-код")],
            [KeyboardButton(text="☕ Мої чашки")],
            [KeyboardButton(text="🎁 Мої безкоштовні кави")],
            [KeyboardButton(text="🏪 Мої кав’ярні")],
            [KeyboardButton(text="🏪 Обрати кав’ярню")],
        ],
        resize_keyboard=True
    )


def admin_main_keyboard(
    scanner_url: str,
    is_owner: bool = False,
    is_super_admin: bool = False,
    can_switch_to_owner: bool = False,
    can_switch_to_super_admin: bool = False,
):
    keyboard = [
        [
            KeyboardButton(
                text="📷 Сканувати QR",
                web_app=WebAppInfo(url=scanner_url)
            )
        ],
        [KeyboardButton(text="☕ Режим: нарахування")],
        [KeyboardButton(text="✅ Режим: списання")],
    ]

    if is_owner:
        keyboard.extend([
            [KeyboardButton(text="📊 Статистика кав’ярні")],
            [KeyboardButton(text="📣 Зробити розсилку")],
            [KeyboardButton(text="👤 Список адміністраторів")],
            [KeyboardButton(text="➕ Додати адміністратора")],
            [KeyboardButton(text="➖ Видалити адміністратора")],
            [KeyboardButton(text="💳 Підписка")],
        ])

    if is_super_admin:
        keyboard.extend([
            [KeyboardButton(text="📊 Загальна статистика")],
            [KeyboardButton(text="🏪 Список кав’ярень")],
            [KeyboardButton(text="➕ Додати кав’ярню")],
            [KeyboardButton(text="🗑 Видалити кав’ярню")],
            [KeyboardButton(text="💳 Продовжити підписку")],
        ])

    if can_switch_to_owner:
        keyboard.append([KeyboardButton(text="👑 Режим owner")])

    if can_switch_to_super_admin:
        keyboard.append([KeyboardButton(text="🛠 Режим super admin")])

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True
    )


def shops_inline_keyboard(shops):
    buttons = []
    for shop in shops:
        title = shop["name"]
        if shop.get("city"):
            title = f"{shop['name']} ({shop['city']})"

        buttons.append([
            InlineKeyboardButton(
                text=title,
                callback_data=f"select_shop:{shop['id']}"
            )
        ])

    return InlineKeyboardMarkup(inline_keyboard=buttons)
