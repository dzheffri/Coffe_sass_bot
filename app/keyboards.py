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
            [KeyboardButton(text="🏪 Мої кав’ярні")],
            [KeyboardButton(text="☕ Мої чашки"), KeyboardButton(text="🎁 Мої безкоштовні кави")],
            [KeyboardButton(text="🔄 Змінити кав’ярню")],
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
            KeyboardButton(text="📷 Режим: нарахування"),
            KeyboardButton(text="✅ Режим: списання")
        ],
        [
            KeyboardButton(text="📱 Відкрити сканер", web_app=WebAppInfo(url=scanner_url))
        ],
    ]

    if is_owner:
        keyboard.extend([
            [KeyboardButton(text="📊 Статистика кав’ярні")],
            [
                KeyboardButton(text="➕ Додати адміністратора"),
                KeyboardButton(text="➖ Видалити адміністратора")
            ],
            [
                KeyboardButton(text="👤 Список адміністраторів"),
                KeyboardButton(text="📣 Зробити розсилку")
            ],
            [KeyboardButton(text="💳 Підписка")],
        ])

   if is_super_admin:
    keyboard.extend([
        [KeyboardButton(text="🌍 Вся система")],
        [
            KeyboardButton(text="🏪 Додати кав’ярню"),
            KeyboardButton(text="🏪 Список кав’ярень")
        ],
        [KeyboardButton(text="🗑 Видалити кав’ярню")],
        [KeyboardButton(text="💳 Продовжити підписку")],
    ])

    if can_switch_to_owner:
        keyboard.append([KeyboardButton(text="🏪 Режим owner")])

    if can_switch_to_super_admin:
        keyboard.append([KeyboardButton(text="👑 Режим super admin")])

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True
    )


def shops_inline_keyboard(shops: list[dict]):
    rows = []
    for shop in shops:
        rows.append([
            InlineKeyboardButton(
                text=f"{shop['name']} • ☕ {shop['cups']}/7 • 🎁 {shop['free_coffee_balance']}",
                callback_data=f"select_shop:{shop['id']}"
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)
