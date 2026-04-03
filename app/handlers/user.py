import io

import qrcode
from aiogram import Router, types, F
from aiogram.types import BufferedInputFile

from app.db import (
    ensure_user,
    get_user_by_telegram_id,
    get_user_shops,
    get_active_shop_for_user,
    set_active_shop,
    get_shop_client_balance,
)
from app.keyboards import shops_inline_keyboard


router = Router()


@router.message(F.text == "📱 Мій QR-код")
async def my_qr_handler(message: types.Message):
    ensure_user(
        telegram_user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )

    user = get_user_by_telegram_id(message.from_user.id)
    if not user:
        await message.answer("❌ Не вдалося знайти користувача.")
        return

    qr_data = f"coffee:{user['personal_qr_token']}"
    img = qrcode.make(qr_data)

    bio = io.BytesIO()
    img.save(bio, format="PNG")
    bio.seek(0)

    photo = BufferedInputFile(bio.read(), filename="my_qr.png")

    await message.answer_photo(
        photo,
        caption=(
            "Ось твій QR-код ☕\n\n"
            "Покажи його баристі під час покупки."
        )
    )


@router.message(F.text == "🏪 Мої кав’ярні")
async def my_shops_handler(message: types.Message):
    ensure_user(
        telegram_user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )

    shops = get_user_shops(message.from_user.id)
    if not shops:
        await message.answer(
            "У тебе ще немає активних кав’ярень.\n"
            "Після першого сканування в кав’ярні вона з’явиться тут."
        )
        return

    await message.answer(
        "Ось твої кав’ярні.\nОбери потрібну:",
        reply_markup=shops_inline_keyboard(shops)
    )


@router.message(F.text == "🏪 Обрати кав’ярню")
async def change_shop_handler(message: types.Message):
    ensure_user(
        telegram_user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )

    shops = get_user_shops(message.from_user.id)
    if not shops:
        await message.answer("У тебе поки немає кав’ярень для вибору.")
        return

    await message.answer(
        "Обери активну кав’ярню:",
        reply_markup=shops_inline_keyboard(shops)
    )


@router.callback_query(F.data.startswith("select_shop:"))
async def select_shop_callback(callback: types.CallbackQuery):
    shop_id = int(callback.data.split(":")[1])

    set_active_shop(callback.from_user.id, shop_id)

    active_shop = get_active_shop_for_user(callback.from_user.id)
    if not active_shop:
        await callback.message.edit_text("✅ Активну кав’ярню змінено.")
        await callback.answer()
        return

    balance = get_shop_client_balance(active_shop["id"], callback.from_user.id)

    if not balance:
        await callback.message.edit_text(
            f"✅ Активну кав’ярню змінено.\n\n"
            f"🏪 {active_shop['name']}\n"
            f"☕ Чашки: 0/7\n"
            f"🎁 Безкоштовні кави: 0"
        )
        await callback.answer()
        return

    await callback.message.edit_text(
        f"✅ Активну кав’ярню змінено.\n\n"
        f"🏪 {balance['shop_name']}\n"
        f"☕ Чашки: {balance['cups']}/7\n"
        f"🎁 Безкоштовні кави: {balance['free_coffee_balance']}"
    )
    await callback.answer()


@router.message(F.text == "☕ Мої чашки")
async def my_cups_handler(message: types.Message):
    ensure_user(
        telegram_user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )

    active_shop = get_active_shop_for_user(message.from_user.id)
    if not active_shop:
        await message.answer("Спочатку обери кав’ярню через кнопку «🏪 Обрати кав’ярню».")
        return

    balance = get_shop_client_balance(active_shop["id"], message.from_user.id)
    if not balance:
        await message.answer(
            f"У кав’ярні {active_shop['name']} у тебе поки немає чашок."
        )
        return

    await message.answer(
        f"🏪 {balance['shop_name']}\n"
        f"☕ Чашки: {balance['cups']}/7\n"
        f"🎁 Безкоштовні кави: {balance['free_coffee_balance']}"
    )


@router.message(F.text == "🎁 Мої безкоштовні кави")
async def my_free_handler(message: types.Message):
    ensure_user(
        telegram_user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )

    active_shop = get_active_shop_for_user(message.from_user.id)
    if not active_shop:
        await message.answer("Спочатку обери кав’ярню через кнопку «🏪 Обрати кав’ярню».")
        return

    balance = get_shop_client_balance(active_shop["id"], message.from_user.id)
    if not balance:
        await message.answer(
            f"У кав’ярні {active_shop['name']} у тебе поки немає бонусів."
        )
        return

    await message.answer(
        f"🏪 {balance['shop_name']}\n"
        f"🎁 Безкоштовні кави: {balance['free_coffee_balance']}\n"
        f"☕ Чашки: {balance['cups']}/7"
    )
