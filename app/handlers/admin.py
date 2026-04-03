from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext

from app.db import (
    ensure_user,
    get_admin_shop_and_role,
    get_user_by_qr_token,
    add_cups_for_shop_client,
    redeem_free_for_shop_client,
    subscription_is_active,
    get_user_by_telegram_id,
    get_shop_client_balance_by_user_id,
)
from app.states import AdminScanStates


router = Router()

admin_modes: dict[int, str] = {}
pending_scan: dict[int, dict] = {}


def is_staff(telegram_user_id: int):
    return get_admin_shop_and_role(telegram_user_id) is not None


@router.message(F.text == "☕ Режим: нарахування")
async def enable_add_mode(message: types.Message):
    ensure_user(
        telegram_user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )

    if not is_staff(message.from_user.id):
        return

    shop = get_admin_shop_and_role(message.from_user.id)
    if not subscription_is_active(shop["id"]):
        await message.answer(
            "❌ Підписка кав’ярні неактивна.\n"
            "Нарахування недоступне."
        )
        return

    admin_modes[message.from_user.id] = "add"
    await message.answer(
        f"☕ Режим нарахування увімкнено\n"
        f"🏪 {shop['name']}\n\n"
        f"Відскануй QR клієнта, потім введи кількість чашок."
    )


@router.message(F.text == "✅ Режим: списання")
async def enable_redeem_mode(message: types.Message):
    ensure_user(
        telegram_user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )

    if not is_staff(message.from_user.id):
        return

    shop = get_admin_shop_and_role(message.from_user.id)
    if not subscription_is_active(shop["id"]):
        await message.answer(
            "❌ Підписка кав’ярні неактивна.\n"
            "Списання недоступне."
        )
        return

    admin_modes[message.from_user.id] = "redeem"
    await message.answer(
        f"✅ Режим списання увімкнено\n"
        f"🏪 {shop['name']}\n\n"
        f"Відскануй QR клієнта."
    )


@router.message(F.web_app_data)
async def handle_scanner_data(message: types.Message, state: FSMContext):
    ensure_user(
        telegram_user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )

    if not is_staff(message.from_user.id):
        return

    mode = admin_modes.get(message.from_user.id)
    if mode not in ("add", "redeem"):
        await message.answer("Спочатку обери режим: нарахування або списання.")
        return

    data = message.web_app_data.data or ""
    if not data.startswith("coffee:"):
        await message.answer("❌ Це не QR нашої системи.")
        return

    token = data.split("coffee:", 1)[1]
    user = get_user_by_qr_token(token)
    if not user:
        await message.answer("❌ Клієнта не знайдено.")
        return

    shop = get_admin_shop_and_role(message.from_user.id)
    admin_user = get_user_by_telegram_id(message.from_user.id)

    current_balance = get_shop_client_balance_by_user_id(
        shop_id=shop["id"],
        user_id=user["id"],
    )

    current_cups = current_balance["cups"] if current_balance else 0
    current_free = current_balance["free_coffee_balance"] if current_balance else 0

    if mode == "add":
        pending_scan[message.from_user.id] = {
            "shop_id": shop["id"],
            "client_user_id": user["id"],
            "client_telegram_user_id": user["telegram_user_id"],
            "client_name": user["full_name"] or "Клієнт",
            "admin_user_id": admin_user["id"],
            "shop_name": shop["name"],
        }

        await state.set_state(AdminScanStates.waiting_cups_count)
        await message.answer(
            f"✅ QR відскановано\n"
            f"🏪 {shop['name']}\n"
            f"👤 {user['full_name'] or user['telegram_user_id']}\n\n"
            f"☕ Зараз чашок: {current_cups}/7\n"
            f"🎁 Безкоштовних кав: {current_free}\n\n"
            f"Введи кількість чашок числом, наприклад: 1, 2, 3"
        )
        return

    if mode == "redeem":
        result = redeem_free_for_shop_client(
            shop_id=shop["id"],
            client_user_id=user["id"],
            admin_user_id=admin_user["id"],
        )

        if result == "NOT_FOUND":
            await message.answer(
                "❌ У цій кав’ярні в клієнта ще немає бонусного рахунку."
            )
            return

        if result == "EMPTY":
            await message.answer(
                f"❌ У клієнта немає безкоштовної кави для списання.\n\n"
                f"☕ Чашок: {current_cups}/7\n"
                f"🎁 Безкоштовних кав: {current_free}"
            )
            return

        await message.answer(
            f"✅ Безкоштовну каву списано\n"
            f"🏪 {shop['name']}\n"
            f"☕ Чашок: {result['cups']}/7\n"
            f"🎁 Залишок безкоштовних кав: {result['free_coffee_balance']}"
        )

        try:
            await message.bot.send_message(
                user["telegram_user_id"],
                f"✅ У тебе списали 1 безкоштовну каву\n"
                f"🏪 {shop['name']}\n"
                f"☕ Чашок: {result['cups']}/7\n"
                f"🎁 Залишок безкоштовних кав: {result['free_coffee_balance']}"
            )
        except Exception:
            pass


@router.message(AdminScanStates.waiting_cups_count)
async def handle_cups_count(message: types.Message, state: FSMContext):
    if message.from_user.id not in pending_scan:
        await state.clear()
        await message.answer("❌ Сесія сканування не знайдена.")
        return

    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("❌ Введи число, наприклад 1, 2 або 3.")
        return

    count = int(text)
    if count <= 0 or count > 20:
        await message.answer("❌ Можна ввести від 1 до 20 чашок.")
        return

    data = pending_scan.pop(message.from_user.id)
    await state.clear()

    result = add_cups_for_shop_client(
        shop_id=data["shop_id"],
        client_user_id=data["client_user_id"],
        admin_user_id=data["admin_user_id"],
        count=count,
    )

    shop_client = result["shop_client"]
    earned_free = result["earned_free"]
    return_source = result.get("return_source")

    answer_text = (
        f"✅ Нараховано {count} чашок\n"
        f"🏪 {data['shop_name']}\n"
        f"☕ Зараз: {shop_client['cups']}/7\n"
        f"🎁 Безкоштовних кав: {shop_client['free_coffee_balance']}"
    )

    if earned_free > 0:
        answer_text += f"\n✨ Нових безкоштовних кав: {earned_free}"

    if return_source == "auto":
        answer_text += "\n📈 Повернення після автонагадування"
    elif return_source == "broadcast":
        answer_text += "\n📈 Повернення після розсилки"

    await message.answer(answer_text)

    try:
        notify_text = (
            f"☕ Тобі нарахували {count} чашок\n"
            f"🏪 {data['shop_name']}\n"
            f"☕ Зараз: {shop_client['cups']}/7\n"
            f"🎁 Безкоштовних кав: {shop_client['free_coffee_balance']}"
        )

        if earned_free > 0:
            notify_text += f"\n✨ Додано безкоштовних кав: {earned_free}"

        await message.bot.send_message(
            data["client_telegram_user_id"],
            notify_text,
        )
    except Exception:
        pass
