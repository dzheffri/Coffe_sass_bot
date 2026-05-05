import json
import tempfile
from datetime import datetime

from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile

from app.config import SUPER_ADMIN_IDS
from app.db import (
    create_shop,
    get_all_shops,
    get_all_shops_with_subscriptions,
    delete_shop,
    extend_subscription,
    get_global_stats,
    get_super_admin_clients_stats,
    get_all_users,
    get_all_shop_admins,
    get_all_shop_clients,
    get_all_transactions,
    get_all_subscriptions,
    get_all_broadcasts,
    get_all_reminder_logs,
)
from app.states import SuperAdminStates


router = Router()


def is_super_admin(user_id: int) -> bool:
    return user_id in SUPER_ADMIN_IDS


@router.message(F.text == "📊 Загальна статистика")
async def global_stats_handler(message: types.Message):
    if not is_super_admin(message.from_user.id):
        return

    stats = get_global_stats()
    clients_stats = get_super_admin_clients_stats()

    per_shop_lines = []
    for item in clients_stats["per_shop"]:
        per_shop_lines.append(
            f"• {item['shop_name']} — {item['clients_count']} клієнтів"
        )

    shops_block = "\n".join(per_shop_lines) if per_shop_lines else "• Немає кав’ярень"

    text = (
        f"📊 Загальна статистика\n\n"
        f"🏪 Всього кав’ярень: {stats['shops_count']}\n"
        f"👤 Всього користувачів у боті: {stats['users_count']}\n"
        f"👥 Всього клієнтів по всіх кав’ярнях: {clients_stats['total_clients']}\n\n"
        f"☕ Всього нарахувань: {stats['total_scans']}\n"
        f"🎁 Безкоштовних зараз: {stats['free_balance']}\n"
        f"✅ Видано безкоштовних: {stats['total_free_redeemed']}\n\n"
        f"💳 Активних підписок: {stats['active_subscriptions']}\n"
        f"⛔ Прострочених / неактивних: {stats['expired_subscriptions']}\n\n"
        f"👥 Клієнти по кав’ярнях:\n{shops_block}"
    )

    await message.answer(text)


@router.message(F.text == "💾 Backup системи")
async def backup_system_handler(message: types.Message):
    if not is_super_admin(message.from_user.id):
        return

    await message.answer("⏳ Створюю backup системи...")

    backup_data = {
        "meta": {
            "created_at": datetime.utcnow().isoformat() + "Z",
            "type": "full_system_backup",
            "format": "json",
        },
        "users": get_all_users(),
        "coffee_shops": get_all_shops(),
        "shop_admins": get_all_shop_admins(),
        "shop_clients": get_all_shop_clients(),
        "transactions": get_all_transactions(),
        "subscriptions": get_all_subscriptions(),
        "broadcasts": get_all_broadcasts(),
        "reminder_logs": get_all_reminder_logs(),
    }

    filename = f"backup_system_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}.json"

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(backup_data, tmp, ensure_ascii=False, indent=2, default=str)
        tmp_path = tmp.name

    document = FSInputFile(tmp_path, filename=filename)

    await message.answer_document(
        document=document,
        caption="✅ Backup системи готовий"
    )


@router.message(F.text.in_(["🏪 Список кав’ярень", " Список кав’ярень", "Список кав’ярень"]))
async def list_shops_handler(message: types.Message):
    if not is_super_admin(message.from_user.id):
        return

    shops = get_all_shops_with_subscriptions()

    if not shops:
        await message.answer("Список кав’ярень порожній.")
        return

    lines = [" Список кав’ярень:", ""]

    for shop in shops:
        city = shop["city"] or "-"
        address = shop["address"] or "-"

        plan = shop["plan"] or "-"
        status = shop["subscription_status"] or "немає"
        expires_at = shop["expires_at"]

        if shop["days_left"] is None:
            days_text = "підписку не знайдено"
        elif shop["days_left"] < 0:
            days_text = f"прострочено на {abs(shop['days_left'])} дн."
        elif shop["days_left"] == 0:
            days_text = "закінчується сьогодні"
        elif shop["days_left"] == 1:
            days_text = "залишився 1 день"
        else:
            days_text = f"залишилось {shop['days_left']} дн."

        lines.append(
            f"ID: {shop['id']}\n"
            f"Назва: {shop['name']}\n"
            f"Місто: {city}\n"
            f"Адреса: {address}\n"
            f"Підписка: {plan} | {status}\n"
            f"До кінця: {days_text}\n"
            f"Діє до: {expires_at}\n"
        )

    await message.answer("\n".join(lines))


@router.message(F.text == "➕ Додати кав’ярню")
async def add_shop_start(message: types.Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        return

    await state.set_state(SuperAdminStates.waiting_shop_name)
    await message.answer("Надішли назву кав’ярні.")


@router.message(SuperAdminStates.waiting_shop_name)
async def add_shop_name(message: types.Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Назва не може бути порожньою.")
        return

    await state.update_data(shop_name=text)
    await state.set_state(SuperAdminStates.waiting_shop_city)
    await message.answer("Тепер надішли місто.")


@router.message(SuperAdminStates.waiting_shop_city)
async def add_shop_city(message: types.Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Місто не може бути порожнім.")
        return

    await state.update_data(shop_city=text)
    await state.set_state(SuperAdminStates.waiting_shop_address)
    await message.answer("Тепер надішли адресу.")


@router.message(SuperAdminStates.waiting_shop_address)
async def add_shop_address(message: types.Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Адреса не може бути порожньою.")
        return

    await state.update_data(shop_address=text)
    await state.set_state(SuperAdminStates.waiting_shop_owner_id)
    await message.answer(
        "Тепер надішли Telegram ID власника.\n\n"
        "Важливо: власник має хоча б один раз зайти в бота через /start."
    )


@router.message(SuperAdminStates.waiting_shop_owner_id)
async def add_shop_finish(message: types.Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("❌ Telegram ID має бути числом.")
        return

    data = await state.get_data()

    shop = create_shop(
        name=data["shop_name"],
        city=data["shop_city"],
        address=data["shop_address"],
        pending_owner_telegram_id=int(text),
    )

    await state.clear()

    await message.answer(
        f"✅ Кав’ярню створено\n\n"
        f"ID: {shop['id']}\n"
        f"Назва: {shop['name']}\n"
        f"Місто: {shop['city']}\n"
        f"Адреса: {shop['address']}\n\n"
        f"Власник буде прив’язаний автоматично, коли зайде в бота."
    )


@router.message(F.text == "🗑 Видалити кав’ярню")
async def delete_shop_start(message: types.Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        return

    await state.set_state(SuperAdminStates.waiting_delete_shop_id)
    await message.answer("Надішли ID кав’ярні, яку треба видалити.")


@router.message(SuperAdminStates.waiting_delete_shop_id)
async def delete_shop_finish(message: types.Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("❌ ID має бути числом.")
        return

    deleted = delete_shop(int(text))
    await state.clear()

    if not deleted:
        await message.answer("❌ Кав’ярню не знайдено.")
        return

    await message.answer(
        f"✅ Кав’ярню видалено:\n{deleted['name']}"
    )


@router.message(F.text == "💳 Продовжити підписку")
async def extend_sub_start(message: types.Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        return

    await state.set_state(SuperAdminStates.waiting_extend_shop_id)
    await message.answer(
        "Надішли ID кав’ярні, якій потрібно продовжити підписку."
    )


@router.message(SuperAdminStates.waiting_extend_shop_id)
async def extend_sub_shop_id(message: types.Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("❌ ID має бути числом.")
        return

    await state.update_data(shop_id=int(text))
    await state.set_state(SuperAdminStates.waiting_extend_days)
    await message.answer("На скільки днів продовжити підписку?")


@router.message(SuperAdminStates.waiting_extend_days)
async def extend_sub_days(message: types.Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("❌ Кількість днів має бути числом.")
        return

    days = int(text)
    if days <= 0:
        await message.answer("❌ Кількість днів має бути більшою за 0.")
        return

    data = await state.get_data()
    shop_id = data["shop_id"]

    sub = extend_subscription(shop_id=shop_id, days=days, plan="basic")
    await state.clear()

    await message.answer(
        f"✅ Підписку продовжено\n\n"
        f"🏪 shop_id: {shop_id}\n"
        f"📦 План: {sub['plan']}\n"
        f"📌 Статус: {sub['status']}\n"
        f"⏳ Діє до: {sub['expires_at']}"
    )
    @router.message(F.text == "📢 Розсилка всім користувачам")
async def global_broadcast_start(message: types.Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        return

    await state.set_state(SuperAdminStates.waiting_global_broadcast_text)
    await message.answer(
        "📢 Надішли текст розсилки для всіх користувачів бота.\n\n"
        "Щоб скасувати — напиши: скасувати"
    )
    
@router.message(SuperAdminStates.waiting_global_broadcast_text)
async def global_broadcast_send(message: types.Message, state: FSMContext):
    if not is_super_admin(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip()

    if text.lower() in ["скасувати", "отмена", "cancel"]:
        await state.clear()
        await message.answer("❌ Розсилку скасовано.")
        return

    users = get_all_users()

    sent = 0
    failed = 0

    await message.answer(f"⏳ Починаю розсилку для {len(users)} користувачів...")

    for user in users:
        telegram_id = user.get("telegram_id")

        if not telegram_id:
            failed += 1
            continue

        try:
            await message.bot.send_message(
                chat_id=int(telegram_id),
                text=text
            )
            sent += 1
        except Exception:
            failed += 1

    await state.clear()

    await message.answer(
        "✅ Розсилку завершено\n\n"
        f"Надіслано: {sent}\n"
        f"Не вдалося: {failed}"
    )
