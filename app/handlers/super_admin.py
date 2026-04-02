import json
import tempfile
from datetime import datetime

from aiogram import Router, types, F
from aiogram.types import FSInputFile

from app.config import SUPER_ADMIN_IDS, SUBSCRIPTION_PRICE_USD
from app.db import (
    create_shop,
    get_all_shops,
    extend_subscription,
    add_shop_admin,
    get_user_by_telegram_id,
    get_global_stats,
    delete_shop,
    get_all_users,
    get_all_shop_admins,
    get_all_shop_clients,
    get_all_transactions,
    get_all_subscriptions,
    get_all_broadcasts,
    get_all_reminder_logs,
)

router = Router()

pending_shop_data: dict[int, bool] = {}
pending_delete_shop: dict[int, bool] = {}


def is_super_admin(user_id: int):
    return user_id in SUPER_ADMIN_IDS


@router.message(F.text == " Вся система")
async def global_stats_handler(message: types.Message):
    if not is_super_admin(message.from_user.id):
        return

    stats = get_global_stats()
    revenue = stats["active_subscriptions"] * SUBSCRIPTION_PRICE_USD

    await message.answer(
        " Вся система\n\n"
        f" Кавʼярень: {stats['shops_count']}\n"
        f" Користувачів: {stats['users_count']}\n"
        f"☕ Всього нарахувань: {stats['total_scans']}\n"
        f" Безкоштовних зараз: {stats['free_balance']}\n"
        f"✅ Списано бонусів: {stats['total_free_redeemed']}\n\n"
        f" Активних підписок: {stats['active_subscriptions']}\n"
        f"❌ Прострочених: {stats['expired_subscriptions']}\n"
        f" Поточний дохід: ${revenue:.2f}"
    )


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


@router.message(F.text == " Додати кав’ярню")
async def add_shop_start(message: types.Message):
    if not is_super_admin(message.from_user.id):
        return

    pending_shop_data[message.from_user.id] = True
    pending_delete_shop.pop(message.from_user.id, None)

    await message.answer(
        "Надішли дані нової кав’ярні одним повідомленням у форматі:\n\n"
        "Назва | Місто | Адреса | Telegram_ID_власника\n\n"
        "Приклад:\n"
        "Coffee A | Київ | Хрещатик 1 | 123456789"
    )


@router.message(F.text == " Видалити кав’ярню")
async def delete_shop_start(message: types.Message):
    if not is_super_admin(message.from_user.id):
        return

    pending_delete_shop[message.from_user.id] = True
    pending_shop_data.pop(message.from_user.id, None)

    shops = get_all_shops()
    if not shops:
        pending_delete_shop.pop(message.from_user.id, None)
        await message.answer("Список кав’ярень порожній.")
        return

    text = [" Введи ID кав’ярні, яку треба видалити.", "", " Список кав’ярень:"]
    for shop in shops:
        text.append(f"• ID {shop['id']} — {shop['name']} ({shop['city'] or '-'})")

    await message.answer("\n".join(text))


@router.message(
    lambda m: m.from_user.id in SUPER_ADMIN_IDS and m.from_user.id in pending_shop_data and "|" in (m.text or "")
)
async def add_shop_finish(message: types.Message):
    parts = [x.strip() for x in (message.text or "").split("|")]
    if len(parts) != 4:
        await message.answer("❌ Формат неправильний.")
        return

    name, city, address, owner_id = parts

    if not owner_id.isdigit():
        await message.answer("❌ Telegram ID власника має бути числом.")
        return

    owner_telegram_id = int(owner_id)

    shop = create_shop(
        name=name,
        city=city,
        address=address,
        pending_owner_telegram_id=owner_telegram_id
    )

    owner_user = get_user_by_telegram_id(owner_telegram_id)
    pending_shop_data.pop(message.from_user.id, None)

    if owner_user:
        add_shop_admin(shop["id"], owner_telegram_id, "owner")
        await message.answer(
            f"✅ Кав’ярню створено\n"
            f" {shop['name']}\n"
            f" Owner призначено одразу"
        )
        return

    await message.answer(
        f"✅ Кав’ярню створено: {shop['name']}\n"
        f"⏳ Власник ще не заходив у бота.\n"
        f"Після його /start роль owner буде видано автоматично."
    )


@router.message(
    lambda m: m.from_user.id in SUPER_ADMIN_IDS and m.from_user.id in pending_delete_shop
)
async def delete_shop_finish(message: types.Message):
    text = (message.text or "").strip()

    if not text.isdigit():
        await message.answer("❌ Надішли числовий ID кав’ярні.")
        return

    shop_id = int(text)
    deleted_shop = delete_shop(shop_id)
    pending_delete_shop.pop(message.from_user.id, None)

    if not deleted_shop:
        await message.answer("❌ Кав’ярню з таким ID не знайдено.")
        return

    await message.answer(
        f"✅ Кав’ярню видалено\n"
        f" {deleted_shop['name']}\n"
        f"ID: {deleted_shop['id']}"
    )


@router.message(F.text == " Список кав’ярень")
async def shops_list_handler(message: types.Message):
    if not is_super_admin(message.from_user.id):
        return

    shops = get_all_shops()
    if not shops:
        await message.answer("Список кав’ярень порожній.")
        return

    text = [" Список кав’ярень:"]
    for shop in shops:
        text.append(f"• ID {shop['id']} — {shop['name']} ({shop['city'] or '-'})")

    await message.answer("\n".join(text))


@router.message(F.text.startswith("/extend_shop "))
async def extend_shop_handler(message: types.Message):
    if not is_super_admin(message.from_user.id):
        return

    parts = message.text.split()
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await message.answer("Формат: /extend_shop SHOP_ID DAYS")
        return

    shop_id = int(parts[1])
    days = int(parts[2])

    sub = extend_subscription(shop_id, days)

    await message.answer(
        f"✅ Підписку продовжено\n"
        f"SHOP_ID: {shop_id}\n"
        f"План: {sub['plan']}\n"
        f"Діє до: {sub['expires_at']}"
    )


@router.message(F.text == " Продовжити підписку")
async def extend_hint(message: types.Message):
    if not is_super_admin(message.from_user.id):
        return

    await message.answer(
        "Використай команду:\n"
        "/extend_shop SHOP_ID DAYS\n\n"
        "Приклад:\n"
        "/extend_shop 3 30"
    )
