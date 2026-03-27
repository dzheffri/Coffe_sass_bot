from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext

from app.db import (
    is_owner,
    get_admin_shop_and_role,
    get_shop_detailed_stats,
    get_shop_admins,
    add_shop_admin,
    remove_shop_admin,
    get_subscription,
    can_send_broadcast,
    get_broadcast_recipients,
    save_broadcast,
)
from app.states import OwnerStates

router = Router()


@router.message(F.text == "📊 Статистика кав’ярні")
async def shop_stats_handler(message: types.Message):
    if not is_owner(message.from_user.id):
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    if not admin_shop:
        return

    stats = get_shop_detailed_stats(admin_shop["id"])

    await message.answer(
        f"🏪 {admin_shop['name']}\n\n"
        f"👥 Усього клієнтів: {stats['total_clients']}\n"
        f"🆕 Нових за 30 днів: {stats['new_clients_30d']}\n"
        f"🔥 Активних за 30 днів: {stats['active_clients_30d']}\n"
        f"☕ Всього нарахувань: {stats['total_scans']}\n"
        f"📅 Сьогодні сканувань: {stats['scans_today']}\n"
        f"📆 За 30 днів сканувань: {stats['scans_30d']}\n"
        f"🎁 Безкоштовних зараз: {stats['free_balance']}\n"
        f"🎁 Нараховано всього: {stats['total_free_earned']}\n"
        f"✅ Списано всього: {stats['total_free_redeemed']}"
    )


@router.message(F.text == "➕ Додати адміністратора")
async def add_admin_start(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return

    await state.set_state(OwnerStates.waiting_add_admin_id)
    await message.answer("Надішли Telegram ID нового адміністратора.")


@router.message(OwnerStates.waiting_add_admin_id)
async def add_admin_finish(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return

    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("❌ Telegram ID має бути числом.")
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    result = add_shop_admin(admin_shop["id"], int(text), "admin")

    await state.clear()

    if not result:
        await message.answer("❌ Користувач ще не заходив у бота через /start.")
        return

    await message.answer("✅ Адміністратора додано.")


@router.message(F.text == "➖ Видалити адміністратора")
async def remove_admin_start(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return

    await state.set_state(OwnerStates.waiting_remove_admin_id)
    await message.answer("Надішли Telegram ID адміністратора, якого треба видалити.")


@router.message(OwnerStates.waiting_remove_admin_id)
async def remove_admin_finish(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return

    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("❌ Telegram ID має бути числом.")
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    ok = remove_shop_admin(admin_shop["id"], int(text))

    await state.clear()

    if not ok:
        await message.answer("❌ Адміністратора не знайдено.")
        return

    await message.answer("✅ Адміністратора видалено.")


@router.message(F.text == "👤 Список адміністраторів")
async def admins_handler(message: types.Message):
    if not is_owner(message.from_user.id):
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    admins = get_shop_admins(admin_shop["id"])

    if not admins:
        await message.answer("Список адміністраторів порожній.")
        return

    lines = [f"🏪 {admin_shop['name']}", "", "👤 Адміністратори:"]
    for item in admins:
        name = item["full_name"] or item["username"] or "-"
        lines.append(f"• {item['telegram_user_id']} | {item['role']} | {name}")

    await message.answer("\n".join(lines))


@router.message(F.text == "💳 Підписка")
async def subscription_handler(message: types.Message):
    if not is_owner(message.from_user.id):
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    sub = get_subscription(admin_shop["id"])

    await message.answer(
        f"🏪 {admin_shop['name']}\n"
        f"💳 План: {sub['plan']}\n"
        f"📌 Статус: {sub['status']}\n"
        f"⏳ Діє до: {sub['expires_at']}\n\n"
        f"Щоб продовжити підписку, напиши адміну сервісу."
    )


@router.message(F.text == "📣 Зробити розсилку")
async def broadcast_start_handler(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)

    if not can_send_broadcast(admin_shop["id"]):
        await message.answer(
            "❌ Ліміт розсилок вичерпано.\n"
            "Дозволено максимум 7 промо-розсилок за 7 днів на кав’ярню."
        )
        return

    await state.set_state(OwnerStates.waiting_broadcast_text)
    await message.answer(
        "Надішли текст або фото з підписом для розсилки.\n\n"
        "Розсилка піде тільки активним клієнтам цієї кав’ярні за останні 60 днів."
    )


@router.message(OwnerStates.waiting_broadcast_text)
async def broadcast_send_handler(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return

    text = (message.text or message.caption or "").strip()
    if not text:
        await message.answer("❌ Текст порожній.")
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    recipients = get_broadcast_recipients(admin_shop["id"])

    if not recipients:
        await state.clear()
        await message.answer("❌ Немає активних клієнтів для розсилки.")
        return

    sent = 0
    failed = 0

    for row in recipients:
        try:
            if message.photo:
                await message.bot.send_photo(
                    chat_id=row["telegram_user_id"],
                    photo=message.photo[-1].file_id,
                    caption=f"🏪 {admin_shop['name']}\n\n{text}"
                )
            else:
                await message.bot.send_message(
                    row["telegram_user_id"],
                    f"🏪 {admin_shop['name']}\n\n{text}"
                )
            sent += 1
        except Exception:
            failed += 1

    save_broadcast(admin_shop["id"], message.from_user.id, text, sent)
    await state.clear()

    await message.answer(
        f"✅ Розсилку завершено\n"
        f"📨 Відправлено: {sent}\n"
        f"❌ Помилок: {failed}"
    )
