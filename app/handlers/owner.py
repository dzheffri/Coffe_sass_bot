from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

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
    log_broadcast_touches,
)
from app.states import OwnerStates
from app.keyboards import admin_main_keyboard
from app.config import SCANNER_URL, SUPER_ADMIN_IDS


router = Router()

# Якщо хочеш для якоїсь конкретної кав'ярні прибрати ліміт розсилок,
# вкажи її ID. Якщо не потрібно — залиш як є.
MY_SHOP_ID = 1


def broadcast_cancel_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="❌ Скасувати розсилку")]
        ],
        resize_keyboard=True
    )


def broadcast_preview_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Підтвердити", callback_data="broadcast_confirm"),
                InlineKeyboardButton(text="❌ Скасувати", callback_data="broadcast_cancel"),
            ]
        ]
    )


def owner_main_keyboard_for_user(user_id: int):
    is_super_admin = user_id in SUPER_ADMIN_IDS
    return admin_main_keyboard(
        scanner_url=SCANNER_URL,
        is_owner=True,
        is_super_admin=False,
        can_switch_to_owner=False,
        can_switch_to_super_admin=is_super_admin,
    )


@router.message(F.text == "📊 Статистика кав’ярні")
async def shop_stats_handler(message: types.Message):
    if not is_owner(message.from_user.id):
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    if not admin_shop:
        return

    stats = get_shop_detailed_stats(admin_shop["id"])

    text = (
        f"📊 Ефективність за 30 днів\n"
        f"🏪 {admin_shop['name']}\n\n"
        f"📩 Відправлено повідомлень: {stats['sent_total']}\n"
        f"├ ⚙️ Автоматичні: {stats['sent_auto']}\n"
        f"└ 📣 Власні розсилки: {stats['sent_broadcast']}\n\n"
        f"💰 Клієнти повернулися після повідомлень: {stats['returns_total']}\n"
        f"├ ⚙️ Автоматичні: {stats['returns_auto']}\n"
        f"└ 📣 Власні розсилки: {stats['returns_broadcast']}\n\n"
        f"📈 Ефективність: {stats['efficiency_percent']}%\n"
        f"🔁 Клієнти повернулися сьогодні: {stats['returned_today']}\n"
        f"🔁 Клієнти повернулися за 7 днів: {stats['returned_7d']}\n"
        f"👥 Клієнти не приходили понад 7 днів: {stats['inactive_gt_7d']}\n"
        f"👥 Активні клієнти (30 днів): {stats['active_clients_30d']}\n"
        f"📅 Сканувань за 30 днів: {stats['scans_30d']}\n"
        f"📅 Сьогодні: {stats['scans_today']}\n"
        f"☕ Нарахувань: {stats['total_scans']}\n"
        f"🎁 Безкоштовних зараз: {stats['free_balance']}\n"
        f"✅ Видано безкоштовних: {stats['total_free_redeemed']}\n"
        f"👤 Всього клієнтів: {stats['total_clients']}\n"
        f"🆕 Нових за 30 днів: {stats['new_clients_30d']}"
    )

    await message.answer(text)


@router.message(F.text == "➕ Додати адміністратора")
async def add_admin_start(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return

    await state.set_state(OwnerStates.waiting_add_admin_id)
    await message.answer("Надішли Telegram ID нового адміністратора.")


@router.message(OwnerStates.waiting_add_admin_id)
async def add_admin_finish(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("❌ Telegram ID має бути числом.")
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    if not admin_shop:
        await state.clear()
        await message.answer(
            "❌ Не вдалося визначити кав’ярню.",
            reply_markup=owner_main_keyboard_for_user(message.from_user.id)
        )
        return

    result = add_shop_admin(admin_shop["id"], int(text), "admin")
    await state.clear()

    if not result:
        await message.answer("❌ Користувач ще не заходив у бота через /start.")
        return

    await message.answer(
        "✅ Адміністратора додано.",
        reply_markup=owner_main_keyboard_for_user(message.from_user.id)
    )


@router.message(F.text == "➖ Видалити адміністратора")
async def remove_admin_start(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return

    await state.set_state(OwnerStates.waiting_remove_admin_id)
    await message.answer("Надішли Telegram ID адміністратора, якого треба видалити.")


@router.message(OwnerStates.waiting_remove_admin_id)
async def remove_admin_finish(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("❌ Telegram ID має бути числом.")
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    if not admin_shop:
        await state.clear()
        await message.answer(
            "❌ Не вдалося визначити кав’ярню.",
            reply_markup=owner_main_keyboard_for_user(message.from_user.id)
        )
        return

    ok = remove_shop_admin(admin_shop["id"], int(text))
    await state.clear()

    if not ok:
        await message.answer("❌ Адміністратора не знайдено.")
        return

    await message.answer(
        "✅ Адміністратора видалено.",
        reply_markup=owner_main_keyboard_for_user(message.from_user.id)
    )


@router.message(F.text == "👤 Список адміністраторів")
async def admins_handler(message: types.Message):
    if not is_owner(message.from_user.id):
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    if not admin_shop:
        return

    admins = get_shop_admins(admin_shop["id"])
    if not admins:
        await message.answer("Список адміністраторів порожній.")
        return

    lines = [f"👤 {admin_shop['name']}", "", "Адміністратори:"]
    for item in admins:
        name = item["full_name"] or item["username"] or "-"
        role_name = "owner" if item["role"] == "owner" else "admin"
        lines.append(f"• {item['telegram_user_id']} | {role_name} | {name}")

    await message.answer("\n".join(lines))


@router.message(F.text == "💳 Підписка")
async def subscription_handler(message: types.Message):
    if not is_owner(message.from_user.id):
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    if not admin_shop:
        return

    sub = get_subscription(admin_shop["id"])
    if not sub:
        await message.answer("❌ Підписку не знайдено.")
        return

    await message.answer(
        f"💳 {admin_shop['name']}\n"
        f"📦 План: {sub['plan']}\n"
        f"📌 Статус: {sub['status']}\n"
        f"⏳ Діє до: {sub['expires_at']}\n\n"
        f"Щоб продовжити підписку, напиши адміну сервісу."
    )


@router.message(F.text == "📣 Зробити розсилку")
async def broadcast_start_handler(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    if not admin_shop:
        await message.answer("❌ Не вдалося визначити кав’ярню.")
        return

    if admin_shop["id"] != MY_SHOP_ID and not can_send_broadcast(admin_shop["id"]):
        await message.answer(
            "❌ Ліміт розсилок вичерпано.\n"
            "Дозволено максимум 4 промо-розсилки за 7 днів на кав’ярню."
        )
        return

    await state.set_state(OwnerStates.waiting_broadcast_text)
    await message.answer(
        "Надішли текст, фото або відео для розсилки.\n\n"
        "Можна:\n"
        "• просто текст\n"
        "• фото з текстом або без тексту\n"
        "• відео з текстом або без тексту\n\n"
        "Спочатку я покажу передперегляд, і тільки після підтвердження розсилка піде клієнтам.\n\n"
        "Щоб скасувати, натисни кнопку нижче.",
        reply_markup=broadcast_cancel_keyboard()
    )


@router.message(OwnerStates.waiting_broadcast_text, F.text == "❌ Скасувати розсилку")
async def broadcast_cancel_handler(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "✅ Розсилку скасовано.",
        reply_markup=owner_main_keyboard_for_user(message.from_user.id)
    )


@router.message(OwnerStates.waiting_broadcast_text)
async def broadcast_preview_handler(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await state.clear()
        await message.answer(
            "❌ Доступ заборонено.",
            reply_markup=owner_main_keyboard_for_user(message.from_user.id)
        )
        return

    admin_shop = get_admin_shop_and_role(message.from_user.id)
    if not admin_shop:
        await state.clear()
        await message.answer(
            "❌ Не вдалося визначити кав’ярню.",
            reply_markup=owner_main_keyboard_for_user(message.from_user.id)
        )
        return

    text = (message.text or message.caption or "").strip()
    photo_id = message.photo[-1].file_id if message.photo else None
    video_id = message.video.file_id if message.video else None

    if not text and not photo_id and not video_id:
        await message.answer("❌ Надішли текст, фото або відео.")
        return

    await state.update_data(
        broadcast_text=text,
        broadcast_photo=photo_id,
        broadcast_video=video_id,
    )

    preview_caption = f"📣 Передперегляд розсилки\n\n🏪 {admin_shop['name']}"
    if text:
        preview_caption += f"\n\n{text}"

    if photo_id:
        await message.answer_photo(
            photo=photo_id,
            caption=preview_caption,
            reply_markup=broadcast_preview_keyboard()
        )
    elif video_id:
        await message.answer_video(
            video=video_id,
            caption=preview_caption,
            reply_markup=broadcast_preview_keyboard()
        )
    else:
        await message.answer(
            preview_caption,
            reply_markup=broadcast_preview_keyboard()
        )


@router.callback_query(F.data == "broadcast_cancel")
async def broadcast_cancel_callback(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.message.answer(
        "✅ Розсилку скасовано.",
        reply_markup=owner_main_keyboard_for_user(callback.from_user.id)
    )
    await callback.answer()


@router.callback_query(F.data == "broadcast_confirm")
async def broadcast_confirm_callback(callback: types.CallbackQuery, state: FSMContext):
    if not is_owner(callback.from_user.id):
        await state.clear()
        await callback.answer("❌ Доступ заборонено.", show_alert=True)
        return

    data = await state.get_data()
    text = (data.get("broadcast_text") or "").strip()
    photo_id = data.get("broadcast_photo")
    video_id = data.get("broadcast_video")

    admin_shop = get_admin_shop_and_role(callback.from_user.id)
    if not admin_shop:
        await state.clear()
        await callback.answer("❌ Не вдалося визначити кав’ярню.", show_alert=True)
        return

    recipients = get_broadcast_recipients(admin_shop["id"])
    if not recipients:
        await state.clear()

        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

        await callback.message.answer(
            "❌ Немає активних клієнтів для розсилки.",
            reply_markup=owner_main_keyboard_for_user(callback.from_user.id)
        )
        await callback.answer()
        return

    await callback.answer("⏳ Запускаю розсилку...")

    sent = 0
    failed = 0
    touched_user_ids = []

    for row in recipients:
        try:
            final_caption = f"🏪 {admin_shop['name']}"
            if text:
                final_caption += f"\n\n{text}"

            if photo_id:
                await callback.bot.send_photo(
                    chat_id=row["telegram_user_id"],
                    photo=photo_id,
                    caption=final_caption
                )
            elif video_id:
                await callback.bot.send_video(
                    chat_id=row["telegram_user_id"],
                    video=video_id,
                    caption=final_caption
                )
            else:
                await callback.bot.send_message(
                    chat_id=row["telegram_user_id"],
                    text=final_caption
                )

            sent += 1
            touched_user_ids.append(row["user_id"])

        except Exception as e:
            print(f"[broadcast] failed for {row['telegram_user_id']}: {e}")
            failed += 1

    save_broadcast(
        admin_shop["id"],
        callback.from_user.id,
        text if text else "[media only]",
        sent
    )

    if touched_user_ids:
        log_broadcast_touches(admin_shop["id"], touched_user_ids)

    await state.clear()

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.message.answer(
        f"✅ Розсилку завершено\n"
        f"📩 Відправлено: {sent}\n"
        f"❌ Помилок: {failed}",
        reply_markup=owner_main_keyboard_for_user(callback.from_user.id)
    )
