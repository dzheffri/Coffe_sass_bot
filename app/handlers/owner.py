import logging
import uuid

from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import SimpleEventIsolation
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from app.db import (
    is_owner,
    get_admin_shop_and_role,
    get_admin_shops_for_telegram,
    set_admin_active_shop,
    get_shop,
    get_shop_detailed_stats,
    get_shop_admins,
    add_shop_admin,
    remove_shop_admin,
    get_subscription,
    can_send_broadcast,
    create_admin_login_ticket,
)
from fastapi import HTTPException
from starlette.concurrency import run_in_threadpool

from app.broadcast_publication import publish_bot_broadcast, record_broadcast_delivery
from app.broadcast_delivery import send_broadcast_app_push
from app.states import OwnerStates
from app.keyboards import admin_main_keyboard
from app.config import SCANNER_URL, SUPER_ADMIN_IDS, ADMIN_PANEL_URL
from app.telegram_broadcast_media import download_telegram_broadcast_media


router = Router()
logger = logging.getLogger(__name__)
_broadcast_isolation = SimpleEventIsolation()

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


def broadcast_preview_keyboard(token: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Підтвердити", callback_data=f"broadcast_confirm:{token}"),
                InlineKeyboardButton(text="❌ Скасувати", callback_data=f"broadcast_cancel:{token}"),
            ]
        ]
    )


def _broadcast_media_reference(message):
    if message.photo:
        return {"kind": "photo", "file_id": message.photo[-1].file_id, "filename": "photo.jpg"}
    if message.video:
        return {"kind": "video", "file_id": message.video.file_id,
                "filename": getattr(message.video, "file_name", None) or "video.mp4"}
    # GIFs/documents still use original-message delivery, without a persisted attachment.
    return None


def _broadcast_callback_token(callback, action):
    data = getattr(callback, "data", None)
    prefix = action + ":"
    return data[len(prefix):] if isinstance(data, str) and data.startswith(prefix) else None


def owner_main_keyboard_for_user(user_id: int):
    is_super_admin = user_id in SUPER_ADMIN_IDS

    ticket = create_admin_login_ticket(user_id)

    base_url = ADMIN_PANEL_URL.rstrip("/")
    personal_admin_url = f"{base_url}/?ticket={ticket}"

    return admin_main_keyboard(
        scanner_url=SCANNER_URL,
        is_owner=True,
        is_super_admin=False,
        can_switch_to_owner=False,
        can_switch_to_super_admin=is_super_admin,
        admin_panel_url=personal_admin_url,
        can_select_shop=len(get_admin_shops_for_telegram(user_id)) > 1,
    )


def admin_shop_picker(shops):
    rows = []
    for shop in shops:
        title = shop["name"]
        if shop.get("city"):
            title += f" · {shop['city']}"
        rows.append([InlineKeyboardButton(text=title[:64], callback_data=f"admin_shop_select:{shop['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.message(F.text == "🏪 Обрати кав’ярню")
async def choose_admin_shop_handler(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    shops = get_admin_shops_for_telegram(message.from_user.id)
    if len(shops) <= 1:
        await message.answer("У вас доступна одна кав’ярня.")
        return
    await message.answer(
        "Оберіть точку для наступних дій. Нарахування, списання та shop-розсилки застосовуватимуться лише до неї.",
        reply_markup=admin_shop_picker(shops),
    )


@router.callback_query(F.data.startswith("admin_shop_select:"))
async def choose_admin_shop_callback(callback: types.CallbackQuery, state: FSMContext):
    if not is_owner(callback.from_user.id):
        await callback.answer("Доступ заборонено.", show_alert=True)
        return
    raw_id = (callback.data or "").partition(":")[2]
    if not raw_id.isascii() or not raw_id.isdigit():
        await callback.answer("Невірна кав’ярня.", show_alert=True)
        return
    shop_id = int(raw_id)
    if not set_admin_active_shop(callback.from_user.id, shop_id):
        await callback.answer("Немає доступу до цієї кав’ярні.", show_alert=True)
        return
    await state.clear()
    admin_modes.pop(callback.from_user.id, None)
    pending_scan.pop(callback.from_user.id, None)
    shop = get_admin_shop_and_role(callback.from_user.id)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.answer("Кав’ярню змінено")
    if shop:
        await callback.message.answer(
            f"Активна кав’ярня: {shop['name']}",
            reply_markup=owner_main_keyboard_for_user(callback.from_user.id),
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
    async with _broadcast_isolation.lock(key=state.key):
        if not is_owner(message.from_user.id):
            return
        admin_shop = get_admin_shop_and_role(message.from_user.id)
        if admin_shop and (admin_shop["id"] == MY_SHOP_ID or can_send_broadcast(admin_shop["id"])):
            await state.clear()
            await state.set_state(OwnerStates.waiting_broadcast_text)
            error = None
        elif admin_shop:
            error = "❌ Ліміт розсилок вичерпано.\nДозволено максимум 8 промо-розсилки за 7 днів на кав’ярню."
        else:
            error = "❌ Не вдалося визначити кав’ярню."
    if error:
        await message.answer(error)
        return

    await message.answer(
        "Надішли текст, фото, відео, GIF або документ для розсилки.\n\n"
        "Можна:\n"
        "• просто текст\n"
        "• фото з текстом або без тексту\n"
        "• відео з текстом або без тексту\n"
        "• GIF або документ із підписом чи без нього\n\n"
        "Спочатку я покажу передперегляд, і тільки після підтвердження розсилка піде клієнтам.\n\n"
        "Щоб скасувати, натисни кнопку нижче.",
        reply_markup=broadcast_cancel_keyboard()
    )


@router.message(OwnerStates.waiting_broadcast_text, F.text == "❌ Скасувати розсилку")
async def broadcast_cancel_handler(message: types.Message, state: FSMContext):
    async with _broadcast_isolation.lock(key=state.key):
        if await state.get_state() != OwnerStates.waiting_broadcast_text.state:
            return
        await state.clear()
    await message.answer(
        "✅ Розсилку скасовано.",
        reply_markup=owner_main_keyboard_for_user(message.from_user.id)
    )


@router.message(OwnerStates.waiting_broadcast_text)
async def broadcast_preview_handler(message: types.Message, state: FSMContext):
    async with _broadcast_isolation.lock(key=state.key):
        # The router's raw_state was loaded before acquiring this local lock.
        if await state.get_state() != OwnerStates.waiting_broadcast_text.state:
            return
        if not is_owner(message.from_user.id):
            await state.clear()
            error = "❌ Доступ заборонено."
        else:
            admin_shop = get_admin_shop_and_role(message.from_user.id)
            error = None if admin_shop else "❌ Не вдалося визначити кав’ярню."
            if not admin_shop:
                await state.clear()
        if error is None:
            if not (message.text or message.photo or message.video or message.animation or message.document):
                error = "❌ Надішли текст, фото, відео, GIF або документ."
            else:
                token = uuid.uuid4().hex
                await state.update_data(
                    broadcast_text=(message.text or message.caption or "").strip(),
                    broadcast_from_chat_id=message.chat.id,
                    broadcast_message_id=message.message_id,
                    broadcast_shop_id=admin_shop["id"],
                    broadcast_preview_token=token,
                    broadcast_media_reference=_broadcast_media_reference(message),
                )
    if error:
        await message.answer(error)
        return
    await message.bot.copy_message(
        chat_id=message.chat.id,
        from_chat_id=message.chat.id,
        message_id=message.message_id,
        reply_markup=broadcast_preview_keyboard(token),
    )


@router.callback_query(F.data.startswith("broadcast_cancel:"))
async def broadcast_cancel_callback(callback: types.CallbackQuery, state: FSMContext):
    token = _broadcast_callback_token(callback, "broadcast_cancel")
    if not token:
        return
    async with _broadcast_isolation.lock(key=state.key):
        data = await state.get_data()
        current = (await state.get_state() == OwnerStates.waiting_broadcast_text.state
                   and data.get("broadcast_preview_token") == token)
        if current:
            await state.clear()
    if not current:
        await callback.answer("Цей передперегляд уже неактуальний.")
        return
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.message.answer(
        "✅ Розсилку скасовано.",
        reply_markup=owner_main_keyboard_for_user(callback.from_user.id)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("broadcast_confirm:"))
async def broadcast_confirm_callback(callback: types.CallbackQuery, state: FSMContext):
    token = _broadcast_callback_token(callback, "broadcast_confirm")
    if not token:
        return
    async with _broadcast_isolation.lock(key=state.key):
        data = await state.get_data()
        if (await state.get_state() != OwnerStates.waiting_broadcast_text.state
                or data.get("broadcast_preview_token") != token):
            error = "Цей передперегляд уже неактуальний."
        elif not is_owner(callback.from_user.id):
            await state.clear()
            error = "❌ Доступ заборонено."
        else:
            admin_shop = get_admin_shop_and_role(callback.from_user.id)
            if not admin_shop or admin_shop["id"] != data.get("broadcast_shop_id"):
                await state.clear()
                error = "❌ Кав’ярня змінилася. Почни розсилку ще раз."
            elif data.get("broadcast_from_chat_id") is None or data.get("broadcast_message_id") is None:
                await state.clear()
                error = "❌ Надішли повідомлення для розсилки ще раз."
            else:
                # Detach and consume this preview before any Telegram await. A new
                # preview may safely be created while this publication is sending.
                data = dict(data)
                await state.clear()
                error = None
    if error:
        await callback.answer(error, show_alert=True)
        return

    shop_id = data["broadcast_shop_id"]
    try:
        current_shop = await run_in_threadpool(get_shop, shop_id)
    except Exception as exc:
        logger.warning("Broadcast shop check failed (%s)", type(exc).__name__)
        await callback.answer("❌ Не вдалося перевірити кав’ярню. Почни ще раз.", show_alert=True)
        return
    if not current_shop or not current_shop["is_active"]:
        await callback.answer("❌ Кав’ярню закрито. Розсилка недоступна.", show_alert=True)
        return
    await callback.answer("⏳ Запускаю розсилку...")
    try:
        attachment = await download_telegram_broadcast_media(callback.bot, data.get("broadcast_media_reference"))
    except Exception as exc:
        logger.warning("Broadcast media download failed (%s)", type(exc).__name__)
        attachment = None
    try:
        publication = await run_in_threadpool(
            publish_bot_broadcast, shop_id, callback.from_user.id,
            data.get("broadcast_text") or "", attachment, quota_exempt=shop_id == MY_SHOP_ID,
        )
    except HTTPException as exc:
        code = exc.detail.get("code") if isinstance(exc.detail, dict) else None
        message = ("❌ Ліміт розсилок вичерпано." if exc.status_code == 429
                   else "❌ Немає активних клієнтів для розсилки." if code in {"NO_RECIPIENTS", "EMPTY_AUDIENCE"}
                   else "❌ Не вдалося опублікувати розсилку. Почни ще раз.")
        await callback.message.answer(message)
        return
    except Exception as exc:
        logger.warning("Broadcast publication failed (%s)", type(exc).__name__)
        await callback.message.answer("❌ Не вдалося опублікувати розсилку. Почни ще раз.")
        return

    sent = 0
    failed = 0
    touched_user_ids = []
    recipients = list({row["user_id"]: row for row in publication["recipients"]}.values())
    for index, row in enumerate(recipients):
        try:
            current_shop = await run_in_threadpool(get_shop, shop_id)
            if not current_shop or not current_shop["is_active"]:
                failed += len(recipients) - index
                break
            push_sent = await send_broadcast_app_push(publication, row)
            if push_sent is False:
                failed += 1
                continue
            if push_sent is None and row.get("telegram_user_id") is None:
                continue
            if push_sent is None:
                await callback.bot.copy_message(
                    chat_id=row["telegram_user_id"],
                    from_chat_id=data["broadcast_from_chat_id"],
                    message_id=data["broadcast_message_id"],
                )
            sent += 1
            touched_user_ids.append(row["user_id"])
        except Exception as exc:
            logger.warning("Broadcast delivery failed (%s)", type(exc).__name__)
            failed += 1

    try:
        await run_in_threadpool(record_broadcast_delivery, shop_id, publication["broadcast_id"], touched_user_ids)
    except Exception as exc:
        logger.warning("Broadcast delivery recording failed (%s)", type(exc).__name__)
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
