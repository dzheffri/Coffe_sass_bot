from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

SUPPORT_ADMIN_ID = 8738388068

router = Router()


class SupportStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_admin_reply = State()


@router.message(F.text == "🛟 Служба підтримки")
async def support_start(message: types.Message, state: FSMContext):
    await state.set_state(SupportStates.waiting_for_message)

    await message.answer(
        "🛟 Служба підтримки\n\n"
        "Якщо у вас не працює QR-код, не нарахувались чашки або виникли будь-які питання, "
        "повʼязані з системою лояльності — сміливо пишіть нам 🙌\n\n"
        "Опишіть ситуацію одним повідомленням."
    )


@router.message(SupportStates.waiting_for_message)
async def support_message(message: types.Message, state: FSMContext):
    user = message.from_user
    text = message.text or "Користувач надіслав повідомлення без тексту."

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✉️ Відповісти користувачу",
                    callback_data=f"support_reply:{user.id}"
                )
            ]
        ]
    )

    await message.bot.send_message(
        SUPPORT_ADMIN_ID,
        "📩 Нове звернення в підтримку\n\n"
        f"👤 Користувач: {user.full_name}\n"
        f"🔗 Username: @{user.username if user.username else 'немає'}\n"
        f"🆔 Telegram ID: {user.id}\n\n"
        f"💬 Повідомлення:\n{text}",
        reply_markup=keyboard
    )

    await message.answer(
        "🛟 Дякуємо за звернення!\n\n"
        "Якщо у вас не працює QR-код, не нарахувались чашки або виникли будь-які питання, "
        "повʼязані з системою лояльності — сміливо пишіть нам 🙌\n\n"
        "Ми отримали ваше повідомлення та звʼяжемося з вами найближчим часом."
    )

    await state.clear()


@router.callback_query(F.data.startswith("support_reply:"))
async def support_reply_start(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != SUPPORT_ADMIN_ID:
        await callback.answer("Немає доступу", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])

    await state.update_data(reply_user_id=user_id)
    await state.set_state(SupportStates.waiting_for_admin_reply)

    await callback.message.answer(
        "✉️ Напиши відповідь користувачу одним повідомленням.\n\n"
        "Щоб скасувати — напиши: скасувати"
    )

    await callback.answer()


@router.message(SupportStates.waiting_for_admin_reply)
async def support_send_reply(message: types.Message, state: FSMContext):
    if message.from_user.id != SUPPORT_ADMIN_ID:
        return

    text = (message.text or "").strip()

    if text.lower() in ["скасувати", "отмена", "cancel"]:
        await state.clear()
        await message.answer("❌ Відповідь скасовано.")
        return

    data = await state.get_data()
    user_id = data.get("reply_user_id")

    if not user_id:
        await state.clear()
        await message.answer("❌ Не знайшов користувача для відповіді.")
        return

    try:
        await message.bot.send_message(
            chat_id=int(user_id),
            text=(
                "🛟 Відповідь служби підтримки\n\n"
                f"{text}"
            )
        )

        await message.answer("✅ Відповідь надіслано користувачу.")
    except Exception as e:
        await message.answer(f"❌ Не вдалося надіслати відповідь: {e}")

    await state.clear()
