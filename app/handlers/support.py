from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

SUPPORT_ADMIN_ID = 8738388068  # твой Telegram ID

router = Router()


class SupportStates(StatesGroup):
    waiting_for_message = State()


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

    await message.bot.send_message(
        SUPPORT_ADMIN_ID,
        "📩 Нове звернення в підтримку\n\n"
        f"👤 Користувач: {user.full_name}\n"
        f"🔗 Username: @{user.username if user.username else 'немає'}\n"
        f"🆔 Telegram ID: {user.id}\n\n"
        f"💬 Повідомлення:\n{text}"
    )

    await message.answer(
        "🛟 Дякуємо за звернення!\n\n"
        "Ми отримали ваше повідомлення та звʼяжемося з вами найближчим часом."
    )

    await state.clear()
