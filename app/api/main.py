import random
from fastapi import FastAPI
from pydantic import BaseModel
from aiogram import Bot

from app.profile_logic import get_user_cups_data
from app.config import BOT_TOKEN

app = FastAPI(title="Coffee Club API")


class SendCodeRequest(BaseModel):
    telegram_id: str


codes_storage: dict[str, str] = {}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/users/{telegram_user_id}/cups")
def user_cups(telegram_user_id: int):
    return get_user_cups_data(telegram_user_id)


@app.post("/auth/send-code")
async def send_code(data: SendCodeRequest):
    telegram_id = data.telegram_id.strip()

    if not telegram_id.isdigit():
        return {
            "ok": False,
            "message": "Некоректний Telegram ID"
        }

    code = str(random.randint(1000, 9999))
    codes_storage[telegram_id] = code

    bot = Bot(token=BOT_TOKEN)

    try:
        await bot.send_message(
            chat_id=int(telegram_id),
            text=f"Ваш код входу: {code}"
        )
    except Exception as e:
        print("SEND CODE ERROR:", e)
        return {
            "ok": False,
            "message": "Не вдалося надіслати код. Напишіть боту /start і спробуйте ще раз."
        }
    finally:
        await bot.session.close()

    return {
        "ok": True,
        "message": "Код надіслано в Telegram"
    }
