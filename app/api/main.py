import random
from datetime import datetime, timedelta

from fastapi import FastAPI
from pydantic import BaseModel
from aiogram import Bot

from app.profile_logic import get_user_cups_data
from app.config import BOT_TOKEN

app = FastAPI(title="Coffee Club API")


class SendCodeRequest(BaseModel):
    telegram_id: str


class VerifyCodeRequest(BaseModel):
    telegram_id: str
    code: str


# {
#   "telegram_id": {
#       "code": "1234",
#       "expires_at": datetime(...)
#   }
# }
codes_storage: dict[str, dict] = {}


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

    # код живет 15 минут
    expires_at = datetime.utcnow() + timedelta(minutes=15)

    codes_storage[telegram_id] = {
        "code": code,
        "expires_at": expires_at
    }

    bot = Bot(token=BOT_TOKEN)

    try:
        await bot.send_message(
            chat_id=int(telegram_id),
            text=f"Ваш код входу: {code}\n\nКод дійсний 15 хвилин."
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


@app.post("/auth/verify-code")
async def verify_code(data: VerifyCodeRequest):
    telegram_id = data.telegram_id.strip()
    code = data.code.strip()

    if not telegram_id.isdigit():
        return {
            "ok": False,
            "message": "Некоректний Telegram ID"
        }

    if not code.isdigit() or len(code) != 4:
        return {
            "ok": False,
            "message": "Некоректний код"
        }

    saved_data = codes_storage.get(telegram_id)

    if not saved_data:
        return {
            "ok": False,
            "message": "Код не знайдено. Запросіть новий код."
        }

    saved_code = saved_data["code"]
    expires_at = saved_data["expires_at"]

    if datetime.utcnow() > expires_at:
        del codes_storage[telegram_id]
        return {
            "ok": False,
            "message": "Термін дії коду закінчився. Запросіть новий код."
        }

    if code != saved_code:
        return {
            "ok": False,
            "message": "Невірний код"
        }

    del codes_storage[telegram_id]

    return {
        "ok": True,
        "message": "Успішний вхід"
    }
