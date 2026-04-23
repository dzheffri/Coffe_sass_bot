import os
import uuid
import random
from datetime import datetime, timedelta

from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from aiogram import Bot

from app.profile_logic import get_user_cups_data
from app.config import BOT_TOKEN
from app.web_panel_logic import get_shop_profile, update_shop_profile
from app.web_panel_db import init_web_panel_db

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
UPLOADS_DIR = os.path.join(BASE_DIR, "uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)

app = FastAPI(title="Coffee Club API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
        "*",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/uploads", StaticFiles(directory=UPLOADS_DIR), name="uploads")

init_web_panel_db()


class SendCodeRequest(BaseModel):
    telegram_id: str


class VerifyCodeRequest(BaseModel):
    telegram_id: str
    code: str


class ShopNewsItem(BaseModel):
    title: str = ""
    price: str = ""
    image_url: str = ""


class UpdateShopRequest(BaseModel):
    name: str = ""
    subtitle: str = ""
    address: str = ""
    work_from: str = ""
    work_to: str = ""
    instagram: str = ""
    description: str = ""
    logo_url: str = ""
    cover_url: str = ""
    news: list[ShopNewsItem] = []


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


@app.get("/owner/shop/{owner_telegram_id}")
def owner_get_shop(owner_telegram_id: int):
    return get_shop_profile(owner_telegram_id)


@app.put("/owner/shop/{owner_telegram_id}")
def owner_update_shop(owner_telegram_id: int, data: UpdateShopRequest):
    return update_shop_profile(
        owner_telegram_id=owner_telegram_id,
        name=data.name,
        subtitle=data.subtitle,
        address=data.address,
        work_from=data.work_from,
        work_to=data.work_to,
        instagram=data.instagram,
        description=data.description,
        logo_url=data.logo_url,
        cover_url=data.cover_url,
        news=[item.dict() for item in data.news],
    )


@app.post("/upload/image")
async def upload_image(file: UploadFile = File(...)):
    if not file.filename:
        return {
            "ok": False,
            "message": "Файл не вибрано"
        }

    allowed_extensions = {".jpg", ".jpeg", ".png", ".webp"}
    extension = os.path.splitext(file.filename)[1].lower()

    if extension not in allowed_extensions:
        return {
            "ok": False,
            "message": "Дозволені тільки JPG, JPEG, PNG, WEBP"
        }

    filename = f"{uuid.uuid4().hex}{extension}"
    file_path = os.path.join(UPLOADS_DIR, filename)

    contents = await file.read()
    with open(file_path, "wb") as f:
        f.write(contents)

    return {
        "ok": True,
        "url": f"/uploads/{filename}"
    }
