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
from app.db import get_connection

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
UPLOADS_DIR = os.path.join(BASE_DIR, "uploads")

os.makedirs(UPLOADS_DIR, exist_ok=True)

app = FastAPI(title="Coffee Club API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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


codes_storage: dict[str, dict] = {}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/users/{telegram_user_id}/cups")
def user_cups(telegram_user_id: int):
    return get_user_cups_data(telegram_user_id)


@app.get("/users/{telegram_user_id}/qr")
def user_qr(telegram_user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT personal_qr_token
                FROM users
                WHERE telegram_user_id = %s
                """,
                (telegram_user_id,)
            )
            row = cur.fetchone()

    if not row:
        return {
            "ok": False,
            "message": "Користувача не знайдено"
        }

    return {
        "ok": True,
        "qr_token": row["personal_qr_token"]
    }


@app.get("/users/{telegram_user_id}/shops")
def user_shops(telegram_user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    cs.id AS shop_id,
                    cs.name AS db_shop_name,
                    cs.city AS city,
                    sc.cups,
                    sc.free_coffee_balance,
                    owner.telegram_user_id AS owner_telegram_id
                FROM shop_clients sc
                JOIN users client ON client.id = sc.user_id
                JOIN coffee_shops cs ON cs.id = sc.shop_id
                LEFT JOIN shop_admins sa
                    ON sa.shop_id = cs.id AND sa.role = 'owner'
                LEFT JOIN users owner
                    ON owner.id = sa.user_id
                WHERE client.telegram_user_id = %s
                ORDER BY cs.name
                """,
                (telegram_user_id,)
            )
            rows = cur.fetchall()

    shops = []

    for row in rows:
        owner_id = row["owner_telegram_id"]

        profile = {}
        if owner_id:
            profile_data = get_shop_profile(owner_id)
            if profile_data and profile_data.get("ok"):
                profile = profile_data.get("shop") or {}

        shops.append({
            "shop_id": row["shop_id"],
            "owner_telegram_id": owner_id,
            "name": profile.get("name") or row["db_shop_name"] or "Кавʼярня",

            # 🔥 ВОТ ОНО — ГОРОД
            "city": row["city"] or "",

            "subtitle": profile.get("subtitle") or "",
            "address": profile.get("address") or "",
            "work_from": profile.get("work_from") or "",
            "work_to": profile.get("work_to") or "",
            "instagram": profile.get("instagram") or "",
            "description": profile.get("description") or "",
            "logo_url": profile.get("logo_url") or "",
            "cover_url": profile.get("cover_url") or "",
            "news": profile.get("news") or [],
            "cups": row["cups"] or 0,
            "free_coffee_balance": row["free_coffee_balance"] or 0,
        })

    return {
        "ok": True,
        "shops": shops
    }

@app.get("/users/{telegram_user_id}/stats")
def user_stats(telegram_user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(sc.total_scans), 0) AS total_cups,
                    COALESCE(SUM(sc.free_coffee_balance), 0) AS total_free,
                    COUNT(sc.id) AS shops_count
                FROM shop_clients sc
                JOIN users u ON u.id = sc.user_id
                WHERE u.telegram_user_id = %s
                """,
                (telegram_user_id,)
            )

            row = cur.fetchone()

    return {
        "ok": True,
        "total_cups": row["total_cups"] or 0,
        "total_free": row["total_free"] or 0,
        "shops_count": row["shops_count"] or 0
    }
@app.get("/shops")
def all_shops():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    cs.id AS shop_id,
                    cs.name AS db_shop_name,
                    cs.city AS city,
                    owner.telegram_user_id AS owner_telegram_id
                FROM coffee_shops cs
                LEFT JOIN shop_admins sa
                    ON sa.shop_id = cs.id AND sa.role = 'owner'
                LEFT JOIN users owner
                    ON owner.id = sa.user_id
                ORDER BY cs.name
                """
            )
            rows = cur.fetchall()

    shops = []

    for row in rows:
        owner_id = row["owner_telegram_id"]

        profile = {}
        if owner_id:
            profile_data = get_shop_profile(owner_id)
            if profile_data and profile_data.get("ok"):
                profile = profile_data.get("shop") or {}

        shops.append({
            "shop_id": row["shop_id"],
            "owner_telegram_id": owner_id,
            "name": profile.get("name") or row["db_shop_name"] or "Кавʼярня",
            "city": row["city"] or "",
            "subtitle": profile.get("subtitle") or "",
            "address": profile.get("address") or "",
            "work_from": profile.get("work_from") or "",
            "work_to": profile.get("work_to") or "",
            "instagram": profile.get("instagram") or "",
            "description": profile.get("description") or "",
            "logo_url": profile.get("logo_url") or "",
            "cover_url": profile.get("cover_url") or "",
            "news": profile.get("news") or [],
            "cups": 0,
            "free_coffee_balance": 0,
        })

    return {
        "ok": True,
        "shops": shops
    }

@app.post("/auth/send-code")
async def send_code(data: SendCodeRequest):
    telegram_id = data.telegram_id.strip()

    if not telegram_id.isdigit():
        return {"ok": False, "message": "Некоректний Telegram ID"}

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
            "message": "Напишіть боту /start і спробуйте ще раз"
        }
    finally:
        await bot.session.close()

    return {"ok": True}


@app.post("/auth/verify-code")
async def verify_code(data: VerifyCodeRequest):
    telegram_id = data.telegram_id.strip()
    code = data.code.strip()

    saved = codes_storage.get(telegram_id)

    if not saved:
        return {"ok": False, "message": "Код не знайдено"}

    if datetime.utcnow() > saved["expires_at"]:
        return {"ok": False, "message": "Код протух"}

    if code != saved["code"]:
        return {"ok": False, "message": "Невірний код"}

    del codes_storage[telegram_id]

    return {"ok": True}


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
        return {"ok": False}

    ext = os.path.splitext(file.filename)[1].lower()
    filename = f"{uuid.uuid4().hex}{ext}"

    path = os.path.join(UPLOADS_DIR, filename)

    contents = await file.read()
    with open(path, "wb") as f:
        f.write(contents)

    return {
        "ok": True,
        "url": f"/uploads/{filename}"
    }
