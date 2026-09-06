import os
import uuid
import random
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from aiogram import Bot

from app.profile_logic import get_user_cups_data
from app.config import BOT_TOKEN
from app.web_panel_logic import (
    get_shop_profile,
    update_shop_profile,
    get_owner_overview_stats,
    get_owner_activity_stats,
    get_owner_clients,
    get_owner_details_stats,
)
from app.web_panel_db import init_web_panel_db
from app.db import (
    get_connection,
    is_owner,
    get_shop_reminder_settings,
    update_shop_reminder_settings,
    get_shop_admins,
    add_shop_admin,
    remove_shop_admin,
    get_subscription,
)


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


class ReminderSettingsRequest(BaseModel):
    one_left_enabled: bool
    one_left_days: int

    free_coffee_enabled: bool
    free_coffee_days: int

    inactive_5_7_enabled: bool
    inactive_5_7_days: int

    inactive_14_30_enabled: bool
    inactive_14_30_days: int


class AddAdminRequest(BaseModel):
    telegram_id: int


codes_storage: dict[str, dict] = {}


def get_owner_shop_id(owner_telegram_id: int):
    if not is_owner(owner_telegram_id):
        return None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sa.shop_id
                FROM shop_admins sa
                JOIN users u ON u.id = sa.user_id
                WHERE u.telegram_user_id = %s
                  AND sa.role = 'owner'
                ORDER BY sa.shop_id
                LIMIT 1
                """,
                (owner_telegram_id,)
            )

            row = cur.fetchone()

    if not row:
        return None

    return row["shop_id"]


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
                    sc.last_activity_at,
                    owner.telegram_user_id AS owner_telegram_id
                FROM shop_clients sc
                JOIN users client ON client.id = sc.user_id
                JOIN coffee_shops cs ON cs.id = sc.shop_id
                LEFT JOIN shop_admins sa
                    ON sa.shop_id = cs.id AND sa.role = 'owner'
                LEFT JOIN users owner
                    ON owner.id = sa.user_id
                WHERE client.telegram_user_id = %s
                ORDER BY sc.last_activity_at DESC NULLS LAST, cs.name
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
            "city": row["city"] or "",
            "last_activity_at": (
                row["last_activity_at"].isoformat()
                if row["last_activity_at"]
                else None
            ),
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
        return {
            "ok": False,
            "message": "Некоректний Telegram ID"
        }

    if not is_owner(int(telegram_id)):
        return {
            "ok": False,
            "message": "У вас немає доступу до панелі кав’ярні"
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
            "message": "Напишіть боту /start і спробуйте ще раз"
        }

    finally:
        await bot.session.close()

    return {
        "ok": True,
        "message": "Код відправлено"
    }


@app.post("/auth/verify-code")
async def verify_code(data: VerifyCodeRequest):
    telegram_id = data.telegram_id.strip()
    code = data.code.strip()

    saved = codes_storage.get(telegram_id)

    if not saved:
        return {
            "ok": False,
            "message": "Код не знайдено"
        }

    if datetime.utcnow() > saved["expires_at"]:
        return {
            "ok": False,
            "message": "Код протух"
        }

    if code != saved["code"]:
        return {
            "ok": False,
            "message": "Невірний код"
        }

    del codes_storage[telegram_id]

    return {"ok": True}


@app.get("/owner/shop/{owner_telegram_id}")
def owner_get_shop(owner_telegram_id: int):
    return get_shop_profile(owner_telegram_id)


@app.put("/owner/shop/{owner_telegram_id}")
def owner_update_shop(
    owner_telegram_id: int,
    data: UpdateShopRequest
):
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


@app.get("/owner/analytics/{owner_telegram_id}/overview")
def owner_analytics_overview(owner_telegram_id: int):
    return get_owner_overview_stats(owner_telegram_id)


@app.get("/owner/analytics/{owner_telegram_id}/activity")
def owner_analytics_activity(owner_telegram_id: int):
    return get_owner_activity_stats(owner_telegram_id)


@app.get("/owner/analytics/{owner_telegram_id}/clients")
def owner_analytics_clients(owner_telegram_id: int):
    return get_owner_clients(owner_telegram_id)


@app.get("/owner/analytics/{owner_telegram_id}/details")
def owner_analytics_details(owner_telegram_id: int):
    return get_owner_details_stats(owner_telegram_id)


# =========================================================
# REMINDER SETTINGS
# =========================================================

@app.get("/owner/settings/{owner_telegram_id}/reminders")
def owner_get_reminder_settings(owner_telegram_id: int):
    shop_id = get_owner_shop_id(owner_telegram_id)

    if not shop_id:
        return {
            "ok": False,
            "message": "Кав’ярню власника не знайдено"
        }

    settings = get_shop_reminder_settings(shop_id)

    if not settings:
        return {
            "ok": False,
            "message": "Налаштування не знайдено"
        }

    return {
        "ok": True,
        "shop_id": shop_id,
        "settings": {
            "one_left_enabled": settings["one_left_enabled"],
            "one_left_days": settings["one_left_days"],

            "free_coffee_enabled": settings["free_coffee_enabled"],
            "free_coffee_days": settings["free_coffee_days"],

            "inactive_5_7_enabled": settings["inactive_5_7_enabled"],
            "inactive_5_7_days": settings["inactive_5_7_days"],

            "inactive_14_30_enabled": settings["inactive_14_30_enabled"],
            "inactive_14_30_days": settings["inactive_14_30_days"],
        }
    }


@app.put("/owner/settings/{owner_telegram_id}/reminders")
def owner_update_reminder_settings(
    owner_telegram_id: int,
    data: ReminderSettingsRequest
):
    shop_id = get_owner_shop_id(owner_telegram_id)

    if not shop_id:
        return {
            "ok": False,
            "message": "Кав’ярню власника не знайдено"
        }

    days_values = [
        data.one_left_days,
        data.free_coffee_days,
        data.inactive_5_7_days,
        data.inactive_14_30_days,
    ]

    if any(value < 1 or value > 7 for value in days_values):
        return {
            "ok": False,
            "message": "Кількість днів має бути від 1 до 7"
        }

    try:
        settings = update_shop_reminder_settings(
            shop_id=shop_id,
            one_left_enabled=data.one_left_enabled,
            one_left_days=data.one_left_days,
            free_coffee_enabled=data.free_coffee_enabled,
            free_coffee_days=data.free_coffee_days,
            inactive_5_7_enabled=data.inactive_5_7_enabled,
            inactive_5_7_days=data.inactive_5_7_days,
            inactive_14_30_enabled=data.inactive_14_30_enabled,
            inactive_14_30_days=data.inactive_14_30_days,
        )

    except ValueError as e:
        return {
            "ok": False,
            "message": str(e)
        }

    return {
        "ok": True,
        "shop_id": shop_id,
        "settings": {
            "one_left_enabled": settings["one_left_enabled"],
            "one_left_days": settings["one_left_days"],

            "free_coffee_enabled": settings["free_coffee_enabled"],
            "free_coffee_days": settings["free_coffee_days"],

            "inactive_5_7_enabled": settings["inactive_5_7_enabled"],
            "inactive_5_7_days": settings["inactive_5_7_days"],

            "inactive_14_30_enabled": settings["inactive_14_30_enabled"],
            "inactive_14_30_days": settings["inactive_14_30_days"],
        }
    }


# =========================================================
# SHOP ADMINS
# =========================================================

@app.get("/owner/settings/{owner_telegram_id}/admins")
def owner_get_admins(owner_telegram_id: int):
    shop_id = get_owner_shop_id(owner_telegram_id)

    if not shop_id:
        return {
            "ok": False,
            "message": "Кав’ярню власника не знайдено"
        }

    rows = get_shop_admins(shop_id)

    admins = []

    for row in rows:
        admins.append({
            "telegram_id": row["telegram_user_id"],
            "full_name": row["full_name"] or "",
            "username": row["username"] or "",
            "role": row["role"],
        })

    return {
        "ok": True,
        "shop_id": shop_id,
        "admins": admins,
    }


@app.post("/owner/settings/{owner_telegram_id}/admins")
def owner_add_admin(
    owner_telegram_id: int,
    data: AddAdminRequest
):
    shop_id = get_owner_shop_id(owner_telegram_id)

    if not shop_id:
        return {
            "ok": False,
            "message": "Кав’ярню власника не знайдено"
        }

    if data.telegram_id == owner_telegram_id:
        return {
            "ok": False,
            "message": "Власник вже має повний доступ"
        }

    existing_admins = get_shop_admins(shop_id)

    existing = next(
        (
            row
            for row in existing_admins
            if row["telegram_user_id"] == data.telegram_id
        ),
        None,
    )

    if existing and existing["role"] == "owner":
        return {
            "ok": False,
            "message": "Цей користувач є власником кав’ярні"
        }

    if existing and existing["role"] == "admin":
        return {
            "ok": True,
            "message": "Цей адміністратор вже доданий"
        }

    result = add_shop_admin(
        shop_id=shop_id,
        admin_telegram_user_id=data.telegram_id,
        role="admin",
    )

    if not result:
        return {
            "ok": False,
            "message": (
                "Користувача не знайдено. "
                "Нехай співробітник спочатку напише боту /start."
            )
        }

    return {
        "ok": True,
        "message": "Адміністратора додано"
    }


@app.delete(
    "/owner/settings/{owner_telegram_id}/admins/{admin_telegram_id}"
)
def owner_delete_admin(
    owner_telegram_id: int,
    admin_telegram_id: int
):
    shop_id = get_owner_shop_id(owner_telegram_id)

    if not shop_id:
        return {
            "ok": False,
            "message": "Кав’ярню власника не знайдено"
        }

    if admin_telegram_id == owner_telegram_id:
        return {
            "ok": False,
            "message": "Власника кав’ярні видалити не можна"
        }

    admins = get_shop_admins(shop_id)

    target = next(
        (
            row
            for row in admins
            if row["telegram_user_id"] == admin_telegram_id
        ),
        None,
    )

    if not target:
        return {
            "ok": False,
            "message": "Адміністратора не знайдено"
        }

    if target["role"] == "owner":
        return {
            "ok": False,
            "message": "Власника кав’ярні видалити не можна"
        }

    deleted = remove_shop_admin(
        shop_id=shop_id,
        admin_telegram_user_id=admin_telegram_id,
    )

    if not deleted:
        return {
            "ok": False,
            "message": "Не вдалося видалити адміністратора"
        }

    return {
        "ok": True,
        "message": "Адміністратора видалено"
    }


# =========================================================
# SUBSCRIPTION
# =========================================================

@app.get("/owner/settings/{owner_telegram_id}/subscription")
def owner_get_subscription(owner_telegram_id: int):
    shop_id = get_owner_shop_id(owner_telegram_id)

    if not shop_id:
        return {
            "ok": False,
            "message": "Кав’ярню власника не знайдено"
        }

    subscription = get_subscription(shop_id)

    if not subscription:
        return {
            "ok": True,
            "shop_id": shop_id,
            "subscription": None,
        }

    expires_at = subscription["expires_at"]

    now = datetime.now(timezone.utc)

    if expires_at.tzinfo is None:
        expires_at_for_calc = expires_at.replace(
            tzinfo=timezone.utc
        )
    else:
        expires_at_for_calc = expires_at.astimezone(
            timezone.utc
        )

    seconds_left = (
        expires_at_for_calc - now
    ).total_seconds()

    days_left = max(
        0,
        int((seconds_left + 86399) // 86400)
    )

    actual_status = subscription["status"]

    if expires_at_for_calc <= now:
        actual_status = "expired"

    return {
        "ok": True,
        "shop_id": shop_id,
        "subscription": {
            "plan": subscription["plan"],
            "status": actual_status,
            "expires_at": expires_at.isoformat(),
            "days_left": days_left,
        }
    }


# =========================================================
# IMAGE UPLOAD
# =========================================================

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
