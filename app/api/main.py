import os
import uuid
import random
import json
import hmac
import hashlib
import secrets
import base64
import time
from urllib.request import urlopen
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes
from urllib.parse import parse_qsl
from datetime import datetime, timedelta, timezone
from app.skin_catalog import SKIN_CATALOG, skin_progress
from fastapi import FastAPI, UploadFile, File, Request, Response, Header, HTTPException, Depends
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
    consume_admin_login_ticket,
    get_user_by_identity,
    link_user_identity,
    merge_users,
    unlink_user_identity,
    create_user_with_identity,
    create_app_session,
    get_app_session,
    revoke_app_session,
    revoke_all_user_sessions,
    delete_user_account,
    get_or_create_wallet_pass,
    get_wallet_pass_by_serial,
    set_wallet_card_design,
    touch_wallet_pass,
    register_wallet_device,
    unregister_wallet_device,
    get_wallet_device_serials,
    get_wallet_push_tokens,
    get_wallet_user_stats,
    register_app_push_device,
    set_app_push_device_enabled,
    unregister_app_push_device,
    get_app_push_devices_for_user,
    get_app_push_devices_for_shop,
)
from app.wallet_pass import (
    WALLET_PASS_TYPE_ID,
    VALID_CARD_DESIGNS,
    create_pkpass,
    wallet_last_modified_http_date,
)

from app.wallet_push import send_wallet_pushes
from app.app_push import send_app_pushes
from app.api.barista import build_barista_router
from app.telegram_link import confirm_telegram_link_session
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
UPLOADS_DIR = "/data/uploads"
SKIN_ASSETS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "wallet_assets",
)
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
app.mount(
    "/skin-assets",
    StaticFiles(directory=SKIN_ASSETS_DIR),
    name="skin-assets",
)
init_web_panel_db()


def ensure_telegram_link_sessions_table():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_link_sessions (
                    token_hash VARCHAR(64) PRIMARY KEY,
                    provider VARCHAR(20) NOT NULL,
                    provider_user_id TEXT NOT NULL,
                    mode VARCHAR(20) NOT NULL DEFAULT 'link',
                    current_user_id BIGINT,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    telegram_user_id BIGINT,
                    user_id INTEGER,
                    personal_qr_token TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL,
                    confirmed_at TIMESTAMPTZ
                )
                """
            )

            cur.execute(
                """
                ALTER TABLE telegram_link_sessions
                ADD COLUMN IF NOT EXISTS mode VARCHAR(20) NOT NULL DEFAULT 'link'
                """
            )

            cur.execute(
                """
                ALTER TABLE telegram_link_sessions
                ADD COLUMN IF NOT EXISTS current_user_id BIGINT
                """
            )


ensure_telegram_link_sessions_table()


class SendCodeRequest(BaseModel):
    telegram_id: str


class VerifyCodeRequest(BaseModel):
    telegram_id: str
    code: str


class TelegramMiniAppAuthRequest(BaseModel):
    init_data: str


class AdminTicketAuthRequest(BaseModel):
    ticket: str


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
class ModerationRejectRequest(BaseModel):
    reason: str = ""

class ReminderSettingsRequest(BaseModel):
    one_left_enabled: bool
    one_left_days: int

    free_coffee_enabled: bool
    free_coffee_days: int

    inactive_5_7_enabled: bool
    inactive_5_7_days: int

    inactive_14_30_enabled: bool
    inactive_14_30_days: int

    # Пока старая админка это поле не отправляет,
    # поэтому оставляем None и сохраняем текущее значение из БД.
    shop_news_enabled: bool | None = None


class AppPushDeviceRequest(BaseModel):
    device_token: str
    environment: str = "production"
    notifications_enabled: bool = True


class AppPushDeviceRemoveRequest(BaseModel):
    device_token: str


class AddAdminRequest(BaseModel):
    telegram_id: int


codes_storage: dict[str, dict] = {}

# =========================================================
# APP SESSION AUTH
# =========================================================

GUEST_USER_ID = 32650
SUPERADMIN_TELEGRAM_IDS = {
    566408696,
}

def _get_bearer_token_or_401(
    authorization: str | None,
) -> str:
    if not authorization:
        raise HTTPException(
            status_code=401,
            detail="Authorization header is required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    scheme, separator, token = authorization.partition(" ")

    if (
        separator != " "
        or scheme.lower() != "bearer"
        or not token.strip()
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return token.strip()


def get_current_user(
    authorization: str | None = Header(default=None),
):
    token = _get_bearer_token_or_401(authorization)
    session = get_app_session(token)

    if not session:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired session",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return session


def get_verified_telegram_actor(authorization: str | None, init_data: str | None):
    """Resolve an actor from an existing verified session or Telegram WebApp proof."""
    if authorization:
        current = get_current_user(authorization)
        telegram_id = current.get("telegram_user_id")
        if telegram_id is None:
            raise HTTPException(403, detail={"code": "TELEGRAM_PROFILE_REQUIRED"})
        return {"telegram_id": int(telegram_id), "user_id": current["user_id"]}
    if init_data:
        validated = validate_telegram_init_data(init_data)
        if not validated:
            raise HTTPException(401, detail={"code": "INVALID_TELEGRAM_CONTEXT"})
        user = get_user_by_identity("telegram", str(validated["telegram_id"]))
        if not user:
            raise HTTPException(403, detail={"code": "TELEGRAM_PROFILE_REQUIRED"})
        return {"telegram_id": validated["telegram_id"], "user_id": user["id"]}
    raise HTTPException(401, detail={"code": "AUTHENTICATION_REQUIRED"})


def require_owner_for_path(
    owner_telegram_id: int,
    authorization: str | None = Header(default=None),
    x_telegram_init_data: str | None = Header(default=None),
):
    actor = get_verified_telegram_actor(authorization, x_telegram_init_data)
    if actor["telegram_id"] != owner_telegram_id:
        raise HTTPException(403, detail={"code": "OWNER_ACCESS_REQUIRED"})
    with get_connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sa.shop_id
                    FROM shop_admins sa
                    JOIN coffee_shops cs ON cs.id = sa.shop_id
                    WHERE sa.user_id = %s AND sa.role = 'owner' AND cs.is_active
                    ORDER BY cs.id, sa.id
                    FOR SHARE OF cs, sa
                """, (actor["user_id"],))
                memberships = cur.fetchall()
                if not memberships:
                    raise HTTPException(403, detail={"code": "OWNER_ACCESS_REQUIRED"})
                yield {**actor, "owner_shop_ids": [row["shop_id"] for row in memberships]}


def require_owner_analytics_context(
    request: Request,
    owner=Depends(require_owner_for_path),
):
    """Verify the selected analytics shop against the actor's current ownership.

    A sole owner membership retains legacy compatibility. Multiple memberships
    require explicit selection, so an absent context never silently picks a cafe.
    """
    if set(request.query_params) - {"shop_id"}:
        raise HTTPException(403, detail={"code": "ANALYTICS_CONTEXT_NOT_ALLOWED"})
    contexts = request.query_params.getlist("shop_id")
    if not contexts:
        if len(owner["owner_shop_ids"]) != 1:
            raise HTTPException(409, detail={"code": "SHOP_CONTEXT_REQUIRED"})
        shop_id = owner["owner_shop_ids"][0]
    else:
        raw = contexts[0]
        if (len(contexts) != 1 or not raw.isascii() or not raw.isdigit()
                or len(raw) > 19 or not 0 < int(raw) <= 9223372036854775807):
            raise HTTPException(422, detail={"code": "INVALID_SHOP_CONTEXT"})
        shop_id = int(raw)
        if shop_id not in owner["owner_shop_ids"]:
            raise HTTPException(403, detail={"code": "OWNER_ACCESS_REQUIRED"})
    return {**owner, "shop_id": shop_id}


def require_superadmin(
    current_user=Depends(get_current_user),
):
    telegram_id = current_user.get("telegram_user_id")

    if not telegram_id:
        raise HTTPException(
            status_code=403,
            detail="Telegram не підключено",
        )

    if int(telegram_id) not in SUPERADMIN_TELEGRAM_IDS:
        raise HTTPException(
            status_code=403,
            detail="Недостатньо прав",
        )

    return current_user

def _create_login_response_session(user_id: int) -> dict:
    return {
        "access_token": create_app_session(user_id),
        "token_type": "bearer",
    }


def verify_google_id_token(token: str, *, audience: str | None = None):
    if not token:
        return None

    try:
        payload = id_token.verify_oauth2_token(
            token,
            google_requests.Request(),
            audience=audience or os.getenv("GOOGLE_CLIENT_ID"),
        )

        if payload.get("iss") not in {
            "accounts.google.com",
            "https://accounts.google.com",
        }:
            return None

        google_sub = payload.get("sub")

        if not google_sub:
            return None

        return payload

    except Exception as e:
        print("GOOGLE TOKEN VERIFY ERROR:", e)
        return None


APPLE_ISSUER = "https://appleid.apple.com"
APPLE_KEYS_URL = "https://appleid.apple.com/auth/keys"
APPLE_CLIENT_ID = os.getenv(
    "APPLE_CLIENT_ID",
    "com.dzheffri.coffeeclubpass",
)

_apple_keys_cache = {
    "keys": None,
    "expires_at": 0,
}


def _b64url_decode(value: str) -> bytes:
    padding_len = (-len(value)) % 4
    return base64.urlsafe_b64decode(
        value + ("=" * padding_len)
    )


def _load_apple_keys():
    now = time.time()

    if (
        _apple_keys_cache["keys"] is not None
        and now < _apple_keys_cache["expires_at"]
    ):
        return _apple_keys_cache["keys"]

    with urlopen(APPLE_KEYS_URL, timeout=10) as response:
        data = json.loads(response.read().decode("utf-8"))

    keys = data.get("keys") or []

    _apple_keys_cache["keys"] = keys
    _apple_keys_cache["expires_at"] = now + 3600

    return keys


def verify_apple_id_token(token: str, *, audience: str | None = None):
    if not token:
        return None

    expected_audience = audience or APPLE_CLIENT_ID

    try:
        parts = token.split(".")

        if len(parts) != 3:
            return None

        header = json.loads(
            _b64url_decode(parts[0]).decode("utf-8")
        )

        payload = json.loads(
            _b64url_decode(parts[1]).decode("utf-8")
        )

        if header.get("alg") != "RS256":
            return None

        kid = header.get("kid")
        if not kid:
            return None

        apple_keys = _load_apple_keys()

        jwk = next(
            (
                key
                for key in apple_keys
                if key.get("kid") == kid
            ),
            None,
        )

        if not jwk:
            # На случай ротации ключей Apple обновляем кэш один раз.
            _apple_keys_cache["keys"] = None
            _apple_keys_cache["expires_at"] = 0

            apple_keys = _load_apple_keys()

            jwk = next(
                (
                    key
                    for key in apple_keys
                    if key.get("kid") == kid
                ),
                None,
            )

        if not jwk:
            return None

        n = int.from_bytes(
            _b64url_decode(jwk["n"]),
            "big",
        )

        e = int.from_bytes(
            _b64url_decode(jwk["e"]),
            "big",
        )

        public_key = rsa.RSAPublicNumbers(
            e,
            n,
        ).public_key()

        signing_input = (
            f"{parts[0]}.{parts[1]}"
        ).encode("utf-8")

        signature = _b64url_decode(parts[2])

        public_key.verify(
            signature,
            signing_input,
            padding.PKCS1v15(),
            hashes.SHA256(),
        )

        if payload.get("iss") != APPLE_ISSUER:
            return None

        token_audience = payload.get("aud")

        if isinstance(token_audience, list):
            if expected_audience not in token_audience:
                return None
        elif token_audience != expected_audience:
            return None

        exp = payload.get("exp")
        if not isinstance(exp, (int, float)):
            return None

        if exp <= time.time():
            return None

        apple_sub = payload.get("sub")
        if not apple_sub:
            return None

        return payload

    except Exception as e:
        print("APPLE TOKEN VERIFY ERROR:", e)
        return None


app.include_router(build_barista_router(
    verify_google=verify_google_id_token,
    verify_apple=verify_apple_id_token,
    find_user=get_user_by_identity,
    parse_bearer=_get_bearer_token_or_401,
))


def validate_telegram_init_data(init_data: str, max_age_seconds: int = 86400):
    if not init_data:
        return None

    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
    except Exception:
        return None

    received_hash = parsed.pop("hash", None)
    if not received_hash:
        return None

    auth_date_raw = parsed.get("auth_date")
    if not auth_date_raw or not auth_date_raw.isdigit():
        return None

    auth_date = int(auth_date_raw)
    now_ts = int(datetime.now(timezone.utc).timestamp())

    if auth_date > now_ts + 60:
        return None

    if now_ts - auth_date > max_age_seconds:
        return None

    data_check_string = "\n".join(
        f"{key}={value}"
        for key, value in sorted(parsed.items())
    )

    secret_key = hmac.new(
        b"WebAppData",
        BOT_TOKEN.encode("utf-8"),
        hashlib.sha256,
    ).digest()

    calculated_hash = hmac.new(
        secret_key,
        data_check_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(calculated_hash, received_hash):
        return None

    user_raw = parsed.get("user")
    if not user_raw:
        return None

    try:
        user = json.loads(user_raw)
    except Exception:
        return None

    telegram_id = user.get("id")
    if not isinstance(telegram_id, int):
        return None

    return {
        "telegram_id": telegram_id,
        "user": user,
        "auth_date": auth_date,
    }


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
                JOIN coffee_shops cs ON cs.id = sa.shop_id
                WHERE u.telegram_user_id = %s
                  AND sa.role = 'owner' AND cs.is_active
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

@app.get("/account/{user_id}/qr")
def account_qr(user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT personal_qr_token
                FROM users
                WHERE id = %s
                """,
                (user_id,)
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
                WHERE client.telegram_user_id = %s AND cs.is_active
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

@app.get("/account/{user_id}/shops")
def account_shops(user_id: int):
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
                WHERE client.id = %s AND cs.is_active
                ORDER BY sc.last_activity_at DESC NULLS LAST, cs.name
                """,
                (user_id,)
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
        "shops": shops,
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

@app.get("/account/{user_id}/stats")
def account_stats(user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(sc.total_scans), 0) AS total_cups,
                    COALESCE(SUM(sc.free_coffee_balance), 0) AS total_free,
                    COUNT(sc.id) AS shops_count
                FROM shop_clients sc
                WHERE sc.user_id = %s
                """,
                (user_id,)
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
                WHERE cs.is_active
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


@app.post("/auth/telegram-miniapp")
def telegram_miniapp_auth(data: TelegramMiniAppAuthRequest):
    validated = validate_telegram_init_data(data.init_data)

    if not validated:
        return {
            "ok": False,
            "message": "Не вдалося підтвердити Telegram"
        }

    telegram_id = validated["telegram_id"]

    if not is_owner(telegram_id):
        return {
            "ok": False,
            "message": "У вас немає доступу до панелі кав’ярні"
        }

    return {
        "ok": True,
        "telegram_id": telegram_id
    }

class TestIdentityAuthRequest(BaseModel):
    provider: str
    provider_user_id: str = ""
    id_token: str | None = None
    action: str = "check"
    telegram_id: int | None = None

@app.post("/auth/test-identity")
def test_identity_auth(data: TestIdentityAuthRequest):
    provider = data.provider.strip().lower()
    provider_user_id = data.provider_user_id.strip()
    action = data.action.strip().lower()

    if provider not in {"apple", "google"}:
        return {
            "ok": False,
            "message": "Підтримуються тільки apple або google",
        }

    # Для Google больше не доверяем provider_user_id от телефона.
    # Проверяем настоящий Google ID Token и сами получаем Google sub.
    if provider == "google":
        payload = verify_google_id_token(data.id_token or "")

        if not payload:
            return {
                "ok": False,
                "message": "Invalid Google ID token",
            }

        provider_user_id = str(payload["sub"])
        

        
    # Для Apple тоже доверяем только проверенному ID Token.
    if provider == "apple":
        payload = verify_apple_id_token(data.id_token or "")

        if not payload:
            return {
                "ok": False,
                "message": "Invalid Apple ID token",
            }

        provider_user_id = str(payload["sub"])
    # -------------------------------------------------
    # 1. Проверяем, существует ли уже Apple/Google login
    # -------------------------------------------------

    existing_user = get_user_by_identity(
        provider,
        provider_user_id,
    )

    if existing_user:
        session_data = _create_login_response_session(
            existing_user["id"]
        )

        return {
            "ok": True,
            "status": "existing",
            "user_id": existing_user["id"],
            "telegram_user_id": existing_user["telegram_user_id"],
            "personal_qr_token": existing_user["personal_qr_token"],
            **session_data,
        }

    # -------------------------------------------------
    # 2. Только проверка.
    # НИКОГО пока не создаём.
    # -------------------------------------------------

    if action == "check":
        return {
            "ok": True,
            "status": "needs_onboarding",
            "message": "Потрібно вибрати: прив'язати Telegram або почати з нуля",
        }

    # -------------------------------------------------
    # 3. Пользователь выбрал "Привязать Telegram"
    # -------------------------------------------------

    if action == "link_telegram":
        return {
            "ok": False,
            "status": "requires_verified_link",
            "message": "Підтвердіть підключення через Telegram link flow",
        }

    # -------------------------------------------------
    # 4. Пользователь выбрал "Начать с нуля"
    # -------------------------------------------------

    if action == "create":

        result = create_user_with_identity(
            provider=provider,
            provider_user_id=provider_user_id,
        )

        user = result["user"]
        session_data = _create_login_response_session(
            user["id"]
        )

        return {
            "ok": True,
            "status": result["status"],
            "user_id": user["id"],
            "telegram_user_id": user["telegram_user_id"],
            "personal_qr_token": user["personal_qr_token"],
            **session_data,
        }

    return {
        "ok": False,
        "message": "Unknown action",
    }

# =========================================================
# TELEGRAM DEEP-LINK ACCOUNT LINK
# =========================================================

class TelegramLinkStartRequest(BaseModel):
    provider: str
    provider_user_id: str = ""
    id_token: str | None = None


class TelegramLinkConfirmRequest(BaseModel):
    token: str
    telegram_id: int | None = None
    init_data: str | None = None


class TelegramMergeStartRequest(BaseModel):
    current_user_id: int
    provider: str
    provider_user_id: str = ""
    id_token: str | None = None


def _telegram_link_token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


@app.post("/auth/telegram-link/start")
def telegram_link_start(data: TelegramLinkStartRequest):
    provider = data.provider.strip().lower()
    provider_user_id = data.provider_user_id.strip()

    if provider not in {"apple", "google"}:
        return {
            "ok": False,
            "message": "Некоректний спосіб входу",
        }

    # Для Google доверяем только проверенному ID Token.
    if provider == "google":
        payload = verify_google_id_token(data.id_token or "")

        if not payload:
            return {
                "ok": False,
                "message": "Invalid Google ID token",
            }

        provider_user_id = str(payload["sub"])

    # Для Apple тоже доверяем только проверенному ID Token.
    if provider == "apple":
        payload = verify_apple_id_token(data.id_token or "")

        if not payload:
            return {
                "ok": False,
                "message": "Invalid Apple ID token",
            }

        provider_user_id = str(payload["sub"])

    # Если этот Google/Apple уже привязан, новую сессию не создаём.
    existing_user = get_user_by_identity(
        provider,
        provider_user_id,
    )

    if existing_user:
        session_data = _create_login_response_session(
            existing_user["id"]
        )

        return {
            "ok": True,
            "status": "existing",
            "user_id": existing_user["id"],
            "telegram_user_id": existing_user["telegram_user_id"],
            "personal_qr_token": existing_user["personal_qr_token"],
            **session_data,
        }

    token = secrets.token_urlsafe(24)
    token_hash = _telegram_link_token_hash(token)
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Удаляем старые незавершённые сессии этого способа входа,
            # чтобы у пользователя была только одна актуальная ссылка.
            cur.execute(
                """
                DELETE FROM telegram_link_sessions
                WHERE provider = %s
                  AND provider_user_id = %s
                  AND status = 'pending'
                """,
                (provider, provider_user_id),
            )

            cur.execute(
                """
                INSERT INTO telegram_link_sessions (
                    token_hash,
                    provider,
                    provider_user_id,
                    mode,
                    status,
                    expires_at
                )
                VALUES (%s, %s, %s, 'link', 'pending', %s)
                """,
                (
                    token_hash,
                    provider,
                    provider_user_id,
                    expires_at,
                ),
            )

    deep_link = (
        "https://t.me/forYouMeCoffeBot"
        f"?start=link_{token}"
    )

    return {
        "ok": True,
        "status": "pending",
        "token": token,
        "deep_link": deep_link,
        "expires_in": 900,
    }


@app.post("/auth/telegram-merge/start")
def telegram_merge_start(data: TelegramMergeStartRequest):
    provider = data.provider.strip().lower()
    provider_user_id = data.provider_user_id.strip()

    if data.current_user_id <= 0:
        return {
            "ok": False,
            "message": "Некоректний поточний профіль",
        }

    if provider not in {"apple", "google"}:
        return {
            "ok": False,
            "message": "Некоректний спосіб входу",
        }

    # Для Google обязательно подтверждаем настоящий Google ID Token
    # и проверяем, что он принадлежит именно current_user_id.
    if provider == "google":
        payload = verify_google_id_token(data.id_token or "")

        if not payload:
            return {
                "ok": False,
                "message": "Invalid Google ID token",
            }

        provider_user_id = str(payload["sub"])

    # Для Apple тоже доверяем только проверенному ID Token.
    if provider == "apple":
        payload = verify_apple_id_token(data.id_token or "")

        if not payload:
            return {
                "ok": False,
                "message": "Invalid Apple ID token",
            }

        provider_user_id = str(payload["sub"])

    authenticated_user = get_user_by_identity(
        provider,
        provider_user_id,
    )

    if not authenticated_user:
        return {
            "ok": False,
            "message": "Поточний профіль входу не знайдено",
        }

    if authenticated_user["id"] != data.current_user_id:
        return {
            "ok": False,
            "message": "Спосіб входу не відповідає поточному профілю",
        }

    token = secrets.token_urlsafe(24)
    token_hash = _telegram_link_token_hash(token)
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM telegram_link_sessions
                WHERE provider = %s
                  AND provider_user_id = %s
                  AND status = 'pending'
                """,
                (provider, provider_user_id),
            )

            cur.execute(
                """
                INSERT INTO telegram_link_sessions (
                    token_hash,
                    provider,
                    provider_user_id,
                    mode,
                    current_user_id,
                    status,
                    expires_at
                )
                VALUES (%s, %s, %s, 'merge', %s, 'pending', %s)
                """,
                (
                    token_hash,
                    provider,
                    provider_user_id,
                    data.current_user_id,
                    expires_at,
                ),
            )

    deep_link = (
        "https://t.me/forYouMeCoffeBot"
        f"?start=link_{token}"
    )

    return {
        "ok": True,
        "status": "pending",
        "token": token,
        "deep_link": deep_link,
        "expires_in": 900,
    }


@app.get("/auth/telegram-link/status/{token}")
def telegram_link_status(token: str):
    clean_token = token.strip()

    if not clean_token:
        return {
            "ok": False,
            "status": "not_found",
            "message": "Сесію не знайдено",
        }

    token_hash = _telegram_link_token_hash(clean_token)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    status,
                    telegram_user_id,
                    user_id,
                    personal_qr_token,
                    expires_at
                FROM telegram_link_sessions
                WHERE token_hash = %s
                LIMIT 1
                """,
                (token_hash,),
            )
            row = cur.fetchone()

    if not row:
        return {
            "ok": False,
            "status": "not_found",
            "message": "Сесію не знайдено",
        }

    if (
        row["status"] == "pending"
        and datetime.now(timezone.utc) > row["expires_at"]
    ):
        return {
            "ok": True,
            "status": "expired",
            "message": "Час підтвердження завершився",
        }

    return {
        "ok": True,
        "status": row["status"],
        "user_id": row["user_id"],
        "telegram_user_id": row["telegram_user_id"],
        "personal_qr_token": row["personal_qr_token"],
    }


@app.post("/auth/telegram-link/confirm")
def telegram_link_confirm(
    data: TelegramLinkConfirmRequest,
    authorization: str | None = Header(default=None),
    x_telegram_init_data: str | None = Header(default=None),
):
    actor = get_verified_telegram_actor(
        authorization, x_telegram_init_data or data.init_data,
    )
    if data.telegram_id is not None and data.telegram_id != actor["telegram_id"]:
        raise HTTPException(403, detail={"code": "TELEGRAM_ACTOR_MISMATCH"})
    result = confirm_telegram_link_session(data.token, actor["telegram_id"])
    if result.get("status") in {"actor_mismatch", "unbound", "not_bound"}:
        raise HTTPException(403, detail={"code": "TELEGRAM_ACTOR_MISMATCH"})
    return result


# =========================================================
# APPLE REVIEW GUEST LOGIN
# =========================================================

@app.post("/auth/guest")
def guest_auth():
    guest_user_id = GUEST_USER_ID

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    telegram_user_id,
                    personal_qr_token
                FROM users
                WHERE id = %s
                LIMIT 1
                """,
                (guest_user_id,)
            )

            user = cur.fetchone()

    if not user:
        return {
            "ok": False,
            "message": "Гостьовий профіль не знайдено",
        }

    session_data = _create_login_response_session(
        user["id"]
    )

    return {
        "ok": True,
        "status": "guest",
        "user_id": user["id"],
        "telegram_user_id": user["telegram_user_id"],
        "personal_qr_token": user["personal_qr_token"],
        **session_data,
    }


# =========================================================
# AUTHENTICATED APP SESSION ENDPOINTS
# =========================================================

@app.post("/auth/logout")
def app_logout(
    authorization: str | None = Header(default=None),
):
    token = _get_bearer_token_or_401(authorization)
    session = get_app_session(token)

    if not session:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired session",
            headers={"WWW-Authenticate": "Bearer"},
        )

    revoked = revoke_app_session(token)

    return {
        "ok": True,
        "revoked": revoked,
    }


@app.get("/me")
def me(
    current_user=Depends(get_current_user),
):
    return {
        "ok": True,
        "user_id": current_user["user_id"],
        "telegram_user_id": current_user["telegram_user_id"],
        "username": current_user["username"],
        "full_name": current_user["full_name"],
        "personal_qr_token": current_user["personal_qr_token"],
    }


@app.get("/me/qr")
def me_qr(
    current_user=Depends(get_current_user),
):
    return account_qr(current_user["user_id"])


@app.get("/me/shops")
def me_shops(
    current_user=Depends(get_current_user),
):
    return account_shops(current_user["user_id"])


@app.get("/me/stats")
def me_stats(
    current_user=Depends(get_current_user),
):
    return account_stats(current_user["user_id"])


@app.delete("/me")
def delete_me(
    current_user=Depends(get_current_user),
):
    user_id = current_user["user_id"]

    if user_id == GUEST_USER_ID:
        raise HTTPException(
            status_code=403,
            detail="Guest account cannot be deleted",
        )

    try:
        result = delete_user_account(user_id)
    except HTTPException:
        # Shared-account safeguards (e.g. sole shop owner) stay actionable.
        raise
    except Exception as exc:
        print("DELETE ACCOUNT ERROR:", type(exc).__name__)
        raise HTTPException(
            status_code=500,
            detail="Unable to delete account",
        )

    if result["status"] == "user_not_found":
        raise HTTPException(
            status_code=404,
            detail="User not found",
        )

    return {
        "ok": True,
        "status": "deleted",
    }


class MeUnlinkIdentityRequest(BaseModel):
    provider: str


@app.post("/me/unlink-identity")
def unlink_my_identity(
    data: MeUnlinkIdentityRequest,
    current_user=Depends(get_current_user),
):
    provider = data.provider.strip().lower()

    if provider not in {"apple", "google"}:
        return {
            "ok": False,
            "status": "invalid_provider",
            "message": "Можна відв'язати тільки Apple або Google",
        }

    result = unlink_user_identity(
        user_id=current_user["user_id"],
        provider=provider,
    )

    status = result["status"]

    if status == "unlinked":
        return {
            "ok": True,
            "status": "unlinked",
            "message": "Спосіб входу успішно відв'язано",
        }

    if status == "not_linked":
        return {
            "ok": True,
            "status": "not_linked",
            "message": "Цей спосіб входу вже не прив'язаний",
        }

    if status == "last_identity":
        return {
            "ok": False,
            "status": "last_identity",
            "message": "Неможливо відв'язати єдиний спосіб входу",
        }

    if status == "user_not_found":
        raise HTTPException(
            status_code=404,
            detail="User not found",
        )

    return {
        "ok": False,
        "status": status,
        "message": "Не вдалося відв'язати спосіб входу",
    }


class UnlinkIdentityRequest(BaseModel):
    user_id: int
    provider: str


@app.post("/auth/unlink-identity")
def unlink_identity(
    data: UnlinkIdentityRequest,
    current_user=Depends(get_current_user),
):
    if data.user_id != current_user["user_id"]:
        raise HTTPException(403, detail={"code": "IDENTITY_OWNER_REQUIRED"})
    provider = data.provider.strip().lower()

    if provider not in {"apple", "google"}:
        return {
            "ok": False,
            "status": "invalid_provider",
            "message": "Можна відв'язати тільки Apple або Google",
        }

    result = unlink_user_identity(
        user_id=current_user["user_id"],
        provider=provider,
    )

    status = result["status"]

    if status == "unlinked":
        return {
            "ok": True,
            "status": "unlinked",
            "message": "Спосіб входу успішно відв'язано",
        }

    if status == "not_linked":
        return {
            "ok": True,
            "status": "not_linked",
            "message": "Цей спосіб входу вже не прив'язаний",
        }

    if status == "last_identity":
        return {
            "ok": False,
            "status": "last_identity",
            "message": "Неможливо відв'язати єдиний спосіб входу",
        }

    if status == "user_not_found":
        return {
            "ok": False,
            "status": "user_not_found",
            "message": "Користувача не знайдено",
        }

    return {
        "ok": False,
        "status": status,
        "message": "Не вдалося відв'язати спосіб входу",
    }


class LinkTelegramSendCodeRequest(BaseModel):
    telegram_id: str


class LinkTelegramVerifyRequest(BaseModel):
    telegram_id: str
    code: str
    provider: str
    provider_user_id: str
    id_token: str | None = None

class MergeTelegramVerifyRequest(BaseModel):
    telegram_id: str
    code: str
    current_user_id: int
    provider: str
    provider_user_id: str
    id_token: str | None = None


@app.post("/auth/link-telegram/send-code")
async def link_telegram_send_code(data: LinkTelegramSendCodeRequest):
    telegram_id = data.telegram_id.strip()

    if not telegram_id.isdigit():
        return {
            "ok": False,
            "message": "Некоректний Telegram ID",
        }

    telegram_user = get_user_by_identity(
        "telegram",
        telegram_id,
    )

    if not telegram_user:
        return {
            "ok": False,
            "message": "Профіль з таким Telegram ID не знайдено",
        }

    code = str(random.randint(1000, 9999))
    expires_at = datetime.utcnow() + timedelta(minutes=15)

    storage_key = f"link:{telegram_id}"

    codes_storage[storage_key] = {
        "code": code,
        "expires_at": expires_at,
    }

    bot = Bot(token=BOT_TOKEN)

    try:
        await bot.send_message(
            chat_id=int(telegram_id),
            text=(
                f"Код для прив’язки профілю «Наші»: {code}\n\n"
                "Код дійсний 15 хвилин."
            ),
        )

    except Exception as e:
        print("LINK TELEGRAM SEND ERROR:", e)

        return {
            "ok": False,
            "message": "Напишіть боту /start і спробуйте ще раз",
        }

    finally:
        await bot.session.close()

    return {
        "ok": True,
        "message": "Код надіслано у Telegram",
    }
@app.post("/auth/link-telegram/verify")
def link_telegram_verify(data: LinkTelegramVerifyRequest):

    telegram_id = data.telegram_id.strip()
    code = data.code.strip()
    provider = data.provider.strip().lower()
    provider_user_id = data.provider_user_id.strip()

    if provider not in {"apple", "google"}:
        return {
            "ok": False,
            "message": "Некоректний спосіб входу",
        }

    # Для Google не доверяем ID, который прислал телефон.
    # Проверяем настоящий Google ID Token.
    if provider == "google":
        payload = verify_google_id_token(data.id_token or "")

        if not payload:
            return {
                "ok": False,
                "message": "Invalid Google ID token",
            }

        provider_user_id = str(payload["sub"])

    # Apple пока оставляем по старой схеме.
    # Для Apple тоже доверяем только проверенному ID Token.
    if provider == "apple":
        payload = verify_apple_id_token(data.id_token or "")

        if not payload:
            return {
                "ok": False,
                "message": "Invalid Apple ID token",
            }

        provider_user_id = str(payload["sub"])

    storage_key = f"link:{telegram_id}"
    saved = codes_storage.get(storage_key)

    if not saved:
        return {
            "ok": False,
            "message": "Код не знайдено",
        }

    if datetime.utcnow() > saved["expires_at"]:
        del codes_storage[storage_key]

        return {
            "ok": False,
            "message": "Термін дії коду завершився",
        }

    if code != saved["code"]:
        return {
            "ok": False,
            "message": "Невірний код",
        }

    telegram_user = get_user_by_identity(
        "telegram",
        telegram_id,
    )

    if not telegram_user:
        return {
            "ok": False,
            "message": "Профіль Telegram не знайдено",
        }

    link_result = link_user_identity(
        telegram_user["id"],
        provider,
        provider_user_id,
    )

    if link_result["status"] not in {
        "linked",
        "already_linked",
    }:
        return {
            "ok": False,
            "status": link_result["status"],
            "message": "Не вдалося прив’язати профіль",
        }

    del codes_storage[storage_key]

    session_data = _create_login_response_session(
        telegram_user["id"]
    )

    return {
        "ok": True,
        "status": "linked",
        "message": "Профіль успішно прив’язано",
        "user_id": telegram_user["id"],
        "telegram_user_id": telegram_user["telegram_user_id"],
        "personal_qr_token": telegram_user["personal_qr_token"],
        **session_data,
    }


@app.post("/auth/merge-telegram/verify")
def merge_telegram_verify(data: MergeTelegramVerifyRequest):
    telegram_id = data.telegram_id.strip()
    code = data.code.strip()
    provider = data.provider.strip().lower()
    provider_user_id = data.provider_user_id.strip()

    if provider not in {"apple", "google"}:
        return {
            "ok": False,
            "message": "Некоректний спосіб входу",
        }

    if provider == "google":
        payload = verify_google_id_token(data.id_token or "")

        if not payload:
            return {
                "ok": False,
                "message": "Invalid Google ID token",
            }

        provider_user_id = str(payload["sub"])
        authenticated_user = get_user_by_identity(
            provider,
            provider_user_id,
        )

        if not authenticated_user:
            return {
                "ok": False,
                "message": "Google профіль не знайдено",
            }

        if authenticated_user["id"] != data.current_user_id:
            return {
                "ok": False,
                "message": "Google профіль не відповідає поточному користувачу",
            }

    if provider == "apple":
        payload = verify_apple_id_token(data.id_token or "")

        if not payload:
            return {
                "ok": False,
                "message": "Invalid Apple ID token",
            }

        provider_user_id = str(payload["sub"])

        authenticated_user = get_user_by_identity(
            provider,
            provider_user_id,
        )

        if not authenticated_user:
            return {
                "ok": False,
                "message": "Apple профіль не знайдено",
            }

        if authenticated_user["id"] != data.current_user_id:
            return {
                "ok": False,
                "message": "Apple профіль не відповідає поточному користувачу",
            }
    # -------------------------------------------------
    # 1. Проверяем Telegram ID
    # -------------------------------------------------
    # 1. Проверяем Telegram ID
    # -------------------------------------------------

    if not telegram_id.isdigit():
        return {
            "ok": False,
            "message": "Некоректний Telegram ID",
        }

    # -------------------------------------------------
    # 2. Проверяем код
    # -------------------------------------------------

    storage_key = f"link:{telegram_id}"
    saved = codes_storage.get(storage_key)

    if not saved:
        return {
            "ok": False,
            "message": "Код не знайдено",
        }

    if datetime.utcnow() > saved["expires_at"]:
        del codes_storage[storage_key]

        return {
            "ok": False,
            "message": "Термін дії коду завершився",
        }

    if code != saved["code"]:
        return {
            "ok": False,
            "message": "Невірний код",
        }

    # -------------------------------------------------
    # 3. Находим Telegram-профиль
    # -------------------------------------------------

    telegram_user = get_user_by_identity(
        "telegram",
        telegram_id,
    )

    if not telegram_user:
        return {
            "ok": False,
            "message": "Профіль Telegram не знайдено",
        }

    target_user_id = telegram_user["id"]

    # -------------------------------------------------
    # 4. Если это уже один и тот же аккаунт
    # -------------------------------------------------

    if data.current_user_id == target_user_id:
        del codes_storage[storage_key]

        session_data = _create_login_response_session(
            target_user_id
        )

        return {
            "ok": True,
            "status": "already_merged",
            "message": "Telegram вже підключено до цього профілю",
            "user_id": target_user_id,
            "telegram_user_id": telegram_user["telegram_user_id"],
            "personal_qr_token": telegram_user["personal_qr_token"],
            **session_data,
        }

    # -------------------------------------------------
    # 5. Объединяем аккаунты
    #
    # current_user_id = текущий Google/Apple users.id
    # target_user_id  = существующий Telegram users.id
    # -------------------------------------------------

    try:
        result = merge_users(
            source_user_id=data.current_user_id,
            target_user_id=target_user_id,
        )

    except Exception as e:
        print("MERGE USERS ERROR:", e)

        return {
            "ok": False,
            "status": "merge_error",
            "message": "Не вдалося об’єднати профілі",
        }

    status = result["status"]

    # -------------------------------------------------
    # 6. Обрабатываем результат
    # -------------------------------------------------

    if status not in {
        "merged",
        "already_merged",
    }:
        return {
            "ok": False,
            "status": status,
            "message": (
                "Не вдалося об’єднати профілі"
                if status != "provider_conflict"
                else "Ці профілі мають різні способи входу одного типу"
            ),
        }

    # Код удаляем только после успешного merge.
    del codes_storage[storage_key]

    # -------------------------------------------------
    # 7. Возвращаем итоговый Telegram users.id
    # -------------------------------------------------

    session_data = _create_login_response_session(
        target_user_id
    )

    return {
        "ok": True,
        "status": status,
        "message": "Профілі успішно об’єднано",
        "user_id": target_user_id,
        "telegram_user_id": telegram_user["telegram_user_id"],
        "personal_qr_token": telegram_user["personal_qr_token"],
        **session_data,
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
            "message": "Код не знайдено",
        }

    if datetime.utcnow() > saved["expires_at"]:
        return {
            "ok": False,
            "message": "Код протух",
        }

    if code != saved["code"]:
        return {
            "ok": False,
            "message": "Невірний код",
        }

    del codes_storage[telegram_id]

    telegram_user = get_user_by_identity(
        "telegram",
        telegram_id,
    )

    if not telegram_user:
        raise HTTPException(
            status_code=404,
            detail="Користувача Telegram не знайдено",
        )

    session_data = _create_login_response_session(
        telegram_user["id"]
    )

    return {
        "ok": True,
        "telegram_id": int(telegram_id),
        "is_superadmin": (
            int(telegram_id)
            in SUPERADMIN_TELEGRAM_IDS
        ),
        **session_data,
    }


# =========================================================
# APP PUSH DEVICES
# =========================================================

@app.post("/users/{user_id}/push-token")
def register_user_push_token(
    user_id: int,
    data: AppPushDeviceRequest,
):
    """
    Регистрирует обычный APNs device token приложения «Наші».
    Это НЕ Wallet pushToken.
    """

    clean_token = (data.device_token or "").strip()

    if not clean_token:
        raise HTTPException(
            status_code=400,
            detail="device_token is required",
        )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM users WHERE id = %s",
                (user_id,),
            )
            user = cur.fetchone()

    if not user:
        raise HTTPException(
            status_code=404,
            detail="User not found",
        )

    try:
        device = register_app_push_device(
            user_id=user_id,
            device_token=clean_token,
            environment=data.environment,
            platform="ios",
        )

        if not data.notifications_enabled:
            device = set_app_push_device_enabled(
                clean_token,
                False,
            )

    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        )

    return {
        "ok": True,
        "user_id": user_id,
        "environment": device["environment"],
        "notifications_enabled": device["notifications_enabled"],
    }
@app.post("/users/{user_id}/push-test")
async def send_test_app_push(
    user_id: int,
):
    devices = get_app_push_devices_for_user(
        user_id
    )

    if not devices:
        raise HTTPException(
            status_code=404,
            detail="No push devices registered",
        )

    result = await send_app_pushes(
        devices=devices,
        title="☕ Тестовий push від «Наші»",
        body=(
            "Все працює 🎉 "
            "Тепер застосунок може отримувати "
            "справжні push-сповіщення."
        ),
        data={
            "type": "test",
        },
    )

    return {
        "ok": True,
        "devices": len(devices),
        "sent": result.get("sent", 0),
        "failed": result.get("failed", 0),
        "errors": result.get("errors", []),
    }

@app.delete("/users/{user_id}/push-token")
def unregister_user_push_token(
    user_id: int,
    data: AppPushDeviceRemoveRequest,
):
    clean_token = (data.device_token or "").strip()

    if not clean_token:
        raise HTTPException(
            status_code=400,
            detail="device_token is required",
        )

    removed = unregister_app_push_device(
        clean_token
    )

    return {
        "ok": True,
        "removed": removed,
    }


@app.post("/me/push-token")
def register_my_push_token(
    data: AppPushDeviceRequest,
    current_user=Depends(get_current_user),
):
    return register_user_push_token(
        user_id=current_user["user_id"],
        data=data,
    )


@app.delete("/me/push-token")
def unregister_my_push_token(
    data: AppPushDeviceRemoveRequest,
    current_user=Depends(get_current_user),
):
    clean_token = (data.device_token or "").strip()

    if not clean_token:
        raise HTTPException(
            status_code=400,
            detail="device_token is required",
        )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM app_push_devices
                WHERE user_id = %s
                  AND device_token = %s
                LIMIT 1
                """,
                (
                    current_user["user_id"],
                    clean_token,
                ),
            )
            device = cur.fetchone()

    if not device:
        return {
            "ok": True,
            "removed": False,
        }

    removed = unregister_app_push_device(clean_token)

    return {
        "ok": True,
        "removed": removed,
    }


@app.get("/owner/shop/{owner_telegram_id}")
def owner_get_shop(owner_telegram_id: int):
    if get_owner_shop_id(owner_telegram_id) is None:
        raise HTTPException(403, detail="Кав’ярню закрито або доступ відсутній")
    return get_shop_profile(owner_telegram_id)
@app.post("/owner/shop/{owner_telegram_id}/submit-changes")
def owner_submit_shop_changes(
    owner_telegram_id: int,
    data: UpdateShopRequest,
):
    shop_id = get_owner_shop_id(owner_telegram_id)

    if not shop_id:
        raise HTTPException(
            status_code=403,
            detail="У вас немає доступу до цієї кавʼярні",
        )

    payload = {
        "name": data.name,
        "subtitle": data.subtitle,
        "address": data.address,
        "work_from": data.work_from,
        "work_to": data.work_to,
        "instagram": data.instagram,
        "description": data.description,
        "logo_url": data.logo_url,
        "cover_url": data.cover_url,
        "news": [
            item.dict()
            for item in data.news
        ],
    }

    payload_json = json.dumps(
        payload,
        ensure_ascii=False,
    )

    with get_connection() as conn:
        with conn.cursor() as cur:

            # Если у этой кофейни уже есть заявка,
            # которая ещё не проверена,
            # просто обновляем её.
            # Так не будет 10 pending-заявок подряд.
            cur.execute(
                """
                SELECT id
                FROM shop_change_requests
                WHERE shop_id = %s
                  AND status = 'pending'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (shop_id,),
            )

            pending_request = cur.fetchone()

            if pending_request:
                request_id = pending_request["id"]

                cur.execute(
                    """
                    UPDATE shop_change_requests
                    SET
                        submitted_by_telegram_id = %s,
                        payload = %s::jsonb,
                        reject_reason = NULL,
                        created_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        owner_telegram_id,
                        payload_json,
                        request_id,
                    ),
                )

            else:
                cur.execute(
                    """
                    INSERT INTO shop_change_requests (
                        shop_id,
                        submitted_by_telegram_id,
                        status,
                        payload
                    )
                    VALUES (
                        %s,
                        %s,
                        'pending',
                        %s::jsonb
                    )
                    RETURNING id
                    """,
                    (
                        shop_id,
                        owner_telegram_id,
                        payload_json,
                    ),
                )

                created = cur.fetchone()
                request_id = created["id"]

    return {
        "ok": True,
        "status": "pending",
        "request_id": request_id,
        "shop_id": shop_id,
        "message": (
            "Зміни збережено та надіслано "
            "на перевірку"
        ),
    }

@app.put("/owner/shop/{owner_telegram_id}")
async def owner_update_shop(
    owner_telegram_id: int,
    data: UpdateShopRequest
):
    if get_owner_shop_id(owner_telegram_id) is None:
        raise HTTPException(403, detail="Кав’ярню закрито або доступ відсутній")
    # До сохранения запоминаем текущие карточки "Новинки".
    # Сравниваем не количество карточек, а их содержимое.
    # Поэтому сценарий:
    #   удалили старую -> добавили новую -> сохранили
    # тоже даст push, даже если общее количество осталось тем же.
    old_profile = get_shop_profile(owner_telegram_id)

    old_news = []
    if old_profile and old_profile.get("ok"):
        old_news = (
            old_profile.get("shop", {}).get("news", [])
            or []
        )

    def news_fields(item):
        if isinstance(item, dict):
            title = str(item.get("title") or "").strip()
            price = str(item.get("price") or "").strip()
            image_url = str(item.get("image_url") or "").strip()
        else:
            title = str(getattr(item, "title", "") or "").strip()
            price = str(getattr(item, "price", "") or "").strip()
            image_url = str(getattr(item, "image_url", "") or "").strip()

        return title, price, image_url

    def is_meaningful_news(item):
        title, price, image_url = news_fields(item)
        return bool(title or price or image_url)

    def news_identity(item):
        """
        Идентичность новинки для определения именно новой карточки.

        Цена специально не участвует:
        изменение только цены не должно слать новый push.
        """
        title, _, image_url = news_fields(item)

        return (
            title.casefold(),
            image_url,
        )

    old_identities = {
        news_identity(item)
        for item in old_news
        if is_meaningful_news(item)
    }

    new_news_payload = [
        item.dict()
        for item in data.news
    ]

    new_meaningful = [
        item
        for item in new_news_payload
        if is_meaningful_news(item)
    ]

    newly_added_news = [
        item
        for item in new_meaningful
        if news_identity(item) not in old_identities
    ]

    result = update_shop_profile(
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
        news=new_news_payload,
    )

    # Push отправляем только если появилась реально новая
    # карточка. Удаление само по себе push не вызывает.
    if (
        result
        and result.get("ok")
        and newly_added_news
    ):
        try:
            shop_id = get_owner_shop_id(
                owner_telegram_id
            )

            if shop_id:
                settings = get_shop_reminder_settings(
                    shop_id
                )

                shop_news_enabled = bool(
                    settings
                    and settings.get(
                        "shop_news_enabled",
                        True,
                    )
                )

                if shop_news_enabled:
                    devices = (
                        get_app_push_devices_for_shop(
                            shop_id
                        )
                    )

                    newest_item = newly_added_news[-1]
                    news_title = (
                        str(
                            newest_item.get("title")
                            or ""
                        ).strip()
                    )

                    shop_name = (
                        (data.name or "").strip()
                        or "Кавʼярня"
                    )

                    if news_title:
                        body = (
                            f"{news_title}. "
                            f"Зазирни в «Наші» 👀"
                        )
                    else:
                        body = (
                            "Кавʼярня додала новинку — "
                            "зазирни в «Наші» 👀"
                        )

                    push_result = await send_app_pushes(
                        devices=devices,
                        title=(
                            f"📰 {shop_name} має новинку"
                        ),
                        body=body,
                        data={
                            "type": "shop_news",
                            "shop_id": shop_id,
                        },
                    )

                    print(
                        "🔔 SHOP NEWS PUSH:",
                        f"shop_id={shop_id}",
                        f"devices={len(devices)}",
                        f"sent={push_result.get('sent', 0)}",
                        f"failed={push_result.get('failed', 0)}",
                    )

        except Exception as exc:
            # Ошибка push никогда не должна мешать
            # сохранению профиля кофейни.
            print(
                "SHOP NEWS PUSH ERROR:",
                repr(exc),
            )

    return result


@app.get("/owner/shops/{owner_telegram_id}")
def owner_accessible_shops(
    owner_telegram_id: int,
    owner=Depends(require_owner_for_path),
):
    """List only current owner memberships using the established web auth proof."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id AS shop_id, name
                FROM coffee_shops
                WHERE id = ANY(%s)
                ORDER BY id
            """, (owner["owner_shop_ids"],))
            shops = cur.fetchall()
    return {
        "ok": True,
        "shops": [{"shop_id": row["shop_id"], "name": row["name"]} for row in shops],
        "selected_shop_id": shops[0]["shop_id"] if len(shops) == 1 else None,
    }


@app.get("/owner/analytics/{owner_telegram_id}/overview")
def owner_analytics_overview(
    owner_telegram_id: int,
    owner=Depends(require_owner_analytics_context),
):
    return get_owner_overview_stats(
        owner["telegram_id"], authorized_shop_id=owner["shop_id"]
    )


@app.get("/owner/analytics/{owner_telegram_id}/activity")
def owner_analytics_activity(
    owner_telegram_id: int,
    owner=Depends(require_owner_analytics_context),
):
    return get_owner_activity_stats(
        owner["telegram_id"], authorized_shop_id=owner["shop_id"]
    )


@app.get("/owner/analytics/{owner_telegram_id}/clients")
def owner_analytics_clients(
    owner_telegram_id: int,
    owner=Depends(require_owner_analytics_context),
):
    return get_owner_clients(
        owner["telegram_id"], authorized_shop_id=owner["shop_id"]
    )


@app.get("/owner/analytics/{owner_telegram_id}/details")
def owner_analytics_details(
    owner_telegram_id: int,
    owner=Depends(require_owner_analytics_context),
):
    return get_owner_details_stats(
        owner["telegram_id"], authorized_shop_id=owner["shop_id"]
    )

# =========================================================
# SUPERADMIN MODERATION
# =========================================================

@app.get("/superadmin/moderation/pending")
def superadmin_pending_requests(
    current_user=Depends(require_superadmin),
):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    scr.id,
                    scr.shop_id,
                    scr.submitted_by_telegram_id,
                    scr.status,
                    scr.payload,
                    scr.created_at,
                    cs.name AS shop_name
                FROM shop_change_requests scr
                LEFT JOIN coffee_shops cs
                    ON cs.id = scr.shop_id
                WHERE scr.status = 'pending'
                ORDER BY scr.created_at ASC
                """
            )

            rows = cur.fetchall()

    requests = []

    for row in rows:
        requests.append({
            "id": row["id"],
            "shop_id": row["shop_id"],
            "shop_name": row["shop_name"] or "",
            "submitted_by_telegram_id": (
                row["submitted_by_telegram_id"]
            ),
            "status": row["status"],
            "payload": row["payload"],
            "created_at": (
                row["created_at"].isoformat()
                if row["created_at"]
                else None
            ),
        })

    return {
        "ok": True,
        "count": len(requests),
        "requests": requests,
    }


@app.post(
    "/superadmin/moderation/{request_id}/approve"
)
async def superadmin_approve_request(
    request_id: int,
    current_user=Depends(require_superadmin),
):
    reviewer_telegram_id = int(
        current_user["telegram_user_id"]
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    scr.id,
                    scr.shop_id,
                    scr.status,
                    scr.payload,
                    owner.telegram_user_id AS owner_telegram_id
                FROM shop_change_requests scr

                LEFT JOIN shop_admins sa
                    ON sa.shop_id = scr.shop_id
                   AND sa.role = 'owner'

                LEFT JOIN users owner
                    ON owner.id = sa.user_id

                WHERE scr.id = %s
                LIMIT 1
                """,
                (request_id,),
            )

            request_row = cur.fetchone()

    if not request_row:
        raise HTTPException(
            status_code=404,
            detail="Заявку не знайдено",
        )

    if request_row["status"] != "pending":
        raise HTTPException(
            status_code=400,
            detail="Заявка вже оброблена",
        )

    owner_telegram_id = request_row[
        "owner_telegram_id"
    ]

    if not owner_telegram_id:
        raise HTTPException(
            status_code=400,
            detail="Власника кавʼярні не знайдено",
        )

    payload = request_row["payload"] or {}

    news_payload = payload.get("news") or []

    result = update_shop_profile(
        owner_telegram_id=owner_telegram_id,
        name=payload.get("name", ""),
        subtitle=payload.get("subtitle", ""),
        address=payload.get("address", ""),
        work_from=payload.get("work_from", ""),
        work_to=payload.get("work_to", ""),
        instagram=payload.get("instagram", ""),
        description=payload.get("description", ""),
        logo_url=payload.get("logo_url", ""),
        cover_url=payload.get("cover_url", ""),
        news=news_payload,
    )

    if not result or not result.get("ok"):
        raise HTTPException(
            status_code=500,
            detail="Не вдалося застосувати зміни",
        )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE shop_change_requests
                SET
                    status = 'approved',
                    reviewed_at = NOW(),
                    reviewed_by_telegram_id = %s,
                    reject_reason = NULL
                WHERE id = %s
                  AND status = 'pending'
                """,
                (
                    reviewer_telegram_id,
                    request_id,
                ),
            )

    return {
        "ok": True,
        "status": "approved",
        "request_id": request_id,
        "shop_id": request_row["shop_id"],
    }


@app.post(
    "/superadmin/moderation/{request_id}/reject"
)
def superadmin_reject_request(
    request_id: int,
    data: ModerationRejectRequest,
    current_user=Depends(require_superadmin),
):
    reviewer_telegram_id = int(
        current_user["telegram_user_id"]
    )

    reason = (data.reason or "").strip()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    shop_id,
                    status
                FROM shop_change_requests
                WHERE id = %s
                LIMIT 1
                """,
                (request_id,),
            )

            request_row = cur.fetchone()

            if not request_row:
                raise HTTPException(
                    status_code=404,
                    detail="Заявку не знайдено",
                )

            if request_row["status"] != "pending":
                raise HTTPException(
                    status_code=400,
                    detail="Заявка вже оброблена",
                )

            cur.execute(
                """
                UPDATE shop_change_requests
                SET
                    status = 'rejected',
                    reject_reason = %s,
                    reviewed_at = NOW(),
                    reviewed_by_telegram_id = %s
                WHERE id = %s
                  AND status = 'pending'
                """,
                (
                    reason,
                    reviewer_telegram_id,
                    request_id,
                ),
            )

    return {
        "ok": True,
        "status": "rejected",
        "request_id": request_id,
        "shop_id": request_row["shop_id"],
        "reason": reason,
    }
# =========================================================
# SUPERADMIN SHOPS MANAGEMENT
# =========================================================

@app.get("/superadmin/shops")
def superadmin_get_shops(
    current_user=Depends(require_superadmin),
):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    cs.id,
                    cs.name,
                    cs.city,
                    cs.address,
                    cs.subtitle,
                    cs.work_from,
                    cs.work_to,
                    cs.instagram,
                    cs.description,
                    cs.logo_url,
                    cs.cover_url,
                    cs.is_active,
                    cs.pending_owner_telegram_id,

                    (
                        SELECT u.telegram_user_id
                        FROM shop_admins sa
                        JOIN users u
                            ON u.id = sa.user_id
                        WHERE sa.shop_id = cs.id
                          AND sa.role = 'owner'
                        ORDER BY sa.id
                        LIMIT 1
                    ) AS owner_telegram_id

                FROM coffee_shops cs
                ORDER BY cs.name ASC, cs.id ASC
                """
            )

            rows = cur.fetchall()

    shops = []

    for row in rows:
        shops.append(
            {
                "shop_id": row["id"],
                "name": row["name"] or "",
                "city": row["city"] or "",
                "address": row["address"] or "",
                "subtitle": row["subtitle"] or "",
                "work_from": row["work_from"] or "",
                "work_to": row["work_to"] or "",
                "instagram": row["instagram"] or "",
                "description": row["description"] or "",
                "logo_url": row["logo_url"] or "",
                "cover_url": row["cover_url"] or "",
                "is_active": bool(row["is_active"]),
                "owner_telegram_id": (
                    row["owner_telegram_id"]
                    or row["pending_owner_telegram_id"]
                ),
            }
        )

    return {
        "ok": True,
        "count": len(shops),
        "shops": shops,
    }


@app.get("/superadmin/shops/{shop_id}")
def superadmin_get_shop(
    shop_id: int,
    current_user=Depends(require_superadmin),
):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    cs.id,
                    cs.name,
                    cs.city,
                    cs.address,
                    cs.subtitle,
                    cs.work_from,
                    cs.work_to,
                    cs.instagram,
                    cs.description,
                    cs.logo_url,
                    cs.cover_url,
                    cs.is_active,
                    cs.pending_owner_telegram_id,

                    (
                        SELECT u.telegram_user_id
                        FROM shop_admins sa
                        JOIN users u
                            ON u.id = sa.user_id
                        WHERE sa.shop_id = cs.id
                          AND sa.role = 'owner'
                        ORDER BY sa.id
                        LIMIT 1
                    ) AS owner_telegram_id

                FROM coffee_shops cs
                WHERE cs.id = %s
                LIMIT 1
                """,
                (shop_id,),
            )

            shop = cur.fetchone()

            if not shop:
                raise HTTPException(
                    status_code=404,
                    detail="Кавʼярню не знайдено",
                )

            cur.execute(
                """
                SELECT
                    id,
                    title,
                    price,
                    image_url,
                    sort_order
                FROM shop_news
                WHERE shop_id = %s
                ORDER BY sort_order ASC, id ASC
                """,
                (shop_id,),
            )

            news_rows = cur.fetchall()

    return {
        "ok": True,
        "shop": {
            "shop_id": shop["id"],
            "name": shop["name"] or "",
            "city": shop["city"] or "",
            "address": shop["address"] or "",
            "subtitle": shop["subtitle"] or "",
            "work_from": shop["work_from"] or "",
            "work_to": shop["work_to"] or "",
            "instagram": shop["instagram"] or "",
            "description": shop["description"] or "",
            "logo_url": shop["logo_url"] or "",
            "cover_url": shop["cover_url"] or "",
            "is_active": bool(shop["is_active"]),
            "owner_telegram_id": (
                shop["owner_telegram_id"]
                or shop["pending_owner_telegram_id"]
            ),
            "news": [
                {
                    "id": item["id"],
                    "title": item["title"] or "",
                    "price": item["price"] or "",
                    "image_url": item["image_url"] or "",
                    "sort_order": item["sort_order"] or 0,
                }
                for item in news_rows
            ],
        },
    }


@app.put("/superadmin/shops/{shop_id}")
def superadmin_update_shop(
    shop_id: int,
    data: UpdateShopRequest,
    current_user=Depends(require_superadmin),
):
    with get_connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM coffee_shops
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (shop_id,),
                )

                existing_shop = cur.fetchone()

                if not existing_shop:
                    raise HTTPException(
                        status_code=404,
                        detail="Кавʼярню не знайдено",
                    )

                cur.execute(
                    """
                    UPDATE coffee_shops
                    SET
                        name = %s,
                        subtitle = %s,
                        address = %s,
                        work_from = %s,
                        work_to = %s,
                        instagram = %s,
                        description = %s,
                        logo_url = %s,
                        cover_url = %s
                    WHERE id = %s
                    """,
                    (
                        data.name,
                        data.subtitle,
                        data.address,
                        data.work_from,
                        data.work_to,
                        data.instagram,
                        data.description,
                        data.logo_url,
                        data.cover_url,
                        shop_id,
                    ),
                )

                cur.execute(
                    """
                    DELETE FROM shop_news
                    WHERE shop_id = %s
                    """,
                    (shop_id,),
                )

                for index, item in enumerate(data.news):
                    cur.execute(
                        """
                        INSERT INTO shop_news (
                            shop_id,
                            title,
                            price,
                            image_url,
                            sort_order
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            shop_id,
                            item.title,
                            item.price,
                            item.image_url,
                            index,
                        ),
                    )

    return {
        "ok": True,
        "shop_id": shop_id,
        "status": "updated",
        "message": "Кавʼярню оновлено супер-адміністратором",
    }    
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

            "shop_news_enabled": settings.get(
                "shop_news_enabled",
                True,
            ),
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
        current_settings = get_shop_reminder_settings(
            shop_id
        )

        if data.shop_news_enabled is None:
            shop_news_enabled = bool(
                current_settings.get(
                    "shop_news_enabled",
                    True,
                )
            )
        else:
            shop_news_enabled = data.shop_news_enabled

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
            shop_news_enabled=shop_news_enabled,
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

            "shop_news_enabled": settings.get(
                "shop_news_enabled",
                True,
            ),
        }
    }


# =========================================================
# SHOP ADMINS
# =========================================================

@app.get("/owner/settings/{owner_telegram_id}/admins")
def owner_get_admins(owner_telegram_id: int, owner=Depends(require_owner_for_path)):
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
    data: AddAdminRequest,
    owner=Depends(require_owner_for_path),
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
    admin_telegram_id: int,
    owner=Depends(require_owner_for_path),
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
@app.get("/skins/{user_id}")
async def get_skins(user_id: int):
    if user_id == GUEST_USER_ID:
        stats = {
            "total_cups": 0,
            "total_free": 0,
            "shops_count": 0,
        }
    else:
        stats = get_wallet_user_stats(user_id)

        if not stats:
            raise HTTPException(
                status_code=404,
                detail="User not found",
            )

    result = []

    for skin in SKIN_CATALOG:
        progress = skin_progress(
            skin,
            total_cups=stats["total_cups"],
            total_free=stats["total_free"],
            shops_count=stats["shops_count"],
        )

        result.append(
            {
                "id": skin["id"],
                "title": skin["title"],
                "achievement": skin["achievement"],
                "metric": skin["metric"],
                "target": skin["target"],
                "current": progress["current"],
                "unlocked": progress["unlocked"],
                "card_color": skin["card_color"],
                "foreground_color": skin["foreground_color"],
                "image_url": (
                    "https://coffesassbot-production.up.railway.app"
                    f"/skin-assets/{skin['filename']}"
                ),
            }
        )

    return {
        "ok": True,
        "skins": result,
    }    
# persistence test
# postgres persistence test 2026-09-07
# =========================================================
# APPLE WALLET
# =========================================================


class WalletDesignRequest(BaseModel):
    design_id: str


class WalletPushTokenRequest(BaseModel):
    pushToken: str


class WalletLogRequest(BaseModel):
    logs: list[str]


def _wallet_authorized(
    authorization: str | None,
    authentication_token: str,
) -> bool:
    """
    Проверяет стандартный заголовок Apple Wallet:

    Authorization: ApplePass <authenticationToken>
    """

    if not authorization:
        return False

    expected = f"ApplePass {authentication_token}"

    return hmac.compare_digest(
        authorization.strip(),
        expected,
    )


def _build_wallet_pkpass(wallet_data):
    """
    Собирает актуальную версию карты пользователя.
    """

    stats = get_wallet_user_stats(
        wallet_data["user_id"]
    )

    return create_pkpass(
        user_id=wallet_data["user_id"],
        full_name=wallet_data["full_name"],
        personal_qr_token=wallet_data["personal_qr_token"],
        selected_design=wallet_data["selected_card_design"],
        total_cups=stats["total_cups"],
        total_free=stats["total_free"],
        shops_count=stats["shops_count"],
        authentication_token=wallet_data["authentication_token"],
    )


# ---------------------------------------------------------
# НАШЕ ПРИЛОЖЕНИЕ:
# первоначальное получение Wallet-карты
# ---------------------------------------------------------

@app.get("/wallet/pass/{user_id}")
async def download_wallet_pass(user_id: int):

    # Общему Apple Review guest-профилю
    # настоящую Wallet-карту не выдаём.
    if user_id == GUEST_USER_ID:
        raise HTTPException(
            status_code=403,
            detail="Wallet is unavailable in guest mode",
        )

    wallet_data = get_or_create_wallet_pass(user_id)

    if not wallet_data:
        raise HTTPException(
            status_code=404,
            detail="User not found",
        )

    try:
        pkpass = _build_wallet_pkpass(wallet_data)
    except Exception as exc:
        print(
            "WALLET BUILD ERROR:",
            repr(exc),
        )

        raise HTTPException(
            status_code=500,
            detail="Unable to create Wallet pass",
        )

    return Response(
        content=pkpass,
        media_type="application/vnd.apple.pkpass",
        headers={
            "Content-Disposition":
                'attachment; filename="nashi.pkpass"',
            "Cache-Control": "no-store",
        },
    )


# ---------------------------------------------------------
# НАШЕ ПРИЛОЖЕНИЕ:
# выбор дизайна карты
# ---------------------------------------------------------

@app.post("/wallet/pass/{user_id}/design")
async def update_wallet_design(
    user_id: int,
    payload: WalletDesignRequest,
):
    if user_id == GUEST_USER_ID:
        raise HTTPException(
            status_code=403,
            detail="Wallet is unavailable in guest mode",
        )

    design_id = (
        payload.design_id or ""
    ).strip().lower()

    remote_skin_ids = {
        skin["id"]
        for skin in SKIN_CATALOG
    }

    if (
        design_id not in VALID_CARD_DESIGNS
        and design_id not in remote_skin_ids
    ):
        raise HTTPException(
            status_code=400,
            detail="Invalid card design",
        )

    wallet_data = get_or_create_wallet_pass(user_id)

    if not wallet_data:
        raise HTTPException(
            status_code=404,
            detail="User not found",
        )

    result = set_wallet_card_design(
        user_id,
        design_id,
    )

    if result is None:
        raise HTTPException(
            status_code=400,
            detail="Unable to change card design",
        )

    updated_wallet = get_or_create_wallet_pass(user_id)

    serial_number = updated_wallet["serial_number"]

    push_tokens = get_wallet_push_tokens(
        serial_number
    )

    print(
        f"📲 WALLET UPDATE: "
        f"user_id={user_id} "
        f"design={design_id} "
        f"serial={serial_number} "
        f"devices={len(push_tokens)}"
    )

    push_result = await send_wallet_pushes(
        push_tokens
    )

    print(
        "📲 WALLET APNS RESULT:",
        f"sent={push_result.get('sent', 0)},",
        f"failed={push_result.get('failed', 0)}",
    )

    return {
        "ok": True,
        "design_id": design_id,
        "serial_number": serial_number,
        "update_tag": updated_wallet["update_tag"],
    }


@app.get("/me/wallet")
async def download_my_wallet_pass(
    current_user=Depends(get_current_user),
):
    return await download_wallet_pass(
        current_user["user_id"]
    )


@app.post("/me/wallet/design")
async def update_my_wallet_design(
    payload: WalletDesignRequest,
    current_user=Depends(get_current_user),
):
    return await update_wallet_design(
        user_id=current_user["user_id"],
        payload=payload,
    )


# ---------------------------------------------------------
# APPLE WALLET WEB SERVICE
#
# Регистрация устройства для обновлений.
# Apple вызывает этот endpoint автоматически после
# добавления карты в Wallet.
# ---------------------------------------------------------

@app.post(
    "/v1/devices/{device_library_identifier}"
    "/registrations/{pass_type_identifier}/{serial_number}"
)
async def wallet_register_device(
    device_library_identifier: str,
    pass_type_identifier: str,
    serial_number: str,
    body: WalletPushTokenRequest,
    authorization: str | None = Header(
        default=None,
        alias="Authorization",
    ),
):

    if pass_type_identifier != WALLET_PASS_TYPE_ID:
        raise HTTPException(
            status_code=404,
            detail="Pass not found",
        )

    wallet_data = get_wallet_pass_by_serial(
        serial_number
    )

    if not wallet_data:
        raise HTTPException(
            status_code=404,
            detail="Pass not found",
        )

    if not _wallet_authorized(
        authorization,
        wallet_data["authentication_token"],
    ):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
        )

    push_token = (body.pushToken or "").strip()

    if not push_token:
        raise HTTPException(
            status_code=400,
            detail="pushToken is required",
        )

    created = register_wallet_device(
        device_library_identifier,
        pass_type_identifier,
        serial_number,
        push_token,
    )

    # Apple ожидает 201 для новой регистрации
    # и 200, если регистрация уже существовала.
    return Response(
        status_code=201 if created else 200
    )


# ---------------------------------------------------------
# APPLE WALLET WEB SERVICE
# удаление регистрации
# ---------------------------------------------------------

@app.delete(
    "/v1/devices/{device_library_identifier}"
    "/registrations/{pass_type_identifier}/{serial_number}"
)
async def wallet_unregister_device(
    device_library_identifier: str,
    pass_type_identifier: str,
    serial_number: str,
    authorization: str | None = Header(
        default=None,
        alias="Authorization",
    ),
):

    if pass_type_identifier != WALLET_PASS_TYPE_ID:
        raise HTTPException(
            status_code=404,
            detail="Pass not found",
        )

    wallet_data = get_wallet_pass_by_serial(
        serial_number
    )

    if not wallet_data:
        raise HTTPException(
            status_code=404,
            detail="Pass not found",
        )

    if not _wallet_authorized(
        authorization,
        wallet_data["authentication_token"],
    ):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
        )

    unregister_wallet_device(
        device_library_identifier,
        pass_type_identifier,
        serial_number,
    )

    return Response(status_code=200)


# ---------------------------------------------------------
# APPLE WALLET WEB SERVICE
# какие карты на устройстве изменились
# ---------------------------------------------------------

@app.get(
    "/v1/devices/{device_library_identifier}"
    "/registrations/{pass_type_identifier}"
)
async def wallet_get_updated_passes(
    device_library_identifier: str,
    pass_type_identifier: str,
    passesUpdatedSince: str | None = None,
):

    if pass_type_identifier != WALLET_PASS_TYPE_ID:
        raise HTTPException(
            status_code=404,
            detail="Pass type not found",
        )

    updated_since = None

    if passesUpdatedSince:
        try:
            updated_since = int(
                passesUpdatedSince
            )
        except ValueError:
            updated_since = None

    result = get_wallet_device_serials(
        device_library_identifier,
        pass_type_identifier,
        updated_since,
    )

    serial_numbers = result["serial_numbers"]

    if not serial_numbers:
        return Response(status_code=204)

    return {
        "serialNumbers": serial_numbers,
        "lastUpdated": result["last_updated"],
    }


# ---------------------------------------------------------
# APPLE WALLET WEB SERVICE
# Wallet запрашивает новую версию .pkpass
# ---------------------------------------------------------

@app.get(
    "/v1/passes/{pass_type_identifier}/{serial_number}"
)
async def wallet_get_updated_pass(
    pass_type_identifier: str,
    serial_number: str,
    authorization: str | None = Header(
        default=None,
        alias="Authorization",
    ),
):

    if pass_type_identifier != WALLET_PASS_TYPE_ID:
        raise HTTPException(
            status_code=404,
            detail="Pass not found",
        )

    wallet_data = get_wallet_pass_by_serial(
        serial_number
    )

    if not wallet_data:
        raise HTTPException(
            status_code=404,
            detail="Pass not found",
        )

    if not _wallet_authorized(
        authorization,
        wallet_data["authentication_token"],
    ):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
        )

    try:
        pkpass = _build_wallet_pkpass(
            wallet_data
        )
    except Exception as exc:
        print(
            "WALLET UPDATE BUILD ERROR:",
            repr(exc),
        )

        raise HTTPException(
            status_code=500,
            detail="Unable to create Wallet pass",
        )

    last_modified = wallet_last_modified_http_date(
        wallet_data["updated_at"]
    )

    return Response(
        content=pkpass,
        media_type="application/vnd.apple.pkpass",
        headers={
            "Last-Modified": last_modified,
            "Cache-Control": "no-cache",
        },
    )


# ---------------------------------------------------------
# APPLE WALLET WEB SERVICE
# диагностические сообщения от Wallet
# ---------------------------------------------------------

@app.post("/v1/log")
async def wallet_log_messages(
    body: WalletLogRequest,
):

    for message in body.logs:
        print(
            "APPLE WALLET LOG:",
            message,
        )

    return Response(status_code=200)
