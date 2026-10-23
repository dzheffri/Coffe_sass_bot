"""Physical-shop QR designs and aggregate analytics; no customer identities."""

import json
import os
import re
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit


PUBLIC_BASE_DEFAULT = "https://coffee-admin-production-340d.up.railway.app"
TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_-]{32}\Z")
KINDS = {"telegram": "TELEGRAM_CLICK", "app_store": "APP_STORE_CLICK", "google_play": "GOOGLE_PLAY_CLICK"}


class QrError(Exception):
    def __init__(self, status: int, code: str):
        self.status = status
        self.code = code
        super().__init__(code)


def public_base() -> str:
    value = os.getenv("QR_PUBLIC_BASE_URL", PUBLIC_BASE_DEFAULT).strip().rstrip("/")
    parsed = urlsplit(value)
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.path not in ("", "/")):
        raise QrError(503, "QR_PUBLIC_URL_NOT_CONFIGURED")
    return value


def _store_url(value: str, host: str) -> str | None:
    try:
        parsed = urlsplit(value.strip())
        if (parsed.scheme == "https" and parsed.hostname == host and parsed.port in (None, 443)
                and not parsed.username and not parsed.password and not parsed.fragment
                and parsed.path not in ("", "/")):
            return value.strip()
    except ValueError:
        pass
    return None


def destinations() -> dict[str, str]:
    """Only server configuration may select a destination, never a public request."""
    result = {}
    username = os.getenv("TELEGRAM_BOT_USERNAME", "forYouMeCoffeBot").strip().lstrip("@")
    if re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
        result["telegram"] = f"https://t.me/{username}"
    for kind, environment, host in (
        ("app_store", "QR_APP_STORE_URL", "apps.apple.com"),
        ("google_play", "QR_GOOGLE_PLAY_URL", "play.google.com"),
    ):
        value = _store_url(os.getenv(environment, ""), host)
        if value:
            result[kind] = value
    return result


def platform_from_user_agent(value: str) -> str:
    agent = (value or "").lower()[:1024]
    if any(token in agent for token in ("iphone", "ipad", "ipod")) or ("macintosh" in agent and "mobile" in agent):
        return "ios"
    if "android" in agent:
        return "android"
    if any(token in agent for token in ("windows", "macintosh", "x11", "linux")):
        return "desktop"
    return "other"


def safe_image_url(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    # No remote fetches, protocols, traversal, query strings, SVG, or arbitrary paths.
    if re.fullmatch(r"/uploads/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+\.(?:png|jpe?g|webp)", value, re.I):
        return value
    raise ValueError("Only existing uploaded images are allowed")


def default_design() -> dict:
    return {
        "template": "minimal", "display_name": "", "title": "Приєднуйся до програми лояльності",
        "subtitle": "Скануй QR та обирай зручний спосіб", "footer": "Telegram або застосунок «Наші»",
        "background_mode": "color", "background_color": "#F6E9BA", "gradient_color": "#FFFFFF",
        "background_image_url": None, "background_dim": 0.0, "background_blur": 0,
        "background_scale": 1.0, "background_x": 50, "background_y": 50,
        "show_logo": True, "logo_url": None, "logo_size": 160, "logo_position": "top",
        "text_color": "dark", "text_size": "medium",
    }


class QrMaterialsRepository:
    def __init__(self, connection):
        self.connection = connection

    def shop(self, shop_id: int) -> dict:
        # JSON projection tolerates legacy schemas without network/image columns.
        row = self.connection.execute(
            """SELECT cs.id AS shop_id,cs.name,cs.is_active,
                to_jsonb(cs)->>'city' AS city,to_jsonb(cs)->>'logo_url' AS logo_url,
                to_jsonb(cs)->>'cover_url' AS cover_url,
                (to_jsonb(cs)->>'network_id')::bigint AS network_id,
                to_jsonb(cs)->>'deleted_at' AS deleted_at
                FROM coffee_shops cs WHERE cs.id=%s""", (shop_id,),
        ).fetchone()
        if row is None or row["deleted_at"] is not None:
            raise QrError(404, "QR_SHOP_NOT_FOUND")
        return {key: value for key, value in row.items() if key != "deleted_at"}

    def list_shops(self) -> list[dict]:
        rows = self.connection.execute(
            """SELECT cs.id AS shop_id,cs.name,cs.is_active,
                to_jsonb(cs)->>'city' AS city,to_jsonb(cs)->>'logo_url' AS logo_url,
                to_jsonb(cs)->>'cover_url' AS cover_url,
                (to_jsonb(cs)->>'network_id')::bigint AS network_id,
                qm.id IS NOT NULL AS has_material
                FROM coffee_shops cs LEFT JOIN shop_qr_materials qm ON qm.shop_id=cs.id
                WHERE to_jsonb(cs)->>'deleted_at' IS NULL ORDER BY cs.name,cs.id""",
        ).fetchall()
        return rows

    def material(self, shop_id: int, design: dict | None = None) -> dict:
        shop = self.shop(shop_id)
        token = secrets.token_urlsafe(24)
        row = self.connection.execute(
            """INSERT INTO shop_qr_materials(shop_id,public_token) VALUES(%s,%s)
                ON CONFLICT(shop_id) DO UPDATE SET shop_id=EXCLUDED.shop_id RETURNING *""", (shop_id, token),
        ).fetchone()
        if design is not None:
            row = self.connection.execute(
                "UPDATE shop_qr_materials SET design=%s::jsonb,updated_at=NOW() WHERE id=%s RETURNING *",
                (json.dumps(design, ensure_ascii=False), row["id"]),
            ).fetchone()
        stored = row["design"]
        if isinstance(stored, str):
            stored = json.loads(stored)
        path = f"/go/{row['public_token']}"
        return {"public_token": row["public_token"], "public_path": path,
                "public_url": public_base() + path, "design": {**default_design(), **stored}, "shop": shop,
                "available_destinations": list(destinations())}

    def public_material(self, token: str) -> dict:
        if not TOKEN_PATTERN.fullmatch(token):
            raise QrError(404, "QR_NOT_FOUND")
        row = self.connection.execute(
            """SELECT qm.id,qm.public_token,cs.name,to_jsonb(cs)->>'logo_url' AS logo_url
               FROM shop_qr_materials qm JOIN coffee_shops cs ON cs.id=qm.shop_id
               WHERE qm.public_token=%s AND cs.is_active
                 AND to_jsonb(cs)->>'deleted_at' IS NULL""", (token,),
        ).fetchone()
        if row is None:
            raise QrError(404, "QR_NOT_FOUND")
        return row

    def record_event(self, material_id: int, event_type: str, platform: str, event_id=None) -> bool:
        # Cross-worker, per-QR cap without retaining IP/user agents/customer IDs.
        self.connection.execute("SELECT pg_advisory_xact_lock(hashtextextended('qr-material:' || %s::text,0))", (material_id,))
        if event_id is not None and self.connection.execute(
            "SELECT 1 FROM shop_qr_events WHERE material_id=%s AND event_id=%s", (material_id, event_id)
        ).fetchone():
            return False
        recent = self.connection.execute(
            """SELECT COUNT(*) AS count FROM shop_qr_events
               WHERE material_id=%s AND created_at>NOW()-INTERVAL '1 minute'""", (material_id,),
        ).fetchone()["count"]
        if recent >= 120:
            raise QrError(429, "QR_EVENT_RATE_LIMITED")
        self.connection.execute(
            """INSERT INTO shop_qr_events(material_id,event_type,platform,event_id) VALUES(%s,%s,%s,%s)
               ON CONFLICT(material_id,event_id) WHERE event_id IS NOT NULL DO NOTHING""",
            (material_id, event_type, platform, event_id),
        )
        return True

    def stats(self, period: str, shop_id: int | None = None) -> dict:
        if shop_id is not None:
            self.shop(shop_id)
        since = None if period == "all" else datetime.now(timezone.utc) - timedelta(days=7 if period == "7d" else 30)
        rows = self.connection.execute(
            """SELECT cs.id AS shop_id,cs.name,(to_jsonb(cs)->>'network_id')::bigint AS network_id,
               COUNT(e.id) FILTER(WHERE e.event_type='QR_PAGE_OPEN') AS scans,
               COUNT(e.id) FILTER(WHERE e.event_type='TELEGRAM_CLICK') AS telegram,
               COUNT(e.id) FILTER(WHERE e.event_type='APP_STORE_CLICK') AS app_store,
               COUNT(e.id) FILTER(WHERE e.event_type='GOOGLE_PLAY_CLICK') AS google_play
               FROM coffee_shops cs LEFT JOIN shop_qr_materials qm ON qm.shop_id=cs.id
               LEFT JOIN shop_qr_events e ON e.material_id=qm.id AND (%s::timestamptz IS NULL OR e.created_at>=%s)
               WHERE (%s::bigint IS NULL OR cs.id=%s) AND to_jsonb(cs)->>'deleted_at' IS NULL
               GROUP BY cs.id ORDER BY cs.name,cs.id""", (since, since, shop_id, shop_id),
        ).fetchall()
        daily = self.connection.execute(
            """SELECT (e.created_at AT TIME ZONE 'UTC')::date AS date,COUNT(*) AS scans
               FROM shop_qr_events e JOIN shop_qr_materials qm ON qm.id=e.material_id
               JOIN coffee_shops cs ON cs.id=qm.shop_id
               WHERE e.event_type='QR_PAGE_OPEN' AND (%s::timestamptz IS NULL OR e.created_at>=%s)
                 AND (%s::bigint IS NULL OR qm.shop_id=%s) AND to_jsonb(cs)->>'deleted_at' IS NULL
               GROUP BY 1 ORDER BY 1""", (since, since, shop_id, shop_id),
        ).fetchall()
        keys = ("scans", "telegram", "app_store", "google_play")

        def converted(item):
            return {**item, "conversion": {key: item[key] / item["scans"] if item["scans"] else 0
                                          for key in keys if key != "scans"}}

        totals = converted({key: sum(int(item[key]) for item in rows) for key in keys})
        return {"period": period, "totals": totals, "shops": [converted(row) for row in rows],
                "daily": [{"date": row["date"].isoformat(), "scans": int(row["scans"])} for row in daily],
                **({"shop_id": shop_id} if shop_id is not None else {})}
