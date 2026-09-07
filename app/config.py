import os
from dotenv import load_dotenv

load_dotenv()


def parse_super_admin_ids(value: str) -> list[int]:
    if not value:
        return []
    return [int(x.strip()) for x in value.split(",") if x.strip()]


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
SUPER_ADMIN_IDS = parse_super_admin_ids(os.getenv("SUPER_ADMIN_IDS", ""))
SCANNER_URL = os.getenv("SCANNER_URL", "").strip()
ADMIN_PANEL_URL = os.getenv(
    "ADMIN_PANEL_URL",
    "https://coffee-admin-production-340d.up.railway.app",
).strip()
SUBSCRIPTION_PRICE_USD = float(os.getenv("SUBSCRIPTION_PRICE_USD", "5").strip())

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set")

if not SCANNER_URL:
    raise ValueError("SCANNER_URL is not set")

if not ADMIN_PANEL_URL:
    raise ValueError("ADMIN_PANEL_URL is not set")
