import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, "web_panel.db")


def get_web_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_web_panel_db():
    conn = get_web_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS shop_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_telegram_id INTEGER UNIQUE NOT NULL,
        name TEXT DEFAULT '',
        subtitle TEXT DEFAULT '',
        address TEXT DEFAULT '',
        work_from TEXT DEFAULT '',
        work_to TEXT DEFAULT '',
        instagram TEXT DEFAULT '',
        description TEXT DEFAULT '',
        logo_url TEXT DEFAULT '',
        cover_url TEXT DEFAULT '',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS shop_news (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_telegram_id INTEGER NOT NULL,
        title TEXT DEFAULT '',
        price TEXT DEFAULT '',
        image_url TEXT DEFAULT '',
        sort_order INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()
