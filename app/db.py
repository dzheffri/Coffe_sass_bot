import uuid
from datetime import datetime, timezone

from psycopg import connect
from psycopg.rows import dict_row

from app.config import DATABASE_URL


def utc_now():
    return datetime.now(timezone.utc)


def get_connection():
    return connect(DATABASE_URL, autocommit=True, row_factory=dict_row)


def init_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                telegram_user_id BIGINT UNIQUE NOT NULL,
                username TEXT,
                full_name TEXT,
                personal_qr_token TEXT UNIQUE NOT NULL,
                active_shop_id BIGINT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS coffee_shops (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                city TEXT,
                address TEXT,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                pending_owner_telegram_id BIGINT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS shop_admins (
                id BIGSERIAL PRIMARY KEY,
                shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                role TEXT NOT NULL CHECK (role IN ('admin', 'owner')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(shop_id, user_id)
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS shop_clients (
                id BIGSERIAL PRIMARY KEY,
                shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                cups INTEGER NOT NULL DEFAULT 0,
                free_coffee_balance INTEGER NOT NULL DEFAULT 0,
                total_scans INTEGER NOT NULL DEFAULT 0,
                total_free_coffee_earned INTEGER NOT NULL DEFAULT 0,
                total_free_coffee_redeemed INTEGER NOT NULL DEFAULT 0,
                last_activity_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(shop_id, user_id)
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id BIGSERIAL PRIMARY KEY,
                shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                admin_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                type TEXT NOT NULL CHECK (type IN ('add_cups', 'redeem_free')),
                cups_added INTEGER NOT NULL DEFAULT 0,
                free_redeemed INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id BIGSERIAL PRIMARY KEY,
                shop_id BIGINT UNIQUE NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
                plan TEXT NOT NULL CHECK (plan IN ('trial', 'basic', 'pro')),
                status TEXT NOT NULL CHECK (status IN ('active', 'expired', 'blocked')),
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS broadcasts (
                id BIGSERIAL PRIMARY KEY,
                shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
                sender_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                text TEXT NOT NULL,
                recipients_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """)

            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_shop_clients_shop_user
            ON shop_clients(shop_id, user_id)
            """)

            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_shop_clients_last_activity
            ON shop_clients(shop_id, last_activity_at)
            """)

            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_transactions_shop_created
            ON transactions(shop_id, created_at)
            """)

            cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_broadcasts_shop_created
            ON broadcasts(shop_id, created_at)
            """)

            cur.execute("""
            ALTER TABLE coffee_shops
            ADD COLUMN IF NOT EXISTS pending_owner_telegram_id BIGINT NULL
            """)


init_db()


def ensure_user(telegram_user_id: int, username: str | None, full_name: str | None):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO users (telegram_user_id, username, full_name, personal_qr_token)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (telegram_user_id) DO UPDATE
            SET username = EXCLUDED.username,
                full_name = EXCLUDED.full_name
            RETURNING *
            """, (telegram_user_id, username, full_name, str(uuid.uuid4())))
            return cur.fetchone()


def get_user_by_telegram_id(telegram_user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE telegram_user_id = %s", (telegram_user_id,))
            return cur.fetchone()


def get_user_by_qr_token(token: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE personal_qr_token = %s", (token,))
            return cur.fetchone()


def set_active_shop(telegram_user_id: int, shop_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE users
            SET active_shop_id = %s
            WHERE telegram_user_id = %s
            """, (shop_id, telegram_user_id))


def get_active_shop_for_user(telegram_user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT cs.*
            FROM users u
            LEFT JOIN coffee_shops cs ON cs.id = u.active_shop_id
            WHERE u.telegram_user_id = %s
            """, (telegram_user_id,))
            return cur.fetchone()


def create_shop(name: str, city: str, address: str, pending_owner_telegram_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO coffee_shops (name, city, address, pending_owner_telegram_id)
            VALUES (%s, %s, %s, %s)
            RETURNING *
            """, (name, city, address, pending_owner_telegram_id))
            shop = cur.fetchone()

            cur.execute("""
            INSERT INTO subscriptions (shop_id, plan, status, expires_at)
            VALUES (%s, 'trial', 'active', NOW() + INTERVAL '7 days')
            RETURNING *
            """, (shop["id"],))
            return shop


def get_shop(shop_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM coffee_shops WHERE id = %s", (shop_id,))
            return cur.fetchone()


def get_all_shops():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM coffee_shops ORDER BY id")
            return cur.fetchall()


def get_user_shops(telegram_user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT cs.*, sc.cups, sc.free_coffee_balance, sc.last_activity_at
            FROM shop_clients sc
            JOIN users u ON u.id = sc.user_id
            JOIN coffee_shops cs ON cs.id = sc.shop_id
            WHERE u.telegram_user_id = %s
            ORDER BY cs.name
            """, (telegram_user_id,))
            return cur.fetchall()


def add_shop_admin(shop_id: int, admin_telegram_user_id: int, role: str):
    user = get_user_by_telegram_id(admin_telegram_user_id)
    if not user:
        return None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO shop_admins (shop_id, user_id, role)
            VALUES (%s, %s, %s)
            ON CONFLICT (shop_id, user_id) DO UPDATE
            SET role = EXCLUDED.role
            RETURNING *
            """, (shop_id, user["id"], role))

            result = cur.fetchone()

            if role == "owner":
                cur.execute("""
                UPDATE coffee_shops
                SET pending_owner_telegram_id = NULL
                WHERE id = %s
                """, (shop_id,))

            return result


def assign_pending_owner_if_exists(owner_telegram_user_id: int):
    user = get_user_by_telegram_id(owner_telegram_user_id)
    if not user:
        return []

    assigned_shop_ids = []

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT id
            FROM coffee_shops
            WHERE pending_owner_telegram_id = %s
            """, (owner_telegram_user_id,))
            shops = cur.fetchall()

            for shop in shops:
                cur.execute("""
                INSERT INTO shop_admins (shop_id, user_id, role)
                VALUES (%s, %s, 'owner')
                ON CONFLICT (shop_id, user_id) DO UPDATE
                SET role = 'owner'
                """, (shop["id"], user["id"]))

                cur.execute("""
                UPDATE coffee_shops
                SET pending_owner_telegram_id = NULL
                WHERE id = %s
                """, (shop["id"],))

                assigned_shop_ids.append(shop["id"])

    return assigned_shop_ids


def remove_shop_admin(shop_id: int, admin_telegram_user_id: int):
    user = get_user_by_telegram_id(admin_telegram_user_id)
    if not user:
        return False

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            DELETE FROM shop_admins
            WHERE shop_id = %s AND user_id = %s
            """, (shop_id, user["id"]))
            return cur.rowcount > 0


def get_admin_shop_and_role(admin_telegram_user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT cs.*, sa.role
            FROM shop_admins sa
            JOIN users u ON u.id = sa.user_id
            JOIN coffee_shops cs ON cs.id = sa.shop_id
            WHERE u.telegram_user_id = %s
            ORDER BY sa.id
            LIMIT 1
            """, (admin_telegram_user_id,))
            return cur.fetchone()


def is_owner(admin_telegram_user_id: int):
    row = get_admin_shop_and_role(admin_telegram_user_id)
    return bool(row and row["role"] == "owner")


def is_any_shop_admin(admin_telegram_user_id: int):
    return get_admin_shop_and_role(admin_telegram_user_id) is not None


def get_shop_admins(shop_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT u.telegram_user_id, u.full_name, u.username, sa.role
            FROM shop_admins sa
            JOIN users u ON u.id = sa.user_id
            WHERE sa.shop_id = %s
            ORDER BY sa.role DESC, u.telegram_user_id
            """, (shop_id,))
            return cur.fetchall()


def add_cups_for_shop_client(shop_id: int, client_user_id: int, admin_user_id: int, count: int):
    if count <= 0:
        raise ValueError("count must be > 0")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT *
            FROM shop_clients
            WHERE shop_id = %s AND user_id = %s
            """, (shop_id, client_user_id))
            row = cur.fetchone()

            if not row:
                cur.execute("""
                INSERT INTO shop_clients (shop_id, user_id, last_activity_at)
                VALUES (%s, %s, NOW())
                RETURNING *
                """, (shop_id, client_user_id))
                row = cur.fetchone()

            total_cups = row["cups"] + count
            earned_free = total_cups // 7
            remaining_cups = total_cups % 7

            cur.execute("""
            UPDATE shop_clients
            SET cups = %s,
                total_scans = total_scans + %s,
                free_coffee_balance = free_coffee_balance + %s,
                total_free_coffee_earned = total_free_coffee_earned + %s,
                last_activity_at = NOW()
            WHERE shop_id = %s AND user_id = %s
            RETURNING *
            """, (
                remaining_cups,
                count,
                earned_free,
                earned_free,
                shop_id,
                client_user_id
            ))
            updated = cur.fetchone()

            cur.execute("""
            INSERT INTO transactions (
                shop_id, user_id, admin_user_id, type, cups_added, free_redeemed
            ) VALUES (%s, %s, %s, 'add_cups', %s, 0)
            """, (shop_id, client_user_id, admin_user_id, count))

            return {
                "shop_client": updated,
                "earned_free": earned_free
            }


def redeem_free_for_shop_client(shop_id: int, client_user_id: int, admin_user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT *
            FROM shop_clients
            WHERE shop_id = %s AND user_id = %s
            """, (shop_id, client_user_id))
            row = cur.fetchone()

            if not row:
                return "NOT_FOUND"

            if row["free_coffee_balance"] <= 0:
                return "EMPTY"

            cur.execute("""
            UPDATE shop_clients
            SET free_coffee_balance = free_coffee_balance - 1,
                total_free_coffee_redeemed = total_free_coffee_redeemed + 1,
                last_activity_at = NOW()
            WHERE shop_id = %s AND user_id = %s
            RETURNING *
            """, (shop_id, client_user_id))
            updated = cur.fetchone()

            cur.execute("""
            INSERT INTO transactions (
                shop_id, user_id, admin_user_id, type, cups_added, free_redeemed
            ) VALUES (%s, %s, %s, 'redeem_free', 0, 1)
            """, (shop_id, client_user_id, admin_user_id))

            return updated


def get_shop_client_balance(shop_id: int, telegram_user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT sc.*, cs.name AS shop_name
            FROM shop_clients sc
            JOIN users u ON u.id = sc.user_id
            JOIN coffee_shops cs ON cs.id = sc.shop_id
            WHERE sc.shop_id = %s AND u.telegram_user_id = %s
            """, (shop_id, telegram_user_id))
            return cur.fetchone()


def get_shop_detailed_stats(shop_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT
                COUNT(*) AS total_clients,
                COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days') AS new_clients_30d,
                COUNT(*) FILTER (WHERE last_activity_at >= NOW() - INTERVAL '30 days') AS active_clients_30d,
                COALESCE(SUM(total_scans), 0) AS total_scans,
                COALESCE(SUM(free_coffee_balance), 0) AS free_balance,
                COALESCE(SUM(total_free_coffee_earned), 0) AS total_free_earned,
                COALESCE(SUM(total_free_coffee_redeemed), 0) AS total_free_redeemed
            FROM shop_clients
            WHERE shop_id = %s
            """, (shop_id,))
            base = cur.fetchone()

            cur.execute("""
            SELECT COALESCE(SUM(cups_added), 0) AS scans_today
            FROM transactions
            WHERE shop_id = %s
              AND type = 'add_cups'
              AND created_at >= CURRENT_DATE
            """, (shop_id,))
            today = cur.fetchone()

            cur.execute("""
            SELECT COALESCE(SUM(cups_added), 0) AS scans_30d
            FROM transactions
            WHERE shop_id = %s
              AND type = 'add_cups'
              AND created_at >= NOW() - INTERVAL '30 days'
            """, (shop_id,))
            month = cur.fetchone()

            return {
                "total_clients": base["total_clients"] or 0,
                "new_clients_30d": base["new_clients_30d"] or 0,
                "active_clients_30d": base["active_clients_30d"] or 0,
                "total_scans": base["total_scans"] or 0,
                "scans_today": today["scans_today"] or 0,
                "scans_30d": month["scans_30d"] or 0,
                "free_balance": base["free_balance"] or 0,
                "total_free_earned": base["total_free_earned"] or 0,
                "total_free_redeemed": base["total_free_redeemed"] or 0,
            }


def get_global_stats():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT COUNT(*) AS shops_count
            FROM coffee_shops
            """)
            shops = cur.fetchone()

            cur.execute("""
            SELECT COUNT(*) AS users_count
            FROM users
            """)
            users = cur.fetchone()

            cur.execute("""
            SELECT
                COALESCE(SUM(total_scans), 0) AS total_scans,
                COALESCE(SUM(free_coffee_balance), 0) AS free_balance,
                COALESCE(SUM(total_free_coffee_redeemed), 0) AS total_free_redeemed
            FROM shop_clients
            """)
            balances = cur.fetchone()

            cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'active' AND expires_at > NOW()) AS active_subscriptions,
                COUNT(*) FILTER (WHERE status != 'active' OR expires_at <= NOW()) AS expired_subscriptions
            FROM subscriptions
            """)
            subs = cur.fetchone()

            return {
                "shops_count": shops["shops_count"] or 0,
                "users_count": users["users_count"] or 0,
                "total_scans": balances["total_scans"] or 0,
                "free_balance": balances["free_balance"] or 0,
                "total_free_redeemed": balances["total_free_redeemed"] or 0,
                "active_subscriptions": subs["active_subscriptions"] or 0,
                "expired_subscriptions": subs["expired_subscriptions"] or 0,
            }


def get_subscription(shop_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT * FROM subscriptions WHERE shop_id = %s
            """, (shop_id,))
            return cur.fetchone()


def subscription_is_active(shop_id: int):
    sub = get_subscription(shop_id)
    if not sub:
        return False
    if sub["status"] != "active":
        return False
    return sub["expires_at"] > utc_now()


def extend_subscription(shop_id: int, days: int, plan: str = "basic"):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT * FROM subscriptions WHERE shop_id = %s
            """, (shop_id,))
            sub = cur.fetchone()

            if not sub:
                cur.execute("""
                INSERT INTO subscriptions (shop_id, plan, status, expires_at)
                VALUES (%s, %s, 'active', NOW() + (%s || ' days')::interval)
                RETURNING *
                """, (shop_id, plan, days))
                return cur.fetchone()

            cur.execute("""
            UPDATE subscriptions
            SET plan = %s,
                status = 'active',
                expires_at = GREATEST(expires_at, NOW()) + (%s || ' days')::interval
            WHERE shop_id = %s
            RETURNING *
            """, (plan, days, shop_id))
            return cur.fetchone()


def can_send_broadcast(shop_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT COUNT(*) AS cnt
            FROM broadcasts
            WHERE shop_id = %s
              AND created_at >= NOW() - INTERVAL '7 days'
            """, (shop_id,))
            row = cur.fetchone()
            return row["cnt"] < 2


def get_broadcast_recipients(shop_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT u.telegram_user_id
            FROM shop_clients sc
            JOIN users u ON u.id = sc.user_id
            WHERE sc.shop_id = %s
              AND sc.last_activity_at >= NOW() - INTERVAL '60 days'
            ORDER BY u.telegram_user_id
            """, (shop_id,))
            return cur.fetchall()


def save_broadcast(shop_id: int, sender_telegram_user_id: int, text: str, recipients_count: int):
    sender = get_user_by_telegram_id(sender_telegram_user_id)
    if not sender:
        return None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO broadcasts (shop_id, sender_user_id, text, recipients_count)
            VALUES (%s, %s, %s, %s)
            RETURNING *
            """, (shop_id, sender["id"], text, recipients_count))
            return cur.fetchone()