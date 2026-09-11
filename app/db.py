import uuid
from math import ceil
from datetime import datetime, timezone

from psycopg import connect
from psycopg.rows import dict_row

from app.config import DATABASE_URL


TOUCH_TYPE_AUTO = "auto"
TOUCH_TYPE_BROADCAST = "broadcast"
TOUCH_TYPE_SERVICE = "service"

VALID_TOUCH_TYPES = (TOUCH_TYPE_AUTO, TOUCH_TYPE_BROADCAST, TOUCH_TYPE_SERVICE)


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
                    panel_mode TEXT NOT NULL DEFAULT 'auto',
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
                CREATE TABLE IF NOT EXISTS shop_reminder_settings (
                    shop_id BIGINT PRIMARY KEY
                        REFERENCES coffee_shops(id)
                        ON DELETE CASCADE,

                    one_left_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    one_left_days INTEGER NOT NULL DEFAULT 3
                        CHECK (one_left_days BETWEEN 1 AND 7),

                    free_coffee_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    free_coffee_days INTEGER NOT NULL DEFAULT 3
                        CHECK (free_coffee_days BETWEEN 1 AND 7),

                    inactive_5_7_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    inactive_5_7_days INTEGER NOT NULL DEFAULT 5
                        CHECK (inactive_5_7_days BETWEEN 1 AND 7),

                    inactive_14_30_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    inactive_14_30_days INTEGER NOT NULL DEFAULT 7
                        CHECK (inactive_14_30_days BETWEEN 1 AND 7),

                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
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
                CREATE TABLE IF NOT EXISTS reminder_logs (
                    id BIGSERIAL PRIMARY KEY,
                    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    reminder_type TEXT NOT NULL,
                    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS touch_logs (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
                    type TEXT NOT NULL CHECK (type IN ('auto', 'broadcast', 'service')),
                    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            # НОВАЯ таблица — фиксированные возвраты, чтобы статистика не прыгала
            cur.execute("""
                CREATE TABLE IF NOT EXISTS return_logs (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
                    touch_log_id BIGINT NOT NULL REFERENCES touch_logs(id) ON DELETE CASCADE,
                    touch_type TEXT NOT NULL CHECK (touch_type IN ('auto', 'broadcast')),
                    returned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(shop_id, user_id, touch_log_id)
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
                CREATE INDEX IF NOT EXISTS idx_reminder_logs_lookup
                ON reminder_logs(shop_id, user_id, reminder_type, sent_at DESC)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_touch_logs_shop_user_sent
                ON touch_logs(shop_id, user_id, sent_at DESC)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_touch_logs_shop_type_sent
                ON touch_logs(shop_id, type, sent_at DESC)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_return_logs_shop_returned
                ON return_logs(shop_id, returned_at DESC)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_return_logs_touch_type
                ON return_logs(shop_id, touch_type, returned_at DESC)
            """)

            cur.execute("""
                ALTER TABLE coffee_shops
                ADD COLUMN IF NOT EXISTS pending_owner_telegram_id BIGINT NULL
            """)

            cur.execute("""
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS panel_mode TEXT NOT NULL DEFAULT 'auto'
            """)
            cur.execute("""
                ALTER TABLE users
                ALTER COLUMN telegram_user_id DROP NOT NULL
            """)            

            cur.execute("""
                CREATE TABLE IF NOT EXISTS admin_login_tickets (
                    id BIGSERIAL PRIMARY KEY,
                    ticket TEXT UNIQUE NOT NULL,
                    telegram_user_id BIGINT NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL,
                    used_at TIMESTAMPTZ NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_admin_login_tickets_ticket
                ON admin_login_tickets(ticket)
            """)
            # Способы авторизации пользователя:
            # Telegram, Apple и другие в будущем.
            # Все они привязываются к внутреннему users.id.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_identities (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL
                        REFERENCES users(id)
                        ON DELETE CASCADE,
                    provider TEXT NOT NULL,
                    provider_user_id TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                    UNIQUE(provider, provider_user_id),
                    UNIQUE(user_id, provider)
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_identities_user_id
                ON user_identities(user_id)
            """)

            # Безопасно переносим существующие Telegram ID.
            # Пользователи, QR, чашки и подарки не изменяются.
            cur.execute("""
    INSERT INTO user_identities (
        user_id,
        provider,
        provider_user_id
    )
    SELECT
        id,
        'telegram',
        telegram_user_id::TEXT
    FROM users
    WHERE telegram_user_id IS NOT NULL
    ON CONFLICT (provider, provider_user_id) DO NOTHING
""")

init_db()


def ensure_user(
    telegram_user_id: int,
    username: str | None,
    full_name: str | None,
):
    with get_connection() as conn:
        with conn.cursor() as cur:

            # -------------------------------------------------
            # 1. Создаём пользователя или обновляем его данные
            # -------------------------------------------------

            cur.execute("""
                INSERT INTO users (
                    telegram_user_id,
                    username,
                    full_name,
                    personal_qr_token
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (telegram_user_id) DO UPDATE
                SET username = EXCLUDED.username,
                    full_name = EXCLUDED.full_name
                RETURNING *
            """, (
                telegram_user_id,
                username,
                full_name,
                str(uuid.uuid4()),
            ))

            user = cur.fetchone()

            # -------------------------------------------------
            # 2. Гарантируем Telegram identity
            #
            # Важно для Apple / Google / merge авторизации.
            # Даже старый пользователь после /start будет
            # автоматически восстановлен в user_identities.
            # -------------------------------------------------

            cur.execute("""
                INSERT INTO user_identities (
                    user_id,
                    provider,
                    provider_user_id
                )
                VALUES (%s, 'telegram', %s)
                ON CONFLICT (provider, provider_user_id)
                DO NOTHING
            """, (
                user["id"],
                str(telegram_user_id),
            ))

            return user


def get_user_by_telegram_id(telegram_user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM users WHERE telegram_user_id = %s",
                (telegram_user_id,),
            )
            return cur.fetchone()

def get_user_by_identity(provider: str, provider_user_id: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.*
                FROM user_identities ui
                JOIN users u ON u.id = ui.user_id
                WHERE ui.provider = %s
                  AND ui.provider_user_id = %s
                LIMIT 1
            """, (provider, str(provider_user_id)))

            return cur.fetchone()
def link_user_identity(user_id: int, provider: str, provider_user_id: str):
    clean_provider = (provider or "").strip().lower()
    clean_provider_user_id = str(provider_user_id or "").strip()

    if not clean_provider:
        raise ValueError("provider is required")

    if not clean_provider_user_id:
        raise ValueError("provider_user_id is required")

    with get_connection() as conn:
        with conn.cursor() as cur:

            # -------------------------------------------------
            # 1. Проверяем, привязан ли этот Apple/Google/Telegram ID
            # -------------------------------------------------

            cur.execute("""
                SELECT *
                FROM user_identities
                WHERE provider = %s
                  AND provider_user_id = %s
                LIMIT 1
            """, (
                clean_provider,
                clean_provider_user_id,
            ))

            existing_identity = cur.fetchone()

            if existing_identity:

                existing_user_id = existing_identity["user_id"]

                # Уже принадлежит именно этому users.id
                if existing_user_id == user_id:
                    return {
                        "status": "already_linked",
                        "identity": existing_identity,
                    }

                # -------------------------------------------------
                # 2. Apple / Google уже находится на другом users.id
                #
                # Разрешаем перенос ТОЛЬКО если тот аккаунт пустой.
                # Это как раз наш сценарий:
                #
                # пустой Apple users.id = 31257
                #            ↓
                # старый Telegram users.id с чашками
                # -------------------------------------------------

                if clean_provider in {"apple", "google"}:

                    # Есть ли у старого Apple/Google users.id
                    # данные программы лояльности?
                    cur.execute("""
                        SELECT COUNT(*) AS count
                        FROM shop_clients
                        WHERE user_id = %s
                    """, (existing_user_id,))

                    shop_clients_row = cur.fetchone()
                    shop_clients_count = (
                        shop_clients_row["count"]
                        if shop_clients_row
                        else 0
                    )

                    # Является ли этот users.id владельцем/админом кофейни?
                    cur.execute("""
                        SELECT COUNT(*) AS count
                        FROM shop_admins
                        WHERE user_id = %s
                    """, (existing_user_id,))

                    shop_admins_row = cur.fetchone()
                    shop_admins_count = (
                        shop_admins_row["count"]
                        if shop_admins_row
                        else 0
                    )

                    # Если на исходном аккаунте уже есть реальные данные —
                    # автоматически объединять его нельзя.
                    if (
                        shop_clients_count > 0
                        or shop_admins_count > 0
                    ):
                        return {
                            "status": "identity_in_use",
                            "identity": existing_identity,
                        }

                    # Проверяем, нет ли уже Apple/Google
                    # такого же типа на целевом Telegram users.id.
                    cur.execute("""
                        SELECT *
                        FROM user_identities
                        WHERE user_id = %s
                          AND provider = %s
                        LIMIT 1
                    """, (
                        user_id,
                        clean_provider,
                    ))

                    target_provider = cur.fetchone()

                    if target_provider:
                        return {
                            "status": "provider_already_linked",
                            "identity": target_provider,
                        }

                    # -------------------------------------------------
                    # Переносим Apple / Google identity
                    # с пустого users.id на существующий Telegram users.id.
                    #
                    # Сам Telegram-профиль, QR, чашки и подарки
                    # вообще не трогаем.
                    # -------------------------------------------------

                    cur.execute("""
                        UPDATE user_identities
                        SET user_id = %s
                        WHERE id = %s
                        RETURNING *
                    """, (
                        user_id,
                        existing_identity["id"],
                    ))

                    moved_identity = cur.fetchone()

                    return {
                        "status": "linked",
                        "identity": moved_identity,
                    }

                # Telegram identity автоматически переносить нельзя.
                return {
                    "status": "identity_in_use",
                    "identity": existing_identity,
                }

            # -------------------------------------------------
            # 3. Проверяем:
            # у одного внутреннего аккаунта одна identity каждого типа
            # -------------------------------------------------

            cur.execute("""
                SELECT *
                FROM user_identities
                WHERE user_id = %s
                  AND provider = %s
                LIMIT 1
            """, (
                user_id,
                clean_provider,
            ))

            existing_provider = cur.fetchone()

            if existing_provider:
                return {
                    "status": "provider_already_linked",
                    "identity": existing_provider,
                }

            # -------------------------------------------------
            # 4. Обычная новая привязка
            # -------------------------------------------------

            cur.execute("""
                INSERT INTO user_identities (
                    user_id,
                    provider,
                    provider_user_id
                )
                VALUES (%s, %s, %s)
                RETURNING *
            """, (
                user_id,
                clean_provider,
                clean_provider_user_id,
            ))

            return {
                "status": "linked",
                "identity": cur.fetchone(),
            }

def merge_users(source_user_id: int, target_user_id: int):
    """
    Объединяет два users.id.

    source_user_id:
        текущий Apple / Google профиль.

    target_user_id:
        старый Telegram профиль, который становится основным.

    Все бонусы и история source переносятся в target.
    """

    if source_user_id == target_user_id:
        return {
            "status": "already_merged",
            "user_id": target_user_id,
        }

    # ВАЖНО:
    # get_connection() обычно autocommit=True.
    # Для merge нужна одна настоящая транзакция.
    conn = get_connection()

    try:
        conn.autocommit = False

        with conn.cursor() as cur:

            # -------------------------------------------------
            # 1. Блокируем оба аккаунта на время merge
            # -------------------------------------------------

            cur.execute("""
                SELECT *
                FROM users
                WHERE id IN (%s, %s)
                ORDER BY id
                FOR UPDATE
            """, (
                source_user_id,
                target_user_id,
            ))

            users = cur.fetchall()

            if len(users) != 2:
                conn.rollback()
                return {
                    "status": "user_not_found",
                }

            # -------------------------------------------------
            # 2. Проверяем identities.
            #
            # Нельзя получить два разных Google или два разных
            # Apple на одном итоговом users.id.
            # -------------------------------------------------

            cur.execute("""
                SELECT *
                FROM user_identities
                WHERE user_id IN (%s, %s)
                ORDER BY id
            """, (
                source_user_id,
                target_user_id,
            ))

            identities = cur.fetchall()

            source_identities = {
                row["provider"]: row
                for row in identities
                if row["user_id"] == source_user_id
            }

            target_identities = {
                row["provider"]: row
                for row in identities
                if row["user_id"] == target_user_id
            }

            for provider, source_identity in source_identities.items():
                target_identity = target_identities.get(provider)

                if (
                    target_identity
                    and target_identity["provider_user_id"]
                    != source_identity["provider_user_id"]
                ):
                    conn.rollback()
                    return {
                        "status": "provider_conflict",
                        "provider": provider,
                    }

            # -------------------------------------------------
            # 3. shop_clients
            #
            # Если оба аккаунта были в одной кофейне —
            # складываем прогресс.
            #
            # cups может стать >= 7, поэтому сразу превращаем
            # полные семёрки в бесплатный кофе.
            # -------------------------------------------------

            cur.execute("""
                SELECT *
                FROM shop_clients
                WHERE user_id = %s
                ORDER BY id
            """, (source_user_id,))

            source_clients = cur.fetchall()

            for source_client in source_clients:

                cur.execute("""
                    SELECT *
                    FROM shop_clients
                    WHERE shop_id = %s
                      AND user_id = %s
                    LIMIT 1
                    FOR UPDATE
                """, (
                    source_client["shop_id"],
                    target_user_id,
                ))

                target_client = cur.fetchone()

                if target_client:

                    combined_cups = (
                        target_client["cups"]
                        + source_client["cups"]
                    )

                    extra_free = combined_cups // 7
                    remaining_cups = combined_cups % 7

                    combined_free_balance = (
                        target_client["free_coffee_balance"]
                        + source_client["free_coffee_balance"]
                        + extra_free
                    )

                    combined_total_scans = (
                        target_client["total_scans"]
                        + source_client["total_scans"]
                    )

                    combined_earned = (
                        target_client["total_free_coffee_earned"]
                        + source_client["total_free_coffee_earned"]
                        + extra_free
                    )

                    combined_redeemed = (
                        target_client["total_free_coffee_redeemed"]
                        + source_client["total_free_coffee_redeemed"]
                    )

                    created_at = min(
                        target_client["created_at"],
                        source_client["created_at"],
                    )

                    last_activity_at = max(
                        target_client["last_activity_at"],
                        source_client["last_activity_at"],
                    )

                    cur.execute("""
                        UPDATE shop_clients
                        SET cups = %s,
                            free_coffee_balance = %s,
                            total_scans = %s,
                            total_free_coffee_earned = %s,
                            total_free_coffee_redeemed = %s,
                            created_at = %s,
                            last_activity_at = %s
                        WHERE id = %s
                    """, (
                        remaining_cups,
                        combined_free_balance,
                        combined_total_scans,
                        combined_earned,
                        combined_redeemed,
                        created_at,
                        last_activity_at,
                        target_client["id"],
                    ))

                    cur.execute("""
                        DELETE FROM shop_clients
                        WHERE id = %s
                    """, (
                        source_client["id"],
                    ))

                else:
                    cur.execute("""
                        UPDATE shop_clients
                        SET user_id = %s
                        WHERE id = %s
                    """, (
                        target_user_id,
                        source_client["id"],
                    ))

            # -------------------------------------------------
            # 4. История операций
            # -------------------------------------------------

            cur.execute("""
                UPDATE transactions
                SET user_id = %s
                WHERE user_id = %s
            """, (
                target_user_id,
                source_user_id,
            ))

            # Если source когда-либо был бариста/админом,
            # сохраняем и ссылки admin_user_id в истории.
            cur.execute("""
                UPDATE transactions
                SET admin_user_id = %s
                WHERE admin_user_id = %s
            """, (
                target_user_id,
                source_user_id,
            ))

            # -------------------------------------------------
            # 5. reminder_logs
            # -------------------------------------------------

            cur.execute("""
                UPDATE reminder_logs
                SET user_id = %s
                WHERE user_id = %s
            """, (
                target_user_id,
                source_user_id,
            ))

            # -------------------------------------------------
            # 6. touch_logs
            # -------------------------------------------------

            cur.execute("""
                UPDATE touch_logs
                SET user_id = %s
                WHERE user_id = %s
            """, (
                target_user_id,
                source_user_id,
            ))

            # -------------------------------------------------
            # 7. return_logs
            #
            # Здесь есть UNIQUE(shop_id, user_id, touch_log_id).
            # Сначала удаляем потенциальные дубли.
            # -------------------------------------------------

            cur.execute("""
                DELETE FROM return_logs source
                USING return_logs target
                WHERE source.user_id = %s
                  AND target.user_id = %s
                  AND source.shop_id = target.shop_id
                  AND source.touch_log_id = target.touch_log_id
            """, (
                source_user_id,
                target_user_id,
            ))

            cur.execute("""
                UPDATE return_logs
                SET user_id = %s
                WHERE user_id = %s
            """, (
                target_user_id,
                source_user_id,
            ))

            # -------------------------------------------------
            # 8. broadcasts
            # -------------------------------------------------

            cur.execute("""
                UPDATE broadcasts
                SET sender_user_id = %s
                WHERE sender_user_id = %s
            """, (
                target_user_id,
                source_user_id,
            ))

            # -------------------------------------------------
            # 9. shop_admins
            #
            # UNIQUE(shop_id, user_id), поэтому сначала
            # разбираем каждую запись source отдельно.
            # -------------------------------------------------

            cur.execute("""
                SELECT *
                FROM shop_admins
                WHERE user_id = %s
                ORDER BY id
            """, (source_user_id,))

            source_admins = cur.fetchall()

            for source_admin in source_admins:

                cur.execute("""
                    SELECT *
                    FROM shop_admins
                    WHERE shop_id = %s
                      AND user_id = %s
                    LIMIT 1
                """, (
                    source_admin["shop_id"],
                    target_user_id,
                ))

                target_admin = cur.fetchone()

                if target_admin:

                    # owner важнее admin
                    final_role = (
                        "owner"
                        if (
                            source_admin["role"] == "owner"
                            or target_admin["role"] == "owner"
                        )
                        else "admin"
                    )

                    cur.execute("""
                        UPDATE shop_admins
                        SET role = %s
                        WHERE id = %s
                    """, (
                        final_role,
                        target_admin["id"],
                    ))

                    cur.execute("""
                        DELETE FROM shop_admins
                        WHERE id = %s
                    """, (
                        source_admin["id"],
                    ))

                else:

                    cur.execute("""
                        UPDATE shop_admins
                        SET user_id = %s
                        WHERE id = %s
                    """, (
                        target_user_id,
                        source_admin["id"],
                    ))

            # -------------------------------------------------
            # 10. Переносим Apple / Google identities
            # -------------------------------------------------

            for provider, source_identity in source_identities.items():

                target_identity = target_identities.get(provider)

                if target_identity:
                    # Одинаковая identity уже есть на target.
                    # Source-копию можно удалить.
                    cur.execute("""
                        DELETE FROM user_identities
                        WHERE id = %s
                    """, (
                        source_identity["id"],
                    ))

                else:
                    cur.execute("""
                        UPDATE user_identities
                        SET user_id = %s
                        WHERE id = %s
                    """, (
                        target_user_id,
                        source_identity["id"],
                    ))

            # -------------------------------------------------
            # 11. Удаляем уже пустой source users.id
            # -------------------------------------------------

            cur.execute("""
                DELETE FROM users
                WHERE id = %s
            """, (
                source_user_id,
            ))

            conn.commit()

            return {
                "status": "merged",
                "user_id": target_user_id,
            }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()
def unlink_user_identity(user_id: int, provider: str):
    clean_provider = (provider or "").strip().lower()

    if clean_provider not in {"apple", "google"}:
        return {
            "status": "invalid_provider"
        }

    with get_connection() as conn:
        with conn.cursor() as cur:

            # Проверяем, существует ли пользователь.
            cur.execute("""
                SELECT *
                FROM users
                WHERE id = %s
                LIMIT 1
            """, (user_id,))

            user = cur.fetchone()

            if not user:
                return {
                    "status": "user_not_found"
                }

            # Ищем конкретную Apple / Google привязку.
            cur.execute("""
                SELECT *
                FROM user_identities
                WHERE user_id = %s
                  AND provider = %s
                LIMIT 1
            """, (
                user_id,
                clean_provider,
            ))

            identity = cur.fetchone()

            if not identity:
                return {
                    "status": "not_linked"
                }

            # Проверяем, останется ли хотя бы один другой способ
            # восстановить доступ к этому users.id.
            cur.execute("""
                SELECT COUNT(*) AS count
                FROM user_identities
                WHERE user_id = %s
                  AND id <> %s
            """, (
                user_id,
                identity["id"],
            ))

            row = cur.fetchone()
            remaining_identities = row["count"] if row else 0

            if remaining_identities == 0:
                return {
                    "status": "last_identity"
                }

            # Удаляем ТОЛЬКО Apple / Google identity.
            # users, QR, чашки, подарки, история и Telegram
            # не изменяются.
            cur.execute("""
                DELETE FROM user_identities
                WHERE id = %s
                RETURNING *
            """, (identity["id"],))

            deleted_identity = cur.fetchone()

            return {
                "status": "unlinked",
                "identity": deleted_identity,
            }

           
def create_user_with_identity(
    provider: str,
    provider_user_id: str,
    username: str | None = None,
    full_name: str | None = None,
):
    clean_provider = (provider or "").strip().lower()
    clean_provider_user_id = str(provider_user_id or "").strip()

    if not clean_provider:
        raise ValueError("provider is required")

    if not clean_provider_user_id:
        raise ValueError("provider_user_id is required")

    existing_user = get_user_by_identity(
        clean_provider,
        clean_provider_user_id,
    )

    if existing_user:
        return {
            "status": "existing",
            "user": existing_user,
        }

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (
                    telegram_user_id,
                    username,
                    full_name,
                    personal_qr_token
                )
                VALUES (NULL, %s, %s, %s)
                RETURNING *
            """, (
                username,
                full_name,
                str(uuid.uuid4()),
            ))

            user = cur.fetchone()

            cur.execute("""
                INSERT INTO user_identities (
                    user_id,
                    provider,
                    provider_user_id
                )
                VALUES (%s, %s, %s)
                RETURNING *
            """, (
                user["id"],
                clean_provider,
                clean_provider_user_id,
            ))

            identity = cur.fetchone()

            return {
                "status": "created",
                "user": user,
                "identity": identity,
            }
def get_user_by_qr_token(token: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM users WHERE personal_qr_token = %s",
                (token,),
            )
            return cur.fetchone()


def get_panel_mode(telegram_user_id: int) -> str:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT panel_mode
                FROM users
                WHERE telegram_user_id = %s
            """, (telegram_user_id,))
            row = cur.fetchone()

    if not row:
        return "auto"

    value = row["panel_mode"] or "auto"
    if value not in ("auto", "super_admin", "owner"):
        return "auto"
    return value


def set_panel_mode(telegram_user_id: int, mode: str):
    if mode not in ("auto", "super_admin", "owner"):
        raise ValueError("invalid panel mode")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users
                SET panel_mode = %s
                WHERE telegram_user_id = %s
            """, (mode, telegram_user_id))


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
                JOIN coffee_shops cs ON cs.id = u.active_shop_id
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
def get_all_shops_with_subscriptions():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    cs.*,
                    s.plan,
                    s.status AS subscription_status,
                    s.expires_at,
                    CEIL(EXTRACT(EPOCH FROM (s.expires_at - NOW())) / 86400.0)::int AS days_left
                FROM coffee_shops cs
                LEFT JOIN subscriptions s ON s.shop_id = cs.id
                ORDER BY cs.id
            """)
            return cur.fetchall()


def get_owners_for_subscription_last_day():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    cs.id AS shop_id,
                    cs.name AS shop_name,
                    s.expires_at,
                    u.id AS user_id,
                    u.telegram_user_id
                FROM subscriptions s
                JOIN coffee_shops cs ON cs.id = s.shop_id
                JOIN shop_admins sa ON sa.shop_id = cs.id AND sa.role = 'owner'
                JOIN users u ON u.id = sa.user_id
                WHERE s.status = 'active'
                  AND s.expires_at > NOW()
                  AND s.expires_at <= NOW() + INTERVAL '1 day'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM reminder_logs rl
                      WHERE rl.shop_id = cs.id
                        AND rl.user_id = u.id
                        AND rl.reminder_type = 'subscription_last_day'
                        AND rl.sent_at >= NOW() - INTERVAL '2 days'
                  )
                ORDER BY s.expires_at
            """)
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


def save_touch_log(shop_id: int, user_id: int, touch_type: str, sent_at=None):
    if touch_type not in VALID_TOUCH_TYPES:
        raise ValueError("invalid touch type")

    with get_connection() as conn:
        with conn.cursor() as cur:
            if sent_at is None:
                cur.execute("""
                    INSERT INTO touch_logs (user_id, shop_id, type)
                    VALUES (%s, %s, %s)
                    RETURNING *
                """, (user_id, shop_id, touch_type))
            else:
                cur.execute("""
                    INSERT INTO touch_logs (user_id, shop_id, type, sent_at)
                    VALUES (%s, %s, %s, %s)
                    RETURNING *
                """, (user_id, shop_id, touch_type, sent_at))
            return cur.fetchone()


def get_last_marketing_touch(shop_id: int, user_id: int, days: int = 7):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM touch_logs
                WHERE shop_id = %s
                  AND user_id = %s
                  AND type IN ('auto', 'broadcast')
                  AND sent_at >= NOW() - (%s || ' days')::interval
                ORDER BY sent_at DESC
                LIMIT 1
            """, (shop_id, user_id, days))
            return cur.fetchone()


def save_return_log(shop_id: int, user_id: int, touch_log_id: int, touch_type: str):
    if touch_type not in ("auto", "broadcast"):
        return None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO return_logs (user_id, shop_id, touch_log_id, touch_type)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (shop_id, user_id, touch_log_id) DO NOTHING
                RETURNING *
            """, (user_id, shop_id, touch_log_id, touch_type))
            return cur.fetchone()


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

            last_touch = get_last_marketing_touch(shop_id=shop_id, user_id=client_user_id, days=7)

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
                client_user_id,
            ))
            updated = cur.fetchone()

            cur.execute("""
                INSERT INTO transactions (
                    shop_id, user_id, admin_user_id, type, cups_added, free_redeemed
                ) VALUES (%s, %s, %s, 'add_cups', %s, 0)
            """, (shop_id, client_user_id, admin_user_id, count))

            cur.execute("""
                INSERT INTO touch_logs (user_id, shop_id, type)
                VALUES (%s, %s, 'service')
            """, (client_user_id, shop_id))

            saved_return = None
            if last_touch:
                saved_return = save_return_log(
                    shop_id=shop_id,
                    user_id=client_user_id,
                    touch_log_id=last_touch["id"],
                    touch_type=last_touch["type"],
                )

            return {
                "shop_client": updated,
                "earned_free": earned_free,
                "last_touch": last_touch,
                "return_source": last_touch["type"] if saved_return else None,
                "saved_return": saved_return,
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

            # Ищем последнее маркетинговое сообщение клиенту
            # за последние 7 дней.
            last_touch = get_last_marketing_touch(
                shop_id=shop_id,
                user_id=client_user_id,
                days=7
            )

            # Списываем бесплатный кофе
            cur.execute("""
                UPDATE shop_clients
                SET free_coffee_balance = free_coffee_balance - 1,
                    total_free_coffee_redeemed = total_free_coffee_redeemed + 1,
                    last_activity_at = NOW()
                WHERE shop_id = %s AND user_id = %s
                RETURNING *
            """, (shop_id, client_user_id))

            updated = cur.fetchone()

            # Сохраняем операцию списания
            cur.execute("""
                INSERT INTO transactions (
                    shop_id,
                    user_id,
                    admin_user_id,
                    type,
                    cups_added,
                    free_redeemed
                )
                VALUES (%s, %s, %s, 'redeem_free', 0, 1)
            """, (
                shop_id,
                client_user_id,
                admin_user_id
            ))

            # Сам визит тоже фиксируем как service-touch
            cur.execute("""
                INSERT INTO touch_logs (
                    user_id,
                    shop_id,
                    type
                )
                VALUES (%s, %s, 'service')
            """, (
                client_user_id,
                shop_id
            ))

            # Если перед визитом была авто-рассылка
            # или собственная рассылка владельца,
            # считаем этот визит возвратом.
            if last_touch:
                save_return_log(
                    shop_id=shop_id,
                    user_id=client_user_id,
                    touch_log_id=last_touch["id"],
                    touch_type=last_touch["type"],
                )

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


def get_shop_client_balance_by_user_id(shop_id: int, user_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT sc.*, cs.name AS shop_name
                FROM shop_clients sc
                JOIN coffee_shops cs ON cs.id = sc.shop_id
                WHERE sc.shop_id = %s AND sc.user_id = %s
            """, (shop_id, user_id))
            return cur.fetchone()


def get_shop_detailed_stats(shop_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_clients,
                    COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '30 days') AS new_clients_30d,
                    COUNT(*) FILTER (WHERE last_activity_at >= NOW() - INTERVAL '30 days') AS active_clients_30d,
                    COUNT(*) FILTER (WHERE last_activity_at < NOW() - INTERVAL '7 days') AS inactive_gt_7d,
                    COALESCE(SUM(total_scans), 0) AS total_scans,
                    COALESCE(SUM(free_coffee_balance), 0) AS free_balance,
                    COALESCE(SUM(total_free_coffee_earned), 0) AS total_free_earned,
                    COALESCE(SUM(total_free_coffee_redeemed), 0) AS total_free_redeemed
                FROM shop_clients
                WHERE shop_id = %s
            """, (shop_id,))
            base = cur.fetchone()

            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE type IN ('auto', 'broadcast')) AS sent_total,
                    COUNT(*) FILTER (WHERE type = 'auto') AS sent_auto,
                    COUNT(*) FILTER (WHERE type = 'broadcast') AS sent_broadcast
                FROM touch_logs
                WHERE shop_id = %s
                  AND sent_at >= NOW() - INTERVAL '30 days'
            """, (shop_id,))
            touches = cur.fetchone()

            cur.execute("""
                SELECT
                    COUNT(*) AS returns_total,
                    COUNT(*) FILTER (WHERE touch_type = 'auto') AS returns_auto,
                    COUNT(*) FILTER (WHERE touch_type = 'broadcast') AS returns_broadcast
                FROM return_logs
                WHERE shop_id = %s
                  AND returned_at >= NOW() - INTERVAL '30 days'
            """, (shop_id,))
            returns_30d = cur.fetchone()

            cur.execute("""
                SELECT COUNT(*) AS returned_7d
                FROM return_logs
                WHERE shop_id = %s
                  AND returned_at >= NOW() - INTERVAL '7 days'
            """, (shop_id,))
            returned_7d = cur.fetchone()

            cur.execute("""
                SELECT COUNT(*) AS returned_today
                FROM return_logs
                WHERE shop_id = %s
                  AND returned_at >= CURRENT_DATE
            """, (shop_id,))
            returned_today = cur.fetchone()

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

    sent_total = touches["sent_total"] or 0
    returns_total = returns_30d["returns_total"] or 0
    efficiency = round((returns_total / sent_total) * 100, 2) if sent_total else 0.0

    return {
        "sent_total": sent_total,
        "sent_auto": touches["sent_auto"] or 0,
        "sent_broadcast": touches["sent_broadcast"] or 0,
        "returns_total": returns_total,
        "returns_auto": returns_30d["returns_auto"] or 0,
        "returns_broadcast": returns_30d["returns_broadcast"] or 0,
        "efficiency_percent": efficiency,
        "returned_today": returned_today["returned_today"] or 0,
        "returned_7d": returned_7d["returned_7d"] or 0,
        "inactive_gt_7d": base["inactive_gt_7d"] or 0,
        "active_clients_30d": base["active_clients_30d"] or 0,
        "scans_30d": month["scans_30d"] or 0,
        "scans_today": today["scans_today"] or 0,
        "total_scans": base["total_scans"] or 0,
        "free_balance": base["free_balance"] or 0,
        "total_free_earned": base["total_free_earned"] or 0,
        "total_free_redeemed": base["total_free_redeemed"] or 0,
        "total_clients": base["total_clients"] or 0,
        "new_clients_30d": base["new_clients_30d"] or 0,
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


def get_super_admin_clients_stats():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    cs.id AS shop_id,
                    cs.name AS shop_name,
                    COUNT(sc.id) AS clients_count
                FROM coffee_shops cs
                LEFT JOIN shop_clients sc ON sc.shop_id = cs.id
                GROUP BY cs.id, cs.name
                ORDER BY cs.name
            """)
            per_shop = cur.fetchall()

            cur.execute("""
                SELECT COUNT(*) AS total_clients
                FROM shop_clients
            """)
            total = cur.fetchone()

    return {
        "per_shop": per_shop,
        "total_clients": total["total_clients"] or 0,
    }


def get_subscription(shop_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM subscriptions
                WHERE shop_id = %s
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
                SELECT *
                FROM subscriptions
                WHERE shop_id = %s
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
            return row["cnt"] < 8


def get_broadcast_recipients(shop_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.telegram_user_id, sc.user_id
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


def log_broadcast_touches(shop_id: int, user_ids: list[int]):
    if not user_ids:
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            for user_id in user_ids:
                cur.execute("""
                    INSERT INTO touch_logs (user_id, shop_id, type)
                    VALUES (%s, %s, 'broadcast')
                """, (user_id, shop_id))


def was_reminder_sent_recently(shop_id: int, user_id: int, reminder_type: str, days: int) -> bool:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1
                FROM reminder_logs
                WHERE shop_id = %s
                  AND user_id = %s
                  AND reminder_type = %s
                  AND sent_at >= NOW() - (%s || ' days')::interval
                LIMIT 1
            """, (shop_id, user_id, reminder_type, days))
            return cur.fetchone() is not None


def save_reminder_log(shop_id: int, user_id: int, reminder_type: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reminder_logs (shop_id, user_id, reminder_type)
                VALUES (%s, %s, %s)
            """, (shop_id, user_id, reminder_type))


def save_auto_touch(shop_id: int, user_id: int):
    return save_touch_log(shop_id=shop_id, user_id=user_id, touch_type=TOUCH_TYPE_AUTO)


def save_service_touch(shop_id: int, user_id: int):
    return save_touch_log(shop_id=shop_id, user_id=user_id, touch_type=TOUCH_TYPE_SERVICE)


def save_broadcast_touch(shop_id: int, user_id: int):
    return save_touch_log(shop_id=shop_id, user_id=user_id, touch_type=TOUCH_TYPE_BROADCAST)


def get_clients_for_one_left_reminder():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    sc.shop_id,
                    sc.user_id,
                    sc.cups,
                    sc.free_coffee_balance,
                    sc.last_activity_at,
                    u.telegram_user_id,
                    u.full_name,
                    cs.name AS shop_name
                FROM shop_clients sc
                JOIN users u ON u.id = sc.user_id
                JOIN coffee_shops cs ON cs.id = sc.shop_id
                JOIN subscriptions s ON s.shop_id = sc.shop_id
                WHERE sc.cups = 6
                  AND sc.free_coffee_balance = 0
                  AND s.status = 'active'
                  AND s.expires_at > NOW()
            """)
            return cur.fetchall()


def get_clients_with_free_coffee():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    sc.shop_id,
                    sc.user_id,
                    sc.cups,
                    sc.free_coffee_balance,
                    sc.last_activity_at,
                    u.telegram_user_id,
                    u.full_name,
                    cs.name AS shop_name
                FROM shop_clients sc
                JOIN users u ON u.id = sc.user_id
                JOIN coffee_shops cs ON cs.id = sc.shop_id
                JOIN subscriptions s ON s.shop_id = sc.shop_id
                WHERE sc.free_coffee_balance > 0
                  AND s.status = 'active'
                  AND s.expires_at > NOW()
            """)
            return cur.fetchall()


def get_clients_for_inactive_reminder(days_from: int, days_to: int | None = None):
    with get_connection() as conn:
        with conn.cursor() as cur:
            if days_to is None:
                cur.execute("""
                    SELECT
                        sc.shop_id,
                        sc.user_id,
                        sc.cups,
                        sc.free_coffee_balance,
                        sc.last_activity_at,
                        u.telegram_user_id,
                        u.full_name,
                        cs.name AS shop_name
                    FROM shop_clients sc
                    JOIN users u ON u.id = sc.user_id
                    JOIN coffee_shops cs ON cs.id = sc.shop_id
                    JOIN subscriptions s ON s.shop_id = sc.shop_id
                    WHERE sc.last_activity_at < NOW() - (%s || ' days')::interval
                      AND s.status = 'active'
                      AND s.expires_at > NOW()
                """, (days_from,))
            else:
                cur.execute("""
                    SELECT
                        sc.shop_id,
                        sc.user_id,
                        sc.cups,
                        sc.free_coffee_balance,
                        sc.last_activity_at,
                        u.telegram_user_id,
                        u.full_name,
                        cs.name AS shop_name
                    FROM shop_clients sc
                    JOIN users u ON u.id = sc.user_id
                    JOIN coffee_shops cs ON cs.id = sc.shop_id
                    JOIN subscriptions s ON s.shop_id = sc.shop_id
                    WHERE sc.last_activity_at < NOW() - (%s || ' days')::interval
                      AND sc.last_activity_at >= NOW() - (%s || ' days')::interval
                      AND s.status = 'active'
                      AND s.expires_at > NOW()
                """, (days_from, days_to))

            return cur.fetchall()


def delete_shop(shop_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM coffee_shops
                WHERE id = %s
                RETURNING *
            """, (shop_id,))
            return cur.fetchone()


def get_all_users():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users ORDER BY id")
            return cur.fetchall()


def get_all_shop_admins():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shop_admins ORDER BY id")
            return cur.fetchall()


def get_all_shop_clients():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shop_clients ORDER BY id")
            return cur.fetchall()


def get_all_transactions():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM transactions ORDER BY id")
            return cur.fetchall()


def get_all_subscriptions():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM subscriptions ORDER BY id")
            return cur.fetchall()


def get_all_broadcasts():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM broadcasts ORDER BY id")
            return cur.fetchall()


def get_all_reminder_logs():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM reminder_logs ORDER BY id")
            return cur.fetchall()


def get_all_touch_logs():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM touch_logs ORDER BY id")
            return cur.fetchall()


def get_all_return_logs():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM return_logs ORDER BY id")
            return cur.fetchall()

def get_shop_reminder_settings(shop_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO shop_reminder_settings (shop_id)
                VALUES (%s)
                ON CONFLICT (shop_id) DO NOTHING
            """, (shop_id,))

            cur.execute("""
                SELECT *
                FROM shop_reminder_settings
                WHERE shop_id = %s
            """, (shop_id,))

            return cur.fetchone()


def update_shop_reminder_settings(
    shop_id: int,
    one_left_enabled: bool,
    one_left_days: int,
    free_coffee_enabled: bool,
    free_coffee_days: int,
    inactive_5_7_enabled: bool,
    inactive_5_7_days: int,
    inactive_14_30_enabled: bool,
    inactive_14_30_days: int,
):
    values = [
        one_left_days,
        free_coffee_days,
        inactive_5_7_days,
        inactive_14_30_days,
    ]

    if any(value < 1 or value > 7 for value in values):
        raise ValueError("Reminder days must be between 1 and 7")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO shop_reminder_settings (
                    shop_id,
                    one_left_enabled,
                    one_left_days,
                    free_coffee_enabled,
                    free_coffee_days,
                    inactive_5_7_enabled,
                    inactive_5_7_days,
                    inactive_14_30_enabled,
                    inactive_14_30_days,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (shop_id)
                DO UPDATE SET
                    one_left_enabled = EXCLUDED.one_left_enabled,
                    one_left_days = EXCLUDED.one_left_days,
                    free_coffee_enabled = EXCLUDED.free_coffee_enabled,
                    free_coffee_days = EXCLUDED.free_coffee_days,
                    inactive_5_7_enabled = EXCLUDED.inactive_5_7_enabled,
                    inactive_5_7_days = EXCLUDED.inactive_5_7_days,
                    inactive_14_30_enabled = EXCLUDED.inactive_14_30_enabled,
                    inactive_14_30_days = EXCLUDED.inactive_14_30_days,
                    updated_at = NOW()
                RETURNING *
            """, (
                shop_id,
                one_left_enabled,
                one_left_days,
                free_coffee_enabled,
                free_coffee_days,
                inactive_5_7_enabled,
                inactive_5_7_days,
                inactive_14_30_enabled,
                inactive_14_30_days,
            ))

            return cur.fetchone()

def create_admin_login_ticket(telegram_user_id: int, ttl_minutes: int = 5) -> str:
    ticket = str(uuid.uuid4())

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM admin_login_tickets
                WHERE telegram_user_id = %s
                   OR expires_at <= NOW()
                   OR used_at IS NOT NULL
            """, (telegram_user_id,))

            cur.execute("""
                INSERT INTO admin_login_tickets (
                    ticket,
                    telegram_user_id,
                    expires_at
                )
                VALUES (
                    %s,
                    %s,
                    NOW() + (%s || ' minutes')::interval
                )
            """, (
                ticket,
                telegram_user_id,
                ttl_minutes,
            ))

    return ticket


def consume_admin_login_ticket(ticket: str):
    clean_ticket = (ticket or "").strip()

    if not clean_ticket:
        return None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE admin_login_tickets
                SET used_at = NOW()
                WHERE ticket = %s
                  AND used_at IS NULL
                  AND expires_at > NOW()
                RETURNING
                    telegram_user_id,
                    expires_at,
                    used_at
            """, (clean_ticket,))

            return cur.fetchone()

