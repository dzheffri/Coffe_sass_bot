from app.web_panel_db import get_web_connection, init_web_panel_db
from app.db import get_connection, get_admin_shop_and_role


init_web_panel_db()


def ensure_shop_profile_exists(owner_telegram_id: int):
    conn = get_web_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT id FROM shop_profiles WHERE owner_telegram_id = ?",
        (owner_telegram_id,)
    )
    row = cur.fetchone()

    if not row:
        cur.execute("""
        INSERT INTO shop_profiles (
            owner_telegram_id, name, subtitle, address, work_from, work_to,
            instagram, description, logo_url, cover_url
        )
        VALUES (?, '', '', '', '', '', '', '', '', '')
        """, (owner_telegram_id,))

        default_news = [
            ("", "", "", 0),
            ("", "", "", 1),
            ("", "", "", 2),
        ]

        for title, price, image_url, sort_order in default_news:
            cur.execute("""
            INSERT INTO shop_news (
                owner_telegram_id, title, price, image_url, sort_order
            )
            VALUES (?, ?, ?, ?, ?)
            """, (owner_telegram_id, title, price, image_url, sort_order))

        conn.commit()

    conn.close()


def get_shop_profile(owner_telegram_id: int):
    ensure_shop_profile_exists(owner_telegram_id)

    conn = get_web_connection()
    cur = conn.cursor()

    cur.execute("""
    SELECT *
    FROM shop_profiles
    WHERE owner_telegram_id = ?
    """, (owner_telegram_id,))
    profile = cur.fetchone()

    cur.execute("""
    SELECT id, title, price, image_url, sort_order
    FROM shop_news
    WHERE owner_telegram_id = ?
    ORDER BY sort_order ASC, id ASC
    """, (owner_telegram_id,))
    news = cur.fetchall()

    conn.close()

    return {
        "ok": True,
        "shop": {
            "owner_telegram_id": owner_telegram_id,
            "name": profile["name"] or "",
            "subtitle": profile["subtitle"] or "",
            "address": profile["address"] or "",
            "work_from": profile["work_from"] or "",
            "work_to": profile["work_to"] or "",
            "instagram": profile["instagram"] or "",
            "description": profile["description"] or "",
            "logo_url": profile["logo_url"] or "",
            "cover_url": profile["cover_url"] or "",
            "news": [
                {
                    "id": item["id"],
                    "title": item["title"] or "",
                    "price": item["price"] or "",
                    "image_url": item["image_url"] or "",
                    "sort_order": item["sort_order"] or 0,
                }
                for item in news
            ]
        }
    }
def update_shop_profile(
    owner_telegram_id: int,
    name: str,
    subtitle: str,
    address: str,
    work_from: str,
    work_to: str,
    instagram: str,
    description: str,
    logo_url: str,
    cover_url: str,
    news: list[dict],
):
    ensure_shop_profile_exists(owner_telegram_id)

    conn = get_web_connection()
    cur = conn.cursor()

    cur.execute("""
    UPDATE shop_profiles
    SET
        name = ?,
        subtitle = ?,
        address = ?,
        work_from = ?,
        work_to = ?,
        instagram = ?,
        description = ?,
        logo_url = ?,
        cover_url = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE owner_telegram_id = ?
    """, (
        name,
        subtitle,
        address,
        work_from,
        work_to,
        instagram,
        description,
        logo_url,
        cover_url,
        owner_telegram_id,
    ))

    cur.execute(
        "DELETE FROM shop_news WHERE owner_telegram_id = ?",
        (owner_telegram_id,)
    )

    for index, item in enumerate(news):
        cur.execute("""
        INSERT INTO shop_news (
            owner_telegram_id, title, price, image_url, sort_order, updated_at
        )
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            owner_telegram_id,
            item.get("title", ""),
            item.get("price", ""),
            item.get("image_url", ""),
            index,
        ))

    conn.commit()
    conn.close()
    def get_owner_overview_stats(owner_telegram_id: int):
    shop = get_admin_shop_and_role(owner_telegram_id)

    if not shop:
        return {
            "ok": False,
            "message": "Кав’ярню власника не знайдено"
        }

    shop_id = shop["id"]

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_clients,

                    COUNT(*) FILTER (
                        WHERE last_activity_at >= NOW() - INTERVAL '30 days'
                    ) AS active_30_days,

                    COUNT(*) FILTER (
                        WHERE created_at >= NOW() - INTERVAL '30 days'
                    ) AS new_30_days,

                    COALESCE(SUM(free_coffee_balance), 0) AS free_coffees_now

                FROM shop_clients
                WHERE shop_id = %s
            """, (shop_id,))

            stats = cur.fetchone()

    return {
        "ok": True,
        "shop_id": shop_id,
        "stats": {
            "total_clients": int(stats["total_clients"] or 0),
            "active_30_days": int(stats["active_30_days"] or 0),
            "new_30_days": int(stats["new_30_days"] or 0),
            "free_coffees_now": int(stats["free_coffees_now"] or 0),
        }
    }

    return {
        "ok": True,
        "message": "Профіль кав’ярні оновлено"
    }
