"""Super Admin assignment of additional locations to an existing internal owner."""

from app.admin_invites import InviteError


def has_owner_access(connection, user_id: int) -> bool:
    if connection.execute("SELECT EXISTS(SELECT 1 FROM shop_admins WHERE user_id=%s AND role='owner') AS owned",
                          (user_id,)).fetchone()["owned"]:
        return True
    if connection.execute("SELECT to_regclass('network_memberships') AS memberships").fetchone()["memberships"] is None:
        return False
    return connection.execute("""SELECT EXISTS(SELECT 1 FROM network_memberships
        WHERE user_id=%s AND role='NETWORK_OWNER') AS owned""", (user_id,)).fetchone()["owned"]


def public_owner(user: dict) -> dict:
    return {"user_id": user["id"], "name": user.get("full_name") or user.get("username") or f"User #{user['id']}",
            "username": user.get("username"), "telegram_user_id": user.get("telegram_user_id")}


def search_owners(connection, query: str = "") -> list[dict]:
    query = query.strip()
    if not query:
        return []
    network_clause = ""
    if connection.execute("SELECT to_regclass('network_memberships') AS memberships").fetchone()["memberships"] is not None:
        network_clause = " OR EXISTS(SELECT 1 FROM network_memberships nm WHERE nm.user_id=u.id AND nm.role='NETWORK_OWNER')"
    pattern = "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    rows = connection.execute("""SELECT u.id,u.full_name,u.username,u.telegram_user_id FROM users u
        WHERE (EXISTS(SELECT 1 FROM shop_admins sa WHERE sa.user_id=u.id AND sa.role='owner')""" + network_clause + """)
          AND (u.id::text=%s OR u.full_name ILIKE %s OR u.username ILIKE %s)
        ORDER BY u.id LIMIT 50""", (query, pattern, pattern)).fetchall()
    return [public_owner(row) for row in rows]


def search_owners_by_shop(connection, query: str = "") -> list[dict]:
    """Find existing owners through real active locations, without changing access."""
    query = query.strip()
    if not query:
        return []
    network_edges = ""
    if connection.execute("SELECT to_regclass('network_memberships') AS memberships").fetchone()["memberships"] is not None:
        network_edges = """UNION SELECT cs.id,nm.user_id FROM visible_shops cs
            JOIN network_memberships nm ON nm.network_id=cs.network_id AND nm.role='NETWORK_OWNER'"""
    pattern = "%" + query.casefold().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    # Some existing PostgreSQL databases use the C locale, where lower() leaves
    # Ukrainian letters unchanged. Keep name search and demo filtering consistent.
    uppercase = "АБВГҐДЕЄЖЗИІЇЙКЛМНОПРСТУФХЦЧШЩЬЮЯЫЭЁЪ"
    lowercase = "абвгґдеєжзиіїйклмнопрстуфхцчшщьюяыэёъ"
    rows = connection.execute("""WITH visible_shops AS (
            SELECT cs.id,cs.name,(to_jsonb(cs)->>'network_id')::bigint AS network_id,
                   translate(lower(cs.name),%s,%s) AS search_name
            FROM coffee_shops cs
            WHERE cs.is_active IS TRUE AND to_jsonb(cs)->>'deleted_at' IS NULL
              AND COALESCE(lower(to_jsonb(cs)->>'is_test'),'false') NOT IN ('true','1')
              AND COALESCE(lower(to_jsonb(cs)->>'is_demo'),'false') NOT IN ('true','1')
              AND translate(COALESCE(lower(btrim(to_jsonb(cs)->>'city')),''),%s,%s)
                    NOT IN ('test','demo','тест','демо')
              AND translate(lower(cs.name),%s,%s)
                    !~ '(^|[^[:alnum:]а-яіїєґ])(test|demo|тест|демо)([^[:alnum:]а-яіїєґ]|$)'
        ), owner_edges AS (
            SELECT cs.id AS shop_id,sa.user_id FROM visible_shops cs
            JOIN shop_admins sa ON sa.shop_id=cs.id AND sa.role='owner'
            """ + network_edges + """
        ), owner_counts AS (
            SELECT user_id,COUNT(DISTINCT shop_id)::integer AS shops_count
            FROM owner_edges GROUP BY user_id
        ), matches AS (
            SELECT id,name,search_name FROM visible_shops WHERE search_name LIKE %s ORDER BY search_name,id LIMIT 50
        )
        SELECT m.id AS shop_id,m.name AS shop_name,u.id,u.full_name,u.username,u.telegram_user_id,
               oc.shops_count
        FROM matches m LEFT JOIN owner_edges oe ON oe.shop_id=m.id
        LEFT JOIN users u ON u.id=oe.user_id LEFT JOIN owner_counts oc ON oc.user_id=u.id
        ORDER BY m.search_name,m.id,u.id""", (uppercase, lowercase, uppercase, lowercase, uppercase, lowercase, pattern)).fetchall()
    shops = {}
    for row in rows:
        shop = shops.setdefault(row["shop_id"], {"shop_id": row["shop_id"], "name": row["shop_name"], "owners": []})
        if row["id"] is not None:
            shop["owners"].append({**public_owner(row), "shops_count": row["shops_count"]})
    return list(shops.values())


def _existing_owner(connection, user_id: int) -> dict:
    user = connection.execute("SELECT id,full_name,username,telegram_user_id FROM users WHERE id=%s FOR KEY SHARE",
                              (user_id,)).fetchone()
    if user is None:
        raise InviteError("USER_NOT_FOUND", 404)
    if not has_owner_access(connection, user_id):
        raise InviteError("OWNER_ACCESS_REQUIRED", 409)
    return user


def assign_shop_owner(connection, shop_id: int, user_id: int) -> dict:
    shop = connection.execute("""SELECT id,pending_owner_telegram_id FROM coffee_shops
        WHERE id=%s AND is_active IS TRUE FOR NO KEY UPDATE""", (shop_id,)).fetchone()
    if shop is None:
        raise InviteError("SHOP_NOT_FOUND", 404)
    user = _existing_owner(connection, user_id)
    owners = connection.execute("SELECT user_id FROM shop_admins WHERE shop_id=%s AND role='owner' FOR UPDATE",
                                (shop_id,)).fetchall()
    if any(row["user_id"] != user_id for row in owners) or (
        shop["pending_owner_telegram_id"] is not None and shop["pending_owner_telegram_id"] != user["telegram_user_id"]
    ):
        raise InviteError("OWNER_ALREADY_CONNECTED", 409)
    membership = connection.execute("""INSERT INTO shop_admins(shop_id,user_id,role)
        VALUES(%s,%s,'owner') ON CONFLICT(shop_id,user_id) DO UPDATE SET role='owner'
        RETURNING id AS membership_id,shop_id,role""", (shop_id, user_id)).fetchone()
    connection.execute("UPDATE coffee_shops SET pending_owner_telegram_id=NULL WHERE id=%s", (shop_id,))
    return {"owner": public_owner(user), "membership": membership}


def assign_network_owner(connection, network_id: int, user_id: int) -> dict:
    network = connection.execute("SELECT id FROM coffee_networks WHERE id=%s FOR UPDATE", (network_id,)).fetchone()
    if network is None:
        raise InviteError("NETWORK_NOT_FOUND", 404)
    shops = connection.execute("""SELECT id FROM coffee_shops WHERE network_id=%s AND is_active IS TRUE
        ORDER BY id FOR NO KEY UPDATE""", (network_id,)).fetchall()
    user = _existing_owner(connection, user_id)
    membership = connection.execute("""INSERT INTO network_memberships(user_id,network_id,role)
        VALUES(%s,%s,'NETWORK_OWNER') ON CONFLICT(user_id,network_id) DO UPDATE SET role='NETWORK_OWNER'
        RETURNING id AS network_membership_id,network_id,role""", (user_id, network_id)).fetchone()
    for shop in shops:
        connection.execute("""INSERT INTO shop_admins(shop_id,user_id,role,network_membership_id)
            VALUES(%s,%s,'owner',%s) ON CONFLICT(shop_id,user_id) DO NOTHING""",
            (shop["id"], user_id, membership["network_membership_id"]))
    return {"owner": public_owner(user), "membership": membership}
