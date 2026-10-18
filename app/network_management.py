"""Network ownership, location membership, shared loyalty and roll-up stats."""

import secrets

from app.admin_invites import InviteError, _digest, _pepper
from app.db import LOYALTY_TARGET


def _mode(value):
    if value not in {"SHARED", "PER_SHOP"}:
        raise InviteError("INVALID_LOYALTY_MODE", 422)
    return value


def create_network(connection, name: str, logo_url: str | None = None,
                   loyalty_mode: str = "PER_SHOP") -> dict:
    name = name.strip()
    if not name or len(name) > 180:
        raise InviteError("INVALID_NETWORK_NAME", 422)
    with connection.cursor() as cur:
        return cur.execute(
            """INSERT INTO coffee_networks(name,logo_url,loyalty_mode)
               VALUES(%s,%s,%s) RETURNING id,name,logo_url,loyalty_mode,created_at,updated_at""",
            (name, logo_url or None, _mode(loyalty_mode)),
        ).fetchone()


def list_networks(connection) -> list[dict]:
    with connection.cursor() as cur:
        cur.execute("""SELECT n.id,n.name,n.logo_url,n.loyalty_mode,n.created_at,n.updated_at,
            COALESCE(json_agg(json_build_object('shop_id',s.id,'name',s.name)
                ORDER BY s.id) FILTER (WHERE s.id IS NOT NULL),'[]'::json) AS shops
            FROM coffee_networks n LEFT JOIN coffee_shops s ON s.network_id=n.id
            GROUP BY n.id ORDER BY n.name,n.id""")
        return cur.fetchall()


def update_network(connection, network_id: int, *, name: str | None = None,
                   logo_url: str | None = None, loyalty_mode: str | None = None) -> dict:
    with connection.cursor() as cur:
        network = cur.execute(
            "SELECT * FROM coffee_networks WHERE id=%s FOR UPDATE", (network_id,),
        ).fetchone()
        if network is None:
            raise InviteError("NETWORK_NOT_FOUND", 404)
        next_name = network["name"] if name is None else name.strip()
        next_logo = network["logo_url"] if logo_url is None else (logo_url or None)
        next_mode = network["loyalty_mode"] if loyalty_mode is None else _mode(loyalty_mode)
        if not next_name or len(next_name) > 180:
            raise InviteError("INVALID_NETWORK_NAME", 422)
        if next_mode != network["loyalty_mode"]:
            change_network_loyalty_mode(connection, network_id, network["loyalty_mode"], next_mode)
        return cur.execute(
            """UPDATE coffee_networks SET name=%s,logo_url=%s,loyalty_mode=%s,
                      updated_at=statement_timestamp() WHERE id=%s
               RETURNING id,name,logo_url,loyalty_mode,created_at,updated_at""",
            (next_name, next_logo, next_mode, network_id),
        ).fetchone()


def ensure_network_shop_memberships(connection, network_id: int, shop_id: int) -> None:
    connection.execute("""INSERT INTO shop_admins(shop_id,user_id,role,network_membership_id)
        SELECT %s,nm.user_id,'owner',nm.id FROM network_memberships nm
        WHERE nm.network_id=%s AND nm.role='NETWORK_OWNER'
        ON CONFLICT(shop_id,user_id) DO NOTHING""", (shop_id, network_id))


def attach_shop(connection, network_id: int, shop_id: int) -> dict:
    network = connection.execute(
        "SELECT id,loyalty_mode FROM coffee_networks WHERE id=%s FOR UPDATE", (network_id,),
    ).fetchone()
    if network is None:
        raise InviteError("NETWORK_NOT_FOUND", 404)
    shop = connection.execute(
        "SELECT id,network_id FROM coffee_shops WHERE id=%s FOR UPDATE", (shop_id,),
    ).fetchone()
    if shop is None:
        raise InviteError("SHOP_NOT_FOUND", 404)
    old_network_id = shop["network_id"]
    if old_network_id == network_id:
        return {"shop_id": shop_id, "network_id": network_id}
    if old_network_id is not None:
        old = connection.execute(
            "SELECT loyalty_mode FROM coffee_networks WHERE id=%s FOR UPDATE", (old_network_id,),
        ).fetchone()
        connection.execute("""DELETE FROM shop_admins
            WHERE shop_id=%s AND network_membership_id IN
                (SELECT id FROM network_memberships WHERE network_id=%s)""",
            (shop_id, old_network_id))
        # Existing SHARED balance remains with its former network by design;
        # an attached point must not transfer that network's accrued rewards.
        if old and old["loyalty_mode"] == "SHARED":
            connection.execute("UPDATE shop_clients SET cups=0,free_coffee_balance=0 WHERE shop_id=%s", (shop_id,))
    connection.execute("UPDATE coffee_shops SET network_id=%s WHERE id=%s", (network_id, shop_id))
    ensure_network_shop_memberships(connection, network_id, shop_id)
    if network["loyalty_mode"] == "SHARED":
        merge_shop_balances_into_network(connection, network_id, shop_id=shop_id)
    return {"shop_id": shop_id, "network_id": network_id}


def detach_shop(connection, network_id: int, shop_id: int) -> dict:
    network = connection.execute(
        "SELECT id,loyalty_mode FROM coffee_networks WHERE id=%s FOR UPDATE", (network_id,),
    ).fetchone()
    if network is None:
        raise InviteError("NETWORK_NOT_FOUND", 404)
    shop = connection.execute(
        "SELECT id,network_id FROM coffee_shops WHERE id=%s FOR UPDATE", (shop_id,),
    ).fetchone()
    if shop is None or shop["network_id"] != network_id:
        raise InviteError("SHOP_NOT_IN_NETWORK", 404)
    connection.execute("""DELETE FROM shop_admins WHERE shop_id=%s AND network_membership_id IN
        (SELECT id FROM network_memberships WHERE network_id=%s)""", (shop_id, network_id))
    connection.execute("UPDATE coffee_shops SET network_id=NULL WHERE id=%s", (shop_id,))
    if network["loyalty_mode"] == "PER_SHOP":
        return {"shop_id": shop_id, "network_id": None}
    # Shared rewards belong to the remaining network, not to the detached point.
    connection.execute("UPDATE shop_clients SET cups=0,free_coffee_balance=0 WHERE shop_id=%s", (shop_id,))
    return {"shop_id": shop_id, "network_id": None}


def create_network_owner_invite(connection, network_id: int) -> dict:
    pepper = _pepper()
    if connection.execute("SELECT id FROM coffee_networks WHERE id=%s FOR UPDATE",
                          (network_id,)).fetchone() is None:
        raise InviteError("NETWORK_NOT_FOUND", 404)
    shops = connection.execute(
        "SELECT id FROM coffee_shops WHERE network_id=%s AND is_active IS TRUE ORDER BY id FOR UPDATE",
        (network_id,),
    ).fetchall()
    if not shops:
        raise InviteError("NETWORK_HAS_NO_SHOPS", 409)
    old_hashes = {row["code_hash"] for row in connection.execute(
        "SELECT code_hash FROM barista_admin_invites WHERE network_id=%s AND role='owner' AND closed_at IS NULL",
        (network_id,),
    ).fetchall()}
    connection.execute("""UPDATE barista_admin_invites SET revoked_at=statement_timestamp(),
        closed_at=statement_timestamp() WHERE network_id=%s AND role='owner' AND closed_at IS NULL""",
        (network_id,))
    for _ in range(32):
        code = f"{secrets.randbelow(1_000_000):06d}"
        code_hash = _digest(pepper, "invite-code:" + code)
        if code_hash in old_hashes:
            continue
        row = connection.execute("""INSERT INTO barista_admin_invites
            (shop_id,network_id,role,creator_user_id,code_hash,expires_at)
            VALUES(%s,%s,'owner',NULL,%s,statement_timestamp()+INTERVAL '7 days')
            ON CONFLICT (code_hash) WHERE closed_at IS NULL DO NOTHING RETURNING *""",
            (shops[0]["id"], network_id, code_hash)).fetchone()
        if row:
            return {key: row[key] for key in ("id", "created_at", "expires_at")} | {
                "network_id": network_id, "role": "NETWORK_OWNER", "code": code,
            }
    raise InviteError("INVITE_CREATION_UNAVAILABLE", 503)


def network_access_for_user(connection, user_id: int) -> list[dict]:
    return connection.execute("""SELECT n.id AS network_id,n.name,n.logo_url,n.loyalty_mode,
        nm.id AS network_membership_id,
        COALESCE(json_agg(json_build_object('shop_id',s.id,'name',s.name)
            ORDER BY s.id) FILTER (WHERE s.id IS NOT NULL),'[]'::json) AS shops
        FROM network_memberships nm JOIN coffee_networks n ON n.id=nm.network_id
        LEFT JOIN coffee_shops s ON s.network_id=n.id AND s.is_active IS TRUE
        WHERE nm.user_id=%s AND nm.role='NETWORK_OWNER'
        GROUP BY n.id,nm.id ORDER BY n.id""", (user_id,)).fetchall()


def merge_shop_balances_into_network(connection, network_id: int, *, shop_id: int | None = None) -> None:
    """Move shop-scoped balances to the shared ledger without losing cups or gifts."""
    params = [network_id]
    shop_filter = ""
    if shop_id is not None:
        shop_filter = " AND sc.shop_id=%s"
        params.append(shop_id)
    rows = connection.execute("""SELECT sc.*,
        COALESCE(sc.last_activity_at,sc.created_at,statement_timestamp()) AS effective_activity_at
        FROM shop_clients sc JOIN coffee_shops s ON s.id=sc.shop_id
        WHERE s.network_id=%s""" + shop_filter + " ORDER BY sc.user_id,sc.shop_id FOR UPDATE OF sc", tuple(params)).fetchall()
    by_user: dict[int, list[dict]] = {}
    for row in rows:
        by_user.setdefault(row["user_id"], []).append(row)
    for user_id, clients in by_user.items():
        shop_cups = sum(row["cups"] for row in clients)
        existing = connection.execute("""SELECT *,
            COALESCE(last_activity_at,created_at,statement_timestamp()) AS effective_activity_at
            FROM network_client_loyalty
            WHERE network_id=%s AND user_id=%s FOR UPDATE""", (network_id, user_id)).fetchone()
        existing_cups = existing["cups"] if existing else 0
        total_cups = shop_cups + existing_cups
        extra = total_cups // LOYALTY_TARGET
        cups = total_cups % LOYALTY_TARGET
        free = sum(row["free_coffee_balance"] for row in clients) + extra
        scans = sum(row["total_scans"] for row in clients)
        earned = sum(row["total_free_coffee_earned"] for row in clients) + extra
        redeemed = sum(row["total_free_coffee_redeemed"] for row in clients)
        latest = max(clients, key=lambda row: row["effective_activity_at"])
        latest = {**latest, "last_activity_at": latest["effective_activity_at"]}
        if existing:
            free += existing["free_coffee_balance"]
            scans += existing["total_scans"]
            earned += existing["total_free_coffee_earned"]
            redeemed += existing["total_free_coffee_redeemed"]
            if existing["effective_activity_at"] > latest["last_activity_at"]:
                latest = {**latest, "shop_id": existing["last_shop_id"] or latest["shop_id"],
                          "last_activity_at": existing["effective_activity_at"]}
        connection.execute("""INSERT INTO network_client_loyalty
            (network_id,user_id,cups,free_coffee_balance,total_scans,total_free_coffee_earned,
             total_free_coffee_redeemed,last_shop_id,last_activity_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(network_id,user_id) DO UPDATE SET cups=EXCLUDED.cups,
                free_coffee_balance=EXCLUDED.free_coffee_balance,total_scans=EXCLUDED.total_scans,
                total_free_coffee_earned=EXCLUDED.total_free_coffee_earned,
                total_free_coffee_redeemed=EXCLUDED.total_free_coffee_redeemed,
                last_shop_id=EXCLUDED.last_shop_id,last_activity_at=EXCLUDED.last_activity_at""",
            (network_id,user_id,cups,free,scans,earned,redeemed,latest["shop_id"],latest["last_activity_at"]))
        connection.execute("UPDATE shop_clients SET cups=0,free_coffee_balance=0 WHERE id=ANY(%s)",
                           ([row["id"] for row in clients],))


def change_network_loyalty_mode(connection, network_id: int, old_mode: str, new_mode: str) -> None:
    if old_mode == new_mode:
        return
    if new_mode == "SHARED":
        merge_shop_balances_into_network(connection, network_id)
        return
    rows = connection.execute("""SELECT ncl.*,
        COALESCE((SELECT id FROM coffee_shops WHERE network_id=ncl.network_id AND id=ncl.last_shop_id
                  AND is_active IS TRUE),
                 (SELECT id FROM coffee_shops WHERE network_id=ncl.network_id AND is_active IS TRUE ORDER BY id LIMIT 1)) AS target_shop_id
        FROM network_client_loyalty ncl WHERE network_id=%s FOR UPDATE""", (network_id,)).fetchall()
    for row in rows:
        if row["target_shop_id"] is None:
            continue
        connection.execute("""INSERT INTO shop_clients(shop_id,user_id,cups,free_coffee_balance,
                total_scans,total_free_coffee_earned,total_free_coffee_redeemed,last_activity_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(shop_id,user_id) DO UPDATE SET
            cups=shop_clients.cups+EXCLUDED.cups,
            free_coffee_balance=shop_clients.free_coffee_balance+EXCLUDED.free_coffee_balance,
            total_scans=shop_clients.total_scans+EXCLUDED.total_scans,
            total_free_coffee_earned=shop_clients.total_free_coffee_earned+EXCLUDED.total_free_coffee_earned,
            total_free_coffee_redeemed=shop_clients.total_free_coffee_redeemed+EXCLUDED.total_free_coffee_redeemed,
            last_activity_at=GREATEST(shop_clients.last_activity_at,EXCLUDED.last_activity_at)""",
            (row["target_shop_id"], row["user_id"], row["cups"], row["free_coffee_balance"],
             row["total_scans"], row["total_free_coffee_earned"], row["total_free_coffee_redeemed"],
             row["last_activity_at"]))
    connection.execute("DELETE FROM network_client_loyalty WHERE network_id=%s", (network_id,))


def network_dashboard(connection, network_id: int) -> dict:
    shops = connection.execute("""SELECT id AS shop_id,name FROM coffee_shops
        WHERE network_id=%s AND is_active IS TRUE ORDER BY id""", (network_id,)).fetchall()
    if not shops:
        return {"network_id": network_id, "stats": {}, "periods": {"7": {}, "30": {}}, "shops": []}
    ids = [shop["shop_id"] for shop in shops]
    by_shop = connection.execute("""SELECT s.id AS shop_id,s.name,
        COALESCE(sc.clients,0) AS clients,COALESCE(sc.active_clients,0) AS active_clients,
        COALESCE(sc.new_clients_30d,0) AS new_clients_30d,
        COALESCE(t.visits_today,0) AS visits_today,COALESCE(t.cups_today,0) AS cups_today,
        COALESCE(t.gifts_used_today,0) AS gifts_used_today,COALESCE(t.scans_today,0) AS scans_today,
        COALESCE(t.cups_7d,0) AS cups_7d,COALESCE(t.gifts_7d,0) AS gifts_7d,
        COALESCE(t.visits_7d,0) AS visits_7d,COALESCE(t.cups_30d,0) AS cups_30d,
        COALESCE(t.gifts_30d,0) AS gifts_30d,COALESCE(t.visits_30d,0) AS visits_30d
        FROM coffee_shops s
        LEFT JOIN (SELECT shop_id,COUNT(DISTINCT user_id) AS clients,
            COUNT(DISTINCT user_id) FILTER (WHERE last_activity_at>=NOW()-INTERVAL '30 days') AS active_clients,
            COUNT(DISTINCT user_id) FILTER (WHERE created_at>=NOW()-INTERVAL '30 days') AS new_clients_30d
            FROM shop_clients GROUP BY shop_id) sc ON sc.shop_id=s.id
        LEFT JOIN (SELECT shop_id,
            COUNT(*) FILTER (WHERE created_at>=CURRENT_DATE AND type='add_cups') AS visits_today,
            COALESCE(SUM(cups_added) FILTER (WHERE created_at>=CURRENT_DATE AND type='add_cups'),0) AS cups_today,
            COALESCE(SUM(free_redeemed) FILTER (WHERE created_at>=CURRENT_DATE AND type='redeem_free'),0) AS gifts_used_today,
            COALESCE(SUM(cups_added) FILTER (WHERE created_at>=CURRENT_DATE AND type='add_cups'),0) AS scans_today,
            COALESCE(SUM(cups_added) FILTER (WHERE created_at>=NOW()-INTERVAL '7 days' AND type='add_cups'),0) AS cups_7d,
            COALESCE(SUM(free_redeemed) FILTER (WHERE created_at>=NOW()-INTERVAL '7 days' AND type='redeem_free'),0) AS gifts_7d,
            COUNT(*) FILTER (WHERE created_at>=NOW()-INTERVAL '7 days' AND type='add_cups') AS visits_7d,
            COALESCE(SUM(cups_added) FILTER (WHERE created_at>=NOW()-INTERVAL '30 days' AND type='add_cups'),0) AS cups_30d,
            COALESCE(SUM(free_redeemed) FILTER (WHERE created_at>=NOW()-INTERVAL '30 days' AND type='redeem_free'),0) AS gifts_30d,
            COUNT(*) FILTER (WHERE created_at>=NOW()-INTERVAL '30 days' AND type='add_cups') AS visits_30d
            FROM transactions GROUP BY shop_id) t ON t.shop_id=s.id
        WHERE s.id=ANY(%s) ORDER BY s.id""", (ids,)).fetchall()
    aggregate = connection.execute("""WITH clients AS (
            SELECT COUNT(DISTINCT sc.user_id) AS clients,
              COUNT(DISTINCT sc.user_id) FILTER (WHERE sc.last_activity_at>=NOW()-INTERVAL '30 days') AS active_clients,
              COUNT(DISTINCT sc.user_id) FILTER (WHERE sc.created_at>=NOW()-INTERVAL '30 days') AS new_clients_30d
            FROM shop_clients sc WHERE sc.shop_id=ANY(%s)),
          activity AS (
            SELECT COUNT(*) FILTER (WHERE created_at>=CURRENT_DATE AND type='add_cups') AS visits_today,
              COALESCE(SUM(cups_added) FILTER (WHERE created_at>=CURRENT_DATE AND type='add_cups'),0) AS cups_today,
              COALESCE(SUM(free_redeemed) FILTER (WHERE created_at>=CURRENT_DATE AND type='redeem_free'),0) AS gifts_used_today,
              COALESCE(SUM(cups_added) FILTER (WHERE created_at>=CURRENT_DATE AND type='add_cups'),0) AS scans_today,
              COALESCE(SUM(cups_added) FILTER (WHERE created_at>=NOW()-INTERVAL '7 days' AND type='add_cups'),0) AS cups_7d,
              COALESCE(SUM(free_redeemed) FILTER (WHERE created_at>=NOW()-INTERVAL '7 days' AND type='redeem_free'),0) AS gifts_7d,
              COUNT(*) FILTER (WHERE created_at>=NOW()-INTERVAL '7 days' AND type='add_cups') AS visits_7d,
              COALESCE(SUM(cups_added) FILTER (WHERE created_at>=NOW()-INTERVAL '30 days' AND type='add_cups'),0) AS cups_30d,
              COALESCE(SUM(free_redeemed) FILTER (WHERE created_at>=NOW()-INTERVAL '30 days' AND type='redeem_free'),0) AS gifts_30d,
              COUNT(*) FILTER (WHERE created_at>=NOW()-INTERVAL '30 days' AND type='add_cups') AS visits_30d
            FROM transactions WHERE shop_id=ANY(%s))
          SELECT * FROM clients CROSS JOIN activity""", (ids, ids)).fetchone()
    return {"network_id": network_id, "stats": dict(aggregate),
            "periods": {"7": {key: aggregate[key] for key in ("visits_7d","cups_7d","gifts_7d")},
                        "30": {key: aggregate[key] for key in ("visits_30d","cups_30d","gifts_30d")}},
            "shops": [dict(row) for row in by_shop]}
