"""Shared-account deletion without cascading away a shop's business history.

The client and staff apps share users/identities. Delete the original user and
its access/profile/balances, but move business-only records to a fresh anonymous
reference. No identity, session, membership or balance is given to that row.
"""

import secrets
from contextlib import nullcontext

from fastapi import HTTPException
from psycopg import sql

from app.db import get_connection


def account_deletion_state(user_id: int, connection) -> dict:
    """Report every shop the global account deletion would close.

    A session's selected shop does not restrict account deletion: the personal
    account and its memberships are shared by both apps and all coffee shops.
    Deletion repeats this advisory query while all affected memberships lock.
    """
    closing = connection.execute("""
        SELECT cs.id,cs.name
        FROM shop_admins own JOIN coffee_shops cs ON cs.id=own.shop_id
        WHERE own.user_id=%s AND own.role='owner' AND cs.is_active IS TRUE
          AND NOT EXISTS(SELECT 1 FROM shop_admins other
              WHERE other.shop_id=own.shop_id AND other.user_id<>%s
                AND other.role='owner')
        ORDER BY cs.id
    """, (user_id, user_id)).fetchall()
    apple = connection.execute(
        "SELECT 1 FROM user_identities WHERE user_id=%s AND provider='apple'",
        (user_id,),
    ).fetchone() is not None
    # Current Apple auth retains only id_token, not a revocable access/refresh
    # token or authorization code. TN3194 explicitly permits deletion followed
    # by instructions for manual revocation; never claim Apple was revoked.
    return {"ok": True, "can_delete": True,
            "blocking_shops": [], "closing_shops": [dict(row) for row in closing],
            "apple_revocation": "manual_required" if apple else "not_applicable"}


def _lock_account(user_id: int, connection):
    # Follow staff's membership-before-session lock order. Lock every affected
    # shop/member, not merely the session's selected coffee shop: two co-owners
    # deleting simultaneously must close the shop when the final owner leaves.
    shops = connection.execute(
        "SELECT shop_id FROM shop_admins WHERE user_id=%s ORDER BY shop_id",
        (user_id,),
    ).fetchall()
    shop_ids = [row["shop_id"] for row in shops]
    if shop_ids:
        connection.execute(
            "SELECT id FROM coffee_shops WHERE id=ANY(%s) ORDER BY id FOR UPDATE",
            (shop_ids,),
        ).fetchall()
        connection.execute(
            "SELECT id FROM shop_admins WHERE shop_id=ANY(%s) ORDER BY id FOR UPDATE",
            (shop_ids,),
        ).fetchall()
    user = connection.execute(
        "SELECT id,telegram_user_id FROM users WHERE id=%s FOR UPDATE", (user_id,),
    ).fetchone()
    if user is None:
        return None
    # The user lock prevents new membership FK inserts. If one committed while
    # the initial shop locks were acquired, retry instead of deleting unchecked
    # ownership of a newly discovered shop.
    current_shops = connection.execute(
        "SELECT shop_id FROM shop_admins WHERE user_id=%s", (user_id,),
    ).fetchall()
    if not {row["shop_id"] for row in current_shops}.issubset(set(shop_ids)):
        raise HTTPException(409, detail={"code": "MEMBERSHIP_CHANGED"})
    return user


def _exists(connection, table):
    # Table names are constants below; to_regclass also respects test search_path.
    return connection.execute("SELECT to_regclass(%s) AS name", (table,)).fetchone()["name"] is not None


def _close_owned_shops(shop_ids: list[int], connection):
    """Close shops without deleting client profiles, balances or their history.

    Memberships/sessions are access records only. Deleting them cannot cascade
    to users or shop_clients; those foreign keys point in the opposite direction.
    The coffee_shops row remains, preserving every business-history shop FK.
    The caller already holds the shop and all membership locks.
    """
    if not shop_ids:
        return
    staff = connection.execute("""
        SELECT sa.id,sa.user_id,u.telegram_user_id
        FROM shop_admins sa JOIN users u ON u.id=sa.user_id
        WHERE sa.shop_id=ANY(%s)
    """, (shop_ids,)).fetchall()
    membership_ids = [row["id"] for row in staff]
    connection.execute("UPDATE coffee_shops SET is_active=FALSE WHERE id=ANY(%s)", (shop_ids,))
    if membership_ids:
        # Explicit revoke precedes the selected_membership FK cascade. Sessions
        # selected to other shops and ordinary client sessions remain untouched.
        connection.execute("""
            UPDATE app_sessions SET revoked_at=statement_timestamp()
            WHERE purpose='barista' AND selected_membership_id=ANY(%s)
              AND revoked_at IS NULL
        """, (membership_ids,))
    if _exists(connection, "barista_admin_invites"):
        connection.execute("""
            UPDATE barista_admin_invites
            SET revoked_at=statement_timestamp(),closed_at=statement_timestamp()
            WHERE shop_id=ANY(%s) AND used_at IS NULL AND revoked_at IS NULL
        """, (shop_ids,))
    if _exists(connection, "admin_broadcast_previews"):
        connection.execute("""
            UPDATE admin_broadcast_previews
            SET invalidated_at=statement_timestamp(),media_bytes=NULL
            WHERE shop_id=ANY(%s) AND used_at IS NULL AND invalidated_at IS NULL
        """, (shop_ids,))
    connection.execute("DELETE FROM shop_admins WHERE shop_id=ANY(%s)", (shop_ids,))
    # Legacy login tickets have no shop column. Remove them only for staff with
    # no remaining active shop access; do not break a multi-shop employee's login.
    if _exists(connection, "admin_login_tickets"):
        telegram_ids = [row["telegram_user_id"] for row in staff if row["telegram_user_id"] is not None]
        if telegram_ids:
            connection.execute("""
                DELETE FROM admin_login_tickets ticket
                WHERE ticket.telegram_user_id=ANY(%s)
                  AND NOT EXISTS(
                    SELECT 1 FROM users u JOIN shop_admins sa ON sa.user_id=u.id
                    JOIN coffee_shops cs ON cs.id=sa.shop_id
                    WHERE u.telegram_user_id=ticket.telegram_user_id
                      AND cs.is_active IS TRUE AND sa.role IN ('owner','admin'))
            """, (telegram_ids,))


def delete_personal_account(user_id: int, *, connection=None, barista_token=None):
    """Only trusted session callers may supply user_id; no request actor fields.

    Removing the original users.id makes any in-flight login/session insert
    fail its FK instead of recreating access to an anonymized account. The
    anonymous reference is different for every deletion and exposes no mapping
    to the former user. All cleanup and history updates commit atomically.
    """
    with (get_connection() if connection is None else nullcontext(connection)) as conn:
        with conn.transaction():
            user = _lock_account(user_id, conn)
            if user is None:
                return {"status": "user_not_found"}
            if barista_token is not None:
                from app.barista_sessions import resolve_barista_session
                session = resolve_barista_session(barista_token, connection=conn)
                if session["user_id"] != user_id:
                    raise HTTPException(401, detail={"code": "INVALID_SESSION"})

            state = account_deletion_state(user_id, conn)
            _close_owned_shops([shop["id"] for shop in state["closing_shops"]], conn)

            # Preserve shop aggregates/audit amounts without the former profile
            # or credentials. Personal balances are NOT copied to this row.
            history = (("transactions", "user_id"),
                       ("transactions", "admin_user_id"),
                       ("broadcasts", "sender_user_id"),
                       ("touch_logs", "user_id"),
                       ("return_logs", "user_id"),
                       ("reminder_logs", "user_id"))
            present = [(table, column) for table, column in history if _exists(conn, table)]
            has_history = any(conn.execute(
                sql.SQL("SELECT 1 FROM {} WHERE {}=%s LIMIT 1").format(
                    sql.Identifier(table), sql.Identifier(column)), (user_id,),
            ).fetchone() is not None for table, column in present)
            if has_history:
                anonymous = conn.execute("""
                    INSERT INTO users(telegram_user_id,username,full_name,personal_qr_token)
                    VALUES(NULL,NULL,'Видалений користувач',%s) RETURNING id
                """, (secrets.token_urlsafe(48),)).fetchone()["id"]
                for table, column in present:
                    conn.execute(sql.SQL("UPDATE {} SET {}=%s WHERE {}=%s").format(
                        sql.Identifier(table), sql.Identifier(column), sql.Identifier(column)),
                        (anonymous, user_id))

            # Non-FK device/link/ticket records require explicit removal.
            if _exists(conn, "wallet_passes") and _exists(conn, "wallet_device_registrations"):
                conn.execute("""
                    DELETE FROM wallet_device_registrations
                    WHERE serial_number IN(SELECT serial_number FROM wallet_passes WHERE user_id=%s)
                """, (user_id,))
            if _exists(conn, "telegram_link_sessions"):
                conn.execute("""
                    DELETE FROM telegram_link_sessions links
                    WHERE user_id=%s OR current_user_id=%s
                       OR (%s::bigint IS NOT NULL AND telegram_user_id=%s)
                       OR EXISTS(SELECT 1 FROM user_identities identity
                           WHERE identity.user_id=%s AND identity.provider=links.provider
                             AND identity.provider_user_id=links.provider_user_id)
                """, (user_id, user_id, user["telegram_user_id"], user["telegram_user_id"], user_id))
            if _exists(conn, "admin_login_tickets") and user["telegram_user_id"] is not None:
                conn.execute("DELETE FROM admin_login_tickets WHERE telegram_user_id=%s",
                             (user["telegram_user_id"],))
            # Legacy shop-registration ownership is a Telegram ID, not a FK.
            # Clear only this deleted person's pending reference; never change
            # a shop's real owner memberships or other registration fields.
            pending_owner_column = conn.execute("""
                SELECT 1 FROM pg_attribute
                WHERE attrelid='coffee_shops'::regclass
                  AND attname='pending_owner_telegram_id' AND NOT attisdropped
            """).fetchone()
            if pending_owner_column and user["telegram_user_id"] is not None:
                conn.execute("UPDATE coffee_shops SET pending_owner_telegram_id=NULL "
                             "WHERE pending_owner_telegram_id=%s", (user["telegram_user_id"],))

            conn.execute("UPDATE app_sessions SET revoked_at=statement_timestamp() "
                         "WHERE user_id=%s AND revoked_at IS NULL", (user_id,))
            # Cascades remove identities, both session purposes, memberships,
            # personal balances, push/Wallet profile and unconsumed previews.
            conn.execute("DELETE FROM users WHERE id=%s", (user_id,))
            return {"status": "deleted", "user_id": user_id,
                    "apple_revocation": state["apple_revocation"]}
