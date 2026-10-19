"""Native identity-only sessions and owner claims using existing invite/membership rows."""

import secrets

from psycopg.errors import UniqueViolation

from app.admin_invites import InviteError, _digest, _pepper, _public_invite, _record_attempt
from app.barista_sessions import BaristaSessionError, resolve_barista_session
from app.db import _hash_barista_session_token, create_app_session, get_connection
from app.owner_assignment import has_owner_access


def create_onboarding_identity(provider: str, subject: str) -> dict:
    """Create only a verified native identity; never infer/link by email or Telegram."""
    with get_connection() as connection:
        try:
            with connection.transaction():
                with connection.cursor() as cur:
                    cur.execute(
                        """SELECT u.* FROM user_identities ui JOIN users u ON u.id=ui.user_id
                           WHERE ui.provider=%s AND ui.provider_user_id=%s""", (provider, subject),
                    )
                    existing = cur.fetchone()
                    if existing:
                        return existing
                    cur.execute(
                        """INSERT INTO users(telegram_user_id,personal_qr_token)
                           VALUES(NULL,%s) RETURNING *""", (secrets.token_urlsafe(32),),
                    )
                    user = cur.fetchone()
                    cur.execute(
                        """INSERT INTO user_identities(user_id,provider,provider_user_id)
                           VALUES(%s,%s,%s)""", (user["id"], provider, subject),
                    )
                    return user
        except UniqueViolation:
            # A simultaneous native/consumer login may have linked the subject.
            # The losing user's tentative row is rolled back with its transaction.
            with connection.cursor() as cur:
                cur.execute(
                    """SELECT u.* FROM user_identities ui JOIN users u ON u.id=ui.user_id
                       WHERE ui.provider=%s AND ui.provider_user_id=%s""", (provider, subject),
                )
                existing = cur.fetchone()
                if existing is None:
                    raise
                return existing


def resolve_onboarding_session(token: str, *, connection=None, logout: bool = False,
                               lock: bool = False, upgrade_if_assigned: bool = False) -> dict:
    if connection is None:
        with get_connection() as connection:
            with connection.transaction():
                return resolve_onboarding_session(token, connection=connection, logout=logout, lock=lock,
                                                  upgrade_if_assigned=upgrade_if_assigned)
    digest = _hash_barista_session_token(token)
    if not digest:
        raise BaristaSessionError("INVALID_SESSION")
    with connection.cursor() as cur:
        cur.execute(
            """SELECT s.id AS session_id,s.user_id,s.expires_at AS session_expires_at,
                      u.full_name,u.username,u.telegram_user_id
               FROM app_sessions s JOIN users u ON u.id=s.user_id
               WHERE s.token_hash=%s AND s.purpose='barista_onboarding'
                 AND s.selected_membership_id IS NULL AND s.revoked_at IS NULL
                 AND s.expires_at>clock_timestamp()""" + (" FOR UPDATE OF s" if lock or logout else ""),
            (digest,),
        )
        session = cur.fetchone()
        if session is None:
            raise BaristaSessionError("INVALID_SESSION")
        if (lock or logout) and not cur.execute(
            "SELECT %s>clock_timestamp() AS valid", (session["session_expires_at"],),
        ).fetchone()["valid"]:
            raise BaristaSessionError("INVALID_SESSION")
        if logout:
            cur.execute("UPDATE app_sessions SET revoked_at=clock_timestamp() WHERE id=%s", (session["session_id"],))
        elif upgrade_if_assigned:
            # Match staff authorization's shop -> membership -> session lock order.
            # A Super Admin can assign access while this device is on onboarding.
            cur.execute("""SELECT cs.id FROM coffee_shops cs
                WHERE cs.is_active IS TRUE AND EXISTS(SELECT 1 FROM shop_admins sa
                    WHERE sa.shop_id=cs.id AND sa.user_id=%s AND sa.role IN ('admin','owner'))
                ORDER BY cs.id FOR NO KEY UPDATE OF cs""", (session["user_id"],))
            cur.fetchall()
            memberships = cur.execute("""SELECT sa.id FROM shop_admins sa
                JOIN coffee_shops cs ON cs.id=sa.shop_id WHERE sa.user_id=%s
                AND sa.role IN ('admin','owner') AND cs.is_active IS TRUE
                ORDER BY sa.id FOR SHARE OF sa""", (session["user_id"],)).fetchall()
            if memberships:
                promoted = cur.execute("""UPDATE app_sessions SET purpose='barista',selected_membership_id=%s
                    WHERE id=%s AND purpose='barista_onboarding' AND selected_membership_id IS NULL
                    AND revoked_at IS NULL AND expires_at>clock_timestamp() RETURNING id""",
                    (memberships[0]["id"], session["session_id"])).fetchone()
                if promoted is None:
                    raise BaristaSessionError("INVALID_SESSION")
                return resolve_barista_session(token, connection=connection)
        session.update(memberships=[], selected_membership_id=None, onboarding_required=True)
        return session


def create_owner_invite(connection, shop_id: int) -> dict:
    pepper = _pepper()
    with connection.cursor() as cur:
        cur.execute(
            """SELECT id,pending_owner_telegram_id FROM coffee_shops
               WHERE id=%s AND is_active IS TRUE FOR NO KEY UPDATE""", (shop_id,),
        )
        shop = cur.fetchone()
        if shop is None:
            raise InviteError("SHOP_NOT_FOUND", 404)
        cur.execute("SELECT id FROM shop_admins WHERE shop_id=%s AND role='owner' LIMIT 1", (shop_id,))
        if cur.fetchone() or shop["pending_owner_telegram_id"] is not None:
            raise InviteError("OWNER_ALREADY_CONNECTED", 409)
        cur.execute(
            """SELECT code_hash FROM barista_admin_invites WHERE shop_id=%s
               AND role='owner' AND expires_at>statement_timestamp()""", (shop_id,),
        )
        previous_hashes = {row["code_hash"] for row in cur.fetchall()}
        cur.execute(
            """UPDATE barista_admin_invites SET revoked_at=statement_timestamp(),closed_at=statement_timestamp()
               WHERE shop_id=%s AND role='owner' AND closed_at IS NULL""", (shop_id,),
        )
        for _ in range(32):
            code = f"{secrets.randbelow(1_000_000):06d}"
            code_hash = _digest(pepper, "invite-code:" + code)
            if code_hash in previous_hashes:
                continue
            cur.execute(
                """INSERT INTO barista_admin_invites(shop_id,role,code_hash,expires_at)
                   VALUES(%s,'owner',%s,statement_timestamp()+INTERVAL '7 days')
                   ON CONFLICT (code_hash) WHERE closed_at IS NULL DO NOTHING RETURNING *""",
                (shop_id, code_hash),
            )
            row = cur.fetchone()
            if row:
                return {**_public_invite(row), "code": code}
    raise InviteError("INVITE_CREATION_UNAVAILABLE", 503)


def claim_invite(*, token: str, code: str) -> tuple[str, dict]:
    """Bearer-derived actor; shop, role and one-time consumption are server-owned."""
    pepper = _pepper()
    error, result = None, None
    with get_connection() as connection:
        with connection.transaction():
            actor = resolve_onboarding_session(token, connection=connection)
            error = _record_attempt(connection, pepper, actor["user_id"])
            with connection.cursor() as cur:
                if error is None:
                    cur.execute(
                        "SELECT * FROM barista_admin_invites WHERE code_hash=%s AND closed_at IS NULL",
                        (_digest(pepper, "invite-code:" + code),),
                    )
                    candidate = cur.fetchone()
                    shop = None
                    network_shops = []
                    if candidate:
                        if candidate.get("network_id") is not None:
                            cur.execute("""SELECT id FROM coffee_networks WHERE id=%s FOR NO KEY UPDATE""",
                                        (candidate["network_id"],))
                            network = cur.fetchone()
                            cur.execute("""SELECT id FROM coffee_shops WHERE network_id=%s
                                AND is_active IS TRUE ORDER BY id FOR NO KEY UPDATE""",
                                (candidate["network_id"],))
                            network_shops = cur.fetchall()
                            if network is None or not network_shops:
                                error = InviteError("INVALID_INVITE")
                            else:
                                cur.execute("""SELECT id FROM network_memberships
                                    WHERE network_id=%s AND user_id=%s FOR SHARE""",
                                    (candidate["network_id"], actor["user_id"]))
                                if cur.fetchone():
                                    error = InviteError("MEMBERSHIP_ALREADY_EXISTS", 409)
                        else:
                            cur.execute(
                                """SELECT id,pending_owner_telegram_id FROM coffee_shops
                                   WHERE id=%s AND is_active IS TRUE FOR NO KEY UPDATE""", (candidate["shop_id"],),
                            )
                            shop = cur.fetchone()
                    if candidate and candidate.get("network_id") is None and shop is None:
                        error = InviteError("INVALID_INVITE")
                    elif candidate and candidate.get("network_id") is None and candidate["role"] == "owner":
                        cur.execute("SELECT id FROM shop_admins WHERE shop_id=%s AND role='owner' LIMIT 1", (shop["id"],))
                        if cur.fetchone() or shop["pending_owner_telegram_id"] is not None:
                            error = InviteError("OWNER_ALREADY_CONNECTED", 409)
                    elif candidate and candidate.get("network_id") is None:
                        cur.execute(
                            """SELECT id FROM shop_admins WHERE id=%s AND user_id=%s AND shop_id=%s
                               AND role='owner' FOR SHARE""",
                            (candidate["creator_membership_id"], candidate["creator_user_id"], candidate["shop_id"]),
                        )
                        if cur.fetchone() is None:
                            error = InviteError("INVALID_INVITE")
                    elif candidate is None:
                        error = InviteError("INVALID_INVITE")
                    if error is None:
                        # Shop first, then account/session/invite: deletion cannot
                        # win after this bearer has been checked under its lock.
                        cur.execute("SELECT id FROM users WHERE id=%s FOR " +
                                    ("UPDATE" if candidate["role"] == "owner" else "SHARE"), (actor["user_id"],))
                        resolve_onboarding_session(token, connection=connection, lock=True)
                        cur.execute(
                            """SELECT * FROM barista_admin_invites WHERE id=%s
                               AND closed_at IS NULL AND used_at IS NULL AND revoked_at IS NULL
                               AND expires_at>clock_timestamp() FOR UPDATE""", (candidate["id"],),
                        )
                        invite = cur.fetchone()
                        if invite is None or not cur.execute(
                            "SELECT %s>clock_timestamp() AS valid", (invite["expires_at"],),
                        ).fetchone()["valid"]:
                            error = InviteError("INVALID_INVITE")
                        elif invite["role"] == "owner" and has_owner_access(connection, actor["user_id"]):
                            error = InviteError("OWNER_ACCESS_ALREADY_EXISTS", 409)
                        else:
                            try:
                                with connection.transaction():
                                    if invite.get("network_id") is not None:
                                        cur.execute("""INSERT INTO network_memberships(user_id,network_id,role)
                                            VALUES(%s,%s,'NETWORK_OWNER') RETURNING id""",
                                            (actor["user_id"], invite["network_id"]))
                                        network_membership = cur.fetchone()
                                        for point in network_shops:
                                            cur.execute("""INSERT INTO shop_admins
                                                (shop_id,user_id,role,network_membership_id)
                                                VALUES(%s,%s,'owner',%s)
                                                ON CONFLICT(shop_id,user_id) DO NOTHING""",
                                                (point["id"],actor["user_id"],network_membership["id"]))
                                        cur.execute("""SELECT id FROM shop_admins
                                            WHERE shop_id=%s AND user_id=%s""",
                                            (network_shops[0]["id"],actor["user_id"]))
                                        membership = cur.fetchone()
                                    else:
                                        cur.execute(
                                            """INSERT INTO shop_admins(shop_id,user_id,role) VALUES(%s,%s,%s)
                                               ON CONFLICT(shop_id,user_id) DO NOTHING RETURNING id""",
                                            (invite["shop_id"], actor["user_id"], invite["role"]),
                                        )
                                        membership = cur.fetchone()
                                        if membership is None:
                                            raise InviteError("MEMBERSHIP_ALREADY_EXISTS", 409)
                                    cur.execute(
                                        """UPDATE barista_admin_invites SET used_at=clock_timestamp(),
                                           closed_at=clock_timestamp(),used_by_user_id=%s
                                           WHERE id=%s AND closed_at IS NULL AND expires_at>clock_timestamp()
                                           RETURNING id""", (actor["user_id"], invite["id"]),
                                    )
                                    if cur.fetchone() is None:
                                        raise InviteError("INVALID_INVITE")
                                    new_token = create_app_session(actor["user_id"], purpose="barista",
                                        selected_membership_id=membership["id"], connection=connection)
                                    cur.execute("UPDATE app_sessions SET revoked_at=clock_timestamp() WHERE id=%s", (actor["session_id"],))
                                    result = (new_token, resolve_barista_session(new_token, connection=connection))
                            except InviteError as exc:
                                error = exc
        if error:
            raise error
    assert result is not None
    return result
