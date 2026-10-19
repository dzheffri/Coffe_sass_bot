"""Short-lived browser pairing authorized by existing native staff membership.

Only hashes are persisted. QR/code cannot poll/consume a browser's request:
the separate browser-only poll secret is required. Web sessions never enter
consumer or Super Admin authorization.
"""

import hashlib
import hmac
import re
import secrets

from app.admin_invites import _pepper
from app.db import get_connection


class WebLoginError(Exception):
    def __init__(self, code, status=400):
        self.code, self.status = code, status
        super().__init__(code)


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def code_digest(code):
    return hmac.new(_pepper(), ("admin-web-code:" + code).encode(), hashlib.sha256).hexdigest()


def check_rate_limit(scope, limit=5):
    """Commit shared counters separately so failed guesses cannot roll them back."""
    blocked = False
    with get_connection() as conn:
        with conn.transaction():
            for name, budget in (("global:" + scope.split(":")[0], 500), (scope, limit)):
                key = digest("admin-web-rate:" + name)
                row = conn.execute("""
                    INSERT INTO admin_web_login_attempts(scope_hash,window_started_at,attempts)
                    VALUES (%s,statement_timestamp(),1)
                    ON CONFLICT (scope_hash) DO UPDATE SET
                      attempts=CASE WHEN admin_web_login_attempts.window_started_at
                        < statement_timestamp()-INTERVAL '5 minutes' THEN 1
                        ELSE admin_web_login_attempts.attempts+1 END,
                      window_started_at=CASE WHEN admin_web_login_attempts.window_started_at
                        < statement_timestamp()-INTERVAL '5 minutes' THEN statement_timestamp()
                        ELSE admin_web_login_attempts.window_started_at END
                    RETURNING attempts
                """, (key,)).fetchone()
                blocked = blocked or row["attempts"] > budget
    if blocked:
        raise WebLoginError("LOGIN_RATE_LIMITED", 429)


def start_request(client_address):
    check_rate_limit("start:" + client_address, 10)
    token, poll = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    with get_connection() as conn:
        with conn.transaction():
            conn.execute("""UPDATE admin_web_login_requests SET consumed_at=statement_timestamp()
                WHERE consumed_at IS NULL AND expires_at<=statement_timestamp()""")
            for _ in range(32):
                code = f"{secrets.randbelow(1_000_000):06d}"
                row = conn.execute("""
                    INSERT INTO admin_web_login_requests(token_hash,poll_hash,code_hash,expires_at)
                    VALUES (%s,%s,%s,statement_timestamp()+INTERVAL '5 minutes')
                    ON CONFLICT (code_hash) WHERE consumed_at IS NULL DO NOTHING
                    RETURNING expires_at
                """, (digest(token), digest(poll), code_digest(code))).fetchone()
                if row:
                    return {"ok": True, "request_token": token, "poll_token": poll,
                            "code": code, "expires_at": row["expires_at"]}
    raise WebLoginError("LOGIN_NOT_AVAILABLE", 503)


def find_request(conn, code_or_token):
    clean = code_or_token.strip()
    if re.fullmatch(r"[0-9]{6}", clean):
        column, value = "code_hash", code_digest(clean)
    elif re.fullmatch(r"[A-Za-z0-9_-]{32,256}", clean):
        column, value = "token_hash", digest(clean)
    else:
        raise WebLoginError("LOGIN_NOT_AVAILABLE", 404)
    row = conn.execute(f"""SELECT * FROM admin_web_login_requests
        WHERE {column}=%s AND consumed_at IS NULL AND approved_at IS NULL
          AND expires_at>statement_timestamp() FOR UPDATE""", (value,)).fetchone()
    if not row or not conn.execute("SELECT %s>clock_timestamp() AS valid", (row["expires_at"],)).fetchone()["valid"]:
        raise WebLoginError("LOGIN_NOT_AVAILABLE", 404)
    return row


def approve_request(code_or_token, token, authorize, *, confirm):
    # The native token is checked before spending the per-user guessing budget.
    actor = authorize(token)
    check_rate_limit("approve:" + str(actor["user_id"]), 5)
    with get_connection() as conn:
        with conn.transaction():
            actor = authorize(token, connection=conn)
            membership = next(m for m in actor["memberships"]
                              if m["membership_id"] == actor["selected_membership_id"])
            # Existing Web Admin login admits owners only. Native 'admin'
            # is the barista/staff role, not a new Web permission.
            if membership["role"] != "owner":
                raise WebLoginError("WEB_ACCESS_REQUIRED", 403)
            row = find_request(conn, code_or_token)
            if confirm:
                conn.execute("""UPDATE admin_web_login_requests SET approved_at=statement_timestamp(),
                    approved_user_id=%s,approved_membership_id=%s WHERE id=%s""",
                    (actor["user_id"], membership["membership_id"], row["id"]))
            return {"ok": True, "status": "confirmed" if confirm else "pending",
                    "expires_at": row["expires_at"],
                    "shop": {"shop_id": membership["shop_id"], "name": membership["name"]}}


def memberships(conn, user_id):
    if conn.execute("SELECT to_regclass('coffee_networks') AS networks").fetchone()["networks"] is not None:
        return conn.execute("""SELECT sa.id AS membership_id,sa.shop_id,
        CASE WHEN nm.id IS NOT NULL THEN 'owner' ELSE sa.role END AS role,cs.name,
        cs.network_id,n.name AS network_name
        FROM shop_admins sa JOIN coffee_shops cs ON cs.id=sa.shop_id
        LEFT JOIN coffee_networks n ON n.id=cs.network_id
        LEFT JOIN network_memberships nm ON nm.user_id=sa.user_id
            AND nm.network_id=cs.network_id AND nm.role='NETWORK_OWNER'
        WHERE sa.user_id=%s AND (sa.role='owner' OR nm.id IS NOT NULL) AND cs.is_active IS TRUE
        ORDER BY sa.id FOR SHARE OF sa,cs""", (user_id,)).fetchall()
    return conn.execute("""SELECT sa.id AS membership_id,sa.shop_id,sa.role,cs.name
        FROM shop_admins sa JOIN coffee_shops cs ON cs.id=sa.shop_id
        WHERE sa.user_id=%s AND sa.role='owner' AND cs.is_active IS TRUE
        ORDER BY sa.id FOR SHARE OF sa,cs""", (user_id,)).fetchall()


def network_memberships(conn, user_id):
    if conn.execute("SELECT to_regclass('network_memberships') AS memberships").fetchone()["memberships"] is None:
        return []
    return conn.execute("""SELECT n.id AS network_id,n.name,n.logo_url,n.loyalty_mode,
        nm.id AS network_membership_id,
        COALESCE(json_agg(json_build_object('shop_id',cs.id,'name',cs.name)
            ORDER BY cs.id) FILTER (WHERE cs.id IS NOT NULL),'[]'::json) AS shops
        FROM network_memberships nm JOIN coffee_networks n ON n.id=nm.network_id
        LEFT JOIN coffee_shops cs ON cs.network_id=n.id AND cs.is_active IS TRUE
        WHERE nm.user_id=%s AND nm.role='NETWORK_OWNER'
        GROUP BY n.id,nm.id ORDER BY n.id""", (user_id,)).fetchall()


def poll_request(token, poll_token):
    with get_connection() as conn:
        with conn.transaction():
            row = conn.execute("""SELECT *
                FROM admin_web_login_requests WHERE token_hash=%s FOR UPDATE""",
                (digest(token),)).fetchone()
            if not row or not hmac.compare_digest(row["poll_hash"], digest(poll_token)):
                raise WebLoginError("LOGIN_NOT_AVAILABLE", 404)
            valid = conn.execute("SELECT %s>clock_timestamp() AS valid", (row["expires_at"],)).fetchone()["valid"]
            if row["consumed_at"] or not valid:
                return {"ok": True, "status": "expired"}
            if not row["approved_at"]:
                return {"ok": True, "status": "pending"}
            shops = memberships(conn, row["approved_user_id"])
            selected = next((s for s in shops if s["membership_id"] == row["approved_membership_id"]), None)
            if selected is None:
                raise WebLoginError("WEB_ACCESS_REQUIRED", 403)
            session_token = secrets.token_urlsafe(32)
            conn.execute("""INSERT INTO admin_web_sessions(token_hash,user_id,selected_membership_id,expires_at)
                VALUES (%s,%s,%s,statement_timestamp()+INTERVAL '12 hours')""",
                (digest("admin-web-session:" + session_token), row["approved_user_id"], selected["membership_id"]))
            conn.execute("UPDATE admin_web_login_requests SET consumed_at=statement_timestamp() WHERE id=%s", (row["id"],))
            return {"ok": True, "status": "approved", "access_token": session_token,
                    "actor": {"user_id": row["approved_user_id"], "role": selected["role"]},
                    "shops": shops, "networks": network_memberships(conn, row["approved_user_id"]),
                    "selected_shop_id": selected["shop_id"]}


def resolve_web_session(conn, token, *, shop_id=None, owner_only=False):
    row = conn.execute("""SELECT s.*,u.telegram_user_id,u.full_name,u.username
        FROM admin_web_sessions s JOIN users u ON u.id=s.user_id
        WHERE s.token_hash=%s AND s.expires_at>statement_timestamp()
          AND s.revoked_at IS NULL FOR UPDATE OF s""",
        (digest("admin-web-session:" + token),)).fetchone()
    if row is None:
        raise WebLoginError("INVALID_SESSION", 401)
    shops = memberships(conn, row["user_id"])
    selected = next((s for s in shops if s["shop_id"] == shop_id), None) if shop_id else next(
        (s for s in shops if s["membership_id"] == row["selected_membership_id"]), None)
    if selected is None or (owner_only and selected["role"] != "owner"):
        raise WebLoginError("WEB_ACCESS_REQUIRED", 403)
    if shop_id is not None:
        conn.execute("UPDATE admin_web_sessions SET selected_membership_id=%s WHERE id=%s",
                     (selected["membership_id"], row["id"]))
    return {**row, "shops": shops, "networks": network_memberships(conn, row["user_id"]),
            "selected": selected, "shop_id": selected["shop_id"]}
