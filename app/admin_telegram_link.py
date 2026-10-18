"""Optional Telegram attachment initiated by an authenticated Nashi Admin user.

Uses the existing one-time link table and trusted private bot confirmation;
neither browser nor app supplies a Telegram identity. The Admin user ID stays
the target, including when existing Telegram account data must be merged.
"""

import secrets

from app import db
from app.admin_web_login import check_rate_limit, digest, WebLoginError


def start_admin_link(session, bot_username):
    check_rate_limit("telegram:" + str(session["user_id"]), 5)
    if not bot_username:
        raise WebLoginError("TELEGRAM_LINK_UNAVAILABLE", 503)
    token = secrets.token_urlsafe(24)
    with db.get_connection() as conn:
        with conn.transaction():
            user = conn.execute("SELECT telegram_user_id FROM users WHERE id=%s", (session["user_id"],)).fetchone()
            if user is None:
                raise WebLoginError("INVALID_SESSION", 401)
            if user["telegram_user_id"] is not None:
                raise WebLoginError("TELEGRAM_ALREADY_LINKED", 409)
            conn.execute("""UPDATE telegram_link_sessions SET status='expired'
                WHERE mode='admin_link' AND current_user_id=%s AND status='pending'""", (session["user_id"],))
            conn.execute("""INSERT INTO telegram_link_sessions
                (token_hash,provider,provider_user_id,mode,current_user_id,expires_at)
                VALUES (%s,'admin',%s,'admin_link',%s,statement_timestamp()+INTERVAL '10 minutes')""",
                (digest(token), str(session["user_id"]), session["user_id"]))
    return {"ok": True, "token": token,
            "deep_link": f"https://t.me/{bot_username}?start=link_{token}", "expires_in": 600}


def admin_link_status(session, token):
    with db.get_connection() as conn:
        row = conn.execute("""SELECT status,telegram_user_id,
            expires_at>statement_timestamp() AS valid FROM telegram_link_sessions
            WHERE token_hash=%s AND mode='admin_link' AND current_user_id=%s""",
            (digest(token), session["user_id"])).fetchone()
    if row is None:
        raise WebLoginError("LINK_NOT_AVAILABLE", 404)
    return {"ok": True, "status": "linked" if row["status"] == "confirmed" else
            ("pending" if row["valid"] and row["status"] == "pending" else "expired"),
            "telegram_user_id": row["telegram_user_id"] if row["status"] == "confirmed" else None}


def confirm_admin_link(conn, session, telegram_id):
    """Called only after the shared bot flow verified the bound private actor."""
    target = session["current_user_id"]
    current = conn.execute("SELECT * FROM users WHERE id=%s", (target,)).fetchone()
    if current is None:
        return {"ok": False, "status": "not_found", "message": "Профіль більше недоступний"}
    if current["telegram_user_id"] not in (None, telegram_id):
        return {"ok": False, "status": "provider_conflict", "message": "Telegram уже підключено"}
    # An explicit Telegram identity wins over a stale legacy column.
    source = db.get_user_by_identity("telegram", str(telegram_id))
    if source is None:
        source = conn.execute("SELECT * FROM users WHERE telegram_user_id=%s", (telegram_id,)).fetchone()
    if source and source["id"] != target:
        result = db.merge_users(source_user_id=source["id"], target_user_id=target)
        if result["status"] not in {"merged", "already_merged"}:
            return {"ok": False, "status": "provider_conflict",
                    "message": "Не вдалося безпечно підключити Telegram. Зверніться до підтримки"}
    # Serialize the identity binding with account deletion and other link attempts.
    current = conn.execute("SELECT * FROM users WHERE id=%s FOR UPDATE", (target,)).fetchone()
    identity = conn.execute("""SELECT user_id FROM user_identities
        WHERE provider='telegram' AND provider_user_id=%s FOR UPDATE""", (str(telegram_id),)).fetchone()
    if identity and identity["user_id"] != target:
        return {"ok": False, "status": "provider_conflict", "message": "Telegram уже підключено до іншого профілю"}
    if current is None or current["telegram_user_id"] not in (None, telegram_id):
        return {"ok": False, "status": "provider_conflict", "message": "Профіль змінився. Почніть знову"}
    conn.execute("""INSERT INTO user_identities(user_id,provider,provider_user_id)
        VALUES (%s,'telegram',%s) ON CONFLICT (provider,provider_user_id) DO NOTHING""", (target, str(telegram_id)))
    conn.execute("UPDATE users SET telegram_user_id=%s WHERE id=%s", (telegram_id, target))
    conn.execute("""UPDATE telegram_link_sessions SET status='confirmed',user_id=%s,
        personal_qr_token=%s,confirmed_at=statement_timestamp() WHERE token_hash=%s""",
        (target, current["personal_qr_token"], session["token_hash"]))
    return {"ok": True, "status": "confirmed", "admin_link": True,
            "user_id": target, "telegram_user_id": telegram_id,
            "personal_qr_token": current["personal_qr_token"]}
