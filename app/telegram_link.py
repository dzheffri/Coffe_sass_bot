"""One-time Telegram linking shared by authenticated HTTP and bot callbacks.

Callers must establish the Telegram actor before invoking these functions.
Only the bot's private /start handler may bind a pending link to that actor;
confirmation never accepts an unbound session.
"""

import hashlib
from datetime import datetime, timezone

from app.db import (
    get_connection,
    get_user_by_identity,
    link_user_identity,
    merge_users,
)


def _failure(status: str, message: str) -> dict:
    return {"ok": False, "status": status, "message": message}


def _token_hash(token: str) -> str | None:
    clean_token = (token or "").strip()
    if not clean_token:
        return None
    return hashlib.sha256(clean_token.encode("utf-8")).hexdigest()


def _actor_matches(session: dict, telegram_id: int) -> bool:
    return (session["telegram_user_id"] is not None
            and session["telegram_user_id"] == telegram_id)


def _confirmed_response(session: dict) -> dict:
    return {
        "ok": True,
        "status": "confirmed",
        "user_id": session["user_id"],
        "telegram_user_id": session["telegram_user_id"],
        "personal_qr_token": session["personal_qr_token"],
    }


def bind_telegram_link_session(token: str, telegram_id: int) -> dict:
    """Bind a link to the actor from a trusted private Telegram /start update."""
    token_hash = _token_hash(token)
    if not token_hash or not isinstance(telegram_id, int) or telegram_id <= 0:
        return _failure("not_found", "Сесію не знайдено")

    with get_connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM telegram_link_sessions
                    WHERE token_hash = %s
                    FOR UPDATE
                """, (token_hash,))
                session = cur.fetchone()
                if not session:
                    return _failure("not_found", "Сесію не знайдено")

                if (session["telegram_user_id"] is not None
                        and not _actor_matches(session, telegram_id)):
                    return _failure("actor_mismatch", "Не вдалося підтвердити підключення")

                if session["status"] == "confirmed":
                    if _actor_matches(session, telegram_id):
                        return _confirmed_response(session)
                    return _failure("actor_mismatch", "Не вдалося підтвердити підключення")

                if session["status"] != "pending":
                    return _failure("not_found", "Сесію не знайдено")
                if datetime.now(timezone.utc) >= session["expires_at"]:
                    return _failure("expired", "Час підтвердження завершився")

                cur.execute("""
                    UPDATE telegram_link_sessions
                    SET telegram_user_id = %s
                    WHERE token_hash = %s
                """, (telegram_id, token_hash))
                return {"ok": True, "status": "pending"}


def confirm_telegram_link_session(token: str, telegram_id: int) -> dict:
    """Confirm an already bound link using a caller-verified Telegram actor."""
    token_hash = _token_hash(token)
    if not token_hash or not isinstance(telegram_id, int) or telegram_id <= 0:
        return _failure("not_found", "Сесію не знайдено")

    with get_connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM telegram_link_sessions
                    WHERE token_hash = %s
                    FOR UPDATE
                """, (token_hash,))
                session = cur.fetchone()
                if not session:
                    return _failure("not_found", "Сесію не знайдено")

                # Check the actor before even returning an idempotent receipt.
                if not _actor_matches(session, telegram_id):
                    return _failure("actor_mismatch", "Не вдалося підтвердити підключення")
                if session["status"] == "confirmed":
                    return _confirmed_response(session)
                if session["status"] != "pending":
                    return _failure("not_found", "Сесію не знайдено")
                if datetime.now(timezone.utc) >= session["expires_at"]:
                    return _failure("expired", "Час підтвердження завершився")

                telegram_user = get_user_by_identity("telegram", str(telegram_id))
                if not telegram_user:
                    return _failure(
                        "telegram_not_found", "Профіль Telegram у «Наші» не знайдено"
                    )

                session_mode = session["mode"] or "link"
                if session_mode == "merge":
                    current_user_id = session["current_user_id"]
                    if not current_user_id:
                        return _failure("merge_error", "Не вдалося визначити поточний профіль")

                    # The existing merge commits independently. If it succeeded
                    # before a process failure, retry finishes this receipt rather
                    # than merging/debiting the source a second time.
                    identity_user = get_user_by_identity(
                        session["provider"], session["provider_user_id"]
                    )
                    if not identity_user:
                        return _failure("merge_error", "Поточний спосіб входу не знайдено")
                    if identity_user["id"] == telegram_user["id"]:
                        action_status = "already_merged"
                    elif identity_user["id"] != current_user_id:
                        return _failure("provider_conflict", "Спосіб входу змінився. Почніть підключення знову")
                    else:
                        try:
                            merge_result = merge_users(
                                source_user_id=current_user_id,
                                target_user_id=telegram_user["id"],
                            )
                        except Exception:
                            return _failure("merge_error", "Не вдалося об’єднати профілі")
                        action_status = merge_result["status"]
                        if action_status not in {"merged", "already_merged"}:
                            return _failure(
                                action_status,
                                "Ці профілі мають різні способи входу одного типу"
                                if action_status == "provider_conflict"
                                else "Не вдалося об’єднати профілі",
                            )
                elif session_mode == "link":
                    link_result = link_user_identity(
                        telegram_user["id"],
                        session["provider"],
                        session["provider_user_id"],
                    )
                    action_status = link_result["status"]
                    if action_status not in {"linked", "already_linked"}:
                        return _failure(action_status, "Не вдалося прив’язати профіль")
                else:
                    return _failure("not_found", "Сесію не знайдено")

                cur.execute("""
                    UPDATE telegram_link_sessions
                    SET status = 'confirmed', user_id = %s,
                        personal_qr_token = %s, confirmed_at = NOW()
                    WHERE token_hash = %s
                """, (telegram_user["id"], telegram_user["personal_qr_token"], token_hash))
                return {
                    "ok": True,
                    "status": "confirmed",
                    "action_status": action_status,
                    "message": "Профілі успішно об’єднано"
                    if session_mode == "merge" else "Профіль успішно підключено",
                    "user_id": telegram_user["id"],
                    "telegram_user_id": telegram_user["telegram_user_id"],
                    "personal_qr_token": telegram_user["personal_qr_token"],
                }
