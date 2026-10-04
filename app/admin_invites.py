"""One-use admin invitations; no plaintext codes or independent user identity."""

import hashlib
import hmac
import os
import secrets
from datetime import timedelta, timezone

from app.barista_sessions import resolve_barista_session
from app.db import create_app_session, get_connection


ATTEMPT_WINDOW_SECONDS = 15 * 60
USER_ATTEMPT_LIMIT = 5
GLOBAL_ATTEMPT_LIMIT = 100
DAILY_ATTEMPT_WINDOW_SECONDS = 24 * 60 * 60
DAILY_USER_ATTEMPT_LIMIT = 5
DAILY_GLOBAL_ATTEMPT_LIMIT = 100
MAX_ACTIVE_INVITES_PER_SHOP = 10


class InviteError(Exception):
    def __init__(self, code: str, status: int = 400, retry_after: int | None = None):
        self.code, self.status, self.retry_after = code, status, retry_after
        super().__init__(code)


def _pepper() -> bytes:
    value = os.getenv("BARISTA_INVITE_PEPPER", "")
    if len(value.encode("utf-8")) < 32:
        raise InviteError("INVITES_NOT_CONFIGURED", 503)
    return value.encode("utf-8")


def _digest(pepper: bytes, value: str) -> str:
    return hmac.new(pepper, value.encode("utf-8"), hashlib.sha256).hexdigest()


def _public_invite(row: dict) -> dict:
    return {key: row[key] for key in (
        "id", "role", "created_at", "expires_at", "used_at", "revoked_at",
    )}


def create_invite(connection, *, owner: dict, user_id: int) -> dict:
    pepper = _pepper()
    with connection.cursor() as cur:
        # Authorization already holds a shared membership lock. Serializing
        # creation on the shop prevents the active-invitation limit racing.
        cur.execute("SELECT id FROM coffee_shops WHERE id=%s FOR NO KEY UPDATE", (owner["shop_id"],))
        cur.execute(
            """UPDATE barista_admin_invites SET closed_at=expires_at
               WHERE shop_id=%s AND expires_at<=statement_timestamp() AND closed_at IS NULL""",
            (owner["shop_id"],),
        )
        cur.execute(
            """SELECT COUNT(*) AS n FROM barista_admin_invites
               WHERE shop_id=%s AND closed_at IS NULL""", (owner["shop_id"],)
        )
        if cur.fetchone()["n"] >= MAX_ACTIVE_INVITES_PER_SHOP:
            raise InviteError("INVITE_LIMIT_REACHED", 409)
        for _ in range(32):
            code = f"{secrets.randbelow(1_000_000):06d}"
            cur.execute(
                """INSERT INTO barista_admin_invites
                   (shop_id,creator_user_id,creator_membership_id,code_hash,expires_at)
                   VALUES (%s,%s,%s,%s,statement_timestamp()+INTERVAL '24 hours')
                   ON CONFLICT (code_hash) WHERE closed_at IS NULL DO NOTHING
                   RETURNING *""",
                (owner["shop_id"], user_id, owner["membership_id"],
                 _digest(pepper, "invite-code:" + code)),
            )
            row = cur.fetchone()
            if row:
                return {**_public_invite(row), "code": code}
        raise InviteError("INVITE_CREATION_UNAVAILABLE", 503)


def list_invites(connection, shop_id: int) -> list[dict]:
    with connection.cursor() as cur:
        cur.execute(
            """SELECT * FROM barista_admin_invites
               WHERE shop_id=%s AND closed_at IS NULL
                 AND expires_at>statement_timestamp()
               ORDER BY created_at DESC,id DESC""", (shop_id,)
        )
        return [_public_invite(row) for row in cur.fetchall()]


def revoke_invite(connection, shop_id: int, invite_id: int) -> None:
    with connection.cursor() as cur:
        cur.execute(
            """UPDATE barista_admin_invites
               SET revoked_at=statement_timestamp(),closed_at=statement_timestamp()
               WHERE id=%s AND shop_id=%s AND used_at IS NULL AND revoked_at IS NULL
               RETURNING id""", (invite_id, shop_id)
        )
        if cur.fetchone() is None:
            raise InviteError("INVITE_NOT_FOUND", 404)


def _record_attempt(connection, pepper: bytes, user_id: int) -> InviteError | None:
    """Persist a shared, row-locked user/global guard, including invalid codes.

    Both scopes use a single consistent lock order. Requests already blocked
    for one user do not consume the global budget and cannot lock out others.
    """
    # Daily budgets align with a code's 24-hour TTL; short-window resets cannot
    # turn a six-digit invitation into an unrestricted day-long guessing API.
    scopes = (
        ("global:day", DAILY_GLOBAL_ATTEMPT_LIMIT, DAILY_ATTEMPT_WINDOW_SECONDS),
        ("global", GLOBAL_ATTEMPT_LIMIT, ATTEMPT_WINDOW_SECONDS),
        (f"user:{user_id}:day", DAILY_USER_ATTEMPT_LIMIT, DAILY_ATTEMPT_WINDOW_SECONDS),
        (f"user:{user_id}", USER_ATTEMPT_LIMIT, ATTEMPT_WINDOW_SECONDS),
    )
    rows = []
    with connection.cursor() as cur:
        for scope, limit, window_seconds in scopes:
            digest = _digest(pepper, "invite-attempt:" + scope)
            cur.execute(
                """INSERT INTO barista_invite_attempts(scope_hash,window_started_at)
                   VALUES (%s,statement_timestamp()) ON CONFLICT DO NOTHING""", (digest,)
            )
            cur.execute(
                """SELECT *, statement_timestamp() AS now FROM barista_invite_attempts
                   WHERE scope_hash=%s FOR UPDATE""", (digest,)
            )
            row = cur.fetchone()
            expires = row["window_started_at"].astimezone(timezone.utc) + timedelta(seconds=window_seconds)
            now = row["now"].astimezone(timezone.utc)
            count = row["attempts"] if expires > now else 0
            rows.append((digest, row, count))
            if count >= limit:
                retry = max(1, int((expires - now).total_seconds()) + 1)
                return InviteError("INVITE_RATE_LIMITED", 429, retry)
        for digest, row, count in rows:
            cur.execute(
                """UPDATE barista_invite_attempts SET attempts=%s,window_started_at=%s
                   WHERE scope_hash=%s""",
                (count + 1, row["window_started_at"] if count else row["now"], digest),
            )
    return None


def accept_invite(*, provider: str, subject: str, user_id: int, code: str) -> tuple[str, dict]:
    """Recheck a verified existing identity and atomically consume one invite.

    Validation errors are raised after committing attempt counters. A failed
    membership/session write rolls back the complete acceptance transaction.
    """
    pepper = _pepper()
    error, result = None, None
    with get_connection() as connection:
        with connection.transaction():
            with connection.cursor() as cur:
                cur.execute(
                    """SELECT ui.user_id FROM user_identities ui JOIN users u ON u.id=ui.user_id
                       WHERE ui.provider=%s AND ui.provider_user_id=%s AND ui.user_id=%s
                       FOR SHARE OF ui,u""", (provider, subject, user_id)
                )
                if cur.fetchone() is None:
                    raise InviteError("IDENTITY_NOT_LINKED", 403)
                error = _record_attempt(connection, pepper, user_id)
                if error is None:
                    digest = _digest(pepper, "invite-code:" + code)
                    cur.execute(
                        """SELECT * FROM barista_admin_invites
                           WHERE code_hash=%s AND closed_at IS NULL""", (digest,)
                    )
                    candidate = cur.fetchone()
                    owner = None
                    if candidate is not None:
                        # Lock membership before invitation, matching owner
                        # authorization/revocation and membership FK deletion.
                        cur.execute(
                            """SELECT id FROM shop_admins WHERE id=%s AND user_id=%s
                               AND shop_id=%s AND role='owner' FOR SHARE""",
                            (candidate["creator_membership_id"], candidate["creator_user_id"],
                             candidate["shop_id"]),
                        )
                        owner = cur.fetchone()
                    if candidate is None or owner is None:
                        error = InviteError("INVALID_INVITE")
                    else:
                        cur.execute(
                            """SELECT * FROM barista_admin_invites WHERE id=%s
                               AND closed_at IS NULL AND used_at IS NULL AND revoked_at IS NULL
                               AND expires_at>statement_timestamp() FOR UPDATE""", (candidate["id"],)
                        )
                        invite = cur.fetchone()
                        # FOR UPDATE may have waited beyond the code TTL.
                        # Its statement_timestamp was taken before that wait;
                        # compare against fresh server time after acquiring it.
                        if invite is None or not cur.execute(
                            "SELECT %s > clock_timestamp() AS valid", (invite["expires_at"],)
                        ).fetchone()["valid"]:
                            error = InviteError("INVALID_INVITE")
                        else:
                            try:
                                # The membership unique/FK checks can also wait.
                                # A savepoint rolls this tentative insert back
                                # on TTL failure while retaining attempt limits.
                                with connection.transaction():
                                    cur.execute(
                                        """INSERT INTO shop_admins(shop_id,user_id,role) VALUES (%s,%s,'admin')
                                           ON CONFLICT (shop_id,user_id) DO NOTHING RETURNING id""",
                                        (invite["shop_id"], user_id),
                                    )
                                    membership = cur.fetchone()
                                    if membership is None:
                                        raise InviteError("MEMBERSHIP_ALREADY_EXISTS", 409)
                                    cur.execute(
                                        """UPDATE barista_admin_invites SET used_at=clock_timestamp(),
                                           closed_at=clock_timestamp(),used_by_user_id=%s
                                           WHERE id=%s AND used_at IS NULL AND revoked_at IS NULL
                                             AND closed_at IS NULL AND expires_at>clock_timestamp()
                                           RETURNING id""", (user_id, invite["id"]),
                                    )
                                    if cur.fetchone() is None:
                                        raise InviteError("INVALID_INVITE")
                                    token = create_app_session(
                                        user_id, purpose="barista", selected_membership_id=membership["id"],
                                        connection=connection,
                                    )
                                    result = (token, resolve_barista_session(token, connection=connection))
                            except InviteError as exc:
                                error = exc
        if error is not None:
            raise error
    assert result is not None
    return result
