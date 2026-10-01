"""Opaque staff sessions; no client token can enter this authorization path."""

from app.db import _hash_barista_session_token, get_connection


class BaristaSessionError(Exception):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def get_barista_memberships(user_id: int) -> list[dict]:
    """Read staff access by internal user ID, without Telegram or client roles."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sa.id AS membership_id, sa.shop_id, cs.name, sa.role
                FROM shop_admins sa
                JOIN coffee_shops cs ON cs.id = sa.shop_id
                WHERE sa.user_id = %s AND sa.role IN ('admin', 'owner')
                ORDER BY sa.id
                """,
                (user_id,),
            )
            return cur.fetchall()


def resolve_barista_session(
    token: str,
    *,
    shop_id: int | None = None,
    logout: bool = False,
) -> dict:
    """Authorize current membership before extending, selecting context or logout.

    Membership rows are locked before the session row, matching the order of
    membership deletion followed by the session FK cascade. Context changes and
    revocations are re-read under the session lock; a stale read cannot revive
    an expired/revoked session or overwrite an unchecked shop context.
    """
    token_hash = _hash_barista_session_token(token)
    if not token_hash:
        raise BaristaSessionError("INVALID_SESSION")

    with get_connection() as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                # This lookup does not extend the session or lock it before staff.
                cur.execute(
                    """
                    SELECT user_id
                    FROM app_sessions
                    WHERE token_hash = %s AND purpose = 'barista'
                      AND revoked_at IS NULL
                      AND expires_at > statement_timestamp()
                    """,
                    (token_hash,),
                )
                candidate = cur.fetchone()
                if candidate is None:
                    raise BaristaSessionError("INVALID_SESSION")

                cur.execute(
                    """
                    SELECT sa.id AS membership_id, sa.shop_id, cs.name, sa.role
                    FROM shop_admins sa
                    JOIN coffee_shops cs ON cs.id = sa.shop_id
                    WHERE sa.user_id = %s AND sa.role IN ('admin', 'owner')
                    ORDER BY sa.id
                    FOR SHARE OF sa
                    """,
                    (candidate["user_id"],),
                )
                memberships = cur.fetchall()

                cur.execute(
                    """
                    SELECT s.id AS session_id, s.user_id, s.selected_membership_id,
                           s.expires_at AS session_expires_at,
                           u.full_name, u.username
                    FROM app_sessions s
                    JOIN users u ON u.id = s.user_id
                    WHERE s.token_hash = %s AND s.user_id = %s
                      AND s.purpose = 'barista' AND s.revoked_at IS NULL
                      AND s.expires_at > statement_timestamp()
                    FOR UPDATE OF s
                    """,
                    (token_hash, candidate["user_id"]),
                )
                session = cur.fetchone()
                if session is None:
                    raise BaristaSessionError("INVALID_SESSION")

                selected = next(
                    (
                        membership
                        for membership in memberships
                        if membership["membership_id"]
                        == session["selected_membership_id"]
                    ),
                    None,
                )
                if selected is None:
                    raise BaristaSessionError("STAFF_ACCESS_REQUIRED")

                if shop_id is not None:
                    selected = next(
                        (
                            membership
                            for membership in memberships
                            if membership["shop_id"] == shop_id
                        ),
                        None,
                    )
                    if selected is None:
                        raise BaristaSessionError("SHOP_ACCESS_DENIED")

                if logout:
                    cur.execute(
                        """
                        UPDATE app_sessions
                        SET revoked_at = statement_timestamp()
                        WHERE id = %s AND purpose = 'barista'
                          AND revoked_at IS NULL
                          AND expires_at > statement_timestamp()
                        RETURNING expires_at AS session_expires_at
                        """,
                        (session["session_id"],),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE app_sessions
                        SET selected_membership_id = %s,
                            last_used_at = statement_timestamp(),
                            -- 30 elapsed days, independent of the DB timezone/DST.
                            expires_at = statement_timestamp() + INTERVAL '720 hours'
                        WHERE id = %s AND purpose = 'barista'
                          AND revoked_at IS NULL
                          AND expires_at > statement_timestamp()
                        RETURNING expires_at AS session_expires_at
                        """,
                        (selected["membership_id"], session["session_id"]),
                    )
                updated = cur.fetchone()
                if updated is None:
                    raise BaristaSessionError("INVALID_SESSION")

                session.update(updated)
                session["selected_membership_id"] = selected["membership_id"]
                session["memberships"] = memberships
                return session
