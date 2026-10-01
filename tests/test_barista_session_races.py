"""Real PostgreSQL locking tests for expiry/revocation during authorization."""

import importlib
import time
from concurrent.futures import ThreadPoolExecutor

import pytest


def wait_for_authorization_lock(database, statement_fragment, future):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if future.done():
            pytest.fail("Authorization finished before the expected row lock")
        blocked = database.query(
            """SELECT pid FROM pg_stat_activity
               WHERE datname = current_database() AND pid <> pg_backend_pid()
                 AND wait_event_type = 'Lock' AND query LIKE %s""",
            ("%" + statement_fragment + "%",),
        )
        if blocked:
            return
        time.sleep(0.01)
    pytest.fail("Authorization did not reach the expected PostgreSQL lock")


@pytest.mark.parametrize("change", ["expire", "revoke"])
def test_session_changed_while_authorization_waits_cannot_be_revived(database, change):
    sessions = importlib.import_module("app.barista_sessions")
    token = database.staff_token()
    token_hash = database.db._hash_barista_session_token(token)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with database.db.get_connection() as locker:
            with locker.transaction():
                locker.execute("SELECT id FROM app_sessions WHERE token_hash=%s FOR UPDATE", (token_hash,))
                future = executor.submit(sessions.resolve_barista_session, token)
                wait_for_authorization_lock(database, "FOR UPDATE OF s", future)
                if change == "expire":
                    # The waiting SELECT's statement_timestamp predates this
                    # expiry. The final UPDATE must use a fresh server timestamp.
                    locker.execute("UPDATE app_sessions SET expires_at=clock_timestamp()+INTERVAL '150 milliseconds' WHERE token_hash=%s",
                                   (token_hash,))
                    time.sleep(0.25)
                else:
                    locker.execute("UPDATE app_sessions SET revoked_at=clock_timestamp() WHERE token_hash=%s",
                                   (token_hash,))
        with pytest.raises(sessions.BaristaSessionError, match="INVALID_SESSION"):
            future.result(timeout=3)
    row = database.session(token)
    assert row["last_used_at"] is None
    if change == "expire":
        assert database.query("SELECT expires_at <= clock_timestamp() AS expired FROM app_sessions WHERE token_hash=%s",
                              (token_hash,))[0]["expired"]
    else:
        assert row["revoked_at"] is not None


def test_committed_staff_removal_wins_over_waiting_authorization(database):
    sessions = importlib.import_module("app.barista_sessions")
    token = database.staff_token()
    client_token = database.db.create_app_session(1)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with database.db.get_connection() as locker:
            with locker.transaction():
                locker.execute("DELETE FROM shop_admins WHERE id=1")
                future = executor.submit(sessions.resolve_barista_session, token)
                wait_for_authorization_lock(database, "FOR SHARE OF sa", future)
        with pytest.raises(sessions.BaristaSessionError, match="INVALID_SESSION"):
            future.result(timeout=3)
    assert database.db.get_app_session(client_token) is not None
    assert database.query("SELECT id FROM app_sessions WHERE token_hash=%s",
                          (database.db._hash_barista_session_token(token),)) == []
