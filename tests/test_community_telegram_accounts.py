"""Community Telegram receipts survive account operations in local test DB."""

from datetime import datetime, timezone

import pytest

from conftest import PROJECT, extract_definitions
from test_account_deletion import deletion_database, operations_database


@pytest.fixture
def telegram_accounts(deletion_database):
    db = deletion_database
    db.query((PROJECT / "migrations/20261015_community_posts.sql").read_text())
    db.query((PROJECT / "migrations/20261025_community_telegram.sql").read_text())
    namespace = dict(db.db.__dict__)
    extract_definitions(PROJECT / "app/db.py", {"merge_users"}, namespace)
    return db, namespace["merge_users"]


def _post(db):
    return db.query("""INSERT INTO community_posts(type,title,text,published_at)
        VALUES('news','Спільнота','Текст',NOW()) RETURNING id""")[0]["id"]


def _receipt(db, post_id, user_id, *, status, chat_id=None, message_id=None):
    return db.query("""INSERT INTO community_telegram_deliveries(
        post_id,user_id,telegram_user_id,status,message_id,error_code,attempted_at)
        VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING *""", (
            post_id, user_id, chat_id or 1000 + user_id, status, message_id,
            "TelegramNetworkError" if status == "unknown" else None,
            datetime(2026, 10, 20, tzinfo=timezone.utc),
        ))[0]


@pytest.mark.parametrize("status", ["sent", "unknown", "sending", "skipped", "failed", "pending"])
def test_merge_preserves_source_only_receipt_and_app_identity(telegram_accounts, status):
    db, merge = telegram_accounts
    post_id = _post(db)
    source = _receipt(db, post_id, 2, status=status, message_id=25 if status == "sent" else None)
    unrelated = _receipt(db, post_id, 3, status="sent", message_id=99)

    assert merge(2, 4) == {"status": "merged", "user_id": 4}
    assert db.query("SELECT * FROM community_telegram_deliveries ORDER BY user_id") == [
        unrelated, {**source, "user_id": 4}]
    assert db.query("SELECT user_id FROM user_identities WHERE provider='google'") == [{"user_id": 1}, {"user_id": 4}]
    assert db.query("SELECT id FROM users WHERE id=2") == []


@pytest.mark.parametrize("source_status,target_status,expected", [
    ("sent", "pending", "sent"),
    ("sent", "failed", "sent"),
    ("failed", "sent", "sent"),
    ("unknown", "failed", "unknown"),
    ("pending", "unknown", "unknown"),
    ("sending", "pending", "sending"),
    ("skipped", "failed", "skipped"),
    ("failed", "pending", "failed"),
    ("sent", "sent", "sent"),
])
def test_merge_overlapping_receipts_preserves_strongest_no_replay_evidence(
    telegram_accounts, source_status, target_status, expected,
):
    db, merge = telegram_accounts
    post_id = _post(db)
    source = _receipt(db, post_id, 2, status=source_status, message_id=22)
    target = _receipt(db, post_id, 4, status=target_status, message_id=44)
    priority = {"pending": 0, "failed": 1, "skipped": 2, "sending": 3, "unknown": 3, "sent": 4}
    winner = source if priority[source_status] > priority[target_status] else target

    assert merge(2, 4) == {"status": "merged", "user_id": 4}
    rows = db.query("SELECT * FROM community_telegram_deliveries")
    assert rows == [{**winner, "user_id": 4, "created_at": min(source["created_at"], target["created_at"])}]
    assert rows[0]["status"] == expected
    before_repeat = list(rows)
    assert merge(4, 4) == {"status": "already_merged", "user_id": 4}
    assert db.query("SELECT * FROM community_telegram_deliveries") == before_repeat


def test_delete_account_cascades_personal_delivery_rows_only(telegram_accounts):
    db, _ = telegram_accounts
    post_id = _post(db)
    _receipt(db, post_id, 2, status="sent")
    other = _receipt(db, post_id, 4, status="sent")
    post_before = db.query("SELECT * FROM community_posts")

    assert db.db.delete_user_account(2)["status"] == "deleted"
    assert db.query("SELECT * FROM community_telegram_deliveries") == [other]
    assert db.query("SELECT * FROM community_posts") == post_before
