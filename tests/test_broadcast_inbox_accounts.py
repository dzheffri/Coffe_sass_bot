"""Recipient ownership survives shared-account operations in guarded local PostgreSQL.

Use the existing disposable database fixture and compile merge_users only; do not
import backend DB/config startup code or contact delivery providers.
"""

from datetime import datetime, timezone

import pytest

from conftest import PROJECT, extract_definitions
from test_account_deletion import deletion_database, operations_database


CREATED_EARLY = datetime(2026, 2, 1, tzinfo=timezone.utc)
CREATED_LATE = datetime(2026, 2, 3, tzinfo=timezone.utc)
READ_EARLY = datetime(2026, 3, 1, tzinfo=timezone.utc)
READ_LATE = datetime(2026, 3, 3, tzinfo=timezone.utc)


@pytest.fixture
def inbox_database(deletion_database):
    db = deletion_database
    db.query((PROJECT / "migrations/20261013_broadcast_recipients.sql").read_text())
    return db


@pytest.fixture
def merge_accounts(deletion_database):
    namespace = dict(deletion_database.db.__dict__)
    extract_definitions(PROJECT / "app/db.py", {"merge_users"}, namespace)
    return namespace["merge_users"]


def broadcast(db, *, sender_user_id=1, text="Повідомлення кав’ярні", media_type=None, media_key=None):
    return db.query("""
        INSERT INTO broadcasts(shop_id,sender_user_id,text,media_type,media_key)
        VALUES(1,%s,%s,%s,%s) RETURNING id
    """, (sender_user_id, text, media_type, media_key))[0]["id"]


def recipient(db, broadcast_id, user_id, *, read_at=None, delivery_status=None, created_at=CREATED_LATE):
    return db.query("""
        INSERT INTO broadcast_recipients(broadcast_id,user_id,read_at,delivery_status,created_at)
        VALUES(%s,%s,%s,%s,%s) RETURNING *
    """, (broadcast_id, user_id, read_at, delivery_status, created_at))[0]


def broadcast_audit(db):
    # Sender references may be anonymized or merged; content and media must remain.
    return db.query("SELECT id,shop_id,text,media_type,media_key FROM broadcasts ORDER BY id")


def snapshot(db, tables):
    return {table: db.query(f"SELECT * FROM {table} ORDER BY id") for table in tables}


def test_account_deletion_removes_only_deleted_users_recipient_rows(inbox_database):
    db = inbox_database
    second = broadcast(db, media_type="photo", media_key="retained-photo.jpg")
    recipient(db, 1, 2, read_at=READ_EARLY, delivery_status="sent")
    recipient(db, second, 2, delivery_status="failed")
    recipient(db, 1, 4, delivery_status="sent")
    recipient(db, second, 4, read_at=READ_LATE, delivery_status="sent")
    recipient(db, second, 1)
    other_recipients = db.query("SELECT * FROM broadcast_recipients WHERE user_id<>2 ORDER BY id")
    audit_before = broadcast_audit(db)

    assert db.db.delete_user_account(2)["status"] == "deleted"

    assert db.query("SELECT id FROM users WHERE id=2") == []
    assert db.query("SELECT * FROM broadcast_recipients WHERE user_id=2") == []
    assert db.query("SELECT * FROM broadcast_recipients ORDER BY id") == other_recipients
    assert broadcast_audit(db) == audit_before
    # The deleted sender's broadcast is retained through an anonymous audit reference.
    audit_sender = db.query("""
        SELECT u.id,u.full_name,u.telegram_user_id
        FROM broadcasts b JOIN users u ON u.id=b.sender_user_id WHERE b.id=1
    """)[0]
    assert audit_sender["id"] != 2
    assert audit_sender["full_name"] == "Видалений користувач"
    assert audit_sender["telegram_user_id"] is None
    assert db.query("SELECT is_active FROM coffee_shops WHERE id=1") == [{"is_active": True}]


def test_sole_owner_deletion_soft_closes_shop_and_preserves_other_recipients_and_broadcast_audit(inbox_database):
    db = inbox_database
    own_broadcast = broadcast(db, text="Збережений текст власника", media_type="video", media_key="retained-video.mp4")
    recipient(db, own_broadcast, 1, read_at=READ_EARLY)
    recipient(db, own_broadcast, 2, delivery_status="sent")
    recipient(db, own_broadcast, 4, read_at=READ_LATE, delivery_status="failed")
    recipient(db, 1, 4)
    other_recipients = db.query("SELECT * FROM broadcast_recipients WHERE user_id<>1 ORDER BY id")
    other_users = db.query("SELECT * FROM users WHERE id IN(2,4) ORDER BY id")
    audit_before = broadcast_audit(db)

    assert db.db.delete_user_account(1)["status"] == "deleted"

    assert db.query("SELECT id,is_active FROM coffee_shops ORDER BY id") == [
        {"id": 1, "is_active": False}, {"id": 2, "is_active": True}]
    assert db.query("SELECT id FROM users WHERE id=1") == []
    assert db.query("SELECT * FROM users WHERE id IN(2,4) ORDER BY id") == other_users
    assert db.query("SELECT * FROM broadcast_recipients ORDER BY id") == other_recipients
    assert broadcast_audit(db) == audit_before
    sender = db.query("""
        SELECT u.id,u.full_name,u.telegram_user_id
        FROM broadcasts b JOIN users u ON u.id=b.sender_user_id WHERE b.id=%s
    """, (own_broadcast,))[0]
    assert sender["id"] != 1 and sender["full_name"] == "Видалений користувач"
    assert sender["telegram_user_id"] is None
    assert db.query("SELECT sender_user_id FROM broadcasts WHERE id=1") == [{"sender_user_id": 2}]


def test_merge_transfers_unique_recipient_rows_without_changing_target_or_other_users(inbox_database, merge_accounts):
    db = inbox_database
    second = broadcast(db)
    source = recipient(db, 1, 2, read_at=READ_EARLY, delivery_status="failed", created_at=CREATED_EARLY)
    target = recipient(db, second, 4, read_at=READ_LATE, delivery_status="sent")
    recipient(db, 1, 3, delivery_status="sent")
    recipient(db, second, 3, read_at=READ_EARLY)
    unrelated = db.query("SELECT * FROM broadcast_recipients WHERE user_id=3 ORDER BY id")
    audit_before = broadcast_audit(db)

    assert merge_accounts(2, 4) == {"status": "merged", "user_id": 4}

    assert db.query("SELECT id FROM users WHERE id=2") == []
    assert db.query("SELECT * FROM broadcast_recipients WHERE user_id=2") == []
    assert db.query("SELECT * FROM broadcast_recipients WHERE id=%s", (source["id"],)) == [
        {**source, "user_id": 4}]
    assert db.query("SELECT * FROM broadcast_recipients WHERE id=%s", (target["id"],)) == [target]
    assert db.query("SELECT * FROM broadcast_recipients WHERE user_id=3 ORDER BY id") == unrelated
    assert broadcast_audit(db) == audit_before
    assert db.query("SELECT sender_user_id FROM broadcasts WHERE id=1") == [{"sender_user_id": 4}]
    before_repeat = db.query("SELECT * FROM broadcast_recipients ORDER BY id")
    assert merge_accounts(4, 4) == {"status": "already_merged", "user_id": 4}
    assert db.query("SELECT * FROM broadcast_recipients ORDER BY id") == before_repeat


@pytest.mark.parametrize(
    "source_read,target_read,source_delivery,target_delivery,source_created,target_created,expected_read,expected_delivery",
    [
        (None, None, None, None, CREATED_EARLY, CREATED_LATE, None, None),
        (READ_EARLY, None, "sent", None, CREATED_LATE, CREATED_EARLY, READ_EARLY, "sent"),
        (None, READ_LATE, "failed", "sent", CREATED_EARLY, CREATED_LATE, READ_LATE, "sent"),
        (READ_EARLY, READ_LATE, "sent", "failed", CREATED_LATE, CREATED_EARLY, READ_EARLY, "failed"),
        (READ_LATE, READ_EARLY, "sent", "", CREATED_EARLY, CREATED_LATE, READ_EARLY, ""),
    ],
    ids=["both-unread", "source-read-and-delivery", "target-read-and-delivery",
         "source-read-earlier-target-delivery", "target-read-earlier-non-null-empty-delivery"],
)
def test_merge_collapses_overlap_preserving_any_read_earliest_timestamps_and_target_delivery(
    inbox_database, merge_accounts, source_read, target_read, source_delivery, target_delivery,
    source_created, target_created, expected_read, expected_delivery,
):
    db = inbox_database
    recipient(db, 1, 2, read_at=source_read, delivery_status=source_delivery, created_at=source_created)
    recipient(db, 1, 4, read_at=target_read, delivery_status=target_delivery, created_at=target_created)
    unrelated = recipient(db, 1, 3, read_at=READ_LATE, delivery_status="sent")

    assert merge_accounts(2, 4) == {"status": "merged", "user_id": 4}

    rows = db.query("""
        SELECT broadcast_id,user_id,read_at,delivery_status,created_at
        FROM broadcast_recipients WHERE broadcast_id=1 AND user_id=4
    """)
    assert rows == [{"broadcast_id": 1, "user_id": 4, "read_at": expected_read,
                     "delivery_status": expected_delivery, "created_at": CREATED_EARLY}]
    assert db.query("SELECT * FROM broadcast_recipients WHERE user_id=2") == []
    assert db.query("SELECT * FROM broadcast_recipients WHERE user_id=3") == [unrelated]
    assert db.query("SELECT COUNT(*) AS count FROM broadcast_recipients") == [{"count": 2}]


def test_provider_conflict_leaves_recipients_and_account_data_unchanged(inbox_database, merge_accounts):
    db = inbox_database
    db.query("""
        INSERT INTO user_identities(user_id,provider,provider_user_id)
        VALUES(4,'google','different-target-google')
    """)
    second = broadcast(db)
    recipient(db, 1, 2, read_at=READ_EARLY, delivery_status="sent")
    recipient(db, 1, 4, delivery_status="failed", created_at=CREATED_EARLY)
    recipient(db, second, 2)
    recipient(db, second, 3, read_at=READ_LATE)
    before = snapshot(db, ("users", "user_identities", "shop_clients", "shop_admins",
                           "broadcasts", "broadcast_recipients", "transactions",
                           "touch_logs", "return_logs", "reminder_logs", "app_sessions"))

    assert merge_accounts(2, 4) == {"status": "provider_conflict", "provider": "google"}

    assert snapshot(db, before) == before


def test_merge_before_inbox_migration_retains_existing_account_behavior(deletion_database, merge_accounts):
    db = deletion_database
    assert db.query("SELECT to_regclass('broadcast_recipients') AS name") == [{"name": None}]
    other_user = db.query("SELECT * FROM users WHERE id=1")
    target_user = db.query("SELECT * FROM users WHERE id=4")

    assert merge_accounts(2, 4) == {"status": "merged", "user_id": 4}

    assert db.query("SELECT to_regclass('broadcast_recipients') AS name") == [{"name": None}]
    assert db.query("SELECT id FROM users WHERE id=2") == []
    assert db.query("SELECT * FROM users WHERE id=1") == other_user
    assert db.query("SELECT * FROM users WHERE id=4") == target_user
    assert db.query("""
        SELECT cups,free_coffee_balance FROM shop_clients WHERE shop_id=1 AND user_id=4
    """) == [{"cups": 3, "free_coffee_balance": 4}]
    assert db.query("SELECT user_id FROM user_identities WHERE provider='google' AND provider_user_id='google-client'") == [
        {"user_id": 4}]
    assert db.query("SELECT sender_user_id FROM broadcasts WHERE id=1") == [{"sender_user_id": 4}]
    assert db.query("SELECT user_id FROM reminder_logs") == [{"user_id": 4}]
    assert db.query("SELECT user_id FROM touch_logs") == [{"user_id": 4}]
    assert db.query("SELECT user_id FROM return_logs") == [{"user_id": 4}]
    assert db.query("SELECT id,user_id,admin_user_id FROM transactions ORDER BY id") == [
        {"id": 1, "user_id": 4, "admin_user_id": 1},
        {"id": 2, "user_id": 4, "admin_user_id": 4},
    ]
