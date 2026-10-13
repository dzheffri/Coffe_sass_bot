"""Inbox DDL on guarded, disposable local PostgreSQL; no delivery or startup."""

from datetime import datetime, timezone
import uuid

import psycopg
from psycopg import sql
from psycopg.errors import ForeignKeyViolation, NotNullViolation, UniqueViolation
import pytest

from conftest import PROJECT, extract_definitions


MIGRATION = PROJECT / "migrations/20261013_broadcast_recipients.sql"
LEGACY_SCHEMA = """
CREATE TABLE users (id BIGSERIAL PRIMARY KEY, telegram_user_id BIGINT UNIQUE);
CREATE TABLE coffee_shops (
    id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, is_active BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE TABLE broadcasts (
    id BIGSERIAL PRIMARY KEY,
    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
    sender_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    text TEXT NOT NULL,
    recipients_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_broadcasts_shop_created ON broadcasts(shop_id, created_at);
INSERT INTO users(telegram_user_id) VALUES(1001),(1002),(1003);
INSERT INTO coffee_shops(name) VALUES('Кав’ярня'),('Інша кав’ярня');
INSERT INTO broadcasts(shop_id,sender_user_id,text,recipients_count)
    VALUES(1,1,'Стара розсилка',2);
"""


class InboxDatabase:
    def __init__(self, connect):
        self.connect = connect
        self.legacy = []

    def query(self, statement, values=()):
        with self.connect() as connection:
            cursor = connection.execute(statement, values)
            return cursor.fetchall() if cursor.description else []

    def recipient(self, broadcast_id=1, user_id=2):
        return self.query("""INSERT INTO broadcast_recipients(broadcast_id,user_id)
            VALUES(%s,%s) RETURNING *""", (broadcast_id, user_id))[0]


@pytest.fixture
def inbox_database(local_connection):
    dsn, state, connect = local_connection
    schema = "broadcast_inbox_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    state["schema"] = schema
    db = InboxDatabase(connect)
    try:
        db.query(LEGACY_SCHEMA)
        db.legacy = db.query("SELECT * FROM broadcasts ORDER BY id")
        db.query(MIGRATION.read_text(encoding="utf-8"))
        yield db
    finally:
        state["schema"] = None
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def test_legacy_broadcasts_retained_without_backfilled_recipients(inbox_database):
    db = inbox_database
    rows = db.query("SELECT * FROM broadcasts ORDER BY id")
    assert rows == [{**row, "media_type": None, "media_key": None} for row in db.legacy]
    assert db.query("SELECT * FROM broadcast_recipients") == []


def test_existing_broadcast_writer_and_reader_work_with_nullable_media(inbox_database):
    db = inbox_database
    namespace = {"get_connection": db.connect}
    extract_definitions(PROJECT / "app/db.py", {
        "get_user_by_telegram_id", "save_broadcast", "get_all_broadcasts",
    }, namespace)
    broadcast = namespace["save_broadcast"](1, 1001, "Нова текстова розсилка", 2)
    assert broadcast["text"] == "Нова текстова розсилка"
    assert broadcast["media_type"] is None and broadcast["media_key"] is None
    assert namespace["get_all_broadcasts"]()[-1] == broadcast


@pytest.mark.parametrize("media_type", [None, "photo", "video"])
def test_nullable_media_fields_accept_future_storage_metadata(inbox_database, media_type):
    key = None if media_type is None else "future-storage-key"
    row = inbox_database.query("""INSERT INTO broadcasts
        (shop_id,sender_user_id,text,media_type,media_key)
        VALUES(1,1,'Текст',%s,%s) RETURNING media_type,media_key""", (media_type, key))[0]
    assert row == {"media_type": media_type, "media_key": key}


def test_recipient_defaults_and_read_timestamp(inbox_database):
    db = inbox_database
    recipient = db.recipient()
    assert recipient["id"] > 0 and recipient["broadcast_id"] == 1 and recipient["user_id"] == 2
    assert recipient["read_at"] is None and recipient["delivery_status"] is None
    assert isinstance(recipient["created_at"], datetime)
    read_at = datetime(2026, 10, 13, 9, 0, tzinfo=timezone.utc)
    db.query("UPDATE broadcast_recipients SET read_at=%s WHERE id=%s", (read_at, recipient["id"]))
    assert db.query("SELECT read_at FROM broadcast_recipients WHERE id=%s", (recipient["id"],)) == [
        {"read_at": read_at}]


def test_duplicate_recipient_pair_rejected(inbox_database):
    db = inbox_database
    db.recipient()
    with pytest.raises(UniqueViolation):
        db.recipient()
    assert len(db.query("SELECT * FROM broadcast_recipients")) == 1


def test_many_users_and_many_broadcasts(inbox_database):
    db = inbox_database
    broadcast = db.query("""INSERT INTO broadcasts(shop_id,sender_user_id,text)
        VALUES(2,1,'Інша розсилка') RETURNING id""")[0]["id"]
    db.recipient(1, 2)
    db.recipient(1, 3)
    db.recipient(broadcast, 2)
    assert db.query("SELECT broadcast_id,user_id FROM broadcast_recipients ORDER BY broadcast_id,user_id") == [
        {"broadcast_id": 1, "user_id": 2}, {"broadcast_id": 1, "user_id": 3},
        {"broadcast_id": broadcast, "user_id": 2}]


@pytest.mark.parametrize("broadcast_id,user_id", [(999, 2), (1, 999)])
def test_foreign_keys_reject_orphans(inbox_database, broadcast_id, user_id):
    with pytest.raises(ForeignKeyViolation):
        inbox_database.recipient(broadcast_id, user_id)


@pytest.mark.parametrize("broadcast_id,user_id", [(None, 2), (1, None)])
def test_foreign_keys_are_required(inbox_database, broadcast_id, user_id):
    with pytest.raises(NotNullViolation):
        inbox_database.recipient(broadcast_id, user_id)


def test_user_delete_only_removes_own_inbox(inbox_database):
    db = inbox_database
    db.recipient(1, 2)
    other = db.recipient(1, 3)
    before = db.query("SELECT * FROM broadcasts")
    db.query("DELETE FROM users WHERE id=2")
    assert db.query("SELECT * FROM broadcast_recipients") == [other]
    assert db.query("SELECT * FROM broadcasts") == before
    assert db.query("SELECT id FROM users ORDER BY id") == [{"id": 1}, {"id": 3}]


def test_broadcast_delete_removes_dependents_not_users(inbox_database):
    db = inbox_database
    db.recipient()
    users = db.query("SELECT * FROM users ORDER BY id")
    db.query("DELETE FROM broadcasts WHERE id=1")
    assert db.query("SELECT * FROM broadcast_recipients") == []
    assert db.query("SELECT * FROM users ORDER BY id") == users


def test_soft_shop_close_retains_broadcasts_and_recipients(inbox_database):
    db = inbox_database
    recipient = db.recipient()
    before = db.query("SELECT * FROM broadcasts")
    db.query("UPDATE coffee_shops SET is_active=FALSE WHERE id=1")
    assert db.query("SELECT * FROM broadcasts") == before
    assert db.query("SELECT * FROM broadcast_recipients") == [recipient]


def test_physical_shop_delete_leaves_no_orphans_or_cross_shop_damage(inbox_database):
    db = inbox_database
    db.recipient()
    other_broadcast = db.query("""INSERT INTO broadcasts(shop_id,sender_user_id,text)
        VALUES(2,1,'Інша розсилка') RETURNING *""")[0]
    other_recipient = db.recipient(other_broadcast["id"], 2)
    users = db.query("SELECT * FROM users ORDER BY id")
    db.query("DELETE FROM coffee_shops WHERE id=1")
    assert db.query("SELECT * FROM broadcasts") == [other_broadcast]
    assert db.query("SELECT * FROM broadcast_recipients") == [other_recipient]
    assert db.query("SELECT * FROM users ORDER BY id") == users


def test_migration_repeats_without_modifying_data_or_constraints(inbox_database):
    db = inbox_database
    db.recipient()
    broadcasts = db.query("SELECT * FROM broadcasts")
    recipients = db.query("SELECT * FROM broadcast_recipients")
    db.query(MIGRATION.read_text(encoding="utf-8"))
    db.query(MIGRATION.read_text(encoding="utf-8"))
    assert db.query("SELECT * FROM broadcasts") == broadcasts
    assert db.query("SELECT * FROM broadcast_recipients") == recipients
    with pytest.raises(UniqueViolation):
        db.recipient()
    with pytest.raises(ForeignKeyViolation):
        db.recipient(999, 2)
    columns = db.query("""SELECT column_name FROM information_schema.columns
        WHERE table_schema=current_schema() AND table_name='broadcast_recipients'
        ORDER BY ordinal_position""")
    assert [row["column_name"] for row in columns] == [
        "id", "broadcast_id", "user_id", "read_at", "delivery_status", "created_at"]
