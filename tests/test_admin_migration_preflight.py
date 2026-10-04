"""DDL preflight on the guarded disposable database, never production.

Exercise both exact release files, inspect their PostgreSQL locks/catalogs,
and retain the existing active/expired/revoked client rows byte-for-byte.
"""

import json
import time

import psycopg
import pytest

from conftest import PROJECT


MIGRATIONS = ("20261004_admin_invites.sql", "20261004_admin_broadcast.sql")
EXISTING_TABLES = ("users", "user_identities", "shop_admins", "app_sessions")
NEW_TABLES = ("barista_admin_invites", "barista_invite_attempts", "admin_broadcast_previews")


def apply(connection, name):
    started = time.perf_counter()
    connection.execute((PROJECT / "migrations" / name).read_text())
    return round(time.perf_counter() - started, 6)


def snapshot(database):
    return {table: database.query(f"SELECT * FROM {table} ORDER BY id")
            for table in EXISTING_TABLES}


def test_both_exact_migrations_and_rerun_preserve_existing_client_rows(database):
    before = snapshot(database)
    durations = {}
    with database.db.get_connection() as connection:
        for name in MIGRATIONS:
            durations[name] = apply(connection, name)
    assert snapshot(database) == before
    for table in NEW_TABLES:
        assert database.query(f"SELECT COUNT(*) AS n FROM {table}") == [{"n": 0}]
    with database.db.get_connection() as connection:
        for name in MIGRATIONS:
            apply(connection, name)
    assert snapshot(database) == before
    assert all(row["purpose"] == "client" and row["selected_membership_id"] is None
               for row in before["app_sessions"])
    assert database.db.get_app_session("legacy-active")["user_id"] == 1
    assert database.db.get_app_session("legacy-expired") is None
    assert database.db.get_app_session("legacy-revoked") is None
    print("LOCAL migration timings (seconds):", json.dumps(durations, sort_keys=True))


@pytest.mark.parametrize("name, referenced", [
    (MIGRATIONS[0], {"coffee_shops", "users", "shop_admins"}),
    (MIGRATIONS[1], {"coffee_shops", "users", "app_sessions"}),
])
def test_migration_referenced_table_locks_and_rollback(database, name, referenced):
    before = snapshot(database)
    # Hold the exact migration transaction open solely in this test schema to
    # inspect locks before its COMMIT; an explicit ROLLBACK exercises failure.
    source = (PROJECT / "migrations" / name).read_text()
    assert source.rstrip().endswith("COMMIT;")
    pending = source.rsplit("COMMIT;", 1)[0]
    with database.db.get_connection() as connection:
        try:
            connection.execute(pending)
            locks = connection.execute("""
                SELECT c.relname, l.mode FROM pg_locks l
                JOIN pg_class c ON c.oid=l.relation
                JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE l.pid=pg_backend_pid() AND l.granted
                  AND n.nspname=current_schema()
                ORDER BY c.relname,l.mode
            """).fetchall()
            for table in referenced:
                assert {"relname": table, "mode": "ShareRowExclusiveLock"} in locks
            print("LOCAL lock profile:", name, json.dumps(locks, sort_keys=True))
        finally:
            connection.execute("ROLLBACK")
    assert snapshot(database) == before
    assert database.query("SELECT to_regclass('barista_admin_invites') AS invites, "
                          "to_regclass('admin_broadcast_previews') AS previews") == [
                              {"invites": None, "previews": None}]


def test_new_catalog_indexes_constraints_and_foreign_key_targets(database):
    with database.db.get_connection() as connection:
        for name in MIGRATIONS:
            apply(connection, name)
    constraints = database.query("""
        SELECT c.relname AS table_name, k.conname, k.contype, k.convalidated,
               r.relname AS referenced_table, pg_get_constraintdef(k.oid) AS definition
        FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_class r ON r.oid=k.confrelid
        WHERE n.nspname=current_schema() AND c.relname=ANY(%s)
        ORDER BY c.relname,k.conname
    """, (list(NEW_TABLES),))
    indexes = database.query("""
        SELECT c.relname AS table_name, i.relname AS index_name,
               x.indisvalid, x.indisready, pg_get_indexdef(x.indexrelid) AS definition
        FROM pg_index x JOIN pg_class c ON c.oid=x.indrelid
        JOIN pg_class i ON i.oid=x.indexrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=current_schema() AND c.relname=ANY(%s)
        ORDER BY c.relname,i.relname
    """, (list(NEW_TABLES),))
    assert all(row["convalidated"] for row in constraints)
    assert all(row["indisvalid"] and row["indisready"] for row in indexes)
    assert len(indexes) == 8
    assert {(row["table_name"], row["referenced_table"])
            for row in constraints if row["contype"] == "f"} == {
                ("barista_admin_invites", "coffee_shops"),
                ("barista_admin_invites", "users"),
                ("barista_admin_invites", "shop_admins"),
                ("admin_broadcast_previews", "coffee_shops"),
                ("admin_broadcast_previews", "users"),
                ("admin_broadcast_previews", "app_sessions"),
            }
    report = {"database": "guarded local test schema only",
              "postgresql_version": database.query("SELECT version() AS version")[0]["version"],
              "constraints": constraints, "indexes": indexes}
    destination = PROJECT.parent.parent / ".build" / "AdminMigrationCatalog.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


@pytest.mark.parametrize("name", MIGRATIONS)
def test_busy_referenced_table_fails_with_lock_timeout_and_rolls_back(database, name):
    before = snapshot(database)
    with database.db.get_connection() as locker:
        with locker.transaction():
            locker.execute("LOCK TABLE users IN ROW EXCLUSIVE MODE")
            with database.db.get_connection() as migration:
                try:
                    with pytest.raises(psycopg.errors.LockNotAvailable) as error:
                        apply(migration, name)
                    assert error.value.sqlstate == "55P03"
                finally:
                    migration.execute("ROLLBACK")
    assert snapshot(database) == before
    assert database.query("SELECT to_regclass('barista_admin_invites') AS invites, "
                          "to_regclass('admin_broadcast_previews') AS previews") == [
                              {"invites": None, "previews": None}]
