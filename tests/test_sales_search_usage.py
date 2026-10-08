"""Usage reservations against guarded, disposable local PostgreSQL only.

No application DB/config startup or Google request is imported or invoked.
Each test owns a temporary schema through the existing local-connection guard.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier
import uuid
from zoneinfo import ZoneInfo

import psycopg
from psycopg import sql
import pytest

from conftest import PROJECT
from app import sales_search_usage
from app.sales_search_usage import PostgresSalesSearchUsage, SalesSearchUsageError


MIGRATION = PROJECT / "migrations/20261007_sales_search_usage.sql"
KYIV = ZoneInfo("Europe/Kyiv")


class UsageDatabase:
    def __init__(self, connect):
        self.connect = connect

    def query(self, statement, values=()):
        with self.connect() as connection:
            cursor = connection.execute(statement, values)
            return cursor.fetchall() if cursor.description else []

    def seed(self, requested_at, count):
        self.query(
            "INSERT INTO sales_search_google_requests(requested_at) "
            "SELECT %s::timestamptz FROM generate_series(1, %s)",
            (requested_at, count),
        )

    def count(self):
        return self.query("SELECT count(*) AS n FROM sales_search_google_requests")[0]["n"]


@pytest.fixture
def usage_database(local_connection):
    dsn, state, connect = local_connection
    schema = "sales_search_usage_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    state["schema"] = schema
    database = UsageDatabase(connect)
    try:
        database.query("CREATE TABLE existing_control(id integer PRIMARY KEY, value text)")
        database.query("INSERT INTO existing_control VALUES (1, 'preserved')")
        database.query(MIGRATION.read_text(encoding="utf-8"))
        yield database
    finally:
        state["schema"] = None
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def freeze_clock(monkeypatch):
    """Keep all production SQL/transactions, replacing only the clock expression."""
    original = sales_search_usage._READ_USAGE_SQL

    def freeze(value):
        moment = datetime.fromisoformat(value)
        assert moment.tzinfo is not None
        monkeypatch.setattr(
            sales_search_usage, "_READ_USAGE_SQL",
            original.replace("clock_timestamp()", f"'{moment.isoformat()}'::timestamptz"),
        )
        return moment

    return freeze


def test_migration_is_idempotent_and_preserves_existing_table(usage_database):
    db = usage_database
    db.query(MIGRATION.read_text(encoding="utf-8"))
    assert db.query("SELECT * FROM existing_control") == [{"id": 1, "value": "preserved"}]
    columns = db.query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema=current_schema() AND table_name='sales_search_google_requests' "
        "ORDER BY ordinal_position"
    )
    assert columns == [
        {"column_name": "id", "data_type": "bigint"},
        {"column_name": "requested_at", "data_type": "timestamp with time zone"},
    ]
    indexes = db.query(
        "SELECT indexname FROM pg_indexes WHERE schemaname=current_schema() "
        "AND tablename='sales_search_google_requests'"
    )
    assert {row["indexname"] for row in indexes} == {
        "sales_search_google_requests_pkey", "sales_search_google_requests_requested_at_idx",
    }


def test_snapshot_uses_server_time_and_reservation_is_committed(usage_database):
    db = usage_database
    guard = PostgresSalesSearchUsage(db.connect)
    before = db.query("SELECT clock_timestamp() AS now")[0]["now"]
    usage = guard.snapshot()
    assert usage["used"] == 0 and usage["limit"] == 950
    assert usage["month"] == before.astimezone(KYIV).strftime("%Y-%m")
    local = before.astimezone(KYIV)
    reset = datetime(local.year + (local.month == 12), local.month % 12 + 1, 1, tzinfo=KYIV)
    assert datetime.fromisoformat(usage["reset_at"]) == reset
    assert db.count() == 0
    assert guard.reserve() == {**usage, "used": 1}
    # A separate connection can observe the row before reserve() returns.
    requested_at = db.query("SELECT requested_at FROM sales_search_google_requests")[0]["requested_at"]
    after = db.query("SELECT clock_timestamp() AS now")[0]["now"]
    assert before <= requested_at <= after
    assert guard.snapshot() == {**usage, "used": 1}


def test_monthly_949_to_950_then_blocks(usage_database, freeze_clock):
    now = freeze_clock("2026-10-07T12:00:00+00:00")
    db = usage_database
    db.seed(now - timedelta(minutes=2), 949)
    guard = PostgresSalesSearchUsage(db.connect)
    assert guard.snapshot()["used"] == 949
    usage = guard.reserve()
    assert usage["used"] == 950
    with pytest.raises(SalesSearchUsageError) as error:
        guard.reserve()
    assert error.value.code == "MONTHLY_GOOGLE_PLACES_LIMIT_REACHED"
    assert error.value.status_code == 429
    assert error.value.usage == usage
    assert error.value.retry_after > 0
    assert db.count() == 950


def test_rolling_window_allows_ten_and_expires_after_sixty_seconds(usage_database, freeze_clock):
    now = freeze_clock("2026-10-07T12:00:00+00:00")
    db = usage_database
    guard = PostgresSalesSearchUsage(db.connect)
    for used in range(1, 11):
        assert guard.reserve()["used"] == used
    with pytest.raises(SalesSearchUsageError) as error:
        guard.reserve()
    assert error.value.code == "GOOGLE_PLACES_RATE_LIMIT_REACHED"
    assert error.value.status_code == 429
    assert error.value.usage["used"] == 10
    assert error.value.retry_after == 60
    assert db.count() == 10
    freeze_clock((now + timedelta(seconds=60)).isoformat())
    assert guard.reserve()["used"] == 11


def test_rate_window_does_not_reset_with_calendar_month(usage_database, freeze_clock):
    now = freeze_clock("2026-09-30T21:00:00+00:00")
    db = usage_database
    db.seed(now - timedelta(seconds=1), 10)
    guard = PostgresSalesSearchUsage(db.connect)
    assert guard.snapshot()["used"] == 0
    with pytest.raises(SalesSearchUsageError) as error:
        guard.reserve()
    assert error.value.code == "GOOGLE_PLACES_RATE_LIMIT_REACHED"
    assert error.value.usage["month"] == "2026-10"
    assert error.value.usage["used"] == 0
    assert error.value.retry_after == 59
    assert db.count() == 10


@pytest.mark.parametrize("moment,month,month_start,reset_at", [
    ("2026-09-30T20:59:59+00:00", "2026-09", "2026-08-31T21:00:00+00:00", "2026-09-30T21:00:00+00:00"),
    ("2026-09-30T21:00:00+00:00", "2026-10", "2026-09-30T21:00:00+00:00", "2026-10-31T22:00:00+00:00"),
    ("2026-03-31T21:00:00+00:00", "2026-04", "2026-03-31T21:00:00+00:00", "2026-04-30T21:00:00+00:00"),
    ("2026-12-31T22:00:00+00:00", "2027-01", "2026-12-31T22:00:00+00:00", "2027-01-31T22:00:00+00:00"),
])
def test_month_boundaries_are_kyiv_calendar_and_include_dst(
        usage_database, freeze_clock, moment, month, month_start, reset_at):
    freeze_clock(moment)
    db = usage_database
    start = datetime.fromisoformat(month_start)
    end = datetime.fromisoformat(reset_at)
    db.seed(start - timedelta(microseconds=1), 3)
    db.seed(start, 2)
    db.seed(end, 5)
    usage = PostgresSalesSearchUsage(db.connect).snapshot()
    assert usage == {"used": 2, "limit": 950, "month": month, "reset_at": end.isoformat()}


def test_monthly_limit_resets_at_kyiv_midnight(usage_database, freeze_clock):
    freeze_clock("2026-09-30T20:59:59+00:00")
    db = usage_database
    db.seed(datetime(2026, 9, 10, tzinfo=timezone.utc), 950)
    guard = PostgresSalesSearchUsage(db.connect)
    with pytest.raises(SalesSearchUsageError) as error:
        guard.reserve()
    assert error.value.code == "MONTHLY_GOOGLE_PLACES_LIMIT_REACHED"
    freeze_clock("2026-09-30T21:00:00+00:00")
    assert guard.snapshot()["used"] == 0
    assert guard.reserve()["used"] == 1
    assert db.count() == 951


def _race_reservations(db, count):
    barrier = Barrier(count)

    def reserve():
        # Each worker owns a separate guard and separate PostgreSQL connection.
        guard = PostgresSalesSearchUsage(db.connect)
        barrier.wait(timeout=10)
        try:
            return guard.reserve()
        except SalesSearchUsageError as error:
            return error

    with ThreadPoolExecutor(max_workers=count) as pool:
        return list(pool.map(lambda _: reserve(), range(count)))


def test_concurrent_workers_cannot_cross_monthly_cap(usage_database, freeze_clock):
    now = freeze_clock("2026-10-07T12:00:00+00:00")
    db = usage_database
    db.seed(now - timedelta(minutes=2), 949)
    results = _race_reservations(db, 8)
    successes = [result for result in results if isinstance(result, dict)]
    errors = [result for result in results if isinstance(result, SalesSearchUsageError)]
    assert len(successes) == 1
    assert successes[0]["used"] == 950
    assert len(errors) == 7
    assert all(error.code == "MONTHLY_GOOGLE_PLACES_LIMIT_REACHED" for error in errors)
    assert db.count() == 950


def test_concurrent_workers_share_rolling_rate_limit(usage_database, freeze_clock):
    freeze_clock("2026-10-07T12:00:00+00:00")
    db = usage_database
    results = _race_reservations(db, 20)
    successes = [result for result in results if isinstance(result, dict)]
    errors = [result for result in results if isinstance(result, SalesSearchUsageError)]
    assert sorted(result["used"] for result in successes) == list(range(1, 11))
    assert len(errors) == 10
    assert all(error.code == "GOOGLE_PLACES_RATE_LIMIT_REACHED" for error in errors)
    assert db.count() == 10


@pytest.mark.parametrize("operation", ["snapshot", "reserve"])
def test_missing_table_fails_closed(usage_database, operation):
    db = usage_database
    db.query("DROP TABLE sales_search_google_requests")
    with pytest.raises(SalesSearchUsageError) as error:
        getattr(PostgresSalesSearchUsage(db.connect), operation)()
    assert error.value.code == "GOOGLE_PLACES_USAGE_UNAVAILABLE"
    assert error.value.status_code == 503
    assert error.value.usage is None
    assert error.value.retry_after is None


@pytest.mark.parametrize("operation", ["snapshot", "reserve"])
def test_connection_failure_fails_closed_without_exposing_details(operation):
    def unavailable():
        raise psycopg.OperationalError("private database credentials")

    with pytest.raises(SalesSearchUsageError) as error:
        getattr(PostgresSalesSearchUsage(unavailable), operation)()
    assert error.value.code == "GOOGLE_PLACES_USAGE_UNAVAILABLE"
    assert error.value.status_code == 503
    assert "private database credentials" not in error.value.message


def test_insert_failure_rolls_back_and_fails_closed(usage_database, freeze_clock):
    freeze_clock("2026-10-07T12:00:00+00:00")
    db = usage_database
    db.query("ALTER TABLE sales_search_google_requests ADD CONSTRAINT refuse_request CHECK (FALSE)")
    with pytest.raises(SalesSearchUsageError) as error:
        PostgresSalesSearchUsage(db.connect).reserve()
    assert error.value.code == "GOOGLE_PLACES_USAGE_UNAVAILABLE"
    assert error.value.status_code == 503
    assert db.count() == 0


def test_commit_failure_rolls_back_and_fails_closed(usage_database, freeze_clock):
    freeze_clock("2026-10-07T12:00:00+00:00")
    db = usage_database
    db.query("""
        CREATE FUNCTION refuse_usage_commit() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'local test commit failure'; END;
        $$;
        CREATE CONSTRAINT TRIGGER refuse_usage_commit
        AFTER INSERT ON sales_search_google_requests
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION refuse_usage_commit();
    """)
    with pytest.raises(SalesSearchUsageError) as error:
        PostgresSalesSearchUsage(db.connect).reserve()
    assert error.value.code == "GOOGLE_PLACES_USAGE_UNAVAILABLE"
    assert error.value.status_code == 503
    assert db.count() == 0
