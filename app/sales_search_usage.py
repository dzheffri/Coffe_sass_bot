"""Committed, global Google Places reservations backed by PostgreSQL.

The caller must reserve immediately before every outbound request. Reservations
remain counted when Google or the network fails; this module never refunds them,
creates schema, or imports application database/configuration startup code.
"""

from collections.abc import Callable
from datetime import timedelta, timezone
from math import ceil


MONTHLY_LIMIT = 950
RATE_LIMIT = 10
RATE_WINDOW_SECONDS = 60
# A fixed, database-wide two-integer advisory-lock key shared by all workers.
_LOCK_KEY = (1397969746, 1196444755)
_READ_USAGE_SQL = """
WITH moment AS (
    SELECT clock_timestamp() AS now
), bounds AS (
    SELECT now,
           date_trunc('month', now AT TIME ZONE 'Europe/Kyiv')
               AT TIME ZONE 'Europe/Kyiv' AS month_start,
           (date_trunc('month', now AT TIME ZONE 'Europe/Kyiv') + INTERVAL '1 month')
               AT TIME ZONE 'Europe/Kyiv' AS reset_at
    FROM moment
)
SELECT bounds.now, bounds.reset_at,
       to_char(bounds.now AT TIME ZONE 'Europe/Kyiv', 'YYYY-MM') AS month,
       (SELECT count(*) FROM sales_search_google_requests
        WHERE requested_at >= bounds.month_start AND requested_at < bounds.reset_at) AS used,
       recent.recent_count, recent.oldest_recent
FROM bounds
CROSS JOIN LATERAL (
    SELECT count(*) AS recent_count, min(requested_at) AS oldest_recent
    FROM sales_search_google_requests
    WHERE requested_at > bounds.now - INTERVAL '60 seconds'
) AS recent
"""


class SalesSearchUsageError(Exception):
    def __init__(self, code: str, status_code: int, message: str,
                 usage: dict | None = None, retry_after: int | None = None):
        self.code = code
        self.status_code = status_code
        self.message = message
        self.usage = usage
        self.retry_after = retry_after
        super().__init__(message)


class PostgresSalesSearchUsage:
    """Use fresh psycopg dict-row connections supplied by application wiring."""

    def __init__(self, connection_factory: Callable):
        self.connection_factory = connection_factory

    @staticmethod
    def _read(cur):
        cur.execute(_READ_USAGE_SQL)
        row = cur.fetchone()
        usage = {
            "used": int(row["used"]),
            "limit": MONTHLY_LIMIT,
            "month": row["month"],
            "reset_at": row["reset_at"].astimezone(timezone.utc).isoformat(),
        }
        return row, usage

    @staticmethod
    def _unavailable():
        return SalesSearchUsageError(
            "GOOGLE_PLACES_USAGE_UNAVAILABLE", 503,
            "Облік запитів Google Places тимчасово недоступний. Повторіть спробу пізніше.",
        )

    def snapshot(self) -> dict:
        try:
            with self.connection_factory() as conn:
                with conn.cursor() as cur:
                    _, usage = self._read(cur)
            return usage
        except SalesSearchUsageError:
            raise
        except Exception as exc:
            raise self._unavailable() from exc

    def reserve(self) -> dict:
        try:
            with self.connection_factory() as conn:
                with conn.transaction():
                    with conn.cursor() as cur:
                        # The count must see a previous holder's commit even if
                        # the database default uses repeatable-read snapshots.
                        cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", _LOCK_KEY)
                        # Read server time after waiting for the lock: transaction
                        # start time could belong to an earlier minute or month.
                        row, usage = self._read(cur)
                        if usage["used"] >= MONTHLY_LIMIT:
                            retry_after = max(1, ceil((row["reset_at"] - row["now"]).total_seconds()))
                            raise SalesSearchUsageError(
                                "MONTHLY_GOOGLE_PLACES_LIMIT_REACHED", 429,
                                "Місячний ліміт запитів Google Places вичерпано.",
                                usage, retry_after,
                            )
                        if row["recent_count"] >= RATE_LIMIT:
                            retry_at = row["oldest_recent"] + timedelta(seconds=RATE_WINDOW_SECONDS)
                            retry_after = max(1, ceil((retry_at - row["now"]).total_seconds()))
                            raise SalesSearchUsageError(
                                "GOOGLE_PLACES_RATE_LIMIT_REACHED", 429,
                                "Забагато запитів Google Places. Повторіть спробу пізніше.",
                                usage, retry_after,
                            )
                        cur.execute(
                            "INSERT INTO sales_search_google_requests(requested_at) VALUES (%s)",
                            (row["now"],),
                        )
                        usage["used"] += 1
            # Leaving both contexts commits before the caller can contact Google.
            return usage
        except SalesSearchUsageError:
            raise
        except Exception as exc:
            # Missing migration, insert failures, and commit failures all prevent
            # the caller from obtaining a reservation and therefore fail closed.
            raise self._unavailable() from exc
