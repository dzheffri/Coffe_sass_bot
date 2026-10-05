"""Stage 2 HTTP/loyalty tests with real, guarded local PostgreSQL transactions.

Delivery is mocked; actual QR/balance/loyalty SQL and session authorization run
against a disposable schema. No production credentials or external requests.
"""

import importlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock

import pytest
from psycopg.errors import CheckViolation

from conftest import bearer


OPERATION_SCHEMA = """
CREATE TABLE shop_clients (
    id BIGSERIAL PRIMARY KEY,
    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    cups INTEGER NOT NULL DEFAULT 0,
    free_coffee_balance INTEGER NOT NULL DEFAULT 0,
    total_scans INTEGER NOT NULL DEFAULT 0,
    total_free_coffee_earned INTEGER NOT NULL DEFAULT 0,
    total_free_coffee_redeemed INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(shop_id,user_id)
);
CREATE TABLE transactions (
    id BIGSERIAL PRIMARY KEY,
    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    admin_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    type TEXT NOT NULL CHECK(type IN ('add_cups','redeem_free')),
    cups_added INTEGER NOT NULL DEFAULT 0,
    free_redeemed INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE subscriptions (
    id BIGSERIAL PRIMARY KEY,
    shop_id BIGINT UNIQUE NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
    plan TEXT NOT NULL CHECK(plan IN ('trial','basic','pro')),
    status TEXT NOT NULL CHECK(status IN ('active','expired','blocked')),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE touch_logs (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
    type TEXT NOT NULL CHECK(type IN ('auto','broadcast','service')),
    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE return_logs (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
    touch_log_id BIGINT NOT NULL REFERENCES touch_logs(id) ON DELETE CASCADE,
    touch_type TEXT NOT NULL CHECK(touch_type IN ('auto','broadcast')),
    returned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(shop_id,user_id,touch_log_id)
);
INSERT INTO subscriptions(shop_id,plan,status,expires_at)
VALUES (1,'basic','active',NOW()+INTERVAL '30 days'),
       (2,'basic','active',NOW()+INTERVAL '30 days');
"""

PATHS = ("/barista/scan", "/barista/add-cup", "/barista/redeem")
BODY = {"qr_token": "coffee:customer-qr"}


@pytest.fixture
def operations_database(database):
    database.query(OPERATION_SCHEMA)
    return database


@pytest.fixture
def operations_api(operations_database, api, monkeypatch):
    router = importlib.import_module("app.api.barista")
    added, redeemed = AsyncMock(), AsyncMock()
    monkeypatch.setattr(router, "notify_cups_added", added)
    monkeypatch.setattr(router, "notify_free_redeemed", redeemed)
    client, _ = api
    return client, added, redeemed


def seed_balance(database, cups=0, free=0, *, shop_id=1):
    database.query(
        """INSERT INTO shop_clients(shop_id,user_id,cups,free_coffee_balance)
           VALUES (%s,2,%s,%s) ON CONFLICT(shop_id,user_id)
           DO UPDATE SET cups=EXCLUDED.cups,free_coffee_balance=EXCLUDED.free_coffee_balance""",
        (shop_id, cups, free),
    )


def balance(database, shop_id=1):
    rows = database.query("SELECT * FROM shop_clients WHERE shop_id=%s AND user_id=2", (shop_id,))
    return rows[0] if rows else None


def ledger(database):
    return {table: database.query(f"SELECT * FROM {table} ORDER BY id")
            for table in ("shop_clients", "transactions", "touch_logs", "return_logs")}


def post(client, token, path="/barista/add-cup", body=None):
    return client.post(path, headers=bearer(token), json=BODY if body is None else body)


@pytest.mark.parametrize("qr", ["coffee:customer-qr", "customer-qr"])
def test_scan_returns_only_safe_current_shop_balance_and_never_writes(operations_api, operations_database, qr):
    client, added, redeemed = operations_api
    database = operations_database
    seed_balance(database, cups=5, free=1)
    seed_balance(database, cups=2, free=9, shop_id=2)
    token = database.staff_token()
    session_before, before = database.session(token), ledger(database)
    response = post(client, token, "/barista/scan", {"qr_token": qr})
    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "client": {"name": "Іван", "avatar": None, "cups": 5,
                   "free_coffee_balance": 1, "loyalty_target": 7, "progress": 5 / 7},
        "shop": {"id": 1, "name": "Наші"},
    }
    assert database.session(token) == session_before
    assert ledger(database) == before
    added.assert_not_awaited()
    redeemed.assert_not_awaited()


def test_first_scan_returns_zero_without_creating_a_shop_client(operations_api, operations_database):
    client, _, _ = operations_api
    database = operations_database
    token = database.staff_token()
    before = database.session(token)
    result = post(client, token, "/barista/scan")
    assert result.status_code == 200
    assert result.json()["client"]["cups"] == 0
    assert result.json()["client"]["free_coffee_balance"] == 0
    assert balance(database) is None
    assert database.session(token) == before


@pytest.mark.parametrize("path", PATHS)
def test_unknown_qr_is_not_found_without_mutation(operations_api, operations_database, path):
    client, added, redeemed = operations_api
    database = operations_database
    token = database.staff_token()
    before, session_before = ledger(database), database.session(token)
    response = post(client, token, path, {"qr_token": "coffee:unknown-qr"})
    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "CLIENT_NOT_FOUND"
    assert ledger(database) == before
    assert database.session(token) == session_before
    added.assert_not_awaited()
    redeemed.assert_not_awaited()


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("extra", [{"shop_id": 2}, {"user_id": 3}, {"admin_user_id": 3},
                                   {"role": "owner"}, {"count": 20}])
def test_body_cannot_override_authorization_or_quantity(operations_api, operations_database, path, extra):
    client, _, _ = operations_api
    database = operations_database
    token = database.staff_token()
    before = database.session(token)
    response = post(client, token, path, {**BODY, **extra})
    assert response.status_code == 422
    assert ledger(database)["shop_clients"] == []
    assert database.session(token) == before


@pytest.mark.parametrize("qr", ["", "coffee:", "   ", "coffee:bad token", "coffee:bad\nvalue"])
def test_invalid_qr_is_rejected(operations_api, operations_database, qr):
    client, _, _ = operations_api
    response = post(client, operations_database.staff_token(), "/barista/scan", {"qr_token": qr})
    assert response.status_code in (400, 422)


@pytest.mark.parametrize("path", PATHS)
def test_client_token_and_missing_auth_cannot_enter_operations(operations_api, operations_database, path):
    client, _, _ = operations_api
    before = operations_database.session("legacy-active")
    assert post(client, "legacy-active", path).status_code == 401
    assert client.post(path, json=BODY).status_code == 401
    assert operations_database.session("legacy-active") == before


@pytest.mark.parametrize("path", PATHS)
def test_deleted_membership_denies_even_when_another_shop_membership_exists(operations_api, operations_database, path):
    client, _, _ = operations_api
    database = operations_database
    database.add_second_membership()
    token = database.staff_token()
    database.query("DELETE FROM shop_admins WHERE id=1")
    assert post(client, token, path).status_code == 401
    assert ledger(database)["transactions"] == []


@pytest.mark.parametrize("path", PATHS)
def test_downgraded_membership_is_rechecked_without_extending_session(operations_api, operations_database, path):
    client, _, _ = operations_api
    database = operations_database
    token = database.staff_token()
    database.query("UPDATE shop_admins SET role='inactive' WHERE id=1")
    before = database.session(token)
    response = post(client, token, path)
    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "STAFF_ACCESS_REQUIRED"
    assert database.session(token) == before
    assert ledger(database)["transactions"] == []


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("dead", ["expired", "revoked"])
def test_expired_or_revoked_session_never_revives(operations_api, operations_database, path, dead):
    client, _, _ = operations_api
    database = operations_database
    token = database.staff_token()
    update = "expires_at=clock_timestamp()-INTERVAL '1 second'" if dead == "expired" else "revoked_at=clock_timestamp()"
    database.query(f"UPDATE app_sessions SET {update} WHERE token_hash=%s",
                   (database.db._hash_barista_session_token(token),))
    before = database.session(token)
    assert post(client, token, path).status_code == 401
    assert database.session(token) == before


@pytest.mark.parametrize("path", ["/barista/add-cup", "/barista/redeem"])
def test_inactive_subscription_rejects_without_balance_or_session_writes(operations_api, operations_database, path):
    client, _, _ = operations_api
    database = operations_database
    seed_balance(database, cups=4, free=1)
    token = database.staff_token()
    database.query("UPDATE subscriptions SET status='expired' WHERE shop_id=1")
    before, session_before = ledger(database), database.session(token)
    response = post(client, token, path)
    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "SHOP_SUBSCRIPTION_INACTIVE"
    assert ledger(database) == before
    assert database.session(token) == session_before


@pytest.mark.parametrize("existing", [False, True])
def test_add_defaults_to_one_and_records_server_actor(operations_api, operations_database, existing):
    client, added, redeemed = operations_api
    database = operations_database
    if existing:
        seed_balance(database, cups=2, free=1)
    token = database.staff_token()
    sessions_before = database.query("SELECT COUNT(*) AS n FROM app_sessions")[0]["n"]
    response = post(client, token)
    assert response.status_code == 200
    assert response.json()["operation"] == {"type": "add_cup", "cups_added": 1, "free_coffee_earned": 0}
    current = balance(database)
    assert current["cups"] == (3 if existing else 1)
    assert current["total_scans"] == 1
    assert current["free_coffee_balance"] == (1 if existing else 0)
    transaction = ledger(database)["transactions"][0]
    assert (transaction["shop_id"], transaction["user_id"], transaction["admin_user_id"],
            transaction["type"], transaction["cups_added"], transaction["free_redeemed"]) == (1, 2, 1, "add_cups", 1, 0)
    assert len(ledger(database)["touch_logs"]) == 1
    assert database.query("SELECT COUNT(*) AS n FROM app_sessions")[0]["n"] == sessions_before
    added.assert_awaited_once()
    redeemed.assert_not_awaited()


@pytest.mark.parametrize("cups,count,expected_cups,earned_free", [
    (0, 1, 1, 0),
    (0, 2, 2, 0),
    (0, 7, 0, 1),
    (6, 2, 1, 1),
    (6, 7, 6, 1),
    (6, 10, 2, 2),
])
def test_add_validated_count_reuses_loyalty_cycles_and_records_one_operation(
    operations_api, operations_database, cups, count, expected_cups, earned_free,
):
    client, added, redeemed = operations_api
    database = operations_database
    seed_balance(database, cups=cups, free=1)
    response = post(client, database.staff_token(), body={**BODY, "count": count})
    assert response.status_code == 200
    assert response.json()["operation"] == {
        "type": "add_cup", "cups_added": count, "free_coffee_earned": earned_free,
    }
    assert response.json()["client"]["cups"] == expected_cups
    assert response.json()["client"]["progress"] == expected_cups / 7
    assert response.json()["client"]["free_coffee_balance"] == 1 + earned_free
    current = balance(database)
    assert current["cups"] == expected_cups
    assert current["free_coffee_balance"] == 1 + earned_free
    assert current["total_scans"] == count
    assert current["total_free_coffee_earned"] == earned_free
    transactions = ledger(database)["transactions"]
    assert len(transactions) == 1
    assert (transactions[0]["cups_added"], transactions[0]["admin_user_id"],
            transactions[0]["shop_id"], transactions[0]["user_id"]) == (count, 1, 1, 2)
    assert len(ledger(database)["touch_logs"]) == 1
    added.assert_awaited_once()
    assert added.await_args.kwargs["count"] == count
    assert added.await_args.kwargs["earned_free"] == earned_free
    redeemed.assert_not_awaited()


@pytest.mark.parametrize("count", [0, -1, 11, 2.0, 2.5, "2", True, False, None])
def test_invalid_count_rejected_before_session_or_loyalty_changes(
    operations_api, operations_database, count,
):
    client, added, redeemed = operations_api
    database = operations_database
    seed_balance(database, cups=6, free=1)
    token = database.staff_token()
    before, session_before = ledger(database), database.session(token)
    response = post(client, token, body={**BODY, "count": count})
    assert response.status_code == 422
    assert ledger(database) == before
    assert database.session(token) == session_before
    added.assert_not_awaited()
    redeemed.assert_not_awaited()


@pytest.mark.parametrize("path", ["/barista/scan", "/barista/redeem"])
def test_count_is_not_accepted_for_scan_or_redeem(operations_api, operations_database, path):
    client, _, _ = operations_api
    database = operations_database
    seed_balance(database, cups=6, free=1)
    token = database.staff_token()
    before, session_before = ledger(database), database.session(token)
    assert post(client, token, path, {**BODY, "count": 1}).status_code == 422
    assert ledger(database) == before
    assert database.session(token) == session_before


def test_seventh_purchase_preserves_existing_loyalty_rule(operations_api, operations_database):
    client, added, _ = operations_api
    database = operations_database
    seed_balance(database, cups=6, free=1)
    result = post(client, database.staff_token())
    assert result.status_code == 200
    assert result.json()["client"]["cups"] == 0
    assert result.json()["client"]["progress"] == 0
    assert result.json()["client"]["free_coffee_balance"] == 2
    assert result.json()["operation"]["free_coffee_earned"] == 1
    assert balance(database)["total_free_coffee_earned"] == 1
    added.assert_awaited_once()


def test_redeem_last_gift_then_zero_never_goes_negative(operations_api, operations_database):
    client, added, redeemed = operations_api
    database = operations_database
    seed_balance(database, cups=4, free=1)
    token = database.staff_token()
    first = post(client, token, "/barista/redeem")
    assert first.status_code == 200
    assert first.json()["operation"] == {"type": "redeem", "free_redeemed": 1}
    assert first.json()["client"]["free_coffee_balance"] == 0
    before, session_before = ledger(database), database.session(token)
    second = post(client, token, "/barista/redeem")
    assert second.status_code == 409
    assert second.json()["detail"]["code"] == "NO_FREE_COFFEE"
    assert balance(database)["cups"] == 4
    assert balance(database)["free_coffee_balance"] == 0
    assert balance(database)["total_free_coffee_redeemed"] == 1
    assert ledger(database) == before
    assert database.session(token) == session_before
    redeemed.assert_awaited_once()
    added.assert_not_awaited()


def test_redeem_missing_shop_client_does_not_create_it(operations_api, operations_database):
    client, _, redeemed = operations_api
    database = operations_database
    response = post(client, database.staff_token(), "/barista/redeem")
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "SHOP_CLIENT_NOT_FOUND"
    assert balance(database) is None
    redeemed.assert_not_awaited()


def test_selected_server_context_is_the_only_write_shop(operations_api, operations_database):
    client, _, _ = operations_api
    database = operations_database
    database.add_second_membership()
    seed_balance(database, cups=5, free=4)
    before = balance(database)
    token = database.staff_token()
    assert client.post("/barista/context", headers=bearer(token), json={"shop_id": 2}).status_code == 200
    result = post(client, token)
    assert result.status_code == 200
    assert result.json()["shop"] == {"id": 2, "name": "Інша кав’ярня"}
    assert balance(database) == before
    assert balance(database, shop_id=2)["cups"] == 1
    assert ledger(database)["transactions"][0]["admin_user_id"] == 1


@pytest.mark.parametrize("path,notification", [("/barista/add-cup", "add"), ("/barista/redeem", "redeem")])
def test_delivery_exception_happens_after_commit_and_does_not_rollback(operations_api, operations_database, path, notification):
    client, added, redeemed = operations_api
    database = operations_database
    seed_balance(database, cups=3, free=1)
    notifier = added if notification == "add" else redeemed
    seen = []

    async def fail_delivery(*args, **kwargs):
        # A separate DB connection must already see the fully committed ledger.
        seen.append(ledger(database))
        raise RuntimeError("offline delivery failure")

    notifier.side_effect = fail_delivery
    response = post(client, database.staff_token(), path)
    assert response.status_code == 200
    assert len(seen[0]["transactions"]) == 1
    assert len(seen[0]["touch_logs"]) == 1
    assert balance(database)["cups"] == (4 if notification == "add" else 3)
    assert balance(database)["free_coffee_balance"] == (1 if notification == "add" else 0)


@pytest.mark.parametrize("operation,first_client", [("add", False), ("add", True), ("redeem", False)])
def test_any_ledger_failure_rolls_back_balance_visit_and_first_insert(operations_database, operation, first_client):
    database = operations_database
    if not first_client:
        seed_balance(database, cups=3, free=1)
    before = ledger(database)
    # Local test schema only: force an error after the balance update.
    database.query("ALTER TABLE transactions ADD CONSTRAINT reject_test_writes CHECK(FALSE) NOT VALID")
    with pytest.raises(CheckViolation):
        if operation == "add":
            database.db.add_cups_for_shop_client(1, 2, 1, 1)
        else:
            database.db.redeem_free_for_shop_client(1, 2, 1)
    assert ledger(database) == before


def test_legacy_count_and_marketing_return_logic_remain_atomic(operations_database):
    database = operations_database
    seed_balance(database, cups=6, free=1)
    database.query("INSERT INTO touch_logs(shop_id,user_id,type) VALUES(1,2,'broadcast')")
    result = database.db.add_cups_for_shop_client(1, 2, 1, 20)
    assert result["shop_client"]["cups"] == 5
    assert result["earned_free"] == 3
    assert result["shop_client"]["free_coffee_balance"] == 4
    assert result["return_source"] == "broadcast"
    assert len(ledger(database)["return_logs"]) == 1
    database.db.redeem_free_for_shop_client(1, 2, 1)
    assert balance(database)["free_coffee_balance"] == 3
    assert len(ledger(database)["return_logs"]) == 1
    assert len(ledger(database)["transactions"]) == 2
    assert len(ledger(database)["touch_logs"]) == 3


def parallel_requests(client, database, monkeypatch, function_name, path, bodies=None):
    router = importlib.import_module("app.api.barista")
    original = router.resolve_barista_session
    entered = threading.Barrier(2)

    def synchronized(*args, **kwargs):
        entered.wait(timeout=4)
        return original(*args, **kwargs)

    monkeypatch.setattr(router, "resolve_barista_session", synchronized)
    # Both requests arrive concurrently BEFORE authorization acquires shop locks.
    # A barrier inside loyalty would wait for a second request that is correctly
    # blocked by the shop closure lock. Verify the committed ledger, not bypass
    # the production lock order, using independent existing staff sessions.
    tokens = (database.staff_token(), database.staff_token())
    bodies = bodies if bodies is not None else (BODY, BODY)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(post, client, token, path, body)
                   for token, body in zip(tokens, bodies)]
        return [future.result(timeout=8) for future in futures]


@pytest.mark.parametrize("existing", [False, True])
def test_two_concurrent_adds_preserve_both_purchases_and_one_reward(operations_api, operations_database, monkeypatch, existing):
    client, added, _ = operations_api
    database = operations_database
    if existing:
        seed_balance(database, cups=6)
    responses = parallel_requests(client, database, monkeypatch, "add_cups_for_shop_client", "/barista/add-cup")
    assert [result.status_code for result in responses] == [200, 200]
    row = balance(database)
    assert row["cups"] == (1 if existing else 2)
    assert row["free_coffee_balance"] == (1 if existing else 0)
    assert row["total_scans"] == 2
    assert row["total_free_coffee_earned"] == (1 if existing else 0)
    assert len(ledger(database)["transactions"]) == 2
    assert len(ledger(database)["touch_logs"]) == 2
    assert added.await_count == 2


@pytest.mark.parametrize("existing", [False, True])
def test_two_concurrent_counted_adds_preserve_every_cup_and_reward(
    operations_api, operations_database, monkeypatch, existing,
):
    client, added, _ = operations_api
    database = operations_database
    if existing:
        seed_balance(database, cups=6)
    counts = (2, 7)
    responses = parallel_requests(
        client, database, monkeypatch, "add_cups_for_shop_client", "/barista/add-cup",
        bodies=tuple({**BODY, "count": count} for count in counts),
    )
    assert [result.status_code for result in responses] == [200, 200]
    assert [result.json()["operation"]["cups_added"] for result in responses] == list(counts)
    total = (6 if existing else 0) + sum(counts)
    row = balance(database)
    assert row["cups"] == total % 7
    assert row["free_coffee_balance"] == total // 7
    assert row["total_scans"] == sum(counts)
    assert row["total_free_coffee_earned"] == total // 7
    assert sorted(item["cups_added"] for item in ledger(database)["transactions"]) == list(counts)
    assert len(ledger(database)["touch_logs"]) == 2
    assert added.await_count == 2


def test_two_concurrent_redeems_of_last_gift_only_one_succeeds(operations_api, operations_database, monkeypatch):
    client, _, redeemed = operations_api
    database = operations_database
    seed_balance(database, cups=2, free=1)
    responses = parallel_requests(client, database, monkeypatch, "redeem_free_for_shop_client", "/barista/redeem")
    assert sorted(result.status_code for result in responses) == [200, 409]
    failure = next(result for result in responses if result.status_code == 409)
    assert failure.json()["detail"]["code"] == "NO_FREE_COFFEE"
    assert balance(database)["free_coffee_balance"] == 0
    assert balance(database)["total_free_coffee_redeemed"] == 1
    assert len(ledger(database)["transactions"]) == 1
    assert len(ledger(database)["touch_logs"]) == 1
    redeemed.assert_awaited_once()


def wait_for_lock(database, fragment, future):
    deadline = time.monotonic() + 4
    while time.monotonic() < deadline:
        if future.done():
            pytest.fail("Request finished before reaching the expected PostgreSQL row lock")
        rows = database.query(
            """SELECT pid FROM pg_stat_activity
               WHERE datname=current_database() AND pid<>pg_backend_pid()
                 AND wait_event_type='Lock' AND query LIKE %s""", ("%" + fragment + "%",))
        if rows:
            return
        time.sleep(0.01)
    pytest.fail("Expected PostgreSQL row lock was not reached")


def test_membership_removal_committed_before_waiting_write_prevents_operation(operations_api, operations_database):
    client, _, _ = operations_api
    database = operations_database
    seed_balance(database, cups=4)
    before = ledger(database)
    token = database.staff_token()
    with ThreadPoolExecutor(max_workers=1) as executor:
        with database.db.get_connection() as locker:
            with locker.transaction():
                locker.execute("DELETE FROM shop_admins WHERE id=1")
                future = executor.submit(post, client, token)
                wait_for_lock(database, "FOR SHARE OF sa", future)
        result = future.result(timeout=5)
    assert result.status_code == 401
    assert ledger(database) == before


def test_membership_lock_lasts_until_loyalty_transaction_commits(operations_api, operations_database, monkeypatch):
    client, _, _ = operations_api
    database = operations_database
    token = database.staff_token()
    router = importlib.import_module("app.api.barista")
    original = router.add_cups_for_shop_client
    entered, release = threading.Event(), threading.Event()

    def pause_write(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=5), "Test failed to release the paused loyalty operation"
        return original(*args, **kwargs)

    monkeypatch.setattr(router, "add_cups_for_shop_client", pause_write)
    with ThreadPoolExecutor(max_workers=2) as executor:
        request = executor.submit(post, client, token)
        try:
            assert entered.wait(timeout=4)
            deletion = executor.submit(database.query, "DELETE FROM shop_admins WHERE id=1")
            wait_for_lock(database, "DELETE FROM shop_admins WHERE id=1", deletion)
        finally:
            release.set()
        assert request.result(timeout=5).status_code == 200
        deletion.result(timeout=5)
    assert balance(database)["cups"] == 1
    assert post(client, token).status_code == 401
    assert len(ledger(database)["transactions"]) == 1
