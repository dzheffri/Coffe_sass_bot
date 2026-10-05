"""Web/native recorded-return parity on guarded local PostgreSQL fixtures."""

import importlib

import pytest

from conftest import bearer
from test_barista_operations import operations_database, seed_balance
from test_barista_owner_tools import owner_api, owner_database


@pytest.fixture
def recorded_returns_database(owner_database):
    database = owner_database
    seed_balance(database, cups=3, free=1)
    database.query("""
        INSERT INTO touch_logs(shop_id,user_id,type,sent_at)
            SELECT 1,4,'auto',
                NOW()-CASE
                    WHEN number<=18 THEN INTERVAL '2 days'
                    WHEN number<=49 THEN INTERVAL '15 days'
                    WHEN number<=131 THEN INTERVAL '1 day'
                    ELSE INTERVAL '15 days'
                END
            FROM generate_series(1,326) AS series(number);
        INSERT INTO return_logs(shop_id,user_id,touch_log_id,touch_type,returned_at)
            SELECT 1,4,number,'auto',
                NOW()-CASE WHEN number<=18 THEN INTERVAL '1 day' ELSE INTERVAL '12 days' END
            FROM generate_series(1,49) AS series(number);
        INSERT INTO touch_logs(shop_id,user_id,type)
            SELECT 1,4,'service' FROM generate_series(1,11);
        INSERT INTO touch_logs(shop_id,user_id,type,sent_at)
            SELECT 1,4,'auto',NOW()-INTERVAL '31 days' FROM generate_series(1,2);
        INSERT INTO return_logs(shop_id,user_id,touch_log_id,touch_type,returned_at)
            SELECT 1,4,id,'auto',NOW()-INTERVAL '31 days'
            FROM touch_logs WHERE shop_id=1 AND sent_at<NOW()-INTERVAL '30 days';
        INSERT INTO touch_logs(shop_id,user_id,type)
            SELECT 2,5,'auto' FROM generate_series(1,9);
        INSERT INTO return_logs(shop_id,user_id,touch_log_id,touch_type)
            SELECT 2,5,id,'auto' FROM touch_logs WHERE shop_id=2;
    """)
    return database


def web_details(shop_id=1):
    # The actual Web module/helper run against the fixture's guarded app.db.
    module = importlib.import_module("app.web_panel_logic")
    return module.get_owner_details_stats(1001, authorized_shop_id=shop_id)


@pytest.mark.parametrize("days,expected_returns", [(7,18), (30,49)])
def test_native_owner_uses_web_recorded_returns_for_the_selected_period(
    owner_api, recorded_returns_database, days, expected_returns,
):
    client, _ = owner_api
    database = recorded_returns_database
    token = database.staff_token()
    before = database.session(token)
    web = web_details()
    assert web["efficiency"] == {"percent": 15.03, "returned_clients": 49}
    assert web["mailings"] == {
        "auto_sent": 326, "auto_returns": 49,
        "own_sent": 0, "own_returns": 0, "total_sent": 326,
    }
    assert web["returns"]["days_7"] == 18 and web["returns"]["days_30"] == 49
    response = client.get(f"/barista/statistics?days={days}", headers=bearer(token))
    assert response.status_code == 200
    assert response.json()["metrics"]["returns_after_broadcast"] == expected_returns
    if days == 30:
        assert response.json()["metrics"]["returns_after_broadcast"] == web["efficiency"]["returned_clients"]
    assert database.session(token) == before


def test_shared_helper_keeps_default_period_rounding_and_cross_shop_isolation(recorded_returns_database):
    database = recorded_returns_database
    marketing = database.db.get_shop_marketing_efficiency(1)
    assert marketing["total_sent"] == 326 and marketing["total_returns"] == 49
    assert marketing["percent"] == 15.03
    recent = database.db.get_shop_marketing_efficiency(1, days=7)
    assert recent["total_sent"] == 100 and recent["total_returns"] == 18
    assert recent["percent"] == 18.0
    other = web_details(shop_id=2)
    assert other["efficiency"] == {"percent": 100.0, "returned_clients": 9}
    assert other["mailings"]["total_sent"] == 9


def test_mixed_and_legacy_return_types_count_events_without_deduplicating_clients(
    owner_api, recorded_returns_database,
):
    client, _ = owner_api
    database = recorded_returns_database
    # Model an old return-log schema that retained arbitrary historical labels.
    # This change applies only to this disposable local fixture schema.
    database.query("ALTER TABLE return_logs DROP CONSTRAINT return_logs_touch_type_check")
    database.query("""
        INSERT INTO touch_logs(shop_id,user_id,type)
            SELECT 1,4,'broadcast' FROM generate_series(1,3);
        INSERT INTO return_logs(shop_id,user_id,touch_log_id,touch_type)
            SELECT 1,4,id,'broadcast' FROM touch_logs WHERE shop_id=1 AND type='broadcast';
        INSERT INTO return_logs(shop_id,user_id,touch_log_id,touch_type)
            SELECT 1,4,id,CASE WHEN mod(id,2)=0 THEN 'owner' ELSE 'legacy' END
            FROM touch_logs WHERE shop_id=1 AND type='service' ORDER BY id LIMIT 4;
    """)
    web = web_details()
    marketing = database.db.get_shop_marketing_efficiency(1)
    assert marketing == {
        "auto_sent": 326, "own_sent": 3, "total_sent": 329,
        "auto_returns": 49, "own_returns": 3, "total_returns": 56,
        "percent": 17.02,
    }
    assert web["efficiency"] == {"percent": 17.02, "returned_clients": 56}
    assert web["mailings"]["own_returns"] == 3
    # All 56 rows belong to one client: COUNT(DISTINCT user_id) would be wrong.
    assert database.query("SELECT COUNT(DISTINCT user_id) AS count FROM return_logs WHERE shop_id=1")[0]["count"] == 1
    response = client.get("/barista/statistics?days=30", headers=bearer(database.staff_token()))
    assert response.status_code == 200
    assert response.json()["metrics"]["returns_after_broadcast"] == 56


def test_admin_return_metric_remains_unavailable(owner_api, recorded_returns_database):
    client, _ = owner_api
    token = recorded_returns_database.staff_token(user_id=2, membership_id=3)
    response = client.get("/barista/statistics?days=7", headers=bearer(token))
    assert response.status_code == 200
    data = response.json()
    assert data["metrics"]["returns_after_broadcast"] is None
    assert "returns_after_broadcast" in data["unavailable_metrics"]


def test_zero_marketing_denominator_preserves_zero_efficiency(owner_database):
    marketing = owner_database.db.get_shop_marketing_efficiency(1)
    assert marketing["total_sent"] == marketing["total_returns"] == 0
    assert marketing["percent"] == 0.0
    assert web_details()["efficiency"] == {"percent": 0.0, "returned_clients": 0}
