"""All-owner statistics use current memberships and never widen shop access."""

import pytest

from conftest import bearer
from test_coffee_networks import add_shop, attach, make_network, network_api

pytest_plugins = ("test_owner_onboarding",)

PATH = "/barista/owner/shops/statistics"


def grant_owner(database, shop_id, user_id=1):
    return database.query("""INSERT INTO shop_admins(shop_id,user_id,role)
        VALUES(%s,%s,'owner') RETURNING id""", (shop_id, user_id))[0]["id"]


def get_dashboard(client, token):
    response = client.get(PATH, headers=bearer(token))
    assert response.status_code == 200, response.text
    return response.json()


def test_existing_token_immediately_sees_new_second_and_third_owner_shops(network_api, database):
    client, _ = network_api
    token = database.staff_token()
    assert [row["shop_id"] for row in get_dashboard(client, token)["shops"]] == [1]
    second = add_shop(database, "Second independent")
    grant_owner(database, second)
    assert [row["shop_id"] for row in get_dashboard(client, token)["shops"]] == [1, second]
    third = add_shop(database, "Third independent")
    grant_owner(database, third)
    assert [row["shop_id"] for row in get_dashboard(client, token)["shops"]] == [1, second, third]
    assert database.query("SELECT role FROM shop_admins WHERE id=1") == [{"role": "owner"}]
    # Neither the original account nor its identity/session was replaced.
    assert database.session(token)["user_id"] == 1
    assert database.query("SELECT provider FROM user_identities WHERE user_id=1 ORDER BY provider") == [
        {"provider": "apple"}, {"provider": "google"},
    ]


def test_owner_scope_excludes_admin_foreign_and_inactive_shops(network_api, database):
    client, _ = network_api
    token = database.staff_token()
    database.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(2,1,'admin')")
    inactive = add_shop(database, "Inactive owned")
    grant_owner(database, inactive)
    database.query("UPDATE coffee_shops SET is_active=FALSE WHERE id=%s", (inactive,))
    foreign = add_shop(database, "Foreign owner's shop")
    grant_owner(database, foreign, user_id=3)
    database.query("""INSERT INTO transactions(shop_id,user_id,admin_user_id,type,cups_added)
        VALUES(1,2,1,'add_cups',2),(2,2,3,'add_cups',99),
              (%s,2,1,'add_cups',99),(%s,2,3,'add_cups',99)""", (inactive, foreign))
    result = get_dashboard(client, token)
    assert [row["shop_id"] for row in result["shops"]] == [1]
    assert result["stats"]["cups_today"] == 2
    assert result["stats"]["visits_today"] == 1


def test_network_and_independent_shops_aggregate_without_foreign_network(network_api, database):
    client, _ = network_api
    network = make_network(database, "Owned shared network", mode="SHARED")
    first, second = add_shop(database, "Network A"), add_shop(database, "Network B")
    database.query("INSERT INTO network_memberships(network_id,user_id,role) VALUES(%s,1,'NETWORK_OWNER')",
                   (network["id"],))
    attach(database, network["id"], first)
    attach(database, network["id"], second)
    foreign_network = make_network(database, "Another owner's network")
    foreign = add_shop(database, "Foreign point")
    database.query("INSERT INTO network_memberships(network_id,user_id,role) VALUES(%s,3,'NETWORK_OWNER')",
                   (foreign_network["id"],))
    attach(database, foreign_network["id"], foreign)
    database.query("""INSERT INTO transactions(shop_id,user_id,admin_user_id,type,cups_added)
        VALUES(1,2,1,'add_cups',2),(%s,2,1,'add_cups',3),
              (%s,2,1,'add_cups',4),(%s,2,3,'add_cups',99)""", (first, second, foreign))
    token = database.staff_token()
    result = get_dashboard(client, token)
    assert [row["shop_id"] for row in result["shops"]] == [1, first, second]
    assert result["stats"]["cups_today"] == 9
    assert [row["cups_today"] for row in result["shops"]] == [2, 3, 4]
    network_result = client.get(f"/barista/owner/networks/{network['id']}/statistics",
                                headers=bearer(token))
    assert network_result.status_code == 200
    assert network_result.json()["stats"]["cups_today"] == 7
    assert [row["shop_id"] for row in network_result.json()["shops"]] == [first, second]
    assert client.get(f"/barista/owner/networks/{foreign_network['id']}/statistics",
                      headers=bearer(token)).status_code == 403


def test_periods_deduplicate_clients_and_preserve_per_shop_statistics(network_api, database):
    client, _ = network_api
    second = add_shop(database, "Second owner shop")
    grant_owner(database, second)
    database.query("INSERT INTO users(id,full_name,personal_qr_token) VALUES(4,'Longer client','fourth-client')")
    database.query("""INSERT INTO shop_clients(shop_id,user_id,created_at,last_activity_at)
        VALUES(1,2,NOW()-INTERVAL '100 days',NOW()),
              (%s,2,NOW(),NOW()),
              (%s,3,NOW()-INTERVAL '3 days',NOW()-INTERVAL '3 days'),
              (1,4,NOW()-INTERVAL '20 days',NOW()-INTERVAL '20 days')""", (second, second))
    database.query("""INSERT INTO transactions(shop_id,user_id,admin_user_id,type,cups_added,free_redeemed,created_at)
        VALUES(1,2,1,'add_cups',3,0,NOW()),
              (1,2,1,'redeem_free',0,1,NOW()),
              (1,2,1,'add_cups',2,0,NOW()-INTERVAL '5 days'),
              (1,4,1,'add_cups',4,0,NOW()-INTERVAL '20 days'),
              (%s,2,1,'add_cups',5,0,NOW()),
              (%s,3,1,'redeem_free',0,2,NOW()-INTERVAL '5 days'),
              (%s,2,1,'add_cups',6,0,NOW()-INTERVAL '40 days'),
              (2,2,3,'add_cups',99,0,NOW())""", (second, second, second))
    result = get_dashboard(client, database.staff_token())
    assert result["stats"]["clients"] == 3
    assert result["stats"]["active_clients"] == 3
    assert result["periods"] == {
        "today": {"visits_today": 2, "cups_today": 8, "gifts_used_today": 1,
                  "new_clients_today": 0, "active_clients_today": 1},
        "7": {"visits_7d": 3, "cups_7d": 10, "gifts_7d": 3,
              "new_clients_7d": 1, "active_clients_7d": 2},
        "30": {"visits_30d": 4, "cups_30d": 14, "gifts_30d": 3,
               "new_clients_30d": 2, "active_clients_30d": 3},
    }
    by_shop = {row["shop_id"]: row for row in result["shops"]}
    assert by_shop[1]["cups_today"] == 3
    assert by_shop[second]["cups_today"] == 5
    assert by_shop[second]["clients"] == 2
    assert by_shop[second]["new_clients_today"] == 1
    assert by_shop[second]["new_clients_7d"] == 2


def test_empty_owned_shop_has_zero_metrics_and_read_does_not_extend_session(network_api, database):
    client, _ = network_api
    token = database.staff_token()
    before = database.session(token)
    result = get_dashboard(client, token)
    assert set(result["stats"].values()) == {0}
    assert set(value for row in result["periods"].values() for value in row.values()) == {0}
    assert result["shops"][0]["active_clients_today"] == 0
    after = database.session(token)
    assert before["expires_at"] == after["expires_at"]
    assert before["last_used_at"] == after["last_used_at"]


def test_current_gifts_sum_independent_and_per_shop_balances_only(network_api, database):
    client, _ = network_api
    network = make_network(database, "Separate loyalty", mode="PER_SHOP")
    first, second, foreign = (add_shop(database, name) for name in ("Separate A", "Separate B", "Foreign"))
    for point in (first, second):
        attach(database, network["id"], point)
        grant_owner(database, point)
    grant_owner(database, foreign, user_id=3)
    database.query("""INSERT INTO shop_clients(shop_id,user_id,free_coffee_balance)
        VALUES(1,2,5),(%s,2,2),(%s,2,3),(%s,2,99)""", (first, second, foreign))
    # A leftover shared ledger must not override PER_SHOP's authoritative rows.
    database.query("""INSERT INTO network_client_loyalty(network_id,user_id,free_coffee_balance)
        VALUES(%s,2,99)""", (network["id"],))
    result = get_dashboard(client, database.staff_token())
    assert result["stats"]["gifts_balance"] == 10
    assert {row["shop_id"]: row["gifts_balance"] for row in result["shops"]} == {
        1: 5, first: 2, second: 3,
    }
    assert result["stats"]["gifts_used_today"] == 0


def test_shared_gifts_deduplicate_ledger_and_exclude_foreign_only_clients(network_api, database):
    client, _ = network_api
    network = make_network(database, "Shared scoped balance", mode="SHARED")
    first, second, foreign = (add_shop(database, name) for name in ("Owned A", "Owned B", "Foreign point"))
    for point in (first, second, foreign):
        attach(database, network["id"], point)
    for point in (first, second):
        grant_owner(database, point)
    grant_owner(database, foreign, user_id=3)
    database.query("INSERT INTO users(id,full_name,personal_qr_token) VALUES(4,'Foreign-only client','gifts-foreign')")
    database.query("""INSERT INTO shop_clients(shop_id,user_id,free_coffee_balance)
        VALUES(1,2,4),(%s,2,20),(%s,2,30),(%s,3,40),(%s,4,99)""",
        (first, second, first, foreign))
    database.query("""INSERT INTO network_client_loyalty(network_id,user_id,free_coffee_balance)
        VALUES(%s,2,3),(%s,3,2),(%s,4,99)""", (network["id"],) * 3)
    result = get_dashboard(client, database.staff_token())
    # User 2's shared balance is counted once despite relationships at both
    # owned points. User 4 belongs only to the inaccessible point.
    assert result["stats"]["gifts_balance"] == 4 + 3 + 2
    assert {row["shop_id"]: row["gifts_balance"] for row in result["shops"]} == {
        1: 4, first: 5, second: 3,
    }
    assert result["stats"]["clients"] == 2
    assert result["stats"]["gifts_30d"] == 0


def test_client_onboarding_admin_only_and_stale_sessions_cannot_read_owner_stats(network_api, database):
    client, _ = network_api
    for purpose in ("client", "barista_onboarding"):
        token = database.db.create_app_session(1, purpose=purpose)
        assert client.get(PATH, headers=bearer(token)).status_code == 401
    admin_token = database.staff_token(user_id=3, membership_id=2)
    response = client.get(PATH, headers=bearer(admin_token))
    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "OWNER_ACCESS_REQUIRED"
    owner_token = database.staff_token()
    database.query("UPDATE shop_admins SET role='admin' WHERE id=1")
    assert client.get(PATH, headers=bearer(owner_token)).status_code == 403
    database.query("DELETE FROM shop_admins WHERE id=1")
    assert client.get(PATH, headers=bearer(owner_token)).status_code in (401, 403)
    assert client.get(PATH).status_code == 401
