"""Super Admin owner discovery by real shop name, using the guarded local DB."""

import pytest

from conftest import bearer
from test_coffee_networks import network_api, add_shop, make_network, attach
from test_owner_onboarding import onboarding_api, SUPERADMIN, new_invite


def search(client, query):
    response = client.get("/superadmin/owners/by-shop", headers=SUPERADMIN, params={"query": query})
    assert response.status_code == 200, response.text
    return response.json()["shops"]


def test_partial_shop_name_returns_current_owner_and_internal_id(network_api, database):
    client, _ = network_api
    database.query("UPDATE coffee_shops SET name='For you & me' WHERE id=1")
    expected = [{"shop_id": 1, "name": "For you & me", "owners": [
        {"user_id": 1, "name": "Ольга", "username": "owner", "telegram_user_id": 1001, "shops_count": 1},
    ]}]
    assert search(client, "  YOU &  ") == expected
    assert search(client, "for") == expected
    database.query("UPDATE coffee_shops SET name='КАВОВА Мрія' WHERE id=1")
    assert search(client, "кавова мрія")[0]["shop_id"] == 1
    assert search(client, "") == search(client, "   ") == search(client, "missing") == []


@pytest.mark.parametrize("token", [None, "local-owner", "local-admin"])
def test_shop_owner_search_is_superadmin_only(network_api, database, token):
    client, _ = network_api
    before = database.query("SELECT * FROM shop_admins ORDER BY id")
    response = client.get("/superadmin/owners/by-shop", headers=bearer(token) if token else {},
                          params={"query": "Наші"})
    assert response.status_code == (403 if token else 401)
    assert database.query("SELECT * FROM shop_admins ORDER BY id") == before


def test_multiple_owners_are_listed_once_and_staff_is_not_an_owner(network_api, database):
    client, _ = network_api
    database.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(1,2,'owner'),(1,3,'admin')")
    assert [owner["user_id"] for owner in search(client, "Наші")[0]["owners"]] == [1, 2]
    # The second shop has an ADMIN, not an OWNER: it must remain selectable with an empty owner list.
    assert search(client, "Інша") == [{"shop_id": 2, "name": "Інша кав’ярня", "owners": []}]


def test_existing_telegram_owner_uses_membership_and_pending_mapping_is_not_authority(onboarding_api, database):
    client, _ = onboarding_api
    # The pre-network schema stores claimed legacy Telegram owners in shop_admins.
    assert search(client, "Наші")[0]["owners"][0]["user_id"] == 1
    database.query("UPDATE coffee_shops SET pending_owner_telegram_id=1001 WHERE id=2")
    assert search(client, "Інша")[0]["owners"] == []
    assert database.query("SELECT user_id,role FROM shop_admins WHERE shop_id=2") == [{"user_id": 3, "role": "admin"}]


def test_inactive_deleted_and_explicit_test_shops_are_hidden_from_results_and_counts(network_api, database):
    client, _ = network_api
    database.query("ALTER TABLE coffee_shops ADD COLUMN deleted_at TIMESTAMPTZ; ALTER TABLE coffee_shops ADD COLUMN is_test BOOLEAN; ALTER TABLE coffee_shops ADD COLUMN is_demo BOOLEAN")
    visible = add_shop(database, "Coffee Real")
    for name in ("Coffee TEST", "[TEST] Coffee", "Coffee ТЕСТ", "Демо · Coffee", "Coffee DEMO"):
        hidden = add_shop(database, name)
        database.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(%s,1,'owner')", (hidden,))
    inactive, deleted, flagged = (add_shop(database, f"Coffee {label}") for label in ("Closed", "Removed", "Fixture"))
    database.query("UPDATE coffee_shops SET is_active=FALSE WHERE id=%s", (inactive,))
    database.query("UPDATE coffee_shops SET deleted_at=NOW() WHERE id=%s", (deleted,))
    database.query("UPDATE coffee_shops SET is_test=TRUE WHERE id=%s", (flagged,))
    for shop_id in (visible, inactive, deleted, flagged):
        database.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(%s,1,'owner')", (shop_id,))
    for column, value in (("is_demo", True), ("city", "Демо"), ("city", " DEMO ")):
        demo = add_shop(database, "Coffee House")
        database.query(f"UPDATE coffee_shops SET {column}=%s WHERE id=%s", (value, demo))
        database.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(%s,1,'owner')", (demo,))
    result = search(client, "Coffee")
    assert [shop["shop_id"] for shop in result] == [visible]
    assert result[0]["owners"][0]["shops_count"] == 2  # The original shop and Coffee Real.
    # Test markers are words; normal business names containing those letters remain searchable.
    contest = add_shop(database, "Contest Coffee")
    assert contest in {shop["shop_id"] for shop in search(client, "Coffee")}


def test_network_owner_and_direct_membership_are_deduplicated_in_results_and_counts(network_api, database):
    client, _ = network_api
    network = make_network(database)
    attach(database, network["id"], 1)
    second = add_shop(database, "Network Coffee", network["id"])
    # A network owner remains discoverable even if compatibility shop rows have not yet been materialized.
    database.query("INSERT INTO network_memberships(network_id,user_id,role) VALUES(%s,1,'NETWORK_OWNER')", (network["id"],))
    first = search(client, "Наші")[0]
    assert len(first["owners"]) == 1
    assert first["owners"][0]["shops_count"] == 2
    assert search(client, "Network Coffee")[0]["owners"][0]["user_id"] == 1
    database.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(%s,1,'owner')", (second,))
    assert search(client, "Network Coffee")[0]["owners"] == first["owners"]


def test_name_search_treats_sql_wildcards_literally_and_orders_results_predictably(network_api, database):
    client, _ = network_api
    matching = add_shop(database, "Coffee_100%")
    add_shop(database, "CoffeeX1000")
    assert [row["shop_id"] for row in search(client, "_100%")] == [matching]
    assert search(client, "' OR 1=1 --") == []
    same_one = add_shop(database, "Coffee Same")
    same_two = add_shop(database, "Coffee Same")
    assert [row["shop_id"] for row in search(client, "Coffee Same")] == [same_one, same_two]
    assert client.get("/superadmin/owners/by-shop", headers=SUPERADMIN, params={"query": "x" * 201}).status_code == 422


def test_selected_owner_assigns_new_shop_preserving_old_access_and_owner_code_fallback(network_api, database):
    client, _ = network_api
    old_memberships = database.query("SELECT * FROM shop_admins WHERE user_id=1 ORDER BY id")
    token = database.staff_token()
    owner_id = search(client, "Наші")[0]["owners"][0]["user_id"]
    response = client.post("/superadmin/shops", headers=SUPERADMIN,
                           json={"name": "Second Coffee", "owner_user_id": owner_id})
    assert response.status_code == 201, response.text
    new_id = response.json()["shop"]["shop_id"]
    assert response.json()["shop"]["owner_user_id"] == owner_id
    assert database.query("SELECT * FROM shop_admins WHERE id=1") == old_memberships
    assert database.query("SELECT role FROM shop_admins WHERE shop_id=%s AND user_id=1", (new_id,)) == [{"role": "owner"}]
    assert {row["shop_id"] for row in client.get("/barista/me", headers=bearer(token)).json()["shops"]} == {1, new_id}
    assert database.query("SELECT * FROM barista_admin_invites WHERE shop_id=%s", (new_id,)) == []
    unassigned = client.post("/superadmin/shops", headers=SUPERADMIN, json={"name": "New Person Coffee"})
    assert unassigned.status_code == 201 and unassigned.json()["shop"]["owner_connected"] is False
    invitation = new_invite(client, unassigned.json()["shop"]["shop_id"])
    assert invitation["code"]
