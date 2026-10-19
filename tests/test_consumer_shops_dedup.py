"""Consumer shop rows stay canonical and unique on the guarded local PostgreSQL DB.

Only selected function definitions are compiled from main.py. Importing this test
never loads app.db/main startup code or reads production configuration.
"""

from datetime import datetime, timezone

import pytest
from fastapi import Depends, Header, HTTPException

from conftest import PROJECT, bearer, extract_definitions
from test_coffee_networks import add_shop, attach, make_network, network_api

pytest_plugins = ("test_owner_onboarding",)


PROFILE_FIELDS = (
    "name", "city", "subtitle", "address", "work_from", "work_to", "instagram",
    "description", "logo_url", "cover_url",
)
NEWS_FIELDS = {"id", "title", "price", "image_url", "sort_order"}
PERSONAL_PATHS = ("/me/shops", "/account/2/shops", "/users/2002/shops")


@pytest.fixture
def consumer_api(network_api):
    client, database = network_api
    database.query("UPDATE users SET telegram_user_id=2002 WHERE id=2")
    # Keep the legacy fixture rows available for owner/staff compatibility while
    # each assertion names only the active locations created for that scenario.
    database.query("UPDATE coffee_shops SET is_active=FALSE")
    database.query("""CREATE TABLE IF NOT EXISTS shop_news (
        id BIGSERIAL PRIMARY KEY,shop_id BIGINT,title TEXT,price TEXT,image_url TEXT,sort_order INT)""")

    def forbidden_owner_default_profile(_owner_id):
        raise AssertionError("A consumer location must not use its owner's default shop profile")

    namespace = {
        "app": client.app, "Depends": Depends, "Header": Header, "HTTPException": HTTPException,
        "get_connection": database.db.get_connection,
        "get_app_session": database.db.get_app_session,
        "get_shop_profile": forbidden_owner_default_profile,
    }
    extract_definitions(PROJECT / "app/api/main.py", {
        "_get_bearer_token_or_401", "get_current_user", "me_shops", "all_shops", "account_shops",
        "user_shops", "_user_shop_loyalty_rows", "_consumer_shop_news", "_consumer_shop_payload",
    }, namespace)
    token = database.db.create_app_session(2, purpose="client")
    return client, database, bearer(token)


def personal_shops(client, headers, expected_ids):
    """Exercise /me plus both retained account/Telegram consumer route contracts."""
    results = []
    for path in PERSONAL_PATHS:
        response = client.get(path, headers=headers)
        assert response.status_code == 200, (path, response.text)
        assert response.json()["ok"] is True
        shops = response.json()["shops"]
        ids = [shop["shop_id"] for shop in shops]
        assert len(ids) == len(set(ids)) == len(expected_ids), (path, ids)
        assert set(ids) == set(expected_ids), (path, ids)
        results.append(shops)
    assert results[0] == results[1] == results[2]
    return results[0]


def discovery_shops(client, expected_ids):
    response = client.get("/shops")
    assert response.status_code == 200, response.text
    assert response.json()["ok"] is True
    shops = response.json()["shops"]
    ids = [shop["shop_id"] for shop in shops]
    assert len(ids) == len(set(ids)) == len(expected_ids), ids
    assert set(ids) == set(expected_ids), ids
    # Discovery never projects a customer's private loyalty counters.
    assert all((item["cups"], item["free_coffee_balance"]) == (0, 0) for item in shops)
    return shops


def direct_client(database, shop_id, *, user_id=2, cups=2, gifts=3, activity="2026-10-01T10:00:00+00:00"):
    database.query("""INSERT INTO shop_clients
        (shop_id,user_id,cups,free_coffee_balance,last_activity_at)
        VALUES(%s,%s,%s,%s,%s)""", (shop_id, user_id, cups, gifts, activity))


def shared_client(database, network_id, *, user_id=2, cups=5, gifts=2, last_shop_id=None):
    database.query("""INSERT INTO network_client_loyalty
        (network_id,user_id,cups,free_coffee_balance,last_shop_id,last_activity_at)
        VALUES(%s,%s,%s,%s,%s,'2026-10-02T12:00:00+00:00')""",
        (network_id, user_id, cups, gifts, last_shop_id))


def owner(database, shop_id, user_id=1, network_membership_id=None):
    return database.query("""INSERT INTO shop_admins(shop_id,user_id,role,network_membership_id)
        VALUES(%s,%s,'owner',%s) RETURNING id""", (shop_id, user_id, network_membership_id))[0]["id"]


def new_user(database, label):
    return database.query("""INSERT INTO users(username,full_name,personal_qr_token)
        VALUES(%s,%s,%s) RETURNING id""", (label, label, "local-dedup-qr-" + label))[0]["id"]


def profile(database, shop_id, label):
    expected = {
        "name": label, "city": "Київ " + label, "subtitle": "Підзаголовок " + label,
        "address": "Адреса " + label, "work_from": "07:15", "work_to": "21:45",
        "instagram": "https://instagram.com/" + label,
        "description": "Опис саме цієї точки " + label,
        "logo_url": "/uploads/" + label + "-logo.png", "cover_url": "/uploads/" + label + "-cover.jpg",
    }
    database.query("""UPDATE coffee_shops SET name=%s,city=%s,subtitle=%s,address=%s,
        work_from=%s,work_to=%s,instagram=%s,description=%s,logo_url=%s,cover_url=%s
        WHERE id=%s""", tuple(expected[field] for field in PROFILE_FIELDS) + (shop_id,))
    return expected


def news(database, shop_id, label):
    created = []
    for title, price, image, position in (
        (label + " last", "95", "/uploads/" + label + "-last.jpg", 2),
        (label + " first", "70", "/uploads/" + label + "-first.jpg", 1),
        (label + " tied", "80", "/uploads/" + label + "-tied.jpg", 1),
    ):
        item = database.query("""INSERT INTO shop_news(shop_id,title,price,image_url,sort_order)
            VALUES(%s,%s,%s,%s,%s) RETURNING id,title,price,image_url,sort_order""",
            (shop_id, title, price, image, position))[0]
        created.append(item)
    return sorted(created, key=lambda item: (item["sort_order"], item["id"]))


@pytest.mark.parametrize("relationship", ["direct", "shared", "direct_and_shared"])
def test_one_location_is_one_consumer_card_for_each_loyalty_source(consumer_api, relationship):
    client, database, headers = consumer_api
    if relationship == "direct":
        point = add_shop(database, "Direct only")
    else:
        network = make_network(database, mode="SHARED")
        point = add_shop(database, "Shared only")
        attach(database, network["id"], point)
        shared_client(database, network["id"], last_shop_id=point)
    if relationship in ("direct", "direct_and_shared"):
        # This stale per-location value must never be added to a SHARED ledger.
        direct_client(database, point, cups=2, gifts=3)
    owner(database, point)

    item = personal_shops(client, headers, [point])[0]
    expected = (2, 3) if relationship == "direct" else (5, 2)
    assert (item["cups"], item["free_coffee_balance"]) == expected
    assert item["owner_telegram_id"] == 1001
    discovery_shops(client, [point])


@pytest.mark.parametrize("networks_enabled", [True, False])
def test_multiple_legacy_direct_and_network_owners_never_multiply_consumer_rows(consumer_api, networks_enabled):
    client, database, headers = consumer_api
    network = make_network(database, mode="SHARED")
    point = add_shop(database, "Owner fanout")
    attach(database, network["id"], point)
    direct_client(database, point, cups=2, gifts=3)
    shared_client(database, network["id"], cups=4, gifts=2, last_shop_id=point)
    network_membership = database.query("""INSERT INTO network_memberships(user_id,network_id,role)
        VALUES(1,%s,'NETWORK_OWNER') RETURNING id""", (network["id"],))[0]["id"]
    owner(database, point, user_id=1, network_membership_id=network_membership)
    owner(database, point, user_id=3)  # Retained Telegram owner.
    owner(database, point, user_id=new_user(database, "second-direct-owner"))
    staff = new_user(database, "point-admin")
    database.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(%s,%s,'admin')", (point, staff))
    if not networks_enabled:
        # Execute the real pre-network branch, retaining its legacy membership rows.
        database.query("DROP TABLE coffee_networks CASCADE")

    item = personal_shops(client, headers, [point])[0]
    assert (item["cups"], item["free_coffee_balance"]) == ((4, 2) if networks_enabled else (2, 3))
    assert item["owner_telegram_id"] == 1001  # First owner metadata is stable, never a JOIN fanout.
    public = discovery_shops(client, [point])[0]
    assert public["owner_telegram_id"] == 1001


@pytest.mark.parametrize("loyalty_mode", [None, "PER_SHOP", "SHARED"])
def test_two_shops_of_one_owner_keep_their_own_metadata_news_and_balances(consumer_api, loyalty_mode):
    client, database, headers = consumer_api
    first, second = add_shop(database, "A"), add_shop(database, "B")
    if loyalty_mode:
        network = make_network(database, mode=loyalty_mode)
        attach(database, network["id"], first)
        attach(database, network["id"], second)
        if loyalty_mode == "SHARED":
            shared_client(database, network["id"], cups=5, gifts=2, last_shop_id=second)
    for point in (first, second):
        owner(database, point)
    expected_profiles = {first: profile(database, first, "PointA"), second: profile(database, second, "PointB")}
    expected_news = {first: news(database, first, "PointA"), second: news(database, second, "PointB")}
    direct_client(database, first, cups=1, gifts=2)
    direct_client(database, second, cups=3, gifts=4)

    personal = personal_shops(client, headers, [first, second])
    public = discovery_shops(client, [first, second])
    for item in personal + public:
        point = item["shop_id"]
        assert {key: item[key] for key in PROFILE_FIELDS} == expected_profiles[point]
        assert item["news"] == expected_news[point]
        assert all(set(entry) == NEWS_FIELDS for entry in item["news"])
        assert item["owner_telegram_id"] == 1001
    by_id = {item["shop_id"]: item for item in personal}
    if loyalty_mode == "SHARED":
        assert [(item["cups"], item["free_coffee_balance"]) for item in personal] == [(5, 2), (5, 2)]
    else:
        assert (by_id[first]["cups"], by_id[first]["free_coffee_balance"]) == (1, 2)
        assert (by_id[second]["cups"], by_id[second]["free_coffee_balance"]) == (3, 4)
    # Repeated discovery must preserve both location order and news tie ordering.
    assert discovery_shops(client, [first, second]) == public


def test_per_shop_mode_ignores_stale_network_ledger_for_eligibility_and_balances(consumer_api):
    client, database, headers = consumer_api
    network = make_network(database, mode="PER_SHOP")
    first, second = add_shop(database, "Per shop A"), add_shop(database, "Per shop B")
    attach(database, network["id"], first)
    attach(database, network["id"], second)
    direct_client(database, first, cups=3, gifts=1)
    shared_client(database, network["id"], cups=6, gifts=9, last_shop_id=second)
    item = personal_shops(client, headers, [first])[0]
    assert (item["cups"], item["free_coffee_balance"]) == (3, 1)
    discovery_shops(client, [first, second])


def test_staff_network_membership_alone_does_not_grant_consumer_loyalty_access(consumer_api):
    client, database, headers = consumer_api
    network = make_network(database, mode="SHARED")
    first, second = add_shop(database, "Staff A"), add_shop(database, "Staff B")
    attach(database, network["id"], first)
    attach(database, network["id"], second)
    membership_id = database.query("""INSERT INTO network_memberships(user_id,network_id,role)
        VALUES(2,%s,'NETWORK_OWNER') RETURNING id""", (network["id"],))[0]["id"]
    for point in (first, second):
        owner(database, point, user_id=2, network_membership_id=membership_id)
    # Another consumer's activity must not become this staff user's relationship.
    direct_client(database, first, user_id=3, cups=1, gifts=4)
    shared_client(database, network["id"], user_id=3, cups=6, gifts=8, last_shop_id=first)
    assert personal_shops(client, headers, []) == []
    discovery_shops(client, [first, second])


def test_closed_and_physically_deleted_locations_do_not_appear_through_shared_projection(consumer_api):
    client, database, headers = consumer_api
    network = make_network(database, mode="SHARED")
    active, closed, deleted = (add_shop(database, name) for name in ("Active", "Closed", "Deleted"))
    for point in (active, closed, deleted):
        attach(database, network["id"], point)
        direct_client(database, point)
        news(database, point, str(point))
    shared_client(database, network["id"], cups=4, gifts=2, last_shop_id=deleted)
    database.query("UPDATE coffee_shops SET is_active=FALSE WHERE id=%s", (closed,))
    database.query("DELETE FROM coffee_shops WHERE id=%s", (deleted,))
    item = personal_shops(client, headers, [active])[0]
    assert (item["cups"], item["free_coffee_balance"]) == (4, 2)
    discovery_shops(client, [active])
    assert database.query("SELECT cups,free_coffee_balance FROM shop_clients WHERE shop_id=%s", (closed,)) == [
        {"cups": 2, "free_coffee_balance": 3}
    ]


def test_equal_activity_and_names_have_stable_location_id_order(consumer_api):
    client, database, headers = consumer_api
    first, second = add_shop(database, "Same name"), add_shop(database, "Same name")
    for point in (first, second):
        direct_client(database, point, activity=datetime(2026, 10, 3, 12, tzinfo=timezone.utc))
        owner(database, point)
    assert [item["shop_id"] for item in personal_shops(client, headers, [first, second])] == [first, second]
    assert [item["shop_id"] for item in discovery_shops(client, [first, second])] == [first, second]


def test_current_consumer_session_cannot_project_another_network_customers_balance(consumer_api):
    client, database, headers = consumer_api
    own_network = make_network(database, name="Own shared", mode="SHARED")
    foreign_network = make_network(database, name="Foreign shared", mode="SHARED")
    own, foreign = add_shop(database, "Own point"), add_shop(database, "Foreign point")
    attach(database, own_network["id"], own)
    attach(database, foreign_network["id"], foreign)
    owner(database, own)
    owner(database, foreign)  # Same owner never establishes a consumer relationship.
    shared_client(database, own_network["id"], cups=2, gifts=1, last_shop_id=own)
    shared_client(database, foreign_network["id"], user_id=3, cups=6, gifts=9, last_shop_id=foreign)
    item = personal_shops(client, headers, [own])[0]
    assert (item["cups"], item["free_coffee_balance"]) == (2, 1)
    response = client.get("/me/shops?user_id=3&shop_id=" + str(foreign), headers=headers)
    assert response.status_code == 200
    assert response.json()["shops"] == [item]
    discovery_shops(client, [own, foreign])


def test_authenticated_consumer_route_keeps_client_session_boundary(consumer_api):
    client, database, headers = consumer_api
    point = add_shop(database, "Authenticated point")
    direct_client(database, point)
    assert client.get("/me/shops").status_code == 401
    staff_token = database.db.create_app_session(1, purpose="barista", selected_membership_id=1)
    assert client.get("/me/shops", headers=bearer(staff_token)).status_code == 401
    assert personal_shops(client, headers, [point])[0]["shop_id"] == point
