"""Existing-owner assignment and initial-only owner codes on the guarded local DB."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from conftest import bearer
from test_coffee_networks import network_api, make_network, add_shop, attach
from test_owner_onboarding import onboarding_api, SUPERADMIN, login, claim, new_shop, new_invite


def test_owner_search_uses_public_metadata_and_excludes_consumers_and_staff(network_api, database):
    client, _ = network_api
    assert client.get("/superadmin/owners", headers=SUPERADMIN).json()["owners"] == []
    result = client.get("/superadmin/owners", headers=SUPERADMIN, params={"query": "owner"})
    assert result.status_code == 200
    assert result.json()["owners"] == [
        {"user_id": 1, "name": "Ольга", "username": "owner", "telegram_user_id": 1001},
    ]
    for query in ("1", "Оль", "owner"):
        assert client.get("/superadmin/owners", headers=SUPERADMIN, params={"query": query}).json()["owners"] == result.json()["owners"]
    for query in ("customer", "other", "google-staff", "no match"):
        assert client.get("/superadmin/owners", headers=SUPERADMIN, params={"query": query}).json()["owners"] == []
    network = make_network(database)
    database.query("INSERT INTO network_memberships(network_id,user_id,role) VALUES(%s,2,'NETWORK_OWNER')", (network["id"],))
    assert [row["user_id"] for row in client.get("/superadmin/owners", headers=SUPERADMIN,
                                                params={"query": "customer"}).json()["owners"]] == [2]


@pytest.mark.parametrize("token", [None, "local-owner", "local-admin"])
def test_owner_search_and_direct_assignment_require_superadmin(network_api, database, token):
    client, _ = network_api
    headers = bearer(token) if token else {}
    expected = 403 if token else 401
    assert client.get("/superadmin/owners", headers=headers).status_code == expected
    for path in ("/superadmin/shops/2/owner", "/superadmin/networks/1/owner"):
        assert client.post(path, headers=headers, json={"user_id": 1}).status_code == expected
    assert len(database.query("SELECT * FROM shop_admins")) == 2


def test_second_and_third_shop_assignment_preserves_all_old_access_and_is_idempotent(network_api, database):
    client, _ = network_api
    before = database.query("SELECT * FROM shop_admins ORDER BY id")
    token = database.staff_token()
    second, third = new_shop(client), new_shop(client)
    for shop in (second, third):
        first = client.post(f"/superadmin/shops/{shop['shop_id']}/owner", headers=SUPERADMIN, json={"user_id": 1})
        repeated = client.post(f"/superadmin/shops/{shop['shop_id']}/owner", headers=SUPERADMIN, json={"user_id": 1})
        assert first.status_code == repeated.status_code == 200
        assert first.json() == repeated.json()
    assert database.query("SELECT * FROM shop_admins WHERE id IN (1,2) ORDER BY id") == before
    me = client.get("/barista/me", headers=bearer(token))
    assert me.status_code == 200
    assert {shop["shop_id"] for shop in me.json()["shops"]} == {1, second["shop_id"], third["shop_id"]}
    assert len(database.query("SELECT * FROM shop_admins WHERE user_id=1")) == 3
    assert database.db.get_app_session("legacy-active") is not None


def test_shop_create_and_existing_owner_assignment_are_atomic(network_api, database):
    client, _ = network_api
    response = client.post("/superadmin/shops", headers=SUPERADMIN,
                           json={"name": "Already connected", "owner_user_id": 1})
    assert response.status_code == 201, response.text
    assert response.json()["shop"]["owner_connected"] is True
    assert response.json()["shop"]["owner_user_id"] == 1
    count = len(database.query("SELECT * FROM coffee_shops"))
    for user_id, error in ((2, "OWNER_ACCESS_REQUIRED"), (99999, "USER_NOT_FOUND")):
        denied = client.post("/superadmin/shops", headers=SUPERADMIN,
                             json={"name": "Must roll back", "owner_user_id": user_id})
        assert denied.status_code == (404 if user_id == 99999 else 409)
        assert denied.json()["detail"]["code"] == error
    assert len(database.query("SELECT * FROM coffee_shops")) == count
    assert client.post("/superadmin/shops/1/owner", headers=SUPERADMIN,
                       json={"user_id": 1, "role": "admin"}).status_code == 422


def test_new_network_point_response_reports_inherited_owner_without_another_code(network_api, database):
    client, _ = network_api
    network = make_network(database)
    assigned = client.post(f"/superadmin/networks/{network['id']}/owner", headers=SUPERADMIN, json={"user_id": 1})
    assert assigned.status_code == 200, assigned.text
    created = client.post("/superadmin/shops", headers=SUPERADMIN,
                          json={"name": "Inherited owner", "network_id": network["id"]})
    assert created.status_code == 201, created.text
    shop = created.json()["shop"]
    detail = client.get(f"/superadmin/shops/{shop['shop_id']}", headers=SUPERADMIN).json()["shop"]
    for key in ("owner_connected", "owner_user_id", "owner_name", "owner_telegram_id", "telegram_connected"):
        assert shop[key] == detail[key]
    assert shop["owner_connected"] is True and shop["owner_user_id"] == 1
    assert database.query("SELECT role FROM shop_admins WHERE shop_id=%s AND user_id=1",
                          (shop["shop_id"],)) == [{"role": "owner"}]
    assert database.query("SELECT * FROM barista_admin_invites WHERE shop_id=%s", (shop["shop_id"],)) == []


def test_direct_assignment_does_not_replace_another_shop_owner(network_api, database):
    client, _ = network_api
    database.query("UPDATE shop_admins SET role='owner' WHERE user_id=3")
    response = client.post("/superadmin/shops/2/owner", headers=SUPERADMIN, json={"user_id": 1})
    assert response.status_code == 409 and response.json()["detail"]["code"] == "OWNER_ALREADY_CONNECTED"
    assert database.query("SELECT user_id,role FROM shop_admins WHERE shop_id=2") == [{"user_id": 3, "role": "owner"}]


def test_direct_assignment_rejects_disabled_shop_without_changing_access(network_api, database):
    client, _ = network_api
    database.query("UPDATE coffee_shops SET is_active=FALSE WHERE id=2")
    before = database.query("SELECT * FROM shop_admins ORDER BY id")
    response = client.post("/superadmin/shops/2/owner", headers=SUPERADMIN, json={"user_id": 1})
    assert response.status_code == 404
    assert database.query("SELECT * FROM shop_admins ORDER BY id") == before


def test_network_owner_assignment_preserves_original_staff_role_after_detach(network_api, database):
    client, _ = network_api
    database.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(2,1,'admin')")
    original = database.query("SELECT * FROM shop_admins WHERE shop_id=2 AND user_id=1")[0]
    network = make_network(database)
    attach(database, network["id"], 2)
    response = client.post(f"/superadmin/networks/{network['id']}/owner", headers=SUPERADMIN, json={"user_id": 1})
    assert response.status_code == 200, response.text
    assert database.query("SELECT * FROM shop_admins WHERE shop_id=2 AND user_id=1") == [original]
    token = database.staff_token()
    context = client.post("/barista/context", headers=bearer(token), json={"shop_id": 2})
    assert context.status_code == 200 and context.json()["selected_context"]["role"] == "owner"
    assert database.db.set_admin_active_shop(1001, 2) is True
    assert database.db.get_admin_shop_and_role(1001)["role"] == "owner"
    assert next(shop for shop in database.db.get_admin_shops_for_telegram(1001) if shop["id"] == 2)["role"] == "owner"
    request = client.post("/auth/admin-web-login/start", json={}).json()
    confirmation = client.post("/barista/web-login/confirm", headers=bearer(token), json={"code_or_token": request["code"]})
    assert confirmation.status_code == 200, confirmation.text
    web = client.post("/auth/admin-web-login/poll", json={"request_token": request["request_token"],
                                                        "poll_token": request["poll_token"]}).json()
    web_headers = bearer(web["access_token"])
    assert client.get("/admin-web/shops", headers=web_headers).json()["actor"]["role"] == "owner"

    detached = client.delete(f"/superadmin/networks/{network['id']}/shops/2", headers=SUPERADMIN)
    assert detached.status_code == 200, detached.text
    assert database.query("SELECT * FROM shop_admins WHERE shop_id=2 AND user_id=1") == [original]
    me = client.get("/barista/me", headers=bearer(token))
    assert me.status_code == 200 and me.json()["selected_context"]["role"] == "admin"
    assert database.db.get_admin_shop_and_role(1001)["role"] == "admin"
    assert client.get("/admin-web/shops", headers=web_headers).status_code == 403
    assert client.post("/barista/owner/invites", headers=bearer(token), json={}).status_code == 403


def test_same_owner_can_have_multiple_networks_and_an_independent_shop(network_api, database):
    client, _ = network_api
    first, second = make_network(database, "Network A"), make_network(database, "Network B")
    first_shop, second_shop = add_shop(database, "A"), add_shop(database, "B")
    attach(database, first["id"], first_shop); attach(database, second["id"], second_shop)
    token = database.staff_token()
    for network in (first, second):
        response = client.post(f"/superadmin/networks/{network['id']}/owner", headers=SUPERADMIN, json={"user_id": 1})
        assert response.status_code == 200, response.text
        repeated = client.post(f"/superadmin/networks/{network['id']}/owner", headers=SUPERADMIN, json={"user_id": 1})
        assert repeated.json() == response.json()
    me = client.get("/barista/me", headers=bearer(token)).json()
    assert {shop["shop_id"] for shop in me["shops"]} == {1, first_shop, second_shop}
    assert {network["network_id"] for network in me["networks"]} == {first["id"], second["id"]}
    assert len(database.query("SELECT * FROM network_memberships WHERE user_id=1")) == 2
    assert database.query("SELECT role,network_membership_id FROM shop_admins WHERE id=1") == [{"role": "owner", "network_membership_id": None}]


@pytest.mark.parametrize("existing_access", ["shop", "network"])
@pytest.mark.parametrize("invite_type", ["shop", "network"])
def test_stale_onboarding_token_cannot_claim_an_additional_owner_code(network_api, database, existing_access, invite_type):
    client, _ = network_api
    onboarding = login(client)
    user_id = onboarding["employee"]["user_id"]
    if invite_type == "shop":
        target_shop = new_shop(client)["shop_id"]
        invitation = new_invite(client, target_shop)
    else:
        network = make_network(database, "Target network")
        target_shop = add_shop(database, "Target point", network["id"])
        invitation = client.post(f"/superadmin/networks/{network['id']}/owner-invite", headers=SUPERADMIN, json={}).json()["invite"]
    if existing_access == "shop":
        owned = new_shop(client)["shop_id"]
        database.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(%s,%s,'owner')", (owned, user_id))
    else:
        owned_network = make_network(database, "Already owned network")
        database.query("INSERT INTO network_memberships(network_id,user_id,role) VALUES(%s,%s,'NETWORK_OWNER')", (owned_network["id"], user_id))
    response = claim(client, onboarding["access_token"], invitation["code"])
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "OWNER_ACCESS_ALREADY_EXISTS"
    assert database.query("SELECT * FROM shop_admins WHERE shop_id=%s AND user_id=%s", (target_shop, user_id)) == []
    assert database.query("SELECT used_at,closed_at FROM barista_admin_invites WHERE id=%s", (invitation["id"],)) == [{"used_at": None, "closed_at": None}]


def test_staff_invite_remains_available_after_owner_role_is_acquired(network_api, database):
    client, _ = network_api
    invitation = client.post("/barista/owner/invites", headers=bearer(database.staff_token()), json={}).json()["invite"]
    onboarding = login(client)
    user_id = onboarding["employee"]["user_id"]
    owned = new_shop(client)["shop_id"]
    database.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(%s,%s,'owner')", (owned, user_id))
    response = claim(client, onboarding["access_token"], invitation["code"])
    assert response.status_code == 200, response.text
    assert response.json()["selected_context"]["role"] == "admin"
    assert database.query("SELECT role FROM shop_admins WHERE shop_id=%s AND user_id=%s", (owned, user_id)) == [{"role": "owner"}]


def test_pending_onboarding_session_upgrades_when_superadmin_assigns_existing_owner(network_api, database):
    client, _ = network_api
    onboarding = login(client)
    user_id = onboarding["employee"]["user_id"]
    # Initial ownership was acquired on another device after this token was issued.
    database.query("INSERT INTO shop_admins(shop_id,user_id,role) VALUES(2,%s,'owner')", (user_id,))
    extra = new_shop(client)["shop_id"]
    assigned = client.post(f"/superadmin/shops/{extra}/owner", headers=SUPERADMIN, json={"user_id": user_id})
    assert assigned.status_code == 200, assigned.text
    me = client.get("/barista/me", headers=bearer(onboarding["access_token"]))
    assert me.status_code == 200, me.text
    assert me.json()["onboarding_required"] is False
    assert me.json()["session"]["purpose"] == "barista"
    assert {shop["shop_id"] for shop in me.json()["shops"]} == {2, extra}
    assert client.get("/barista/me", headers=bearer(onboarding["access_token"])).status_code == 200


def test_two_first_owner_codes_cannot_both_be_claimed_by_same_user(network_api, database):
    client, _ = network_api
    onboarding = login(client)
    first, second = new_shop(client), new_shop(client)
    codes = [new_invite(client, shop["shop_id"])["code"] for shop in (first, second)]
    from app.owner_onboarding import claim_invite
    from app.admin_invites import InviteError
    from app.barista_sessions import BaristaSessionError
    # Two outstanding onboarding tokens for the same identity, as on two devices.
    other_token = database.db.create_app_session(onboarding["employee"]["user_id"], purpose="barista_onboarding")
    def attempt(pair):
        try:
            claim_invite(token=pair[0], code=pair[1])
            return "claimed"
        except (InviteError, BaristaSessionError) as exc:
            return exc.code
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, zip((onboarding["access_token"], other_token), codes)))
    assert sorted(results) == ["OWNER_ACCESS_ALREADY_EXISTS", "claimed"]
    assert len(database.query("SELECT * FROM shop_admins WHERE user_id=%s", (onboarding["employee"]["user_id"],))) == 1
