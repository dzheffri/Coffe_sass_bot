"""Closed shop discovery/legacy loyalty checks on the guarded local DB only."""

import pytest
from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient

from conftest import PROJECT, extract_definitions
from test_barista_operations import operations_database


def test_closed_shop_hidden_from_client_discovery_without_deleting_balances(operations_database):
    db = operations_database
    db.query("ALTER TABLE coffee_shops ADD COLUMN city TEXT")
    db.query("ALTER TABLE users ADD COLUMN active_shop_id BIGINT REFERENCES coffee_shops(id)")
    db.query("INSERT INTO shop_clients(shop_id,user_id,cups,free_coffee_balance) VALUES(1,1,6,2),(2,1,3,1)")
    db.query("UPDATE users SET active_shop_id=1 WHERE id=1")
    balances = db.query("SELECT * FROM shop_clients ORDER BY id")
    db.query("UPDATE coffee_shops SET is_active=false WHERE id=1")
    app = FastAPI()
    namespace = {"app": app, "get_connection": db.db.get_connection,
                 "get_shop_profile": lambda owner_id: {"ok": False}}
    extract_definitions(PROJECT / "app/api/main.py", {"all_shops", "account_shops", "user_shops"}, namespace)
    with TestClient(app) as client:
        for path in ("/shops", "/account/1/shops", "/users/1001/shops"):
            response = client.get(path)
            assert response.status_code == 200
            assert [shop["shop_id"] for shop in response.json()["shops"]] == [2]
    assert [shop["id"] for shop in db.db.get_all_shops()] == [2]
    assert [shop["id"] for shop in db.db.get_user_shops(1001)] == [2]
    assert db.db.get_active_shop_for_user(1001) is None
    assert db.query("SELECT * FROM shop_clients ORDER BY id") == balances


def test_closed_shop_rejects_direct_legacy_loyalty_writes_without_ledger_changes(operations_database):
    db = operations_database
    db.query("INSERT INTO shop_clients(shop_id,user_id,cups,free_coffee_balance) VALUES(1,2,6,1),(2,2,4,2)")
    db.query("UPDATE coffee_shops SET is_active=false WHERE id=1")
    balances = db.query("SELECT * FROM shop_clients ORDER BY id")
    with pytest.raises(ValueError, match="^SHOP_CLOSED$"):
        db.db.add_cups_for_shop_client(1, 2, 1, 1)
    with pytest.raises(ValueError, match="^SHOP_CLOSED$"):
        db.db.redeem_free_for_shop_client(1, 2, 1)
    assert db.query("SELECT * FROM shop_clients ORDER BY id") == balances
    assert db.query("SELECT * FROM transactions") == []


def test_closed_shop_cannot_authorize_legacy_web_even_with_retained_membership(operations_database):
    db = operations_database
    namespace = {"get_connection": db.db.get_connection, "Header": Header,
                 "HTTPException": HTTPException,
                 "get_verified_telegram_actor": lambda *_: {"user_id": 1, "telegram_id": 1001}}
    extract_definitions(PROJECT / "app/api/main.py", {"require_owner_for_path"}, namespace)
    assert list(namespace["require_owner_for_path"](1001, "Bearer verified-existing", None))[0]["owner_shop_ids"] == [1]
    db.query("UPDATE coffee_shops SET is_active=false WHERE id=1")
    with pytest.raises(HTTPException) as failure:
        list(namespace["require_owner_for_path"](1001, "Bearer verified-existing", None))
    assert failure.value.status_code == 403
