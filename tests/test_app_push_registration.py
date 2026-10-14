"""Local-only tests for atomic APNs device registration and opt-out lookup.

The production DB module is loaded through the guarded AST helper, so importing
this file never runs backend startup or reads a production environment file.
"""

import types
from pathlib import Path

import psycopg
import pytest
from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel
from psycopg.rows import dict_row

from conftest import extract_definitions


PROJECT = Path(__file__).resolve().parents[1]


@pytest.fixture
def push_database(local_connection):
    dsn, state, connect = local_connection
    schema = "push_registration_" + __import__("uuid").uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
    state["schema"] = schema

    module = types.ModuleType("offline_push_db")
    module.__dict__["get_connection"] = connect
    extract_definitions(PROJECT / "app/db.py", {
        "register_app_push_device", "set_app_push_device_enabled",
        "unregister_app_push_device", "get_app_push_devices_for_user",
    }, module.__dict__)

    try:
        with connect() as connection:
            connection.execute("""
                CREATE TABLE users (
                    id BIGINT PRIMARY KEY
                );
                CREATE TABLE app_push_devices (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    device_token TEXT NOT NULL UNIQUE,
                    platform TEXT NOT NULL DEFAULT 'ios',
                    environment TEXT NOT NULL DEFAULT 'production'
                        CHECK (environment IN ('sandbox', 'production')),
                    notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                INSERT INTO users(id) VALUES (1), (2);
            """)
        yield module, connect
    finally:
        state["schema"] = None
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f"DROP SCHEMA {schema} CASCADE")


def rows(connect, query, values=()):
    with connect() as connection:
        return connection.execute(query, values).fetchall()


def test_registration_upsert_is_atomic_and_disabled_devices_are_not_selected(push_database):
    db, connect = push_database

    first = db.register_app_push_device(1, " ABC ", environment="production", notifications_enabled=True)
    assert first["user_id"] == 1
    assert first["device_token"] == "abc"
    assert db.get_app_push_devices_for_user(1) == [{"device_token": "abc", "environment": "production"}]

    rebound = db.register_app_push_device(
        2, "abc", environment="sandbox", notifications_enabled=False
    )
    assert rebound["user_id"] == 2
    assert rebound["notifications_enabled"] is False
    assert db.get_app_push_devices_for_user(1) == []
    assert db.get_app_push_devices_for_user(2) == []
    assert rows(connect, "SELECT COUNT(*) AS count FROM app_push_devices") == [{"count": 1}]

    enabled = db.set_app_push_device_enabled(" ABC ", True)
    assert enabled["user_id"] == 2 and enabled["environment"] == "sandbox"
    assert db.get_app_push_devices_for_user(2) == [
        {"device_token": "abc", "environment": "sandbox"}
    ]


def test_rebinding_existing_token_never_leaves_a_stale_user_row(push_database):
    db, connect = push_database
    db.register_app_push_device(1, "shared", notifications_enabled=True)
    db.register_app_push_device(2, "shared", notifications_enabled=True)

    assert db.get_app_push_devices_for_user(1) == []
    assert db.get_app_push_devices_for_user(2) == [
        {"device_token": "shared", "environment": "production"}
    ]
    assert rows(connect, "SELECT user_id FROM app_push_devices WHERE device_token='shared'") == [
        {"user_id": 2}
    ]


def test_authenticated_me_registration_uses_session_user_and_persists_opt_out(push_database):
    db, connect = push_database

    class AppPushDeviceRequest(BaseModel):
        device_token: str
        environment: str = "production"
        notifications_enabled: bool = True

    class AppPushDeviceRemoveRequest(BaseModel):
        device_token: str

    app = FastAPI()

    def current_user():
        return {"user_id": 2}

    namespace = {
        "app": app,
        "HTTPException": HTTPException,
        "Depends": Depends,
        "AppPushDeviceRequest": AppPushDeviceRequest,
        "AppPushDeviceRemoveRequest": AppPushDeviceRemoveRequest,
        "get_current_user": current_user,
        "get_connection": connect,
        "register_app_push_device": db.register_app_push_device,
        "unregister_app_push_device": db.unregister_app_push_device,
        "get_app_push_devices_for_user": db.get_app_push_devices_for_user,
    }
    extract_definitions(PROJECT / "app/api/main.py", {
        "register_user_push_token", "register_my_push_token",
    }, namespace)
    app.post("/me/push-token")(namespace["register_my_push_token"])

    with TestClient(app) as client:
        response = client.post("/me/push-token", json={
            "device_token": "current-user-device",
            "notifications_enabled": False,
        })

    assert response.status_code == 200, response.text
    assert response.json() == {
        "ok": True,
        "user_id": 2,
        "environment": "production",
        "notifications_enabled": False,
    }
    assert rows(connect, "SELECT user_id,notifications_enabled FROM app_push_devices") == [
        {"user_id": 2, "notifications_enabled": False}
    ]

