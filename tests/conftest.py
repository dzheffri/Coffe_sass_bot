"""Isolated PostgreSQL tests. Never import backend DB/config startup code.

Only explicitly whitelisted function definitions are compiled from app/db.py
and app/api/main.py. Every connection targets a guarded, disposable local test
database; production environment files are never read.
"""

import ast
import hashlib
import importlib
import os
import secrets
import sys
import types
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.testclient import TestClient
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row


PROJECT = Path(__file__).resolve().parents[1]
TEST_ROOT = PROJECT.parent / ".barista-auth-tests"


def extract_definitions(path, names, namespace):
    """Execute definitions only: imports, assignments and init_db() are omitted."""
    source = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    definitions = [node for node in source.body
                   if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                   and node.name in names]
    found = {node.name for node in definitions}
    assert found == set(names), f"Missing test extraction definitions: {set(names) - found}"
    module = ast.Module(body=definitions, type_ignores=[])
    exec(compile(module, str(path), "exec"), namespace)


@pytest.fixture(scope="session")
def local_connection():
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.fail("Set TEST_DATABASE_URL to the disposable local barista_auth_tests database")
    settings = conninfo_to_dict(dsn)
    host = settings.get("host", "")
    socket = Path(host).resolve() if host.startswith("/") else None
    if (settings.get("dbname") != "barista_auth_tests"
            or settings.get("user") != "barista_test"
            or socket is None
            or not socket.is_relative_to(TEST_ROOT.resolve())
            or settings.get("port") != "55439"):
        pytest.fail("Refusing unsafe TEST_DATABASE_URL: only the isolated local test socket is allowed")

    state = {"schema": None}

    def connect():
        if state["schema"] is None:
            raise RuntimeError("No isolated test schema selected")
        return psycopg.connect(dsn, autocommit=True, row_factory=dict_row,
                               options=f"-c search_path={state['schema']}")

    return dsn, state, connect


@pytest.fixture(scope="session")
def safe_db(local_connection):
    _, _, connect = local_connection
    module = types.ModuleType("app.db")
    module.__file__ = str(PROJECT / "app/db.py")
    module.__dict__.update({
        "hashlib": hashlib, "secrets": secrets, "datetime": datetime,
        "timedelta": timedelta, "timezone": timezone,
        "APP_SESSION_TTL_DAYS": 30, "get_connection": connect,
    })
    extract_definitions(PROJECT / "app/db.py", {
        "utc_now", "_hash_app_session_token", "_hash_barista_session_token", "create_app_session",
        "get_app_session", "revoke_app_session", "revoke_all_user_sessions",
        "get_user_by_identity",
    }, module.__dict__)
    previous = sys.modules.get("app.db")
    sys.modules["app.db"] = module
    yield module
    if previous is None:
        sys.modules.pop("app.db", None)
    else:
        sys.modules["app.db"] = previous


LEGACY_SCHEMA = """
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    telegram_user_id BIGINT UNIQUE,
    username TEXT,
    full_name TEXT,
    personal_qr_token TEXT UNIQUE NOT NULL,
    selected_card_design TEXT DEFAULT 'basic'
);
CREATE TABLE coffee_shops (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE shop_admins (
    id BIGSERIAL PRIMARY KEY,
    shop_id BIGINT NOT NULL REFERENCES coffee_shops(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    UNIQUE (shop_id, user_id)
);
CREATE TABLE user_identities (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    provider_user_id TEXT NOT NULL,
    UNIQUE(provider, provider_user_id), UNIQUE(user_id, provider)
);
CREATE TABLE app_sessions (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ
);
INSERT INTO users(id, telegram_user_id, username, full_name, personal_qr_token)
VALUES (1, 1001, 'owner', 'Ольга', 'owner-qr'),
       (2, NULL, 'customer', 'Іван', 'customer-qr'),
       (3, 1003, 'other', 'Інший власник', 'other-qr');
INSERT INTO coffee_shops(id, name) VALUES (1, 'Наші'), (2, 'Інша кав’ярня');
INSERT INTO shop_admins(id, shop_id, user_id, role)
VALUES (1, 1, 1, 'owner'), (2, 2, 3, 'admin');
INSERT INTO user_identities(user_id, provider, provider_user_id)
VALUES (1, 'google', 'google-staff'), (1, 'apple', 'apple-staff'),
       (2, 'google', 'google-client');
SELECT setval('shop_admins_id_seq', 2);
"""


class DatabaseHarness:
    def __init__(self, db):
        self.db = db
        self.legacy_before = []

    def query(self, statement, values=()):
        with self.db.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, values)
                return cursor.fetchall() if cursor.description else []

    def session(self, token):
        return self.query("SELECT * FROM app_sessions WHERE token_hash IN (%s,%s)",
                          (self.db._hash_app_session_token(token),
                           self.db._hash_barista_session_token(token)))[0]

    def staff_token(self, user_id=1, membership_id=1):
        return self.db.create_app_session(
            user_id, purpose="barista", selected_membership_id=membership_id)

    def add_second_membership(self):
        return self.query(
            "INSERT INTO shop_admins(shop_id,user_id,role) VALUES (2,1,'admin') RETURNING id"
        )[0]["id"]


@pytest.fixture
def database(local_connection, safe_db):
    dsn, state, _ = local_connection
    schema = "auth_stage1_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
    state["schema"] = schema
    harness = DatabaseHarness(safe_db)
    try:
        with safe_db.get_connection() as connection:
            connection.execute(LEGACY_SCHEMA)
            for token, expiry, revoked in (
                ("legacy-active", datetime.now(timezone.utc) + timedelta(days=9), None),
                ("legacy-expired", datetime.now(timezone.utc) - timedelta(days=1), None),
                ("legacy-revoked", datetime.now(timezone.utc) + timedelta(days=9),
                 datetime.now(timezone.utc) - timedelta(hours=1)),
            ):
                connection.execute(
                    "INSERT INTO app_sessions(user_id,token_hash,expires_at,revoked_at) VALUES (1,%s,%s,%s)",
                    (safe_db._hash_app_session_token(token), expiry, revoked))
            harness.legacy_before = harness.query("SELECT * FROM app_sessions ORDER BY id")
            connection.execute((PROJECT / "migrations/20260930_barista_sessions.sql").read_text())
        yield harness
    finally:
        state["schema"] = None
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f"DROP SCHEMA {schema} CASCADE")


@pytest.fixture
def api(database, monkeypatch):
    monkeypatch.setenv("BARISTA_GOOGLE_CLIENT_ID", "nashi-google-ios")
    monkeypatch.setenv("BARISTA_APPLE_CLIENT_ID", "com.nashi.barista")
    monkeypatch.setenv("BARISTA_LOGIN_ENABLED", "true")
    app = FastAPI()
    namespace = {
        "app": app, "HTTPException": HTTPException, "Header": Header,
        "Depends": Depends, "get_app_session": database.db.get_app_session,
        "revoke_app_session": database.db.revoke_app_session,
        "account_qr": lambda user_id: {"user_id": user_id, "qr": "coffee:owner-qr"},
        "account_shops": lambda user_id: {"user_id": user_id, "shops": []},
        "account_stats": lambda user_id: {"user_id": user_id, "cups": 0},
    }
    extract_definitions(PROJECT / "app/api/main.py", {
        "_get_bearer_token_or_401", "get_current_user", "app_logout",
        "me", "me_qr", "me_shops", "me_stats",
    }, namespace)

    calls = []

    def verifier(provider):
        def verify(token, *, audience):
            calls.append((provider, token, audience))
            accepted = {"google": {"valid-staff": "google-staff",
                                   "valid-client": "google-client",
                                   "valid-unknown": "unknown-sub"},
                        "apple": {"valid-staff": "apple-staff"}}
            subject = accepted[provider].get(token)
            return {"sub": subject} if subject else None
        return verify

    router = importlib.import_module("app.api.barista")
    app.include_router(router.build_barista_router(
        verify_google=verifier("google"), verify_apple=verifier("apple"),
        find_user=database.db.get_user_by_identity,
        parse_bearer=namespace["_get_bearer_token_or_401"],
    ))
    with TestClient(app) as client:
        yield client, calls


def bearer(token):
    return {"Authorization": "Bearer " + token}
