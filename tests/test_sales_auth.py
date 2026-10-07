"""Offline sales auth checks plus optional guarded local startup integration."""

import ast
import hashlib
import hmac
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode

import psycopg
from psycopg import sql
import pytest
from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient

from app.api.sales import build_sales_router
from conftest import PROJECT, TEST_ROOT, extract_definitions


TEST_BOT_TOKEN = "123456789:" + "A" * 35


def signed_init_data(telegram_id, *, age=0, forged=False):
    data = {
        "auth_date": str(int(time.time()) - age),
        "user": json.dumps({"id": telegram_id}),
    }
    checked = "\n".join(f"{key}={value}" for key, value in sorted(data.items()))
    key = hmac.new(b"WebAppData", TEST_BOT_TOKEN.encode(), hashlib.sha256).digest()
    data["hash"] = "0" * 64 if forged else hmac.new(
        key, checked.encode(), hashlib.sha256,
    ).hexdigest()
    return urlencode(data)


@pytest.fixture
def sales_client():
    tree = ast.parse((PROJECT / "app/api/main.py").read_text(encoding="utf-8"))
    allowlist = next(
        ast.literal_eval(node.value) for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "SUPERADMIN_TELEGRAM_IDS"
                for target in node.targets)
    )
    superadmin_id = next(iter(allowlist))
    ordinary_id = next(value for value in range(81001, 81100) if value not in allowlist)
    sessions = {
        "superadmin": {"user_id": 41, "telegram_user_id": superadmin_id},
        "ordinary": {"user_id": 42, "telegram_user_id": ordinary_id},
        "unlinked": {"user_id": 43, "telegram_user_id": None},
    }
    users = {
        str(superadmin_id): {"id": 41},
        str(ordinary_id): {"id": 42},
    }
    namespace = {
        "Header": Header, "HTTPException": HTTPException,
        "get_app_session": sessions.get,
        "get_user_by_identity": lambda provider, subject: users.get(subject),
        "BOT_TOKEN": TEST_BOT_TOKEN, "datetime": datetime, "timezone": timezone,
        "parse_qsl": parse_qsl, "json": json, "hmac": hmac, "hashlib": hashlib,
    }
    # Execute the real proof/actor functions without imports or startup writes.
    extract_definitions(PROJECT / "app/api/main.py", {
        "_get_bearer_token_or_401", "get_current_user",
        "get_verified_telegram_actor", "validate_telegram_init_data",
    }, namespace)
    app = FastAPI()
    app.include_router(build_sales_router(
        verify_actor=namespace["get_verified_telegram_actor"],
        superadmin_telegram_ids=allowlist,
    ))
    with TestClient(app) as client:
        yield client, superadmin_id, ordinary_id


def test_existing_superadmin_bearer_and_signed_miniapp(sales_client):
    client, superadmin_id, _ = sales_client
    for headers in (
        {"Authorization": "Bearer superadmin"},
        {"X-Telegram-Init-Data": signed_init_data(superadmin_id)},
    ):
        response = client.get("/sales/me", headers=headers)
        assert response.status_code == 200
        assert response.json() == {"allowed": True, "user_id": 41, "role": "superadmin"}
        assert response.headers["Cache-Control"] == "no-store"
        assert response.headers["Vary"] == "Authorization, X-Telegram-Init-Data"


@pytest.mark.parametrize("headers", [
    {},
    {"Authorization": "superadmin"},
    {"Authorization": "Bearer "},
    {"Authorization": "Basic superadmin"},
    {"Authorization": "Bearer unknown"},
    {"X-Telegram-Init-Data": "user=%7B%22id%22%3A566408696%7D"},
    {"X-Telegram-Id": "566408696", "X-Role": "superadmin", "X-Is-Superadmin": "true"},
])
def test_missing_invalid_and_unsigned_auth_is_401(sales_client, headers):
    client, _, _ = sales_client
    assert client.get("/sales/me", headers=headers).status_code == 401


@pytest.mark.parametrize("age,forged", [(86401, False), (0, True), (-120, False)])
def test_expired_forged_or_future_miniapp_is_401(sales_client, age, forged):
    client, superadmin_id, _ = sales_client
    headers = {"X-Telegram-Init-Data": signed_init_data(superadmin_id, age=age, forged=forged)}
    assert client.get("/sales/me", headers=headers).status_code == 401


def test_ordinary_actor_and_frontend_role_claims_are_403(sales_client):
    client, _, ordinary_id = sales_client
    for headers in (
        {"Authorization": "Bearer ordinary"},
        {"X-Telegram-Init-Data": signed_init_data(ordinary_id)},
        {"Authorization": "Bearer unlinked"},
    ):
        forged_headers = {**headers, "X-Role": "superadmin", "X-Is-Superadmin": "true"}
        response = client.request(
            "GET", "/sales/me?role=superadmin&is_superadmin=true&user_id=41",
            headers=forged_headers,
            json={"telegram_id": 566408696, "user_id": 41, "role": "superadmin"},
        )
        assert response.status_code == 403


def test_signed_actor_requires_existing_linked_user(sales_client):
    client, _, _ = sales_client
    headers = {"X-Telegram-Init-Data": signed_init_data(89999)}
    assert client.get("/sales/me", headers=headers).status_code == 403


def test_invalid_bearer_cannot_fall_back_to_miniapp(sales_client):
    client, superadmin_id, _ = sales_client
    headers = {
        "Authorization": "Bearer invalid",
        "X-Telegram-Init-Data": signed_init_data(superadmin_id),
    }
    assert client.get("/sales/me", headers=headers).status_code == 401


def test_verified_bearer_actor_takes_precedence_over_other_launch_data(sales_client):
    client, superadmin_id, _ = sales_client
    headers = {
        "Authorization": "Bearer ordinary",
        "X-Telegram-Init-Data": signed_init_data(superadmin_id),
    }
    assert client.get("/sales/me", headers=headers).status_code == 403


def test_superadmin_cannot_supply_actor_query_parameters(sales_client):
    client, _, _ = sales_client
    response = client.get("/sales/me?user_id=42", headers={"Authorization": "Bearer superadmin"})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "UNEXPECTED_PARAMETER"


def test_real_startup_and_session_lifecycle(local_connection):
    dsn, _, _ = local_connection  # Strict disposable DB/socket guard applies first.
    schema = "full_security_sales_" + uuid.uuid4().hex
    audit_root = Path(tempfile.mkdtemp(prefix="sales_auth_", dir=TEST_ROOT))
    environment = {
        "PATH": os.environ.get("PATH", ""), "TEST_DATABASE_URL": dsn,
        "PYTHONDONTWRITEBYTECODE": "1", "PYTHON_DOTENV_DISABLED": "1",
        "BARISTA_LOGIN_ENABLED": "false", "SUPER_ADMIN_IDS": "81001",
    }
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        result = subprocess.run([
            sys.executable, str(PROJECT / "tests/sales_auth_worker.py"),
            str(PROJECT), schema, str(audit_root),
        ], cwd=audit_root, env=environment, capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stdout + result.stderr
        report = json.loads(result.stdout.strip().splitlines()[-1])
        assert report == {
            "real_startup_and_sales_router": True,
            "both_verified_auth_mechanisms": True,
            "ordinary_and_forged_claims_denied": True,
            "dead_and_barista_sessions_denied": True,
            "no_sales_storage_or_business_changes": True,
        }
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        shutil.rmtree(audit_root)
