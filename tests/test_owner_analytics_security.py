"""Exhaustive routes and real startup isolated owner analytics security checks."""

import ast
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import uuid

import psycopg
from psycopg import sql
import pytest

from conftest import TEST_ROOT


PROJECT = Path(__file__).resolve().parents[1]
EXPECTED_ROUTES = {
    f"/owner/analytics/{{owner_telegram_id}}/{suffix}": f"owner_analytics_{suffix}"
    for suffix in ("overview", "activity", "clients", "details")
}


def test_all_owner_analytics_http_routes_use_verified_owner_context():
    routes = {}
    for source in (PROJECT / "app").rglob("*.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                if (not isinstance(decorator, ast.Call) or not decorator.args
                        or not isinstance(decorator.func, ast.Attribute)
                        or not isinstance(decorator.args[0], ast.Constant)):
                    continue
                path = decorator.args[0].value
                if not isinstance(path, str) or not path.startswith("/owner/analytics/"):
                    continue
                assert decorator.func.attr == "get", (source, path)
                assert path not in routes, path
                routes[path] = node.name
                assert source == PROJECT / "app/api/main.py"
                assert any(
                    isinstance(default, ast.Call)
                    and isinstance(default.func, ast.Name)
                    and default.func.id == "Depends"
                    and len(default.args) == 1
                    and isinstance(default.args[0], ast.Name)
                    and default.args[0].id == "require_owner_analytics_context"
                    for default in node.args.defaults
                ), f"Unprotected legacy analytics route: {path}"
    assert routes == EXPECTED_ROUTES


@pytest.fixture(scope="module")
def owner_analytics_audit(local_connection):
    dsn, _, _ = local_connection  # Existing strict local-only socket/database guard.
    schema = "owner_analytics_" + uuid.uuid4().hex
    audit_root = Path(tempfile.mkdtemp(prefix="owner_analytics_", dir=TEST_ROOT))
    environment = {
        "PATH": os.environ.get("PATH", ""), "TEST_DATABASE_URL": dsn,
        "PYTHONDONTWRITEBYTECODE": "1", "PYTHON_DOTENV_DISABLED": "1",
    }
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        result = subprocess.run([
            sys.executable, str(PROJECT / "tests/owner_analytics_worker.py"),
            str(PROJECT), schema, str(audit_root),
        ], cwd=audit_root, env=environment, capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stdout + result.stderr
        yield json.loads(result.stdout.strip().splitlines()[-1])
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        shutil.rmtree(audit_root)


@pytest.mark.parametrize("check", [
    "real_backend_and_bot_startup",
    "complete_legacy_analytics_inventory",
    "owned_shop_list_authentication_and_isolation",
    "overview_authentication_explicit_AB_context_C_denied",
    "activity_authentication_explicit_AB_context_C_denied",
    "clients_authentication_explicit_AB_context_C_denied",
    "details_authentication_explicit_AB_context_C_denied",
    "existing_sessions_preserved_and_client_staff_purpose_isolation",
])
def test_real_owner_analytics_explicit_context_and_security(owner_analytics_audit, check):
    assert owner_analytics_audit[check] is True
