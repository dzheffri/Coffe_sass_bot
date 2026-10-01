"""Regression audit through real run.py/main.py imports, never extracted code.

The existing narrow tests remain useful for races. This module additionally
executes complete old/new startup and real HTTP dependencies against one
disposable schema on the whitelisted local PostgreSQL socket.
"""

import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import uuid

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import pytest

from conftest import PROJECT, TEST_ROOT


LEGACY_BACKEND_REVISION = "50cbfc5be3af00260fe1ca888c5e805667c79b66"


MIGRATIONS = (
    "20260930_barista_sessions.sql",
    "20260930_barista_sessions_index.sql",
    "20260930_barista_sessions_validate.sql",
)


@pytest.fixture(scope="module")
def full_backend_audit(local_connection):
    dsn, _, _ = local_connection  # Applies the strict DB/socket guard first.
    schema = "full_security_" + uuid.uuid4().hex
    root = Path(tempfile.mkdtemp(prefix="full_security_", dir=TEST_ROOT))
    old = root / "old_backend"
    old.mkdir()
    baseline = subprocess.run(["git", "archive", LEGACY_BACKEND_REVISION], cwd=PROJECT,
                              capture_output=True, check=True).stdout
    with tarfile.open(fileobj=io.BytesIO(baseline)) as archive:
        for member in archive.getmembers():
            assert not Path(member.name).is_absolute() and ".." not in Path(member.name).parts
            assert not member.issym() and not member.islnk()
        archive.extractall(old)

    environment = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHON_DOTENV_DISABLED": "1",
        "TEST_DATABASE_URL": dsn,
    }
    report = {}

    def worker(source, phase):
        result = subprocess.run([
            sys.executable, str(PROJECT / "tests/full_backend_worker.py"),
            str(source), phase, schema, str(root),
        ], cwd=root, env=environment, capture_output=True, text=True, timeout=90)
        assert result.returncode == 0, f"{phase}:\n{result.stdout}\n{result.stderr}"
        current = json.loads(result.stdout.strip().splitlines()[-1])
        report.update(current)

    def connect():
        return psycopg.connect(dsn, autocommit=True, row_factory=dict_row,
                                options=f"-c search_path={schema}")

    def migration(filename):
        # psql dispatches individual statements; execute(multi_sql) would make
        # CREATE INDEX CONCURRENTLY enter an implicit transaction block.
        psql = shutil.which("psql")
        assert psql, "Full online-migration audit requires local psql"
        result = subprocess.run([
            psql, "-X", "-v", "ON_ERROR_STOP=1", "--dbname", dsn,
            "--file", str(PROJECT / "migrations" / filename),
        ], env={**environment, "PGOPTIONS": f"-c search_path={schema}"},
            capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stdout + result.stderr

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        worker(old, "legacy_setup")
        before = json.loads((root / "state.json").read_text())["before"]
        migration(MIGRATIONS[0])
        with connect() as conn:
            assert not any(row["convalidated"] for row in conn.execute("""
                SELECT convalidated FROM pg_constraint
                WHERE conrelid='app_sessions'::regclass
                AND conname IN ('app_sessions_purpose_check',
                    'app_sessions_context_check','app_sessions_selected_membership_fkey')
            """))
        migration(MIGRATIONS[1])
        migration(MIGRATIONS[2])
        with connect() as conn:
            after = conn.execute("""SELECT id,user_id,token_hash,created_at,
                expires_at,last_used_at,revoked_at FROM app_sessions ORDER BY id""").fetchall()
            assert json.loads(json.dumps(after, default=str)) == before
            assert all(row["purpose"] == "client" and row["selected_membership_id"] is None
                for row in conn.execute("SELECT purpose,selected_membership_id FROM app_sessions"))
            assert all(row["convalidated"] for row in conn.execute("""
                SELECT convalidated FROM pg_constraint
                WHERE conrelid='app_sessions'::regclass
                AND conname IN ('app_sessions_purpose_check',
                    'app_sessions_context_check','app_sessions_selected_membership_fkey')
            """))
            index = conn.execute("""SELECT indisvalid,indisready FROM pg_index
                WHERE indexrelid='idx_app_sessions_selected_membership_id'::regclass""").fetchone()
            assert index["indisvalid"] and index["indisready"]
        for filename in MIGRATIONS:
            migration(filename)
        with connect() as conn:
            after_repeat = conn.execute("""SELECT id,user_id,token_hash,created_at,
                expires_at,last_used_at,revoked_at FROM app_sessions ORDER BY id""").fetchall()
            assert json.loads(json.dumps(after_repeat, default=str)) == before
        report["online_migration_preserves_exact_legacy_state_and_repeats"] = True
        worker(old, "legacy_post_migration")
        worker(PROJECT, "current_security")
        worker(old, "legacy_after_issuance")
        yield report
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        shutil.rmtree(root)


@pytest.mark.parametrize("check", [
    "full_import_run_and_main",
    "online_migration_preserves_exact_legacy_state_and_repeats",
    "legacy_client_routes_and_dead_sessions",
    "ordinary_identity_check_login_create_preserved",
    "cannot_link_identity_to_foreign_telegram",
    "cannot_confirm_foreign_telegram_link_and_valid_flow_works",
    "verified_telegram_http_confirmation_works",
    "cannot_self_grant_admin_and_authorized_owner_flows_work",
    "cannot_unlink_foreign_identity_and_own_unlink_works",
    "session_purpose_isolation_real_routes_and_issuance_gate",
    "unmodified_old_backend_rejects_barista",
    "real_bot_private_actor_binding_and_confirmation",
    "real_bot_existing_merge_preserves_balance_and_revokes_source",
])
def test_full_real_backend_security_and_rollout(full_backend_audit, check):
    assert full_backend_audit[check] is True
