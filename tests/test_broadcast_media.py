"""Run existing real-import media handlers apart from narrow DB/router fixtures."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


APNS_TRIPWIRE_WORKER = """
import sys
from types import ModuleType
import unittest
from unittest.mock import AsyncMock

push = ModuleType("app.app_push")
push.send_app_push = AsyncMock(side_effect=AssertionError("Broadcast called APNs"))
push.send_app_pushes = AsyncMock(side_effect=AssertionError("Broadcast called APNs"))
sys.modules["app.app_push"] = push

from tests.broadcast_media_worker import BroadcastMediaTests

suite = unittest.TestSuite([BroadcastMediaTests(sys.argv[1])])
result = unittest.TextTestRunner(verbosity=2).run(suite)
push.send_app_push.assert_not_called()
push.send_app_pushes.assert_not_called()
sys.exit(not result.wasSuccessful())
"""


@pytest.mark.parametrize("scenario", [
    "test_ordinary_owner_cannot_start_or_send_global_broadcast",
    "test_global_media_preserves_original_and_existing_recipient_dedup",
    "test_owner_preview_preserves_media_and_caption_without_overrides",
    "test_owner_confirmation_uses_only_server_shop_and_saved_original",
    "test_owner_confirmation_without_original_ids_cannot_send",
    "test_owner_confirmation_cannot_send_to_closed_shop",
    "test_owner_confirmation_rechecks_role_before_recipient_query",
])
def test_real_broadcast_media_regression(scenario):
    project = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-c", APNS_TRIPWIRE_WORKER, scenario],
        cwd=project,
        env={"PATH": os.environ.get("PATH", ""), "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
