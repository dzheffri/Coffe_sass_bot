"""Isolated real-import bot checks; cannot reuse another test's DB modules."""

from pathlib import Path
import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize("scenario", [
    "test_deep_link_calls_existing_flow_with_identical_state_and_prompt",
    "test_real_start_router_and_fsm_accept_barista_support",
    "test_ordinary_and_unrecognized_start_keep_existing_user_panel_flow",
    "test_verified_telegram_link_start_is_preserved",
    "test_existing_support_delivery_and_state_clear_are_preserved",
    "test_non_support_admin_cannot_reply",
    "test_support_admin_reply_and_cancel_remain_unchanged",
])
def test_real_barista_support_regression(scenario):
    project = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-m", "tests.bot_support_worker", "BotSupportTests." + scenario],
        cwd=project,
        env={"PATH": os.environ.get("PATH", ""), "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
