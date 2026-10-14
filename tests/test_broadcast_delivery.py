"""Native app-push delivery decisions for shop broadcasts.

These tests isolate the delivery adapter from PostgreSQL and APNs.  Telegram
fallback is decided by the tri-state result; this module never sends a real
push or Telegram message.
"""

import asyncio
import sys
from types import ModuleType
from unittest.mock import AsyncMock, Mock

BROADCAST = {"broadcast_id": 73, "shop_id": 4, "shop_name": "Наші"}
RECIPIENT = {"user_id": 22, "telegram_user_id": 202}


def _delivery(monkeypatch, *, devices, push_result=None, lookup_error=None):
    from app import broadcast_delivery

    lookup = Mock(side_effect=lookup_error) if lookup_error else Mock(return_value=devices)
    push = AsyncMock(return_value=push_result)

    # The production adapter imports these two functions inside the async
    # operation so importing it never initializes DB/APNs configuration.  Use
    # module stubs for the call rather than creating production-level globals.
    db_module = ModuleType("app.db")
    db_module.get_app_push_devices_for_user = lookup
    push_module = ModuleType("app.app_push")
    push_module.send_app_pushes = push
    monkeypatch.setitem(sys.modules, "app.db", db_module)
    monkeypatch.setitem(sys.modules, "app.app_push", push_module)

    async def call_sync(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(broadcast_delivery, "run_in_threadpool", call_sync)
    return broadcast_delivery, lookup, push


def test_no_enabled_devices_returns_telegram_fallback_without_apns(monkeypatch):
    module, lookup, push = _delivery(monkeypatch, devices=[])

    result = asyncio.run(module.send_broadcast_app_push(BROADCAST, RECIPIENT))

    assert result is None
    lookup.assert_called_once_with(22)
    push.assert_not_awaited()


def test_all_permanently_invalid_devices_return_telegram_fallback(monkeypatch):
    devices = [
        {"device_token": "bad-one", "environment": "production"},
        {"device_token": "bad-two", "environment": "sandbox"},
    ]
    module, _, push = _delivery(
        monkeypatch,
        devices=devices,
        push_result={
            "sent": 0,
            "failed": 2,
            "errors": [
                {"status": 410, "reason": "Unregistered"},
                {"status": 400, "reason": "BadDeviceToken"},
            ],
        },
    )

    result = asyncio.run(module.send_broadcast_app_push(BROADCAST, RECIPIENT))

    assert result is None
    push.assert_awaited_once()
    assert push.await_args.kwargs["title"] == "Наші"
    assert push.await_args.kwargs["body"] == "Нове повідомлення від кав’ярні ☕️"
    assert push.await_args.kwargs["data"] == {"shop_id": 4, "broadcast_id": 73}


def test_any_success_returns_true_and_deduplicates_tokens(monkeypatch):
    devices = [
        {"device_token": " ABC ", "environment": "production"},
        {"device_token": "abc", "environment": "sandbox"},
        {"device_token": "DEF", "environment": "sandbox"},
    ]
    module, _, push = _delivery(
        monkeypatch,
        devices=devices,
        push_result={
            "sent": 1,
            "failed": 1,
            "errors": [{"status": 503, "reason": "ServiceUnavailable"}],
        },
    )

    result = asyncio.run(module.send_broadcast_app_push(BROADCAST, RECIPIENT))

    assert result is True
    push.assert_awaited_once()
    sent_devices = push.await_args.kwargs["devices"]
    assert [item["device_token"].strip().lower() for item in sent_devices] == ["abc", "def"]


def test_temporary_failure_returns_false_without_telegram_fallback(monkeypatch):
    module, _, push = _delivery(
        monkeypatch,
        devices=[{"device_token": "temporary", "environment": "production"}],
        push_result={
            "sent": 0,
            "failed": 1,
            "errors": [{"status": 503, "reason": "ServiceUnavailable"}],
        },
    )

    result = asyncio.run(module.send_broadcast_app_push(BROADCAST, RECIPIENT))

    assert result is False
    push.assert_awaited_once()


def test_lookup_error_returns_false_without_retry_or_push(monkeypatch):
    module, lookup, push = _delivery(
        monkeypatch,
        devices=None,
        lookup_error=RuntimeError("database unavailable"),
    )

    result = asyncio.run(module.send_broadcast_app_push(BROADCAST, RECIPIENT))

    assert result is False
    lookup.assert_called_once_with(22)
    push.assert_not_awaited()


def test_missing_user_returns_telegram_fallback_without_lookup(monkeypatch):
    module, lookup, push = _delivery(monkeypatch, devices=[])
    recipient = {"user_id": None, "telegram_user_id": 202}

    result = asyncio.run(module.send_broadcast_app_push(BROADCAST, recipient))

    assert result is None
    lookup.assert_not_called()
    push.assert_not_awaited()


def test_payload_does_not_include_telegram_or_private_fields(monkeypatch):
    module, _, push = _delivery(
        monkeypatch,
        devices=[{"device_token": "device", "environment": "production"}],
        push_result={"sent": 1, "failed": 0, "errors": []},
    )
    broadcast = {
        **BROADCAST,
        "text": "private caption",
        "recipients": [RECIPIENT],
        "sender_user_id": 999,
    }

    assert asyncio.run(module.send_broadcast_app_push(broadcast, RECIPIENT)) is True
    payload = push.await_args.kwargs
    assert payload["data"] == {"shop_id": 4, "broadcast_id": 73}
    assert "telegram_user_id" not in payload["data"]
    assert "text" not in payload["data"]
