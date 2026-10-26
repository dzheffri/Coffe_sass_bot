"""App-first notification routing for already published broadcast recipients."""

import logging

from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)

_PERMANENT_PUSH_ERRORS = {
    (400, "BadDeviceToken"),
    (400, "DeviceTokenNotForTopic"),
    (410, "Unregistered"),
}


async def send_broadcast_app_push(broadcast, recipient, *, audit=None) -> bool | None:
    """True: sent; None: Telegram fallback; False: defer without duplication.

    The existing sender deletes permanently invalid tokens. Publication and
    its recipient snapshot remain intact regardless of notification delivery.
    """
    from app.app_push import send_app_pushes
    from app.db import get_app_push_devices_for_user

    user_id = recipient.get("user_id")
    if not user_id:
        return None
    try:
        devices = await run_in_threadpool(get_app_push_devices_for_user, user_id)
        unique_devices = []
        seen = set()
        for device in devices or []:
            token = (device.get("device_token") or "").strip().lower()
            if token and token not in seen:
                seen.add(token)
                unique_devices.append(dict(device, device_token=token))
        if audit is not None:
            audit.update(attempted=0,sent=0,failed=0)
        if not unique_devices:
            return None

        result = await send_app_pushes(
            devices=unique_devices,
            title=broadcast["shop_name"],
            body="Нове повідомлення від кав’ярні ☕️",
            data={"shop_id": broadcast["shop_id"], "broadcast_id": broadcast["broadcast_id"]},
        )
        sent = result.get("sent", 0)
        failed = result.get("failed", 0)
        if audit is not None:
            audit.update(attempted=len(unique_devices),sent=int(sent),failed=int(failed))
        errors = result.get("errors", [])
        invalid = sum((error.get("status"), error.get("reason")) in _PERMANENT_PUSH_ERRORS
                      for error in errors)
        if sent > 0:
            delivered, outcome = True, "sent"
        elif failed == len(unique_devices) and len(errors) == len(unique_devices) and invalid == len(unique_devices):
            delivered, outcome = None, "all_tokens_invalid telegram_fallback"
        else:
            delivered, outcome = False, "temporary_or_unknown_failure"
        # Never log the raw sender errors: network exception messages may
        # include the APNs URL, which contains a device token.
        logger.info("[broadcast][APP PUSH] broadcast_id=%s user_id=%s devices=%s sent=%s failed=%s invalid=%s outcome=%s",
                    broadcast["broadcast_id"], user_id, len(unique_devices), sent, failed, invalid, outcome)
        return delivered
    except Exception as exc:
        if audit is not None:
            audit['error_class'] = type(exc).__name__
        logger.warning("[broadcast][APP PUSH] broadcast_id=%s user_id=%s error=%s",
                       broadcast.get("broadcast_id"), user_id, type(exc).__name__)
        return False
