import os
import time
import json
import base64

import httpx

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

from app.db import remove_invalid_app_push_token


APNS_TEAM_ID = os.getenv("APNS_TEAM_ID", "").strip()
APNS_KEY_ID = os.getenv("APNS_KEY_ID", "").strip()
APNS_BUNDLE_ID = os.getenv(
    "APNS_BUNDLE_ID",
    "com.dzheffri.coffeeclubpass",
).strip()
APNS_AUTH_KEY_BASE64 = os.getenv("APNS_AUTH_KEY_BASE64", "").strip()

APNS_PRODUCTION_URL = "https://api.push.apple.com"
APNS_SANDBOX_URL = "https://api.sandbox.push.apple.com"

_cached_jwt = {
    "token": None,
    "created_at": 0,
}


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _load_apns_private_key():
    if not APNS_AUTH_KEY_BASE64:
        raise RuntimeError("APNS_AUTH_KEY_BASE64 is not configured")

    try:
        key_bytes = base64.b64decode(APNS_AUTH_KEY_BASE64)
    except Exception as exc:
        raise RuntimeError("Invalid APNS_AUTH_KEY_BASE64") from exc

    try:
        return serialization.load_pem_private_key(
            key_bytes,
            password=None,
        )
    except Exception as exc:
        raise RuntimeError("Unable to load APNs .p8 private key") from exc


def _make_apns_jwt() -> str:
    now = int(time.time())

    if (
        _cached_jwt["token"]
        and now - _cached_jwt["created_at"] < 50 * 60
    ):
        return _cached_jwt["token"]

    if not APNS_TEAM_ID:
        raise RuntimeError("APNS_TEAM_ID is not configured")

    if not APNS_KEY_ID:
        raise RuntimeError("APNS_KEY_ID is not configured")

    header = {
        "alg": "ES256",
        "kid": APNS_KEY_ID,
    }

    payload = {
        "iss": APNS_TEAM_ID,
        "iat": now,
    }

    header_encoded = _b64url(
        json.dumps(
            header,
            separators=(",", ":"),
        ).encode("utf-8")
    )

    payload_encoded = _b64url(
        json.dumps(
            payload,
            separators=(",", ":"),
        ).encode("utf-8")
    )

    signing_input = (
        f"{header_encoded}.{payload_encoded}"
    ).encode("ascii")

    private_key = _load_apns_private_key()

    if not isinstance(
        private_key,
        ec.EllipticCurvePrivateKey,
    ):
        raise RuntimeError(
            "APNs auth key is not an EC private key"
        )

    der_signature = private_key.sign(
        signing_input,
        ec.ECDSA(hashes.SHA256()),
    )

    r, s = decode_dss_signature(der_signature)

    raw_signature = (
        r.to_bytes(32, "big")
        + s.to_bytes(32, "big")
    )

    token = (
        f"{header_encoded}."
        f"{payload_encoded}."
        f"{_b64url(raw_signature)}"
    )

    _cached_jwt["token"] = token
    _cached_jwt["created_at"] = now

    return token


def _apns_base_url(environment: str) -> str:
    if environment == "sandbox":
        return APNS_SANDBOX_URL
    return APNS_PRODUCTION_URL


def _payload(
    title: str,
    body: str,
    data: dict | None = None,
    badge: int | None = None,
) -> dict:
    aps = {
        "alert": {
            "title": title,
            "body": body,
        },
        "sound": "default",
    }

    if badge is not None:
        aps["badge"] = badge

    payload = {"aps": aps}

    if data:
        for key, value in data.items():
            if key != "aps":
                payload[key] = value

    return payload


async def send_app_push(
    device_token: str,
    title: str,
    body: str,
    environment: str = "production",
    data: dict | None = None,
    badge: int | None = None,
):
    clean_token = (device_token or "").strip().lower()

    if not clean_token:
        return {
            "ok": False,
            "status": 0,
            "reason": "EmptyDeviceToken",
        }

    jwt_token = _make_apns_jwt()

    url = (
        f"{_apns_base_url(environment)}"
        f"/3/device/{clean_token}"
    )

    headers = {
        "authorization": f"bearer {jwt_token}",
        "apns-topic": APNS_BUNDLE_ID,
        "apns-push-type": "alert",
        "apns-priority": "10",
    }

    async with httpx.AsyncClient(
        http2=True,
        timeout=15.0,
    ) as client:
        response = await client.post(
            url,
            headers=headers,
            json=_payload(
                title=title,
                body=body,
                data=data,
                badge=badge,
            ),
        )

    reason = None

    if response.content:
        try:
            reason = response.json().get("reason")
        except Exception:
            reason = response.text

    if response.status_code == 200:
        return {
            "ok": True,
            "status": 200,
            "reason": None,
        }

    if reason in {
        "BadDeviceToken",
        "DeviceTokenNotForTopic",
        "Unregistered",
    }:
        try:
            remove_invalid_app_push_token(
                clean_token
            )
        except Exception as exc:
            print(
                "APP PUSH TOKEN CLEANUP ERROR:",
                repr(exc),
            )

    return {
        "ok": False,
        "status": response.status_code,
        "reason": reason,
    }


async def send_app_pushes(
    devices: list,
    title: str,
    body: str,
    data: dict | None = None,
    badge: int | None = None,
):
    sent = 0
    failed = 0
    errors = []

    for device in devices:
        if isinstance(device, dict):
            device_token = device.get("device_token")
            environment = (
                device.get("environment")
                or "production"
            )
        else:
            device_token = str(device)
            environment = "production"

        try:
            result = await send_app_push(
                device_token=device_token,
                title=title,
                body=body,
                environment=environment,
                data=data,
                badge=badge,
            )

            if result.get("ok"):
                sent += 1
            else:
                failed += 1
                errors.append({
                    "device_token": (
                        f"{str(device_token)[:8]}..."
                        if device_token
                        else ""
                    ),
                    "status": result.get("status"),
                    "reason": result.get("reason"),
                })

        except Exception as exc:
            failed += 1
            errors.append({
                "device_token": (
                    f"{str(device_token)[:8]}..."
                    if device_token
                    else ""
                ),
                "status": 0,
                "reason": repr(exc),
            })

    return {
        "sent": sent,
        "failed": failed,
        "errors": errors,
    }
