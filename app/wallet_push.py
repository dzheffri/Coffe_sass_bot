import base64
import os
import tempfile

import httpx

from cryptography.hazmat.primitives.serialization import (
    pkcs12,
    Encoding,
    PrivateFormat,
    NoEncryption,
)


WALLET_P12_BASE64 = os.getenv("WALLET_P12_BASE64", "")
WALLET_P12_PASSWORD = os.getenv("WALLET_P12_PASSWORD", "")
WALLET_PASS_TYPE_ID = os.getenv(
    "WALLET_PASS_TYPE_ID",
    "pass.com.dzheffri.coffeeclub",
)

APNS_URL = "https://api.push.apple.com"


def _create_apns_pem_files():
    if not WALLET_P12_BASE64:
        raise RuntimeError("WALLET_P12_BASE64 is missing")

    p12_data = base64.b64decode(WALLET_P12_BASE64)

    password = (
        WALLET_P12_PASSWORD.encode("utf-8")
        if WALLET_P12_PASSWORD
        else None
    )

    private_key, certificate, additional_certificates = (
        pkcs12.load_key_and_certificates(
            p12_data,
            password,
        )
    )

    if private_key is None:
        raise RuntimeError("Wallet private key not found in P12")

    if certificate is None:
        raise RuntimeError("Wallet certificate not found in P12")

    cert_pem = certificate.public_bytes(Encoding.PEM)

    if additional_certificates:
        for extra_certificate in additional_certificates:
            cert_pem += extra_certificate.public_bytes(Encoding.PEM)

    key_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )

    cert_file = tempfile.NamedTemporaryFile(
        mode="wb",
        suffix=".pem",
        delete=False,
    )

    key_file = tempfile.NamedTemporaryFile(
        mode="wb",
        suffix=".pem",
        delete=False,
    )

    try:
        cert_file.write(cert_pem)
        key_file.write(key_pem)

        cert_file.close()
        key_file.close()

        return cert_file.name, key_file.name

    except Exception:
        cert_file.close()
        key_file.close()

        if os.path.exists(cert_file.name):
            os.unlink(cert_file.name)

        if os.path.exists(key_file.name):
            os.unlink(key_file.name)

        raise


async def send_wallet_push(push_token: str):
    clean_token = (push_token or "").strip()

    if not clean_token:
        return False

    cert_path = None
    key_path = None

    try:
        cert_path, key_path = _create_apns_pem_files()

        async with httpx.AsyncClient(
            http2=True,
            cert=(cert_path, key_path),
            timeout=15.0,
        ) as client:

            response = await client.post(
                f"{APNS_URL}/3/device/{clean_token}",
                headers={
                    "apns-topic": WALLET_PASS_TYPE_ID,
                },
                content=b"{}",
            )

        if response.status_code == 200:
            print(
                "✅ WALLET APNS SENT:",
                clean_token[:12] + "...",
            )
            return True

        print(
            "❌ WALLET APNS ERROR:",
            response.status_code,
            response.text,
        )

        return False

    except Exception as exc:
        print(
            "❌ WALLET APNS EXCEPTION:",
            repr(exc),
        )
        return False

    finally:
        if cert_path and os.path.exists(cert_path):
            os.unlink(cert_path)

        if key_path and os.path.exists(key_path):
            os.unlink(key_path)


async def send_wallet_pushes(push_tokens: list[str]):
    sent = 0
    failed = 0

    for push_token in push_tokens:
        success = await send_wallet_push(push_token)

        if success:
            sent += 1
        else:
            failed += 1

    print(
        f"📲 WALLET APNS RESULT: sent={sent}, failed={failed}"
    )

    return {
        "sent": sent,
        "failed": failed,
    }
