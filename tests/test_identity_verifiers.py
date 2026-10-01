"""Offline tests for the existing identity verifiers and barista audiences.

Importing app.api.main initializes database tables and filesystem paths. Compile
only the verifier definitions instead; all certificates and HTTP responses here
are local test data, with no application import or production connection.
"""

import ast
import base64
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from google.oauth2 import id_token


CLIENT_APPLE_AUDIENCE = "test.client.apple"
BARISTA_APPLE_AUDIENCE = "test.barista.apple"
CLIENT_GOOGLE_AUDIENCE = "test.client.google"
BARISTA_GOOGLE_AUDIENCE = "test.barista.google"
KEY_ID = "offline-test-key"


def _encode(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _signed_token(private_key, payload, *, header=None):
    header = header or {"alg": "RS256", "kid": KEY_ID}
    parts = [
        _encode(json.dumps(header, separators=(",", ":")).encode()),
        _encode(json.dumps(payload, separators=(",", ":")).encode()),
    ]
    signature = private_key.sign(
        ".".join(parts).encode("ascii"),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )
    return ".".join([*parts, _encode(signature)])


@pytest.fixture(scope="module")
def signing_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def verifiers(signing_key, monkeypatch):
    numbers = signing_key.public_key().public_numbers()
    jwk = {
        "kid": KEY_ID,
        "kty": "RSA",
        "alg": "RS256",
        "n": _encode(numbers.n.to_bytes((numbers.n.bit_length() + 7) // 8, "big")),
        "e": _encode(numbers.e.to_bytes((numbers.e.bit_length() + 7) // 8, "big")),
    }
    pem = signing_key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")
    certificate_requests = []

    def certificate_request(url, method="GET", **kwargs):
        certificate_requests.append((url, method))
        return SimpleNamespace(
            status=200,
            data=json.dumps({KEY_ID: pem}).encode("utf-8"),
        )

    namespace = {
        "base64": base64,
        "json": json,
        "os": os,
        "time": time,
        "rsa": rsa,
        "padding": padding,
        "hashes": hashes,
        "id_token": id_token,
        "google_requests": SimpleNamespace(Request=lambda: certificate_request),
        "APPLE_ISSUER": "https://appleid.apple.com",
        "APPLE_CLIENT_ID": CLIENT_APPLE_AUDIENCE,
        "_load_apple_keys": lambda: [jwk],
        "_apple_keys_cache": {"keys": None, "expires_at": 0},
    }
    monkeypatch.setenv("GOOGLE_CLIENT_ID", CLIENT_GOOGLE_AUDIENCE)
    source_path = Path(__file__).resolve().parents[1] / "app" / "api" / "main.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    names = {"_b64url_decode", "verify_apple_id_token", "verify_google_id_token"}
    definitions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in names
    ]
    assert {node.name for node in definitions} == names
    isolated_module = ast.Module(body=definitions, type_ignores=[])
    exec(compile(isolated_module, str(source_path), "exec"), namespace)
    return SimpleNamespace(
        apple=namespace["verify_apple_id_token"],
        google=namespace["verify_google_id_token"],
        certificate_requests=certificate_requests,
    )


def _payload(provider, audience):
    now = int(time.time())
    return {
        "iss": "https://appleid.apple.com" if provider == "apple" else "https://accounts.google.com",
        "sub": "existing-provider-subject",
        "aud": audience,
        "iat": now - 10,
        "exp": now + 600,
    }


@pytest.mark.parametrize("provider", ["apple", "google"])
def test_explicit_barista_audience_accepts_real_signature(verifiers, signing_key, provider):
    audience = BARISTA_APPLE_AUDIENCE if provider == "apple" else BARISTA_GOOGLE_AUDIENCE
    payload = _payload(provider, audience)
    result = getattr(verifiers, provider)(_signed_token(signing_key, payload), audience=audience)
    assert result == payload
    if provider == "google":
        assert verifiers.certificate_requests
        assert all(method == "GET" for _, method in verifiers.certificate_requests)


@pytest.mark.parametrize("provider", ["apple", "google"])
def test_legacy_default_client_audience_still_works(verifiers, signing_key, provider):
    audience = CLIENT_APPLE_AUDIENCE if provider == "apple" else CLIENT_GOOGLE_AUDIENCE
    payload = _payload(provider, audience)
    assert getattr(verifiers, provider)(_signed_token(signing_key, payload)) == payload


@pytest.mark.parametrize("provider", ["apple", "google"])
def test_client_identity_token_rejected_for_barista_audience(verifiers, signing_key, provider):
    client = CLIENT_APPLE_AUDIENCE if provider == "apple" else CLIENT_GOOGLE_AUDIENCE
    barista = BARISTA_APPLE_AUDIENCE if provider == "apple" else BARISTA_GOOGLE_AUDIENCE
    token = _signed_token(signing_key, _payload(provider, client))
    assert getattr(verifiers, provider)(token, audience=barista) is None


@pytest.mark.parametrize("provider", ["apple", "google"])
@pytest.mark.parametrize("invalid_claim", ["expired", "issuer", "subject"])
def test_invalid_identity_claims_are_rejected(verifiers, signing_key, provider, invalid_claim):
    audience = BARISTA_APPLE_AUDIENCE if provider == "apple" else BARISTA_GOOGLE_AUDIENCE
    payload = _payload(provider, audience)
    if invalid_claim == "expired":
        payload["exp"] = int(time.time()) - 60
    elif invalid_claim == "issuer":
        payload["iss"] = "https://untrusted.example"
    else:
        payload.pop("sub")
    token = _signed_token(signing_key, payload)
    assert getattr(verifiers, provider)(token, audience=audience) is None


@pytest.mark.parametrize("provider", ["apple", "google"])
def test_tampered_payload_is_rejected(verifiers, signing_key, provider):
    audience = BARISTA_APPLE_AUDIENCE if provider == "apple" else BARISTA_GOOGLE_AUDIENCE
    payload = _payload(provider, audience)
    token = _signed_token(signing_key, payload)
    header, _, signature = token.split(".")
    payload["sub"] = "attacker-controlled-subject"
    tampered_token = ".".join([
        header,
        _encode(json.dumps(payload).encode("utf-8")),
        signature,
    ])
    assert getattr(verifiers, provider)(tampered_token, audience=audience) is None


def test_apple_audience_list_support(verifiers, signing_key):
    payload = _payload("apple", [CLIENT_APPLE_AUDIENCE, BARISTA_APPLE_AUDIENCE])
    token = _signed_token(signing_key, payload)
    assert verifiers.apple(token, audience=BARISTA_APPLE_AUDIENCE) == payload
    assert verifiers.apple(token, audience="different-app") is None


@pytest.mark.parametrize("invalid_exp", [None, "not-a-timestamp"])
def test_apple_invalid_expiration_type_rejected(verifiers, signing_key, invalid_exp):
    payload = _payload("apple", BARISTA_APPLE_AUDIENCE)
    payload["exp"] = invalid_exp
    assert verifiers.apple(
        _signed_token(signing_key, payload), audience=BARISTA_APPLE_AUDIENCE
    ) is None


@pytest.mark.parametrize("provider", ["apple", "google"])
@pytest.mark.parametrize("token", ["", "malformed.jwt"])
def test_missing_or_malformed_token_rejected(verifiers, provider, token):
    audience = BARISTA_APPLE_AUDIENCE if provider == "apple" else BARISTA_GOOGLE_AUDIENCE
    assert getattr(verifiers, provider)(token, audience=audience) is None


@pytest.mark.parametrize("issuer", ["accounts.google.com", "https://accounts.google.com"])
def test_google_supported_issuers(verifiers, signing_key, issuer):
    payload = _payload("google", BARISTA_GOOGLE_AUDIENCE)
    payload["iss"] = issuer
    assert verifiers.google(
        _signed_token(signing_key, payload), audience=BARISTA_GOOGLE_AUDIENCE
    ) == payload
