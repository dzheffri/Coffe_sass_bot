import base64
import hashlib
import io
import json
import os
import secrets
import zipfile
from datetime import datetime, timezone

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.hazmat.primitives.serialization.pkcs7 import (
    PKCS7Options,
    PKCS7SignatureBuilder,
)


# =========================================================
# CONFIG
# =========================================================

WALLET_PASS_TYPE_ID = os.getenv(
    "WALLET_PASS_TYPE_ID",
    "pass.com.dzheffri.coffeeclub",
)

WALLET_TEAM_ID = os.getenv(
    "WALLET_TEAM_ID",
    "976LHSCXA7",
)

WALLET_P12_BASE64 = os.getenv("WALLET_P12_BASE64")
WALLET_P12_PASSWORD = os.getenv("WALLET_P12_PASSWORD")
WALLET_WWDR_BASE64 = os.getenv("WALLET_WWDR_BASE64")

WALLET_WEB_SERVICE_URL = os.getenv(
    "WALLET_WEB_SERVICE_URL",
    "https://coffesassbot-production.up.railway.app",
)

WALLET_ASSETS_DIR = os.path.join(
    os.path.dirname(__file__),
    "wallet_assets",
)


# =========================================================
# CARD DESIGNS
# =========================================================

CARD_DESIGNS = {
    "basic": {
        "title": "Класика",
        "background": "rgb(242, 242, 242)",
        "foreground": "rgb(30, 30, 30)",
        "label": "rgb(90, 90, 90)",
        "strip": "strip_basic",
    },
    "gold": {
        "title": "Золота",
        "background": "rgb(225, 164, 45)",
        "foreground": "rgb(35, 25, 10)",
        "label": "rgb(90, 60, 10)",
        "strip": "strip_gold",
    },
    "fire": {
        "title": "Вогонь",
        "background": "rgb(200, 55, 35)",
        "foreground": "rgb(255, 255, 255)",
        "label": "rgb(255, 220, 200)",
        "strip": "strip_fire",
    },
    "diamond": {
        "title": "Діамант",
        "background": "rgb(60, 165, 215)",
        "foreground": "rgb(255, 255, 255)",
        "label": "rgb(220, 245, 255)",
        "strip": "strip_diamond",
    },
    "coffee": {
        "title": "Кавоман",
        "background": "rgb(70, 45, 35)",
        "foreground": "rgb(255, 255, 255)",
        "label": "rgb(220, 200, 185)",
        "strip": "strip_coffee",
    },
    "explorer": {
        "title": "Дослідник",
        "background": "rgb(45, 125, 105)",
        "foreground": "rgb(255, 255, 255)",
        "label": "rgb(210, 240, 230)",
        "strip": "strip_explorer",
    },
}

VALID_CARD_DESIGNS = set(CARD_DESIGNS.keys())


# =========================================================
# HELPERS
# =========================================================

def normalize_design_id(design_id: str | None) -> str:
    value = (design_id or "basic").strip().lower()

    if value not in VALID_CARD_DESIGNS:
        return "basic"

    return value


def wallet_serial_number(user_id: int) -> str:
    return f"nashi-user-{user_id}"


def generate_authentication_token() -> str:
    return secrets.token_urlsafe(32)


def load_wallet_asset(filename: str) -> bytes:
    path = os.path.join(
        WALLET_ASSETS_DIR,
        filename,
    )

    if not os.path.isfile(path):
        raise RuntimeError(
            f"Wallet asset not found: {path}"
        )

    with open(path, "rb") as file:
        return file.read()


# =========================================================
# CERTIFICATES
# =========================================================

def _require_env(name: str, value: str | None):
    if not value:
        raise RuntimeError(
            f"{name} is not configured"
        )


def load_signing_material():
    _require_env(
        "WALLET_P12_BASE64",
        WALLET_P12_BASE64,
    )

    _require_env(
        "WALLET_P12_PASSWORD",
        WALLET_P12_PASSWORD,
    )

    _require_env(
        "WALLET_WWDR_BASE64",
        WALLET_WWDR_BASE64,
    )

    try:
        p12_data = base64.b64decode(
            WALLET_P12_BASE64
        )
    except Exception as exc:
        raise RuntimeError(
            "WALLET_P12_BASE64 contains invalid Base64"
        ) from exc

    try:
        private_key, certificate, additional_certificates = (
            pkcs12.load_key_and_certificates(
                p12_data,
                WALLET_P12_PASSWORD.encode("utf-8"),
            )
        )
    except Exception as exc:
        raise RuntimeError(
            "Unable to load Wallet P12 certificate"
        ) from exc

    if private_key is None:
        raise RuntimeError(
            "Wallet P12 does not contain a private key"
        )

    if certificate is None:
        raise RuntimeError(
            "Wallet P12 does not contain a certificate"
        )

    try:
        wwdr_data = base64.b64decode(
            WALLET_WWDR_BASE64
        )
    except Exception as exc:
        raise RuntimeError(
            "WALLET_WWDR_BASE64 contains invalid Base64"
        ) from exc

    try:
        wwdr_certificate = (
            x509.load_der_x509_certificate(
                wwdr_data
            )
        )
    except ValueError:
        try:
            wwdr_certificate = (
                x509.load_pem_x509_certificate(
                    wwdr_data
                )
            )
        except Exception as exc:
            raise RuntimeError(
                "Unable to load Apple WWDR certificate"
            ) from exc

    return (
        private_key,
        certificate,
        wwdr_certificate,
        additional_certificates or [],
    )


# =========================================================
# PASS.JSON
# =========================================================

def build_pass_json(
    *,
    user_id: int,
    full_name: str | None,
    personal_qr_token: str,
    selected_design: str,
    total_cups: int,
    total_free: int,
    shops_count: int,
    authentication_token: str,
) -> dict:

    design_id = normalize_design_id(
        selected_design
    )

    design = CARD_DESIGNS[design_id]

    display_name = (
        (full_name or "").strip()
        or "Кавоман"
    )

    qr_message = (
        f"coffee:{personal_qr_token}"
    )

    return {
        "formatVersion": 1,

        "passTypeIdentifier": WALLET_PASS_TYPE_ID,
        "serialNumber": wallet_serial_number(user_id),
        "teamIdentifier": WALLET_TEAM_ID,

        "organizationName": "Наші",
        "description": "Картка лояльності «Наші»",

        # Автоматические обновления Wallet
        "webServiceURL": (
            WALLET_WEB_SERVICE_URL.rstrip("/")
        ),
        "authenticationToken": authentication_token,

        # Цвета выбранного скина
        "backgroundColor": design["background"],
        "foregroundColor": design["foreground"],
        "labelColor": design["label"],

        # Название рядом с логотипом
        "logoText": "Наші",

        # QR
        "barcodes": [
            {
                "format": "PKBarcodeFormatQR",
                "message": qr_message,
                "messageEncoding": "utf-8",
                "altText": "Наші",
            }
        ],

        "barcode": {
            "format": "PKBarcodeFormatQR",
            "message": qr_message,
            "messageEncoding": "utf-8",
            "altText": "Наші",
        },

        # Карта
        "storeCard": {

            # Справа сверху
            "headerFields": [
                {
                    "key": "skin",
                    "label": "СТИЛЬ",
                    "value": design["title"],
                }
            ],

            # ВАЖНО:
            # primaryFields оставляем пустым.
            # Именно primary раньше делал имя огромным.
            "primaryFields": [],

            # Имя + основные показатели нормального размера
            "secondaryFields": [
                {
                    "key": "member",
                    "label": "ВЛАСНИК",
                    "value": display_name,
                },
                {
                    "key": "cups",
                    "label": "КАВИ",
                    "value": int(total_cups),
                },
            ],

            "auxiliaryFields": [
                {
                    "key": "gifts",
                    "label": "ПОДАРУНКИ",
                    "value": int(total_free),
                },
                {
                    "key": "shops",
                    "label": "КАВʼЯРНІ",
                    "value": int(shops_count),
                },
            ],

            "backFields": [
                {
                    "key": "about",
                    "label": "ПРО КАРТКУ",
                    "value": (
                        "Персональна картка програми "
                        "лояльності «Наші»."
                    ),
                },
                {
                    "key": "design",
                    "label": "СТИЛЬ КАРТКИ",
                    "value": design["title"],
                },
                {
                    "key": "usage",
                    "label": "ЯК КОРИСТУВАТИСЯ",
                    "value": (
                        "Покажіть QR-код баристі "
                        "під час покупки."
                    ),
                },
            ],
        },
    }


# =========================================================
# MANIFEST
# =========================================================

def create_manifest(
    files: dict[str, bytes],
) -> bytes:

    manifest = {}

    for filename, data in files.items():
        manifest[filename] = (
            hashlib.sha1(data).hexdigest()
        )

    return json.dumps(
        manifest,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


# =========================================================
# SIGNATURE
# =========================================================

def sign_manifest(
    manifest_data: bytes,
) -> bytes:

    (
        private_key,
        certificate,
        wwdr_certificate,
        _additional_certificates,
    ) = load_signing_material()

    builder = (
        PKCS7SignatureBuilder()
        .set_data(manifest_data)
    )

    builder = builder.add_signer(
        certificate,
        private_key,
        hashes.SHA256(),
    )

    builder = builder.add_certificate(
        wwdr_certificate
    )

    signature = builder.sign(
        serialization.Encoding.DER,
        [
            PKCS7Options.DetachedSignature,
            PKCS7Options.Binary,
        ],
    )

    return signature


# =========================================================
# PKPASS
# =========================================================

def create_pkpass(
    *,
    user_id: int,
    full_name: str | None,
    personal_qr_token: str,
    selected_design: str,
    total_cups: int,
    total_free: int,
    shops_count: int,
    authentication_token: str,
) -> bytes:

    if not personal_qr_token:
        raise RuntimeError(
            "User does not have personal_qr_token"
        )

    if not authentication_token:
        raise RuntimeError(
            "Wallet authentication token is missing"
        )

    design_id = normalize_design_id(
        selected_design
    )

    design = CARD_DESIGNS[design_id]
    strip_name = design["strip"]

    pass_json = build_pass_json(
        user_id=user_id,
        full_name=full_name,
        personal_qr_token=personal_qr_token,
        selected_design=design_id,
        total_cups=total_cups,
        total_free=total_free,
        shops_count=shops_count,
        authentication_token=authentication_token,
    )

    files: dict[str, bytes] = {

        "pass.json": json.dumps(
            pass_json,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8"),

        # Настоящий логотип/иконка «Наші»
        "icon.png": load_wallet_asset(
            "icon.png"
        ),
        "icon@2x.png": load_wallet_asset(
            "icon@2x.png"
        ),
        "icon@3x.png": load_wallet_asset(
            "icon@3x.png"
        ),

        "logo.png": load_wallet_asset(
            "logo.png"
        ),
        "logo@2x.png": load_wallet_asset(
            "logo@2x.png"
        ),
        "logo@3x.png": load_wallet_asset(
            "logo@3x.png"
        ),

        # Фирменная широкая картинка.
        # Имя внутри .pkpass всегда strip.png,
        # но исходный файл выбирается по скину.
        "strip.png": load_wallet_asset(
            f"{strip_name}.png"
        ),

        "strip@2x.png": load_wallet_asset(
            f"{strip_name}@2x.png"
        ),
    }

    manifest_data = create_manifest(
        files
    )

    files["manifest.json"] = manifest_data

    signature = sign_manifest(
        manifest_data
    )

    files["signature"] = signature

    output = io.BytesIO()

    with zipfile.ZipFile(
        output,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:

        for filename, data in files.items():
            archive.writestr(
                filename,
                data,
            )

    return output.getvalue()


# =========================================================
# LAST MODIFIED
# =========================================================

def wallet_last_modified_http_date(
    updated_at: datetime | None,
) -> str:

    value = (
        updated_at
        or datetime.now(timezone.utc)
    )

    if value.tzinfo is None:
        value = value.replace(
            tzinfo=timezone.utc
        )

    value = value.astimezone(
        timezone.utc
    )

    return value.strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )
