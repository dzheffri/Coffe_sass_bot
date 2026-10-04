"""Bounded local media validation. No remote URLs, Telegram file IDs or logs."""

from io import BytesIO
from pathlib import PurePath
import warnings

from fastapi import HTTPException, Request
from PIL import Image
from starlette.datastructures import UploadFile
from starlette.formparsers import MultiPartException


PHOTO_LIMIT = 8 * 1024 * 1024
VIDEO_LIMIT = 20 * 1024 * 1024
MULTIPART_LIMIT = VIDEO_LIMIT + 64 * 1024
JSON_LIMIT = 20 * 1024


def validate_text_length(text: str, limit: int, code: str):
    # Telegram entity/text offsets and the native client use UTF-16 units.
    # Counting Python characters alone lets astral emoji bypass the limit.
    try:
        units = len(text.encode("utf-16-le")) // 2
    except UnicodeEncodeError as exc:
        raise HTTPException(422, detail={"code": code}) from exc
    if units > limit:
        raise HTTPException(422, detail={"code": code})


async def bounded_body(request: Request, limit: int) -> bytes:
    length = request.headers.get("content-length")
    if length is not None:
        try:
            if int(length) < 0 or int(length) > limit:
                raise HTTPException(413, detail={"code": "MEDIA_TOO_LARGE"})
        except ValueError as exc:
            raise HTTPException(400, detail={"code": "INVALID_CONTENT_LENGTH"}) from exc
    data = bytearray()
    async for chunk in request.stream():
        if len(data) + len(chunk) > limit:
            raise HTTPException(413, detail={"code": "MEDIA_TOO_LARGE"})
        data.extend(chunk)
    return bytes(data)


def validate_media(kind: str, filename: str, data: bytes) -> dict:
    if kind not in {"photo", "video"}:
        raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
    if not data:
        raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
    if len(data) > (PHOTO_LIMIT if kind == "photo" else VIDEO_LIMIT):
        raise HTTPException(413, detail={"code": "MEDIA_TOO_LARGE"})
    if kind == "photo":
        if data.startswith(b"\xff\xd8\xff"):
            mime, extension, image_format = "image/jpeg", ".jpg", "JPEG"
        elif data.startswith(b"\x89PNG\r\n\x1a\n"):
            mime, extension, image_format = "image/png", ".png", "PNG"
        else:
            raise HTTPException(415, detail={"code": "UNSUPPORTED_MEDIA_TYPE"})
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("error", Image.DecompressionBombWarning)
                with Image.open(BytesIO(data)) as image:
                    width, height = image.size
                    if (image.format != image_format or width <= 0 or height <= 0
                            or width + height > 10_000 or max(width, height) / min(width, height) > 20
                            or width * height > 64_000_000):
                        raise ValueError("Invalid dimensions")
                    image.verify()
        except Exception as exc:
            raise HTTPException(422, detail={"code": "INVALID_MEDIA"}) from exc
    else:
        # Bounded ISO-BMFF/QuickTime container validation, not a codec decoder.
        # Native iOS transcodes to MP4/H.264 before uploading for Telegram video.
        if len(data) < 24 or data[4:8] != b"ftyp":
            raise HTTPException(415, detail={"code": "UNSUPPORTED_MEDIA_TYPE"})
        offset, boxes, brands, kinds = 0, 0, [], set()
        while offset < len(data):
            if len(data) - offset < 8 or boxes >= 4096:
                raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
            size = int.from_bytes(data[offset:offset + 4], "big")
            box_type = data[offset + 4:offset + 8]
            header = 8
            if size == 1:
                if len(data) - offset < 16:
                    raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
                size, header = int.from_bytes(data[offset + 8:offset + 16], "big"), 16
            elif size == 0:
                size = len(data) - offset
            if size < header or offset + size > len(data):
                raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
            if box_type == b"ftyp":
                payload = data[offset + header:offset + size]
                if len(payload) < 8 or len(payload) % 4:
                    raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
                brands = [payload[:4], *[payload[index:index + 4] for index in range(8, len(payload), 4)]]
            kinds.add(box_type)
            offset += size
            boxes += 1
        if not {b"ftyp", b"moov", b"mdat"}.issubset(kinds):
            raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
        if b"qt  " in brands:
            mime, extension = "video/quicktime", ".mov"
        elif set(brands) & {b"isom", b"iso2", b"mp41", b"mp42", b"avc1", b"hvc1", b"hev1", b"M4V "}:
            mime, extension = "video/mp4", ".mp4"
        else:
            raise HTTPException(415, detail={"code": "UNSUPPORTED_MEDIA_TYPE"})
    # Preserve only a short printable basename with an extension derived from bytes.
    name = PurePath((filename or "attachment").replace("\\", "/")).name
    stem = "".join(character for character in PurePath(name).stem if character.isprintable())[:96].strip()
    return {"kind": kind, "filename": (stem or "attachment") + extension,
            "mime_type": mime, "size_bytes": len(data), "bytes": data}


async def multipart_media(request: Request) -> tuple[str, dict]:
    data = await bounded_body(request, MULTIPART_LIMIT)
    delivered = False

    async def receive():
        nonlocal delivered
        if delivered:
            return {"type": "http.request", "body": b"", "more_body": False}
        delivered = True
        return {"type": "http.request", "body": data, "more_body": False}

    bounded = Request(request.scope, receive=receive)
    try:
        async with bounded.form(max_files=1, max_fields=2, max_part_size=4096) as form:
            pairs = list(form.multi_items())
            names = [name for name, _ in pairs]
            if (set(names) - {"text", "media_kind", "media"}
                    or len(names) != len(set(names)) or "media_kind" not in names or "media" not in names):
                raise HTTPException(422, detail={"code": "UNEXPECTED_PARAMETER"})
            text, kind, uploaded = form.get("text", ""), form["media_kind"], form["media"]
            if not isinstance(text, str) or not isinstance(kind, str) or not isinstance(uploaded, UploadFile):
                raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
            text = text.strip()
            validate_text_length(text, 1024, "INVALID_CAPTION")
            if kind not in {"photo", "video"}:
                raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
            limit = PHOTO_LIMIT if kind == "photo" else VIDEO_LIMIT
            content = await uploaded.read(limit + 1)
            media = validate_media(kind, uploaded.filename or "attachment", content)
            return text, media
    except MultiPartException as exc:
        raise HTTPException(422, detail={"code": "INVALID_MEDIA"}) from exc
