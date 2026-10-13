"""Bounded Telegram attachment downloads for private broadcast persistence.

References come only from the bot's received photo/video message. Download paths
and Telegram file IDs are transient; returned media contains validated bytes only.
"""

from io import BytesIO

from fastapi import HTTPException

from app.broadcast_media import PHOTO_LIMIT, VIDEO_LIMIT, validate_media


class _BoundedMediaBuffer(BytesIO):
    def __init__(self, limit: int):
        super().__init__()
        self.limit = limit

    def write(self, data):
        if self.tell() + len(data) > self.limit:
            raise HTTPException(413, detail={"code": "MEDIA_TOO_LARGE"})
        return super().write(data)


async def download_telegram_broadcast_media(bot, media_reference: dict | None) -> dict | None:
    if media_reference is None:
        return None
    kind = media_reference.get("kind")
    if kind not in {"photo", "video"}:
        return None
    file_id = media_reference.get("file_id")
    if not isinstance(file_id, str) or not file_id:
        raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
    limit = PHOTO_LIMIT if kind == "photo" else VIDEO_LIMIT
    remote_file = await bot.get_file(file_id)
    size = getattr(remote_file, "file_size", None)
    if isinstance(size, int) and size > limit:
        raise HTTPException(413, detail={"code": "MEDIA_TOO_LARGE"})
    file_path = getattr(remote_file, "file_path", None)
    if not file_path:
        raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
    buffer = _BoundedMediaBuffer(limit)
    try:
        await bot.download_file(file_path, destination=buffer, chunk_size=65536, seek=True)
        return validate_media(kind, media_reference.get("filename") or "attachment", buffer.getvalue())
    finally:
        buffer.close()
