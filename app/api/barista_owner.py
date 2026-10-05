"""Native statistics, employees and explicitly confirmed shop-only broadcasts."""

import logging
import asyncio
from contextlib import suppress
from typing import Callable

from aiogram import Bot
from aiogram.types import BufferedInputFile
from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field, ValidationError
from psycopg.errors import UndefinedColumn, UndefinedTable
from starlette.concurrency import run_in_threadpool

from app.admin_tools import (claim_broadcast, cleanup_broadcast_previews, list_members, make_broadcast_preview,
                             preview_limits,
                             read_statistics, remove_member, require_owner,
                             selected_shop, shop_response)
from app.db import get_connection, log_broadcast_touches
from app.broadcast_media import JSON_LIMIT, bounded_body, multipart_media, validate_text_length

logger = logging.getLogger(__name__)


def _broadcast_shop_is_active(shop_id):
    with get_connection() as connection:
        with connection.transaction():
            connection.execute("SET TRANSACTION READ ONLY")
            return connection.execute(
                "SELECT id FROM coffee_shops WHERE id=%s AND is_active IS TRUE", (shop_id,),
            ).fetchone() is not None


class BroadcastPreviewRequest(BaseModel):
    text: str = Field(strict=True, min_length=1, max_length=4096)

    class Config:
        extra = "forbid"


class BroadcastSendRequest(BaseModel):
    confirmation_token: str = Field(strict=True, min_length=32, max_length=256)

    class Config:
        extra = "forbid"


async def deliver_owner_broadcast(broadcast):
    """Copy the pinned text/media to the verified shop recipients only."""
    from app.config import BOT_TOKEN

    bot = Bot(BOT_TOKEN)
    touched = []
    failed = 0
    telegram_media_id = None
    try:
        for index, row in enumerate(broadcast["recipients"]):
            try:
                # Sending happens after claim commits. Recheck between sends
                # so shop closure also stops the pending portion of delivery.
                if not await run_in_threadpool(_broadcast_shop_is_active, broadcast["shop_id"]):
                    failed += len(broadcast["recipients"]) - index
                    break
                media = broadcast.get("media")
                if media is None:
                    await bot.send_message(chat_id=row["telegram_user_id"], text=broadcast["text"])
                else:
                    # Reuse only a file ID returned by our own successful Bot
                    # upload. App-supplied IDs/URLs are never accepted.
                    file = telegram_media_id or BufferedInputFile(media["bytes"], filename=media["filename"])
                    if media["kind"] == "photo":
                        message = await bot.send_photo(chat_id=row["telegram_user_id"], photo=file,
                                                       caption=broadcast["text"] or None)
                        sizes = getattr(message, "photo", None)
                        candidate = getattr(sizes[-1], "file_id", None) if isinstance(sizes, list) and sizes else None
                    else:
                        message = await bot.send_video(chat_id=row["telegram_user_id"], video=file,
                                                       caption=broadcast["text"] or None)
                        candidate = getattr(getattr(message, "video", None), "file_id", None)
                    if isinstance(candidate, str) and candidate:
                        telegram_media_id = candidate
                touched.append(row["user_id"])
            except Exception as exc:
                logger.warning("Owner broadcast delivery failed (%s)", type(exc).__name__)
                failed += 1
    finally:
        await bot.session.close()
    return touched, failed


def build_owner_router(*, authorize: Callable, parse_bearer: Callable):
    router = APIRouter(tags=["barista-owner"])
    cleanup_task = None

    def cleanup_media():
        try:
            with get_connection() as connection:
                with connection.transaction():
                    cleanup_broadcast_previews(connection)
        except (UndefinedTable, UndefinedColumn):
            # Feature unavailable before its separately approved migration.
            pass

    async def cleanup_loop():
        while True:
            try:
                await run_in_threadpool(cleanup_media)
            except Exception as exc:
                logger.warning("Owner preview maintenance failed (%s)", type(exc).__name__)
            await asyncio.sleep(60)

    @router.on_event("startup")
    async def start_media_cleanup():
        nonlocal cleanup_task
        cleanup_task = asyncio.create_task(cleanup_loop())

    @router.on_event("shutdown")
    async def stop_media_cleanup():
        if cleanup_task is not None:
            cleanup_task.cancel()
            with suppress(asyncio.CancelledError):
                await cleanup_task

    @router.get("/owner/members")
    def members(authorization: str | None = Header(default=None)):
        with get_connection() as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                session = authorize(parse_bearer(authorization), connection=connection, touch=False)
                shop = require_owner(session)
                return {"ok": True, "shop": shop_response(shop), "members": list_members(shop, connection)}

    @router.delete("/owner/members/{membership_id}")
    def delete_member(membership_id: int, authorization: str | None = Header(default=None)):
        with get_connection() as connection:
            with connection.transaction():
                session = authorize(parse_bearer(authorization), connection=connection)
                shop = require_owner(session)
                remove_member(shop, membership_id, connection)
        return {"ok": True, "removed_membership_id": membership_id}

    @router.get("/statistics")
    def statistics(request: Request, days: int = Query(default=7),
                   authorization: str | None = Header(default=None)):
        if set(request.query_params.keys()) - {"days"}:
            raise HTTPException(422, detail={"code": "UNEXPECTED_PARAMETER"})
        if days not in (7, 30):
            raise HTTPException(422, detail={"code": "INVALID_PERIOD"})
        with get_connection() as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                session = authorize(parse_bearer(authorization), connection=connection, touch=False)
                shop = selected_shop(session)
                if shop["role"] != "owner" and days != 7:
                    raise HTTPException(403, detail={"code": "OWNER_REQUIRED"})
                result = read_statistics(shop, days, connection, actor_user_id=session["user_id"])
                return {"ok": True, "shop": shop_response(shop), "role": shop["role"],
                        "days": days, **result}

    def authorize_upload(token):
        with get_connection() as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                session = authorize(token, connection=connection, touch=False)
                shop = require_owner(session)
                # Fail closed before accepting bytes when this new feature's
                # separate migration has not been applied completely.
                connection.execute("SELECT media_bytes,media_kind,media_filename,media_mime FROM admin_broadcast_previews WHERE FALSE")
                preview_limits(session, shop, connection)
                return shop["membership_id"]

    def save_preview(token, text, media, expected_membership_id):
        with get_connection() as connection:
            with connection.transaction():
                # Identity/selected shop/owner access may change during upload.
                session = authorize(token, connection=connection)
                shop = require_owner(session)
                if shop["membership_id"] != expected_membership_id:
                    raise HTTPException(409, detail={"code": "PREVIEW_CONTEXT_MISMATCH"})
                result = make_broadcast_preview(session, shop, text, connection, media=media)
                return {"ok": True, "shop": shop_response(shop), **result}

    @router.post("/owner/broadcast/preview")
    async def preview(request: Request, authorization: str | None = Header(default=None)):
        token = parse_bearer(authorization)
        try:
            # Verify owner access before consuming even one upload byte.
            expected_membership_id = await run_in_threadpool(authorize_upload, token)
            content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
            if content_type == "application/json":
                raw = await bounded_body(request, JSON_LIMIT)
                try:
                    body = BroadcastPreviewRequest.model_validate_json(raw)
                except ValidationError as exc:
                    raise HTTPException(422, detail={"code": "INVALID_BROADCAST_REQUEST"}) from exc
                text, media = body.text.strip(), None
                if not text:
                    raise HTTPException(422, detail={"code": "EMPTY_BROADCAST"})
                validate_text_length(text, 4096, "INVALID_BROADCAST_REQUEST")
            elif content_type == "multipart/form-data":
                text, media = await multipart_media(request)
            else:
                raise HTTPException(415, detail={"code": "UNSUPPORTED_MEDIA_TYPE"})
            return await run_in_threadpool(save_preview, token, text, media, expected_membership_id)
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "BROADCAST_NOT_CONFIGURED"}) from exc

    def prepare_send(token, confirmation_token):
        with get_connection() as connection:
            with connection.transaction():
                session = authorize(token, connection=connection)
                shop = require_owner(session)
                broadcast = claim_broadcast(session, shop, confirmation_token, connection)
                return shop, broadcast

    def save_delivery(shop_id, broadcast_id, touched):
        with get_connection() as connection:
            with connection.transaction():
                connection.execute("UPDATE broadcasts SET recipients_count=%s WHERE id=%s AND shop_id=%s",
                                   (len(touched), broadcast_id, shop_id))
                log_broadcast_touches(shop_id, touched, connection=connection)

    @router.post("/owner/broadcast")
    async def send(body: BroadcastSendRequest, authorization: str | None = Header(default=None)):
        token = parse_bearer(authorization)
        try:
            shop, broadcast = await run_in_threadpool(prepare_send, token, body.confirmation_token)
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "BROADCAST_NOT_CONFIGURED"}) from exc
        touched, failed = await deliver_owner_broadcast(broadcast)
        try:
            await run_in_threadpool(save_delivery, shop["shop_id"], broadcast["broadcast_id"], touched)
        except Exception as exc:
            # The messages have already been attempted. An audit failure must
            # not look like permission to resend the same confirmed broadcast.
            logger.warning("Owner broadcast audit update failed (%s)", type(exc).__name__)
        return {"ok": True, "shop": shop_response(shop), "sent": len(touched), "failed": failed,
                "status": "completed" if failed == 0 else "partial"}

    return router
