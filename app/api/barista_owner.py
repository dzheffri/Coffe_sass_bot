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
                             claim_network_broadcast, make_network_broadcast_preview,
                             preview_limits, _lock_active_shop,
                             read_statistics, remove_member, require_owner,
                             selected_shop, shop_response)
from app.db import get_connection, log_broadcast_touches
from app.broadcast_media import JSON_LIMIT, bounded_body, multipart_media, validate_text_length
from app.broadcast_publication import BroadcastPublication
from app.broadcast_delivery import send_broadcast_app_push
from app.broadcast_jobs import (broadcast_job_loop, delivery_status, enqueue_broadcast,
                                existing_confirmation, owner_history, telegram_plan)

logger = logging.getLogger(__name__)


def _broadcast_shop_is_active(shop_id):
    with get_connection() as connection:
        with connection.transaction():
            connection.execute("SET TRANSACTION READ ONLY")
            return connection.execute(
                "SELECT id FROM coffee_shops WHERE id=%s AND is_active IS TRUE", (shop_id,),
            ).fetchone() is not None


def _broadcast_audience_is_active(broadcast):
    with get_connection() as connection:
        with connection.transaction():
            connection.execute("SET TRANSACTION READ ONLY")
            if broadcast.get("scope") == "NETWORK":
                return connection.execute("""SELECT 1 FROM coffee_shops
                    WHERE network_id=%s AND is_active IS TRUE LIMIT 1""",
                    (broadcast["network_id"],)).fetchone() is not None
            return connection.execute(
                "SELECT id FROM coffee_shops WHERE id=%s AND is_active IS TRUE", (broadcast["shop_id"],),
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
    """Notify the snapshot recipients via APNs, falling back to Telegram."""
    bot = None
    touched = []
    failed = 0
    telegram_media_id = None
    recipients = list({row["user_id"]: row for row in broadcast["recipients"]}.values())
    try:
        for index, row in enumerate(recipients):
            try:
                # Sending happens after claim commits. Recheck between sends
                # so shop closure also stops the pending portion of delivery.
                if not await run_in_threadpool(_broadcast_audience_is_active, broadcast):
                    failed += len(recipients) - index
                    break
                push_sent = await send_broadcast_app_push(broadcast, row)
                if push_sent is True:
                    touched.append(row["user_id"])
                    continue
                if push_sent is False:
                    failed += 1
                    continue
                if row.get("telegram_user_id") is None:
                    # The recipient still has the message in their inbox.
                    continue
                if bot is None:
                    from app.config import BOT_TOKEN
                    bot = Bot(BOT_TOKEN)
                media = broadcast.get("media")
                for stage in telegram_plan(broadcast['shop_name'], broadcast['text'], media['kind'] if media else None):
                    if stage['kind'] == 'text':
                        await bot.send_message(chat_id=row['telegram_user_id'], text=stage['text'],parse_mode=None)
                        continue
                    # Reuse only a file ID returned by our own successful Bot
                    # upload. App-supplied IDs/URLs are never accepted.
                    file = telegram_media_id or BufferedInputFile(media["bytes"], filename=media["filename"])
                    if media["kind"] == "photo":
                        message = await bot.send_photo(chat_id=row["telegram_user_id"], photo=file,
                                                       caption=stage['text'],parse_mode=None)
                        sizes = getattr(message, "photo", None)
                        candidate = getattr(sizes[-1], "file_id", None) if isinstance(sizes, list) and sizes else None
                    else:
                        message = await bot.send_video(chat_id=row["telegram_user_id"], video=file,
                                                       caption=stage['text'],parse_mode=None)
                        candidate = getattr(getattr(message, "video", None), "file_id", None)
                    if isinstance(candidate, str) and candidate:
                        telegram_media_id = candidate
                touched.append(row["user_id"])
            except Exception as exc:
                logger.warning("Owner broadcast delivery failed (%s)", type(exc).__name__)
                failed += 1
    finally:
        if bot is not None:
            await bot.session.close()
    return touched, failed


def build_owner_router(*, authorize: Callable, parse_bearer: Callable):
    router = APIRouter(tags=["barista-owner"])
    cleanup_task = None
    delivery_task = None

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
        nonlocal cleanup_task, delivery_task
        cleanup_task = asyncio.create_task(cleanup_loop())
        delivery_task = asyncio.create_task(broadcast_job_loop())

    @router.on_event("shutdown")
    async def stop_media_cleanup():
        if delivery_task is not None:
            delivery_task.cancel()
            with suppress(asyncio.CancelledError):
                await delivery_task
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
        with BroadcastPublication() as publication:
            with get_connection() as connection:
                with connection.transaction():
                    session = authorize(token, connection=connection)
                    shop = require_owner(session)
                    # Serialize the lookup with preview claiming. A concurrent
                    # retry must observe the first committed job, not used_at.
                    _lock_active_shop(shop['shop_id'], connection)
                    # Current owner/tenant checked before looking up an old
                    # action. The result survives preview cleanup/session refresh.
                    existing = existing_confirmation(connection, confirmation_token, session["user_id"],
                                                     shop_id=shop["shop_id"])
                    if existing is not None:
                        return delivery_status(connection, existing)
                    broadcast = claim_broadcast(session, shop, confirmation_token, connection, publication)
                    enqueue_broadcast(connection, broadcast, confirmation_token, session["user_id"])
                    return delivery_status(connection, broadcast["broadcast_id"])

    @router.post("/owner/broadcast", status_code=202)
    async def send(body: BroadcastSendRequest, authorization: str | None = Header(default=None)):
        try:
            return await run_in_threadpool(prepare_send, parse_bearer(authorization), body.confirmation_token)
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "BROADCAST_NOT_CONFIGURED"}) from exc

    def owner_job_context(connection, token, broadcast_id=None):
        session = authorize(token, connection=connection, touch=False)
        shop = require_owner(session)
        if broadcast_id is not None and connection.execute(
                """SELECT 1 FROM broadcasts b WHERE b.id=%s AND b.shop_id=%s
                AND COALESCE(to_jsonb(b)->>'scope','SHOP')='SHOP'
                AND COALESCE(to_jsonb(b)->>'origin','manual')='manual'""",
                (broadcast_id, shop["shop_id"])).fetchone() is None:
            raise HTTPException(404, detail={"code":"BROADCAST_NOT_FOUND"})
        return session, shop

    @router.get("/owner/broadcasts")
    def history(limit: int = Query(default=20,ge=1,le=100), offset: int = Query(default=0,ge=0,le=1000000),
                authorization: str | None = Header(default=None)):
        with get_connection() as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                _, shop = owner_job_context(connection, parse_bearer(authorization))
                return owner_history(connection, shop_id=shop["shop_id"], limit=limit, offset=offset)

    @router.get("/owner/broadcasts/{broadcast_id}")
    def status(broadcast_id: int, authorization: str | None = Header(default=None)):
        with get_connection() as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                owner_job_context(connection, parse_bearer(authorization), broadcast_id)
                return delivery_status(connection, broadcast_id)

    @router.post("/owner/broadcast/status")
    def reconcile(body: BroadcastSendRequest, authorization: str | None = Header(default=None)):
        with get_connection() as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                session,shop = owner_job_context(connection, parse_bearer(authorization))
                found = existing_confirmation(connection,body.confirmation_token,session["user_id"],shop_id=shop["shop_id"])
                if found is None:
                    raise HTTPException(404, detail={"code":"BROADCAST_NOT_FOUND"})
                return delivery_status(connection,found)

    def authorize_network(token, network_id, *, connection=None, touch=False):
        session = authorize(token, connection=connection, touch=touch)
        network = next((item for item in session.get("networks", [])
                        if item["network_id"] == network_id), None)
        if network is None:
            raise HTTPException(403, detail={"code": "NETWORK_ACCESS_DENIED"})
        return session, network

    def network_upload_context(token, network_id):
        with get_connection() as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                session, _ = authorize_network(token, network_id, connection=connection, touch=False)
                connection.execute("SELECT media_bytes FROM admin_broadcast_previews WHERE FALSE")
                return session["session_id"]

    def save_network_preview(token, network_id, text, media, expected_session_id):
        with get_connection() as connection:
            with connection.transaction():
                session, network = authorize_network(token, network_id, connection=connection, touch=True)
                if session["session_id"] != expected_session_id:
                    raise HTTPException(409, detail={"code": "PREVIEW_CONTEXT_MISMATCH"})
                result = make_network_broadcast_preview(session, network_id, text, connection, media=media)
                return {"ok": True, "network": network, **result}

    @router.post("/owner/networks/{network_id}/broadcast/preview")
    async def network_preview(network_id: int, request: Request,
                              authorization: str | None = Header(default=None)):
        token = parse_bearer(authorization)
        try:
            expected_session_id = await run_in_threadpool(network_upload_context, token, network_id)
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
            return await run_in_threadpool(save_network_preview, token, network_id, text, media,
                                           expected_session_id)
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "BROADCAST_NOT_CONFIGURED"}) from exc

    def prepare_network_send(token, network_id, confirmation_token):
        with BroadcastPublication() as publication:
            with get_connection() as connection:
                with connection.transaction():
                    session, network = authorize_network(token, network_id, connection=connection, touch=True)
                    connection.execute('SELECT id FROM coffee_networks WHERE id=%s FOR NO KEY UPDATE',
                                       (network_id,)).fetchone()
                    existing = existing_confirmation(connection, confirmation_token, session["user_id"],network_id=network_id)
                    if existing is not None:
                        return delivery_status(connection, existing)
                    broadcast = claim_network_broadcast(session, network_id, confirmation_token,
                                                        connection, publication)
                    enqueue_broadcast(connection,broadcast,confirmation_token,session["user_id"])
                    return delivery_status(connection,broadcast["broadcast_id"])

    @router.post("/owner/networks/{network_id}/broadcast",status_code=202)
    async def send_network(body: BroadcastSendRequest, network_id: int,
                           authorization: str | None = Header(default=None)):
        try:
            return await run_in_threadpool(prepare_network_send, parse_bearer(authorization),network_id,body.confirmation_token)
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code":"BROADCAST_NOT_CONFIGURED"}) from exc

    def network_job_context(connection, token, network_id, broadcast_id=None):
        session,network = authorize_network(token,network_id,connection=connection,touch=False)
        if broadcast_id is not None and connection.execute(
                """SELECT 1 FROM broadcasts b WHERE b.id=%s AND b.network_id=%s AND b.scope='NETWORK'
                AND COALESCE(to_jsonb(b)->>'origin','manual')='manual'""",(broadcast_id,network_id)).fetchone() is None:
            raise HTTPException(404,detail={"code":"BROADCAST_NOT_FOUND"})
        return session,network

    @router.get("/owner/networks/{network_id}/broadcasts")
    def network_history(network_id: int,limit: int=Query(default=20,ge=1,le=100),
                        offset: int=Query(default=0,ge=0,le=1000000),authorization: str|None=Header(default=None)):
        with get_connection() as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                network_job_context(connection,parse_bearer(authorization),network_id)
                return owner_history(connection,network_id=network_id,limit=limit,offset=offset)

    @router.get("/owner/networks/{network_id}/broadcasts/{broadcast_id}")
    def network_status(network_id: int,broadcast_id: int,authorization: str|None=Header(default=None)):
        with get_connection() as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                network_job_context(connection,parse_bearer(authorization),network_id,broadcast_id)
                return delivery_status(connection,broadcast_id)

    @router.post("/owner/networks/{network_id}/broadcast/status")
    def network_reconcile(body:BroadcastSendRequest,network_id:int,authorization:str|None=Header(default=None)):
        with get_connection() as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                session,_ = network_job_context(connection,parse_bearer(authorization),network_id)
                found = existing_confirmation(connection,body.confirmation_token,session["user_id"],network_id=network_id)
                if found is None:
                    raise HTTPException(404,detail={"code":"BROADCAST_NOT_FOUND"})
                return delivery_status(connection,found)

    return router
