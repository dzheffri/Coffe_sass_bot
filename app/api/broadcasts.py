"""Consumer inbox: recipient snapshots authorize every message and attachment."""

from contextlib import contextmanager
import logging
import re
from typing import Callable

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from fastapi.responses import StreamingResponse
from psycopg.errors import UndefinedColumn, UndefinedTable

from app.broadcast_media_storage import BroadcastMediaStorage


logger = logging.getLogger(__name__)
_MAX_ID = 2**63 - 1
_PRIVATE_HEADERS = {"Cache-Control": "private, no-store", "Vary": "Authorization",
                    "X-Content-Type-Options": "nosniff"}
_FIELDS = ("broadcast_id", "shop_id", "shop_name", "text", "media_type", "created_at", "read_at")
_COLUMNS = """b.id AS broadcast_id, b.shop_id, shop.name AS shop_name,
    b.text, b.media_type, b.created_at, recipient.read_at"""
_JOINS = """FROM broadcast_recipients recipient
    JOIN broadcasts b ON b.id=recipient.broadcast_id
    JOIN coffee_shops shop ON shop.id=b.shop_id"""


def _not_found():
    raise HTTPException(404, detail={"code": "BROADCAST_NOT_FOUND"}, headers=_PRIVATE_HEADERS)


def _media_range(value: str | None, size: int) -> tuple[int, int, int]:
    if value is None:
        return 0, size - 1, 200
    match = re.fullmatch(r"bytes=(\d*)-(\d*)", value.strip()) if len(value) <= 128 else None
    if match and size > 0:
        left, right = match.groups()
        if left:
            start = int(left)
            end = min(int(right), size - 1) if right else size - 1
            if 0 <= start <= end < size:
                return start, end, 206
        elif right and int(right) > 0:
            return max(0, size - int(right)), size - 1, 206
    raise HTTPException(416, detail={"code": "INVALID_MEDIA_RANGE"}, headers={
        **_PRIVATE_HEADERS, "Accept-Ranges": "bytes", "Content-Range": f"bytes */{size}"})


def build_broadcast_inbox_router(*, authenticate: Callable, connection_factory: Callable,
                                 media_storage_factory: Callable = BroadcastMediaStorage):
    router = APIRouter(prefix="/me", tags=["broadcasts"])

    def current_recipient(response: Response, user=Depends(authenticate)):
        response.headers.update(_PRIVATE_HEADERS)
        return user

    @contextmanager
    def database(*, readonly=True):
        try:
            with connection_factory() as connection:
                with connection.transaction():
                    if readonly:
                        # Counts and page share a snapshot; GET never marks read.
                        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                    yield connection
        except (UndefinedTable, UndefinedColumn) as error:
            raise HTTPException(503, detail={"code": "BROADCAST_NOT_CONFIGURED"},
                                headers=_PRIVATE_HEADERS) from error

    def list_messages(user_id, limit, offset, shop_id=None):
        scope = "recipient.user_id=%s AND recipient.deleted_at IS NULL" + (
            " AND b.shop_id=%s" if shop_id is not None else ""
        )
        parameters = (user_id, shop_id) if shop_id is not None else (user_id,)
        with database() as connection:
            counts = connection.execute("""SELECT COUNT(*) AS total,
                COUNT(*) FILTER (WHERE recipient.read_at IS NULL) AS unread_count
            """ + _JOINS + " WHERE " + scope, parameters).fetchone()
            rows = connection.execute("SELECT " + _COLUMNS + " " + _JOINS + " WHERE " + scope + """
                ORDER BY b.created_at DESC, b.id DESC LIMIT %s OFFSET %s
            """, (*parameters, limit, offset)).fetchall()
        return {"items": [{field: row[field] for field in _FIELDS} for row in rows],
                "total": counts["total"], "unread_count": counts["unread_count"],
                "limit": limit, "offset": offset}

    def owned_message(user_id, shop_id, broadcast_id):
        with database() as connection:
            row = connection.execute("SELECT " + _COLUMNS + ", b.media_key " + _JOINS + """
                WHERE recipient.user_id=%s AND recipient.deleted_at IS NULL
                    AND b.shop_id=%s AND b.id=%s
            """, (user_id, shop_id, broadcast_id)).fetchone()
        if row is None:
            _not_found()
        return row

    @router.get("/broadcasts")
    def global_inbox(user=Depends(current_recipient),
                     limit: int = Query(default=20, ge=1, le=100),
                     offset: int = Query(default=0, ge=0, le=1_000_000)):
        return list_messages(user["user_id"], limit, offset)

    @router.get("/shops/{shop_id}/broadcasts")
    def shop_inbox(shop_id: int = Path(gt=0, le=_MAX_ID), user=Depends(current_recipient),
                   limit: int = Query(default=20, ge=1, le=100),
                   offset: int = Query(default=0, ge=0, le=1_000_000)):
        return list_messages(user["user_id"], limit, offset, shop_id)

    @router.get("/shops/{shop_id}/broadcasts/{broadcast_id}")
    def detail(shop_id: int = Path(gt=0, le=_MAX_ID), broadcast_id: int = Path(gt=0, le=_MAX_ID),
               user=Depends(current_recipient)):
        row = owned_message(user["user_id"], shop_id, broadcast_id)
        return {**{field: row[field] for field in _FIELDS},
                "media_endpoint": f"/me/shops/{shop_id}/broadcasts/{broadcast_id}/media"
                if row["media_type"] in {"photo", "video"} and row["media_key"] else None}

    @router.post("/shops/{shop_id}/broadcasts/{broadcast_id}/read")
    def mark_read(shop_id: int = Path(gt=0, le=_MAX_ID), broadcast_id: int = Path(gt=0, le=_MAX_ID),
                  user=Depends(current_recipient)):
        with database(readonly=False) as connection:
            row = connection.execute("""
                UPDATE broadcast_recipients recipient
                SET read_at=COALESCE(recipient.read_at,statement_timestamp())
                FROM broadcasts b
                WHERE recipient.broadcast_id=b.id AND recipient.user_id=%s
                    AND recipient.deleted_at IS NULL
                    AND b.shop_id=%s AND b.id=%s
                RETURNING b.id AS broadcast_id,b.shop_id,recipient.read_at
            """, (user["user_id"], shop_id, broadcast_id)).fetchone()
        if row is None:
            _not_found()
        return {"ok": True, **row}

    @router.delete("/shops/{shop_id}/broadcasts/{broadcast_id}")
    def delete_message(shop_id: int = Path(gt=0, le=_MAX_ID),
                       broadcast_id: int = Path(gt=0, le=_MAX_ID),
                       user=Depends(current_recipient)):
        # Deletion is scoped to the authenticated recipient snapshot. It only
        # hides this user's inbox row and never removes the broadcast/media or
        # any other recipient's copy.
        with database(readonly=False) as connection:
            row = connection.execute("""
                UPDATE broadcast_recipients recipient
                SET deleted_at=COALESCE(recipient.deleted_at,statement_timestamp())
                FROM broadcasts b
                WHERE recipient.broadcast_id=b.id AND recipient.user_id=%s
                    AND b.shop_id=%s AND b.id=%s
                RETURNING b.id AS broadcast_id,b.shop_id,recipient.deleted_at
            """, (user["user_id"], shop_id, broadcast_id)).fetchone()
        if row is None:
            _not_found()
        return {"ok": True, **row}

    @router.get("/shops/{shop_id}/broadcasts/{broadcast_id}/media")
    def media(request: Request, shop_id: int = Path(gt=0, le=_MAX_ID),
              broadcast_id: int = Path(gt=0, le=_MAX_ID), user=Depends(current_recipient)):
        # Authorize before filesystem reads or Range parsing; no key/URL input.
        row = owned_message(user["user_id"], shop_id, broadcast_id)
        attachment = None
        if row["media_type"] in {"photo", "video"} and row["media_key"]:
            try:
                attachment = media_storage_factory().get(row["media_key"])
            except (ValueError, OSError, HTTPException) as error:
                logger.warning("Broadcast media unavailable (%s)", type(error).__name__)
        if attachment is None or attachment["kind"] != row["media_type"]:
            raise HTTPException(404, detail={"code": "BROADCAST_MEDIA_NOT_FOUND"}, headers=_PRIVATE_HEADERS)
        data = attachment["bytes"]
        start, end, status = _media_range(request.headers.get("range"), len(data))
        headers = {**_PRIVATE_HEADERS, "Accept-Ranges": "bytes", "Content-Length": str(end - start + 1)}
        if status == 206:
            headers["Content-Range"] = f"bytes {start}-{end}/{len(data)}"

        def chunks():
            for position in range(start, end + 1, 65536):
                yield data[position:min(position + 65536, end + 1)]

        return StreamingResponse(chunks(), status_code=status, media_type=attachment["mime_type"], headers=headers)

    return router
