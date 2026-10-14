"""Community feed and Super Admin publication API."""

from contextlib import contextmanager
import logging
from typing import Callable, Collection, Literal

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Path, Query, Request, Response
from pydantic import BaseModel, Field
from psycopg.errors import UndefinedColumn, UndefinedTable
from starlette.datastructures import UploadFile

from app.broadcast_media import PHOTO_LIMIT, bounded_body, validate_media
from app.community import CommunityError, CommunityRepository
from app.community_media import CommunityMediaStorage


logger = logging.getLogger(__name__)
_MAX_ID = 2**63 - 1
_POST_TYPES = Literal["fact", "tip", "history", "news", "poll", "series"]


class CommunityReactionRequest(BaseModel):
    interested: bool

    class Config:
        extra = "forbid"


class CommunityVoteRequest(BaseModel):
    option_id: int = Field(strict=True, gt=0)

    class Config:
        extra = "forbid"


class CommunityPostInput(BaseModel):
    type: _POST_TYPES
    title: str = Field(min_length=1, max_length=180)
    text: str = Field(max_length=20_000)
    question: str | None = Field(default=None, max_length=300)
    options: list[str] = Field(default_factory=list, max_length=10)
    series_title: str | None = Field(default=None, max_length=180)
    series_part: int | None = Field(default=None, gt=0, le=365)
    series_total: int | None = Field(default=None, gt=0, le=365)

    class Config:
        extra = "forbid"


class CommunityPublishRequest(BaseModel):
    notify: bool = False

    class Config:
        extra = "forbid"


def _invalid(message: str = "INVALID_COMMUNITY_POST") -> CommunityError:
    return CommunityError(422, message)


def _normalise_post(data: CommunityPostInput) -> dict:
    values = data.model_dump(mode="python")
    values["title"] = values["title"].strip()
    values["text"] = values["text"].strip()
    if not values["title"] or (values["type"] != "poll" and not values["text"]):
        raise _invalid()
    values["options"] = [item.strip() if isinstance(item, str) else item for item in values["options"]]
    if any(not isinstance(item, str) or not item or len(item) > 160 for item in values["options"]):
        raise _invalid("INVALID_POLL_OPTIONS")
    if values["type"] == "poll":
        if not values["question"] or not values["question"].strip() or not 2 <= len(values["options"]) <= 10:
            raise _invalid("INVALID_POLL_OPTIONS")
        values["question"] = values["question"].strip()
        if len({item.casefold() for item in values["options"]}) != len(values["options"]):
            raise _invalid("INVALID_POLL_OPTIONS")
    elif values["question"] is not None or values["options"]:
        raise _invalid("POLL_FIELDS_REQUIRE_POLL")
    if values["type"] == "series":
        if not values["series_title"] or values["series_part"] is None or values["series_total"] is None:
            raise _invalid("INVALID_SERIES")
        values["series_title"] = values["series_title"].strip()
        if not values["series_title"] or values["series_part"] > values["series_total"]:
            raise _invalid("INVALID_SERIES")
    elif any(values[key] is not None for key in ("series_title", "series_part", "series_total")):
        raise _invalid("SERIES_FIELDS_REQUIRE_SERIES")
    return values


async def _photo_from_request(request: Request) -> dict:
    raw = await bounded_body(request, PHOTO_LIMIT + 64 * 1024)
    delivered = False

    async def receive():
        nonlocal delivered
        if delivered:
            return {"type": "http.request", "body": b"", "more_body": False}
        delivered = True
        return {"type": "http.request", "body": raw, "more_body": False}

    bounded = Request(request.scope, receive=receive)
    try:
        async with bounded.form(max_files=1, max_fields=2, max_part_size=PHOTO_LIMIT) as form:
            pairs = list(form.multi_items())
            if set(name for name, _ in pairs) - {"photo", "media", "media_kind"}:
                raise HTTPException(422, detail={"code": "UNEXPECTED_PARAMETER"})
            uploads = [item for name, item in pairs if name in {"photo", "media"} and isinstance(item, UploadFile)]
            kinds = [item for name, item in pairs if name == "media_kind"]
            if len(uploads) != 1 or len(kinds) > 1 or (kinds and kinds[0] != "photo"):
                raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
            upload = uploads[0]
            content = await upload.read(PHOTO_LIMIT + 1)
            return validate_media("photo", upload.filename or "photo", content)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(422, detail={"code": "INVALID_MEDIA"}) from exc


def _error(exc: CommunityError) -> HTTPException:
    return HTTPException(exc.status_code, detail={"code": exc.code, "message": exc.message})


def build_community_router(
    *, authenticate: Callable,
    verify_actor: Callable,
    superadmin_telegram_ids: Collection[int],
    connection_factory: Callable,
    media_storage_factory: Callable = CommunityMediaStorage,
    push_sender: Callable | None = None,
) -> APIRouter:
    router = APIRouter(tags=["community"])

    @contextmanager
    def database(*, readonly: bool = True):
        try:
            with connection_factory() as connection:
                with connection.transaction():
                    if readonly:
                        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                    yield connection
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "COMMUNITY_SCHEMA_NOT_READY"}) from exc

    def current_user(response: Response, user=Depends(authenticate)):
        response.headers["Cache-Control"] = "private, no-store"
        response.headers["Vary"] = "Authorization"
        return user

    def current_admin(
        authorization: str | None = Header(default=None),
        x_telegram_init_data: str | None = Header(default=None),
    ):
        actor = verify_actor(authorization, x_telegram_init_data)
        if actor["telegram_id"] not in superadmin_telegram_ids:
            raise HTTPException(403, detail={"code": "SUPERADMIN_ACCESS_REQUIRED"})
        return {"user_id": actor["user_id"], "role": "superadmin"}

    def admin_response(response: Response, actor=Depends(current_admin)):
        response.headers["Cache-Control"] = "no-store"
        response.headers["Vary"] = "Authorization, X-Telegram-Init-Data"
        return actor

    def repository(connection) -> CommunityRepository:
        return CommunityRepository(connection)

    @router.get("/me/community/posts")
    def list_public_posts(
        user=Depends(current_user),
        limit: int = Query(default=20, ge=1, le=100),
        offset: int = Query(default=0, ge=0, le=1_000_000),
    ):
        with database() as connection:
            try:
                return repository(connection).list_posts(user["user_id"], limit=limit, offset=offset,
                                                        published_only=True)
            except CommunityError as exc:
                raise _error(exc) from exc

    @router.get("/me/community/posts/{post_id}")
    def public_post(post_id: int = Path(gt=0, le=_MAX_ID), user=Depends(current_user)):
        with database() as connection:
            try:
                return repository(connection).get_post(post_id, user["user_id"], published_only=True,
                                                      include_series=True)
            except CommunityError as exc:
                raise _error(exc) from exc

    @router.put("/me/community/posts/{post_id}/reaction")
    def react(post_id: int = Path(gt=0, le=_MAX_ID), body: CommunityReactionRequest = Body(...),
              user=Depends(current_user)):
        with database(readonly=False) as connection:
            try:
                return repository(connection).react(post_id, user["user_id"], body.interested)
            except CommunityError as exc:
                raise _error(exc) from exc

    @router.post("/me/community/posts/{post_id}/vote")
    def vote(post_id: int = Path(gt=0, le=_MAX_ID), body: CommunityVoteRequest = Body(...),
             user=Depends(current_user)):
        with database(readonly=False) as connection:
            try:
                return repository(connection).vote(post_id, user["user_id"], body.option_id)
            except CommunityError as exc:
                raise _error(exc) from exc

    @router.get("/community/admin/me")
    def admin_me(actor=Depends(admin_response)):
        return {"allowed": True, "user_id": actor["user_id"], "role": actor["role"]}

    @router.get("/community/admin/posts")
    def admin_list(
        actor=Depends(admin_response),
        limit: int = Query(default=20, ge=1, le=100),
        offset: int = Query(default=0, ge=0, le=1_000_000),
    ):
        with database() as connection:
            try:
                return repository(connection).list_posts(actor["user_id"], limit=limit, offset=offset,
                                                        published_only=False, admin=True)
            except CommunityError as exc:
                raise _error(exc) from exc

    @router.post("/community/admin/posts", status_code=201)
    def admin_create(body: CommunityPostInput, actor=Depends(admin_response)):
        try:
            values = _normalise_post(body)
        except CommunityError as exc:
            raise _error(exc) from exc
        with database(readonly=False) as connection:
            try:
                return repository(connection).create_post(values, actor["user_id"])
            except CommunityError as exc:
                raise _error(exc) from exc

    @router.get("/community/admin/posts/{post_id}")
    def admin_get(post_id: int = Path(gt=0, le=_MAX_ID), actor=Depends(admin_response)):
        with database() as connection:
            try:
                return repository(connection).get_post(post_id, actor["user_id"], published_only=False,
                                                      include_series=True, admin=True)
            except CommunityError as exc:
                raise _error(exc) from exc

    @router.put("/community/admin/posts/{post_id}")
    def admin_update(post_id: int = Path(gt=0, le=_MAX_ID), body: CommunityPostInput = Body(...),
                     actor=Depends(admin_response)):
        try:
            values = _normalise_post(body)
        except CommunityError as exc:
            raise _error(exc) from exc
        with database(readonly=False) as connection:
            try:
                return repository(connection).update_post(post_id, values, actor["user_id"])
            except CommunityError as exc:
                raise _error(exc) from exc

    @router.post("/community/admin/posts/{post_id}/image")
    async def admin_image(request: Request, post_id: int = Path(gt=0, le=_MAX_ID),
                          actor=Depends(admin_response)):
        media = await _photo_from_request(request)
        try:
            with database(readonly=False) as connection:
                # Lock and check the draft before touching persistent storage.
                # A published post must never receive a new asset, even as an
                # unreachable orphan.
                draft = repository(connection)._post_row(post_id, lock=True)
                if draft["published_at"] is not None:
                    raise CommunityError(409, "COMMUNITY_POST_ALREADY_PUBLISHED")
                image_url = media_storage_factory().save(media)
                return repository(connection).attach_image(post_id, image_url, actor["user_id"])
        except CommunityError as exc:
            # Keep a randomized asset after an ambiguous database commit rather
            # than risk deleting an image already referenced by a saved post.
            raise _error(exc) from exc
        except HTTPException:
            raise
        except Exception as exc:
            logger.warning("Community image persistence failed (%s)", type(exc).__name__)
            raise HTTPException(503, detail={"code": "COMMUNITY_MEDIA_UNAVAILABLE"}) from exc

    @router.post("/community/admin/posts/{post_id}/publish")
    async def admin_publish(post_id: int = Path(gt=0, le=_MAX_ID), body: CommunityPublishRequest = Body(...),
                             actor=Depends(admin_response)):
        with database(readonly=False) as connection:
            try:
                post, should_notify = repository(connection).claim_publication(
                    post_id, actor["user_id"], body.notify)
                devices = repository(connection).enabled_push_devices() if should_notify else []
            except CommunityError as exc:
                raise _error(exc) from exc
        if should_notify and devices:
            try:
                sender = push_sender
                if sender is None:
                    from app.app_push import send_app_pushes as sender

                result = await sender(
                    devices,
                    title="Наші ☕️",
                    body=post["title"],
                    data={"community_post_id": post["id"]},
                )
                if isinstance(result, dict):
                    errors = result.get("errors")
                    invalid = sum(
                        1 for error in errors if isinstance(error, dict)
                        and error.get("status") in {400, 404, 410}
                    ) if isinstance(errors, list) else 0
                    logger.info(
                        "Community push delivery completed: sent=%d failed=%d invalid=%d",
                        int(result.get("sent") or 0), int(result.get("failed") or 0), invalid,
                    )
            except Exception as exc:
                # The claim is deliberately already committed.  A later retry
                # must never send the same community notification twice.
                logger.warning("Community push delivery failed (%s)", type(exc).__name__)
        elif should_notify:
            logger.info("Community push skipped: no enabled iOS devices (sent=0 failed=0 invalid=0)")
        return post

    return router
