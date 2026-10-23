"""Super Admin QR materials and deliberately small, anonymous public surface."""

from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Collection, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, ConfigDict, Field
from psycopg.errors import UndefinedColumn, UndefinedTable
from starlette.datastructures import UploadFile

from app.broadcast_media import PHOTO_LIMIT, bounded_body, validate_media
from app.community_media import CommunityMediaStorage
from app.qr_materials import (
    KINDS, QrError, QrMaterialsRepository, destinations, platform_from_user_agent, safe_image_url,
)


class QrDesignInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    template: Literal["minimal", "light", "dark", "photo"] = "minimal"
    display_name: str = Field(default="", strict=True, max_length=180)
    title: str = Field(default="Приєднуйся до програми лояльності", strict=True, max_length=180)
    subtitle: str = Field(default="Скануй QR та обирай зручний спосіб", strict=True, max_length=300)
    footer: str = Field(default="Telegram або застосунок «Наші»", strict=True, max_length=180)
    background_mode: Literal["color", "gradient", "image"] = "color"
    background_color: str = Field(default="#F6E9BA", pattern=r"^#[0-9A-Fa-f]{6}$")
    gradient_color: str = Field(default="#FFFFFF", pattern=r"^#[0-9A-Fa-f]{6}$")
    background_image_url: str | None = Field(default=None, max_length=2048)
    background_dim: float = Field(default=0, ge=0, le=0.85, allow_inf_nan=False)
    background_blur: float = Field(default=0, ge=0, le=16, allow_inf_nan=False)
    background_scale: float = Field(default=1, ge=1, le=2, allow_inf_nan=False)
    background_x: float = Field(default=50, ge=0, le=100, allow_inf_nan=False)
    background_y: float = Field(default=50, ge=0, le=100, allow_inf_nan=False)
    show_logo: bool = Field(default=True, strict=True)
    logo_url: str | None = Field(default=None, max_length=2048)
    logo_size: int = Field(default=160, strict=True, ge=80, le=240)
    logo_position: Literal["top", "above_qr", "center_top"] = "top"
    text_color: Literal["light", "dark"] = "dark"
    text_size: Literal["small", "medium", "large"] = "medium"


class QrPageOpen(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["QR_PAGE_OPEN"] = "QR_PAGE_OPEN"
    event_id: UUID | None = None


class QrMediaStorage:
    """Reuse existing validated, atomic upload operations in a separate QR folder."""
    def __init__(self, root: str | Path = "/data/uploads/qr"):
        self.storage = CommunityMediaStorage(root)

    def save(self, media: dict) -> str:
        path = self.storage.save(media)
        return path.replace("/uploads/community/", "/uploads/qr/", 1)


def build_qr_materials_router(
    *, verify_actor: Callable, superadmin_telegram_ids: Collection[int], connection_factory: Callable,
    media_storage_factory: Callable = QrMediaStorage,
) -> APIRouter:
    router = APIRouter(tags=["qr-materials"])

    def admin(
        response: Response,
        authorization: str | None = Header(default=None),
        x_telegram_init_data: str | None = Header(default=None),
    ):
        actor = verify_actor(authorization, x_telegram_init_data)
        if actor.get("telegram_id") not in superadmin_telegram_ids:
            raise HTTPException(403, detail={"code": "SUPERADMIN_ACCESS_REQUIRED"})
        response.headers["Cache-Control"] = "private, no-store"
        response.headers["Vary"] = "Authorization, X-Telegram-Init-Data"
        return actor

    @contextmanager
    def database():
        try:
            with connection_factory() as connection:
                with connection.transaction():
                    yield QrMaterialsRepository(connection)
        except QrError as exc:
            raise HTTPException(exc.status, detail={"code": exc.code}) from exc
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "QR_SCHEMA_NOT_READY"}) from exc

    def public_headers(response: Response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Content-Type-Options"] = "nosniff"

    @router.get("/qr/admin/me")
    def me(actor=Depends(admin)):
        return {"ok": True, "role": "superadmin"}

    @router.get("/superadmin/qr-materials")
    def list_materials(actor=Depends(admin)):
        with database() as repository:
            return {"ok": True, "shops": repository.list_shops()}

    @router.get("/superadmin/qr-materials/stats")
    def global_stats(period: Literal["7d", "30d", "all"] = Query(default="7d"), actor=Depends(admin)):
        with database() as repository:
            return {"ok": True, **repository.stats(period)}

    @router.get("/superadmin/shops/{shop_id}/qr-material")
    def get_material(shop_id: int, actor=Depends(admin)):
        with database() as repository:
            return {"ok": True, "material": repository.material(shop_id)}

    @router.put("/superadmin/shops/{shop_id}/qr-material")
    def save_material(shop_id: int, body: QrDesignInput, actor=Depends(admin)):
        with database() as repository:
            shop = repository.shop(shop_id)
            data = body.model_dump()
            for key in ("logo_url", "background_image_url"):
                value = data[key]
                if value and value not in {shop.get("logo_url"), shop.get("cover_url")}:
                    try:
                        data[key] = safe_image_url(value)
                    except ValueError as exc:
                        raise HTTPException(422, detail={"code": "INVALID_QR_IMAGE"}) from exc
            return {"ok": True, "material": repository.material(shop_id, data)}

    @router.get("/superadmin/shops/{shop_id}/qr-material/stats")
    def shop_stats(shop_id: int, period: Literal["7d", "30d", "all"] = Query(default="7d"), actor=Depends(admin)):
        with database() as repository:
            return {"ok": True, **repository.stats(period, shop_id)}

    @router.post("/superadmin/shops/{shop_id}/qr-material/image", status_code=201)
    async def upload_image(shop_id: int, request: Request, actor=Depends(admin)):
        with database() as repository:
            repository.shop(shop_id)
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
            async with bounded.form(max_files=1, max_fields=0, max_part_size=PHOTO_LIMIT) as form:
                pairs = list(form.multi_items())
                if len(pairs) != 1 or pairs[0][0] != "file" or not isinstance(pairs[0][1], UploadFile):
                    raise HTTPException(422, detail={"code": "INVALID_MEDIA"})
                file = pairs[0][1]
                media = validate_media("photo", file.filename or "photo", await file.read(PHOTO_LIMIT + 1))
            return {"ok": True, "url": media_storage_factory().save(media)}
        except HTTPException:
            raise
        except ValueError as exc:
            raise HTTPException(422, detail={"code": "INVALID_MEDIA"}) from exc

    @router.get("/public/qr/{token}")
    def public_material(token: str, response: Response):
        public_headers(response)
        with database() as repository:
            material = repository.public_material(token)
        return {"name": material["name"], "logo_url": material["logo_url"], "brand": "Наші",
                "destinations": [{"kind": kind, "url": f"/public/qr/{token}/click/{kind}"} for kind in destinations()]}

    @router.post("/public/qr/{token}/events")
    def page_open(token: str, body: QrPageOpen, request: Request, response: Response):
        public_headers(response)
        with database() as repository:
            material = repository.public_material(token)
            recorded = repository.record_event(material["id"], "QR_PAGE_OPEN",
                platform_from_user_agent(request.headers.get("user-agent", "")), body.event_id)
        return {"ok": True, "recorded": recorded}

    @router.get("/public/qr/{token}/click/{kind}")
    def click(token: str, kind: Literal["telegram", "app_store", "google_play"], request: Request):
        # Never honour query parameters, forwarded hosts, or client-provided destinations.
        if request.query_params:
            raise HTTPException(422, detail={"code": "UNEXPECTED_PARAMETER"})
        target = destinations().get(kind)
        if target is None:
            raise HTTPException(404, detail={"code": "QR_DESTINATION_NOT_AVAILABLE"})
        with database() as repository:
            material = repository.public_material(token)
            try:
                repository.record_event(material["id"], KINDS[kind],
                    platform_from_user_agent(request.headers.get("user-agent", "")))
            except QrError as exc:
                # Analytics abuse must not prevent a real visitor reaching the
                # fixed legitimate destination. Suppress only the event cap.
                if exc.status != 429:
                    raise
        response = RedirectResponse(target, status_code=302)
        public_headers(response)
        return response

    return router
