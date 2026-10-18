"""Super Admin shop creation and one-time owner invitations."""

from typing import Callable

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator
from psycopg.errors import UndefinedColumn, UndefinedTable

from app.admin_invites import InviteError
from app.db import get_connection
from app.owner_onboarding import create_owner_invite
from app.network_management import ensure_network_shop_memberships


class CreateShopRequest(BaseModel):
    name: str = Field(strict=True, min_length=1, max_length=180)
    subtitle: str = Field(default="", strict=True, max_length=500)
    city: str = Field(default="", strict=True, max_length=180)
    address: str = Field(default="", strict=True, max_length=500)
    work_from: str = Field(default="", strict=True, max_length=32)
    work_to: str = Field(default="", strict=True, max_length=32)
    instagram: str = Field(default="", strict=True, max_length=500)
    description: str = Field(default="", strict=True, max_length=20000)
    logo_url: str = Field(default="", strict=True, max_length=2048)
    cover_url: str = Field(default="", strict=True, max_length=2048)
    network_id: int | None = Field(default=None, strict=True, gt=0)

    @field_validator("name")
    @classmethod
    def nonblank_name(cls, value):
        if not value.strip():
            raise ValueError("A shop name is required")
        return value.strip()

    class Config:
        extra = "forbid"


class OwnerInviteRequest(BaseModel):
    class Config:
        extra = "forbid"


def owner_metadata(row: dict) -> dict:
    owner = row.get("owner_info") or {}
    return {
        "owner_connected": owner.get("user_id") is not None,
        "owner_user_id": owner.get("user_id"),
        "owner_name": owner.get("name"),
        "telegram_connected": bool(owner.get("telegram_connected")),
    }


def build_shop_onboarding_router(*, require_superadmin: Callable) -> APIRouter:
    router = APIRouter()

    @router.post("/superadmin/shops", status_code=201)
    def create(body: CreateShopRequest, current_user=Depends(require_superadmin)):
        values = body.model_dump()
        with get_connection() as connection:
            with connection.transaction():
                with connection.cursor() as cur:
                    fields = tuple(values[key] for key in ("name", "subtitle", "city", "address", "work_from", "work_to",
                                                           "instagram", "description", "logo_url", "cover_url"))
                    if values["network_id"] is None:
                        cur.execute(
                            """INSERT INTO coffee_shops(name,subtitle,city,address,work_from,work_to,
                                   instagram,description,logo_url,cover_url)
                               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id,is_active""", fields,
                        )
                        network_id = None
                    else:
                        if cur.execute("SELECT id FROM coffee_networks WHERE id=%s FOR SHARE",
                                       (values["network_id"],)).fetchone() is None:
                            raise HTTPException(404, detail={"code": "NETWORK_NOT_FOUND"})
                        cur.execute(
                            """INSERT INTO coffee_shops(name,subtitle,city,address,work_from,work_to,
                                   instagram,description,logo_url,cover_url,network_id)
                               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id,is_active,network_id""",
                            fields + (values["network_id"],),
                        )
                        network_id = values["network_id"]
                    shop = cur.fetchone()
                    if network_id is not None:
                        ensure_network_shop_memberships(connection, network_id, shop["id"])
                    # Match existing Telegram-created shop trial behavior.
                    cur.execute(
                        """INSERT INTO subscriptions(shop_id,plan,status,expires_at)
                           VALUES(%s,'trial','active',statement_timestamp()+INTERVAL '7 days')""", (shop["id"],),
                    )
        return {"ok": True, "shop": {"shop_id": shop["id"], **values,
                "network_id": network_id, "is_active": shop["is_active"],
                "owner_telegram_id": None, "news": [], **owner_metadata({})}}

    @router.post("/superadmin/shops/{shop_id}/owner-invite", status_code=201)
    def invite(shop_id: int, body: OwnerInviteRequest, current_user=Depends(require_superadmin)):
        try:
            with get_connection() as connection:
                with connection.transaction():
                    invitation = create_owner_invite(connection, shop_id)
        except InviteError as exc:
            raise HTTPException(exc.status, detail={"code": exc.code}) from exc
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "INVITES_NOT_CONFIGURED"}) from exc
        return {"ok": True, "invite": invitation}

    return router
