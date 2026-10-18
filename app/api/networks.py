"""Super Admin network management and authenticated network-owner dashboard."""

from typing import Callable, Literal

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from psycopg.errors import UndefinedTable, UndefinedColumn

from app.admin_invites import InviteError
from app.db import get_connection
from app.network_management import (
    attach_shop, create_network, create_network_owner_invite, detach_shop,
    list_networks, network_dashboard, update_network,
)
from app.barista_sessions import BaristaSessionError


class NetworkInput(BaseModel):
    name: str = Field(strict=True, min_length=1, max_length=180)
    logo_url: str | None = Field(default=None, strict=True, max_length=2048)
    loyalty_mode: Literal["SHARED", "PER_SHOP"] = "PER_SHOP"

    class Config:
        extra = "forbid"


class NetworkUpdate(BaseModel):
    name: str | None = Field(default=None, strict=True, min_length=1, max_length=180)
    logo_url: str | None = Field(default=None, strict=True, max_length=2048)
    loyalty_mode: Literal["SHARED", "PER_SHOP"] | None = None

    class Config:
        extra = "forbid"


class ShopLink(BaseModel):
    shop_id: int = Field(strict=True, gt=0)
    class Config:
        extra = "forbid"


class EmptyBody(BaseModel):
    class Config:
        extra = "forbid"


def _raise_invite(exc: InviteError):
    raise HTTPException(exc.status, detail={"code": exc.code}) from exc


def build_superadmin_network_router(*, require_superadmin: Callable) -> APIRouter:
    router = APIRouter(tags=["coffee-networks"])

    @router.get("/superadmin/networks")
    def networks(current_user=Depends(require_superadmin)):
        try:
            with get_connection() as connection:
                with connection.transaction():
                    connection.execute("SET TRANSACTION READ ONLY")
                    return {"ok": True, "networks": list_networks(connection)}
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "NETWORKS_NOT_CONFIGURED"}) from exc

    @router.post("/superadmin/networks", status_code=201)
    def create(body: NetworkInput, current_user=Depends(require_superadmin)):
        try:
            with get_connection() as connection:
                with connection.transaction():
                    network = create_network(connection, **body.model_dump())
        except InviteError as exc:
            _raise_invite(exc)
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "NETWORKS_NOT_CONFIGURED"}) from exc
        return {"ok": True, "network": {**network, "shops": []}}

    @router.patch("/superadmin/networks/{network_id}")
    def update(network_id: int, body: NetworkUpdate, current_user=Depends(require_superadmin)):
        try:
            with get_connection() as connection:
                with connection.transaction():
                    network = update_network(connection, network_id, **body.model_dump(exclude_unset=True))
                    network["shops"] = connection.execute(
                        "SELECT id AS shop_id,name FROM coffee_shops WHERE network_id=%s ORDER BY id",
                        (network_id,),
                    ).fetchall()
        except InviteError as exc:
            _raise_invite(exc)
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "NETWORKS_NOT_CONFIGURED"}) from exc
        return {"ok": True, "network": network}

    @router.post("/superadmin/networks/{network_id}/shops")
    def add_shop(network_id: int, body: ShopLink, current_user=Depends(require_superadmin)):
        try:
            with get_connection() as connection:
                with connection.transaction():
                    result = attach_shop(connection, network_id, body.shop_id)
        except InviteError as exc:
            _raise_invite(exc)
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "NETWORKS_NOT_CONFIGURED"}) from exc
        return {"ok": True, **result}

    @router.delete("/superadmin/networks/{network_id}/shops/{shop_id}")
    def remove_shop(network_id: int, shop_id: int, current_user=Depends(require_superadmin)):
        try:
            with get_connection() as connection:
                with connection.transaction():
                    result = detach_shop(connection, network_id, shop_id)
        except InviteError as exc:
            _raise_invite(exc)
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "NETWORKS_NOT_CONFIGURED"}) from exc
        return {"ok": True, **result}

    @router.post("/superadmin/networks/{network_id}/owner-invite", status_code=201)
    def owner_invite(network_id: int, body: EmptyBody, current_user=Depends(require_superadmin)):
        try:
            with get_connection() as connection:
                with connection.transaction():
                    invite = create_network_owner_invite(connection, network_id)
        except InviteError as exc:
            _raise_invite(exc)
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "NETWORKS_NOT_CONFIGURED"}) from exc
        return {"ok": True, "invite": invite}

    return router


def build_barista_network_router(*, authorize: Callable, parse_bearer: Callable) -> APIRouter:
    router = APIRouter(tags=["barista-network"])

    def session_for(authorization, *, connection=None, touch=True):
        try:
            return authorize(parse_bearer(authorization), connection=connection, touch=touch)
        except BaristaSessionError as exc:
            raise HTTPException(401 if exc.code == "INVALID_SESSION" else 403,
                                detail={"code": exc.code}) from exc

    @router.get("/owner/networks")
    def owner_networks(authorization: str | None = Header(default=None)):
        session = session_for(authorization, touch=False)
        return {"ok": True, "networks": session.get("networks", [])}

    @router.get("/owner/networks/{network_id}/statistics")
    def owner_network_statistics(network_id: int, authorization: str | None = Header(default=None)):
        try:
            with get_connection() as connection:
                with connection.transaction():
                    connection.execute("SET TRANSACTION READ ONLY")
                    session = session_for(authorization, connection=connection, touch=False)
                    network = next((item for item in session.get("networks", [])
                                    if item["network_id"] == network_id), None)
                    if network is None:
                        raise HTTPException(403, detail={"code": "NETWORK_ACCESS_DENIED"})
                    return {"ok": True, "network": network,
                            **network_dashboard(connection, network_id)}
        except (UndefinedTable, UndefinedColumn) as exc:
            raise HTTPException(503, detail={"code": "NETWORKS_NOT_CONFIGURED"}) from exc

    return router
