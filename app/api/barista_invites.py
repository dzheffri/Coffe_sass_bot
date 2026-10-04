"""Verified identity bootstrap and current-owner invitation management."""

from typing import Callable, Literal

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from psycopg.errors import UndefinedTable

from app.admin_invites import (
    InviteError, accept_invite, create_invite, list_invites, revoke_invite,
)
from app.db import get_connection


class AcceptInviteRequest(BaseModel):
    provider: Literal["google", "apple"]
    id_token: str = Field(strict=True, min_length=1, max_length=16384)
    invite_code: str = Field(strict=True, pattern=r"^[0-9]{6}$")

    class Config:
        extra = "forbid"


class CreateInviteRequest(BaseModel):
    class Config:
        extra = "forbid"


def _http_error(exc: InviteError) -> HTTPException:
    return HTTPException(
        exc.status, detail={"code": exc.code},
        headers={"Retry-After": str(exc.retry_after)} if exc.retry_after else None,
    )


def _owner_context(session: dict) -> dict:
    selected = next(item for item in session["memberships"]
                    if item["membership_id"] == session["selected_membership_id"])
    if selected["role"] != "owner":
        raise HTTPException(403, detail={"code": "OWNER_ACCESS_REQUIRED"})
    return selected


def build_invite_router(*, authorize: Callable, parse_bearer: Callable,
                        verify_identity: Callable, me_response: Callable) -> APIRouter:
    # The parent router supplies /barista. No independent auth/session mechanism.
    router = APIRouter()

    @router.post("/auth/accept-invite")
    def accept(body: AcceptInviteRequest):
        user, subject = verify_identity(body.provider, body.id_token)
        try:
            token, session = accept_invite(
                provider=body.provider, subject=subject, user_id=user["id"], code=body.invite_code,
            )
        except InviteError as exc:
            raise _http_error(exc) from exc
        except UndefinedTable as exc:
            raise HTTPException(503, detail={"code": "INVITES_NOT_CONFIGURED"}) from exc
        return {"access_token": token, "token_type": "bearer", **me_response(session)}

    @router.post("/owner/invites", status_code=201)
    def create(body: CreateInviteRequest, authorization: str | None = Header(default=None)):
        try:
            with get_connection() as connection:
                with connection.transaction():
                    session = authorize(parse_bearer(authorization), connection=connection)
                    owner = _owner_context(session)
                    invitation = create_invite(connection, owner=owner, user_id=session["user_id"])
        except InviteError as exc:
            raise _http_error(exc) from exc
        except UndefinedTable as exc:
            raise HTTPException(503, detail={"code": "INVITES_NOT_CONFIGURED"}) from exc
        return {"ok": True, "invite": invitation,
                "shop": {"id": owner["shop_id"], "name": owner["name"]}}

    @router.get("/owner/invites")
    def invitations(authorization: str | None = Header(default=None)):
        try:
            with get_connection() as connection:
                with connection.transaction():
                    connection.execute("SET TRANSACTION READ ONLY")
                    session = authorize(parse_bearer(authorization), connection=connection, touch=False)
                    owner = _owner_context(session)
                    invitations = list_invites(connection, owner["shop_id"])
        except UndefinedTable as exc:
            raise HTTPException(503, detail={"code": "INVITES_NOT_CONFIGURED"}) from exc
        return {"ok": True, "invites": invitations,
                "shop": {"id": owner["shop_id"], "name": owner["name"]}}

    @router.delete("/owner/invites/{invite_id}")
    def revoke(invite_id: int, authorization: str | None = Header(default=None)):
        try:
            with get_connection() as connection:
                with connection.transaction():
                    session = authorize(parse_bearer(authorization), connection=connection)
                    owner = _owner_context(session)
                    revoke_invite(connection, owner["shop_id"], invite_id)
        except InviteError as exc:
            raise _http_error(exc) from exc
        except UndefinedTable as exc:
            raise HTTPException(503, detail={"code": "INVITES_NOT_CONFIGURED"}) from exc
        return {"ok": True, "revoked": True}

    return router
