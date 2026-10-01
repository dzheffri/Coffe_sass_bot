"""Native barista authentication only; no loyalty operations or account linking."""

import os
from typing import Callable, Literal

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from app.barista_sessions import (
    BaristaSessionError,
    get_barista_memberships,
    resolve_barista_session,
)
from app.db import create_app_session


class IdentityRequest(BaseModel):
    provider: Literal["google", "apple"]
    id_token: str = Field(min_length=1, max_length=16384)
    shop_id: int | None = Field(default=None, gt=0)

    class Config:
        extra = "forbid"


class ContextRequest(BaseModel):
    shop_id: int = Field(gt=0)

    class Config:
        extra = "forbid"


def _session_error(exc: BaristaSessionError) -> HTTPException:
    status = 401 if exc.code == "INVALID_SESSION" else 403
    return HTTPException(
        status_code=status,
        detail={"code": exc.code},
        headers={"WWW-Authenticate": "Bearer"} if status == 401 else None,
    )


def _me_response(session: dict) -> dict:
    selected = next(
        membership for membership in session["memberships"]
        if membership["membership_id"] == session["selected_membership_id"]
    )
    return {
        "ok": True,
        "employee": {
            "user_id": session["user_id"],
            "name": session["full_name"] or session["username"] or "Бариста",
        },
        "shops": session["memberships"],
        "selected_context": selected,
        "session": {
            "purpose": "barista",
            "expires_at": session["session_expires_at"],
            "idle_timeout_days": 30,
        },
    }


def build_barista_router(
    *, verify_google: Callable, verify_apple: Callable,
    find_user: Callable, parse_bearer: Callable,
) -> APIRouter:
    """Inject existing identity helpers without importing main or starting the bot."""
    router = APIRouter(prefix="/barista", tags=["barista-auth"])

    def authorize(token: str, **kwargs) -> dict:
        try:
            return resolve_barista_session(token, **kwargs)
        except BaristaSessionError as exc:
            raise _session_error(exc) from exc

    def current_barista(authorization: str | None = Header(default=None)) -> dict:
        return authorize(parse_bearer(authorization))

    @router.post("/auth/identity")
    def identity_login(body: IdentityRequest):
        if os.getenv("BARISTA_LOGIN_ENABLED", "false").strip().lower() != "true":
            raise HTTPException(503, detail={"code": "BARISTA_LOGIN_DISABLED"})
        env_name = (
            "BARISTA_GOOGLE_CLIENT_ID" if body.provider == "google"
            else "BARISTA_APPLE_CLIENT_ID"
        )
        audience = (os.getenv(env_name) or "").strip()
        if not audience:
            raise HTTPException(503, detail={"code": "IDENTITY_PROVIDER_NOT_CONFIGURED"})

        verifier = verify_google if body.provider == "google" else verify_apple
        identity = verifier(body.id_token, audience=audience)
        subject = identity.get("sub") if identity else None
        if not isinstance(subject, str) or not subject:
            raise HTTPException(401, detail={"code": "INVALID_IDENTITY"})

        user = find_user(body.provider, subject)
        if not user:
            # Linking an old Telegram account requires a separate verified flow.
            raise HTTPException(403, detail={"code": "IDENTITY_NOT_LINKED"})

        memberships = get_barista_memberships(user["id"])
        if not memberships:
            raise HTTPException(403, detail={"code": "STAFF_ACCESS_REQUIRED"})

        if body.shop_id is None:
            if len(memberships) != 1:
                raise HTTPException(409, detail={
                    "code": "SHOP_CONTEXT_REQUIRED", "shops": memberships,
                })
            selected = memberships[0]
        else:
            selected = next((item for item in memberships
                             if item["shop_id"] == body.shop_id), None)
            if selected is None:
                raise HTTPException(403, detail={"code": "SHOP_ACCESS_DENIED"})

        try:
            token = create_app_session(
                user["id"], purpose="barista",
                selected_membership_id=selected["membership_id"],
            )
        except ValueError as exc:
            raise HTTPException(403, detail={"code": "STAFF_ACCESS_REQUIRED"}) from exc
        session = authorize(token)
        return {"access_token": token, "token_type": "bearer", **_me_response(session)}

    @router.get("/me")
    def me(session=Depends(current_barista)):
        return _me_response(session)

    @router.post("/context")
    def select_context(body: ContextRequest,
                       authorization: str | None = Header(default=None)):
        session = authorize(parse_bearer(authorization), shop_id=body.shop_id)
        return _me_response(session)

    @router.post("/auth/logout")
    def logout(authorization: str | None = Header(default=None)):
        authorize(parse_bearer(authorization), logout=True)
        return {"ok": True, "revoked": True}

    return router
