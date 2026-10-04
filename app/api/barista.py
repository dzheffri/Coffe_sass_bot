"""Native barista sessions and shop-scoped QR loyalty operations."""

import logging
import os
import re
from typing import Callable, Literal

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from app.barista_sessions import (
    BaristaSessionError,
    get_barista_memberships,
    resolve_barista_session,
)
from app.db import (
    LOYALTY_TARGET,
    add_cups_for_shop_client,
    create_app_session,
    get_connection,
    get_shop_client_balance_by_user_id,
    get_user_by_qr_token,
    redeem_free_for_shop_client,
    subscription_is_active,
)
from app.loyalty_notifications import notify_cups_added, notify_free_redeemed


logger = logging.getLogger(__name__)


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


class QRRequest(BaseModel):
    qr_token: str = Field(strict=True, min_length=1, max_length=512)

    class Config:
        extra = "forbid"


class AddCupRequest(QRRequest):
    # Existing clients omit count and keep the original single-cup behavior.
    # Strict validation also rejects booleans and numeric strings/floats.
    count: int = Field(default=1, strict=True, ge=1, le=10)


def _qr_token(value: str) -> str:
    token = value.strip()
    if token.startswith("coffee:"):
        token = token[len("coffee:"):]
    if re.fullmatch(r"[A-Za-z0-9_-]{1,256}", token) is None:
        raise HTTPException(400, detail={"code": "INVALID_QR"})
    return token


def _selected_membership(session: dict) -> dict:
    return next(
        item for item in session["memberships"]
        if item["membership_id"] == session["selected_membership_id"]
    )


def _client_response(user: dict, balance: dict | None, shop: dict) -> dict:
    cups = balance["cups"] if balance else 0
    free_coffee = balance["free_coffee_balance"] if balance else 0
    return {
        "ok": True,
        "client": {
            "name": user["full_name"] or user["username"] or "Клієнт",
            "avatar": None,
            "cups": cups,
            "free_coffee_balance": free_coffee,
            "loyalty_target": LOYALTY_TARGET,
            "progress": cups / LOYALTY_TARGET,
        },
        "shop": {"id": shop["shop_id"], "name": shop["name"]},
    }


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
    router = APIRouter(prefix="/barista", tags=["barista"])

    def authorize(token: str, **kwargs) -> dict:
        try:
            return resolve_barista_session(token, **kwargs)
        except BaristaSessionError as exc:
            raise _session_error(exc) from exc

    def current_barista(authorization: str | None = Header(default=None)) -> dict:
        return authorize(parse_bearer(authorization))

    def lookup_client(token: str, connection) -> dict:
        user = get_user_by_qr_token(token, connection=connection)
        if user is None:
            raise HTTPException(404, detail={"code": "CLIENT_NOT_FOUND"})
        return user

    def write_operation(token: str, qr: str, operation: str, count: int = 1) -> tuple[dict, dict]:
        # Membership/session locks acquired by authorization live until this
        # outer transaction commits, including the shared loyalty write/logs.
        with get_connection() as connection:
            with connection.transaction():
                session = authorize(token, connection=connection)
                shop = _selected_membership(session)
                user = lookup_client(_qr_token(qr), connection)
                if not subscription_is_active(shop["shop_id"], connection=connection):
                    raise HTTPException(403, detail={"code": "SHOP_SUBSCRIPTION_INACTIVE"})

                values = {
                    "shop_id": shop["shop_id"],
                    "client_user_id": user["id"],
                    "admin_user_id": session["user_id"],
                    "connection": connection,
                }
                if operation == "add_cup":
                    result = add_cups_for_shop_client(**values, count=count)
                    balance = result["shop_client"]
                    earned_free = result["earned_free"]
                    operation_response = {
                        "type": "add_cup", "cups_added": count,
                        "free_coffee_earned": earned_free,
                    }
                else:
                    balance = redeem_free_for_shop_client(**values)
                    if balance == "NOT_FOUND":
                        raise HTTPException(409, detail={"code": "SHOP_CLIENT_NOT_FOUND"})
                    if balance == "EMPTY":
                        raise HTTPException(409, detail={"code": "NO_FREE_COFFEE"})
                    earned_free = 0
                    operation_response = {"type": "redeem", "free_redeemed": 1}

                response = _client_response(user, balance, shop)
                response["operation"] = operation_response
                notification = {
                    "bot": None,
                    "user_id": user["id"],
                    "telegram_user_id": user["telegram_user_id"],
                    "shop_id": shop["shop_id"],
                    "shop_name": shop["name"],
                    "shop_client": balance,
                }
                if operation == "add_cup":
                    notification.update(count=count, earned_free=earned_free)
        return response, notification

    async def perform_operation(token: str, qr: str, operation: str, count: int = 1) -> dict:
        # Synchronous PostgreSQL work runs off the event loop. Notifications
        # happen after commit and cannot turn a saved purchase into a failure.
        response, notification = await run_in_threadpool(write_operation, token, qr, operation, count)
        notifier = notify_cups_added if operation == "add_cup" else notify_free_redeemed
        try:
            await notifier(**notification)
        except Exception as exc:
            logger.warning("Barista notification delivery failed (%s)", type(exc).__name__)
        return response

    def verified_identity(provider: str, id_token: str) -> tuple[dict, str]:
        if os.getenv("BARISTA_LOGIN_ENABLED", "false").strip().lower() != "true":
            raise HTTPException(503, detail={"code": "BARISTA_LOGIN_DISABLED"})
        env_name = (
            "BARISTA_GOOGLE_CLIENT_ID" if provider == "google"
            else "BARISTA_APPLE_CLIENT_ID"
        )
        audience = (os.getenv(env_name) or "").strip()
        if not audience:
            raise HTTPException(503, detail={"code": "IDENTITY_PROVIDER_NOT_CONFIGURED"})

        verifier = verify_google if provider == "google" else verify_apple
        identity = verifier(id_token, audience=audience)
        subject = identity.get("sub") if identity else None
        if not isinstance(subject, str) or not subject:
            raise HTTPException(401, detail={"code": "INVALID_IDENTITY"})

        user = find_user(provider, subject)
        if not user:
            # Linking an old Telegram account requires a separate verified flow.
            raise HTTPException(403, detail={"code": "IDENTITY_NOT_LINKED"})

        return user, subject

    @router.post("/auth/identity")
    def identity_login(body: IdentityRequest):
        user, _ = verified_identity(body.provider, body.id_token)

        memberships = get_barista_memberships(user["id"])
        if not memberships:
            raise HTTPException(403, detail={"code": "NEEDS_INVITE"})

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

    @router.post("/scan")
    def scan(body: QRRequest, authorization: str | None = Header(default=None)):
        with get_connection() as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                session = authorize(parse_bearer(authorization), connection=connection, touch=False)
                shop = _selected_membership(session)
                user = lookup_client(_qr_token(body.qr_token), connection)
                balance = get_shop_client_balance_by_user_id(
                    shop["shop_id"], user["id"], connection=connection,
                )
                return _client_response(user, balance, shop)

    @router.post("/add-cup")
    async def add_cup(body: AddCupRequest, authorization: str | None = Header(default=None)):
        return await perform_operation(parse_bearer(authorization), body.qr_token, "add_cup", body.count)

    @router.post("/redeem")
    async def redeem(body: QRRequest, authorization: str | None = Header(default=None)):
        return await perform_operation(parse_bearer(authorization), body.qr_token, "redeem")

    from app.api.barista_invites import build_invite_router
    from app.api.barista_owner import build_owner_router

    router.include_router(build_invite_router(
        authorize=authorize, parse_bearer=parse_bearer,
        verify_identity=verified_identity, me_response=_me_response,
    ))
    router.include_router(build_owner_router(authorize=authorize, parse_bearer=parse_bearer))

    return router
