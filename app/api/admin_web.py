"""Nashi Admin browser pairing and actor-derived shop management.

Legacy Telegram endpoints remain unchanged. Native Web requests always obtain
their shop and owner from a live membership, never from a caller's Telegram ID.
"""

import json
from contextlib import contextmanager
from datetime import datetime, timezone

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field
from psycopg.errors import UndefinedTable

from app import db
from app.admin_invites import InviteError
from app.admin_web_login import (
    WebLoginError, approve_request, poll_request, resolve_web_session, start_request,
)
from app.admin_telegram_link import admin_link_status, start_admin_link
from app.barista_sessions import BaristaSessionError, resolve_barista_session
from app.network_management import network_dashboard


class EmptyBody(BaseModel):
    class Config:
        extra = "forbid"


class PairingInput(EmptyBody):
    code_or_token: str = Field(strict=True, min_length=6, max_length=256)


class PollInput(EmptyBody):
    request_token: str = Field(strict=True, min_length=32, max_length=256)
    poll_token: str = Field(strict=True, min_length=32, max_length=256)


class ShopContext(EmptyBody):
    shop_id: int = Field(strict=True, gt=0)


class TelegramAdmin(EmptyBody):
    telegram_id: int = Field(strict=True, gt=0)


def _http(exc):
    return HTTPException(exc.status, detail={"code": exc.code},
                         headers={"Retry-After": "300"} if exc.status == 429 else None)


@contextmanager
def _errors():
    try:
        yield
    except (WebLoginError, InviteError) as exc:
        raise _http(exc) from exc
    except BaristaSessionError as exc:
        raise HTTPException(401 if exc.code == "INVALID_SESSION" else 403,
                            detail={"code": exc.code}) from exc
    except UndefinedTable as exc:
        raise HTTPException(503, detail={"code": "ADMIN_ONBOARDING_NOT_CONFIGURED"}) from exc


def build_admin_web_router(*, parse_bearer, bot_username, shop_model, reminder_model,
                           analytics_functions):
    router = APIRouter(tags=["admin-web"])

    @contextmanager
    def web_actor(authorization, shop_id=None):
        with _errors(), db.get_connection() as conn:
            with conn.transaction():
                actor = resolve_web_session(conn, parse_bearer(authorization), shop_id=shop_id, owner_only=True)
                yield conn, actor

    @router.post("/auth/admin-web-login/start", status_code=201)
    def start(body: EmptyBody, request: Request):
        with _errors():
            # Trust the server's peer, not an arbitrary forwarded header.
            return start_request(request.client.host if request.client else "unknown")

    @router.post("/auth/admin-web-login/poll")
    def poll(body: PollInput):
        with _errors():
            return poll_request(body.request_token, body.poll_token)

    @router.post("/barista/web-login/inspect")
    def inspect(body: PairingInput, authorization: str | None = Header(default=None)):
        with _errors():
            return approve_request(body.code_or_token, parse_bearer(authorization), resolve_barista_session, confirm=False)

    @router.post("/barista/web-login/confirm")
    def confirm(body: PairingInput, authorization: str | None = Header(default=None)):
        with _errors():
            return approve_request(body.code_or_token, parse_bearer(authorization), resolve_barista_session, confirm=True)

    @router.post("/barista/telegram-link/start", status_code=201)
    def telegram_start(body: EmptyBody, authorization: str | None = Header(default=None)):
        with _errors():
            session = resolve_barista_session(parse_bearer(authorization))
            return start_admin_link(session, bot_username())

    @router.get("/barista/telegram-link/status/{token}")
    def telegram_status(token: str, authorization: str | None = Header(default=None)):
        with _errors():
            session = resolve_barista_session(parse_bearer(authorization))
            return admin_link_status(session, token)

    def shops_response(actor):
        return {"ok": True, "shops": actor["shops"], "selected_shop_id": actor["shop_id"],
                "networks": actor.get("networks", []),
                "actor": {"user_id": actor["user_id"], "role": actor["selected"]["role"]}}

    @router.get("/admin-web/shops")
    def shops(authorization: str | None = Header(default=None)):
        with web_actor(authorization) as (_, actor):
            return shops_response(actor)

    @router.post("/admin-web/context")
    def context(body: ShopContext, authorization: str | None = Header(default=None)):
        with web_actor(authorization, body.shop_id) as (_, actor):
            return shops_response(actor)

    @router.get("/admin-web/networks")
    def networks(authorization: str | None = Header(default=None)):
        with web_actor(authorization) as (_, actor):
            return {"ok": True, "networks": actor.get("networks", [])}

    @router.get("/admin-web/networks/{network_id}/statistics")
    def network_statistics(network_id: int, authorization: str | None = Header(default=None)):
        with web_actor(authorization) as (conn, actor):
            network = next((item for item in actor.get("networks", [])
                            if item["network_id"] == network_id), None)
            if network is None:
                raise HTTPException(403, detail={"code": "NETWORK_ACCESS_DENIED"})
            return {"ok": True, "network": network, **network_dashboard(conn, network_id)}

    @router.post("/admin-web/logout")
    def logout(body: EmptyBody, authorization: str | None = Header(default=None)):
        with web_actor(authorization) as (conn, actor):
            conn.execute("UPDATE admin_web_sessions SET revoked_at=statement_timestamp() WHERE id=%s", (actor["id"],))
        return {"ok": True}

    @router.get("/admin-web/shop")
    def shop(authorization: str | None = Header(default=None)):
        with web_actor(authorization) as (conn, actor):
            row = conn.execute("""SELECT id AS shop_id,name,city,address,subtitle,work_from,work_to,
                instagram,description,logo_url,cover_url FROM coffee_shops WHERE id=%s""", (actor["shop_id"],)).fetchone()
            news = conn.execute("""SELECT id,title,price,image_url,sort_order FROM shop_news
                WHERE shop_id=%s ORDER BY sort_order,id""", (actor["shop_id"],)).fetchall()
            return {"ok": True, "shop": {**row, "owner_telegram_id": actor["telegram_user_id"], "news": news}}

    @router.post("/admin-web/shop/submit-changes")
    def submit(body: shop_model, authorization: str | None = Header(default=None)):
        with web_actor(authorization) as (conn, actor):
            # Keep the existing moderation flow, now recording internal sender ID.
            payload = json.dumps(body.model_dump(), ensure_ascii=False)
            row = conn.execute("""SELECT id FROM shop_change_requests WHERE shop_id=%s
                AND status='pending' ORDER BY created_at DESC LIMIT 1 FOR UPDATE""", (actor["shop_id"],)).fetchone()
            if row:
                conn.execute("""UPDATE shop_change_requests SET submitted_by_user_id=%s,
                    submitted_by_telegram_id=%s,payload=%s::jsonb,reject_reason=NULL,created_at=statement_timestamp()
                    WHERE id=%s""", (actor["user_id"], actor["telegram_user_id"], payload, row["id"]))
            else:
                row = conn.execute("""INSERT INTO shop_change_requests
                    (shop_id,submitted_by_user_id,submitted_by_telegram_id,status,payload)
                    VALUES (%s,%s,%s,'pending',%s::jsonb) RETURNING id""",
                    (actor["shop_id"], actor["user_id"], actor["telegram_user_id"], payload)).fetchone()
            return {"ok": True, "status": "pending", "request_id": row["id"], "shop_id": actor["shop_id"],
                    "message": "Зміни збережено та надіслано на перевірку"}

    @router.get("/admin-web/settings/reminders")
    def reminders(authorization: str | None = Header(default=None)):
        with web_actor(authorization) as (_, actor):
            return {"ok": True, "shop_id": actor["shop_id"], "settings": db.get_shop_reminder_settings(actor["shop_id"])}

    @router.put("/admin-web/settings/reminders")
    def save_reminders(body: reminder_model, authorization: str | None = Header(default=None)):
        if any(not 1 <= n <= 7 for n in (body.one_left_days, body.free_coffee_days, body.inactive_5_7_days, body.inactive_14_30_days)):
            raise HTTPException(422, detail={"code": "INVALID_REMINDER_DAYS"})
        with web_actor(authorization) as (_, actor):
            fields = body.model_dump()
            if fields["shop_news_enabled"] is None:
                fields["shop_news_enabled"] = db.get_shop_reminder_settings(actor["shop_id"])["shop_news_enabled"]
            return {"ok": True, "shop_id": actor["shop_id"],
                    "settings": db.update_shop_reminder_settings(shop_id=actor["shop_id"], **fields)}

    @router.get("/admin-web/settings/subscription")
    def subscription(authorization: str | None = Header(default=None)):
        with web_actor(authorization) as (_, actor):
            row = db.get_subscription(actor["shop_id"])
            if row:
                end = row["expires_at"].replace(tzinfo=timezone.utc) if row["expires_at"].tzinfo is None else row["expires_at"]
                days = max(0, int(((end-datetime.now(timezone.utc)).total_seconds()+86399)//86400))
                row = {**row, "days_left": days, "status": "expired" if days == 0 else row["status"]}
            return {"ok": True, "shop_id": actor["shop_id"], "subscription": row}

    @router.get("/admin-web/settings/admins")
    def admins(authorization: str | None = Header(default=None)):
        with web_actor(authorization) as (conn, actor):
            rows = conn.execute("""SELECT sa.id AS membership_id,sa.user_id,u.telegram_user_id AS telegram_id,
                u.full_name,u.username,sa.role FROM shop_admins sa JOIN users u ON u.id=sa.user_id
                WHERE sa.shop_id=%s ORDER BY sa.role DESC,sa.id""", (actor["shop_id"],)).fetchall()
            return {"ok": True, "shop_id": actor["shop_id"], "admins": rows}

    @router.post("/admin-web/settings/admins")
    def add_admin(body: TelegramAdmin, authorization: str | None = Header(default=None)):
        with web_actor(authorization) as (conn, actor):
            user = conn.execute("""SELECT u.id FROM users u WHERE u.id=COALESCE(
                (SELECT user_id FROM user_identities WHERE provider='telegram' AND provider_user_id=%s),
                (SELECT id FROM users WHERE telegram_user_id=%s))""", (str(body.telegram_id), body.telegram_id)).fetchone()
            if not user:
                raise HTTPException(404, detail={"code": "USER_NOT_FOUND"})
            conn.execute("""INSERT INTO shop_admins(shop_id,user_id,role) VALUES (%s,%s,'admin')
                ON CONFLICT (shop_id,user_id) DO NOTHING""", (actor["shop_id"], user["id"]))
            return {"ok": True}

    @router.delete("/admin-web/settings/admins/{membership_id}")
    def remove_admin(membership_id: int, authorization: str | None = Header(default=None)):
        with web_actor(authorization) as (conn, actor):
            row = conn.execute("DELETE FROM shop_admins WHERE id=%s AND shop_id=%s AND role='admin' RETURNING id",
                               (membership_id, actor["shop_id"])).fetchone()
            if row is None:
                raise HTTPException(404, detail={"code": "MEMBERSHIP_NOT_FOUND"})
            return {"ok": True}

    @router.get("/admin-web/analytics/{section}")
    def analytics(section: str, authorization: str | None = Header(default=None)):
        if section not in analytics_functions:
            raise HTTPException(404)
        with web_actor(authorization) as (_, actor):
            return analytics_functions[section](actor["telegram_user_id"], authorized_shop_id=actor["shop_id"])

    return router
