"""Superadmin Sales CRM. Contact delivery is always performed manually."""

from collections.abc import Callable, Collection

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from psycopg.errors import UndefinedTable

from app.sales_auth import build_sales_superadmin_guard
from app.sales_db import SalesError, SalesRepository
from app.sales_search import (
    SalesSearchProviderError,
    SalesSearchQuery,
    build_sales_search_provider,
)
from app.sales_models import (
    FollowupBucket,
    SalesEventCreate,
    SalesLeadCreate,
    SalesLeadUpdate,
    SalesStatus,
)


def _check_query(request: Request, allowed: set[str]) -> None:
    if set(request.query_params) - allowed or any(
        len(request.query_params.getlist(key)) != 1 for key in request.query_params
    ):
        raise HTTPException(422, detail={"code": "UNEXPECTED_PARAMETER"})


def build_sales_router(
    *, verify_actor: Callable, superadmin_telegram_ids: Collection[int],
    connection_factory: Callable | None = None,
    search_provider: Callable | None = None,
) -> APIRouter:
    require_sales_superadmin = build_sales_superadmin_guard(
        verify_actor=verify_actor,
        superadmin_telegram_ids=superadmin_telegram_ids,
    )
    router = APIRouter(prefix="/sales", tags=["sales"])

    def authenticated_actor(response: Response, actor=Depends(require_sales_superadmin)):
        response.headers["Cache-Control"] = "no-store"
        response.headers["Vary"] = "Authorization, X-Telegram-Init-Data"
        return actor

    def database_call(method: str, *args, **kwargs):
        if connection_factory is None:
            raise HTTPException(503, detail={"code": "SALES_SCHEMA_NOT_READY"})
        try:
            repository = SalesRepository(connection_factory)
            return getattr(repository, method)(*args, **kwargs)
        except SalesError as exc:
            detail = {"code": exc.code, "message": exc.message}
            if exc.extra:
                detail.update(exc.extra)
            raise HTTPException(exc.status_code, detail=detail) from exc
        except UndefinedTable as exc:
            # Migrations are applied explicitly, never during API startup.
            raise HTTPException(503, detail={"code": "SALES_SCHEMA_NOT_READY"}) from exc

    @router.get("/search")
    def search_places(
        request: Request,
        city: str = Query(..., min_length=1, max_length=160),
        radius_km: float = Query(default=10, gt=0, le=50),
        category: str = Query(default="cafe", min_length=1, max_length=80),
        name: str | None = Query(default=None, max_length=160),
        limit: int = Query(default=20, ge=1, le=20),
        actor=Depends(authenticated_actor),
    ):
        _check_query(request, {"city", "radius_km", "category", "name", "limit"})
        query = SalesSearchQuery(
            city=" ".join(city.split()),
            radius_km=radius_km,
            category=" ".join(category.split()),
            name=" ".join(name.split()) if name and name.strip() else None,
            limit=limit,
        )
        provider = search_provider() if callable(search_provider) and not hasattr(search_provider, "search") else search_provider
        if provider is None:
            provider = build_sales_search_provider()
        try:
            items = provider.search(query)
        except SalesSearchProviderError as exc:
            raise HTTPException(exc.status_code, detail={"code": exc.code, "message": exc.message}) from exc
        return {
            "items": items,
            "total": len(items),
            "limit": query.limit,
            "provider": getattr(provider, "name", "unknown"),
            "query": {
                "city": query.city,
                "radius_km": query.radius_km,
                "category": query.category,
                "name": query.name,
            },
        }

    @router.get("/me")
    def sales_me(request: Request, actor=Depends(authenticated_actor)):
        _check_query(request, set())
        return {"allowed": True, "user_id": actor["user_id"], "role": actor["role"]}

    @router.get("/leads")
    def list_leads(
        request: Request,
        actor=Depends(authenticated_actor),
        q: str | None = Query(default=None, max_length=160),
        status: SalesStatus | None = None,
        city: str | None = Query(default=None, max_length=160),
        followup: FollowupBucket | None = None,
        limit: int = Query(default=20, ge=1, le=100),
        offset: int = Query(default=0, ge=0, le=1_000_000),
    ):
        _check_query(request, {"q", "status", "city", "followup", "limit", "offset"})
        return database_call(
            "list_leads", q=q, status=status, city=city,
            followup=followup, limit=limit, offset=offset,
        )

    @router.post("/leads", status_code=201)
    def create_lead(request: Request, body: SalesLeadCreate, actor=Depends(authenticated_actor)):
        _check_query(request, set())
        return database_call("create_lead", body.model_dump(mode="python"), actor["user_id"])

    @router.get("/leads/{lead_id}")
    def get_lead(
        request: Request,
        lead_id: int = Path(gt=0, le=9_223_372_036_854_775_807),
        actor=Depends(authenticated_actor),
    ):
        _check_query(request, set())
        return database_call("get_lead", lead_id)

    @router.patch("/leads/{lead_id}")
    def update_lead(
        request: Request,
        body: SalesLeadUpdate,
        lead_id: int = Path(gt=0, le=9_223_372_036_854_775_807),
        actor=Depends(authenticated_actor),
    ):
        _check_query(request, set())
        return database_call("update_lead", lead_id, body.model_dump(exclude_unset=True), actor["user_id"])

    @router.post("/leads/{lead_id}/events", status_code=201)
    def create_event(
        request: Request,
        body: SalesEventCreate,
        lead_id: int = Path(gt=0, le=9_223_372_036_854_775_807),
        actor=Depends(authenticated_actor),
    ):
        _check_query(request, set())
        return database_call("add_event", lead_id, body.model_dump(exclude_unset=True), actor["user_id"])

    @router.get("/followups")
    def followups(
        request: Request,
        actor=Depends(authenticated_actor),
        overdue: bool = False,
        today: bool = False,
        upcoming: bool = False,
        limit: int = Query(default=20, ge=1, le=100),
        offset: int = Query(default=0, ge=0, le=1_000_000),
    ):
        _check_query(request, {"overdue", "today", "upcoming", "limit", "offset"})
        selected = [key for key, enabled in {"overdue": overdue, "today": today, "upcoming": upcoming}.items() if enabled]
        if len(selected) > 1:
            raise HTTPException(422, detail={"code": "INVALID_FOLLOWUP_FILTER"})
        return database_call("followups", bucket=selected[0] if selected else None, limit=limit, offset=offset)

    @router.get("/stats")
    def stats(request: Request, actor=Depends(authenticated_actor)):
        _check_query(request, set())
        return database_call("stats")

    return router
