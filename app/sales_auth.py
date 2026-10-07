"""Sales access policy using the existing verified Telegram actor."""

from collections.abc import Callable, Collection

from fastapi import Header, HTTPException


def build_sales_superadmin_guard(
    *, verify_actor: Callable, superadmin_telegram_ids: Collection[int],
) -> Callable:
    def require_sales_superadmin(
        authorization: str | None = Header(default=None),
        x_telegram_init_data: str | None = Header(default=None),
    ) -> dict:
        actor = verify_actor(authorization, x_telegram_init_data)
        if actor["telegram_id"] not in superadmin_telegram_ids:
            raise HTTPException(403, detail={"code": "SUPERADMIN_ACCESS_REQUIRED"})
        return {"user_id": actor["user_id"], "role": "superadmin"}

    return require_sales_superadmin
