from app.db import get_active_shop_for_user, get_shop_client_balance


def get_user_cups_data(telegram_user_id: int) -> dict:
    active_shop = get_active_shop_for_user(telegram_user_id)

    if not active_shop:
        return {
            "ok": False,
            "error": "no_active_shop",
            "shop_name": None,
            "cups": 0,
            "free_coffee_balance": 0,
        }

    balance = get_shop_client_balance(active_shop["id"], telegram_user_id)

    if not balance:
        return {
            "ok": False,
            "error": "no_balance",
            "shop_name": active_shop["name"],
            "cups": 0,
            "free_coffee_balance": 0,
        }

    return {
        "ok": True,
        "error": None,
        "shop_name": balance["shop_name"],
        "cups": balance["cups"],
        "free_coffee_balance": balance["free_coffee_balance"],
    }
