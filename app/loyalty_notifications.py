"""Best-effort loyalty notifications shared by Telegram scanner and Barista API."""

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from app.config import BOT_TOKEN
from app.db import (
    touch_wallet_pass,
    get_wallet_push_tokens,
    get_app_push_devices_for_user,
)
from app.wallet_push import send_wallet_pushes
from app.app_push import send_app_pushes


async def refresh_wallet_and_send_app_push(
    user_id: int,
    title: str,
    body: str,
    event_type: str,
    shop_id: int,
):
    """
    После успешного начисления/списания:
    1) помечаем Wallet-pass обновлённым;
    2) отправляем Wallet silent push;
    3) отправляем обычный push в приложение.

    Любая ошибка push не должна ломать саму операцию с чашками.
    """

    # -------------------------
    # APPLE WALLET
    # -------------------------
    try:
        wallet_state = touch_wallet_pass(user_id)

        if wallet_state:
            serial_number = wallet_state["serial_number"]

            wallet_tokens = get_wallet_push_tokens(
                serial_number
            )

            wallet_result = await send_wallet_pushes(
                wallet_tokens
            )

            print(
                "📲 WALLET LOYALTY UPDATE:",
                f"user_id={user_id}",
                f"serial={serial_number}",
                f"devices={len(wallet_tokens)}",
                f"sent={wallet_result.get('sent', 0)}",
                f"failed={wallet_result.get('failed', 0)}",
            )

    except Exception as exc:
        print(
            "WALLET LOYALTY UPDATE ERROR:",
            repr(exc),
        )

    # -------------------------
    # APP PUSH
    # -------------------------
    try:
        devices = get_app_push_devices_for_user(
            user_id
        )

        if devices:
            push_result = await send_app_pushes(
                devices=devices,
                title=title,
                body=body,
                data={
                    "type": event_type,
                    "shop_id": shop_id,
                },
            )

            print(
                "🔔 APP LOYALTY PUSH:",
                f"user_id={user_id}",
                f"devices={len(devices)}",
                f"sent={push_result.get('sent', 0)}",
                f"failed={push_result.get('failed', 0)}",
            )

    except Exception as exc:
        print(
            "APP LOYALTY PUSH ERROR:",
            repr(exc),
        )


async def _send_telegram_notification(*, bot, telegram_user_id, text):
    """Reuse scanner bot, or open/close an API-only sender without polling."""
    if telegram_user_id is None:
        return

    owned_bot = None
    try:
        if bot is None:
            owned_bot = Bot(
                token=BOT_TOKEN,
                default=DefaultBotProperties(parse_mode=ParseMode.HTML),
            )
            bot = owned_bot

        await bot.send_message(telegram_user_id, text)
    except Exception:
        # Telegram delivery has always been best-effort in the scanner.
        pass
    finally:
        if owned_bot is not None:
            try:
                await owned_bot.session.close()
            except Exception:
                pass


async def notify_cups_added(
    *, bot, user_id, telegram_user_id, shop_id, shop_name,
    count, shop_client, earned_free,
):
    """Run the existing scanner notifications only after loyalty commit."""
    # Keep the scanner's title/body/events and client message unchanged.
    try:
        if earned_free > 0:
            push_title = "🎁 Безкоштовна кава вже ваша"
            push_body = (
                f"{shop_name}. "
                f"Нараховано {count} чашок. "
                f"Безкоштовних кав: "
                f"{shop_client['free_coffee_balance']}"
            )
            event_type = "free_coffee_earned"
        else:
            cup_word = "чашку" if count == 1 else "чашки"
            push_title = f"☕ Нараховано {count} {cup_word}"
            push_body = (
                f"{shop_name}. "
                f"Зараз у вас {shop_client['cups']}/7 чашок"
            )
            event_type = "cups_added"

        await refresh_wallet_and_send_app_push(
            user_id=user_id,
            title=push_title,
            body=push_body,
            event_type=event_type,
            shop_id=shop_id,
        )
    except Exception:
        pass

    try:
        notify_text = (
            f"☕ Тобі нарахували {count} чашок\n"
            f"🏪 {shop_name}\n"
            f"☕ Зараз: {shop_client['cups']}/7\n"
            f"🎁 Безкоштовних кав: {shop_client['free_coffee_balance']}"
        )

        if earned_free > 0:
            notify_text += f"\n✨ Додано безкоштовних кав: {earned_free}"

        await _send_telegram_notification(
            bot=bot,
            telegram_user_id=telegram_user_id,
            text=notify_text,
        )
    except Exception:
        pass


async def notify_free_redeemed(
    *, bot, user_id, telegram_user_id, shop_id, shop_name, shop_client,
):
    """Send the scanner's unchanged redemption events and client message."""
    try:
        await refresh_wallet_and_send_app_push(
            user_id=user_id,
            title="🎁 Безкоштовну каву використано",
            body=(
                f"{shop_name}. "
                f"Залишок безкоштовних кав: "
                f"{shop_client['free_coffee_balance']}"
            ),
            event_type="free_coffee_redeemed",
            shop_id=shop_id,
        )
    except Exception:
        pass

    try:
        await _send_telegram_notification(
            bot=bot,
            telegram_user_id=telegram_user_id,
            text=(
                f"✅ У тебе списали 1 безкоштовну каву\n"
                f"🏪 {shop_name}\n"
                f"☕ Чашок: {shop_client['cups']}/7\n"
                f"🎁 Залишок безкоштовних кав: {shop_client['free_coffee_balance']}"
            ),
        )
    except Exception:
        pass
