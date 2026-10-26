"""Approved new products: transactional inbox/outbox, APNs only, no Telegram.

The existing editor replaces product rows and does not retain item IDs. A shop's
normalized product title is its durable identity here; price/photo edits keep it.
Existing titles are seeded by migration without generating any deliveries.
"""

import asyncio
import logging
import re

logger = logging.getLogger(__name__)


def approve_shop_change(request_id, reviewer_telegram_id):
    """One approval transaction; never deliver inside the moderation request."""
    from fastapi import HTTPException
    from app.db import get_connection
    from app.web_panel_logic import update_shop_profile

    with get_connection() as connection:
        with connection.transaction():
            request = connection.execute("""SELECT id,shop_id,status,payload FROM shop_change_requests
                WHERE id=%s FOR UPDATE""", (request_id,)).fetchone()
            if request is None:
                raise HTTPException(404, detail="Заявку не знайдено")
            if request["status"] == "approved":
                return {"ok": True, "status": "approved", "request_id": request_id, "shop_id": request["shop_id"]}
            if request["status"] != "pending":
                raise HTTPException(400, detail="Заявка вже оброблена")
            shop_id = request["shop_id"]
            shop = connection.execute("""SELECT id FROM coffee_shops
                WHERE id=%s AND is_active IS TRUE FOR NO KEY UPDATE""", (shop_id,)).fetchone()
            if shop is None:
                raise HTTPException(404, detail="Кав’ярню не знайдено")
            owner = connection.execute("""SELECT sa.user_id,u.telegram_user_id FROM shop_admins sa
                JOIN users u ON u.id=sa.user_id WHERE sa.shop_id=%s AND sa.role='owner'
                ORDER BY sa.id LIMIT 1""", (shop_id,)).fetchone()
            if owner is None:
                raise HTTPException(400, detail="Власника кавʼярні не знайдено")
            previous = connection.execute("SELECT title FROM shop_news WHERE shop_id=%s", (shop_id,)).fetchall()
            remember_items(connection, shop_id, previous)
            payload = request["payload"] or {}
            items = payload.get("news") or []
            result = update_shop_profile(
                owner_telegram_id=owner["telegram_user_id"], authorized_shop_id=shop_id,
                connection=connection, news=items,
                **{field: payload.get(field, "") for field in (
                    "name", "subtitle", "address", "work_from", "work_to", "instagram",
                    "description", "logo_url", "cover_url")})
            if not result or not result.get("ok"):
                raise HTTPException(500, detail="Не вдалося застосувати зміни")
            record_approved_novelties(connection, request_id=request_id, shop_id=shop_id,
                                     sender_user_id=owner["user_id"], items=items)
            connection.execute("""UPDATE shop_change_requests SET status='approved',reviewed_at=NOW(),
                reviewed_by_telegram_id=%s,reject_reason=NULL WHERE id=%s""", (reviewer_telegram_id, request_id))
    return {"ok": True, "status": "approved", "request_id": request_id, "shop_id": shop_id}


def item_key(item):
    return re.sub(r"\s+", " ", str(item.get("title") or "").strip()).lower()


def remember_items(connection, shop_id, items):
    for key in {item_key(item) for item in items} - {""}:
        connection.execute("""INSERT INTO shop_novelty_items(shop_id,item_key)
            VALUES(%s,%s) ON CONFLICT DO NOTHING""", (shop_id, key))


def record_approved_novelties(connection, *, request_id, shop_id, sender_user_id, items):
    """Caller holds shop/request locks; commit profile, approval and inbox together."""
    from app.db import get_broadcast_recipients

    shop = connection.execute("SELECT name,is_active FROM coffee_shops WHERE id=%s", (shop_id,)).fetchone()
    if not shop or not shop["is_active"]:
        return None
    if connection.execute("SELECT 1 FROM shop_novelty_events WHERE moderation_request_id=%s",
                          (request_id,)).fetchone():
        return None
    newly_published = []
    for item in items:
        key = item_key(item)
        if not key:
            continue
        claimed = connection.execute("""INSERT INTO shop_novelty_items(shop_id,item_key)
            VALUES(%s,%s) ON CONFLICT DO NOTHING RETURNING item_key""", (shop_id, key)).fetchone()
        if claimed:
            newly_published.append(item)
    if not newly_published:
        return None
    title = f"☕️ Новинка у {shop['name']}"
    lines = []
    for item in newly_published:
        name = str(item.get("title") or "").strip()
        price = str(item.get("price") or "").strip()
        if price and not re.search(r"грн|₴", price, re.IGNORECASE):
            price += " грн"
        lines.append(f"{name} — {price}" if price else name)
    body = "\n".join(lines) + "\nЗазирніть спробувати ☕️"
    broadcast = connection.execute("""INSERT INTO broadcasts
        (shop_id,sender_user_id,text,recipients_count,origin)
        VALUES(%s,%s,%s,0,'shop_novelty') RETURNING id""",
        (shop_id, sender_user_id, body)).fetchone()
    broadcast_id = broadcast["id"]
    recipients = get_broadcast_recipients(shop_id, connection=connection, all_clients=True)
    user_ids = sorted({row["user_id"] for row in recipients})
    connection.execute("""INSERT INTO broadcast_recipients(broadcast_id,user_id)
        SELECT %s,u FROM unnest(%s::bigint[]) u ON CONFLICT DO NOTHING""", (broadcast_id, user_ids))
    connection.execute("""INSERT INTO shop_novelty_events
        (moderation_request_id,shop_id,broadcast_id,push_title,push_body)
        VALUES(%s,%s,%s,%s,%s)""", (request_id, shop_id, broadcast_id, title, body))
    # Respect the existing shop preference for automatic new-product pushes.
    settings = connection.execute("SELECT shop_news_enabled FROM shop_reminder_settings WHERE shop_id=%s",
                                  (shop_id,)).fetchone()
    if settings is None or settings["shop_news_enabled"] is not False:
        connection.execute("""INSERT INTO shop_novelty_deliveries(broadcast_id,user_id)
            SELECT %s,u FROM unnest(%s::bigint[]) u ON CONFLICT DO NOTHING""", (broadcast_id, user_ids))
    return broadcast_id


def claim_delivery():
    from app.db import get_connection

    with get_connection() as connection:
        with connection.transaction():
            # An external send whose acknowledgement was lost is never retried.
            connection.execute("""UPDATE shop_novelty_deliveries SET status='unknown',updated_at=NOW()
                WHERE status='sending' AND updated_at<NOW()-INTERVAL '10 minutes'""")
            row = connection.execute("""SELECT d.broadcast_id,d.user_id,e.shop_id,e.push_title,e.push_body
                FROM shop_novelty_deliveries d
                JOIN shop_novelty_events e ON e.broadcast_id=d.broadcast_id
                WHERE d.status='queued' ORDER BY d.updated_at,d.broadcast_id,d.user_id
                FOR UPDATE OF d SKIP LOCKED LIMIT 1""").fetchone()
            if row:
                connection.execute("""UPDATE shop_novelty_deliveries SET status='sending',updated_at=NOW()
                    WHERE broadcast_id=%s AND user_id=%s""", (row["broadcast_id"], row["user_id"]))
            return row


def delivery_devices(row):
    from app.db import get_app_push_devices_for_user, get_connection

    # The immutable recipient snapshot grants inbox access. A closed shop cannot
    # produce a later notification while its queue is draining.
    with get_connection() as connection:
        active = connection.execute("SELECT 1 FROM coffee_shops WHERE id=%s AND is_active IS TRUE",
                                    (row["shop_id"],)).fetchone()
    if not active:
        return []
    devices = get_app_push_devices_for_user(row["user_id"])
    unique = {}
    for device in devices:
        if device.get("platform", "ios") != "ios" or device.get("notifications_enabled") is False:
            continue
        key = (str(device.get("device_token") or "").strip().lower(), device.get("environment") or "production")
        if key[0]:
            unique[key] = device
    return list(unique.values())


def finish_delivery(row, *, status, attempted=0, sent=0, failed=0):
    from app.db import get_connection

    with get_connection() as connection:
        connection.execute("""UPDATE shop_novelty_deliveries
            SET status=%s,attempted=%s,sent=%s,failed=%s,updated_at=NOW()
            WHERE broadcast_id=%s AND user_id=%s AND status='sending'""",
            (status, attempted, sent, failed, row["broadcast_id"], row["user_id"]))
    logger.info("[shop novelty] broadcast_id=%s shop_id=%s status=%s attempted=%s sent=%s failed=%s",
                row["broadcast_id"], row["shop_id"], status, attempted, sent, failed)


async def deliver_one(*, sender=None):
    row = await asyncio.to_thread(claim_delivery)
    if row is None:
        return False
    attempted = 0
    try:
        devices = await asyncio.to_thread(delivery_devices, row)
        if not devices:
            await asyncio.to_thread(finish_delivery, row, status="skipped")
            return True
        attempted = len(devices)
        if sender is None:
            from app.app_push import send_app_pushes
            sender = send_app_pushes
        result = await sender(
            devices=devices, title=row["push_title"], body=row["push_body"],
            data={"type": "shop_news", "shop_id": row["shop_id"], "broadcast_id": row["broadcast_id"]})
        sent, failed = int(result.get("sent", 0)), int(result.get("failed", 0))
        await asyncio.to_thread(finish_delivery, row, status="sent" if sent else "failed",
                                attempted=attempted, sent=sent, failed=failed)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        # Unknown network outcomes never trigger a second push or Telegram DM.
        await asyncio.to_thread(finish_delivery, row, status="unknown", attempted=attempted)
        logger.warning("Shop novelty delivery uncertain (%s)", type(error).__name__)
    return True


async def worker():
    while True:
        try:
            if not await deliver_one():
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning("Shop novelty worker deferred (%s)", type(error).__name__)
            await asyncio.sleep(5)


def merge_novelty_receipts(cursor, source_user_id, target_user_id):
    """Retain sent/uncertain receipts before overlapping inbox rows are merged."""
    cursor.execute("""UPDATE shop_novelty_deliveries SET status='unknown',updated_at=NOW()
        WHERE user_id=%s AND status='sending'""", (source_user_id,))
    cursor.execute("SELECT * FROM shop_novelty_deliveries WHERE user_id=%s ORDER BY broadcast_id FOR UPDATE",
                   (source_user_id,))
    for source in cursor.fetchall():
        cursor.execute("SELECT 1 FROM broadcast_recipients WHERE broadcast_id=%s AND user_id=%s",
                       (source["broadcast_id"], target_user_id))
        if cursor.fetchone() is None:
            continue  # The parent recipient UPDATE cascades this source-only row.
        cursor.execute("SELECT * FROM shop_novelty_deliveries WHERE broadcast_id=%s AND user_id=%s FOR UPDATE",
                       (source["broadcast_id"], target_user_id))
        target = cursor.fetchone()
        priority = {"queued": 0, "skipped": 1, "failed": 2, "sending": 3, "unknown": 3, "sent": 4}
        if target is None:
            cursor.execute("""INSERT INTO shop_novelty_deliveries
                (broadcast_id,user_id,status,attempted,sent,failed,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s)""",
                (source["broadcast_id"], target_user_id, source["status"], source["attempted"],
                 source["sent"], source["failed"], source["updated_at"]))
        elif priority[source["status"]] > priority[target["status"]]:
            cursor.execute("""UPDATE shop_novelty_deliveries SET status=%s,attempted=%s,sent=%s,failed=%s,
                updated_at=NOW() WHERE broadcast_id=%s AND user_id=%s""",
                (source["status"], source["attempted"], source["sent"], source["failed"],
                 source["broadcast_id"], target_user_id))
