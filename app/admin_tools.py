"""Shop-scoped native owner tools using the existing membership/loyalty tables."""

import hashlib
import json
import secrets

from fastapi import HTTPException

from app.db import (can_send_broadcast, get_broadcast_recipients, get_shop_admins,
                    get_shop_marketing_efficiency, remove_shop_admin)

MAX_PENDING_PREVIEWS_PER_SHOP = 5
MAX_PREVIEWS_PER_SHOP_PER_HOUR = 10


def selected_shop(session):
    return next(item for item in session["memberships"]
                if item["membership_id"] == session["selected_membership_id"])


def require_owner(session):
    shop = selected_shop(session)
    if shop["role"] != "owner":
        raise HTTPException(403, detail={"code": "OWNER_REQUIRED"})
    return shop


def shop_response(shop):
    return {"id": shop["shop_id"], "name": shop["name"]}


def list_members(shop, connection):
    return [{"membership_id": row["membership_id"], "user_id": row["user_id"],
             "name": row["full_name"] or row["username"] or "Працівник",
             "avatar": None, "role": row["role"]}
            for row in get_shop_admins(shop["shop_id"], include_membership=True, connection=connection)
            if row["role"] in {"owner", "admin"}]


def remove_member(shop, membership_id, connection):
    # Lock the actual target before checking its role. A concurrent promotion
    # cannot turn the checked admin into an owner between check and deletion.
    target = connection.execute(
        "SELECT id,role FROM shop_admins WHERE id=%s AND shop_id=%s FOR UPDATE",
        (membership_id, shop["shop_id"]),
    ).fetchone()
    if target is None:
        raise HTTPException(404, detail={"code": "MEMBER_NOT_FOUND"})
    if target["role"] != "admin":
        raise HTTPException(409, detail={"code": "OWNER_CANNOT_BE_REMOVED"})
    if not remove_shop_admin(shop["shop_id"], membership_id=membership_id, connection=connection):
        raise HTTPException(409, detail={"code": "MEMBERSHIP_CHANGED"})


def read_statistics(shop, days, connection, *, actor_user_id: int):
    """Use the verified shop and session actor; admins see their own operations.

    cups_added is a purchase count, not QR scans. Exact scan events and gifts
    earned within a historical window are not persisted, so remain unavailable.
    """
    shop_id = shop["shop_id"]
    is_admin = shop["role"] == "admin"
    actor_filter = " AND admin_user_id=%s" if is_admin else ""
    transaction_parameters = (shop_id, days, actor_user_id) if is_admin else (shop_id, days)
    metrics = dict(connection.execute("""
        SELECT COALESCE(SUM(cups_added),0) AS cups_added,
               COALESCE(SUM(free_redeemed),0) AS free_redeemed,
               COUNT(*) FILTER (WHERE type='add_cups') AS add_operations,
               COUNT(*) FILTER (WHERE type='redeem_free') AS redeem_operations
        FROM transactions WHERE shop_id=%s
          AND created_at >= NOW()-(%s * INTERVAL '1 day')
    """ + actor_filter, transaction_parameters).fetchone())
    if is_admin:
        metrics["operations"] = metrics["add_operations"] + metrics["redeem_operations"]
    metrics.update(scans=None, free_earned=None)
    owner_keys = ("active_clients", "new_clients", "inactive_gt_7d", "inactive_gt_30d",
                  "returns_after_broadcast", "total_clients", "free_balance",
                  "free_earned_total", "free_redeemed_total")
    if shop["role"] == "owner":
        metrics.update(dict(connection.execute("""
            SELECT COUNT(*) AS total_clients,
                   COUNT(*) FILTER (WHERE last_activity_at >= NOW()-(%s*INTERVAL '1 day')) AS active_clients,
                   COUNT(*) FILTER (WHERE created_at >= NOW()-(%s*INTERVAL '1 day')) AS new_clients,
                   COUNT(*) FILTER (WHERE last_activity_at < NOW()-INTERVAL '7 days') AS inactive_gt_7d,
                   COUNT(*) FILTER (WHERE last_activity_at < NOW()-INTERVAL '30 days') AS inactive_gt_30d,
                   COALESCE(SUM(free_coffee_balance),0) AS free_balance,
                   COALESCE(SUM(total_free_coffee_earned),0) AS free_earned_total,
                   COALESCE(SUM(total_free_coffee_redeemed),0) AS free_redeemed_total
            FROM shop_clients WHERE shop_id=%s
        """, (days, days, shop_id)).fetchone()))
        # Retain the response key but use Web's exact recorded-return total,
        # including auto/broadcast and legacy types, for the selected period.
        marketing = get_shop_marketing_efficiency(shop_id, days=days, connection=connection)
        metrics["returns_after_broadcast"] = marketing["total_returns"]
    else:
        metrics.update({key: None for key in owner_keys})

    rows = connection.execute("""
        SELECT EXTRACT(ISODOW FROM created_at AT TIME ZONE 'Europe/Kyiv')::int AS weekday,
               COUNT(*) AS operations, COALESCE(SUM(cups_added),0) AS cups_added,
               COALESCE(SUM(free_redeemed),0) AS free_redeemed
        FROM transactions WHERE shop_id=%s
          AND created_at >= NOW()-(%s*INTERVAL '1 day')
    """ + actor_filter + """
        GROUP BY weekday ORDER BY weekday
    """, transaction_parameters).fetchall()
    by_day = {row["weekday"]: dict(row) for row in rows}
    weekdays = [by_day.get(day, {"weekday": day, "operations": 0, "cups_added": 0,
                                "free_redeemed": 0}) for day in range(1, 8)]
    recent = connection.execute("""
        SELECT t.id, COALESCE(NULLIF(u.full_name,''),NULLIF(u.username,''),'Клієнт') AS name,
               t.type,t.cups_added,t.free_redeemed,t.created_at
        FROM transactions t JOIN users u ON u.id=t.user_id
        WHERE t.shop_id=%s AND t.created_at >= NOW()-(%s*INTERVAL '1 day')
    """ + (" AND t.admin_user_id=%s" if is_admin else "") + """
        ORDER BY t.created_at DESC,t.id DESC LIMIT 20
    """, transaction_parameters).fetchall()
    return {"metrics": metrics, "activity_by_weekday": weekdays,
            "recent_actions": recent,
            "unavailable_metrics": [key for key, value in metrics.items() if value is None]}


def reachable_recipients(shop_id, connection):
    # This is the exact bot recipient helper, not an all-users query. A user
    # without Telegram cannot receive a Telegram message and is not counted.
    return [row for row in get_broadcast_recipients(shop_id, connection=connection)
            if row["telegram_user_id"] is not None]


def recipient_hash(recipients):
    keys = sorted((row["user_id"], row["telegram_user_id"]) for row in recipients)
    return hashlib.sha256(json.dumps(keys, separators=(",", ":")).encode()).hexdigest()


def preview_limits(session, shop, connection):
    counts = connection.execute("""
        SELECT COUNT(*) FILTER (WHERE created_at>clock_timestamp()-INTERVAL '1 hour') AS recent,
               COUNT(*) FILTER (WHERE used_at IS NULL AND invalidated_at IS NULL
                   AND expires_at>clock_timestamp() AND session_id<>%s) AS pending_other
        FROM admin_broadcast_previews WHERE shop_id=%s
    """, (session["session_id"], shop["shop_id"])).fetchone()
    if counts["recent"] >= MAX_PREVIEWS_PER_SHOP_PER_HOUR:
        raise HTTPException(429, detail={"code": "PREVIEW_RATE_LIMITED"})
    if counts["pending_other"] >= MAX_PENDING_PREVIEWS_PER_SHOP:
        raise HTTPException(429, detail={"code": "PREVIEW_LIMIT_REACHED"})


def cleanup_broadcast_previews(connection, shop_id=None):
    """Clear expired bytes and retain small one-use/rate metadata for 24 hours.

    The trusted maintenance task covers only this new table; an owner request
    cleans only its selected shop. SKIP LOCKED never waits on active claims.
    """
    condition = "AND shop_id=%s" if shop_id is not None else ""
    parameters = (shop_id,) if shop_id is not None else ()
    connection.execute("""
        WITH stale AS (
            SELECT token_hash FROM admin_broadcast_previews
            WHERE (expires_at<=clock_timestamp() OR used_at IS NOT NULL OR invalidated_at IS NOT NULL)
              AND (media_bytes IS NOT NULL OR (used_at IS NULL AND invalidated_at IS NULL))
        """ + condition + """
            ORDER BY expires_at FOR UPDATE SKIP LOCKED LIMIT 1000
        )
        UPDATE admin_broadcast_previews p SET media_bytes=NULL,
            invalidated_at=CASE WHEN p.used_at IS NULL THEN COALESCE(p.invalidated_at,clock_timestamp())
                ELSE p.invalidated_at END
        FROM stale WHERE p.token_hash=stale.token_hash
    """, parameters)
    connection.execute("""
        WITH stale AS (
            SELECT token_hash FROM admin_broadcast_previews
            WHERE created_at<clock_timestamp()-INTERVAL '24 hours'
        """ + condition + """
            ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1000
        )
        DELETE FROM admin_broadcast_previews p USING stale WHERE p.token_hash=stale.token_hash
    """, parameters)


def make_broadcast_preview(session, shop, text, connection, media=None):
    connection.execute("SELECT id FROM coffee_shops WHERE id=%s FOR NO KEY UPDATE", (shop["shop_id"],))
    cleanup_broadcast_previews(connection, shop["shop_id"])
    preview_limits(session, shop, connection)
    # One pending preview per session. Replaced bytes disappear immediately;
    # metadata remains so replacement cannot evade the hourly preview budget.
    connection.execute("""
        UPDATE admin_broadcast_previews SET invalidated_at=clock_timestamp(),media_bytes=NULL
        WHERE session_id=%s AND used_at IS NULL AND invalidated_at IS NULL
    """, (session["session_id"],))
    recipients = reachable_recipients(shop["shop_id"], connection)
    token = secrets.token_urlsafe(32)
    row = connection.execute("""
        INSERT INTO admin_broadcast_previews
            (token_hash,shop_id,owner_user_id,session_id,text,recipients_hash,expires_at,
             media_kind,media_filename,media_mime,media_bytes)
        VALUES (%s,%s,%s,%s,%s,%s,clock_timestamp()+INTERVAL '10 minutes',%s,%s,%s,%s)
        RETURNING expires_at
    """, (hashlib.sha256(token.encode()).hexdigest(), shop["shop_id"], session["user_id"],
          session["session_id"], text, recipient_hash(recipients),
          media["kind"] if media else None, media["filename"] if media else None,
          media["mime_type"] if media else None, media["bytes"] if media else None)).fetchone()
    return {"text": text, "recipients_count": len(recipients),
            "confirmation_token": token, "expires_at": row["expires_at"],
            "media": {key: media[key] for key in ("kind","filename","size_bytes","mime_type")} if media else None}


def claim_broadcast(session, shop, token, connection):
    # Same shop-before-preview order as preview creation/replacement.
    connection.execute("SELECT id FROM coffee_shops WHERE id=%s FOR NO KEY UPDATE", (shop["shop_id"],))
    preview = connection.execute("""
        SELECT *,media_bytes FROM admin_broadcast_previews WHERE token_hash=%s FOR UPDATE
    """, (hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
    if preview is None:
        raise HTTPException(404, detail={"code": "PREVIEW_NOT_FOUND"})
    if (preview["shop_id"] != shop["shop_id"] or preview["owner_user_id"] != session["user_id"]
            or preview["session_id"] != session["session_id"]):
        raise HTTPException(403, detail={"code": "PREVIEW_CONTEXT_MISMATCH"})
    if preview["used_at"] is not None:
        raise HTTPException(409, detail={"code": "BROADCAST_ALREADY_USED"})
    if preview["invalidated_at"] is not None:
        raise HTTPException(409, detail={"code": "PREVIEW_STALE"})
    # NOW() is the transaction start and can precede a long row-lock wait.
    valid = connection.execute("SELECT %s > clock_timestamp() AS valid", (preview["expires_at"],)).fetchone()["valid"]
    if not valid:
        raise HTTPException(409, detail={"code": "PREVIEW_EXPIRED"})
    recipients = reachable_recipients(shop["shop_id"], connection)
    if recipient_hash(recipients) != preview["recipients_hash"]:
        raise HTTPException(409, detail={"code": "PREVIEW_STALE"})
    if not recipients:
        raise HTTPException(409, detail={"code": "NO_RECIPIENTS"})
    if not can_send_broadcast(shop["shop_id"], connection=connection):
        raise HTTPException(429, detail={"code": "BROADCAST_LIMIT_REACHED"})
    media = ({"kind":preview["media_kind"],"filename":preview["media_filename"],
              "mime_type":preview["media_mime"],"bytes":bytes(preview["media_bytes"])}
             if preview["media_kind"] is not None and preview["media_bytes"] is not None else None)
    if preview["media_kind"] is not None and media is None:
        raise HTTPException(409, detail={"code": "PREVIEW_EXPIRED"})
    # Reserve/audit before sending. A process crash cannot replay this token;
    # retry requires a fresh explicit preview rather than duplicating messages.
    broadcast = connection.execute("""
        INSERT INTO broadcasts(shop_id,sender_user_id,text,recipients_count)
        VALUES(%s,%s,%s,0) RETURNING id
    """, (shop["shop_id"], session["user_id"], preview["text"] or "[media only]")).fetchone()
    # A quota query or audit INSERT can itself wait on a database lock. Check
    # the real clock one last time at the claim, after every blocking step.
    claimed = connection.execute("""
        UPDATE admin_broadcast_previews SET used_at=clock_timestamp(),media_bytes=NULL
        WHERE token_hash=%s AND expires_at>clock_timestamp() RETURNING token_hash
    """, (preview["token_hash"],)).fetchone()
    if claimed is None:
        raise HTTPException(409, detail={"code": "PREVIEW_EXPIRED"})
    return {"broadcast_id": broadcast["id"], "text": preview["text"], "recipients": recipients,
            "media": media}
