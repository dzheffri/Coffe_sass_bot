"""Confirmed shop broadcasts: atomic audience/content, existing delivery unchanged."""

import logging

from fastapi import HTTPException
from psycopg import InterfaceError, OperationalError

from app.broadcast_media_storage import BroadcastMediaStorage


logger = logging.getLogger(__name__)


class BroadcastPublication:
    """Wrap the DB transaction so failed publications release unreferenced files.

    This context must surround the connection/transaction context, including its
    commit. A lost commit response is ambiguous: never delete a referenced file
    or assume an unavailable database means that the transaction rolled back.
    """

    def __init__(self, storage=None):
        self.storage = storage
        self.saved_keys = []

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, traceback):
        if exception_type is not None and issubclass(exception_type, (InterfaceError, OperationalError)):
            # COMMIT can still be in flight after its acknowledgement is lost.
            # Even a fresh SELECT cannot prove that it will never become visible.
            if self.saved_keys:
                logger.warning("Broadcast media cleanup deferred (%s)", exception_type.__name__)
            return False
        if exception_type is not None:
            from app.db import get_connection

            for key in self.saved_keys:
                try:
                    with get_connection() as connection:
                        referenced = connection.execute(
                            "SELECT 1 FROM broadcasts WHERE media_key=%s LIMIT 1", (key,),
                        ).fetchone()
                    if referenced is None:
                        self.storage.delete(key)
                except Exception as error:
                    # A private orphan is safer than deleting committed media.
                    # Never mask the original DB/claim error or log media secrets.
                    logger.warning("Broadcast media cleanup deferred (%s)", type(error).__name__)
        return False

    def publish(self, connection, shop_id, sender_user_id, text, recipients, media=None,
                *, scope="SHOP", network_id=None):
        metadata = None
        if media is not None:
            try:
                if self.storage is None:
                    self.storage = BroadcastMediaStorage()
                metadata = self.storage.save(media)
                self.saved_keys.append(metadata["media_key"])
            except Exception as error:
                # Keep the original Telegram payload available to the caller.
                # NULL metadata never points to a missing/partial attachment.
                logger.warning("Broadcast media persistence failed (%s)", type(error).__name__)
        if scope == "NETWORK":
            if not network_id:
                raise ValueError("network_id is required for network broadcasts")
            broadcast = connection.execute("""
                INSERT INTO broadcasts(shop_id,sender_user_id,text,recipients_count,media_type,media_key,scope,network_id)
                VALUES(%s,%s,%s,0,%s,%s,'NETWORK',%s) RETURNING *
            """, (shop_id, sender_user_id, text,
                  metadata["media_type"] if metadata else None,
                  metadata["media_key"] if metadata else None, network_id)).fetchone()
        else:
            broadcast = connection.execute("""
                INSERT INTO broadcasts(shop_id,sender_user_id,text,recipients_count,media_type,media_key)
                VALUES(%s,%s,%s,0,%s,%s) RETURNING *
            """, (shop_id, sender_user_id, text,
                  metadata["media_type"] if metadata else None,
                  metadata["media_key"] if metadata else None)).fetchone()
        user_ids = sorted({row["user_id"] for row in recipients})
        connection.execute("""
            INSERT INTO broadcast_recipients(broadcast_id,user_id)
            SELECT %s, user_id FROM unnest(%s::bigint[]) AS audience(user_id)
            ON CONFLICT (broadcast_id,user_id) DO NOTHING
        """, (broadcast["id"], user_ids))
        return broadcast


def publish_bot_broadcast(shop_id, sender_telegram_user_id, text, media=None, *, quota_exempt=False):
    """Publish a bot confirmation using the same shop lock and 60-day helper."""
    from app.db import can_send_broadcast, get_broadcast_recipients, get_connection

    with BroadcastPublication() as publication:
        with get_connection() as connection:
            with connection.transaction():
                shop = connection.execute("""
                    SELECT id,name FROM coffee_shops
                    WHERE id=%s AND is_active IS TRUE FOR NO KEY UPDATE
                """, (shop_id,)).fetchone()
                if shop is None:
                    raise HTTPException(403, detail={"code": "SHOP_ACCESS_DENIED"})
                actor = connection.execute("""
                    SELECT u.id FROM users u JOIN shop_admins membership ON membership.user_id=u.id
                    WHERE u.telegram_user_id=%s AND membership.shop_id=%s AND membership.role='owner'
                """, (sender_telegram_user_id, shop_id)).fetchone()
                if actor is None:
                    raise HTTPException(403, detail={"code": "OWNER_REQUIRED"})
                if not quota_exempt and not can_send_broadcast(shop_id, connection=connection):
                    raise HTTPException(429, detail={"code": "BROADCAST_LIMIT_REACHED"})
                recipients = get_broadcast_recipients(shop_id, connection=connection)
                if not recipients:
                    raise HTTPException(409, detail={"code": "NO_RECIPIENTS"})
                row = publication.publish(connection, shop_id, actor["id"], text, recipients, media)
        return {"broadcast_id": row["id"], "shop_id": shop_id, "shop_name": shop["name"],
                "text": text, "recipients": recipients}


def record_broadcast_delivery(shop_id, broadcast_id, touched):
    """Preserve the old sent count/touch accounting without creating another row."""
    from app.db import get_connection, log_broadcast_touches

    with get_connection() as connection:
        with connection.transaction():
            connection.execute("UPDATE broadcasts SET recipients_count=%s WHERE id=%s AND shop_id=%s",
                               (len(touched), broadcast_id, shop_id))
            if touched:
                log_broadcast_touches(shop_id, touched, connection=connection)
