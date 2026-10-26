"""Durable delivery for NEW native confirmations; no legacy broadcast replay.

One PostgreSQL session advisory lock owns a job across short committed claims.
After a crash, unfinished external attempts become unknown and are not resent.
Only untouched recipients are recovered. Inbox publication is already complete.
"""

import asyncio
from contextlib import suppress
import hashlib
import json
import logging

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramNetworkError, TelegramServerError
from aiogram.types import BufferedInputFile
from fastapi import HTTPException
from psycopg.errors import UndefinedColumn, UndefinedTable

from app.broadcast_media_storage import BroadcastMediaStorage

logger = logging.getLogger(__name__)


def confirmation_hash(token):
    return hashlib.sha256(token.encode()).hexdigest()


def existing_confirmation(connection, token, user_id, shop_id=None, network_id=None):
    condition = "b.network_id=%s AND j.scope='NETWORK'" if network_id is not None else "b.shop_id=%s AND j.scope='SHOP'"
    row = connection.execute(f'''SELECT j.broadcast_id FROM broadcast_delivery_jobs j
        JOIN broadcasts b ON b.id=j.broadcast_id WHERE j.confirmation_hash=%s
        AND j.requested_by_user_id=%s AND {condition}''',
        (confirmation_hash(token), user_id, network_id if network_id is not None else shop_id)).fetchone()
    return row['broadcast_id'] if row else None


def enqueue_broadcast(connection, broadcast, token, actor_user_id):
    """Same transaction as publication/one-use preview: zero external calls."""
    saved = connection.execute('SELECT media_key FROM broadcasts WHERE id=%s', (broadcast['broadcast_id'],)).fetchone()
    fallback = broadcast.get('media') if not saved['media_key'] else None
    connection.execute('''INSERT INTO broadcast_delivery_jobs
        (broadcast_id,confirmation_hash,requested_by_user_id,scope,display_name,target_users,
         fallback_media_kind,fallback_media_filename,fallback_media_bytes)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
        (broadcast['broadcast_id'], confirmation_hash(token), actor_user_id,
         broadcast.get('scope','SHOP'), broadcast['shop_name'],len({row['user_id'] for row in broadcast['recipients']}),
         fallback['kind'] if fallback else None, fallback['filename'] if fallback else None,
         fallback['bytes'] if fallback else None))
    for row in {row['user_id']: row for row in broadcast['recipients']}.values():
        connection.execute('''INSERT INTO broadcast_recipient_deliveries(broadcast_id,user_id,telegram_user_id)
            VALUES(%s,%s,%s) ON CONFLICT DO NOTHING''',
            (broadcast['broadcast_id'], row['user_id'], row.get('telegram_user_id')))


def delivery_status(connection, broadcast_id):
    row = connection.execute('''SELECT b.*,j.status AS job_status,j.display_name,j.started_at,j.completed_at,j.target_users
        FROM broadcasts b LEFT JOIN broadcast_delivery_jobs j ON j.broadcast_id=b.id
        WHERE b.id=%s''', (broadcast_id,)).fetchone()
    if row is None:
        raise HTTPException(404, detail={'code':'BROADCAST_NOT_FOUND'})
    scope = row.get('scope') or 'SHOP'
    name = row.get('display_name')
    if name is None:
        name = connection.execute('SELECT name FROM coffee_shops WHERE id=%s', (row['shop_id'],)).fetchone()['name']
    targets = row['target_users'] if row['job_status'] else connection.execute(
        'SELECT COUNT(*) AS n FROM broadcast_recipients WHERE broadcast_id=%s', (broadcast_id,)).fetchone()['n']
    result = {'ok':True,'broadcast_id':broadcast_id,'scope':scope,'shop':{'id':row['shop_id'],'name':name},
              'target_users':targets,'app_notifications_created':targets,'created_at':row['created_at'],
              'completed_at':row.get('completed_at'),'text_preview':row['text'][:160],
              'status':row['job_status'] or 'legacy'}
    if scope == 'NETWORK':
        result['network'] = {'id':row['network_id'],'name':name}
    if not row['job_status']:
        result.update({key:None for key in ('apns_attempted','apns_success','apns_failed','telegram_attempted',
                                           'telegram_success','telegram_failed','app_unknown','telegram_unknown','failed')})
        result['sent'] = row['recipients_count']
        return result
    counts = connection.execute('''SELECT COALESCE(SUM(apns_attempted),0) AS apns_attempted,
        COALESCE(SUM(apns_success),0) AS apns_success,COALESCE(SUM(apns_failed),0) AS apns_failed,
        COUNT(*) FILTER(WHERE app_status='unknown') AS app_unknown,
        COUNT(*) FILTER(WHERE telegram_status='unknown') AS telegram_unknown,
        COUNT(*) FILTER(WHERE telegram_attempted) AS telegram_attempted,
        COUNT(*) FILTER(WHERE telegram_status='sent') AS telegram_success,
        COUNT(*) FILTER(WHERE telegram_status IN ('failed','unknown')) AS telegram_failed,
        COUNT(*) FILTER(WHERE app_status='sent' OR telegram_status='sent') AS sent,
        COUNT(*) FILTER(WHERE app_status IN ('failed','unknown') OR telegram_status IN ('failed','unknown')) AS failed
        FROM broadcast_recipient_deliveries WHERE broadcast_id=%s''', (broadcast_id,)).fetchone()
    result.update({key:int(value) for key,value in counts.items()})
    if result['app_unknown']:
        # A crash may lose the APNs acknowledgement. Never invent exact totals.
        for key in ('apns_attempted','apns_success','apns_failed'):
            result[key] = None
    return result


def owner_history(connection, *, shop_id=None, network_id=None, limit=20, offset=0):
    where = "b.network_id=%s AND b.scope='NETWORK'" if network_id is not None else "b.shop_id=%s AND COALESCE(to_jsonb(b)->>'scope','SHOP')='SHOP'"
    args = (network_id if network_id is not None else shop_id,)
    where += " AND COALESCE(to_jsonb(b)->>'origin','manual')='manual'"
    total = connection.execute('SELECT COUNT(*) AS n FROM broadcasts b WHERE '+where,args).fetchone()['n']
    rows = connection.execute('SELECT b.id FROM broadcasts b WHERE '+where+' ORDER BY b.created_at DESC,b.id DESC LIMIT %s OFFSET %s',
                              (*args,limit,offset)).fetchall()
    return {'ok':True,'items':[delivery_status(connection,item['id']) for item in rows],
            'total':total,'limit':limit,'offset':offset}


def telegram_plan(name, text, media_kind):
    value = f'☕️ {name}\n\n{text}' if text else f'☕️ {name}'
    plan = []
    def take(limit):
        nonlocal value
        part = value.encode('utf-16-le')[:limit*2].decode('utf-16-le',errors='ignore')
        value = value[len(part):]
        return part
    if media_kind:
        plan.append({'kind':media_kind,'text':take(1024)})
    while value:
        plan.append({'kind':'text','text':take(4096)})
    return plan


def _telegram_error(error):
    return 'failed' if isinstance(error,TelegramAPIError) and not isinstance(error,(TelegramNetworkError,TelegramServerError)) else 'unknown'


def _audience_active(connection, broadcast):
    if broadcast.get('scope') == 'NETWORK':
        return connection.execute('SELECT 1 FROM coffee_shops WHERE network_id=%s AND is_active IS TRUE LIMIT 1',
                                  (broadcast['network_id'],)).fetchone() is not None
    return connection.execute('SELECT 1 FROM coffee_shops WHERE id=%s AND is_active IS TRUE',
                              (broadcast['shop_id'],)).fetchone() is not None


def _default_bot():
    from app.config import BOT_TOKEN
    return Bot(BOT_TOKEN)


async def process_job(connection, broadcast_id, *, push_sender=None, bot_factory=None, media_storage=None):
    """A job lock is held on this connection; every attempt claim is committed."""
    from app.db import log_broadcast_touches
    if push_sender is None:
        from app.broadcast_delivery import send_broadcast_app_push
        push_sender = send_broadcast_app_push
    with connection.transaction():
        job = connection.execute('''SELECT b.*,j.scope AS delivery_scope,j.display_name,j.fallback_media_kind,j.fallback_media_filename,
            j.fallback_media_bytes,j.telegram_file_id FROM broadcasts b JOIN broadcast_delivery_jobs j ON j.broadcast_id=b.id
            WHERE b.id=%s FOR UPDATE OF j''',(broadcast_id,)).fetchone()
        if not job:
            return
        broadcast = dict(job,broadcast_id=broadcast_id,shop_name=job['display_name'],scope=job['delivery_scope'])
        connection.execute("UPDATE broadcast_delivery_jobs SET status='sending',started_at=COALESCE(started_at,NOW()) WHERE broadcast_id=%s",(broadcast_id,))
        connection.execute("UPDATE broadcast_recipient_deliveries SET app_status='unknown',telegram_status='skipped',error_class='InterruptedAppAttempt' WHERE broadcast_id=%s AND app_status='sending'",(broadcast_id,))
        connection.execute("UPDATE broadcast_recipient_deliveries SET telegram_status='unknown',error_class='InterruptedTelegramAttempt' WHERE broadcast_id=%s AND telegram_status='sending'",(broadcast_id,))
        ids = connection.execute('SELECT user_id FROM broadcast_recipient_deliveries WHERE broadcast_id=%s ORDER BY user_id',(broadcast_id,)).fetchall()
    bot = None
    media = None
    telegram_file_id = job['telegram_file_id']
    try:
        for item in ids:
            user_id = item['user_id']
            with connection.transaction():
                row = connection.execute('SELECT * FROM broadcast_recipient_deliveries WHERE broadcast_id=%s AND user_id=%s FOR UPDATE',
                                         (broadcast_id,user_id)).fetchone()
                if not row:
                    continue
                if not _audience_active(connection,broadcast):
                    connection.execute("UPDATE broadcast_recipient_deliveries SET app_status=CASE WHEN app_status='pending' THEN 'failed' ELSE app_status END,telegram_status=CASE WHEN telegram_status='pending' THEN 'skipped' ELSE telegram_status END,error_class='ShopClosed' WHERE broadcast_id=%s AND user_id=%s",(broadcast_id,user_id))
                    continue
                app_pending = row['app_status'] == 'pending'
                if app_pending:
                    connection.execute("UPDATE broadcast_recipient_deliveries SET app_status='sending' WHERE broadcast_id=%s AND user_id=%s",(broadcast_id,user_id))
            if app_pending:
                audit = {}
                try:
                    outcome = await push_sender(broadcast,dict(row),audit=audit)
                except Exception as error:
                    outcome = False
                    audit['error_class'] = type(error).__name__
                with connection.transaction():
                    connection.execute('''UPDATE broadcast_recipient_deliveries SET app_status=%s,
                        apns_attempted=%s,apns_success=%s,apns_failed=%s,
                        telegram_status=%s,error_class=%s WHERE broadcast_id=%s AND user_id=%s''',
                        ('sent' if outcome is True else 'none' if outcome is None else 'unknown' if audit.get('error_class') else 'failed',
                         audit.get('attempted',0),audit.get('sent',0),audit.get('failed',0),
                         'pending' if outcome is None else 'skipped',audit.get('error_class'),broadcast_id,user_id))
            with connection.transaction():
                row = connection.execute('SELECT * FROM broadcast_recipient_deliveries WHERE broadcast_id=%s AND user_id=%s FOR UPDATE',
                                         (broadcast_id,user_id)).fetchone()
                if not row:
                    continue
                if row['telegram_status'] == 'pending':
                    target = connection.execute('SELECT telegram_user_id FROM users WHERE id=%s',(user_id,)).fetchone()
                    if not target or not row['telegram_user_id'] or target['telegram_user_id'] != row['telegram_user_id']:
                        connection.execute("UPDATE broadcast_recipient_deliveries SET telegram_status='skipped' WHERE broadcast_id=%s AND user_id=%s",(broadcast_id,user_id))
                        continue
                    connection.execute("UPDATE broadcast_recipient_deliveries SET telegram_status='sending' WHERE broadcast_id=%s AND user_id=%s",(broadcast_id,user_id))
                    telegram = True
                else:
                    telegram = False
            if telegram:
                request_started = False
                try:
                    if bot is None:
                        bot = (bot_factory or _default_bot)()
                    kind = broadcast.get('media_type') or broadcast.get('fallback_media_kind')
                    if kind and media is None and telegram_file_id is None:
                        if broadcast.get('media_key'):
                            media = (media_storage or BroadcastMediaStorage()).get(broadcast['media_key'])
                        elif broadcast.get('fallback_media_bytes') is not None:
                            media = {'kind':kind,'bytes':bytes(broadcast['fallback_media_bytes']),
                                     'filename':broadcast['fallback_media_filename']}
                        if media is None:
                            raise FileNotFoundError('Broadcast media unavailable')
                    for stage in telegram_plan(broadcast['shop_name'],broadcast['text'],kind):
                        await asyncio.sleep(0.05)
                        with connection.transaction():
                            connection.execute('UPDATE broadcast_recipient_deliveries SET telegram_attempted=TRUE WHERE broadcast_id=%s AND user_id=%s',
                                               (broadcast_id,user_id))
                        request_started = True
                        if stage['kind'] == 'text':
                            message = await bot.send_message(chat_id=row['telegram_user_id'],text=stage['text'],parse_mode=None,request_timeout=15)
                        else:
                            sender = bot.send_photo if stage['kind']=='photo' else bot.send_video
                            message = await sender(chat_id=row['telegram_user_id'],
                                **{stage['kind']:telegram_file_id or BufferedInputFile(media['bytes'],filename=media['filename'])},
                                caption=stage['text'],parse_mode=None,request_timeout=15)
                        message_id = getattr(message,'message_id',None)
                        if type(message_id) is not int or message_id <= 0:
                            raise ValueError('Missing Telegram acknowledgement')
                        with connection.transaction():
                            connection.execute('''UPDATE broadcast_recipient_deliveries SET
                                telegram_message_ids=telegram_message_ids||%s::jsonb WHERE broadcast_id=%s AND user_id=%s''',
                                (json.dumps([message_id]),broadcast_id,user_id))
                            if stage['kind'] in ('photo','video') and telegram_file_id is None:
                                sizes = getattr(message,'photo',None)
                                candidate = (getattr(sizes[-1],'file_id',None) if stage['kind']=='photo' and isinstance(sizes,list) and sizes
                                             else getattr(getattr(message,'video',None),'file_id',None))
                                if isinstance(candidate,str) and candidate:
                                    telegram_file_id = candidate
                                    connection.execute('UPDATE broadcast_delivery_jobs SET telegram_file_id=%s WHERE broadcast_id=%s',
                                                       (telegram_file_id,broadcast_id))
                    state,error_class = 'sent',None
                except Exception as error:
                    state = _telegram_error(error) if request_started else 'failed'
                    error_class = type(error).__name__
                    logger.warning('Queued broadcast Telegram outcome=%s error=%s',state,error_class)
                with connection.transaction():
                    connection.execute('UPDATE broadcast_recipient_deliveries SET telegram_status=%s,error_class=%s WHERE broadcast_id=%s AND user_id=%s',
                                       (state,error_class,broadcast_id,user_id))
            with connection.transaction():
                delivered = connection.execute("SELECT 1 FROM broadcast_recipient_deliveries WHERE broadcast_id=%s AND user_id=%s AND touched_at IS NULL AND (app_status='sent' OR telegram_status='sent') FOR UPDATE",(broadcast_id,user_id)).fetchone()
                if delivered:
                    log_broadcast_touches(broadcast['shop_id'],[user_id],connection=connection)
                    connection.execute('UPDATE broadcast_recipient_deliveries SET touched_at=NOW() WHERE broadcast_id=%s AND user_id=%s',(broadcast_id,user_id))
        with connection.transaction():
            status = delivery_status(connection,broadcast_id)
            pending = connection.execute("SELECT COUNT(*) AS n FROM broadcast_recipient_deliveries WHERE broadcast_id=%s AND (app_status='pending' OR telegram_status='pending')",(broadcast_id,)).fetchone()['n']
            partial = bool(status['failed'] or status['apns_failed'])
            connection.execute('''UPDATE broadcast_delivery_jobs SET status=%s,completed_at=NOW(),fallback_media_bytes=NULL
                WHERE broadcast_id=%s''',('queued' if pending else 'partial' if partial else 'completed',broadcast_id))
            if pending:
                connection.execute('UPDATE broadcast_delivery_jobs SET completed_at=NULL,fallback_media_bytes=%s WHERE broadcast_id=%s',
                                   (broadcast.get('fallback_media_bytes'),broadcast_id))
            connection.execute('UPDATE broadcasts SET recipients_count=%s WHERE id=%s',(status['sent'],broadcast_id))
    finally:
        if bot is not None:
            with suppress(Exception):
                await bot.session.close()


async def process_broadcast_jobs_once(*, connection_factory=None, push_sender=None, bot_factory=None, media_storage=None):
    if connection_factory is None:
        from app.db import get_connection
        connection_factory = get_connection
    with connection_factory() as scan:
        ids = scan.execute("SELECT broadcast_id FROM broadcast_delivery_jobs WHERE status IN ('queued','sending') ORDER BY created_at,broadcast_id LIMIT 10").fetchall()
    for item in ids:
        with connection_factory() as connection:
            key = f'nashi-native-broadcast:{item["broadcast_id"]}'
            locked = connection.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS locked',(key,)).fetchone()['locked']
            if not locked:
                continue
            try:
                state = connection.execute('SELECT status FROM broadcast_delivery_jobs WHERE broadcast_id=%s',(item['broadcast_id'],)).fetchone()
                if state and state['status'] in ('queued','sending'):
                    await process_job(connection,item['broadcast_id'],push_sender=push_sender,bot_factory=bot_factory,media_storage=media_storage)
            finally:
                connection.execute('SELECT pg_advisory_unlock(hashtextextended(%s,0))',(key,))


async def broadcast_job_loop():
    while True:
        try:
            await process_broadcast_jobs_once()
        except (UndefinedTable,UndefinedColumn):
            pass  # No schema and, crucially, no legacy work is inferred.
        except Exception as error:
            logger.warning('Native broadcast worker interrupted (%s)',type(error).__name__)
        await asyncio.sleep(1)


def merge_job_recipient_receipts(cursor, source_user_id, target_user_id):
    """Call before merging broadcast_recipients; retain strongest receipts."""
    cursor.execute('SELECT * FROM broadcast_recipient_deliveries WHERE user_id=%s ORDER BY broadcast_id FOR UPDATE',
                   (source_user_id,))
    for source in cursor.fetchall():
        if source['app_status'] == 'sending' or source['telegram_status'] == 'sending':
            # An already running sender still holds the old user_id. Its
            # acknowledgement cannot safely update a receipt after rekeying.
            source = dict(source)
            if source['app_status'] == 'sending':
                source['app_status'] = 'unknown'
                if source['telegram_status'] == 'pending':
                    source['telegram_status'] = 'skipped'
            if source['telegram_status'] == 'sending':
                source['telegram_status'] = 'unknown'
            cursor.execute('''UPDATE broadcast_recipient_deliveries SET app_status=%s,
                telegram_status=%s,error_class='AccountMergedDuringAttempt'
                WHERE broadcast_id=%s AND user_id=%s''',
                (source['app_status'],source['telegram_status'],source['broadcast_id'],source_user_id))
        cursor.execute('SELECT 1 FROM broadcast_recipients WHERE broadcast_id=%s AND user_id=%s',
                       (source['broadcast_id'],target_user_id))
        if cursor.fetchone() is None:
            continue  # Existing recipient UPDATE cascades the source-only row.
        cursor.execute('SELECT * FROM broadcast_recipient_deliveries WHERE broadcast_id=%s AND user_id=%s FOR UPDATE',
                       (source['broadcast_id'],target_user_id))
        target = cursor.fetchone()
        if target is None:
            cursor.execute('''INSERT INTO broadcast_recipient_deliveries
                (broadcast_id,user_id,telegram_user_id,app_status,apns_attempted,apns_success,apns_failed,
                 telegram_status,telegram_message_ids,telegram_attempted,error_class,touched_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s)''',
                (source['broadcast_id'],target_user_id,source['telegram_user_id'],source['app_status'],
                 source['apns_attempted'],source['apns_success'],source['apns_failed'],source['telegram_status'],
                 json.dumps(source['telegram_message_ids']),source['telegram_attempted'],source['error_class'],source['touched_at']))
            continue
        priority = {'pending':0,'none':1,'skipped':1,'failed':2,'sending':3,'unknown':3,'sent':4}
        app = source if priority[source['app_status']] > priority[target['app_status']] else target
        telegram = source if priority[source['telegram_status']] > priority[target['telegram_status']] else target
        # If either account already had an app success, do not allow a pending
        # Telegram path on the merged target to duplicate that notification.
        tg_status = 'skipped' if app['app_status'] in ('sent','sending','unknown') and telegram['telegram_status']=='pending' else telegram['telegram_status']
        cursor.execute('''UPDATE broadcast_recipient_deliveries SET app_status=%s,apns_attempted=%s,
            apns_success=%s,apns_failed=%s,telegram_user_id=%s,telegram_status=%s,telegram_message_ids=%s::jsonb,
            telegram_attempted=%s,touched_at=COALESCE(touched_at,%s) WHERE broadcast_id=%s AND user_id=%s''',
            (app['app_status'],app['apns_attempted'],app['apns_success'],app['apns_failed'],telegram['telegram_user_id'],
             tg_status,json.dumps(telegram['telegram_message_ids']),telegram['telegram_attempted'],source['touched_at'],
             source['broadcast_id'],target_user_id))
