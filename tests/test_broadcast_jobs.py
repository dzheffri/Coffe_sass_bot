"""Native confirmation receipts, recovery and transport routing on disposable DB."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.broadcast_jobs import (delivery_status, enqueue_broadcast, merge_job_recipient_receipts,
                                owner_history, process_broadcast_jobs_once, telegram_plan)
from app.broadcast_publication import BroadcastPublication
from conftest import bearer
from test_barista_operations import operations_database
from test_barista_owner_tools import owner_database, owner_api, preview, send
from test_owner_broadcast_media import photo_data, video_data


@pytest.fixture
def jobs_database(owner_database, monkeypatch):
    db = owner_database
    db.query((Path(__file__).parents[1] / 'migrations/20261026_broadcast_delivery_jobs.sql').read_text())
    import app.api.barista_owner as owner_module
    monkeypatch.setattr(owner_module, 'broadcast_job_loop', AsyncMock())
    return db


@pytest.fixture
def jobs_api(jobs_database, owner_api):
    return owner_api[0]


def queue(db, *, users=(2, 4, 6), text='Власний текст ☕️', media=None, token='test-action-key'):
    rows = db.query('SELECT id AS user_id,telegram_user_id FROM users WHERE id=ANY(%s) ORDER BY id', (list(users),))
    with BroadcastPublication() as publication:
        with db.db.get_connection() as connection:
            with connection.transaction():
                saved = publication.publish(connection, 1, 1, text, rows, media)
                payload = {'broadcast_id':saved['id'],'shop_id':1,'shop_name':'Наші',
                           'text':text,'recipients':rows,'media':media}
                enqueue_broadcast(connection, payload, token, 1)
                return saved['id']


def fake_bot():
    counter = iter(range(101, 200))
    async def sent(**kwargs):
        return SimpleNamespace(message_id=next(counter))
    return SimpleNamespace(send_message=AsyncMock(side_effect=sent), send_photo=AsyncMock(side_effect=sent),
                           send_video=AsyncMock(side_effect=sent), session=SimpleNamespace(close=AsyncMock()))


def drain(db, push, bot):
    return asyncio.run(process_broadcast_jobs_once(connection_factory=db.db.get_connection,
        push_sender=push, bot_factory=lambda:bot, media_storage=db.broadcast_storage))


def status(db, broadcast_id):
    with db.db.get_connection() as connection:
        return delivery_status(connection, broadcast_id)


def test_confirmation_queues_immediately_and_reconciles_same_action(jobs_database, jobs_api):
    db, client = jobs_database, jobs_api
    token = db.staff_token()
    body = preview(client, token, 'Не змінювати текст').json()
    response = send(client, token, body['confirmation_token'])
    assert response.status_code == 202, response.text
    result = response.json()
    assert result['status'] == 'queued' and result['broadcast_id'] > 0
    assert result['target_users'] == result['app_notifications_created'] == 2
    assert result['apns_attempted'] == result['telegram_attempted'] == 0
    assert db.query('SELECT * FROM touch_logs') == []
    # Old preview data can expire/be cleaned and the actor can refresh session.
    db.query('DELETE FROM admin_broadcast_previews')
    refreshed = db.staff_token()
    repeated = send(client, refreshed, body['confirmation_token'])
    assert repeated.status_code == 202 and repeated.json() == result
    reconciled = client.post('/barista/owner/broadcast/status', headers=bearer(refreshed),
                             json={'confirmation_token':body['confirmation_token']})
    assert reconciled.status_code == 200 and reconciled.json() == result
    assert len(db.query('SELECT id FROM broadcasts')) == 1
    assert len(db.query('SELECT * FROM broadcast_delivery_jobs')) == 1
    assert client.get('/barista/owner/broadcasts',headers=bearer(refreshed)).json()['items'] == [result]


def test_concurrent_confirmation_returns_one_same_job(jobs_database, jobs_api):
    db, client = jobs_database, jobs_api
    token = db.staff_token()
    action = preview(client, token).json()['confirmation_token']
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda _:send(client, token, action), range(2)))
    assert [response.status_code for response in responses] == [202, 202]
    assert responses[0].json()['broadcast_id'] == responses[1].json()['broadcast_id']
    assert len(db.query('SELECT id FROM broadcasts')) == 1


def test_status_history_and_reconcile_require_current_owner_and_tenant(jobs_database, jobs_api):
    db, client = jobs_database, jobs_api
    token = db.staff_token()
    action = preview(client,token).json()['confirmation_token']
    bid = send(client,token,action).json()['broadcast_id']
    foreign = db.staff_token(user_id=3,membership_id=2)
    admin = db.staff_token(user_id=2,membership_id=3)
    assert client.get(f'/barista/owner/broadcasts/{bid}',headers=bearer(foreign)).status_code == 404
    assert client.get('/barista/owner/broadcasts',headers=bearer(foreign)).json()['items'] == []
    assert client.post('/barista/owner/broadcast/status',headers=bearer(foreign),
                       json={'confirmation_token':action}).status_code == 404
    for path in ('/barista/owner/broadcasts',f'/barista/owner/broadcasts/{bid}'):
        assert client.get(path,headers=bearer(admin)).status_code == 403
        assert client.get(path).status_code == 401


def test_app_priority_full_snapshot_counts_and_no_repeat(jobs_database):
    db = jobs_database
    bid = queue(db)
    bot = fake_bot()
    async def push(broadcast, recipient, *, audit):
        assert broadcast['broadcast_id'] == bid
        if recipient['user_id'] in (2,4):
            audit.update(attempted=1,sent=1,failed=0)
            return True
        audit.update(attempted=0,sent=0,failed=0)
        return None
    sender = AsyncMock(side_effect=push)
    drain(db,sender,bot)
    result = status(db,bid)
    assert result['status'] == 'completed' and result['target_users'] == result['sent'] == 3
    assert (result['apns_attempted'],result['apns_success'],result['apns_failed']) == (2,2,0)
    assert (result['telegram_attempted'],result['telegram_success'],result['telegram_failed']) == (1,1,0)
    assert bot.send_message.await_args.kwargs['chat_id'] == 1006
    assert bot.send_message.await_args.kwargs['text'] == '☕️ Наші\n\nВласний текст ☕️'
    assert bot.send_message.await_args.kwargs['parse_mode'] is None
    assert db.query('SELECT text FROM broadcasts')[0]['text'] == 'Власний текст ☕️'
    assert {r['user_id'] for r in db.query('SELECT user_id FROM touch_logs')} == {2,4,6}
    drain(db,sender,bot)
    assert sender.await_count == 3 and bot.send_message.await_count == 1
    assert len(db.query('SELECT * FROM touch_logs')) == 3


def test_temporary_app_error_never_falls_back_to_telegram(jobs_database):
    db = jobs_database
    bid = queue(db,users=(4,))
    async def push(*args, audit):
        audit.update(attempted=1,sent=0,failed=1)
        return False
    bot = fake_bot()
    drain(db,push,bot)
    result = status(db,bid)
    assert result['status'] == 'partial' and result['apns_failed'] == 1
    assert result['telegram_attempted'] == 0 and result['failed'] == 1
    bot.send_message.assert_not_awaited()


@pytest.mark.parametrize('interrupted', ['app','telegram'])
def test_interrupted_external_attempt_is_unknown_and_never_replayed(jobs_database,interrupted):
    db = jobs_database
    bid = queue(db,users=(4,6))
    if interrupted == 'app':
        db.query("UPDATE broadcast_recipient_deliveries SET app_status='sending' WHERE user_id=4")
    else:
        db.query("UPDATE broadcast_recipient_deliveries SET app_status='none',telegram_status='sending' WHERE user_id=4")
    db.query("UPDATE broadcast_delivery_jobs SET status='sending'")
    push = AsyncMock(return_value=None)
    bot = fake_bot()
    drain(db,push,bot)
    assert push.await_count == 1 and push.await_args.args[1]['user_id'] == 6
    assert bot.send_message.await_count == 1 and bot.send_message.await_args.kwargs['chat_id'] == 1006
    result = status(db,bid)
    assert result['status'] == 'partial'
    assert result['app_unknown' if interrupted=='app' else 'telegram_unknown'] == 1
    if interrupted == 'app':
        assert result['apns_attempted'] is None
    drain(db,push,bot)
    assert push.await_count == 1 and bot.send_message.await_count == 1


def test_concurrent_workers_claim_one_job_and_send_once(jobs_database):
    db = jobs_database
    bid = queue(db,users=(4,))
    bot = fake_bot()
    async def scenario():
        entered,release = asyncio.Event(),asyncio.Event()
        async def push(*args, audit):
            entered.set()
            await release.wait()
            return None
        sender = AsyncMock(side_effect=push)
        first = asyncio.create_task(process_broadcast_jobs_once(connection_factory=db.db.get_connection,
            push_sender=sender,bot_factory=lambda:bot))
        await entered.wait()
        await process_broadcast_jobs_once(connection_factory=db.db.get_connection,
            push_sender=sender,bot_factory=lambda:bot)
        assert sender.await_count == 1
        release.set()
        await first
    asyncio.run(scenario())
    assert status(db,bid)['status'] == 'completed'
    bot.send_message.assert_awaited_once()


def test_photo_fallback_survives_storage_error_and_is_cleared_after_delivery(jobs_database,monkeypatch):
    db = jobs_database
    data = photo_data()
    monkeypatch.setattr(db.broadcast_storage,'save',lambda *_:(_ for _ in ()).throw(OSError('disk unavailable')))
    bid = queue(db,users=(4,),media={'kind':'photo','filename':'coffee.jpg','mime_type':'image/jpeg','bytes':data})
    assert bytes(db.query('SELECT fallback_media_bytes FROM broadcast_delivery_jobs')[0]['fallback_media_bytes']) == data
    bot = fake_bot()
    drain(db,AsyncMock(return_value=None),bot)
    assert bot.send_photo.await_args.kwargs['photo'].data == data
    assert bot.send_photo.await_args.kwargs['caption'] == '☕️ Наші\n\nВласний текст ☕️'
    assert db.query('SELECT fallback_media_bytes FROM broadcast_delivery_jobs')[0]['fallback_media_bytes'] is None
    assert status(db,bid)['telegram_success'] == 1


def test_caption_split_preserves_name_text_and_ambiguous_partial_send_not_replayed(jobs_database):
    db = jobs_database
    text = '🥐' * 900
    plan = telegram_plan('Наші',text,'photo')
    assert ''.join(stage['text'] for stage in plan) == '☕️ Наші\n\n' + text
    assert len(plan[0]['text'].encode('utf-16-le')) // 2 <= 1024
    assert all(len(stage['text'].encode('utf-16-le')) // 2 <= 4096 for stage in plan[1:])
    bid = queue(db,users=(4,),text=text,media={'kind':'photo','filename':'coffee.jpg','mime_type':'image/jpeg','bytes':photo_data()})
    bot = fake_bot()
    bot.send_message.side_effect = TimeoutError('do not expose transport detail')
    push = AsyncMock(return_value=None)
    drain(db,push,bot)
    assert status(db,bid)['telegram_unknown'] == 1
    assert db.query('SELECT telegram_message_ids FROM broadcast_recipient_deliveries')[0]['telegram_message_ids'] == [101]
    drain(db,push,bot)
    assert bot.send_photo.await_count == bot.send_message.await_count == 1


def test_legacy_history_unknown_channel_counts_and_never_replayed(jobs_database):
    db = jobs_database
    legacy = db.query("INSERT INTO broadcasts(shop_id,sender_user_id,text,recipients_count) VALUES(1,1,'Стара',144) RETURNING id")[0]['id']
    db.query('INSERT INTO broadcast_recipients(broadcast_id,user_id) VALUES(%s,4),(%s,6)',(legacy,legacy))
    with db.db.get_connection() as connection:
        result = owner_history(connection,shop_id=1)['items'][0]
    assert result['status'] == 'legacy' and result['target_users'] == 2 and result['sent'] == 144
    for key in ('apns_attempted','apns_success','apns_failed','telegram_attempted','telegram_success','telegram_failed'):
        assert result[key] is None
    bot,push = fake_bot(),AsyncMock(return_value=None)
    drain(db,push,bot)
    push.assert_not_awaited()
    bot.send_message.assert_not_awaited()
    db.query((Path(__file__).parents[1] / 'migrations/20261026_broadcast_delivery_jobs.sql').read_text())
    assert db.query('SELECT recipients_count FROM broadcasts')[0]['recipients_count'] == 144
    assert db.query('SELECT * FROM broadcast_delivery_jobs') == []


def test_merge_receipt_preserves_delivery_and_blocks_duplicate_pending_fallback(jobs_database):
    db = jobs_database
    bid = queue(db,users=(2,4))
    db.query("UPDATE broadcast_recipient_deliveries SET app_status='sent',apns_attempted=1,apns_success=1,telegram_status='skipped' WHERE user_id=2")
    with db.db.get_connection() as connection:
        with connection.transaction():
            merge_job_recipient_receipts(connection.cursor(),2,4)
    row = db.query('SELECT * FROM broadcast_recipient_deliveries WHERE broadcast_id=%s AND user_id=4',(bid,))[0]
    assert row['app_status'] == 'sent' and row['telegram_status'] == 'skipped'
    assert row['apns_success'] == 1


def test_initial_target_counts_survive_recipient_account_cleanup(jobs_database):
    db = jobs_database
    bid = queue(db,users=(2,4))
    db.query('DELETE FROM broadcast_recipients WHERE broadcast_id=%s AND user_id=4',(bid,))
    result = status(db,bid)
    assert result['target_users'] == result['app_notifications_created'] == 2


def test_merge_during_inflight_attempt_records_unknown_without_new_send(jobs_database):
    db = jobs_database
    bid = queue(db,users=(2,))
    db.query("UPDATE broadcast_recipient_deliveries SET app_status='sending'")
    with db.db.get_connection() as connection:
        with connection.transaction():
            merge_job_recipient_receipts(connection.cursor(),2,4)
            connection.execute('UPDATE broadcast_recipients SET user_id=4 WHERE broadcast_id=%s AND user_id=2',(bid,))
    bot,push = fake_bot(),AsyncMock(return_value=None)
    drain(db,push,bot)
    result = status(db,bid)
    assert result['status'] == 'partial' and result['app_unknown'] == 1
    push.assert_not_awaited()
    bot.send_message.assert_not_awaited()


def test_successful_app_device_plus_failed_device_keeps_priority_and_partial_status(jobs_database):
    db = jobs_database
    bid = queue(db,users=(4,))
    bot = fake_bot()
    async def push(*args,audit):
        audit.update(attempted=2,sent=1,failed=1)
        return True
    drain(db,push,bot)
    result = status(db,bid)
    assert result['status'] == 'partial' and result['apns_success'] == result['apns_failed'] == 1
    assert result['sent'] == 1 and result['telegram_attempted'] == 0
    bot.send_message.assert_not_awaited()


@pytest.mark.parametrize('kind,data,filename', [('photo',photo_data(),'coffee.jpg'),('video',video_data(),'coffee.mp4')])
def test_first_upload_telegram_file_id_is_persisted_and_reused(jobs_database,kind,data,filename):
    db = jobs_database
    bid = queue(db,users=(4,6),media={'kind':kind,'filename':filename,'mime_type':'image/jpeg' if kind=='photo' else 'video/mp4','bytes':data})
    bot = fake_bot()
    sender = bot.send_photo if kind=='photo' else bot.send_video
    attachment = SimpleNamespace(file_id='trusted-returned-by-our-bot')
    sender.side_effect = [SimpleNamespace(message_id=101,**{kind:[attachment] if kind=='photo' else attachment}),
                          SimpleNamespace(message_id=102)]
    drain(db,AsyncMock(return_value=None),bot)
    first,second = [call.kwargs[kind] for call in sender.await_args_list]
    assert first.data == data and second == attachment.file_id
    assert db.query('SELECT telegram_file_id FROM broadcast_delivery_jobs')[0]['telegram_file_id'] == attachment.file_id
    assert status(db,bid)['telegram_success'] == 2
