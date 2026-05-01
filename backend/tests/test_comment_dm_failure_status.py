import asyncio
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://example.com')
os.environ.setdefault('FRONTEND_URL', 'https://example.com')
os.environ.setdefault('IG_APP_ID', '123')
os.environ.setdefault('IG_APP_SECRET', 'secret')
os.environ.setdefault('CRON_SECRET', 'cron-secret')

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server  # noqa: E402

from test_instagram_token_refresh import FakeDB, _account, _run, _user  # noqa: E402


def _automation(nodes, edges, **overrides):
    now = datetime.utcnow()
    doc = {
        'id': 'auto1',
        'user_id': 'u1',
        'status': 'active',
        'trigger': 'comment:any',
        'match': 'any',
        'instagramAccountDbId': 'accA',
        'instagramAccountId': 'igA',
        'activationStartedAt': now - timedelta(minutes=5),
        'nodes': nodes,
        'edges': edges,
    }
    doc.update(overrides)
    return doc


def _comment_payload(comment_id='comment1', text='Price'):
    return {
        'ig_comment_id': comment_id,
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'commenter_username': 'contact',
        'text': text,
        'timestamp': datetime.utcnow(),
    }


def _user_doc():
    return {
        'id': 'u1',
        'email': 'u1@example.com',
        'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA',
        'meta_access_token': 'token-a',
        'instagramHandle': 'account_a',
    }


def _install_db(monkeypatch, automations, comments=None):
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        automations=automations,
        comments=comments or [],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(
        server,
        'ws_manager',
        SimpleNamespace(send=lambda *_args, **_kwargs: asyncio.sleep(0)),
    )
    return fake_db


def _reply_and_dm_automation():
    return _automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Thanks'}},
            {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Here is the link'}},
        ],
        [
            {'source': 'n_trigger', 'target': 'n_reply'},
            {'source': 'n_trigger', 'target': 'n_dm'},
        ],
    )


def test_reply_enabled_dm_success_records_success(monkeypatch):
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])
    calls = []

    async def reply_ok(*_args):
        calls.append('reply')
        return {'ok': True}

    async def dm_ok(*_args):
        calls.append('dm')
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert calls == ['reply', 'dm']
    assert saved['reply_status'] == 'success'
    assert saved['dm_status'] == 'success'
    assert saved['action_status'] == 'success'
    assert result['action_status'] == 'success'


def test_classifier_prefers_recipient_unavailable_over_generic_permission():
    result = server.classify_instagram_send_error({
        'error': {
            'message': 'This recipient cannot receive messages from this business.',
            'code': 10,
            'error_subcode': 2534022,
        }
    }, 400)

    assert result['failure_reason'] == 'recipient_unavailable'
    assert result['retryable'] is False
    assert result['provider_code'] == 10


def test_dm_recipient_unavailable_does_not_block_public_reply(monkeypatch):
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])
    calls = []

    async def reply_ok(*_args):
        calls.append('reply')
        return {'ok': True}

    async def dm_unavailable(*_args):
        calls.append('dm')
        return {
            'ok': False,
            'failure_reason': 'recipient_unavailable',
            'retryable': False,
            'provider_code': 10,
            'provider_subcode': 2534022,
        }

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_unavailable)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert calls == ['reply', 'dm']
    assert saved['replied'] is True
    assert saved['reply_status'] == 'success'
    assert saved['dm_status'] == 'failed'
    assert saved['dm_failure_reason'] == 'recipient_unavailable'
    assert saved['action_status'] == 'partial_success'
    assert result['action_status'] == 'partial_success'

    calls.clear()
    duplicate = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='polling'))
    assert duplicate['reason'] == 'comment_already_partial_success'
    assert calls == []


def test_reply_disabled_dm_unavailable_records_failed(monkeypatch):
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Here is the link'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_dm'}],
    )])

    async def dm_unavailable(*_args):
        return {'ok': False, 'failure_reason': 'messaging_not_allowed', 'retryable': False}

    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_unavailable)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert saved['reply_status'] == 'disabled'
    assert saved['dm_status'] == 'failed'
    assert saved['action_status'] == 'failed_permanent'
    assert result['action_status'] == 'failed_permanent'


def test_reply_enabled_dm_disabled_records_success(monkeypatch):
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Thanks'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_reply'}],
    )])

    async def reply_ok(*_args):
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert saved['reply_status'] == 'success'
    assert saved['dm_status'] == 'disabled'
    assert saved['action_status'] == 'success'
    assert result['action_status'] == 'success'


def test_temporary_dm_error_retries_dm_without_duplicate_reply(monkeypatch):
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])
    calls = []

    async def reply_ok(*_args):
        calls.append('reply')
        return {'ok': True}

    dm_results = [
        {'ok': False, 'failure_reason': 'temporary_graph_error', 'retryable': True},
        {'ok': True},
    ]

    async def dm_flaky(*_args):
        calls.append('dm')
        return dm_results.pop(0)

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_flaky)

    first = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    assert first['action_status'] == 'partial_success'
    assert calls == ['reply', 'dm']

    second = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='polling'))
    saved = db.comments.docs[0]

    assert second['reprocessed'] is True
    assert calls == ['reply', 'dm', 'dm']
    assert saved['reply_status'] == 'success'
    assert saved['dm_status'] == 'success'
    assert saved['action_status'] == 'success'


def test_comment_diagnostics_is_scoped_and_redacted(monkeypatch):
    db = _install_db(monkeypatch, [], comments=[{
        'id': 'doc1',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'ig_comment_id': 'commentDiag',
        'media_id': 'media1',
        'text': 'private full comment text',
        'reply_status': 'success',
        'dm_status': 'failed',
        'action_status': 'partial_success',
        'dm_failure_reason': 'recipient_unavailable',
        'rule_id': 'auto1',
        'source': 'webhook',
    }])

    result = _run(server.comment_diagnostics('commentDiag', user_id='u1'))

    assert result['commentId'] == 'commentDiag'
    assert result['text_length'] == len('private full comment text')
    assert 'private full comment text' not in str(result)
    assert 'accessToken' not in str(result)
    assert result['dm_failure_reason'] == 'recipient_unavailable'


def test_pending_comment_is_processed_by_queue_tick(monkeypatch):
    now = datetime.utcnow()
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Thanks'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_reply'}],
    )], comments=[{
        'id': 'queued1',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': 'queuedComment',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'text': '🔥👏',
        'matched': True,
        'rule_id': 'auto1',
        'reply_status': 'pending',
        'dm_status': 'disabled',
        'action_status': 'pending',
        'next_retry_at': now - timedelta(seconds=1),
        'attempts': 0,
    }])

    async def reply_ok(*_args):
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)

    summary = _run(server._automation_queue_tick())
    saved = db.comments.docs[0]

    assert summary['processed'] == 1
    assert saved['reply_status'] == 'success'
    assert saved['action_status'] == 'success'
    assert saved['attempts'] == 1
    assert saved['queue_lock_until'] is None


def test_retryable_queue_failure_is_rescheduled(monkeypatch):
    now = datetime.utcnow()
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Link'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_dm'}],
    )], comments=[{
        'id': 'queued2',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': 'queuedDm',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'text': 'PDF',
        'matched': True,
        'rule_id': 'auto1',
        'reply_status': 'disabled',
        'dm_status': 'pending',
        'action_status': 'pending',
        'next_retry_at': now - timedelta(seconds=1),
        'attempts': 0,
    }])

    async def dm_temp(*_args):
        return {'ok': False, 'failure_reason': 'temporary_graph_error', 'retryable': True}

    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_temp)

    summary = _run(server._automation_queue_tick())
    saved = db.comments.docs[0]

    assert summary['failed_retryable'] == 1
    assert saved['dm_status'] == 'failed'
    assert saved['action_status'] == 'failed_retryable'
    assert saved['next_retry_at'] is not None
    assert saved['attempts'] == 1


def test_queue_does_not_duplicate_successful_public_reply(monkeypatch):
    now = datetime.utcnow()
    db = _install_db(monkeypatch, [_reply_and_dm_automation()], comments=[{
        'id': 'queued3',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': 'partialRetry',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'text': 'PDF',
        'matched': True,
        'rule_id': 'auto1',
        'replied': True,
        'reply_status': 'success',
        'dm_status': 'failed',
        'dm_failure_retryable': True,
        'dm_failure_reason': 'temporary_graph_error',
        'action_status': 'partial_success',
        'next_retry_at': now - timedelta(seconds=1),
        'attempts': 0,
    }])
    calls = []

    async def reply_ok(*_args):
        calls.append('reply')
        return {'ok': True}

    async def dm_ok(*_args):
        calls.append('dm')
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    summary = _run(server._automation_queue_tick())
    saved = db.comments.docs[0]

    assert summary['success'] == 1
    assert calls == ['dm']
    assert saved['reply_status'] == 'success'
    assert saved['dm_status'] == 'success'
    assert saved['action_status'] == 'success'


def test_why_not_replied_explains_queued_comment(monkeypatch):
    now = datetime.utcnow()
    _install_db(monkeypatch, [], comments=[{
        'id': 'queuedWhy',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'ig_comment_id': 'whyComment',
        'media_id': 'media1',
        'text': 'private body',
        'matched': True,
        'rule_id': 'auto1',
        'reply_status': 'pending',
        'dm_status': 'disabled',
        'action_status': 'pending',
        'skip_reason': 'queued_rate_limit',
        'next_retry_at': now,
        'attempts': 2,
        'queued': True,
    }])

    result = _run(server.comment_why_not_replied('whyComment', user_id='u1'))

    assert result['eligible'] is True
    assert result['queued'] is True
    assert result['skip_reason'] == 'queued_rate_limit'
    assert result['attempts'] == 2
    assert 'private body' not in str(result)
