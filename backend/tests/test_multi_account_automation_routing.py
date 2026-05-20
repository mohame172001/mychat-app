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

from test_instagram_token_refresh import FakeDB, FakeResponse, _account, _run, _user  # noqa: E402


def _reply_provider_ok():
    return {
        'ok': True,
        'status_code': 200,
        'body': {'id': 'reply_provider_id'},
        'provider_comment_id': 'reply_provider_id',
        'failure_reason': None,
        'retryable': False,
    }


def _comment_rule(account_db_id, ig_id, rule_id, trigger='comment:any'):
    now = datetime.utcnow() - timedelta(minutes=5)
    return {
        'id': rule_id,
        'user_id': 'u1',
        'status': 'active',
        'trigger': trigger,
        'match': 'any',
        'mode': 'reply_and_dm',
        'post_scope': 'any',
        'instagramAccountDbId': account_db_id,
        'instagram_account_id': account_db_id,
        'instagramAccountId': ig_id,
        'igUserId': ig_id,
        'activationStartedAt': now,
        'reply_under_post': True,
        'comment_reply': f'public reply from {ig_id}',
        'opening_dm_enabled': True,
        'opening_dm_text': f'dm from {ig_id}',
        'dm_text': f'dm from {ig_id}',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {
                'id': 'n_reply',
                'type': 'reply_comment',
                'data': {'text': f'public reply from {ig_id}',
                         'replies': [f'public reply from {ig_id}']},
            },
            {'id': 'n_dm', 'type': 'message', 'data': {'text': f'dm from {ig_id}'}},
        ],
        'edges': [
            {'source': 'n_trigger', 'target': 'n_reply'},
            {'source': 'n_reply', 'target': 'n_dm'},
        ],
    }


def _post_specific_comment_flow_rule(account_db_id, ig_id, rule_id, media_id):
    rule = _comment_rule(account_db_id, ig_id, rule_id, trigger=f'comment:{media_id}')
    rule.update({
        'post_scope': 'specific',
        'media_id': media_id,
        'trigger_media_id': media_id,
        'opening_dm_text': 'أهلاً 👋 شوفت تعليقك على الفيديو، حابب أبعتلك اللينك؟',
        'opening_dm_button_text': 'ابعت',
        'link_dm_text': 'اتفضل اللينك',
        'link_button_text': 'افتح اللينك',
        'link_url': 'https://example.com/live-test',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {
                'id': 'n_reply',
                'type': 'reply_comment',
                'data': {'text': f'public reply from {ig_id}',
                         'replies': [f'public reply from {ig_id}']},
            },
            {
                'id': 'n_opening',
                'type': 'message',
                'data': {'text': rule['opening_dm_text']},
            },
        ],
        'edges': [
            {'source': 'n_trigger', 'target': 'n_reply'},
            {'source': 'n_reply', 'target': 'n_opening'},
        ],
    })
    return rule


def _install_multi_account_db(monkeypatch):
    accounts = [
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _account(id='accB', userId='u1', instagramAccountId='igB',
                 igUserId='igB', username='account_b', accessToken='token-b'),
    ]
    user = _user(id='u1', active_instagram_account_id='accA',
                 ig_user_id='igA', meta_access_token='token-a')
    db = FakeDB(
        accounts,
        user,
        automations=[
            _comment_rule('accA', 'igA', 'ruleA'),
            _comment_rule('accB', 'igB', 'ruleB'),
        ],
    )
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(
        server,
        'ws_manager',
        SimpleNamespace(send=lambda *_args, **_kwargs: asyncio.sleep(0)),
    )
    monkeypatch.setattr(
        server,
        'reserve_usage_limit',
        lambda *a, **kw: asyncio.sleep(
            0,
            result={'allowed': True, 'exceeded': False, 'fail_open': False},
        ),
    )
    monkeypatch.setattr(
        server,
        'confirm_usage_reservation',
        lambda *a, **kw: asyncio.sleep(0, result=True),
    )
    return db


def test_default_dm_send_blocks_workspace_recipient_but_comment_flow_can_allow(monkeypatch):
    db = FakeDB([
        _account(id='accA', userId='u1', instagramAccountId='igA'),
        _account(id='accB', userId='u1', instagramAccountId='igB'),
    ], _user())
    monkeypatch.setattr(server, 'db', db)

    blocked = _run(server.send_ig_message('tok', 'igB', 'igA', {'text': 'hi'}))
    assert blocked['ok'] is False
    assert blocked['failure_reason'] == 'cross_account_recipient_blocked'

    class _Client:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

        async def post(self, *_args, **_kwargs):
            return FakeResponse(200, {'message_id': 'mid-ok'})

    monkeypatch.setattr(server.httpx, 'AsyncClient', _Client)
    allowed = _run(server.send_ig_message(
        'tok',
        'igB',
        'igA',
        {'text': 'hi'},
        allow_workspace_recipient=True,
    ))
    assert allowed['ok'] is True


def test_account_a_comment_on_account_b_routes_only_to_b_rule_and_token(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    reply_calls = []
    dm_calls = []

    async def reply_ok(access_token, comment_id, text):
        reply_calls.append({
            'access_token': access_token,
            'comment_id': comment_id,
            'text': text,
        })
        return _reply_provider_ok()

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        dm_calls.append({
            'access_token': access_token,
            'ig_user_id': ig_user_id,
            'recipient_id': recipient_id,
            'message': message,
            'allow_workspace_recipient': allow_workspace_recipient,
        })
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid'}}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    owner = server._with_instagram_account_context(
        db.users.docs[0],
        db.instagram_accounts.docs[1],
    )
    result = _run(server._handle_new_comment(
        owner,
        {
            'ig_comment_id': 'comment-on-b-from-a',
            'media_id': 'mediaB',
            'commenter_id': 'igA',
            'commenter_username': 'account_a',
            'text': 'test',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))

    assert result['matched'] is True
    assert result['rule_id'] == 'ruleB'
    assert reply_calls == [{
        'access_token': 'token-b',
        'comment_id': 'comment-on-b-from-a',
        'text': 'public reply from igB',
    }]
    assert len(dm_calls) == 1
    assert dm_calls[0]['access_token'] == 'token-b'
    assert dm_calls[0]['ig_user_id'] == 'igB'
    assert dm_calls[0]['recipient_id'] == 'igA'
    assert dm_calls[0]['allow_workspace_recipient'] is True
    assert db.comments.docs[0]['instagramAccountId'] == 'igB'


def test_same_commenter_same_post_rule_does_not_restart_opening_flow(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    reply_calls = []
    dm_calls = []

    async def reply_ok(access_token, comment_id, text):
        reply_calls.append((access_token, comment_id, text))
        return _reply_provider_ok()

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        dm_calls.append((access_token, ig_user_id, recipient_id, message,
                         allow_workspace_recipient))
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid'}}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    owner = server._with_instagram_account_context(
        db.users.docs[0],
        db.instagram_accounts.docs[1],
    )
    first = _run(server._handle_new_comment(
        owner,
        {
            'ig_comment_id': 'comment-b-1',
            'media_id': 'mediaB',
            'commenter_id': 'igA',
            'commenter_username': 'account_a',
            'text': 'test',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))
    second = _run(server._handle_new_comment(
        owner,
        {
            'ig_comment_id': 'comment-b-2',
            'media_id': 'mediaB',
            'commenter_id': 'igA',
            'commenter_username': 'account_a',
            'text': 'test again',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))

    assert first['matched'] is True
    assert second['already_processed'] is True
    assert second['classified_reason'] == 'same_commenter_same_post_same_rule'
    assert len(reply_calls) == 1
    assert len(dm_calls) == 1
    assert len(db.comments.docs) == 1
    assert db.comments.docs[0]['opening_dedupe_key']


def test_same_commenter_different_account_is_not_cross_suppressed(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    reply_calls = []
    dm_calls = []

    async def reply_ok(access_token, comment_id, text):
        reply_calls.append((access_token, comment_id, text))
        return _reply_provider_ok()

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        dm_calls.append((access_token, ig_user_id, recipient_id, message,
                         allow_workspace_recipient))
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid'}}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    owner_b = server._with_instagram_account_context(
        db.users.docs[0],
        db.instagram_accounts.docs[1],
    )
    owner_a = server._with_instagram_account_context(
        db.users.docs[0],
        db.instagram_accounts.docs[0],
    )
    _run(server._handle_new_comment(
        owner_b,
        {
            'ig_comment_id': 'comment-b-1',
            'media_id': 'shared-looking-media',
            'commenter_id': 'external-user',
            'text': 'test',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))
    result_a = _run(server._handle_new_comment(
        owner_a,
        {
            'ig_comment_id': 'comment-a-1',
            'media_id': 'shared-looking-media',
            'commenter_id': 'external-user',
            'text': 'test',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))

    assert result_a['matched'] is True
    assert len(reply_calls) == 2
    assert len(dm_calls) == 2
    assert {doc['instagramAccountId'] for doc in db.comments.docs} == {'igA', 'igB'}


def test_comment_flow_quick_reply_uses_payload_session_for_second_account(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    now = datetime.utcnow()
    db.comment_dm_sessions.docs.append({
        'id': 'session-b',
        'user_id': 'u1',
        'instagramAccountId': 'igB',
        'igUserId': 'igB',
        'ig_user_id': 'igB',
        'recipient_id': 'igsid-original',
        'automation_id': 'ruleB',
        'status': 'pending',
        'stage': 'awaiting_user_action',
        'payload': 'comment_flow:session-b:continue',
        'created': now,
        'updated': now,
    })
    completion_calls = []

    async def completion_ok(user_doc, session):
        completion_calls.append({
            'ig_user_id': user_doc.get('ig_user_id'),
            'token': user_doc.get('meta_access_token'),
            'session_id': session.get('id'),
        })
        return True

    monkeypatch.setattr(server, '_send_comment_dm_flow_completion', completion_ok)
    owner_b = server._with_instagram_account_context(
        db.users.docs[0],
        db.instagram_accounts.docs[1],
    )

    result = _run(server._handle_new_dm_message(
        owner_b,
        {
            'sender': {'id': 'igA'},
            'recipient': {'id': 'igB'},
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'message': {
                'mid': 'mid-quick-reply',
                'text': 'Send me the link',
                'quick_reply': {'payload': 'comment_flow:session-b:continue'},
            },
        },
        source='webhook',
    ))

    assert result['matched'] is True
    assert result['status'] == 'replied'
    assert completion_calls == [{
        'ig_user_id': 'igB',
        'token': 'token-b',
        'session_id': 'session-b',
    }]
    assert db.dm_logs.docs[0]['comment_flow_session_id'] == 'session-b'


def test_account_b_post_specific_quick_reply_routes_by_recipient_not_entry(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    db.automations.docs = [
        _comment_rule('accA', 'igA', 'ruleA'),
        _post_specific_comment_flow_rule('accB', 'igB', 'ruleB', 'mediaB'),
    ]
    reply_calls = []
    message_calls = []

    async def reply_ok(access_token, comment_id, text):
        reply_calls.append({
            'access_token': access_token,
            'comment_id': comment_id,
            'text': text,
        })
        return _reply_provider_ok()

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        message_calls.append({
            'access_token': access_token,
            'ig_user_id': ig_user_id,
            'recipient_id': recipient_id,
            'message': message,
            'allow_workspace_recipient': allow_workspace_recipient,
        })
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid'}}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    owner_b = server._with_instagram_account_context(
        db.users.docs[0],
        db.instagram_accounts.docs[1],
    )
    start = _run(server._handle_new_comment(
        owner_b,
        {
            'ig_comment_id': 'comment-on-b-from-a',
            'media_id': 'mediaB',
            'commenter_id': 'igA',
            'commenter_username': 'account_a',
            'text': 'test',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))

    assert start['matched'] is True
    assert reply_calls[0]['access_token'] == 'token-b'
    assert len(message_calls) == 1
    opening = message_calls[0]
    assert opening['access_token'] == 'token-b'
    assert opening['ig_user_id'] == 'igB'
    assert opening['recipient_id'] == 'igA'
    assert opening['allow_workspace_recipient'] is True
    quick_replies = opening['message']['quick_replies']
    assert quick_replies[0]['title'] == 'ابعت'
    payload = quick_replies[0]['payload']
    assert payload.startswith('comment_flow:')
    session_id = payload.split(':')[1]
    assert db.comment_dm_sessions.docs[0]['id'] == session_id
    assert db.comment_dm_sessions.docs[0]['instagramAccountId'] == 'igB'
    assert db.comment_dm_sessions.docs[0]['automation_id'] == 'ruleB'
    assert db.comment_dm_sessions.docs[0]['media_id'] == 'mediaB'

    _run(server._process_webhook({
        'object': 'instagram',
        'entry': [{
            # Live failure mode: entry.id can be ambiguous/wrong for the
            # button click, but recipient.id is the business account that
            # must own the comment-flow continuation.
            'id': 'igA',
            'time': int(datetime.utcnow().timestamp()),
            'messaging': [{
                'sender': {'id': 'igA'},
                'recipient': {'id': 'igB'},
                'timestamp': int(datetime.utcnow().timestamp() * 1000),
                'message': {
                    'mid': 'mid-click-b',
                    'text': 'ابعت',
                    'quick_reply': {'payload': payload},
                },
            }],
        }],
    }))

    assert len(message_calls) == 2
    next_step = message_calls[1]
    assert next_step['access_token'] == 'token-b'
    assert next_step['ig_user_id'] == 'igB'
    assert next_step['recipient_id'] == 'igA'
    assert next_step['allow_workspace_recipient'] is True
    button = next_step['message']['attachment']['payload']['buttons'][0]
    assert button['title'] == 'افتح اللينك'
    assert button['url'].startswith('https://example.com/')
    assert db.dm_logs.docs[0]['comment_flow_session_id'] == session_id


def test_classify_instagram_quick_reply_aliases_and_nested_postback_payloads():
    quick = server._classify_messaging_event({
        'sender': {'id': 'external'},
        'recipient': {'id': 'igB'},
        'message': {
            'mid': 'mid1',
            'text': 'ابعت',
            'quickReply': {'payload': 'comment_flow:session-b:continue'},
        },
    })
    assert quick['kind'] == 'quick_reply'
    assert quick['quick_reply_payload'] == 'comment_flow:session-b:continue'

    nested_postback = server._classify_messaging_event({
        'sender': {'id': 'external'},
        'recipient': {'id': 'igB'},
        'message': {
            'mid': 'mid2',
            'postback': {'payload': 'comment_flow:session-b:continue', 'title': 'ابعت'},
        },
    })
    assert nested_postback['postback_payload'] == 'comment_flow:session-b:continue'
