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
