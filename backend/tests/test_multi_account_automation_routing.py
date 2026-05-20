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


def test_session_lookup_resolves_by_payload_when_session_ig_user_id_is_blank(monkeypatch):
    """Production failure mode: a pending session was written with an
    empty ``ig_user_id`` field (legacy doc, migration, or a session
    created before _with_instagram_account_context applied). The
    secure payload session_id must still resolve the session — adding
    a brittle ig_user_id equality filter is what broke quick-reply
    continuation on the second account."""
    db = _install_multi_account_db(monkeypatch)
    now = datetime.utcnow()
    db.comment_dm_sessions.docs.append({
        'id': 'session-legacy-blank',
        'user_id': 'u1',
        # Intentionally blank account fields — this is the production
        # shape that broke the old query.
        'ig_user_id': '',
        'instagramAccountId': '',
        'igUserId': '',
        'recipient_id': 'igsid-external',
        'automation_id': 'ruleB',
        'status': 'pending',
        'stage': 'awaiting_user_action',
        'payload': 'comment_flow:session-legacy-blank:continue',
        'created': now,
        'updated': now,
    })
    owner_b = server._with_instagram_account_context(
        db.users.docs[0], db.instagram_accounts.docs[1],
    )
    session = _run(server._find_pending_comment_dm_session(
        owner_b, 'igsid-external',
        payload='comment_flow:session-legacy-blank:continue',
    ))
    assert session is not None
    assert session['id'] == 'session-legacy-blank'


def test_session_lookup_payload_first_when_session_account_drifted(monkeypatch):
    """Production failure mode: session was written with a different
    ig_user_id than the click webhook resolved to (e.g. the user
    re-linked an IG account and the canonical id rotated). The signed
    payload session_id is sufficient identity; the lookup must NOT
    silently drop the session because of the drift."""
    db = _install_multi_account_db(monkeypatch)
    now = datetime.utcnow()
    db.comment_dm_sessions.docs.append({
        'id': 'session-drifted',
        'user_id': 'u1',
        'ig_user_id': 'igOLD',  # drifted/stale account id
        'instagramAccountId': 'igOLD',
        'igUserId': 'igOLD',
        'recipient_id': 'igsid-external',
        'automation_id': 'ruleB',
        'status': 'pending',
        'stage': 'awaiting_user_action',
        'payload': 'comment_flow:session-drifted:continue',
        'created': now,
        'updated': now,
    })
    owner_b = server._with_instagram_account_context(
        db.users.docs[0], db.instagram_accounts.docs[1],
    )
    session = _run(server._find_pending_comment_dm_session(
        owner_b, 'igsid-external',
        payload='comment_flow:session-drifted:continue',
    ))
    assert session is not None
    assert session['id'] == 'session-drifted'


def test_selected_specific_media_id_accepts_alias_field_names():
    """Post-specific rules must be matched even when stored under any
    of the historical alias names (selected_media_id, target_post_id,
    instagram_media_id, etc). A field-name drift must never silently
    convert a post-specific rule into a no-match."""
    aliases = [
        'media_id',
        'trigger_media_id',
        'selected_media_id',
        'selectedMediaId',
        'target_media_id',
        'targetMediaId',
        'selected_post_id',
        'selectedPostId',
        'target_post_id',
        'targetPostId',
        'instagram_media_id',
        'instagramMediaId',
        'post_id',
        'postId',
        'ig_media_id',
        'igMediaId',
    ]
    for key in aliases:
        rule = {'post_scope': 'specific', key: 'mediaP'}
        assert server._selected_specific_media_id(rule) == 'mediaP', (
            f'media_id alias {key} should resolve to mediaP but did not')


def test_selected_specific_media_id_returns_none_for_broad_rule():
    rule = {'post_scope': 'any', 'trigger': 'comment:any'}
    assert server._selected_specific_media_id(rule) is None
    latest = {'post_scope': 'latest', 'trigger': 'comment:latest'}
    assert server._selected_specific_media_id(latest) is None


def test_post_specific_rule_does_not_fire_on_unrelated_media(monkeypatch):
    """Account B's post-specific rule for mediaP must NOT fire when a
    comment lands on mediaQ on Account B."""
    db = _install_multi_account_db(monkeypatch)
    db.automations.docs = [
        _post_specific_comment_flow_rule('accB', 'igB', 'ruleB-specific', 'mediaP'),
    ]
    reply_calls = []
    send_calls = []

    async def reply_ok(access_token, comment_id, text):
        reply_calls.append((access_token, comment_id, text))
        return _reply_provider_ok()

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        send_calls.append((access_token, ig_user_id, recipient_id, message))
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid'}}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    owner_b = server._with_instagram_account_context(
        db.users.docs[0], db.instagram_accounts.docs[1],
    )
    result = _run(server._handle_new_comment(
        owner_b,
        {
            'ig_comment_id': 'comment-q-1',
            'media_id': 'mediaQ',
            'commenter_id': 'igsid-external',
            'commenter_username': 'follower',
            'text': 'test',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))
    assert result['matched'] is False
    assert reply_calls == []
    assert send_calls == []


def test_account_a_general_rule_does_not_fire_on_account_b_post(monkeypatch):
    """Sibling-account scoping: Account A's general comment:any rule
    must only fire on Account A posts. A comment on Account B that
    routes via owner_b must hit Account B's rule set only."""
    db = _install_multi_account_db(monkeypatch)
    db.automations.docs = [
        _comment_rule('accA', 'igA', 'ruleA-general'),
        _post_specific_comment_flow_rule('accB', 'igB', 'ruleB-specific', 'mediaP'),
    ]
    reply_calls = []
    send_calls = []

    async def reply_ok(access_token, comment_id, text):
        reply_calls.append({
            'access_token': access_token, 'comment_id': comment_id, 'text': text,
        })
        return _reply_provider_ok()

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        send_calls.append({
            'access_token': access_token, 'ig_user_id': ig_user_id,
            'recipient_id': recipient_id,
        })
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid'}}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    owner_b = server._with_instagram_account_context(
        db.users.docs[0], db.instagram_accounts.docs[1],
    )
    result = _run(server._handle_new_comment(
        owner_b,
        {
            'ig_comment_id': 'comment-on-mediaP',
            'media_id': 'mediaP',
            'commenter_id': 'igsid-external',
            'commenter_username': 'follower',
            'text': 'pls send',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))
    assert result['matched'] is True
    assert result['rule_id'] == 'ruleB-specific'
    # Public reply + opening DM must both go via Account B's token
    assert all(c['access_token'] == 'token-b' for c in reply_calls)
    assert all(c['access_token'] == 'token-b' for c in send_calls)


def test_quick_reply_routes_to_account_b_when_entry_id_belongs_to_account_a(monkeypatch):
    """Live production failure shape: Meta delivers a quick-reply event
    inside an entry whose ``entry.id`` is Account A, but the
    ``messaging[].recipient.id`` is Account B. The flow continuation
    must use Account B's token + ig_user_id (the recipient is the
    business account that received the click)."""
    db = _install_multi_account_db(monkeypatch)
    now = datetime.utcnow()
    db.comment_dm_sessions.docs.append({
        'id': 'session-b-entry-drift',
        'user_id': 'u1',
        'instagramAccountId': 'igB',
        'igUserId': 'igB',
        'ig_user_id': 'igB',
        'recipient_id': 'igsid-external',
        'automation_id': 'ruleB',
        'status': 'pending',
        'stage': 'awaiting_user_action',
        'payload': 'comment_flow:session-b-entry-drift:continue',
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
    _run(server._process_webhook({
        'object': 'instagram',
        'entry': [{
            'id': 'igA',  # entry.id misleadingly points at Account A
            'time': int(datetime.utcnow().timestamp()),
            'messaging': [{
                'sender': {'id': 'igsid-external'},
                'recipient': {'id': 'igB'},
                'timestamp': int(datetime.utcnow().timestamp() * 1000),
                'message': {
                    'mid': 'mid-quick-reply-cross-entry',
                    'text': 'continue',
                    'quick_reply': {
                        'payload': 'comment_flow:session-b-entry-drift:continue',
                    },
                },
            }],
        }],
    }))
    assert completion_calls == [{
        'ig_user_id': 'igB',
        'token': 'token-b',
        'session_id': 'session-b-entry-drift',
    }]


def test_comment_webhook_value_alias_media_id_fields_resolve(monkeypatch):
    """When IG sends the comment value with the older
    ``value.post_id`` / ``value.mediaId`` shape instead of
    ``value.media.id``, the resolved media_id must still flow into the
    comment row so the post-specific rule matching can succeed.

    We assert at the comments doc level (the structural contract) so
    the test does not depend on fire-and-forget action background
    tasks; the public-reply + opening-DM happiness path is already
    covered by test_account_b_post_specific_quick_reply_routes_by_recipient_not_entry.
    """
    import time as _time
    db = _install_multi_account_db(monkeypatch)
    rule_b = _post_specific_comment_flow_rule('accB', 'igB', 'ruleB-specific', 'mediaAliasP')
    # Push the rule's activation cutoff a full day into the past so
    # the entry.time derived from time.time() is unambiguously after
    # the activation regardless of local timezone interactions in the
    # test process.
    rule_b['activationStartedAt'] = datetime.utcnow() - timedelta(days=1)
    db.automations.docs = [rule_b]

    async def reply_ok(access_token, comment_id, text):
        return _reply_provider_ok()

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid'}}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    _run(server._process_webhook({
        'object': 'instagram',
        'entry': [{
            'id': 'igB',
            'time': int(_time.time()),
            'changes': [{
                'field': 'comments',
                'value': {
                    'id': 'comment-alias',
                    'from': {'id': 'igsid-external', 'username': 'follower'},
                    # value.post_id alias — no value.media.id field
                    'post_id': 'mediaAliasP',
                    'text': 'send me',
                },
            }],
        }],
    }))
    assert db.comments.docs, 'expected the alias-shape webhook to produce a comment row'
    assert db.comments.docs[0]['media_id'] == 'mediaAliasP'
    # Post-specific rule MUST match — the resolved media_id is the
    # rule's selected_specific_media_id.
    assert db.comments.docs[0]['rule_id'] == 'ruleB-specific'
    assert db.comments.docs[0]['matched_rule_scope'] == 'specific_post_exact'


def test_quick_reply_continuation_not_blocked_by_sibling_loop_guard(monkeypatch):
    """Task H: when the click on Account B's opening DM comes back
    via Meta as ``sender = sibling Account A``, the secure
    ``comment_flow:`` payload continuation MUST run BEFORE the
    sibling-account DM loop guard. The continuation is a known-safe
    response to a comment-DM session the bot itself started, not a
    free-form DM that could ping-pong with another bot."""
    db = _install_multi_account_db(monkeypatch)
    now = datetime.utcnow()
    db.comment_dm_sessions.docs.append({
        'id': 'session-sibling-click',
        'user_id': 'u1',
        'instagramAccountId': 'igB',
        'igUserId': 'igB',
        'ig_user_id': 'igB',
        # recipient_id captured at session creation == the original
        # commenter, which IS one of our linked accounts (the user
        # tested by commenting from their other linked account).
        'recipient_id': 'igA',
        'automation_id': 'ruleB',
        'status': 'pending',
        'stage': 'awaiting_user_action',
        'payload': 'comment_flow:session-sibling-click:continue',
        'created': now,
        'updated': now,
    })
    completion_calls = []

    async def completion_ok(user_doc, session):
        completion_calls.append({
            'session_id': session.get('id'),
            'ig_user_id': user_doc.get('ig_user_id'),
        })
        return True

    monkeypatch.setattr(server, '_send_comment_dm_flow_completion', completion_ok)
    owner_b = server._with_instagram_account_context(
        db.users.docs[0], db.instagram_accounts.docs[1],
    )

    result = _run(server._handle_new_dm_message(
        owner_b,
        {
            'sender': {'id': 'igA'},  # sibling account sender
            'recipient': {'id': 'igB'},
            'timestamp': int(datetime.utcnow().timestamp() * 1000),
            'message': {
                'mid': 'mid-sibling-click',
                'text': 'continue',
                'quick_reply': {'payload': 'comment_flow:session-sibling-click:continue'},
            },
        },
        source='webhook',
    ))
    # Comment-flow continuation must succeed without being silenced
    # by the sibling-account-sender guard.
    assert result['status'] == 'replied'
    assert completion_calls == [{
        'session_id': 'session-sibling-click',
        'ig_user_id': 'igB',
    }]
    # No skip-row should have been written for the sibling guard.
    assert all(l.get('skip_reason') != 'sibling_account_sender'
               for l in db.dm_logs.docs)


def test_handle_new_comment_does_not_silently_match_when_media_id_missing(monkeypatch):
    """If the webhook delivers a comment with NO resolvable media_id,
    a post-specific rule must NOT silently fire. Without this, a
    payload shape change (e.g. Meta moves the field) could route any
    comment to the post-specific rule's recipient list."""
    db = _install_multi_account_db(monkeypatch)
    db.automations.docs = [
        _post_specific_comment_flow_rule('accB', 'igB', 'ruleB-specific', 'mediaP'),
    ]

    async def reply_ok(access_token, comment_id, text):
        return _reply_provider_ok()

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid'}}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    owner_b = server._with_instagram_account_context(
        db.users.docs[0], db.instagram_accounts.docs[1],
    )
    result = _run(server._handle_new_comment(
        owner_b,
        {
            'ig_comment_id': 'comment-no-media',
            'media_id': None,  # the alias resolution returned nothing
            'commenter_id': 'igsid-external',
            'commenter_username': 'follower',
            'text': 'test',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))
    assert result['matched'] is False


# ---------------------------------------------------------------------------
# Admin reset-test-flow endpoint (Task F: targeted unblock for stale dedupe)
# ---------------------------------------------------------------------------

def _seed_dedupe_state(db, *, user_id, ig_account_id, automation_id,
                      media_id, commenter_id, session_id='sess-x',
                      comment_id='comm-x', extra_session_for_commenter=None):
    """Insert one matching session + one matching comment row with a real
    SHA-256 opening_dedupe_key, so the reset endpoint can find them
    exactly like the production opener would."""
    dedupe_key = server._comment_opening_dedupe_key(
        user_id, ig_account_id, automation_id, media_id, commenter_id,
    )
    now = datetime.utcnow()
    db.comment_dm_sessions.docs.append({
        'id': session_id,
        'user_id': user_id,
        'ig_user_id': ig_account_id,
        'instagramAccountId': ig_account_id,
        'igUserId': ig_account_id,
        'automation_id': automation_id,
        'media_id': media_id,
        'mediaId': media_id,
        'recipient_id': commenter_id,
        'commenter_id': commenter_id,
        'opening_dedupe_key': dedupe_key,
        'status': 'pending',
        'stage': 'awaiting_user_action',
        'payload': f'comment_flow:{session_id}:continue',
        'created': now,
        'updated': now,
        'finalDmSentAt': None,
    })
    db.comments.docs.append({
        'id': comment_id,
        'user_id': user_id,
        'instagramAccountId': ig_account_id,
        'igUserId': ig_account_id,
        'ig_comment_id': f'ig-{comment_id}',
        'media_id': media_id,
        'mediaId': media_id,
        'commenter_id': commenter_id,
        'rule_id': automation_id,
        'ruleId': automation_id,
        'matched_rule_id': automation_id,
        'opening_dedupe_key': dedupe_key,
        'openingDedupeKey': dedupe_key,
        'action_status': 'success',
        'reply_status': 'success',
        'dm_status': 'success',
        'created': now,
        'updated': now,
    })
    return dedupe_key


def _patch_admin_gate(monkeypatch):
    async def allow_admin(*args, **kwargs):
        return ({'id': 'admin-1', 'email': 'owner@example.com'}, 'owner')
    monkeypatch.setattr(server, '_require_admin_permission', allow_admin)


async def _record_action_noop(*args, **kwargs):
    return None


def test_admin_reset_test_flow_dry_run_lists_matching_session_and_comment(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igB',
        automation_id='ruleB', media_id='mediaB', commenter_id='igsid-tester',
    )

    result = _run(server.admin_reset_test_flow(
        body={
            'instagram_account_id': 'igB',
            'automation_id': 'ruleB',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-tester',
            'dry_run': True,
            'confirm': False,
        },
        user_id='u1',
    ))
    assert result['dry_run'] is True
    assert result['confirm'] is False
    assert len(result['would_delete_sessions']) == 1
    assert len(result['would_clear_opening_dedupe_on_comments']) == 1
    # No mutation must have happened.
    assert len(db.comment_dm_sessions.docs) == 1
    assert db.comments.docs[0]['opening_dedupe_key'] is not None


def test_admin_reset_test_flow_confirm_deletes_session_and_clears_dedupe(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igB',
        automation_id='ruleB', media_id='mediaB', commenter_id='igsid-tester',
    )

    result = _run(server.admin_reset_test_flow(
        body={
            'instagram_account_id': 'igB',
            'automation_id': 'ruleB',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-tester',
            'dry_run': False,
            'confirm': True,
        },
        user_id='u1',
    ))
    assert result['dry_run'] is False
    assert result['confirm'] is True
    assert result['sessions_deleted'] == 1
    assert result['comments_cleared'] == 1
    assert db.comment_dm_sessions.docs == []
    # Comment row remains, dedupe key nulled out, reset marker set.
    assert db.comments.docs[0]['opening_dedupe_key'] is None
    assert db.comments.docs[0]['openingDedupeKey'] is None
    assert 'reset_by_admin_at' in db.comments.docs[0]


def test_admin_reset_test_flow_does_not_touch_other_accounts(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    # Account B target — to be reset.
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igB',
        automation_id='ruleB', media_id='mediaB', commenter_id='igsid-tester',
        session_id='sess-b', comment_id='comm-b',
    )
    # Account A neighbor — same commenter on a different IG account.
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igA',
        automation_id='ruleA', media_id='mediaA', commenter_id='igsid-tester',
        session_id='sess-a', comment_id='comm-a',
    )

    _run(server.admin_reset_test_flow(
        body={
            'instagram_account_id': 'igB',
            'automation_id': 'ruleB',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-tester',
            'dry_run': False,
            'confirm': True,
        },
        user_id='u1',
    ))
    # Only Account B's session deleted; A's session untouched.
    remaining_session_ids = {s['id'] for s in db.comment_dm_sessions.docs}
    assert remaining_session_ids == {'sess-a'}
    # Only Account B's comment dedupe cleared.
    a_comment = next(c for c in db.comments.docs if c['id'] == 'comm-a')
    b_comment = next(c for c in db.comments.docs if c['id'] == 'comm-b')
    assert a_comment['opening_dedupe_key'] is not None
    assert b_comment['opening_dedupe_key'] is None


def test_admin_reset_test_flow_does_not_touch_other_commenters(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igB',
        automation_id='ruleB', media_id='mediaB', commenter_id='igsid-target',
        session_id='sess-target', comment_id='comm-target',
    )
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igB',
        automation_id='ruleB', media_id='mediaB', commenter_id='igsid-other',
        session_id='sess-other', comment_id='comm-other',
    )

    _run(server.admin_reset_test_flow(
        body={
            'instagram_account_id': 'igB',
            'automation_id': 'ruleB',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-target',
            'dry_run': False,
            'confirm': True,
        },
        user_id='u1',
    ))
    remaining_session_ids = {s['id'] for s in db.comment_dm_sessions.docs}
    assert remaining_session_ids == {'sess-other'}
    other_comment = next(c for c in db.comments.docs if c['id'] == 'comm-other')
    target_comment = next(c for c in db.comments.docs if c['id'] == 'comm-target')
    assert other_comment['opening_dedupe_key'] is not None
    assert target_comment['opening_dedupe_key'] is None


def test_admin_reset_test_flow_requires_all_four_fields(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)

    for missing_key in ('instagram_account_id', 'automation_id', 'media_id', 'commenter_id'):
        body = {
            'instagram_account_id': 'igB',
            'automation_id': 'ruleB',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-tester',
            'dry_run': True,
            'confirm': False,
        }
        body[missing_key] = ''
        try:
            _run(server.admin_reset_test_flow(body=body, user_id='u1'))
        except server.HTTPException as exc:
            assert exc.status_code == 400
            assert missing_key in str(exc.detail)
        else:
            raise AssertionError(f'expected HTTPException(400) when {missing_key} is empty')


def test_admin_reset_test_flow_dry_run_default_when_confirm_omitted(monkeypatch):
    """Safety: even if the caller forgets dry_run, the endpoint defaults
    to dry-run preview unless BOTH dry_run=false AND confirm=true."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igB',
        automation_id='ruleB', media_id='mediaB', commenter_id='igsid-tester',
    )

    # Caller forgets to set confirm — must default to dry-run, NOT mutate.
    result = _run(server.admin_reset_test_flow(
        body={
            'instagram_account_id': 'igB',
            'automation_id': 'ruleB',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-tester',
            'dry_run': False,
        },
        user_id='u1',
    ))
    assert result['dry_run'] is True
    assert result['confirm'] is False
    assert len(db.comment_dm_sessions.docs) == 1


def test_after_reset_same_commenter_post_rule_can_start_a_new_flow(monkeypatch):
    """End-to-end safety: after a confirmed reset, the same external
    commenter posting a fresh comment on the same post + rule must hit
    the opener path again (no dedupe block) and produce a public reply
    + opening DM."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    db.automations.docs = [
        _post_specific_comment_flow_rule('accB', 'igB', 'ruleB-flow', 'mediaB'),
    ]
    db.automations.docs[0]['activationStartedAt'] = datetime.utcnow() - timedelta(days=1)
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igB',
        automation_id='ruleB-flow', media_id='mediaB', commenter_id='igsid-tester',
    )

    _run(server.admin_reset_test_flow(
        body={
            'instagram_account_id': 'igB',
            'automation_id': 'ruleB-flow',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-tester',
            'dry_run': False,
            'confirm': True,
        },
        user_id='u1',
    ))

    reply_calls = []
    send_calls = []

    async def reply_ok(access_token, comment_id, text):
        reply_calls.append({'access_token': access_token, 'comment_id': comment_id})
        return _reply_provider_ok()

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        send_calls.append({'access_token': access_token, 'ig_user_id': ig_user_id})
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid'}}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    owner_b = server._with_instagram_account_context(
        db.users.docs[0], db.instagram_accounts.docs[1],
    )
    result = _run(server._handle_new_comment(
        owner_b,
        {
            'ig_comment_id': 'fresh-comment-after-reset',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-tester',
            'commenter_username': 'tester',
            'text': 'send me',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))
    assert result['matched'] is True
    assert result['rule_id'] == 'ruleB-flow'
    assert any(c['access_token'] == 'token-b' for c in reply_calls)
    assert any(c['ig_user_id'] == 'igB' for c in send_calls)


def test_after_reset_dedupe_still_blocks_immediate_repeat_on_same_new_comment(monkeypatch):
    """Post-reset, the SAME commenter immediately re-opening must hit
    the freshly created session/comment dedupe — the reset must not
    permanently disable dedupe for this triple."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    db.automations.docs = [
        _post_specific_comment_flow_rule('accB', 'igB', 'ruleB-flow', 'mediaB'),
    ]
    db.automations.docs[0]['activationStartedAt'] = datetime.utcnow() - timedelta(days=1)
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igB',
        automation_id='ruleB-flow', media_id='mediaB', commenter_id='igsid-tester',
    )
    _run(server.admin_reset_test_flow(
        body={
            'instagram_account_id': 'igB',
            'automation_id': 'ruleB-flow',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-tester',
            'dry_run': False,
            'confirm': True,
        },
        user_id='u1',
    ))

    async def reply_ok(access_token, comment_id, text):
        return _reply_provider_ok()

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid'}}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    owner_b = server._with_instagram_account_context(
        db.users.docs[0], db.instagram_accounts.docs[1],
    )
    # First fresh comment — fires.
    _run(server._handle_new_comment(
        owner_b,
        {
            'ig_comment_id': 'fresh-1',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-tester',
            'text': 'test',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))
    # Second fresh comment from same commenter — must be dedupe-blocked.
    second = _run(server._handle_new_comment(
        owner_b,
        {
            'ig_comment_id': 'fresh-2',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-tester',
            'text': 'test again',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))
    assert second.get('already_processed') is True
    assert second.get('classified_reason') == 'same_commenter_same_post_same_rule'
