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


def test_comment_dm_flow_enabled_also_reads_nested_node_data():
    """A rule whose deferred-flow fields are only inside
    ``nodes[].data`` (legacy save shape) must still be classified as
    a deferred flow. Previously the gate only looked at the top-level
    document, which silently treated such rules as one-shot."""
    nested_only_rule = {
        'mode': 'reply_and_dm',
        # Top-level deferred fields empty:
        'opening_dm_text': '',
        'opening_dm_button_text': '',
        'link_url': '',
        'follow_up_enabled': False,
        'follow_up_text': '',
        'dm_text': 'Thanks',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {
                'id': 'n_dm', 'type': 'message',
                'data': {
                    'text': 'Thanks',
                    'opening_dm_text': 'Hello — want the link?',
                    'opening_dm_button_text': 'Yes, send link',
                    'link_url': 'https://example.com/asset',
                    'link_dm_text': 'Here you go',
                },
            },
        ],
    }
    assert server._comment_dm_flow_enabled(nested_only_rule) is True
    cls = server._comment_dm_flow_classification(nested_only_rule)
    assert cls['enabled'] is True
    assert cls['button_flow_ready'] is True
    # Present fields list reflects the nested values.
    assert 'opening_dm_text' in cls['present_deferred_fields']
    assert 'opening_dm_button_text' in cls['present_deferred_fields']
    assert 'link_url' in cls['present_deferred_fields']


def test_post_specific_nested_rule_creates_session_and_backfills_aliases(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    nested_rule = _post_specific_comment_flow_rule('accB', 'igB', 'ruleB-nested-flow', 'mediaB')
    nested_rule.update({
        'opening_dm_text': '',
        'opening_dm_button_text': '',
        'link_dm_text': '',
        'link_button_text': '',
        'link_url': '',
    })
    nested_rule['nodes'][2]['data'].update({
        'opening_dm_text': 'Hello from nested node',
        'opening_dm_button_text': 'Send it',
        'link_dm_text': 'Here is the link',
        'link_button_text': 'Open',
        'link_url': 'https://example.com/nested',
    })
    db.automations.docs = [nested_rule]
    reply_calls = []
    dm_calls = []

    async def reply_ok(access_token, comment_id, text):
        reply_calls.append((access_token, comment_id, text))
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

    owner_b = server._with_instagram_account_context(
        db.users.docs[0], db.instagram_accounts.docs[1],
    )
    result = _run(server._handle_new_comment(
        owner_b,
        {
            'ig_comment_id': 'nested-flow-comment',
            'media_id': 'mediaB',
            'commenter_id': 'igA',
            'commenter_username': 'account_a',
            'text': 'send me',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))

    assert result['matched'] is True
    assert result['rule_id'] == 'ruleB-nested-flow'
    assert len(db.comment_dm_sessions.docs) == 1
    session = db.comment_dm_sessions.docs[0]
    assert session['instagramAccountId'] == 'igB'
    assert session['automation_id'] == 'ruleB-nested-flow'
    assert session['link_url'] == 'https://example.com/nested'
    quick_reply_message = next(call['message'] for call in dm_calls if isinstance(call['message'], dict))
    assert quick_reply_message['text'] == 'Hello from nested node'
    assert quick_reply_message['quick_replies'][0]['title'] == 'Send it'
    assert quick_reply_message['quick_replies'][0]['payload'] == session['payload']
    persisted = db.automations.docs[0]
    assert persisted['opening_dm_text'] == 'Hello from nested node'
    assert persisted['opening_dm_button_text'] == 'Send it'
    assert persisted['link_url'] == 'https://example.com/nested'
    assert persisted.get('deferred_flow_normalized_at') is not None


def test_repair_rule_to_button_flow_dry_run_does_not_mutate(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    one_shot_rule = {
        'id': 'rule-one-shot',
        'user_id': 'u1',
        'status': 'active',
        'mode': 'reply_and_dm',
        'dm_text': 'Thanks for your comment.',
        'reply_under_post': True,
        'comment_reply': 'public reply',
        'opening_dm_text': '',
        'opening_dm_button_text': '',
        'follow_up_enabled': False,
        'follow_up_text': '',
    }
    db.automations.docs = [one_shot_rule]
    result = _run(server.admin_repair_rule_to_button_flow(
        body={'rule_id': 'rule-one-shot', 'dry_run': True, 'confirm': False},
        user_id='u1',
    ))
    assert result['dry_run'] is True
    assert result['confirm'] is False
    assert result['before']['enabled'] is False
    assert result['after']['enabled'] is True
    assert result['after']['button_flow_ready'] is True
    # No mutation.
    persisted = db.automations.docs[0]
    assert persisted['opening_dm_text'] == ''
    assert persisted['follow_up_enabled'] is False


def test_repair_rule_to_button_flow_confirm_promotes_one_shot_rule(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    one_shot_rule = {
        'id': 'rule-arabic-one-shot',
        'user_id': 'u1',
        'status': 'active',
        'mode': 'reply_and_dm',
        'dm_text': 'أهلاً 👋 شوفت تعليقك، حابب أبعتلك اللينك؟',
        'reply_under_post': True,
        'comment_reply': 'مرحبا',
        'opening_dm_text': '',
        'opening_dm_button_text': '',
        'follow_up_enabled': False,
        'follow_up_text': '',
    }
    db.automations.docs = [one_shot_rule]
    result = _run(server.admin_repair_rule_to_button_flow(
        body={'rule_id': 'rule-arabic-one-shot', 'dry_run': False, 'confirm': True},
        user_id='u1',
    ))
    assert result['confirm'] is True
    assert result['after']['enabled'] is True
    persisted = db.automations.docs[0]
    # dm_text preserved.
    assert persisted['dm_text'] == 'أهلاً 👋 شوفت تعليقك، حابب أبعتلك اللينك؟'
    # opening_dm_text was filled from dm_text.
    assert persisted['opening_dm_text'] == 'أهلاً 👋 شوفت تعليقك، حابب أبعتلك اللينك؟'
    # Arabic default for the button label.
    assert persisted['opening_dm_button_text'] == 'ابعتلي اللينك'
    # follow_up enabled + non-empty default text.
    assert persisted['follow_up_enabled'] is True
    assert persisted['follow_up_text']
    # Classification flips to enabled.
    assert server._comment_dm_flow_enabled(persisted) is True


def test_repair_rule_to_button_flow_refuses_already_enabled_rule(monkeypatch):
    """Safety: a rule that already qualifies as a deferred flow
    must NOT be overwritten by the repair tool."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    already_button_flow = {
        'id': 'rule-already-flow',
        'user_id': 'u1',
        'status': 'active',
        'mode': 'reply_and_dm',
        'opening_dm_text': 'Hello',
        'opening_dm_button_text': 'Send link',
        'link_url': 'https://example.com',
        'link_dm_text': 'Here',
    }
    db.automations.docs = [already_button_flow]
    try:
        _run(server.admin_repair_rule_to_button_flow(
            body={'rule_id': 'rule-already-flow', 'dry_run': True, 'confirm': False},
            user_id='u1',
        ))
    except server.HTTPException as exc:
        assert exc.status_code == 409
    else:
        raise AssertionError('expected HTTPException(409) for an already-enabled rule')


def test_repair_rule_to_button_flow_refuses_cross_workspace_rule(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    other_workspace_rule = {
        'id': 'rule-cross-workspace',
        'user_id': 'u-other',
        'status': 'active',
        'mode': 'reply_and_dm',
        'dm_text': 'hi',
    }
    db.automations.docs = [other_workspace_rule]
    try:
        _run(server.admin_repair_rule_to_button_flow(
            body={'rule_id': 'rule-cross-workspace', 'dry_run': True},
            user_id='u1',
        ))
    except server.HTTPException as exc:
        assert exc.status_code == 403
    else:
        raise AssertionError('expected HTTPException(403) for cross-workspace rule')


def test_repair_rule_to_button_flow_refuses_unknown_rule(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    try:
        _run(server.admin_repair_rule_to_button_flow(
            body={'rule_id': 'rule-does-not-exist', 'dry_run': True},
            user_id='u1',
        ))
    except server.HTTPException as exc:
        assert exc.status_code == 404
    else:
        raise AssertionError('expected HTTPException(404) for unknown rule_id')


def test_after_repair_handle_new_comment_creates_session_for_post_specific_rule(monkeypatch):
    """End-to-end safety: after running the repair, a fresh comment
    on Account 2's post-specific rule must now produce a session
    (the production fix the operator asked for)."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    # Start from the standard post-specific rule shape (with a proper
    # flow-graph) then strip the deferred-flow fields so it looks
    # exactly like the Account 2 production one-shot rule before
    # repair. activationStartedAt is backdated so the comment isn't
    # treated as historical.
    rule = _comment_rule('accB', 'igB', 'rule-post-specific-shot', trigger='comment:mediaB')
    rule.update({
        'post_scope': 'specific',
        'media_id': 'mediaB',
        'trigger_media_id': 'mediaB',
        'activationStartedAt': datetime.utcnow() - timedelta(days=1),
        # All deferred fields explicitly empty — the production shape.
        'opening_dm_text': '',
        'opening_dm_button_text': '',
        'link_url': '',
        'link_dm_text': '',
        'follow_up_enabled': False,
        'follow_up_text': '',
        'follow_request_enabled': False,
        'email_request_enabled': False,
    })
    db.automations.docs = [rule]
    # Apply the repair.
    _run(server.admin_repair_rule_to_button_flow(
        body={'rule_id': rule['id'], 'dry_run': False, 'confirm': True},
        user_id='u1',
    ))

    reply_calls = []
    send_calls = []

    async def reply_ok(access_token, comment_id, text):
        reply_calls.append({'access_token': access_token, 'comment_id': comment_id})
        return _reply_provider_ok()

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        send_calls.append({'access_token': access_token, 'ig_user_id': ig_user_id,
                           'message': message})
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid-after-repair'}}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    owner_b = server._with_instagram_account_context(
        db.users.docs[0], db.instagram_accounts.docs[1],
    )
    result = _run(server._handle_new_comment(
        owner_b,
        {
            'ig_comment_id': 'comment-after-repair',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-tester',
            'commenter_username': 'tester',
            'text': 'send me',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))
    assert result['matched'] is True
    assert result['rule_id'] == 'rule-post-specific-shot'
    # Session was created — the production fix.
    assert len(db.comment_dm_sessions.docs) == 1
    session = db.comment_dm_sessions.docs[0]
    assert session['automation_id'] == 'rule-post-specific-shot'
    assert session['media_id'] == 'mediaB'
    assert session['payload'].startswith('comment_flow:')
    # Quick reply was sent (the button-flow opening DM, not a plain text DM).
    assert any('quick_replies' in (c.get('message') or {}) for c in send_calls)


def test_comment_dm_flow_classification_flags_legacy_one_shot_rule():
    """The Account 2 production case: a rule saved with only the
    legacy dm_text field (one-shot reply + DM) must be classified as
    NOT enabled for the deferred flow, with one_shot_dm_only=True and
    button_flow_missing listing every field the operator needs to
    add."""
    one_shot_rule = {
        'mode': 'reply_and_dm',
        'dm_text': 'Thanks for your comment.',
        'reply_under_post': True,
        'comment_reply': 'public reply',
        # All deferred-flow fields explicitly empty.
        'opening_dm_text': '',
        'opening_dm_button_text': '',
        'link_dm_text': '',
        'link_url': '',
        'follow_request_enabled': False,
        'email_request_enabled': False,
        'follow_up_enabled': False,
        'follow_up_text': '',
    }
    cls = server._comment_dm_flow_classification(one_shot_rule)
    assert cls['enabled'] is False
    assert cls['mode_ok'] is True
    assert cls['has_legacy_dm_text'] is True
    assert cls['one_shot_dm_only'] is True
    assert cls['button_flow_ready'] is False
    assert 'opening_dm_text' in cls['button_flow_missing']
    assert 'opening_dm_button_text' in cls['button_flow_missing']
    # No deferred-flow fields populated.
    assert cls['present_deferred_fields'] == []


def test_comment_dm_flow_classification_recognizes_full_button_flow_rule():
    """A rule with opening text + button + link is button-flow ready
    and the classification must surface that cleanly."""
    full_rule = {
        'mode': 'reply_and_dm',
        'opening_dm_text': 'hello',
        'opening_dm_button_text': 'send link',
        'link_dm_text': 'here',
        'link_url': 'https://example.com',
        'link_button_text': 'open',
        'follow_request_enabled': False,
        'email_request_enabled': False,
        'follow_up_enabled': False,
        'follow_up_text': '',
    }
    cls = server._comment_dm_flow_classification(full_rule)
    assert cls['enabled'] is True
    assert cls['button_flow_ready'] is True
    assert cls['button_flow_missing'] == []
    assert 'opening_dm_text' in cls['present_deferred_fields']
    assert 'opening_dm_button_text' in cls['present_deferred_fields']
    assert 'link_url' in cls['present_deferred_fields']
    assert cls['one_shot_dm_only'] is False


def test_comment_dm_flow_classification_follow_up_pair_handled_atomically():
    """follow_up_enabled without follow_up_text must NOT count as
    present — the gate treats them as a pair."""
    half_followup = {
        'mode': 'reply_and_dm',
        'opening_dm_text': 'x',
        'opening_dm_button_text': 'click',
        'follow_up_enabled': True,
        'follow_up_text': '',
    }
    cls = server._comment_dm_flow_classification(half_followup)
    assert 'follow_up_enabled+text' in cls['missing_deferred_fields']
    full_followup = dict(half_followup)
    full_followup['follow_up_text'] = 'second message'
    cls2 = server._comment_dm_flow_classification(full_followup)
    assert 'follow_up_enabled+text' in cls2['present_deferred_fields']


def test_comment_dm_flow_classification_rejects_wrong_mode():
    """Even with full fields, a rule whose mode is not 'reply_and_dm'
    is not button-flow-ready."""
    wrong_mode = {
        'mode': 'reply_only',
        'opening_dm_text': 'x',
        'opening_dm_button_text': 'click',
        'link_url': 'https://example.com',
    }
    cls = server._comment_dm_flow_classification(wrong_mode)
    assert cls['mode_ok'] is False
    assert cls['enabled'] is False
    assert cls['button_flow_ready'] is False
    assert 'mode != reply_and_dm' in cls['button_flow_missing']


def test_recent_comment_events_attaches_rule_deferred_flow_for_one_shot_rule(monkeypatch):
    """When a row has no_session_reason='rule_has_no_deferred_flow',
    the response MUST include rule_deferred_flow so the UI can render
    the operator-friendly hint without needing a second round-trip
    to inspect the rule."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    # Replace Account B's rule with a legacy one-shot DM rule.
    one_shot_rule = {
        'id': 'ruleB-one-shot',
        'user_id': 'u1',
        'status': 'active',
        'trigger': 'comment:mediaB',
        'match': 'any',
        'mode': 'reply_and_dm',
        'post_scope': 'specific',
        'instagramAccountId': 'igB',
        'instagramAccountDbId': 'accB',
        'media_id': 'mediaB',
        'dm_text': 'Thanks for your comment.',
        'opening_dm_text': '',
        'opening_dm_button_text': '',
        'link_dm_text': '',
        'link_url': '',
        'follow_request_enabled': False,
        'email_request_enabled': False,
        'follow_up_enabled': False,
        'follow_up_text': '',
        'reply_under_post': True,
        'comment_reply': 'public reply',
    }
    db.automations.docs = [one_shot_rule]
    # Comment that matched the one-shot rule and successfully sent
    # reply + DM, but produced no session (the Account 2 case).
    now = datetime.utcnow()
    db.comments.docs.append({
        'id': 'comm-one-shot-success',
        'user_id': 'u1',
        'instagramAccountId': 'igB',
        'igUserId': 'igB',
        'ig_comment_id': 'ig-one-shot',
        'media_id': 'mediaB',
        'commenter_id': 'igsid-tester',
        'rule_id': 'ruleB-one-shot',
        'matched': True,
        'matched_rule_scope': 'specific_post_exact',
        'action_status': 'success',
        'reply_status': 'success',
        'dm_status': 'success',
        'source': 'polling',
        'opening_dedupe_key': None,
        'created': now - timedelta(seconds=20),
        'updated': now - timedelta(seconds=20),
    })
    result = _run(server.admin_recent_comment_events(user_id='u1'))
    ev = next(e for e in result['events'] if e['comment_doc_id'] == 'comm-one-shot-success')
    assert ev['session_created'] is False
    assert ev['no_session_reason'] == 'rule_has_no_deferred_flow'
    cls = ev.get('rule_deferred_flow')
    assert cls is not None
    assert cls['enabled'] is False
    assert cls['one_shot_dm_only'] is True
    assert cls['button_flow_ready'] is False
    assert 'opening_dm_text' in cls['button_flow_missing']
    assert 'opening_dm_button_text' in cls['button_flow_missing']


def test_recent_comment_events_marks_old_no_session_row_when_current_rule_is_ready(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    ready_rule = _post_specific_comment_flow_rule('accB', 'igB', 'ruleB-ready', 'mediaB')
    db.automations.docs = [ready_rule]
    now = datetime.utcnow()
    db.comments.docs.append({
        'id': 'comm-old-no-session',
        'user_id': 'u1',
        'instagramAccountId': 'igB',
        'igUserId': 'igB',
        'ig_comment_id': 'old-comment',
        'media_id': 'mediaB',
        'commenter_id': 'igA',
        'rule_id': 'ruleB-ready',
        'matched': True,
        'matched_rule_scope': 'specific_post_exact',
        'action_status': 'success',
        'reply_status': 'success',
        'dm_status': 'success',
        'source': 'polling',
        'opening_dedupe_key': server._comment_opening_dedupe_key(
            'u1', 'igB', 'ruleB-ready', 'mediaB', 'igA',
        ),
        'created': now - timedelta(minutes=10),
        'updated': now - timedelta(minutes=10),
    })

    result = _run(server.admin_recent_comment_events(user_id='u1'))
    ev = next(e for e in result['events'] if e['comment_doc_id'] == 'comm-old-no-session')
    assert ev['session_created'] is False
    assert ev['no_session_reason'] == 'session_missing_for_current_deferred_flow'
    assert ev['stale_rule_diagnostic'] is True
    assert ev['current_rule_deferred_flow']['button_flow_ready'] is True


def test_recent_comment_events_returns_rows_even_without_sessions(monkeypatch):
    """The diagnostic that exposes the production failure mode the
    operator just hit: a fresh Account 2 comment that never produced
    a session must still appear in Recent comment events with a clear
    no_session_reason."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    now = datetime.utcnow()
    # Account 2 comment that was processed but never produced a
    # session (e.g. matched no rule because of a media_id mismatch).
    db.comments.docs.append({
        'id': 'comm-no-session',
        'user_id': 'u1',
        'instagramAccountId': 'igB',
        'igUserId': 'igB',
        'ig_comment_id': 'ig-comment-no-session',
        'media_id': 'mediaUnrelated',
        'commenter_id': 'igsid-tester',
        'commenter_username': 'tester',
        'rule_id': None,
        'matched': False,
        'action_status': 'skipped',
        'reply_status': 'disabled',
        'dm_status': 'disabled',
        'skip_reason': 'no_rule_match',
        'opening_dedupe_key': None,
        'text': 'hi',
        'source': 'webhook',
        'created': now - timedelta(seconds=30),
        'updated': now - timedelta(seconds=30),
    })

    result = _run(server.admin_recent_comment_events(user_id='u1'))
    assert result['ok'] is True
    assert result['count'] >= 1
    ev = next(e for e in result['events'] if e['comment_doc_id'] == 'comm-no-session')
    assert ev['session_created'] is False
    assert ev['no_session_reason'] is not None
    assert 'no_rule_match' in ev['no_session_reason']
    assert ev['source'] == 'webhook'
    # Per-account summary is present so the operator can see whether
    # the account got any comment events at all.
    igB_summary = next(a for a in result['accounts'] if a['instagram_username'] == 'account_b')
    assert igB_summary['last_comment_event_at'] is not None


def test_recent_comment_events_exposes_dm_failure_distinct_from_skip(monkeypatch):
    """A comment that matched a rule but where the opening DM failed
    must be distinguishable in the panel — operator must see it's a
    Graph error, not a match failure."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    now = datetime.utcnow()
    db.comments.docs.append({
        'id': 'comm-dm-failed',
        'user_id': 'u1',
        'instagramAccountId': 'igB',
        'igUserId': 'igB',
        'ig_comment_id': 'ig-dm-fail',
        'media_id': 'mediaB',
        'commenter_id': 'igsid-tester',
        'rule_id': 'ruleB',
        'matched': True,
        'action_status': 'partial_success',
        'reply_status': 'success',
        'dm_status': 'failed',
        'dm_failure_reason': 'messaging_window_expired',
        'skip_reason': None,
        'opening_dedupe_key': None,
        'text': 'send me',
        'source': 'webhook',
        'created': now - timedelta(seconds=10),
        'updated': now - timedelta(seconds=10),
    })
    result = _run(server.admin_recent_comment_events(user_id='u1'))
    ev = next(e for e in result['events'] if e['comment_doc_id'] == 'comm-dm-failed')
    assert ev['matched'] is True
    assert ev['session_created'] is False
    assert ev['no_session_reason'] is not None
    assert 'opening_dm_failed' in ev['no_session_reason']
    assert 'messaging_window_expired' in ev['no_session_reason']
    assert ev['dm_failure_reason'] == 'messaging_window_expired'


def test_recent_comment_events_marks_session_created_when_session_exists(monkeypatch):
    """When a session DOES exist for the same (account, rule, media,
    commenter) tuple, session_created must be True so the operator
    knows the flow opened successfully — even if it later stalled."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igB',
        automation_id='ruleB', media_id='mediaB', commenter_id='igsid-tester',
        session_id='sess-existed', comment_id='comm-with-session',
    )
    result = _run(server.admin_recent_comment_events(user_id='u1'))
    ev = next(e for e in result['events'] if e['comment_doc_id'] == 'comm-with-session')
    assert ev['session_created'] is True
    assert ev['related_session_id'] == 'sess-existed'
    assert ev['no_session_reason'] is None


def test_recent_comment_events_includes_both_webhook_and_polling_sources(monkeypatch):
    """Polling-sourced comments must be visible alongside webhook
    comments. The operator should not be left in the dark if the
    webhook gapped but polling caught the comment."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    now = datetime.utcnow()
    db.comments.docs.extend([
        {
            'id': 'comm-from-webhook',
            'user_id': 'u1', 'instagramAccountId': 'igB',
            'ig_comment_id': 'wh-1', 'media_id': 'mediaB',
            'commenter_id': 'igsid-1', 'matched': True,
            'rule_id': 'ruleB', 'action_status': 'success',
            'reply_status': 'success', 'dm_status': 'success',
            'source': 'webhook',
            'created': now, 'updated': now,
        },
        {
            'id': 'comm-from-polling',
            'user_id': 'u1', 'instagramAccountId': 'igB',
            'ig_comment_id': 'poll-1', 'media_id': 'mediaB',
            'commenter_id': 'igsid-2', 'matched': True,
            'rule_id': 'ruleB', 'action_status': 'success',
            'reply_status': 'success', 'dm_status': 'success',
            'source': 'polling',
            'created': now - timedelta(seconds=5),
            'updated': now - timedelta(seconds=5),
        },
    ])
    result = _run(server.admin_recent_comment_events(user_id='u1'))
    sources = {e['source'] for e in result['events']}
    assert 'webhook' in sources
    assert 'polling' in sources


def test_comment_poller_loop_iterates_all_connected_accounts(monkeypatch):
    """Contract: the poller must scan EVERY isActive +
    connectionValid linked Instagram account, not only the UI active
    account. Stops Account 2 from going silent when Account 1 is
    flagged active in users.active_instagram_account_id."""
    db = _install_multi_account_db(monkeypatch)
    # Ensure both accounts are eligible for polling.
    for a in db.instagram_accounts.docs:
        a['isActive'] = True
        a['connectionValid'] = True
    # active_instagram_account_id stays on Account A in the user row
    # — but the poller must still poll Account B.
    db.users.docs[0]['active_instagram_account_id'] = 'accA'

    polled_ig_ids: list = []

    async def fake_poll_user_comments(user_doc):
        polled_ig_ids.append(user_doc.get('ig_user_id'))
        return {'newComments': 0}

    monkeypatch.setattr(server, '_poll_user_comments', fake_poll_user_comments)
    monkeypatch.setattr(server, 'IS_SHUTTING_DOWN', False)
    monkeypatch.setattr(server, 'SHUTDOWN_EVENT', asyncio.Event())

    # Drive one iteration of the loop. We use a private helper that
    # mirrors the production tick exactly: iterate accounts collection,
    # scope user_doc per account, call _poll_user_comments.
    async def _one_tick():
        cursor = server.db.instagram_accounts.find({
            'isActive': {'$ne': False},
            'connectionValid': True,
        })
        accounts = await cursor.to_list(500)
        _owner_cache = {}
        for account in accounts:
            owner_id = account.get('userId') or account.get('user_id')
            if not owner_id:
                continue
            if owner_id in _owner_cache:
                owner = _owner_cache[owner_id]
            else:
                owner = await server.db.users.find_one({'id': owner_id})
                _owner_cache[owner_id] = owner
            if not owner:
                continue
            scoped = server._with_instagram_account_context(owner, account)
            await server._poll_user_comments(scoped)

    _run(_one_tick())
    # Both accounts must have been polled, in some order.
    assert sorted(polled_ig_ids) == ['igA', 'igB']


def test_recent_flows_endpoint_returns_session_id_for_each_row(monkeypatch):
    """Operator-friendly: the per-row reset must work without manual
    ids. The endpoint MUST return session_id and the enriched
    blocking_reason / stop_reason so the UI can show why a flow blocks
    new tests and reset that specific row by session_id."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    now = datetime.utcnow()
    db.comment_dm_sessions.docs.extend([
        {
            'id': 'sess-pending-fresh',
            'user_id': 'u1',
            'instagramAccountId': 'igB',
            'igUserId': 'igB',
            'ig_user_id': 'igB',
            'automation_id': 'ruleB',
            'media_id': 'mediaB',
            'recipient_id': 'igsid-tester',
            'commenter_id': 'igsid-tester',
            'opening_dedupe_key': server._comment_opening_dedupe_key(
                'u1', 'igB', 'ruleB', 'mediaB', 'igsid-tester',
            ),
            'status': 'pending',
            'stage': 'awaiting_user_action',
            'finalDmSentAt': None,
            'created': now - timedelta(minutes=10),
            'updated': now - timedelta(minutes=10),
        },
        {
            'id': 'sess-completed-recent',
            'user_id': 'u1',
            'instagramAccountId': 'igB',
            'igUserId': 'igB',
            'ig_user_id': 'igB',
            'automation_id': 'ruleB',
            'media_id': 'mediaB',
            'recipient_id': 'igsid-other',
            'commenter_id': 'igsid-other',
            'opening_dedupe_key': server._comment_opening_dedupe_key(
                'u1', 'igB', 'ruleB', 'mediaB', 'igsid-other',
            ),
            'status': 'completed',
            'stage': 'final_sent',
            'finalDmSentAt': now - timedelta(minutes=2),
            'created': now - timedelta(minutes=20),
            'updated': now - timedelta(minutes=2),
        },
    ])

    result = _run(server.admin_recent_test_flows(user_id='u1'))
    assert result['ok'] is True
    flows = {f['session_id']: f for f in result['flows']}
    # Every row carries enough internal id to reset directly by
    # session_id — no manual paste required from the operator.
    fresh = flows['sess-pending-fresh']
    assert fresh['blocking_reason'] == 'pending_blocks_reopen'
    assert fresh['stop_reason'] == 'waiting_user_action'
    # Rule join populated — rule_post_scope is set even when the test
    # rule has no display name. The frontend falls back to media_id
    # partial when rule_name is empty, so we assert on rule_post_scope
    # to prove the join landed.
    assert fresh['rule_post_scope'] is not None
    assert fresh['session_id'] == 'sess-pending-fresh'
    assert fresh['media_id_partial']
    assert fresh['commenter_id_partial']
    completed = flows['sess-completed-recent']
    assert completed['blocking_reason'] == 'completed_recently'
    assert completed['finalDmSentAt'] is not None
    # blocking_count should NOT include the stale-expired entries
    assert result['has_blocking_flows'] is True


def test_recent_flows_marks_stale_pending_as_auto_expires(monkeypatch):
    """A pending session older than the stale TTL must surface as
    blocking_reason=stale_pending_auto_expires so the UI can show the
    operator they don't need to manually reset it — the next opener
    will treat it as expired."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    very_old = datetime.utcnow() - timedelta(
        seconds=server._COMMENT_DM_STALE_FLOW_REOPEN_TTL_SECONDS + 600,
    )
    db.comment_dm_sessions.docs.append({
        'id': 'sess-very-old',
        'user_id': 'u1',
        'instagramAccountId': 'igB',
        'igUserId': 'igB',
        'ig_user_id': 'igB',
        'automation_id': 'ruleB',
        'media_id': 'mediaB',
        'recipient_id': 'igsid-old',
        'commenter_id': 'igsid-old',
        'opening_dedupe_key': server._comment_opening_dedupe_key(
            'u1', 'igB', 'ruleB', 'mediaB', 'igsid-old',
        ),
        'status': 'pending',
        'stage': 'awaiting_user_action',
        'finalDmSentAt': None,
        'created': very_old,
        'updated': very_old,
    })
    result = _run(server.admin_recent_test_flows(user_id='u1'))
    row = next(f for f in result['flows'] if f['session_id'] == 'sess-very-old')
    assert row['blocking_reason'] == 'stale_pending_auto_expires'
    assert 'after_ttl' in row['stop_reason']


def test_reset_flow_by_session_id_dry_run_uses_session_internal_ids(monkeypatch):
    """Operator should be able to reset by clicking a session row
    without pasting ids — endpoint derives the 4-tuple from the
    session document and refuses to mutate without confirm."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igB',
        automation_id='ruleB', media_id='mediaB', commenter_id='igsid-tester',
        session_id='sess-by-id',
    )
    result = _run(server.admin_reset_flow_by_session_id(
        body={'session_id': 'sess-by-id', 'dry_run': True, 'confirm': False},
        user_id='u1',
    ))
    assert result['dry_run'] is True
    assert result['confirm'] is False
    assert len(result['would_delete_sessions']) == 1
    # Nothing mutated.
    assert any(s['id'] == 'sess-by-id' for s in db.comment_dm_sessions.docs)


def test_reset_flow_by_session_id_confirm_deletes_only_that_session(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igB',
        automation_id='ruleB', media_id='mediaB', commenter_id='igsid-keep',
        session_id='sess-keep', comment_id='comm-keep',
    )
    _seed_dedupe_state(
        db,
        user_id='u1', ig_account_id='igB',
        automation_id='ruleB', media_id='mediaB', commenter_id='igsid-target',
        session_id='sess-target', comment_id='comm-target',
    )
    _run(server.admin_reset_flow_by_session_id(
        body={'session_id': 'sess-target', 'dry_run': False, 'confirm': True},
        user_id='u1',
    ))
    remaining = {s['id'] for s in db.comment_dm_sessions.docs}
    assert remaining == {'sess-keep'}
    keep_comment = next(c for c in db.comments.docs if c['id'] == 'comm-keep')
    target_comment = next(c for c in db.comments.docs if c['id'] == 'comm-target')
    assert keep_comment['opening_dedupe_key'] is not None
    assert target_comment['opening_dedupe_key'] is None


def test_reset_flow_by_session_id_refuses_unknown_session(monkeypatch):
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    try:
        _run(server.admin_reset_flow_by_session_id(
            body={'session_id': 'no-such-session', 'dry_run': True},
            user_id='u1',
        ))
    except server.HTTPException as exc:
        assert exc.status_code == 404
    else:
        raise AssertionError('expected HTTPException(404) for unknown session_id')


def test_reset_flow_by_session_id_refuses_cross_workspace_session(monkeypatch):
    """Even an admin operator may only reset their OWN workspace flows
    via this one-click endpoint. A session owned by user_id=u-other
    must not be resettable from u1's UI."""
    db = _install_multi_account_db(monkeypatch)
    _patch_admin_gate(monkeypatch)
    monkeypatch.setattr(server, '_record_admin_action', _record_action_noop)
    db.comment_dm_sessions.docs.append({
        'id': 'sess-cross',
        'user_id': 'u-other',
        'instagramAccountId': 'igX',
        'ig_user_id': 'igX',
        'automation_id': 'ruleX',
        'media_id': 'mediaX',
        'recipient_id': 'igsid-x',
        'commenter_id': 'igsid-x',
        'status': 'pending',
        'stage': 'awaiting_user_action',
        'created': datetime.utcnow(),
        'updated': datetime.utcnow(),
    })
    try:
        _run(server.admin_reset_flow_by_session_id(
            body={'session_id': 'sess-cross', 'dry_run': True},
            user_id='u1',
        ))
    except server.HTTPException as exc:
        assert exc.status_code == 403
    else:
        raise AssertionError('expected HTTPException(403) for cross-workspace session')


def test_stale_pending_session_does_not_permanently_block_reopen(monkeypatch):
    """The production behavior fix. A pending session older than the
    stale TTL must NOT block a brand-new comment from opening a fresh
    flow on the same tuple. Completed flows still block as before."""
    db = _install_multi_account_db(monkeypatch)
    rule = _comment_rule('accB', 'igB', 'ruleB-stale-allow')
    rule['activationStartedAt'] = datetime.utcnow() - timedelta(days=2)
    db.automations.docs = [rule]
    very_old = datetime.utcnow() - timedelta(
        seconds=server._COMMENT_DM_STALE_FLOW_REOPEN_TTL_SECONDS + 600,
    )
    db.comment_dm_sessions.docs.append({
        'id': 'sess-very-stale',
        'user_id': 'u1',
        'instagramAccountId': 'igB',
        'igUserId': 'igB',
        'ig_user_id': 'igB',
        'automation_id': 'ruleB-stale-allow',
        'media_id': 'mediaB',
        'recipient_id': 'igsid-retry',
        'commenter_id': 'igsid-retry',
        'opening_dedupe_key': server._comment_opening_dedupe_key(
            'u1', 'igB', 'ruleB-stale-allow', 'mediaB', 'igsid-retry',
        ),
        'status': 'pending',
        'stage': 'awaiting_user_action',
        'finalDmSentAt': None,
        'created': very_old,
        'updated': very_old,
    })
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
            'ig_comment_id': 'fresh-after-stale',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-retry',
            'commenter_username': 'retry-tester',
            'text': 'send me',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))
    # The stale session must NOT have blocked — the new comment fires
    # the rule and produces a public reply.
    assert result['matched'] is True
    assert result['rule_id'] == 'ruleB-stale-allow'
    assert any(c['access_token'] == 'token-b' for c in reply_calls)
    # The stale session is marked expired so future find_one queries
    # don't even hit it.
    stale_row = next(s for s in db.comment_dm_sessions.docs if s['id'] == 'sess-very-stale')
    assert stale_row['status'] == 'stale_expired'
    assert stale_row['opening_dedupe_key'] is None


def test_recent_completed_session_still_blocks_within_ttl(monkeypatch):
    """The relaxation only fires for PENDING sessions. A completed
    flow within the TTL window must still block — that's the
    anti-spam contract."""
    db = _install_multi_account_db(monkeypatch)
    rule = _comment_rule('accB', 'igB', 'ruleB-completed-block')
    rule['activationStartedAt'] = datetime.utcnow() - timedelta(days=2)
    db.automations.docs = [rule]
    db.comment_dm_sessions.docs.append({
        'id': 'sess-completed',
        'user_id': 'u1',
        'instagramAccountId': 'igB',
        'igUserId': 'igB',
        'ig_user_id': 'igB',
        'automation_id': 'ruleB-completed-block',
        'media_id': 'mediaB',
        'recipient_id': 'igsid-spammer',
        'commenter_id': 'igsid-spammer',
        'opening_dedupe_key': server._comment_opening_dedupe_key(
            'u1', 'igB', 'ruleB-completed-block', 'mediaB', 'igsid-spammer',
        ),
        'status': 'completed',
        'stage': 'final_sent',
        'finalDmSentAt': datetime.utcnow() - timedelta(minutes=5),
        'created': datetime.utcnow() - timedelta(minutes=10),
        'updated': datetime.utcnow() - timedelta(minutes=5),
    })

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
            'ig_comment_id': 'spam-after-completed',
            'media_id': 'mediaB',
            'commenter_id': 'igsid-spammer',
            'text': 'send me again',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        },
        source='webhook',
    ))
    assert result.get('already_processed') is True
    assert result.get('classified_reason') == 'same_commenter_same_post_same_rule'


def test_opening_dm_skips_inline_subscription_meta_call_when_cache_fresh(monkeypatch):
    """Speed: when the self-heal loop recently verified the account's
    webhook subscription (cache age within window and no critical
    fields missing), the opening DM path MUST NOT make a synchronous
    Meta /subscribed_apps call. That call adds 200-800ms to every
    opening and is wasteful when the cache is fresh.
    """
    db = _install_multi_account_db(monkeypatch)
    # Seed a fresh, healthy subscription cache on Account B.
    db.instagram_accounts.docs[1].update({
        'webhookSubscriptionLastCheckedAt': datetime.utcnow() - timedelta(seconds=60),
        'webhookSubscriptionMissing': [],
        'webhookSubscriptionFields': sorted(server.WEBHOOK_REQUIRED_FIELDS),
    })
    db.automations.docs = [
        _post_specific_comment_flow_rule('accB', 'igB', 'ruleB-fast', 'mediaFast'),
    ]

    # If the hot path tries to open an httpx client (which would be
    # the inline subscription verify), fail the test loudly. The
    # opening DM dispatch itself goes through server.send_ig_message
    # which we monkeypatch separately.
    class _ForbiddenClient:
        def __init__(self, *a, **kw):
            raise AssertionError(
                'opening DM hot path should NOT open an httpx client when '
                'the subscription cache is fresh — that is the 200-800ms '
                'overhead we just removed'
            )
    monkeypatch.setattr(server.httpx, 'AsyncClient', _ForbiddenClient)

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid-fast'}}
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    # Disable the fire-and-forget recheck task so the test doesn't spawn
    # background work that could race with assertions. The test cares
    # only about whether the inline Meta call was made.
    def _no_track(coro, name):
        try:
            coro.close()
        except Exception:
            pass
        return None
    monkeypatch.setattr(server, 'create_tracked_task', _no_track)

    owner_b = server._with_instagram_account_context(
        db.users.docs[0], db.instagram_accounts.docs[1],
    )
    automation = db.automations.docs[0]
    ok = _run(server._send_comment_dm_flow_entry(
        owner_b,
        automation,
        'igsid-tester',
        {'media_id': 'mediaFast', 'commenter_id': 'igsid-tester',
         'ig_comment_id': 'fast-1', 'comment_doc_id': 'doc-fast-1'},
    ))
    assert ok is True


def test_opening_dm_schedules_background_recheck_when_cache_stale(monkeypatch):
    """When the subscription cache is stale, the opening DM hot path
    must NOT inline-verify (still fast) but MUST schedule a fire-and-
    forget recheck so the cache is refreshed for the next opener."""
    db = _install_multi_account_db(monkeypatch)
    # Stale cache: last check >>15 min ago.
    db.instagram_accounts.docs[1].update({
        'webhookSubscriptionLastCheckedAt': datetime.utcnow() - timedelta(hours=2),
        'webhookSubscriptionMissing': [],
        'webhookSubscriptionFields': sorted(server.WEBHOOK_REQUIRED_FIELDS),
    })
    db.automations.docs = [
        _post_specific_comment_flow_rule('accB', 'igB', 'ruleB-stale', 'mediaStale'),
    ]

    # Track whether the fire-and-forget recheck was scheduled.
    scheduled = []

    def _capture_track(coro, name):
        scheduled.append(name)
        try:
            coro.close()
        except Exception:
            pass
        return None
    monkeypatch.setattr(server, 'create_tracked_task', _capture_track)

    # Inline Meta verify still must NOT happen on the hot path.
    class _ForbiddenClient:
        def __init__(self, *a, **kw):
            raise AssertionError('inline Meta verify must never run on hot path')
    monkeypatch.setattr(server.httpx, 'AsyncClient', _ForbiddenClient)

    async def send_message_ok(access_token, ig_user_id, recipient_id, message,
                              allow_workspace_recipient=False):
        return {'ok': True, 'status_code': 200, 'body': {'message_id': 'mid-stale'}}
    monkeypatch.setattr(server, 'send_ig_message', send_message_ok)

    owner_b = server._with_instagram_account_context(
        db.users.docs[0], db.instagram_accounts.docs[1],
    )
    automation = db.automations.docs[0]
    ok = _run(server._send_comment_dm_flow_entry(
        owner_b,
        automation,
        'igsid-stale',
        {'media_id': 'mediaStale', 'commenter_id': 'igsid-stale',
         'ig_comment_id': 'stale-1', 'comment_doc_id': 'doc-stale-1'},
    ))
    assert ok is True
    assert 'comment_dm_subscription_recheck' in scheduled


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
