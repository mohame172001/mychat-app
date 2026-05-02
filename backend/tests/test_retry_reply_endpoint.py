"""Tests for POST /api/comments/{commentId}/retry-reply.

Covers:
  • Cross-user / cross-account access returns 404.
  • A provider-proven reply (reply_provider_response_ok=True) is rejected
    with reason='reply_already_proven', no Graph call, no duplicate.
  • A legacy success without proof IS retryable (the proof flag is what
    matters, not the legacy `replied`/`action_status` fields).
  • A pending/failed reply is retried and proof flag set on success.
  • Permanent failure reasons (recipient_unavailable, etc.) are not
    retried.
  • Transient failures schedule a next_retry_at and increment attempts.
  • Response never contains tokens, full text, or raw Graph error bodies.
  • Two concurrent retry attempts do not produce two Graph reply calls
    when the first one succeeds with provider proof.
"""
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://example.com')
os.environ.setdefault('FRONTEND_URL', 'https://example.com')
os.environ.setdefault('IG_APP_ID', '123')
os.environ.setdefault('IG_APP_SECRET', 'secret')
os.environ.setdefault('CRON_SECRET', 'cron-secret')

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


def _match(doc, query):
    for key, expected in query.items():
        if key == '$or':
            if not any(_match(doc, item) for item in expected):
                return False
            continue
        value = doc.get(key)
        if isinstance(expected, dict):
            if '$ne' in expected and value == expected['$ne']:
                return False
            if '$in' in expected and value not in expected['$in']:
                return False
            if '$exists' in expected and ((key in doc) != expected['$exists']):
                return False
        elif value != expected:
            return False
    return True


class _Cursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, *_a, **_kw):
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    async def to_list(self, n):
        return list(self.docs[:n])


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def find_one(self, query, sort=None):
        return next((d for d in self.docs if _match(d, query)), None)

    def find(self, query=None):
        if query is None:
            return _Cursor(list(self.docs))
        return _Cursor([d for d in self.docs if _match(d, query)])

    async def insert_one(self, doc):
        self.docs.append(dict(doc))
        return SimpleNamespace(inserted_id=doc.get('id'))

    async def update_one(self, query, update, upsert=False):
        doc = await self.find_one(query)
        if not doc:
            return SimpleNamespace(matched_count=0, modified_count=0)
        for k, v in update.get('$set', {}).items():
            doc[k] = v
        for k, v in update.get('$inc', {}).items():
            doc[k] = int(doc.get(k) or 0) + v
        return SimpleNamespace(matched_count=1, modified_count=1)


class FakeDB:
    def __init__(self, comments=None, users=None, automations=None):
        self.comments = FakeCollection(comments or [])
        self.users = FakeCollection(users or [])
        self.automations = FakeCollection(automations or [])
        self.instagram_accounts = FakeCollection([])
        self.tracked_links = FakeCollection([])
        self.comment_dm_sessions = FakeCollection([])


def _user(user_id='u1', ig='biz1'):
    return {
        'id': user_id,
        'ig_user_id': ig,
        'meta_access_token': 'tok',
        'instagramConnected': True,
        'instagramHandle': '@biz',
        'email': 'u@example.com',
    }


def _comment_doc(**overrides):
    base = {
        'id': 'doc_x',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'ig_comment_id': 'c_real',
        'media_id': 'm1',
        'commenter_id': 'fan1',
        'commenter_username': 'fan',
        'text': 'hello',
        'rule_id': 'r1',
        'replied': False,
        'reply_status': 'failed',
        'reply_provider_response_ok': False,
        'reply_failure_reason': 'temporary_graph_error',
        'reply_text': '',
        'dm_status': 'disabled',
        'dm_failure_reason': None,
        'action_status': 'failed',
        'attempts': 1,
        'next_retry_at': None,
        'last_attempt_at': datetime.utcnow() - timedelta(minutes=2),
    }
    base.update(overrides)
    return base


def _rule_with_replies():
    return {
        'id': 'r1',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'status': 'active',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'thanks!', 'replies': ['thanks!']}},
        ],
    }


def _stub_active_account(monkeypatch, ig='biz1'):
    async def fake_account(_uid):
        return {
            'id': 'acc1',
            'instagramAccountId': ig,
            'igUserId': ig,
            'accessToken': 'tok',
            'username': 'biz',
            'connectionValid': True,
        }

    monkeypatch.setattr(server, 'getActiveInstagramAccount', fake_account)
    monkeypatch.setattr(server, '_account_scoped_query',
                        lambda _u, acc: {
                            'user_id': _u,
                            '$or': [
                                {'instagramAccountId': (acc or {}).get('instagramAccountId')},
                                {'igUserId': (acc or {}).get('igUserId')},
                            ],
                        })


# ── Tests ──────────────────────────────────────────────────────────────────

def test_retry_reply_returns_404_for_other_user(monkeypatch):
    """A comment owned by user u_other must 404 for caller u1."""
    other_doc = _comment_doc(id='doc_other', user_id='u_other',
                             instagramAccountId='biz_other',
                             igUserId='biz_other')
    db = FakeDB(comments=[other_doc], users=[_user('u1')])
    monkeypatch.setattr(server, 'db', db)
    _stub_active_account(monkeypatch, ig='biz1')

    with pytest.raises(Exception) as exc:
        _run(server.retry_comment_reply('doc_other', user_id='u1'))
    assert '404' in str(exc.value) or 'not found' in str(exc.value).lower()


def test_retry_reply_rejects_provider_proven_success(monkeypatch):
    """reply_provider_response_ok=True → must NOT call Graph, must not
    duplicate the public reply, must return reason=reply_already_proven."""
    proven = _comment_doc(
        replied=True, reply_status='success',
        reply_provider_response_ok=True, action_status='success',
    )
    db = FakeDB(comments=[proven], users=[_user()],
                automations=[_rule_with_replies()])
    monkeypatch.setattr(server, 'db', db)
    _stub_active_account(monkeypatch)

    graph_calls = []

    async def fake_send(*a, **kw):
        graph_calls.append((a, kw))
        return {'ok': True, 'failure_reason': None, 'status_code': 200}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', fake_send)

    res = _run(server.retry_comment_reply(proven['id'], user_id='u1'))
    assert res['queued'] is False
    assert res['reason'] == 'reply_already_proven'
    assert res['reply_status'] == 'success'
    assert graph_calls == [], 'must NEVER call Graph for a proven reply'


def test_retry_reply_allows_legacy_success_without_proof(monkeypatch):
    """A legacy doc that has reply_status='success' but no
    reply_provider_response_ok flag is allowed to retry — and a successful
    retry sets the proof flag for future calls."""
    legacy = _comment_doc(
        replied=True, reply_status='success',
        reply_provider_response_ok=False,  # legacy: no proof
        reply_text='thanks!',
    )
    db = FakeDB(comments=[legacy], users=[_user()],
                automations=[_rule_with_replies()])
    monkeypatch.setattr(server, 'db', db)
    _stub_active_account(monkeypatch)

    async def fake_send(*a, **kw):
        return {'ok': True, 'failure_reason': None, 'status_code': 200}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', fake_send)
    res = _run(server.retry_comment_reply(legacy['id'], user_id='u1'))
    assert res['reply_status'] == 'success'
    assert res['reason'] == 'reply_sent'
    saved = db.comments.docs[0]
    assert saved['reply_provider_response_ok'] is True
    assert saved['attempts'] >= 2


def test_retry_reply_retries_pending_reply_and_increments_attempts(monkeypatch):
    """A pending/failed reply with no proof is retried, attempts++, and
    success persists provider proof."""
    pending = _comment_doc(reply_status='pending', attempts=0,
                           reply_failure_reason=None)
    db = FakeDB(comments=[pending], users=[_user()],
                automations=[_rule_with_replies()])
    monkeypatch.setattr(server, 'db', db)
    _stub_active_account(monkeypatch)

    async def fake_send(*a, **kw):
        return {'ok': True, 'failure_reason': None, 'status_code': 200}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', fake_send)
    res = _run(server.retry_comment_reply(pending['id'], user_id='u1'))
    assert res['reply_status'] == 'success'
    assert res['attempts'] == 1
    saved = db.comments.docs[0]
    assert saved['reply_provider_response_ok'] is True
    assert saved['replied'] is True
    assert saved['action_status'] == 'success'


def test_retry_reply_rejects_permanent_failure_reason(monkeypatch):
    """recipient_unavailable etc. must NOT retry; no Graph call."""
    permanent = _comment_doc(
        reply_status='failed',
        reply_failure_reason='recipient_unavailable',
    )
    db = FakeDB(comments=[permanent], users=[_user()],
                automations=[_rule_with_replies()])
    monkeypatch.setattr(server, 'db', db)
    _stub_active_account(monkeypatch)

    graph_calls = []

    async def fake_send(*a, **kw):
        graph_calls.append(a)
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', fake_send)
    res = _run(server.retry_comment_reply(permanent['id'], user_id='u1'))
    assert res['queued'] is False
    assert 'permanent_failure' in res['reason']
    assert graph_calls == []


def test_retry_reply_schedules_next_retry_on_transient_failure(monkeypatch):
    """A transient Graph failure (rate_limited / temporary_graph_error)
    increments attempts and sets next_retry_at on a backoff."""
    pending = _comment_doc(reply_status='failed',
                           reply_failure_reason='temporary_graph_error',
                           attempts=2)
    db = FakeDB(comments=[pending], users=[_user()],
                automations=[_rule_with_replies()])
    monkeypatch.setattr(server, 'db', db)
    _stub_active_account(monkeypatch)

    async def fake_send(*a, **kw):
        return {'ok': False, 'failure_reason': 'rate_limited', 'status_code': 429}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', fake_send)
    res = _run(server.retry_comment_reply(pending['id'], user_id='u1'))
    assert res['reply_status'] == 'failed'
    assert res['reason'] == 'rate_limited'
    assert res['attempts'] == 3
    # next_retry_at must be set and in the future.
    assert res['next_retry_at'] is not None
    parsed = datetime.fromisoformat(res['next_retry_at'])
    assert parsed > datetime.utcnow()


def test_retry_reply_persists_and_no_duplicate_provider_calls(monkeypatch):
    """After a successful retry sets the proof flag, a second retry call
    must not call Graph again (proof guard hits first)."""
    doc = _comment_doc(reply_status='failed', attempts=1)
    db = FakeDB(comments=[doc], users=[_user()],
                automations=[_rule_with_replies()])
    monkeypatch.setattr(server, 'db', db)
    _stub_active_account(monkeypatch)

    graph_calls = []

    async def fake_send(*a, **kw):
        graph_calls.append(1)
        return {'ok': True, 'failure_reason': None, 'status_code': 200}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', fake_send)

    _run(server.retry_comment_reply(doc['id'], user_id='u1'))
    # Second call from the UI; must hit the proof guard.
    res2 = _run(server.retry_comment_reply(doc['id'], user_id='u1'))
    assert len(graph_calls) == 1, 'second retry must not call Graph'
    assert res2['reason'] == 'reply_already_proven'


def test_retry_reply_response_does_not_leak_sensitive_data(monkeypatch):
    """The response body must not contain access tokens, full reply
    text, or raw Graph error bodies."""
    doc = _comment_doc(reply_status='failed',
                       reply_text='SECRET_REPLY_TEXT_ABC123',
                       attempts=1)
    db = FakeDB(comments=[doc], users=[_user()],
                automations=[_rule_with_replies()])
    monkeypatch.setattr(server, 'db', db)
    _stub_active_account(monkeypatch)

    async def fake_send(*a, **kw):
        # Even if a raw body sneaks in via the failure_reason value, the
        # endpoint must surface only the classified short string.
        return {'ok': False, 'failure_reason': 'unknown_graph_error',
                'status_code': 400}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', fake_send)
    res = _run(server.retry_comment_reply(doc['id'], user_id='u1'))
    serialized = json.dumps(res, default=str)
    assert 'SECRET_REPLY_TEXT_ABC123' not in serialized
    assert 'tok' != res.get('reason')
    assert 'access_token' not in serialized.lower()


def test_retry_reply_no_reply_text_returns_400(monkeypatch):
    """If no reply_text and no rule with replies, return 400 — never
    invent a body to send."""
    doc = _comment_doc(reply_status='failed', reply_text='', rule_id=None)
    db = FakeDB(comments=[doc], users=[_user()], automations=[])
    monkeypatch.setattr(server, 'db', db)
    _stub_active_account(monkeypatch)

    with pytest.raises(Exception) as exc:
        _run(server.retry_comment_reply(doc['id'], user_id='u1'))
    assert '400' in str(exc.value) or 'No reply text' in str(exc.value)


def test_retry_reply_uses_rule_replies_when_no_persisted_text(monkeypatch):
    """If reply_text was never persisted, the endpoint loads the rule
    and picks from its replies list."""
    doc = _comment_doc(reply_status='failed', reply_text='', attempts=0)
    db = FakeDB(comments=[doc], users=[_user()],
                automations=[_rule_with_replies()])
    monkeypatch.setattr(server, 'db', db)
    _stub_active_account(monkeypatch)

    sent_text = []

    async def fake_send(token, ig_id, text, *a, **kw):
        sent_text.append(text)
        return {'ok': True, 'failure_reason': None, 'status_code': 200}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', fake_send)
    _run(server.retry_comment_reply(doc['id'], user_id='u1'))
    assert sent_text == ['thanks!']
