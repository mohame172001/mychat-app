"""Tests for immediate webhook comment processing without artificial delay.

Verifies:
1. Webhook comment missing timestamp is processed from webhook path (not poller).
2. Webhook path calls comment reply without waiting for poller.
3. Poller skips already-replied webhook comment (dedup).
4. No artificial sleep on initial webhook comment processing.
5. A single isolated comment is not delayed excessively.
6. No duplicate replies.
"""
import asyncio
import os
import sys
import time
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


def _run(coro):
    return asyncio.run(coro)


# ── Minimal fakes ────────────────────────────────────────────────────────────

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

    async def count_documents(self, query):
        return len([d for d in self.docs if _match(d, query)])


class FakeDB:
    def __init__(self, automations=None, comments=None):
        self.automations = FakeCollection(automations or [])
        self.comments = FakeCollection(comments or [])
        self.users = FakeCollection([])
        self.tracked_links = FakeCollection([])
        self.comment_dm_sessions = FakeCollection([])


def _user():
    return {'id': 'u1', 'ig_user_id': 'biz1', 'meta_access_token': 'tok',
            'instagramConnected': True, 'instagramHandle': '@biz'}


def _rule(trigger='keyword:any'):
    return {
        'id': 'r1',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'status': 'active',
        'trigger': trigger,
        'match': 'any',
        'mode': 'reply_only',
        'activationStartedAt': datetime.utcnow() - timedelta(hours=1),
        'createdAt': datetime.utcnow() - timedelta(hours=1),
        'processExistingComments': False,
        'process_existing_unreplied_comments': False,
        'comment_reply': 'thanks!',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'thanks!', 'replies': ['thanks!']}},
        ],
        'edges': [{'id': 'e1', 'source': 'n_trigger', 'target': 'n_reply'}],
    }


def _stub_run(monkeypatch):
    ran = {'count': 0}

    async def fake_run(user_doc, automation, *a, **kw):
        ran['count'] += 1

    monkeypatch.setattr(server, '_run_and_record_action', fake_run)
    monkeypatch.setattr(server, '_account_scoped_query',
                        lambda _u, _ig: {'user_id': 'u1'})
    monkeypatch.setattr(server, 'matchesAutomationRule',
                        lambda auto, text, ctx: {'matches': True})
    monkeypatch.setattr(server, '_fetch_latest_media_id',
                        lambda *a, **k: _async_none())
    monkeypatch.setattr(server, 'ws_manager',
                        SimpleNamespace(send=lambda *a, **k: _async_none()))
    return ran


async def _async_none(*_a, **_kw):
    return None


# ── Tests ────────────────────────────────────────────────────────────────────

def test_webhook_path_processes_comment_immediately(monkeypatch):
    """Webhook comment (no timestamp) is processed from webhook path, not poller."""
    db = FakeDB(automations=[_rule()])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    entry_time_iso = (datetime.utcnow() - timedelta(seconds=5)).strftime('%Y-%m-%dT%H:%M:%S+0000')
    comment = {
        'ig_comment_id': 'c_wh_imm',
        'commenter_id': 'fan1',
        'text': '🔥👏',
        'media_id': 'm1',
        'timestamp': None,
        'entry_time': entry_time_iso,
    }

    res = _run(server._handle_new_comment(_user(), comment, source='webhook'))

    assert res.get('matched') is True, f'webhook must match; got {res}'
    assert ran['count'] == 1
    saved = db.comments.docs[0]
    assert saved.get('source') == 'webhook'
    assert saved.get('skip_reason') != 'missing_comment_timestamp'


def test_webhook_reply_called_without_polling(monkeypatch):
    """execute_flow calls reply_to_ig_comment on webhook path without needing poller."""
    reply_calls = []

    async def fake_reply(token, ig_comment_id, text):
        reply_calls.append(ig_comment_id)
        return True

    monkeypatch.setattr(server, 'reply_to_ig_comment', fake_reply)

    db = FakeDB(automations=[_rule()])
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, '_account_scoped_query',
                        lambda _u, _ig: {'user_id': 'u1'})
    monkeypatch.setattr(server, 'matchesAutomationRule',
                        lambda auto, text, ctx: {'matches': True})
    monkeypatch.setattr(server, 'ws_manager',
                        SimpleNamespace(send=lambda *a, **k: _async_none()))

    comment = {
        'ig_comment_id': 'c_reply_wh',
        'commenter_id': 'fan2',
        'text': 'nice',
        'media_id': 'm1',
        'timestamp': None,
        'entry_time': (datetime.utcnow() - timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S+0000'),
    }

    res = _run(server._handle_new_comment(_user(), comment, source='webhook'))

    assert res.get('matched') is True
    assert 'c_reply_wh' in reply_calls, 'reply_to_ig_comment must be called from webhook path'


def test_poller_skips_already_replied_webhook_comment(monkeypatch):
    """After webhook replies, poller must see already_processed and not reply again."""
    rule = _rule()
    existing = {
        'id': 'cdoc1',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'ig_comment_id': 'c_dup_check',
        'replied': True,
        'reply_status': 'success',
        'reply_provider_response_ok': True,
        'reply_provider_comment_id': 'reply_dup_check',
        'replied_at': datetime.utcnow() - timedelta(minutes=1),
        'action_status': 'success',
        'created': datetime.utcnow() - timedelta(minutes=1),
    }
    db = FakeDB(automations=[rule], comments=[existing])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    ts_iso = (datetime.utcnow() - timedelta(minutes=1)).strftime('%Y-%m-%dT%H:%M:%S+0000')
    comment = {
        'ig_comment_id': 'c_dup_check',
        'commenter_id': 'fan3',
        'text': 'hi',
        'media_id': 'm1',
        'timestamp': ts_iso,
    }

    res = _run(server._handle_new_comment(_user(), comment, source='polling'))

    assert res.get('already_processed') is True, f'poller must see dedup; got {res}'
    assert ran['count'] == 0, 'must not reply again'


def test_no_artificial_delay_on_webhook_comment(monkeypatch):
    """Processing a webhook comment (rule has NO delay node) finishes in < 1 second."""
    db = FakeDB(automations=[_rule()])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    comment = {
        'ig_comment_id': 'c_speed',
        'commenter_id': 'fan4',
        'text': 'fast',
        'media_id': 'm1',
        'timestamp': None,
        'entry_time': (datetime.utcnow() - timedelta(seconds=2)).strftime('%Y-%m-%dT%H:%M:%S+0000'),
    }

    start = time.monotonic()
    _run(server._handle_new_comment(_user(), comment, source='webhook'))
    elapsed = time.monotonic() - start

    assert elapsed < 1.0, f'processing took {elapsed:.2f}s — expected < 1s without network'


def test_no_duplicate_reply_on_concurrent_webhook_and_poll(monkeypatch):
    """Race: webhook inserts doc then poll sees it and skips."""
    rule = _rule()
    db = FakeDB(automations=[rule])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    ts_iso = (datetime.utcnow() - timedelta(seconds=10)).strftime('%Y-%m-%dT%H:%M:%S+0000')
    comment_wh = {
        'ig_comment_id': 'c_race', 'commenter_id': 'fan7', 'text': 'race',
        'media_id': 'm1', 'timestamp': ts_iso, 'entry_time': ts_iso,
    }
    comment_poll = {
        'ig_comment_id': 'c_race', 'commenter_id': 'fan7', 'text': 'race',
        'media_id': 'm1', 'timestamp': ts_iso,
    }

    async def _run_both():
        r1 = await server._handle_new_comment(_user(), comment_wh, source='webhook')
        # Simulate the webhook reply succeeding
        if db.comments.docs:
            db.comments.docs[0]['replied'] = True
            db.comments.docs[0]['reply_status'] = 'success'
            db.comments.docs[0]['reply_provider_response_ok'] = True
            db.comments.docs[0]['reply_provider_comment_id'] = 'reply_race'
            db.comments.docs[0]['replied_at'] = datetime.utcnow()
            db.comments.docs[0]['action_status'] = 'success'
        r2 = await server._handle_new_comment(_user(), comment_poll, source='polling')
        return r1, r2

    r1, r2 = _run(_run_both())

    assert r1.get('matched') is True, f'webhook should match; got {r1}'
    assert r2.get('already_processed') is True, f'poll should see dedup; got {r2}'
    assert ran['count'] == 1, 'reply must fire exactly once'
