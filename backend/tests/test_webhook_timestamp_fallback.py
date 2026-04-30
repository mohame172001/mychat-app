"""Tests for webhook comment timestamp-fallback behaviour.

Root cause: webhook payloads from Meta sometimes carry no comment timestamp.
The old code called apply_activation_cutoff() which hit the `not effective_ts`
branch and returned 'missing_comment_timestamp', silently skipping the comment
until the poller picked it up (~60 s later).

Fix: when source='webhook' and the payload has no timestamp, we use entry.time
(the Meta dispatch epoch) or datetime.utcnow() as the effective_ts so the
activation cutoff comparison can proceed.  Polling is unchanged — a polling
comment with no timestamp is still skipped.
"""
import asyncio
import logging
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


# ── Minimal helpers ──────────────────────────────────────────────────────────

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


def _rule_active_now():
    """Rule activated 1 hour ago."""
    return {
        'id': 'r1',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'status': 'active',
        'trigger': 'keyword:any',
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
    """Stub _run_and_record_action and return a call-counter."""
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
    return ran


async def _async_none(*_a, **_kw):
    return None


# ── Tests ────────────────────────────────────────────────────────────────────

def test_webhook_missing_timestamp_uses_entry_time(monkeypatch, caplog):
    """Webhook comment with no timestamp uses entry_time and is processed."""
    db = FakeDB(automations=[_rule_active_now()])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)
    caplog.set_level(logging.INFO, logger=server.logger.name)

    # entry_time is 5 minutes ago — clearly after rule activation (1 h ago)
    entry_time = datetime.utcnow() - timedelta(minutes=5)
    entry_time_iso = entry_time.strftime('%Y-%m-%dT%H:%M:%S+0000')

    comment = {
        'ig_comment_id': 'c_wh1',
        'commenter_id': 'fan1',
        'text': '🔥👏',
        'media_id': 'm1',
        'timestamp': None,        # <-- no payload timestamp
        'entry_time': entry_time_iso,
    }

    res = _run(server._handle_new_comment(_user(), comment, source='webhook'))

    assert res.get('matched') is True, f'should match; got {res}'
    assert ran['count'] == 1, 'rule action should fire'
    saved = db.comments.docs[0]
    assert saved.get('skip_reason') != 'missing_comment_timestamp', \
        'must not be skipped for missing timestamp on webhook path'
    assert saved.get('timestamp_source') == 'entry_time'
    assert 'webhook_comment_missing_timestamp_using_entry_time' in caplog.text
    assert 'webhook_comment_effective_timestamp' in caplog.text
    assert 'rule_matched source=webhook' in caplog.text


def test_webhook_missing_timestamp_no_entry_time_uses_now(monkeypatch):
    """Webhook with no timestamp AND no entry_time falls back to now."""
    db = FakeDB(automations=[_rule_active_now()])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    comment = {
        'ig_comment_id': 'c_wh2',
        'commenter_id': 'fan2',
        'text': 'hi!',
        'media_id': 'm1',
        'timestamp': None,
        'entry_time': None,
    }

    res = _run(server._handle_new_comment(_user(), comment, source='webhook'))

    assert res.get('matched') is True, f'should match using now; got {res}'
    assert ran['count'] == 1


def test_webhook_emoji_only_matches_any_comment_rule(monkeypatch):
    """Emoji-only text must match a keyword:any rule."""
    db = FakeDB(automations=[_rule_active_now()])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    # entry_time present, no payload timestamp
    entry_time_iso = (datetime.utcnow() - timedelta(seconds=30)).strftime('%Y-%m-%dT%H:%M:%S+0000')
    comment = {
        'ig_comment_id': 'c_emoji',
        'commenter_id': 'fan3',
        'text': '🔥👏',
        'media_id': 'm1',
        'timestamp': None,
        'entry_time': entry_time_iso,
    }

    res = _run(server._handle_new_comment(_user(), comment, source='webhook'))

    assert res.get('matched') is True, f'emoji comment should match; got {res}'
    assert ran['count'] == 1


def test_polling_missing_timestamp_still_skipped(monkeypatch):
    """Polling path with no timestamp is still skipped (strict behaviour)."""
    db = FakeDB(automations=[_rule_active_now()])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    comment = {
        'ig_comment_id': 'c_poll_no_ts',
        'commenter_id': 'fan4',
        'text': 'nice',
        'media_id': 'm1',
        'timestamp': None,
        'entry_time': None,
    }

    res = _run(server._handle_new_comment(_user(), comment, source='polling'))

    assert res.get('matched') is False, f'polling missing ts should be skipped; got {res}'
    assert ran['count'] == 0
    saved = db.comments.docs[0] if db.comments.docs else {}
    assert saved.get('skip_reason') == 'missing_comment_timestamp'


def test_polling_dedup_after_webhook_success(monkeypatch):
    """If webhook processed a comment (replied=True), polling must not reply again."""
    rule = _rule_active_now()
    historical_ts = datetime.utcnow() - timedelta(minutes=2)
    existing = {
        'id': 'cdoc1',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'ig_comment_id': 'c_dedup',
        'replied': True,
        'action_status': 'success',
        'created': historical_ts,
    }
    db = FakeDB(automations=[rule], comments=[existing])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    ts_iso = historical_ts.strftime('%Y-%m-%dT%H:%M:%S+0000')
    comment = {
        'ig_comment_id': 'c_dedup',
        'commenter_id': 'fan5',
        'text': 'hey',
        'media_id': 'm1',
        'timestamp': ts_iso,
    }

    res = _run(server._handle_new_comment(_user(), comment, source='polling'))

    assert res.get('already_processed') is True, f'duplicate must short-circuit; got {res}'
    assert ran['count'] == 0, 'must not reply twice'


def test_polling_old_historical_comments_still_skipped(monkeypatch):
    """Polling of a comment created before rule activation is skipped."""
    rule = _rule_active_now()
    db = FakeDB(automations=[rule])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    # Comment is 3 days old — well before rule activation (1 hour ago)
    old_ts = datetime.utcnow() - timedelta(days=3)
    old_ts_iso = old_ts.strftime('%Y-%m-%dT%H:%M:%S+0000')
    comment = {
        'ig_comment_id': 'c_old',
        'commenter_id': 'fan6',
        'text': 'old comment',
        'media_id': 'm1',
        'timestamp': old_ts_iso,
    }

    res = _run(server._handle_new_comment(_user(), comment, source='polling'))

    assert res.get('matched') is False, f'old comment should be skipped; got {res}'
    assert ran['count'] == 0
    saved = db.comments.docs[0] if db.comments.docs else {}
    assert saved.get('skip_reason') == 'historical_before_rule_activation'


def test_execute_flow_logs_webhook_reply_and_dm_timings(monkeypatch, caplog):
    """Webhook comment processing emits immediate-path timing logs for reply and DM."""
    db = FakeDB(comments=[{'id': 'comment_doc'}])
    monkeypatch.setattr(server, 'db', db)

    async def fake_reply(_access_token, _comment_id, _text):
        return True

    async def fake_dm(_access_token, _ig_user_id, _recipient_id, _text):
        return True

    monkeypatch.setattr(server, 'reply_to_ig_comment', fake_reply)
    monkeypatch.setattr(server, 'send_ig_dm', fake_dm)
    caplog.set_level(logging.INFO, logger=server.logger.name)

    automation = {
        'id': 'r_timing',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'thanks!'}},
            {'id': 'n_msg', 'type': 'message',
             'data': {'text': 'check your link'}},
        ],
        'edges': [
            {'id': 'e1', 'source': 'n_trigger', 'target': 'n_reply'},
            {'id': 'e2', 'source': 'n_trigger', 'target': 'n_msg'},
        ],
    }
    user = {'id': 'u1', 'ig_user_id': 'biz1', 'meta_access_token': 'tok'}

    ok = _run(server.execute_flow(
        user,
        automation,
        'fan1',
        'Price',
        comment_context={
            'ig_comment_id': 'c_timing',
            'comment_doc_id': 'comment_doc',
            'source': 'webhook',
            'received_monotonic': time.monotonic(),
        },
    ))

    assert ok is True
    assert 'comment_reply_sent source=webhook' in caplog.text
    assert 'total_webhook_to_reply_ms=' in caplog.text
    assert 'total_webhook_to_dm_ms=' in caplog.text
