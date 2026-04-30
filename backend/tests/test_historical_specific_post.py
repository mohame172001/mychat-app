"""Tests verifying that historical comment processing is always blocked.

Both legacy (processExistingComments) and newer (process_existing_unreplied_comments)
flags are accepted at model level but IGNORED at runtime. No automation should
reply to comments older than activationStartedAt regardless of flag values.

Coverage:
1. Broad rule flag is always ignored.
2. Specific-post rule flag is also always ignored.
3. Already replied comment is not duplicated (dedup layer).
4. Catch-up endpoint uses stored comment docs (not Graph API).
5. Historical docs are still skipped even when catch-up runs.
6. Recently matched comment (after activation) is processed normally.
"""
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


def _run(coro):
    return asyncio.run(coro)


# ── Minimal fakes ─────────────────────────────────────────────────────────────

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
    def __init__(self, automations=None, comments=None, users=None):
        self.automations = FakeCollection(automations or [])
        self.comments = FakeCollection(comments or [])
        self.users = FakeCollection(users or [])
        self.tracked_links = FakeCollection([])
        self.comment_dm_sessions = FakeCollection([])


def _user():
    return {
        'id': 'u1',
        'ig_user_id': 'biz1',
        'meta_access_token': 'tok',
        'instagramConnected': True,
        'instagramHandle': '@biz',
    }


def _broad_rule():
    return {
        'id': 'r_broad',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'status': 'active',
        'trigger': 'comment:any',
        'media_id': '',
        'match': 'any',
        'mode': 'reply_only',
        'activationStartedAt': datetime.utcnow() - timedelta(minutes=10),
        'createdAt': datetime.utcnow() - timedelta(minutes=10),
        'processExistingComments': True,
        'process_existing_unreplied_comments': True,  # ignored — broad
        'comment_reply': 'hi!',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'hi!', 'replies': ['hi!']}},
        ],
        'edges': [{'id': 'e1', 'source': 'n_trigger', 'target': 'n_reply'}],
    }


def _specific_rule(media_id='m111'):
    return {
        'id': 'r_specific',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'status': 'active',
        'trigger': f'comment:{media_id}',
        'media_id': media_id,
        'match': 'any',
        'mode': 'reply_only',
        'activationStartedAt': datetime.utcnow() - timedelta(minutes=10),
        'createdAt': datetime.utcnow() - timedelta(minutes=10),
        'processExistingComments': True,
        'process_existing_unreplied_comments': True,  # ignored — cutoff always applies
        'comment_reply': 'thanks!',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'thanks!', 'replies': ['thanks!']}},
        ],
        'edges': [{'id': 'e1', 'source': 'n_trigger', 'target': 'n_reply'}],
    }


def _old_ts():
    return (datetime.utcnow() - timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%S+0000')


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


# ── apply_activation_cutoff tests ─────────────────────────────────────────────

def test_broad_rule_flag_always_ignored_for_historical(monkeypatch):
    """process_existing_unreplied_comments=True on a broad rule never bypasses cutoff."""
    db = FakeDB(automations=[_broad_rule()])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    comment = {
        'ig_comment_id': 'c_broad_old',
        'commenter_id': 'fan1',
        'text': 'hello',
        'media_id': 'm999',
        'timestamp': _old_ts(),
    }

    res = _run(server._handle_new_comment(_user(), comment, source='polling'))

    assert res.get('matched') is not True, \
        f'broad rule must not process historical comment; got {res}'
    assert ran['count'] == 0


def test_specific_rule_flag_also_ignored_for_historical(monkeypatch):
    """Even a specific-post rule with flag=True does NOT bypass the activation cutoff.
    Historical comments are always blocked regardless of flag values."""
    db = FakeDB(automations=[_specific_rule('m111')])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    comment = {
        'ig_comment_id': 'c_specific_old',
        'commenter_id': 'fan2',
        'text': 'hello',
        'media_id': 'm111',
        'timestamp': _old_ts(),  # before rule activation — always blocked
    }

    res = _run(server._handle_new_comment(_user(), comment, source='polling'))

    assert res.get('matched') is not True, \
        f'specific-post rule must not bypass activation cutoff; got {res}'
    assert ran['count'] == 0


def test_already_replied_not_duplicated(monkeypatch):
    """Dedup layer still fires for already-replied comments regardless of flags."""
    existing = {
        'id': 'cdoc1',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'ig_comment_id': 'c_already',
        'replied': True,
        'action_status': 'success',
        'created': datetime.utcnow() - timedelta(minutes=5),
    }
    db = FakeDB(automations=[_specific_rule('m111')], comments=[existing])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    comment = {
        'ig_comment_id': 'c_already',
        'commenter_id': 'fan3',
        'text': 'hello again',
        'media_id': 'm111',
        'timestamp': _old_ts(),
    }

    res = _run(server._handle_new_comment(_user(), comment, source='polling'))

    assert res.get('already_processed') is True, \
        f'already-replied comment must be deduped; got {res}'
    assert ran['count'] == 0


def test_fresh_comment_after_activation_is_processed(monkeypatch):
    """A comment with timestamp AFTER rule activation is processed normally."""
    db = FakeDB(automations=[_specific_rule('m111')])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    fresh_ts = (datetime.utcnow() - timedelta(seconds=30)).strftime('%Y-%m-%dT%H:%M:%S+0000')
    comment = {
        'ig_comment_id': 'c_fresh',
        'commenter_id': 'fan4',
        'text': 'hi',
        'media_id': 'm111',
        'timestamp': fresh_ts,
    }

    res = _run(server._handle_new_comment(_user(), comment, source='polling'))

    assert res.get('matched') is True, \
        f'fresh comment after activation must be processed; got {res}'
    assert ran['count'] == 1


# ── Catch-up endpoint tests ───────────────────────────────────────────────────

def test_catchup_endpoint_uses_stored_comment_docs(monkeypatch):
    """The catch-up endpoint queries db.comments, not the Graph API."""
    user = {**_user(), 'id': 'u1'}
    # One unreplied comment doc in the DB.
    stored_comment = {
        'id': 'cdoc_unreplied',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'ig_comment_id': 'c_unreplied',
        'media_id': 'm111',
        'text': 'test',
        'commenter_id': 'fan5',
        'timestamp': (datetime.utcnow() - timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%S+0000'),
        'replied': False,
        'action_status': 'skipped',
    }
    db = FakeDB(
        automations=[_specific_rule('m111')],
        comments=[stored_comment],
        users=[user],
    )
    monkeypatch.setattr(server, 'db', db)

    handle_calls = []

    async def fake_handle(u, comment, source='polling'):
        handle_calls.append(comment['ig_comment_id'])
        return {'matched': True, 'action_status': 'success'}

    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)

    result = _run(server.instagram_process_unreplied_comments(user_id='u1'))

    # Endpoint must have processed from stored docs (not calling Graph API)
    assert result['checked'] >= 1, f'must check stored docs; got {result}'
    assert 'c_unreplied' in handle_calls


def test_catchup_endpoint_historical_still_skipped(monkeypatch):
    """Catch-up endpoint skips docs whose activation cutoff returns historical."""
    user = {**_user(), 'id': 'u1'}
    old_stored = {
        'id': 'cdoc_old',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'ig_comment_id': 'c_old_stored',
        'media_id': 'm111',
        'text': 'old comment',
        'commenter_id': 'fan6',
        'timestamp': _old_ts(),  # before rule activation
        'replied': False,
        'action_status': 'skipped',
        'skip_reason': 'historical_before_rule_activation',
    }
    db = FakeDB(
        automations=[_specific_rule('m111')],
        comments=[old_stored],
        users=[user],
    )
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, '_account_scoped_query',
                        lambda _u, _ig: {'user_id': 'u1'})
    monkeypatch.setattr(server, 'matchesAutomationRule',
                        lambda auto, text, ctx: {'matches': True})
    monkeypatch.setattr(server, '_fetch_latest_media_id',
                        lambda *a, **k: _async_none())
    monkeypatch.setattr(server, 'ws_manager',
                        SimpleNamespace(send=lambda *a, **k: _async_none()))
    ran = {'count': 0}

    async def fake_run(user_doc, automation, *a, **kw):
        ran['count'] += 1

    monkeypatch.setattr(server, '_run_and_record_action', fake_run)

    result = _run(server.instagram_process_unreplied_comments(user_id='u1'))

    # The historical doc is blocked at dedup (historical_before_rule_activation is
    # not in retryable_skip, so it returns already_processed → skipped_duplicate).
    not_replied = result['replied'] == 0
    blocked = result['skipped_historical'] >= 1 or result['skipped_duplicate'] >= 1
    assert blocked and not_replied, \
        f'old stored doc must not be replied to; got {result}'
    assert ran['count'] == 0, 'no reply must fire for historical comment'
