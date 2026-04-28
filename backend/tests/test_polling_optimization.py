"""Tests for comment polling optimization, dashboard stats, and security.

Covers:
- poll-now does not reprocess already processed comments
- rules are loaded once per account, not per comment
- comments/logs are scoped by instagramAccountId
- account A data does not appear in account B
- duplicate comment IDs do not create duplicate replies
- missing/expired tokens are handled safely
- Graph API failures do not crash the whole polling run
- dashboard weeklyPerformance returns 7 rows
- dashboard messagesSent matches sent reply logs
- no endpoint returns accessToken
- logs do not contain accessToken or CRON_SECRET
"""
import asyncio
import logging
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
from fastapi import HTTPException  # noqa: E402


# ---------------------------------------------------------------------------
# Fake infrastructure
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = str(body)
        self.headers = {'content-type': 'application/json'}

    def json(self):
        return self._body


class FakeAsyncClient:
    """Fake httpx.AsyncClient that pops from a response queue."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0
        self.get_calls = []
        self.post_calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def get(self, *args, **kwargs):
        self.calls += 1
        self.get_calls.append((args, kwargs))
        if self.responses:
            return self.responses.pop(0)
        return FakeResponse(500, {'error': 'no more responses'})

    async def post(self, *args, **kwargs):
        self.calls += 1
        self.post_calls.append((args, kwargs))
        if self.responses:
            return self.responses.pop(0)
        return FakeResponse(500, {'error': 'no more responses'})


def _match(doc, query):
    for key, expected in query.items():
        if key == '$or':
            if not any(_match(doc, item) for item in expected):
                return False
            continue
        if key == '$and':
            if not all(_match(doc, item) for item in expected):
                return False
            continue
        value = doc.get(key)
        if isinstance(expected, dict):
            if '$exists' in expected and ((key in doc) != expected['$exists']):
                return False
            if '$lte' in expected and not (value is not None and value <= expected['$lte']):
                return False
            if '$gte' in expected and not (value is not None and value >= expected['$gte']):
                return False
            if '$nin' in expected and value in expected['$nin']:
                return False
            if '$ne' in expected and value == expected['$ne']:
                return False
            if '$regex' in expected:
                import re
                flags = re.IGNORECASE if expected.get('$options', '') == 'i' else 0
                if not (isinstance(value, str) and re.search(expected['$regex'], value, flags)):
                    return False
        elif value != expected:
            return False
    return True


class FakeCursor:
    def __init__(self, docs):
        self.docs = docs

    def sort(self, *_args, **_kwargs):
        return self

    def limit(self, _limit):
        return self

    def skip(self, _skip):
        return self

    async def to_list(self, limit):
        return list(self.docs)[:limit] if limit else list(self.docs)


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])
        self.insert_calls = 0
        self.find_calls = 0

    async def find_one(self, query):
        return next((doc for doc in self.docs if _match(doc, query)), None)

    def find(self, query, projection=None):
        """Projection is accepted but ignored — tests return full docs."""
        self.find_calls += 1
        return FakeCursor([doc for doc in self.docs if _match(doc, query)])

    async def insert_one(self, doc):
        self.insert_calls += 1
        self.docs.append(dict(doc))
        return SimpleNamespace(inserted_id=doc.get('id'))

    async def update_one(self, query, update, upsert=False):
        doc = await self.find_one(query)
        if not doc and upsert:
            doc = dict(query)
            self.docs.append(doc)
        if not doc:
            return SimpleNamespace(matched_count=0, modified_count=0)
        if '$setOnInsert' in update and upsert:
            for key, value in update['$setOnInsert'].items():
                doc.setdefault(key, value)
        for key, value in update.get('$set', {}).items():
            doc[key] = value
        for key, value in update.get('$inc', {}).items():
            doc[key] = int(doc.get(key) or 0) + value
        return SimpleNamespace(matched_count=1, modified_count=1)

    async def update_many(self, query, update):
        count = 0
        for doc in self.docs:
            if _match(doc, query):
                for key, value in update.get('$set', {}).items():
                    doc[key] = value
                for key, value in update.get('$inc', {}).items():
                    doc[key] = int(doc.get(key) or 0) + value
                count += 1
        return SimpleNamespace(modified_count=count)

    async def count_documents(self, query):
        return len([doc for doc in self.docs if _match(doc, query)])

    async def find_one_and_update(self, query, update, **_kwargs):
        doc = await self.find_one(query)
        if not doc:
            return None
        await self.update_one({'id': doc['id']}, update)
        return doc

    async def drop_index(self, name):
        pass

    async def create_index(self, *args, **kwargs):
        pass

    async def delete_one(self, query):
        before = len(self.docs)
        self.docs = [d for d in self.docs if not _match(d, query)]
        deleted = before - len(self.docs)
        return SimpleNamespace(deleted_count=deleted)


class FakeDB:
    def __init__(self, account=None, user=None, **collections):
        accounts = account if isinstance(account, list) else ([account] if account else [])
        users = user if isinstance(user, list) else ([user] if user else [])
        self.instagram_accounts = FakeCollection(accounts)
        self.users = FakeCollection(users)
        self.automations = FakeCollection(collections.get('automations', []))
        self.comments = FakeCollection(collections.get('comments', []))
        self.conversations = FakeCollection(collections.get('conversations', []))
        self.contacts = FakeCollection(collections.get('contacts', []))
        self.dm_rules = FakeCollection(collections.get('dm_rules', []))
        self.dm_logs = FakeCollection(collections.get('dm_logs', []))
        self.comment_dm_sessions = FakeCollection(collections.get('comment_dm_sessions', []))
        self.webhook_log = FakeCollection(collections.get('webhook_log', []))
        self.broadcasts = FakeCollection(collections.get('broadcasts', []))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.run(coro)


def _now():
    return datetime.utcnow()


def _account(**overrides):
    now = _now()
    doc = {
        'id': 'acc1',
        'userId': 'u1',
        'instagramAccountId': 'ig1',
        'igUserId': 'ig1',
        'accessToken': 'token-a',
        'tokenSource': 'long_lived',
        'tokenExpiresAt': now + timedelta(days=30),
        'lastRefreshedAt': now - timedelta(days=2),
        'refreshAttempts': 0,
        'connectionValid': True,
        'isActive': True,
        'isCurrent': True,
        'createdAt': now - timedelta(days=10),
        'updatedAt': now,
    }
    doc.update(overrides)
    return doc


def _user(**overrides):
    doc = {
        'id': 'u1',
        'email': 'test@example.com',
        'ig_user_id': 'ig1',
        'meta_access_token': 'token-a',
        'active_instagram_account_id': 'acc1',
        'instagramConnected': True,
        'instagram_connection_valid': True,
        'instagramHandle': 'testuser',
    }
    doc.update(overrides)
    return doc


def _automation(**overrides):
    now = _now()
    doc = {
        'id': 'auto1',
        'user_id': 'u1',
        'instagramAccountId': 'ig1',
        'igUserId': 'ig1',
        'name': 'Test Rule',
        'status': 'active',
        'trigger': 'comment:media1',
        'match': 'any',
        'keyword': '',
        'keywords': [],
        'mode': 'reply_and_dm',
        'comment_reply': 'Thanks!',
        'dm_text': 'Hello!',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger',
             'data': {'trigger': 'comment:media1', 'match': 'any', 'keyword': ''}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'Thanks!', 'replies': ['Thanks!']}},
        ],
        'edges': [{'id': 'e1', 'source': 'n_trigger', 'target': 'n_reply'}],
        'sent': 0,
        'clicks': 0,
        'processExistingComments': True,
        'activationStartedAt': now - timedelta(hours=1),
        'createdAt': now - timedelta(hours=1),
        'updatedAt': now,
    }
    doc.update(overrides)
    return doc


def _comment_data(comment_id='c1', media_id='media1', commenter_id='user1',
                  text='great post', timestamp=None):
    return {
        'ig_comment_id': comment_id,
        'media_id': media_id,
        'commenter_id': commenter_id,
        'commenter_username': 'commenter',
        'text': text,
        'timestamp': timestamp or _now().isoformat(),
    }


# ---------------------------------------------------------------------------
# Test: poll-now does not reprocess already processed comments
# ---------------------------------------------------------------------------

def test_poll_does_not_reprocess_already_processed_comments(monkeypatch):
    """Comments already in DB should be skipped without DB query per comment."""
    existing_comment = {
        'id': 'existing-doc',
        'user_id': 'u1',
        'ig_comment_id': 'c1',
        'instagramAccountId': 'ig1',
        'igUserId': 'ig1',
        'action_status': 'success',
        'matched': True,
        'created': _now(),
    }
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[_automation()],
        comments=[existing_comment],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    # Simulate IG API returning the already-processed comment
    ig_comments = [{'id': 'c1', 'text': 'great post',
                    'from': {'id': 'user1', 'username': 'commenter'},
                    'timestamp': _now().isoformat()}]
    fake_client = FakeAsyncClient([
        FakeResponse(200, {'data': ig_comments}),
    ])
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: fake_client)

    user_doc = _user()
    stats = _run(server._poll_user_comments(user_doc))

    # Comment was seen but not reprocessed
    assert stats['commentsSeen'] == 1
    assert stats['newComments'] == 0
    # No new comment doc inserted
    assert fake_db.comments.insert_calls == 0


def test_poll_processes_new_comments_and_skips_existing(monkeypatch):
    """New comments are processed; existing ones are skipped."""
    existing_comment = {
        'id': 'existing-doc',
        'user_id': 'u1',
        'ig_comment_id': 'c1',
        'instagramAccountId': 'ig1',
        'igUserId': 'ig1',
        'action_status': 'success',
        'matched': True,
        'created': _now(),
    }
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[_automation()],
        comments=[existing_comment],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    ig_comments = [
        {'id': 'c1', 'text': 'old comment',
         'from': {'id': 'user1', 'username': 'commenter'},
         'timestamp': _now().isoformat()},
        {'id': 'c2', 'text': 'new comment',
         'from': {'id': 'user2', 'username': 'commenter2'},
         'timestamp': _now().isoformat()},
    ]
    # c2 triggers a reply_comment node — mock the IG reply API
    fake_client = FakeAsyncClient([
        FakeResponse(200, {'data': ig_comments}),  # GET comments
        FakeResponse(200, {'id': 'reply-id'}),     # POST reply
    ])
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: fake_client)

    user_doc = _user()
    stats = _run(server._poll_user_comments(user_doc))

    assert stats['commentsSeen'] == 2
    assert stats['newComments'] == 1
    # Only c2 was inserted
    new_docs = [d for d in fake_db.comments.docs if d.get('ig_comment_id') == 'c2']
    assert len(new_docs) == 1


# ---------------------------------------------------------------------------
# Test: rules are loaded once per account, not per comment
# ---------------------------------------------------------------------------

def test_rules_loaded_once_not_per_comment(monkeypatch):
    """Automations query should fire once per poll, not once per comment."""
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[_automation()],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    # 3 new comments on the same media
    ig_comments = [
        {'id': f'c{i}', 'text': f'comment {i}',
         'from': {'id': f'user{i}', 'username': f'user{i}'},
         'timestamp': _now().isoformat()}
        for i in range(3)
    ]
    # Each comment triggers a reply — provide 3 reply responses
    fake_client = FakeAsyncClient([
        FakeResponse(200, {'data': ig_comments}),  # GET comments
        FakeResponse(200, {'id': 'r1'}),            # POST reply c0
        FakeResponse(200, {'id': 'r2'}),            # POST reply c1
        FakeResponse(200, {'id': 'r3'}),            # POST reply c2
    ])
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: fake_client)

    # Track automations.find calls
    original_find = fake_db.automations.find
    find_call_count = [0]

    def counting_find(query, projection=None):
        find_call_count[0] += 1
        return original_find(query, projection)

    fake_db.automations.find = counting_find

    user_doc = _user()
    stats = _run(server._poll_user_comments(user_doc))

    assert stats['commentsSeen'] == 3
    # automations.find should be called exactly ONCE (pre-load), not 3 times
    assert find_call_count[0] == 1, (
        f'Expected 1 automations.find call, got {find_call_count[0]}. '
        'Rules must be loaded once per poll, not per comment.'
    )


# ---------------------------------------------------------------------------
# Test: comments/logs are scoped by instagramAccountId
# ---------------------------------------------------------------------------

def test_handle_new_comment_scoped_by_instagram_account(monkeypatch):
    """_handle_new_comment must not match comments from a different account."""
    # Comment from account B already in DB
    comment_from_b = {
        'id': 'doc-b',
        'user_id': 'u1',
        'ig_comment_id': 'c1',
        'instagramAccountId': 'igB',
        'igUserId': 'igB',
        'action_status': 'success',
        'created': _now(),
    }
    fake_db = FakeDB(
        account=_account(instagramAccountId='igA', igUserId='igA'),
        user=_user(ig_user_id='igA'),
        automations=[_automation(instagramAccountId='igA', igUserId='igA',
                                 trigger='comment:media1')],
        comments=[comment_from_b],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    # c1 from account A should NOT be treated as already processed
    user_doc = _user(ig_user_id='igA')
    result = _run(server._handle_new_comment(
        user_doc,
        _comment_data(comment_id='c1', media_id='media1', commenter_id='user1'),
        source='polling',
    ))

    # Should be processed as new (different account scope)
    assert result.get('processed') is True
    new_docs = [d for d in fake_db.comments.docs
                if d.get('ig_comment_id') == 'c1' and d.get('instagramAccountId') == 'igA']
    assert len(new_docs) == 1


# ---------------------------------------------------------------------------
# Test: account A data does not appear in account B
# ---------------------------------------------------------------------------

def test_account_isolation_in_poll(monkeypatch):
    """Polling for account A must not process comments belonging to account B."""
    comment_a = {
        'id': 'doc-a',
        'user_id': 'u1',
        'ig_comment_id': 'cA',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'action_status': 'success',
        'created': _now(),
    }
    comment_b = {
        'id': 'doc-b',
        'user_id': 'u1',
        'ig_comment_id': 'cB',
        'instagramAccountId': 'igB',
        'igUserId': 'igB',
        'action_status': 'success',
        'created': _now(),
    }
    fake_db = FakeDB(
        account=_account(instagramAccountId='igA', igUserId='igA'),
        user=_user(ig_user_id='igA'),
        automations=[_automation(instagramAccountId='igA', igUserId='igA',
                                 trigger='comment:media1')],
        comments=[comment_a, comment_b],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    # IG returns cA (already processed for igA) and cB (belongs to igB)
    ig_comments = [
        {'id': 'cA', 'text': 'old', 'from': {'id': 'u1'}, 'timestamp': _now().isoformat()},
        {'id': 'cB', 'text': 'other account', 'from': {'id': 'u2'}, 'timestamp': _now().isoformat()},
    ]
    fake_client = FakeAsyncClient([FakeResponse(200, {'data': ig_comments})])
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: fake_client)

    user_doc = _user(ig_user_id='igA')
    stats = _run(server._poll_user_comments(user_doc))

    # cA is already processed for igA → skip
    # cB is new for igA (different account) → process
    assert stats['commentsSeen'] == 2
    # cB should be inserted as new for igA
    new_for_a = [d for d in fake_db.comments.docs
                 if d.get('ig_comment_id') == 'cB' and d.get('instagramAccountId') == 'igA']
    assert len(new_for_a) == 1


# ---------------------------------------------------------------------------
# Test: duplicate comment IDs do not create duplicate replies
# ---------------------------------------------------------------------------

def test_duplicate_comment_id_not_reprocessed(monkeypatch):
    """Same ig_comment_id must not trigger duplicate replies."""
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[_automation()],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    # First call — no existing comment
    fake_client = FakeAsyncClient([FakeResponse(200, {'id': 'reply-1'})])
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: fake_client)

    user_doc = _user()
    result1 = _run(server._handle_new_comment(
        user_doc,
        _comment_data(comment_id='c1', media_id='media1', commenter_id='user1'),
        source='polling',
    ))
    assert result1.get('processed') is True
    assert fake_db.comments.insert_calls == 1

    # Second call — same comment_id, should be deduped
    result2 = _run(server._handle_new_comment(
        user_doc,
        _comment_data(comment_id='c1', media_id='media1', commenter_id='user1'),
        source='polling',
    ))
    assert result2.get('already_processed') is True
    assert result2.get('reason') == 'duplicate'
    # Still only 1 insert
    assert fake_db.comments.insert_calls == 1


def test_preloaded_processed_ids_prevents_duplicate(monkeypatch):
    """With preloaded_processed_ids, duplicate is caught without DB query."""
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[_automation()],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    user_doc = _user()
    processed_ids = {'c1'}  # pre-loaded set already contains c1

    result = _run(server._handle_new_comment(
        user_doc,
        _comment_data(comment_id='c1', media_id='media1', commenter_id='user1'),
        source='polling',
        preloaded_processed_ids=processed_ids,
        preloaded_existing_docs={},
    ))

    assert result.get('already_processed') is True
    assert result.get('reason') == 'duplicate'
    assert fake_db.comments.insert_calls == 0


# ---------------------------------------------------------------------------
# Test: missing/expired tokens are handled safely
# ---------------------------------------------------------------------------

def test_poll_missing_token_returns_error_not_crash(monkeypatch):
    """Missing token should return error stats, not raise an exception."""
    user_doc = _user(meta_access_token='', ig_user_id='ig1')
    fake_db = FakeDB(account=_account(), user=user_doc)
    monkeypatch.setattr(server, 'db', fake_db)

    stats = _run(server._poll_user_comments(user_doc))

    assert 'missing_token_or_ig_id' in stats['errors']
    assert stats['mediaChecked'] == 0


def test_poll_missing_ig_id_returns_error_not_crash(monkeypatch):
    """Missing ig_user_id should return error stats, not raise an exception."""
    user_doc = _user(meta_access_token='token-a', ig_user_id='')
    fake_db = FakeDB(account=_account(), user=user_doc)
    monkeypatch.setattr(server, 'db', fake_db)

    stats = _run(server._poll_user_comments(user_doc))

    assert 'missing_token_or_ig_id' in stats['errors']
    assert stats['mediaChecked'] == 0


# ---------------------------------------------------------------------------
# Test: Graph API failures do not crash the whole polling run
# ---------------------------------------------------------------------------

def test_graph_api_failure_does_not_crash_poll(monkeypatch):
    """A 403 from Graph API for one media should not crash the whole poll."""
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[_automation()],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    # Graph API returns 403 for the media
    fake_client = FakeAsyncClient([
        FakeResponse(403, {'error': {'message': 'Insufficient permissions', 'code': 200}}),
    ])
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: fake_client)

    user_doc = _user()
    stats = _run(server._poll_user_comments(user_doc))

    # Should not raise; should record the error
    assert stats['mediaChecked'] == 1
    assert len(stats['errors']) == 1
    assert stats['errors'][0].get('http') == 403
    assert stats['newComments'] == 0


def test_graph_api_exception_does_not_crash_poll(monkeypatch):
    """A network exception for one media should not crash the whole poll."""
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[_automation()],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    class ExplodingClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

        async def get(self, *args, **kwargs):
            raise ConnectionError('network failure')

    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: ExplodingClient())

    user_doc = _user()
    stats = _run(server._poll_user_comments(user_doc))

    assert stats['mediaChecked'] == 1
    assert len(stats['errors']) == 1
    assert 'exc:' in stats['errors'][0].get('error', '')
    assert stats['newComments'] == 0


# ---------------------------------------------------------------------------
# Test: dashboard weeklyPerformance returns 7 rows
# ---------------------------------------------------------------------------

def test_dashboard_weekly_performance_returns_7_rows(monkeypatch):
    """weeklyPerformance must always have exactly 7 entries."""
    acc = _account()
    fake_db = FakeDB(
        account=acc,
        user=_user(),
        automations=[_automation()],
        comments=[],
        conversations=[],
        contacts=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_stats(user_id='u1'))

    weekly = result.get('weeklyPerformance') or result.get('weekly_chart') or []
    assert len(weekly) == 7, f'Expected 7 rows, got {len(weekly)}: {weekly}'
    # Each row must have 'day', 'messages', 'conversions'
    for row in weekly:
        assert 'day' in row
        assert 'messages' in row
        assert 'conversions' in row


def test_dashboard_weekly_performance_counts_successful_comments(monkeypatch):
    """weeklyPerformance messages should count successful comment actions."""
    now = datetime.utcnow()
    today = now.replace(hour=12, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)

    successful_today = {
        'id': 'log1',
        'user_id': 'u1',
        'ig_comment_id': 'c1',
        'instagramAccountId': 'ig1',
        'igUserId': 'ig1',
        'action_status': 'success',
        'matched': True,
        'processed_at': today,
        'created': today,
    }
    successful_yesterday = {
        'id': 'log2',
        'user_id': 'u1',
        'ig_comment_id': 'c2',
        'instagramAccountId': 'ig1',
        'igUserId': 'ig1',
        'action_status': 'success',
        'matched': True,
        'processed_at': yesterday,
        'created': yesterday,
    }
    failed_today = {
        'id': 'log3',
        'user_id': 'u1',
        'ig_comment_id': 'c3',
        'instagramAccountId': 'ig1',
        'igUserId': 'ig1',
        'action_status': 'failed',
        'matched': True,
        'processed_at': today,
        'created': today,
    }

    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[_automation()],
        comments=[successful_today, successful_yesterday, failed_today],
        conversations=[],
        contacts=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_stats(user_id='u1'))

    weekly = result.get('weeklyPerformance') or result.get('weekly_chart') or []
    assert len(weekly) == 7

    # Find today's and yesterday's rows
    today_row = weekly[-1]  # last row is today
    yesterday_row = weekly[-2]

    # Today: 1 success (failed not counted)
    assert today_row['messages'] == 1
    # Yesterday: 1 success
    assert yesterday_row['messages'] == 1


# ---------------------------------------------------------------------------
# Test: dashboard messagesSent matches sent reply logs
# ---------------------------------------------------------------------------

def test_dashboard_messages_sent_matches_automation_sent_count(monkeypatch):
    """messages_sent should sum the 'sent' counter from automations."""
    auto1 = _automation(id='a1', sent=5)
    auto2 = _automation(id='a2', sent=3)
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[auto1, auto2],
        comments=[],
        conversations=[],
        contacts=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_stats(user_id='u1'))

    assert result['messages_sent'] == 8


# ---------------------------------------------------------------------------
# Test: no endpoint returns accessToken
# ---------------------------------------------------------------------------

def test_dashboard_stats_does_not_return_access_token(monkeypatch):
    """Dashboard stats response must not contain any access token."""
    fake_db = FakeDB(
        account=_account(accessToken='super-secret-token'),
        user=_user(meta_access_token='super-secret-token'),
        automations=[],
        comments=[],
        conversations=[],
        contacts=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_stats(user_id='u1'))

    result_str = str(result)
    assert 'super-secret-token' not in result_str
    assert 'accessToken' not in result_str


def test_poll_stats_does_not_return_access_token(monkeypatch):
    """Poll stats response must not contain any access token."""
    fake_db = FakeDB(
        account=_account(accessToken='super-secret-token'),
        user=_user(meta_access_token='super-secret-token'),
        automations=[],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    user_doc = _user(meta_access_token='super-secret-token')
    stats = _run(server._poll_user_comments(user_doc))

    stats_str = str(stats)
    assert 'super-secret-token' not in stats_str


# ---------------------------------------------------------------------------
# Test: logs do not contain accessToken or CRON_SECRET
# ---------------------------------------------------------------------------

def test_logs_do_not_contain_access_token(monkeypatch, caplog):
    """Log output must not contain access tokens."""
    fake_db = FakeDB(
        account=_account(accessToken='my-secret-access-token'),
        user=_user(meta_access_token='my-secret-access-token'),
        automations=[_automation()],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    # Return empty comments so no IG API calls are made
    fake_client = FakeAsyncClient([FakeResponse(200, {'data': []})])
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: fake_client)

    user_doc = _user(meta_access_token='my-secret-access-token')
    with caplog.at_level(logging.INFO, logger='mychat'):
        _run(server._poll_user_comments(user_doc))

    for record in caplog.records:
        assert 'my-secret-access-token' not in record.getMessage(), (
            f'Access token leaked in log: {record.getMessage()}'
        )


def test_cron_secret_not_in_logs(monkeypatch, caplog):
    """CRON_SECRET must never appear in log output."""
    monkeypatch.setattr(server, 'CRON_SECRET', 'ultra-secret-cron-key')

    with caplog.at_level(logging.INFO, logger='mychat'):
        # Trigger a cron secret validation failure
        result = server._cron_secret_is_valid('wrong-key')

    assert result is False
    for record in caplog.records:
        assert 'ultra-secret-cron-key' not in record.getMessage()


# ---------------------------------------------------------------------------
# Test: preloaded_automations parameter avoids per-comment DB query
# ---------------------------------------------------------------------------

def test_handle_new_comment_uses_preloaded_automations(monkeypatch):
    """When preloaded_automations is provided, automations.find must not be called."""
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[_automation()],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    find_call_count = [0]
    original_find = fake_db.automations.find

    def counting_find(query, projection=None):
        find_call_count[0] += 1
        return original_find(query, projection)

    fake_db.automations.find = counting_find

    user_doc = _user()
    preloaded = [_automation()]

    _run(server._handle_new_comment(
        user_doc,
        _comment_data(comment_id='c1', media_id='media1', commenter_id='user1'),
        source='polling',
        preloaded_automations=preloaded,
        preloaded_processed_ids=set(),
        preloaded_existing_docs={},
    ))

    assert find_call_count[0] == 0, (
        'automations.find should not be called when preloaded_automations is provided'
    )


# ---------------------------------------------------------------------------
# Test: dashboard stats filter by active instagram account
# ---------------------------------------------------------------------------

def test_dashboard_stats_active_account_isolation(monkeypatch):
    """Dashboard stats must be scoped to the active Instagram account."""
    acc_a = _account(id='accA', userId='u1', instagramAccountId='igA', igUserId='igA',
                     accessToken='token-a', isCurrent=True)
    acc_b = _account(id='accB', userId='u1', instagramAccountId='igB', igUserId='igB',
                     accessToken='token-b', isCurrent=False)
    user = _user(active_instagram_account_id='accA', ig_user_id='igA',
                 meta_access_token='token-a')

    auto_a = _automation(id='autoA', user_id='u1', instagramAccountId='igA',
                         igUserId='igA', status='active')
    auto_b = _automation(id='autoB', user_id='u1', instagramAccountId='igB',
                         igUserId='igB', status='active')

    fake_db = FakeDB(
        account=[acc_a, acc_b],
        user=user,
        automations=[auto_a, auto_b],
        comments=[],
        conversations=[],
        contacts=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_stats(user_id='u1'))

    # Should only count automations for account A
    assert result['activeInstagramAccountId'] == 'accA'
    assert result['active_automations'] == 1


# ---------------------------------------------------------------------------
# Test: poll-now endpoint requires authentication
# ---------------------------------------------------------------------------

def test_poll_now_authenticated_endpoint_returns_ok_when_connected(monkeypatch):
    """POST /instagram/comments/poll-now should work for connected users."""
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    # No automations → returns immediately with empty stats
    result = _run(server.instagram_comments_poll_now(user_id='u1'))

    assert result['ok'] is True
    assert result['mediaChecked'] == 0


def test_poll_now_returns_not_connected_when_instagram_not_connected(monkeypatch):
    """POST /instagram/comments/poll-now should return ok=False when not connected."""
    user = _user(instagramConnected=False)
    fake_db = FakeDB(
        account=_account(),
        user=user,
        automations=[],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.instagram_comments_poll_now(user_id='u1'))

    assert result['ok'] is False
    assert any('instagram_not_connected' in str(e) for e in result.get('errors', []))


# ---------------------------------------------------------------------------
# Test: self-comments are skipped
# ---------------------------------------------------------------------------

def test_self_comment_is_skipped(monkeypatch):
    """Comments from the business account itself must be skipped."""
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[_automation()],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    user_doc = _user(ig_user_id='ig1')
    # commenter_id == ig_user_id → self-comment
    result = _run(server._handle_new_comment(
        user_doc,
        _comment_data(comment_id='c1', media_id='media1', commenter_id='ig1'),
        source='polling',
    ))

    assert result.get('reason') == 'self_comment'
    assert result.get('processed') is False
    assert fake_db.comments.insert_calls == 0


# ---------------------------------------------------------------------------
# Test: missing comment ID is skipped gracefully
# ---------------------------------------------------------------------------

def test_missing_comment_id_is_skipped(monkeypatch):
    """Comments without an ig_comment_id must be skipped without crashing."""
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[_automation()],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    user_doc = _user()
    result = _run(server._handle_new_comment(
        user_doc,
        {'ig_comment_id': None, 'media_id': 'media1', 'commenter_id': 'user1',
         'text': 'hello', 'timestamp': _now().isoformat()},
        source='polling',
    ))

    assert result.get('reason') == 'missing_id'
    assert result.get('processed') is False
    assert fake_db.comments.insert_calls == 0


# ---------------------------------------------------------------------------
# Test: poll stats structure is correct
# ---------------------------------------------------------------------------

def test_poll_stats_structure(monkeypatch):
    """_poll_user_comments must return all expected stat keys."""
    fake_db = FakeDB(
        account=_account(),
        user=_user(),
        automations=[],
        comments=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    user_doc = _user()
    stats = _run(server._poll_user_comments(user_doc))

    required_keys = {'user_id', 'mediaChecked', 'commentsSeen', 'newComments',
                     'matched', 'actionsSucceeded', 'actionsFailed', 'media', 'errors'}
    assert required_keys.issubset(set(stats.keys()))
