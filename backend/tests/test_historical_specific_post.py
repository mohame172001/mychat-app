"""Tests for selected-post historical comment catch-up safety.

Historical processing is allowed only for one explicitly selected media/post.
Broad rules must never reply to pre-rule historical comments, even if a legacy
flag is present.

Coverage:
1. Broad rule flag is ignored.
2. Specific-post rule flag is honored for that selected media.
3. Specific-post rule does not process comments from other media.
4. Already replied comments are not duplicated.
5. Catch-up endpoint fetches only selected media comments from Graph.
6. Catch-up endpoint ignores broad flagged rules.
7. Fresh comments after activation are processed normally.
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
        self.instagram_accounts = FakeCollection([])
        self.tracked_links = FakeCollection([])
        self.comment_dm_sessions = FakeCollection([])


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = str(self._payload)

    def json(self):
        return self._payload


class FakeAsyncClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.get_calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def get(self, *args, **kwargs):
        self.get_calls.append((args, kwargs))
        if self.responses:
            return self.responses.pop(0)
        return FakeResponse(200, {'data': []})


def _user():
    return {
        'id': 'u1',
        'ig_user_id': 'biz1',
        'meta_access_token': 'tok',
        'instagramConnected': True,
        'instagramHandle': '@biz',
    }


def _active_account():
    return {
        'id': 'acc1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'accessToken': 'tok',
        'username': 'biz',
    }


def _broad_rule():
    return {
        'id': 'r_broad',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'status': 'active',
        'trigger': 'comment:any',
        'post_scope': 'any',
        'media_id': '',
        'match': 'any',
        'mode': 'reply_only',
        'activationStartedAt': datetime.utcnow() - timedelta(minutes=10),
        'createdAt': datetime.utcnow() - timedelta(minutes=10),
        'processExistingComments': True,
        'process_existing_unreplied_comments': True,
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
        'post_scope': 'specific',
        'media_id': media_id,
        'match': 'any',
        'mode': 'reply_only',
        'activationStartedAt': datetime.utcnow() - timedelta(minutes=10),
        'createdAt': datetime.utcnow() - timedelta(minutes=10),
        'processExistingComments': True,
        'process_existing_unreplied_comments': True,
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


async def _async_none(*_a, **_kw):
    return None


async def _async_value(value):
    return value


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


def test_broad_rule_flag_ignored_for_historical(monkeypatch):
    db = FakeDB(automations=[_broad_rule()])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    res = _run(server._handle_new_comment(_user(), {
        'ig_comment_id': 'c_broad_old',
        'commenter_id': 'fan1',
        'text': 'hello',
        'media_id': 'm999',
        'timestamp': _old_ts(),
    }, source='polling'))

    assert res.get('matched') is not True, \
        f'broad rule must not process historical comment; got {res}'
    assert ran['count'] == 0


def test_specific_rule_flag_processes_historical_on_selected_media(monkeypatch):
    db = FakeDB(automations=[_specific_rule('m111')])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    res = _run(server._handle_new_comment(_user(), {
        'ig_comment_id': 'c_specific_old',
        'commenter_id': 'fan2',
        'text': 'hello',
        'media_id': 'm111',
        'timestamp': _old_ts(),
    }, source='polling'))

    assert res.get('matched') is True, \
        f'specific-post catch-up should process selected historical comment; got {res}'
    assert ran['count'] == 1


def test_specific_rule_flag_skips_historical_from_other_media(monkeypatch):
    db = FakeDB(automations=[_specific_rule('m111')])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    res = _run(server._handle_new_comment(_user(), {
        'ig_comment_id': 'c_specific_other_old',
        'commenter_id': 'fan2',
        'text': 'hello',
        'media_id': 'm222',
        'timestamp': _old_ts(),
    }, source='polling'))

    assert res.get('matched') is not True, \
        f'specific-post catch-up must ignore other media; got {res}'
    assert ran['count'] == 0


def test_already_replied_not_duplicated(monkeypatch):
    existing = {
        'id': 'cdoc1',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'ig_comment_id': 'c_already',
        'replied': True,
        'reply_status': 'success',
        'reply_provider_response_ok': True,
        'reply_provider_comment_id': 'reply_c_already',
        'replied_at': datetime.utcnow() - timedelta(minutes=4),
        'action_status': 'success',
        'created': datetime.utcnow() - timedelta(minutes=5),
    }
    db = FakeDB(automations=[_specific_rule('m111')], comments=[existing])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    res = _run(server._handle_new_comment(_user(), {
        'ig_comment_id': 'c_already',
        'commenter_id': 'fan3',
        'text': 'hello again',
        'media_id': 'm111',
        'timestamp': _old_ts(),
    }, source='polling'))

    assert res.get('already_processed') is True, \
        f'already-replied comment must be deduped; got {res}'
    assert ran['count'] == 0


def test_fresh_comment_after_activation_is_processed(monkeypatch):
    db = FakeDB(automations=[_specific_rule('m111')])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_run(monkeypatch)

    fresh_ts = (datetime.utcnow() - timedelta(seconds=30)).strftime('%Y-%m-%dT%H:%M:%S+0000')
    res = _run(server._handle_new_comment(_user(), {
        'ig_comment_id': 'c_fresh',
        'commenter_id': 'fan4',
        'text': 'hi',
        'media_id': 'm111',
        'timestamp': fresh_ts,
    }, source='polling'))

    assert res.get('matched') is True, \
        f'fresh comment after activation must be processed; got {res}'
    assert ran['count'] == 1


def test_catchup_endpoint_fetches_selected_media_from_graph(monkeypatch):
    user = {**_user(), 'id': 'u1'}
    db = FakeDB(automations=[_specific_rule('m111')], users=[user])
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'getActiveInstagramAccount',
                        lambda _user_id: _async_value(_active_account()))

    handle_calls = []

    async def fake_handle(u, comment, source='manual_catchup'):
        handle_calls.append((comment['media_id'], comment['ig_comment_id'], source))
        return {'matched': True, 'action_status': 'success'}

    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)
    client = FakeAsyncClient([
        FakeResponse(200, {'data': [{
            'id': 'c_unreplied',
            'text': 'test',
            'username': 'fan5',
            'from': {'id': 'fan5', 'username': 'fan5'},
            'timestamp': (datetime.utcnow() - timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%S+0000'),
        }]}),
    ])
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda **_kwargs: client)

    result = _run(server.instagram_process_unreplied_comments(user_id='u1'))

    assert result['media_ids'] == ['m111']
    assert result['checked'] == 1, f'must check selected Graph comments; got {result}'
    assert result['replied'] == 1
    assert handle_calls == [('m111', 'c_unreplied', 'manual_catchup')]
    assert client.get_calls[0][0][0].endswith('/m111/comments')


def test_catchup_endpoint_ignores_broad_rule_with_flag(monkeypatch):
    user = {**_user(), 'id': 'u1'}
    db = FakeDB(automations=[_broad_rule()], users=[user])
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'getActiveInstagramAccount',
                        lambda _user_id: _async_value(_active_account()))
    client = FakeAsyncClient([FakeResponse(200, {'data': [{'id': 'should_not_fetch'}]})])
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda **_kwargs: client)

    result = _run(server.instagram_process_unreplied_comments(user_id='u1'))

    assert result['media_ids'] == []
    assert result['skipped_broad_scope'] == 1
    assert result['checked'] == 0
    assert result['replied'] == 0
    assert client.get_calls == []
