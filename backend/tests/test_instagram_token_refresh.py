import asyncio
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

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
from starlette.requests import Request  # noqa: E402


class FakeResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = str(body)
        self.headers = {'content-type': 'application/json'}

    def json(self):
        return self._body


class FakeAsyncClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = 0
        self.post_calls = []
        self.get_calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def get(self, *args, **kwargs):
        self.calls += 1
        self.get_calls.append((args, kwargs))
        return self.responses.pop(0)

    async def post(self, *args, **kwargs):
        self.calls += 1
        self.post_calls.append((args, kwargs))
        return self.responses.pop(0)


def _match(doc, query):
    for key, expected in query.items():
        if key == '$or':
            if not any(_match(doc, item) for item in expected):
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
            if '$in' in expected and value not in expected['$in']:
                return False
            if '$nin' in expected and value in expected['$nin']:
                return False
            if '$ne' in expected and value == expected['$ne']:
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

    def skip(self, n):
        self.docs = self.docs[n:]
        return self

    async def to_list(self, limit):
        return list(self.docs)[:limit]


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def find_one(self, query):
        return next((doc for doc in self.docs if _match(doc, query)), None)

    def find(self, query):
        return FakeCursor([doc for doc in self.docs if _match(doc, query)])

    async def update_one(self, query, update, upsert=False):
        doc = await self.find_one(query)
        if not doc and upsert:
            doc = dict(query)
            self.docs.append(doc)
        if not doc:
            return None
        if '$setOnInsert' in update and upsert:
            for key, value in update['$setOnInsert'].items():
                doc.setdefault(key, value)
        for key, value in update.get('$set', {}).items():
            doc[key] = value
        for key, value in update.get('$inc', {}).items():
            doc[key] = int(doc.get(key) or 0) + value
        return None

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

    async def insert_one(self, doc):
        self.docs.append(dict(doc))
        return SimpleNamespace(inserted_id=doc.get('id'))

    async def count_documents(self, query):
        return len([doc for doc in self.docs if _match(doc, query)])

    async def find_one_and_update(self, query, update, **_kwargs):
        doc = await self.find_one(query)
        if not doc:
            return None
        await self.update_one({'id': doc['id']}, update)
        return doc


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
        self.comment_dm_sessions = FakeCollection(collections.get('comment_dm_sessions', []))
        self.dm_rules = FakeCollection(collections.get('dm_rules', []))
        self.dm_logs = FakeCollection(collections.get('dm_logs', []))
        self.tracked_links = FakeCollection(collections.get('tracked_links', []))
        self.link_click_events = FakeCollection(collections.get('link_click_events', []))
        self.usage_events = FakeCollection(collections.get('usage_events', []))
        self.monthly_usage = FakeCollection(collections.get('monthly_usage', []))


def _account(**overrides):
    now = datetime.utcnow()
    doc = {
        'id': 'acc1',
        'userId': 'u1',
        'instagramAccountId': 'ig1',
        'accessToken': 'old-token',
        'tokenSource': 'long_lived',
        'tokenExpiresAt': now + timedelta(days=5),
        'lastRefreshedAt': now - timedelta(days=2),
        'refreshAttempts': 0,
        'connectionValid': True,
        'isActive': True,
        'createdAt': now - timedelta(days=10),
    }
    doc.update(overrides)
    return doc


def _user(**overrides):
    doc = {
        'id': 'u1',
        'ig_user_id': 'igA',
        'meta_access_token': 'token-a',
        'active_instagram_account_id': 'accA',
        'instagramConnected': True,
        'instagram_connection_valid': True,
    }
    doc.update(overrides)
    return doc


def _run(coro):
    return asyncio.run(coro)


def _request(headers=None):
    encoded_headers = [
        (str(k).lower().encode('latin-1'), str(v).encode('latin-1'))
        for k, v in (headers or {}).items()
    ]
    return Request({
        'type': 'http',
        'method': 'POST',
        'path': '/',
        'query_string': b'',
        'headers': encoded_headers,
    })


def test_successful_refresh_updates_token_and_hides_token(monkeypatch):
    user = {'id': 'u1', 'ig_user_id': 'ig1', 'meta_access_token': 'old-token'}
    fake_db = FakeDB(_account(), user)
    monkeypatch.setattr(server, 'db', fake_db)
    fake_client = FakeAsyncClient([FakeResponse(200, {
        'access_token': 'new-token',
        'expires_in': 60 * 60 * 24 * 60,
    })])
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: fake_client)

    result = _run(server.refreshInstagramToken('acc1'))

    assert result['status'] == 'refreshed'
    assert fake_db.instagram_accounts.docs[0]['accessToken'] == 'new-token'
    assert fake_db.users.docs[0]['meta_access_token'] == 'new-token'
    assert 'new-token' not in str(result)


def test_failed_refresh_keeps_old_token_and_redacts_response(monkeypatch):
    fake_db = FakeDB(_account(), {'id': 'u1', 'ig_user_id': 'ig1'})
    monkeypatch.setattr(server, 'db', fake_db)
    fake_client = FakeAsyncClient([FakeResponse(400, {
        'error': {'message': 'bad token', 'code': 190},
        'access_token': 'leaked-token',
    })])
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: fake_client)

    result = _run(server.refreshInstagramToken('acc1'))

    account = fake_db.instagram_accounts.docs[0]
    assert result['ok'] is False
    assert account['accessToken'] == 'old-token'
    assert account['refreshStatus'] == 'failed'
    assert account['refreshError']['access_token'] == '***REDACTED***'


def test_expired_token_is_marked_expired(monkeypatch):
    fake_db = FakeDB(_account(tokenExpiresAt=datetime.utcnow() - timedelta(days=1)),
                     {'id': 'u1', 'ig_user_id': 'ig1'})
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.refreshInstagramToken('acc1'))

    assert result['status'] == 'expired'
    assert fake_db.instagram_accounts.docs[0]['connectionValid'] is False
    assert fake_db.users.docs[0]['instagram_connection_valid'] is False


def test_token_expiring_after_15_days_is_skipped(monkeypatch):
    fake_db = FakeDB(_account(tokenExpiresAt=datetime.utcnow() + timedelta(days=30)))
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.refreshInstagramToken('acc1'))

    assert result['status'] == 'skipped_not_due'
    assert fake_db.instagram_accounts.docs[0]['accessToken'] == 'old-token'


def test_refresh_lock_prevents_duplicate_refresh(monkeypatch):
    fake_db = FakeDB(_account(refreshLockedUntil=datetime.utcnow() + timedelta(minutes=5)))
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.refreshInstagramToken('acc1'))

    assert result['status'] == 'skipped_locked'
    assert fake_db.instagram_accounts.docs[0]['accessToken'] == 'old-token'


def test_cron_secret_validation(monkeypatch):
    monkeypatch.setattr(server, 'CRON_SECRET', 'expected')

    assert server._cron_secret_is_valid(None) is False
    assert server._cron_secret_is_valid('wrong') is False
    assert server._cron_secret_is_valid('expected') is True


def test_cron_endpoint_rejects_missing_authorization(monkeypatch):
    monkeypatch.setattr(server, 'CRON_SECRET', 'expected')

    try:
        _run(server.cron_refresh_instagram_tokens(_request()))
    except HTTPException as exc:
        assert exc.status_code == 403
        assert 'expected' not in str(exc.detail)
    else:
        raise AssertionError('expected HTTPException')


def test_cron_endpoint_rejects_wrong_authorization(monkeypatch):
    monkeypatch.setattr(server, 'CRON_SECRET', 'expected')

    try:
        _run(server.cron_refresh_instagram_tokens(
            _request({'Authorization': 'Bearer wrong'})
        ))
    except HTTPException as exc:
        assert exc.status_code == 403
        assert 'wrong' not in str(exc.detail)
    else:
        raise AssertionError('expected HTTPException')


def test_cron_endpoint_accepts_correct_authorization(monkeypatch):
    monkeypatch.setattr(server, 'CRON_SECRET', 'expected')

    async def fake_cron():
        return {'totalChecked': 0, 'refreshed': 0, 'results': []}

    monkeypatch.setattr(server, 'runInstagramTokenRefreshCron', fake_cron)

    result = _run(server.cron_refresh_instagram_tokens(
        _request({'Authorization': 'Bearer expected'})
    ))

    assert result['totalChecked'] == 0
    assert 'expected' not in str(result)


def test_public_refresh_status_never_returns_access_token():
    row = server._token_refresh_public_row(_account(accessToken='secret-token'))

    assert 'accessToken' not in row
    assert 'secret-token' not in str(row)


def _multi_account_db():
    acc_a = _account(id='accA', userId='u1', instagramAccountId='igA',
                     igUserId='igA', username='account_a', accessToken='token-a')
    acc_b = _account(id='accB', userId='u1', instagramAccountId='igB',
                     igUserId='igB', username='account_b', accessToken='token-b')
    other = _account(id='accOther', userId='u2', instagramAccountId='igOther',
                     igUserId='igOther', username='other', accessToken='token-other')
    return FakeDB(
        [acc_a, acc_b, other],
        [_user(), _user(id='u2', active_instagram_account_id='accOther', ig_user_id='igOther')],
        automations=[
            {'id': 'autoA', 'user_id': 'u1', 'name': 'A', 'status': 'active',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA', 'updated': datetime.utcnow()},
            {'id': 'autoB', 'user_id': 'u1', 'name': 'B', 'status': 'active',
             'instagramAccountDbId': 'accB', 'instagramAccountId': 'igB', 'updated': datetime.utcnow()},
        ],
        comments=[
            {'id': 'commentA', 'user_id': 'u1', 'ig_comment_id': 'cA',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'created': datetime.utcnow()},
            {'id': 'commentB', 'user_id': 'u1', 'ig_comment_id': 'cB',
             'instagramAccountDbId': 'accB', 'instagramAccountId': 'igB',
             'created': datetime.utcnow()},
        ],
        conversations=[],
    )


def test_instagram_accounts_marks_server_side_active_and_hides_tokens(monkeypatch):
    fake_db = _multi_account_db()
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.instagram_accounts(user_id='u1'))

    assert result['activeInstagramAccountId'] == 'accA'
    assert result['count'] == 2
    assert [row['id'] for row in result['accounts'] if row['active']] == ['accA']
    assert 'token-a' not in str(result)
    assert 'accessToken' not in str(result)


def test_instagram_account_activate_switches_active_account(monkeypatch):
    fake_db = _multi_account_db()
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.instagram_account_activate('accB', user_id='u1'))
    user = _run(fake_db.users.find_one({'id': 'u1'}))

    assert result['account']['id'] == 'accB'
    assert result['account']['active'] is True
    assert user['active_instagram_account_id'] == 'accB'
    assert user['ig_user_id'] == 'igB'
    assert user['meta_access_token'] == 'token-b'
    assert 'token-b' not in str(result)


def test_instagram_account_activate_rejects_other_user_account(monkeypatch):
    fake_db = _multi_account_db()
    monkeypatch.setattr(server, 'db', fake_db)

    try:
        _run(server.instagram_account_activate('accOther', user_id='u1'))
    except HTTPException as exc:
        assert exc.status_code == 404
    else:
        raise AssertionError('expected HTTPException')


def test_instagram_auth_url_uses_signed_add_account_state():
    result = _run(server.instagram_auth_url(
        mode='add_account',
        returnTo='/app',
        user_id='u1',
    ))
    query = parse_qs(urlparse(result['url']).query)
    state = query['state'][0]
    payload = server._verify_instagram_oauth_state(state)

    assert result['mode'] == 'add_account'
    assert result['returnTo'] == '/app'
    assert query['client_id'][0] == server.IG_APP_ID
    assert query['force_authentication'][0] == '1'
    assert payload['userId'] == 'u1'
    assert payload['mode'] == 'add_account'
    assert payload['returnTo'] == '/app'
    assert 'u1' not in state


def _oauth_success_responses(ig_id='igB', username='account_b', token='short-b', long_token='long-b'):
    return [
        FakeResponse(200, {'access_token': token, 'user_id': ig_id}),
        FakeResponse(200, {'user_id': ig_id, 'username': username}),
        FakeResponse(200, {'data': {'app_id': server.IG_APP_ID, 'is_valid': True, 'scopes': []}}),
        FakeResponse(200, {'access_token': long_token, 'expires_in': 60 * 60 * 24 * 60}),
        FakeResponse(200, {'user_id': ig_id, 'username': username}),
    ]


def test_instagram_callback_creates_second_account_and_sets_it_active(monkeypatch):
    fake_db = _multi_account_db()
    monkeypatch.setattr(server, 'db', fake_db)
    fake_client = FakeAsyncClient(_oauth_success_responses())
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: fake_client)
    state = server._sign_instagram_oauth_state({
        'userId': 'u1',
        'mode': 'add_account',
        'returnTo': '/app',
    })

    response = _run(server.instagram_callback(
        _request(), code='code-b', state=state, error=None, error_description=None,
    ))
    user = _run(fake_db.users.find_one({'id': 'u1'}))
    accounts = [doc for doc in fake_db.instagram_accounts.docs if doc.get('userId') == 'u1']
    account_b = _run(fake_db.instagram_accounts.find_one({
        'userId': 'u1',
        'instagramAccountId': 'igB',
    }))

    assert response.status_code == 307
    assert '/app?ig=connected' in response.headers['location']
    assert len(accounts) == 2
    assert account_b['accessToken'] == 'long-b'
    assert user['active_instagram_account_id'] == account_b['id']
    assert user['ig_user_id'] == 'igB'
    assert user['meta_access_token'] == 'long-b'
    public = _run(server.instagram_accounts(user_id='u1'))
    assert [row['id'] for row in public['accounts'] if row['active']] == [account_b['id']]
    assert 'long-b' not in str(public)


def test_instagram_callback_updates_existing_account_without_duplicate(monkeypatch):
    fake_db = _multi_account_db()
    monkeypatch.setattr(server, 'db', fake_db)
    fake_client = FakeAsyncClient(_oauth_success_responses(
        ig_id='igA',
        username='account_a_new',
        token='short-a-new',
        long_token='long-a-new',
    ))
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: fake_client)
    state = server._sign_instagram_oauth_state({
        'userId': 'u1',
        'mode': 'add_account',
        'returnTo': '/app',
    })

    _run(server.instagram_callback(
        _request(), code='code-a', state=state, error=None, error_description=None,
    ))
    accounts = [doc for doc in fake_db.instagram_accounts.docs if doc.get('userId') == 'u1']
    account_a = _run(fake_db.instagram_accounts.find_one({'id': 'accA'}))
    user = _run(fake_db.users.find_one({'id': 'u1'}))

    assert len(accounts) == 2
    assert account_a['accessToken'] == 'long-a-new'
    assert account_a['username'] == 'account_a_new'
    assert user['active_instagram_account_id'] == 'accA'
    assert user['meta_access_token'] == 'long-a-new'


def test_dashboard_stats_filter_by_active_instagram_account(monkeypatch):
    fake_db = _multi_account_db()
    monkeypatch.setattr(server, 'db', fake_db)

    before = _run(server.dashboard_stats(user_id='u1'))
    _run(server.instagram_account_activate('accB', user_id='u1'))
    after = _run(server.dashboard_stats(user_id='u1'))

    assert before['activeInstagramAccountId'] == 'accA'
    assert after['activeInstagramAccountId'] == 'accB'
    assert before['active_automations'] == 1
    assert after['active_automations'] == 1


def test_dashboard_stats_counts_active_account_contacts_messages_and_weekly_rows(monkeypatch):
    now = datetime.utcnow()
    acc_a = _account(id='accA', userId='u1', instagramAccountId='igA',
                     igUserId='igA', username='account_a', accessToken='token-a')
    acc_b = _account(id='accB', userId='u1', instagramAccountId='igB',
                     igUserId='igB', username='account_b', accessToken='token-b')
    fake_db = FakeDB(
        [acc_a, acc_b],
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        automations=[
            {'id': 'autoA', 'user_id': 'u1', 'status': 'active',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'sent': 99, 'updated': now},
            {'id': 'pausedA', 'user_id': 'u1', 'status': 'paused',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'sent': 0, 'updated': now},
            {'id': 'autoB', 'user_id': 'u1', 'status': 'active',
             'instagramAccountDbId': 'accB', 'instagramAccountId': 'igB',
             'sent': 88, 'updated': now},
        ],
        comments=[
            {'id': 'commentA1', 'user_id': 'u1', 'commenter_id': 'contact1',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'replied': True, 'action_status': 'success', 'created': now},
            {'id': 'commentA2', 'user_id': 'u1', 'commenter_id': 'contact2',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'replied': False, 'action_status': 'success', 'created': now},
            {'id': 'commentB', 'user_id': 'u1', 'commenter_id': 'contactB',
             'instagramAccountDbId': 'accB', 'instagramAccountId': 'igB',
             'replied': True, 'created': now},
            {'id': 'oldGlobal', 'user_id': 'u1', 'commenter_id': 'global',
             'replied': True, 'created': now},
        ],
        dm_logs=[
            {'id': 'dmA1', 'user_id': 'u1', 'sender_id': 'contact1',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'status': 'replied', 'created': now},
            {'id': 'dmA2', 'user_id': 'u1', 'sender_id': 'contact2',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'status': 'failed', 'created': now},
            {'id': 'dmA3', 'user_id': 'u1', 'sender_id': 'contact2',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'status': 'sent', 'created': now},
            {'id': 'dmA4', 'user_id': 'u1', 'sender_id': 'contact2',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'status': 'queued', 'created': now},
            {'id': 'dmB', 'user_id': 'u1', 'sender_id': 'contactB',
             'instagramAccountDbId': 'accB', 'instagramAccountId': 'igB',
             'status': 'replied', 'created': now},
        ],
        comment_dm_sessions=[
            {'id': 'sessionA', 'user_id': 'u1', 'recipient_id': 'contact4',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'status': 'completed', 'completedAt': now, 'created': now},
        ],
        conversations=[
            {'id': 'convA', 'user_id': 'u1',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'contact': {'ig_id': 'contact5'},
             'messages': [
                 {'from': 'me', 'created': now, 'delivered': True},
                 {'from': 'contact', 'created': now},
                 {'from': 'me', 'created': now, 'delivered': False},
             ],
             'created': now},
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_stats(user_id='u1'))
    today = now.date().isoformat()

    assert result['instagram']['activeAccountId'] == 'accA'
    assert result['instagram']['instagramAccountId'] == 'igA'
    assert result['activeAutomations'] == 1
    assert result['active_automations'] == 1
    assert result['totalContacts'] == 4
    assert result['messagesSent'] == 6
    assert result['conversionRate'] == 0
    assert len(result['weeklyPerformance']) == 7
    assert all({'day', 'date', 'messages', 'conversions'} <= set(row) for row in result['weeklyPerformance'])
    assert next(row for row in result['weeklyPerformance'] if row['date'] == today)['messages'] == 6
    assert 'token-a' not in str(result)
    assert 'accessToken' not in str(result)


def test_dashboard_messages_sent_isolated_between_instagram_accounts(monkeypatch):
    now = datetime.utcnow()
    acc_a = _account(id='accA', userId='u1', instagramAccountId='igA',
                     igUserId='igA', username='account_a', accessToken='token-a')
    acc_b = _account(id='accB', userId='u1', instagramAccountId='igB',
                     igUserId='igB', username='account_b', accessToken='token-b')
    fake_db = FakeDB(
        [acc_a, acc_b],
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        automations=[
            {'id': 'autoA', 'user_id': 'u1', 'status': 'active',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'sent': 500, 'updated': now},
            {'id': 'autoB', 'user_id': 'u1', 'status': 'active',
             'instagramAccountDbId': 'accB', 'instagramAccountId': 'igB',
             'sent': 500, 'updated': now},
        ],
        comments=[
            {'id': 'commentA1', 'user_id': 'u1', 'commenter_id': 'contact1',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'replied': True, 'created': now},
            {'id': 'commentA2', 'user_id': 'u1', 'commenter_id': 'contact2',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'action_status': 'success', 'created': now},
            {'id': 'oldUnscopedComment', 'user_id': 'u1', 'commenter_id': 'old',
             'replied': True, 'created': now},
            {'id': 'commentBFailed', 'user_id': 'u1', 'commenter_id': 'contactB',
             'instagramAccountDbId': 'accB', 'instagramAccountId': 'igB',
             'action_status': 'failed', 'created': now},
        ],
        dm_logs=[
            {'id': 'dmA1', 'user_id': 'u1', 'sender_id': 'contact1',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'status': 'replied', 'created': now},
            {'id': 'dmA2', 'user_id': 'u1', 'sender_id': 'contact2',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'status': 'sent', 'created': now},
            {'id': 'dmAQueued', 'user_id': 'u1', 'sender_id': 'contact2',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'status': 'queued', 'created': now},
            {'id': 'dmBQueued', 'user_id': 'u1', 'sender_id': 'contactB',
             'instagramAccountDbId': 'accB', 'instagramAccountId': 'igB',
             'status': 'queued', 'created': now},
            {'id': 'oldUnscopedDm', 'user_id': 'u1', 'sender_id': 'old',
             'status': 'replied', 'created': now},
        ],
        comment_dm_sessions=[
            {'id': 'sessionA', 'user_id': 'u1', 'recipient_id': 'contact3',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'status': 'completed', 'completedAt': now, 'created': now},
            {'id': 'sessionBPending', 'user_id': 'u1', 'recipient_id': 'contactB',
             'instagramAccountDbId': 'accB', 'instagramAccountId': 'igB',
             'status': 'pending', 'created': now},
        ],
        conversations=[
            {'id': 'convA', 'user_id': 'u1',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'contact': {'ig_id': 'contact4'},
             'messages': [
                 {'from': 'me', 'status': 'sent', 'created': now},
                 {'from': 'me', 'status': 'failed', 'created': now},
             ],
             'created': now},
            {'id': 'convB', 'user_id': 'u1',
             'instagramAccountDbId': 'accB', 'instagramAccountId': 'igB',
             'contact': {'ig_id': 'contactB'},
             'messages': [
                 {'from': 'me', 'status': 'failed', 'created': now},
             ],
             'created': now},
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result_a = _run(server.dashboard_stats(user_id='u1'))
    _run(server.instagram_account_activate('accB', user_id='u1'))
    result_b = _run(server.dashboard_stats(user_id='u1'))
    today = now.date().isoformat()

    assert result_a['activeInstagramAccountId'] == 'accA'
    assert result_a['messagesSent'] == 6
    assert next(row for row in result_a['weeklyPerformance'] if row['date'] == today)['messages'] == 6
    assert result_a['totalContacts'] == 4

    assert result_b['activeInstagramAccountId'] == 'accB'
    assert result_b['messagesSent'] == 0
    assert next(row for row in result_b['weeklyPerformance'] if row['date'] == today)['messages'] == 0
    assert result_b['totalContacts'] == 1
    assert 'token-a' not in str(result_a)
    assert 'token-b' not in str(result_b)
    assert 'accessToken' not in str(result_a)
    assert 'accessToken' not in str(result_b)


def test_dashboard_stats_includes_unscoped_old_records_only_for_single_account(monkeypatch):
    now = datetime.utcnow()
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        automations=[
            {'id': 'oldAuto', 'user_id': 'u1', 'status': 'active',
             'sent': 2, 'updated': now},
        ],
        comments=[
            {'id': 'oldComment', 'user_id': 'u1', 'commenter_id': 'oldContact',
             'replied': True, 'created': now},
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_stats(user_id='u1'))

    assert result['activeAutomations'] == 1
    assert result['totalContacts'] == 1
    assert result['messagesSent'] == 2


def test_dashboard_conversion_rate_uses_unique_tracked_click_users(monkeypatch):
    now = datetime.utcnow()
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        comments=[
            {'id': 'comment1', 'user_id': 'u1', 'commenter_id': 'contact1',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'created': now},
            {'id': 'comment2', 'user_id': 'u1', 'commenter_id': 'contact2',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'created': now},
        ],
        link_click_events=[
            {'id': 'click1', 'user_id': 'u1', 'instagramUserId': 'contact1',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'clickedAt': now, 'created': now},
            {'id': 'click2', 'user_id': 'u1', 'instagramUserId': 'contact1',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'clickedAt': now, 'created': now},
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_stats(user_id='u1'))
    today = now.date().isoformat()

    assert result['totalContacts'] == 2
    assert result['convertedContacts'] == 1
    assert result['linkClicks'] == 2
    assert result['conversionRate'] == 50.0
    assert next(row for row in result['weeklyPerformance'] if row['date'] == today)['conversions'] == 1
    assert result['conversionTrackingImplemented'] is True
    assert 'accessToken' not in str(result)


def test_dashboard_conversions_are_scoped_by_active_instagram_account(monkeypatch):
    now = datetime.utcnow()
    acc_a = _account(id='accA', userId='u1', instagramAccountId='igA',
                     igUserId='igA', username='account_a', accessToken='token-a')
    acc_b = _account(id='accB', userId='u1', instagramAccountId='igB',
                     igUserId='igB', username='account_b', accessToken='token-b')
    fake_db = FakeDB(
        [acc_a, acc_b],
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        comments=[
            {'id': 'commentA', 'user_id': 'u1', 'commenter_id': 'contactA',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'created': now},
            {'id': 'commentB', 'user_id': 'u1', 'commenter_id': 'contactB',
             'instagramAccountDbId': 'accB', 'instagramAccountId': 'igB',
             'created': now},
        ],
        link_click_events=[
            {'id': 'clickA', 'user_id': 'u1', 'instagramUserId': 'contactA',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'clickedAt': now, 'created': now},
            {'id': 'clickB', 'user_id': 'u1', 'instagramUserId': 'contactB',
             'instagramAccountDbId': 'accB', 'instagramAccountId': 'igB',
             'clickedAt': now, 'created': now},
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result_a = _run(server.dashboard_stats(user_id='u1'))
    _run(server.instagram_account_activate('accB', user_id='u1'))
    result_b = _run(server.dashboard_stats(user_id='u1'))

    assert result_a['activeInstagramAccountId'] == 'accA'
    assert result_a['convertedContacts'] == 1
    assert result_a['conversionRate'] == 100.0
    assert result_b['activeInstagramAccountId'] == 'accB'
    assert result_b['convertedContacts'] == 1
    assert result_b['conversionRate'] == 100.0
    assert 'token-a' not in str(result_a)
    assert 'token-b' not in str(result_b)


def test_tracked_link_redirect_records_click_and_redirects(monkeypatch):
    now = datetime.utcnow()
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        tracked_links=[
            {'id': 'abc123', 'shortCode': 'abc123', 'user_id': 'u1', 'userId': 'u1',
             'instagramAccountDbId': 'accA', 'instagramAccountId': 'igA',
             'igUserId': 'igA', 'instagramUserId': 'contact1',
             'originalUrl': 'https://example.org/product', 'clicksCount': 0,
             'isActive': True, 'expiresAt': now + timedelta(days=5),
             'created': now, 'createdAt': now}
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    scope = {
        'type': 'http',
        'method': 'GET',
        'path': '/r/abc123',
        'headers': [(b'user-agent', b'pytest'), (b'referer', b'https://instagram.com/')],
        'client': ('203.0.113.10', 12345),
    }
    request = Request(scope, receive=lambda: None)

    response = _run(server.tracked_link_redirect('abc123', request))

    assert response.status_code == 302
    assert response.headers['location'] == 'https://example.org/product'
    assert fake_db.tracked_links.docs[0]['clicksCount'] == 1
    assert fake_db.tracked_links.docs[0]['firstClickedAt'] is not None
    assert len(fake_db.link_click_events.docs) == 1
    event = fake_db.link_click_events.docs[0]
    assert event['instagramAccountId'] == 'igA'
    assert event['instagramUserId'] == 'contact1'
    assert event['ipHash'] != '203.0.113.10'


def test_final_dm_link_is_replaced_with_tracking_url(monkeypatch):
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
    )
    monkeypatch.setattr(server, 'db', fake_db)
    sent = {}

    async def fake_send_url_button(_token, _ig_id, _recipient, text, _button, url):
        sent['text'] = text
        sent['url'] = url
        return {'ok': True, 'body': {'message_id': 'mid1'}}

    monkeypatch.setattr(server, 'send_ig_url_button', fake_send_url_button)
    user_doc = {
        'id': 'u1',
        'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA',
        'meta_access_token': 'token-a',
    }
    session = {
        'id': 'session1',
        'user_id': 'u1',
        'instagramAccountDbId': 'accA',
        'instagramAccountId': 'igA',
        'recipient_id': 'contact1',
        'automation_id': 'auto1',
        'ig_comment_id': 'comment1',
        'link_dm_text': 'Here is your product link',
        'link_button_text': 'Open link',
        'link_url': 'https://example.org/product',
        'conversionTrackingEnabled': True,
    }

    ok = _run(server._send_comment_dm_flow_completion(user_doc, session))

    assert ok is True
    assert sent['url'].startswith('https://example.com/r/')
    assert sent['url'] != 'https://example.org/product'
    assert fake_db.tracked_links.docs[0]['originalUrl'] == 'https://example.org/product'
    assert fake_db.tracked_links.docs[0]['isActive'] is True
    assert fake_db.tracked_links.docs[0]['relatedMessageId'] == 'mid1'


def test_follow_gate_sends_custom_prompt_before_final_link(monkeypatch):
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
    )
    monkeypatch.setattr(server, 'db', fake_db)
    sent = {'prompts': [], 'finals': []}

    async def fake_quick_reply(_token, _ig_id, _recipient, text, title, payload):
        sent['prompts'].append({'text': text, 'title': title, 'payload': payload})
        return {'ok': True}

    async def fake_send_url_button(*args):
        sent['finals'].append(args)
        return {'ok': True}

    monkeypatch.setattr(server, 'send_ig_quick_reply', fake_quick_reply)
    monkeypatch.setattr(server, 'send_ig_url_button', fake_send_url_button)
    user_doc = {'id': 'u1', 'active_instagram_account_id': 'accA',
                'ig_user_id': 'igA', 'meta_access_token': 'token-a'}
    session = {
        'id': 'session-follow',
        'user_id': 'u1',
        'instagramAccountDbId': 'accA',
        'instagramAccountId': 'igA',
        'recipient_id': 'contact1',
        'automation_id': 'auto1',
        'status': 'pending',
        'follow_request_enabled': True,
        'follow_request_message': 'Custom follow message',
        'follow_request_button_text': 'Following',
        'follow_confirmation_keywords': ['Following', 'I followed'],
        'link_dm_text': 'Here is the link',
        'link_button_text': 'Open link',
        'link_url': 'https://example.org/product',
    }

    ok = _run(server._send_comment_dm_flow_completion(user_doc, session))

    assert ok is True
    assert sent['prompts'][0]['text'] == 'Custom follow message'
    assert sent['prompts'][0]['title'] == 'Following'
    assert sent['prompts'][0]['payload'].endswith(':followed')
    assert sent['finals'] == []


def test_follow_gate_confirmation_match_uses_custom_button_and_keywords():
    session = {
        'follow_request_enabled': True,
        'follow_request_button_text': 'تمت المتابعة',
        'follow_confirmation_keywords': ['Following', 'تابعت'],
        'follow_payload': 'comment_flow:session-follow:followed',
    }

    assert server._comment_dm_follow_confirmation_matches(session, text='تمت المتابعة') is True
    assert server._comment_dm_follow_confirmation_matches(session, text='تابعت') is True
    assert server._comment_dm_follow_confirmation_matches(
        session, payload='comment_flow:session-follow:followed'
    ) is True
    assert server._comment_dm_follow_confirmation_matches(session, text='hello') is False


def test_comment_matcher_reply_all_matches_unicode_and_emoji_comments():
    rule = {'id': 'autoAny', 'trigger': 'comment:any', 'match': 'any'}
    for text in ['Link', 'url', '🔥👏', '🙌🙌', 'محتاج التفاصيل', '؟؟', '👌', 'Price', 'لينك']:
        result = server.matchesAutomationRule(rule, text)
        assert result['matches'] is True, text

    empty = server.matchesAutomationRule(rule, '   ')
    assert empty['matches'] is False
    assert empty['reason'] == 'skipped_empty_comment'


def test_comment_matcher_keyword_remains_specific_and_unicode_safe():
    english = {'id': 'autoKeyword', 'trigger': 'comment:any', 'match': 'keyword', 'keywords': ['Price']}
    arabic = {'id': 'autoArabic', 'trigger': 'comment:any', 'match': 'keyword', 'keyword': 'لينك'}

    assert server.matchesAutomationRule(english, 'price please')['matches'] is True
    assert server.matchesAutomationRule(arabic, 'ابعت لينك')['matches'] is True
    assert server.matchesAutomationRule(english, '🔥👏')['matches'] is False


def test_handle_new_comment_reply_all_replies_to_emoji_comment(monkeypatch):
    now = datetime.utcnow()
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        automations=[
            {
                'id': 'autoAny',
                'user_id': 'u1',
                'status': 'active',
                'trigger': 'comment:any',
                'match': 'any',
                'instagramAccountDbId': 'accA',
                'instagramAccountId': 'igA',
                'activationStartedAt': now - timedelta(minutes=5),
                'nodes': [
                    {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
                    {'id': 'n_reply', 'type': 'reply_comment',
                     'data': {'text': 'Thanks for your comment'}},
                ],
                'edges': [{'source': 'n_trigger', 'target': 'n_reply'}],
            }
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ws_manager', SimpleNamespace(send=lambda *_args, **_kwargs: asyncio.sleep(0)))
    replies = []

    async def fake_reply(_token, ig_comment_id, text):
        replies.append((ig_comment_id, text))
        return True

    monkeypatch.setattr(server, 'reply_to_ig_comment', fake_reply)
    user_doc = {
        'id': 'u1',
        'email': 'u1@example.com',
        'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA',
        'meta_access_token': 'token-a',
        'instagramHandle': 'account_a',
    }

    result = _run(server._handle_new_comment(user_doc, {
        'ig_comment_id': 'commentEmoji',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'commenter_username': 'contact',
        'text': '🔥👏',
        'timestamp': now,
    }, source='polling'))

    assert result['matched'] is True
    assert result['action_status'] == 'success'
    assert replies == [('commentEmoji', 'Thanks for your comment')]
    assert fake_db.comments.docs[0]['replied'] is True
    assert fake_db.comments.docs[0]['action_status'] == 'success'


def test_pre_rule_comment_skipped_even_if_legacy_process_existing_true(monkeypatch):
    now = datetime.utcnow()
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        automations=[
            {
                'id': 'autoAny',
                'user_id': 'u1',
                'status': 'active',
                'trigger': 'comment:any',
                'match': 'any',
                'instagramAccountDbId': 'accA',
                'instagramAccountId': 'igA',
                'processExistingComments': True,
                'process_existing_unreplied_comments': True,
                'activationStartedAt': now,
                'nodes': [
                    {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
                    {'id': 'n_reply', 'type': 'reply_comment',
                     'data': {'text': 'Should not send'}},
                ],
                'edges': [{'source': 'n_trigger', 'target': 'n_reply'}],
            }
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ws_manager', SimpleNamespace(send=lambda *_args, **_kwargs: asyncio.sleep(0)))

    async def fake_reply(*_args, **_kwargs):
        raise AssertionError('pre-rule historical comments must not be replied to')

    monkeypatch.setattr(server, 'reply_to_ig_comment', fake_reply)
    user_doc = {
        'id': 'u1',
        'email': 'u1@example.com',
        'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA',
        'meta_access_token': 'token-a',
        'instagramHandle': 'account_a',
    }

    result = _run(server._handle_new_comment(user_doc, {
        'ig_comment_id': 'oldComment',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'commenter_username': 'contact',
        'text': 'Link',
        'timestamp': now - timedelta(minutes=10),
    }, source='polling'))

    assert result['matched'] is False
    assert result['action_status'] == 'skipped'
    assert result['rule_id'] == 'autoAny'
    assert fake_db.comments.docs[0]['skipReason'] == 'historical_before_rule_activation'
    assert fake_db.comments.docs[0]['processExistingComments'] is False
    assert fake_db.comments.docs[0]['replied'] is False


def test_pre_rule_comment_on_specific_media_processed_when_catchup_enabled(monkeypatch):
    now = datetime.utcnow()
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        automations=[
            {
                'id': 'autoSpecific',
                'user_id': 'u1',
                'status': 'active',
                'trigger': 'comment:media1',
                'post_scope': 'specific',
                'media_id': 'media1',
                'match': 'any',
                'instagramAccountDbId': 'accA',
                'instagramAccountId': 'igA',
                'processExistingComments': True,
                'process_existing_unreplied_comments': True,
                'activationStartedAt': now,
                'nodes': [
                    {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
                    {'id': 'n_reply', 'type': 'reply_comment',
                     'data': {'text': 'Catch-up reply'}},
                ],
                'edges': [{'source': 'n_trigger', 'target': 'n_reply'}],
            }
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ws_manager', SimpleNamespace(send=lambda *_args, **_kwargs: asyncio.sleep(0)))
    replies = []

    async def fake_reply(_token, ig_comment_id, text):
        replies.append((ig_comment_id, text))
        return True

    monkeypatch.setattr(server, 'reply_to_ig_comment', fake_reply)
    user_doc = {
        'id': 'u1',
        'email': 'u1@example.com',
        'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA',
        'meta_access_token': 'token-a',
        'instagramHandle': 'account_a',
    }

    result = _run(server._handle_new_comment(user_doc, {
        'ig_comment_id': 'oldSpecificComment',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'commenter_username': 'contact',
        'text': 'Link',
        'timestamp': now - timedelta(minutes=10),
    }, source='manual_catchup'))

    assert result['matched'] is True
    assert result['action_status'] == 'success'
    assert replies == [('oldSpecificComment', 'Catch-up reply')]
    assert fake_db.comments.docs[0]['processExistingComments'] is True


def test_post_activation_missed_comment_is_processed_with_legacy_flag_true(monkeypatch):
    now = datetime.utcnow()
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        automations=[
            {
                'id': 'autoAny',
                'user_id': 'u1',
                'status': 'active',
                'trigger': 'comment:any',
                'match': 'any',
                'instagramAccountDbId': 'accA',
                'instagramAccountId': 'igA',
                'processExistingComments': True,
                'process_existing_unreplied_comments': True,
                'activationStartedAt': now - timedelta(minutes=10),
                'nodes': [
                    {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
                    {'id': 'n_reply', 'type': 'reply_comment',
                     'data': {'text': 'Post activation reply'}},
                ],
                'edges': [{'source': 'n_trigger', 'target': 'n_reply'}],
            }
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ws_manager', SimpleNamespace(send=lambda *_args, **_kwargs: asyncio.sleep(0)))
    replies = []

    async def fake_reply(_token, ig_comment_id, text):
        replies.append((ig_comment_id, text))
        return True

    monkeypatch.setattr(server, 'reply_to_ig_comment', fake_reply)
    user_doc = {
        'id': 'u1',
        'email': 'u1@example.com',
        'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA',
        'meta_access_token': 'token-a',
        'instagramHandle': 'account_a',
    }

    result = _run(server._handle_new_comment(user_doc, {
        'ig_comment_id': 'newMissedComment',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'commenter_username': 'contact',
        'text': 'ðŸ”¥ðŸ‘',
        'timestamp': now,
    }, source='polling'))

    assert result['matched'] is True
    assert result['action_status'] == 'success'
    assert replies == [('newMissedComment', 'Post activation reply')]
    assert fake_db.comments.docs[0]['processExistingComments'] is False


def test_polling_enforces_safe_reply_cap(monkeypatch):
    now = datetime.utcnow()
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        automations=[
            {
                'id': 'autoAny',
                'user_id': 'u1',
                'status': 'active',
                'trigger': 'comment:media1',
                'match': 'any',
                'instagramAccountDbId': 'accA',
                'instagramAccountId': 'igA',
                'activationStartedAt': now - timedelta(minutes=10),
            }
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'IG_POLL_REPLY_CAP_PER_RUN', 3)
    monkeypatch.setattr(server, 'IG_POLL_COMMENT_BATCH_LIMIT', 20)
    handled = []
    queued = []

    async def fake_handle(_user_doc, comment_data, source='polling'):
        handled.append(comment_data['ig_comment_id'])
        if comment_data.get('force_queue'):
            queued.append(comment_data['ig_comment_id'])
            return {'processed': True, 'matched': True, 'action_status': 'pending', 'queued': True}
        return {'processed': True, 'matched': True, 'action_status': 'success'}

    class PollClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def get(self, _url, params=None):
            comments = [
                {
                    'id': f'comment{i}',
                    'text': 'Link',
                    'timestamp': now.isoformat(),
                    'from': {'id': f'contact{i}', 'username': f'contact{i}'},
                }
                for i in range(8)
            ]
            return FakeResponse(200, {'data': comments})

    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=20: PollClient())

    stats = _run(server._poll_user_comments({
        'id': 'u1',
        'email': 'u1@example.com',
        'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA',
        'meta_access_token': 'token-a',
    }))

    assert handled == [f'comment{i}' for i in range(8)]
    assert queued == ['comment3', 'comment4', 'comment5', 'comment6', 'comment7']
    assert stats['actionsSucceeded'] == 3
    assert 'reply_cap_reached' in stats['errors']


def test_process_unreplied_endpoint_fetches_only_selected_media_rules(monkeypatch):
    now = datetime.utcnow()
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        automations=[
            {
                'id': 'broad',
                'user_id': 'u1',
                'status': 'active',
                'trigger': 'comment:any',
                'post_scope': 'any',
                'instagramAccountDbId': 'accA',
                'instagramAccountId': 'igA',
                'process_existing_unreplied_comments': True,
            },
            {
                'id': 'specific',
                'user_id': 'u1',
                'status': 'active',
                'trigger': 'comment:media1',
                'post_scope': 'specific',
                'media_id': 'media1',
                'instagramAccountDbId': 'accA',
                'instagramAccountId': 'igA',
                'process_existing_unreplied_comments': True,
                'activationStartedAt': now,
            },
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    handled = []

    async def fake_handle(_user_doc, comment_data, source='manual_catchup'):
        handled.append((comment_data['media_id'], comment_data['ig_comment_id'], source))
        return {'processed': True, 'matched': True, 'action_status': 'success'}

    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)
    client = FakeAsyncClient([
        FakeResponse(200, {'data': [
            {'id': 'c1', 'text': 'old', 'username': 'fan',
             'timestamp': (now - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S+0000')},
        ]})
    ])
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda **_kwargs: client)

    summary = _run(server.instagram_process_unreplied_comments(user_id='u1'))

    assert summary['media_ids'] == ['media1']
    assert summary['skipped_broad_scope'] == 1
    assert summary['checked'] == 1
    assert summary['replied'] == 1
    assert handled == [('media1', 'c1', 'manual_catchup')]
    assert client.get_calls[0][0][0].endswith('/media1/comments')


def test_handle_new_comment_retries_existing_failed_unreplied_comment(monkeypatch):
    now = datetime.utcnow()
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        automations=[
            {
                'id': 'autoAny',
                'user_id': 'u1',
                'status': 'active',
                'trigger': 'comment:any',
                'match': 'any',
                'instagramAccountDbId': 'accA',
                'instagramAccountId': 'igA',
                'activationStartedAt': now - timedelta(minutes=5),
                'nodes': [
                    {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
                    {'id': 'n_reply', 'type': 'reply_comment',
                     'data': {'text': 'Retry worked'}},
                ],
                'edges': [{'source': 'n_trigger', 'target': 'n_reply'}],
            }
        ],
        comments=[
            {
                'id': 'existingComment',
                'user_id': 'u1',
                'ig_comment_id': 'commentRetry',
                'instagramAccountId': 'igA',
                'igUserId': 'igA',
                'commenter_id': 'contact1',
                'text': 'url',
                'replied': False,
                'action_status': 'failed',
                'created': now - timedelta(minutes=1),
            }
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ws_manager', SimpleNamespace(send=lambda *_args, **_kwargs: asyncio.sleep(0)))

    async def fake_reply(_token, _ig_comment_id, _text):
        return True

    monkeypatch.setattr(server, 'reply_to_ig_comment', fake_reply)
    user_doc = {
        'id': 'u1',
        'email': 'u1@example.com',
        'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA',
        'meta_access_token': 'token-a',
        'instagramHandle': 'account_a',
    }

    result = _run(server._handle_new_comment(user_doc, {
        'ig_comment_id': 'commentRetry',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'commenter_username': 'contact',
        'text': 'url',
        'timestamp': now,
    }, source='polling'))

    assert result['reprocessed'] is True
    assert result['matched'] is True
    assert result['action_status'] == 'success'
    assert len(fake_db.comments.docs) == 1
    assert fake_db.comments.docs[0]['replied'] is True


def test_rules_and_comment_logs_filter_by_active_instagram_account(monkeypatch):
    fake_db = _multi_account_db()
    monkeypatch.setattr(server, 'db', fake_db)

    rules_a = _run(server.list_automations(user_id='u1'))
    comments_a = _run(server.list_comments(user_id='u1', limit=50, page=1, unreplied=False))
    _run(server.instagram_account_activate('accB', user_id='u1'))
    rules_b = _run(server.list_automations(user_id='u1'))
    comments_b = _run(server.list_comments(user_id='u1', limit=50, page=1, unreplied=False))

    assert [r['id'] for r in rules_a] == ['autoA']
    assert [r['id'] for r in rules_b] == ['autoB']
    assert [c['id'] for c in comments_a['comments']] == ['commentA']
    assert [c['id'] for c in comments_b['comments']] == ['commentB']


def test_webhook_mapping_uses_target_instagram_account_not_active_account(monkeypatch):
    fake_db = _multi_account_db()
    monkeypatch.setattr(server, 'db', fake_db)

    user_doc, via = _run(server._find_user_doc_for_instagram_account_id('igB'))

    assert via == 'instagram_accounts'
    assert user_doc['id'] == 'u1'
    assert user_doc['active_instagram_account_id'] == 'accB'
    assert user_doc['ig_user_id'] == 'igB'
    assert user_doc['meta_access_token'] == 'token-b'
