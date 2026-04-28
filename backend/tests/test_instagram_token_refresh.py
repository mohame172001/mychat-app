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
        self.dm_rules = FakeCollection(collections.get('dm_rules', []))
        self.dm_logs = FakeCollection(collections.get('dm_logs', []))


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


def test_rules_and_comment_logs_filter_by_active_instagram_account(monkeypatch):
    fake_db = _multi_account_db()
    monkeypatch.setattr(server, 'db', fake_db)

    rules_a = _run(server.list_automations(user_id='u1'))
    comments_a = _run(server.list_comments(user_id='u1'))
    _run(server.instagram_account_activate('accB', user_id='u1'))
    rules_b = _run(server.list_automations(user_id='u1'))
    comments_b = _run(server.list_comments(user_id='u1'))

    assert [r['id'] for r in rules_a] == ['autoA']
    assert [r['id'] for r in rules_b] == ['autoB']
    assert [c['id'] for c in comments_a] == ['commentA']
    assert [c['id'] for c in comments_b] == ['commentB']


def test_webhook_mapping_uses_target_instagram_account_not_active_account(monkeypatch):
    fake_db = _multi_account_db()
    monkeypatch.setattr(server, 'db', fake_db)

    user_doc, via = _run(server._find_user_doc_for_instagram_account_id('igB'))

    assert via == 'instagram_accounts'
    assert user_doc['id'] == 'u1'
    assert user_doc['active_instagram_account_id'] == 'accB'
    assert user_doc['ig_user_id'] == 'igB'
    assert user_doc['meta_access_token'] == 'token-b'
