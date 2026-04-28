import asyncio
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://example.com')
os.environ.setdefault('FRONTEND_URL', 'https://example.com')
os.environ.setdefault('IG_APP_ID', '123')
os.environ.setdefault('IG_APP_SECRET', 'secret')
os.environ.setdefault('CRON_SECRET', 'cron-secret')

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server  # noqa: E402


class FakeResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = str(body)

    def json(self):
        return self._body


class FakeAsyncClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def get(self, *_args, **_kwargs):
        self.calls += 1
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

    async def find_one_and_update(self, query, update, **_kwargs):
        doc = await self.find_one(query)
        if not doc:
            return None
        await self.update_one({'id': doc['id']}, update)
        return doc


class FakeDB:
    def __init__(self, account, user=None):
        self.instagram_accounts = FakeCollection([account] if account else [])
        self.users = FakeCollection([user] if user else [])


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


def _run(coro):
    return asyncio.run(coro)


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


def test_public_refresh_status_never_returns_access_token():
    row = server._token_refresh_public_row(_account(accessToken='secret-token'))

    assert 'accessToken' not in row
    assert 'secret-token' not in str(row)
