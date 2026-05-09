import asyncio
import os
import sys
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
    return all(doc.get(k) == v for k, v in query.items())


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def find_one(self, query, *args, **kwargs):
        return next((doc for doc in self.docs if _match(doc, query)), None)


class FakeDB:
    def __init__(self, users=None):
        self.users = FakeCollection(users or [])


class FakeRequest:
    headers = {}
    client = SimpleNamespace(host='203.0.113.10')


def _user(user_id='u1', status='active'):
    return {'id': user_id, 'status': status, 'email': 'user@example.com'}


def test_existing_jwt_rejects_suspended_user(monkeypatch):
    monkeypatch.setattr(server, 'db', FakeDB(users=[_user(status='suspended')]))

    with pytest.raises(server.HTTPException) as exc:
        _run(server.get_current_active_user_id('u1'))

    assert exc.value.status_code == 403
    assert exc.value.detail == 'account_suspended'


def test_existing_jwt_rejects_deleted_user(monkeypatch):
    monkeypatch.setattr(server, 'db', FakeDB(users=[_user(status='deleted')]))

    with pytest.raises(server.HTTPException) as exc:
        _run(server.get_current_active_user_id('u1'))

    assert exc.value.status_code == 403
    assert exc.value.detail == 'account_deleted'


def test_existing_jwt_rejects_missing_user(monkeypatch):
    monkeypatch.setattr(server, 'db', FakeDB(users=[]))

    with pytest.raises(server.HTTPException) as exc:
        _run(server.get_current_active_user_id('missing'))

    assert exc.value.status_code == 401
    assert exc.value.detail == 'Invalid token'


def test_tracked_link_destination_allows_only_http_and_https():
    assert server._is_valid_original_url('https://example.com/path?x=1')
    assert server._is_valid_original_url('http://example.com/path')

    for unsafe in [
        'javascript:alert(1)',
        'data:text/html,<script>alert(1)</script>',
        'file:///etc/passwd',
        '//example.com/path',
        'https://',
        'not a url',
    ]:
        assert not server._is_valid_original_url(unsafe)


def test_retry_reply_rate_limit_short_circuits(monkeypatch):
    monkeypatch.setattr(server, '_rate_limited', lambda *args, **kwargs: True)

    with pytest.raises(server.HTTPException) as exc:
        _run(server.retry_comment_reply('comment_1', user_id='u1'))

    assert exc.value.status_code == 429


def test_instagram_connect_rate_limit_short_circuits(monkeypatch):
    monkeypatch.setattr(server, '_rate_limited', lambda *args, **kwargs: True)

    with pytest.raises(server.HTTPException) as exc:
        _run(server.instagram_auth_url(user_id='u1'))

    assert exc.value.status_code == 429


def test_data_deletion_callback_rate_limit_short_circuits(monkeypatch):
    monkeypatch.setattr(server, '_rate_limited', lambda *args, **kwargs: True)

    with pytest.raises(server.HTTPException) as exc:
        _run(server.meta_data_deletion_callback(FakeRequest()))

    assert exc.value.status_code == 429


def test_admin_heavy_endpoints_rate_limited_after_permission_check(monkeypatch):
    async def allow_admin(*args, **kwargs):
        return ({'id': 'admin', 'email': 'owner@example.com'}, 'owner')

    monkeypatch.setattr(server, '_require_admin_permission', allow_admin)
    monkeypatch.setattr(server, '_rate_limited', lambda *args, **kwargs: True)

    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_metrics_reconciliation(user_id='admin'))
    assert exc.value.status_code == 429

    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_backfill_instagram_usage_subjects(body={'dry_run': True}, user_id='admin'))
    assert exc.value.status_code == 429
