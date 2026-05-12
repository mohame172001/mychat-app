import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://backend.example.com')
os.environ.setdefault('FRONTEND_URL', 'https://frontend.example.com')
os.environ.setdefault('IG_APP_ID', '123')
os.environ.setdefault('IG_APP_SECRET', 'secret')
os.environ.setdefault('CRON_SECRET', 'cron-secret')

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


class _FakeRequest:
    def __init__(self, body=b'', headers=None, client_host='1.2.3.4'):
        self._body = body
        self.headers = headers or {}
        self.client = SimpleNamespace(host=client_host)
        self.url = SimpleNamespace(scheme='https')

    async def body(self):
        return self._body


class _FakeDeletionRequests:
    def __init__(self):
        self.docs = []

    async def insert_one(self, doc):
        self.docs.append(dict(doc))
        return SimpleNamespace(inserted_id='request_1')


class _FakeDb:
    def __init__(self):
        self.data_deletion_requests = _FakeDeletionRequests()


def test_meta_data_deletion_callback_returns_confirmation_without_user_data(monkeypatch, caplog):
    fake_db = _FakeDb()
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'FRONTEND_URL', 'https://frontend.example.com')
    body = json.dumps({
        'signed_request': 'signed.payload.secret',
        'email': 'reviewer@example.com',
    }).encode('utf-8')
    request = _FakeRequest(body=body, headers={'content-type': 'application/json'})

    with caplog.at_level(logging.INFO, logger='mychat'):
        result = _run(server.meta_data_deletion_callback(request))

    assert result['confirmation_code'].startswith('mychat-del-')
    assert result['confirmation_code'] in result['url']
    assert result['url'].startswith('https://frontend.example.com/data-deletion')
    assert 'signed.payload.secret' not in caplog.text
    assert 'reviewer@example.com' not in caplog.text

    stored = fake_db.data_deletion_requests.docs[0]
    assert stored['signed_request_present'] is True
    assert stored['signed_request_sha256']
    assert stored['email_sha256']
    assert 'signed.payload.secret' not in str(stored)
    assert 'reviewer@example.com' not in str(stored)


def test_meta_data_deletion_callback_accepts_form_encoded_payload(monkeypatch):
    fake_db = _FakeDb()
    monkeypatch.setattr(server, 'db', fake_db)
    request = _FakeRequest(
        body=b'signed_request=form_payload&email=form%40example.com',
        headers={'content-type': 'application/x-www-form-urlencoded'},
    )

    result = _run(server.meta_data_deletion_callback(request))

    assert result['confirmation_code'].startswith('mychat-del-')
    stored = fake_db.data_deletion_requests.docs[0]
    assert stored['signed_request_present'] is True
    assert stored['signed_request_sha256']
    assert stored['email_sha256']


def test_meta_data_deletion_callback_verifies_signed_request_when_secret_configured(monkeypatch):
    fake_db = _FakeDb()
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'META_APP_SECRET', 'meta-secret')
    payload = base64.urlsafe_b64encode(b'{"user_id":"meta-user"}').decode().rstrip('=')
    sig = hmac.new(b'meta-secret', payload.encode('utf-8'), hashlib.sha256).digest()
    encoded_sig = base64.urlsafe_b64encode(sig).decode().rstrip('=')
    signed_request = f'{encoded_sig}.{payload}'
    body = json.dumps({'signed_request': signed_request}).encode('utf-8')
    request = _FakeRequest(body=body, headers={'content-type': 'application/json'})

    result = _run(server.meta_data_deletion_callback(request))

    assert result['confirmation_code'].startswith('mychat-del-')
    stored = fake_db.data_deletion_requests.docs[0]
    assert stored['signed_request_present'] is True
    assert stored['signed_request_valid'] is True
    assert signed_request not in str(stored)


def test_meta_data_deletion_callback_invalid_signed_request_is_non_destructive(monkeypatch):
    fake_db = _FakeDb()
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'META_APP_SECRET', 'meta-secret')
    body = json.dumps({
        'signed_request': 'bad.signature',
        'email': 'victim@example.com',
    }).encode('utf-8')
    request = _FakeRequest(body=body, headers={'content-type': 'application/json'})

    result = _run(server.meta_data_deletion_callback(request))

    assert result['confirmation_code'].startswith('mychat-del-')
    stored = fake_db.data_deletion_requests.docs[0]
    assert stored['signed_request_valid'] is False
    assert stored['status'] == 'received'
    assert 'victim@example.com' not in str(stored)
