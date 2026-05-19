"""Phase 2.19 hardening tests.

Covers the new defensive limits + ops endpoints:
- /api/version returns the deployed sha + environment
- X-Request-Id is echoed on every response
- Client-supplied X-Request-Id is accepted only if it matches the
  whitelist; arbitrary characters get rejected and a fresh id minted
- Pydantic Field caps reject oversize inputs (signup with 200-char
  username / 1024-char password) with HTTP 422 before bcrypt runs
- /api/health stays publicly reachable
"""
import os
import re
import sys
from pathlib import Path

import pytest

# Test env must match what server.py expects at import-time.
os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
os.environ.setdefault('JWT_SECRET', 'test-secret-test-secret-test-secret-32chars')
os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://example.com')
os.environ.setdefault('FRONTEND_URL', 'https://example.com')
os.environ.setdefault('IG_APP_ID', '123')
os.environ.setdefault('IG_APP_SECRET', 'secret')
os.environ.setdefault('META_VERIFY_TOKEN', 'verify')
os.environ.setdefault('CRON_SECRET', 'cron-secret')

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    import server
    return TestClient(server.app)


def test_health_is_public(client):
    r = client.get('/api/health')
    assert r.status_code == 200
    assert r.json()['ok'] is True


def test_version_endpoint_returns_environment_and_sha(client):
    r = client.get('/api/version')
    assert r.status_code == 200
    body = r.json()
    assert body['ok'] is True
    assert body['service'] == 'mychat-backend'
    assert 'environment' in body
    assert 'build_sha' in body
    # 12-char prefix or 'unknown'
    assert body['build_sha'] == 'unknown' or len(body['build_sha']) <= 12


def test_response_carries_request_id(client):
    r = client.get('/api/health')
    rid = r.headers.get('x-request-id')
    assert rid, 'every response must have an X-Request-Id header'
    # default minted id is base64url, no slashes or pluses
    assert re.fullmatch(r'[A-Za-z0-9_-]+', rid)
    assert len(rid) >= 12


def test_client_supplied_request_id_is_echoed_when_whitelisted(client):
    r = client.get('/api/health', headers={'X-Request-Id': 'trace-abc-123'})
    assert r.headers.get('x-request-id') == 'trace-abc-123'


def test_client_supplied_request_id_is_rejected_when_malicious(client):
    # Newline injection attempt → server must mint a fresh id instead
    bad = 'evil\nlog-line-after'
    r = client.get('/api/health', headers={'X-Request-Id': bad})
    rid = r.headers.get('x-request-id')
    assert rid != bad
    assert re.fullmatch(r'[A-Za-z0-9_-]+', rid)


def test_client_supplied_request_id_too_long_is_rejected(client):
    long_id = 'a' * 200
    r = client.get('/api/health', headers={'X-Request-Id': long_id})
    rid = r.headers.get('x-request-id')
    assert rid != long_id
    assert len(rid) <= 64


def test_signup_rejects_oversize_username(client):
    r = client.post('/api/auth/signup', json={
        'username': 'a' * 200,
        'email': 'oversize@example.com',
        'password': 'goodpass123',
    })
    assert r.status_code == 422


def test_signup_rejects_oversize_password(client):
    r = client.post('/api/auth/signup', json={
        'username': 'ok_user',
        'email': 'oversize-pw@example.com',
        'password': 'p' * 200,
    })
    assert r.status_code == 422


def test_signup_rejects_oversize_email(client):
    r = client.post('/api/auth/signup', json={
        'username': 'ok_user',
        'email': 'a' * 250 + '@example.com',
        'password': 'goodpass123',
    })
    assert r.status_code == 422


def test_signup_rejects_too_short_password(client):
    r = client.post('/api/auth/signup', json={
        'username': 'ok_user',
        'email': 'shorty@example.com',
        'password': '1234567',  # 7 chars, below 8
    })
    assert r.status_code == 422


def test_forgot_password_rejects_oversize_email(client):
    r = client.post('/api/auth/forgot-password', json={
        'email': 'a' * 500 + '@example.com',
    })
    assert r.status_code == 422
