import asyncio
import os

import pytest

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://example.com')
os.environ.setdefault('FRONTEND_URL', 'https://example.com')

import server


@pytest.mark.parametrize('kind', ['password_reset', 'email_verification'])
def test_auth_routes_use_resend_without_webhook_fallback(monkeypatch, kind):
    calls = []
    monkeypatch.setattr(server, 'resend_configured', lambda: True)
    monkeypatch.setattr(server, 'EMAIL_VERIFICATION_WEBHOOK_URL', 'https://legacy.example.com')

    async def fake_send(**kwargs):
        calls.append(kwargs)
        return False

    monkeypatch.setattr(server, 'send_auth_email', fake_send)
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda **kwargs: pytest.fail('Legacy fallback used'))
    delivery = server._deliver_password_reset if kind == 'password_reset' else server._deliver_email_verification
    assert asyncio.run(delivery({'email': 'Owner@Example.com'}, 'safe-test-token')) is False
    assert calls[0]['kind'] == kind
    assert calls[0]['recipient'] == 'owner@example.com'
    assert calls[0]['link'].endswith('?token=safe-test-token')
    assert server._email_verification_delivery_configured()


def test_missing_resend_keeps_legacy_readiness(monkeypatch):
    monkeypatch.setattr(server, 'resend_configured', lambda: False)
    monkeypatch.setattr(server, 'EMAIL_VERIFICATION_WEBHOOK_URL', '')
    assert not server._email_verification_delivery_configured()
    monkeypatch.setattr(server, 'EMAIL_VERIFICATION_WEBHOOK_URL', 'https://legacy.example.com')
    assert server._email_verification_delivery_configured()


def test_email_verification_opens_frontend_not_api(monkeypatch):
    monkeypatch.setattr(server, 'FRONTEND_URL', 'https://mychaat.net')
    monkeypatch.setattr(server, 'BACKEND_PUBLIC_URL', 'https://old.example.com')
    assert server._email_verification_url('safe-token') == 'https://mychaat.net/verify-email?token=safe-token'


def test_legacy_get_link_redirects_without_consuming_token(monkeypatch):
    monkeypatch.setattr(server, 'FRONTEND_URL', 'https://mychaat.net')

    async def forbidden(*args):
        pytest.fail('Email scanners must not consume verification tokens')

    monkeypatch.setattr(server, '_verify_email_token_value', forbidden)
    response = asyncio.run(server.verify_email_get('safe-token'))
    assert response.status_code == 303
    assert response.headers['location'] == 'https://mychaat.net/verify-email?token=safe-token'
    assert response.headers['cache-control'] == 'no-store'
    assert response.headers['referrer-policy'] == 'no-referrer'
