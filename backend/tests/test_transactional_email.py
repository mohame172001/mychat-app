import asyncio
import logging

import httpx
import pytest

import transactional_email as mail


@pytest.fixture(autouse=True)
def email_env(monkeypatch):
    monkeypatch.setenv('RESEND_API_KEY', 'test-sending-key')
    monkeypatch.setenv('AUTH_EMAIL_FROM', 'MyChaat <no-reply@mail.example.com>')
    monkeypatch.delenv('AUTH_EMAIL_REPLY_TO', raising=False)


def client_for(monkeypatch, status=200, body=None, error=None):
    calls = []

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, **kwargs):
            calls.append((url, kwargs))
            if error:
                raise error
            return httpx.Response(status, json=body if body is not None else {'id': 'mail-test-id'})

    monkeypatch.setattr(mail.httpx, 'AsyncClient', lambda **kwargs: Client())
    return calls


def send(**overrides):
    args = dict(recipient='owner@example.com', link='https://example.com/reset-password?token=secret-token',
                kind='password_reset', expires_in_minutes=60)
    args.update(overrides)
    return asyncio.run(mail.send_auth_email(**args))


@pytest.mark.parametrize('kind,subject', [
    ('password_reset', 'Reset your MyChaat password'),
    ('email_verification', 'Verify your MyChaat email'),
])
def test_accepted_email(monkeypatch, caplog, kind, subject):
    calls = client_for(monkeypatch)
    with caplog.at_level(logging.INFO, logger='mychat'):
        assert send(kind=kind)
    url, args = calls[0]
    assert url == 'https://api.resend.com/emails'
    assert args['headers']['Authorization'] == 'Bearer test-sending-key'
    assert args['json']['subject'] == subject
    assert args['json']['to'] == ['owner@example.com']
    assert args['json']['from'] == 'MyChaat <no-reply@mail.example.com>'
    assert 'secret-token' in args['json']['text']
    assert 'expires in 60 minutes' in args['json']['html']
    for secret in ['test-sending-key', 'secret-token', 'owner@example.com']:
        assert secret not in caplog.text
    assert 'auth_email_accepted' in caplog.text


@pytest.mark.parametrize('status,body', [(400, {'error': 'secret-token'}), (429, {}),
                                      (500, {}), (200, {}), (200, [])])
def test_failure_does_not_claim_delivery(monkeypatch, caplog, status, body):
    calls = client_for(monkeypatch, status=status, body=body)
    assert not send()
    assert len(calls) == 1
    assert 'secret-token' not in caplog.text
    assert 'auth_email_failed' in caplog.text


def test_timeout_is_redacted(monkeypatch, caplog):
    client_for(monkeypatch, error=httpx.ReadTimeout('secret-token owner@example.com'))
    assert not send()
    assert 'ReadTimeout' in caplog.text
    assert 'secret-token' not in caplog.text
    assert 'owner@example.com' not in caplog.text


@pytest.mark.parametrize('missing', ['RESEND_API_KEY', 'AUTH_EMAIL_FROM'])
def test_partial_configuration_does_not_send(monkeypatch, missing):
    monkeypatch.delenv(missing)
    calls = client_for(monkeypatch)
    assert not mail.resend_configured()
    assert not send()
    assert not calls


def test_html_link_is_escaped(monkeypatch):
    calls = client_for(monkeypatch)
    assert send(link='https://example.com/reset?token=a&value="b"')
    assert 'token=a&amp;value=&quot;b&quot;' in calls[0][1]['json']['html']


def test_support_reply_to_uses_backend_configuration(monkeypatch):
    monkeypatch.setenv('AUTH_EMAIL_REPLY_TO', 'support@mychaat.net')
    calls = client_for(monkeypatch)
    assert send()
    assert calls[0][1]['json']['reply_to'] == 'support@mychaat.net'


@pytest.mark.parametrize('link', ['http://example.com/reset', 'javascript:alert(1)',
                                  'https://user:password@example.com/reset'])
def test_unsafe_links_never_sent(monkeypatch, link):
    calls = client_for(monkeypatch)
    assert not send(link=link)
    assert not calls
