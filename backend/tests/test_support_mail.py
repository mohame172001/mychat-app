import asyncio
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

import support_mail
from test_transactional_email import client_for
from test_password_reset import server, _request


def message(**values):
    return support_mail.SupportMessage(email='customer@example.com', category='automation',
                                       message='My automation is not replying to comments.', **values)


def test_fixed_recipient_and_reply_to(monkeypatch):
    monkeypatch.setenv('RESEND_API_KEY', 'test-key')
    monkeypatch.setenv('AUTH_EMAIL_FROM', 'MyChaat <no-reply@mail.mychaat.net>')
    calls = client_for(monkeypatch)
    assert asyncio.run(support_mail.send_support_message(message()))
    payload = calls[0][1]['json']
    assert payload['to'] == ['support@mychaat.net']
    assert payload['reply_to'] == 'customer@example.com'
    assert payload['from'] == 'MyChaat <no-reply@mail.mychaat.net>'


def test_provider_failure_is_not_success(monkeypatch):
    monkeypatch.setenv('RESEND_API_KEY', 'test-key')
    monkeypatch.setenv('AUTH_EMAIL_FROM', 'no-reply@mail.mychaat.net')
    client_for(monkeypatch, status=403)
    assert not asyncio.run(support_mail.send_support_message(message()))


@pytest.mark.parametrize('values', [
    {'email': 'bad\r\nBcc:attacker@example.com', 'category': 'other', 'message': 'a' * 30},
    {'email': 'a@example.com', 'category': 'injected', 'message': 'a' * 30},
    {'email': 'a@example.com', 'category': 'other', 'message': 'a' * 5001},
])
def test_invalid_intake(values):
    with pytest.raises(ValidationError):
        support_mail.SupportMessage(**values)


@pytest.mark.parametrize('limited,honeypot,delivery,status', [
    (True, '', True, 429), (False, 'spam', True, 400), (False, '', False, 503),
    (False, '', True, 200),
])
def test_route_outcomes(monkeypatch, limited, honeypot, delivery, status):
    sender = AsyncMock(return_value=delivery)
    monkeypatch.setattr(server, '_shared_rate_limited', AsyncMock(return_value=limited))
    monkeypatch.setattr(server, 'send_support_message', sender)
    if status == 200:
        assert asyncio.run(server.contact_support(message(), _request())) == {'status': 'accepted'}
    else:
        with pytest.raises(HTTPException) as exc:
            asyncio.run(server.contact_support(message(website=honeypot), _request()))
        assert exc.value.status_code == status
    if limited or honeypot:
        sender.assert_not_called()
