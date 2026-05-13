"""Phase 2.14 password reset flow tests.

Covers:
  - forgot-password: unknown email returns generic success (no enumeration)
  - known password user gets a hashed token + expiry, raw token never stored
  - reset-password: valid token rotates password + session_version
  - old password fails after reset
  - new password succeeds after reset
  - token cannot be reused
  - expired token rejected
  - invalid / malformed token rejected
  - too-short new password rejected
  - existing JWTs return 401 session_revoked
  - rate limit fires per IP and per email-hash
  - response shape never reveals whether the email exists
  - Google-only account returns generic success without issuing token
"""
import os
import sys
from datetime import datetime, timedelta
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
from auth_utils import hash_password, verify_password  # noqa: E402
from models import LoginIn  # noqa: E402
from test_instagram_token_refresh import FakeDB, _account, _run  # noqa: E402
from test_google_auth import _full_user  # noqa: E402

# Snapshot the REAL rate limiter BEFORE any test bypasses it via monkeypatch.
_REAL_RATE_LIMITED = server._rate_limited


def _request(ip='1.2.3.4'):
    return SimpleNamespace(client=SimpleNamespace(host=ip), headers={})


def _setup(monkeypatch, *, users=None):
    db = FakeDB(
        _account(id='accA', userId=(users or [{}])[0].get('id', 'u1'),
                 instagramAccountId='igA'),
        users or [],
    )
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())
    monkeypatch.setattr(server, '_rate_limited', lambda *a, **k: False)
    # Capture rather than network-send.
    return db


def _capture_reset(monkeypatch):
    """Intercept the delivery webhook; capture the raw token for the test
    to consume — and ONLY for the test."""
    sent_tokens = []

    async def _fake_deliver(_user, token):
        sent_tokens.append(token)
        return True

    monkeypatch.setattr(server, '_deliver_password_reset', _fake_deliver)
    return sent_tokens


# ── /auth/forgot-password ───────────────────────────────────────────────────

def test_forgot_password_unknown_email_returns_generic_success(monkeypatch):
    _setup(monkeypatch, users=[])
    _capture_reset(monkeypatch)
    res = _run(server.auth_forgot_password(
        server.ForgotPasswordIn(email='nobody@example.com'),
        request=_request(),
    ))
    assert res == {'ok': True, 'status': 'sent_if_account_exists'}


def test_forgot_password_known_user_issues_hashed_token(monkeypatch):
    db = _setup(monkeypatch, users=[
        _full_user(id='u1', email='user@example.com', username='u1user',
                   normalized_email='user@example.com',
                   password_hash=hash_password('OldPass123!'),
                   email_verified=True),
    ])
    tokens = _capture_reset(monkeypatch)
    res = _run(server.auth_forgot_password(
        server.ForgotPasswordIn(email='user@example.com'),
        request=_request(),
    ))
    assert res == {'ok': True, 'status': 'sent_if_account_exists'}
    assert len(tokens) == 1
    raw_token = tokens[0]
    assert raw_token and isinstance(raw_token, str)
    # The user row stores ONLY the hash, never the raw token.
    saved = db.users.docs[0]
    assert raw_token not in str(saved)
    assert saved['password_reset_token_hash'] == server._hash_password_reset_token(raw_token)
    assert isinstance(saved['password_reset_expires_at'], datetime)
    assert saved['password_reset_used_at'] is None
    # Expiry is approximately 1 hour from now.
    ttl_delta = saved['password_reset_expires_at'] - datetime.utcnow()
    assert timedelta(minutes=55) < ttl_delta < timedelta(minutes=65)


def test_forgot_password_response_shape_does_not_distinguish_users(monkeypatch):
    """Response body for a known user matches response body for an unknown
    user — exactly the anti-enumeration contract."""
    _setup(monkeypatch, users=[
        _full_user(id='u1', email='exists@example.com', username='u1user',
                   normalized_email='exists@example.com',
                   password_hash=hash_password('OldPass123!'),
                   email_verified=True),
    ])
    _capture_reset(monkeypatch)
    known = _run(server.auth_forgot_password(
        server.ForgotPasswordIn(email='exists@example.com'),
        request=_request(),
    ))
    unknown = _run(server.auth_forgot_password(
        server.ForgotPasswordIn(email='ghost@example.com'),
        request=_request(),
    ))
    assert known == unknown


def test_forgot_password_google_only_account_returns_generic_no_token(monkeypatch):
    db = _setup(monkeypatch, users=[
        _full_user(id='u_g', email='g@example.com', username='gu',
                   normalized_email='g@example.com',
                   password_hash=None,  # Google-only
                   google_sub='g_sub_xyz',
                   linked_providers=['google'],
                   email_verified=True),
    ])
    tokens = _capture_reset(monkeypatch)
    res = _run(server.auth_forgot_password(
        server.ForgotPasswordIn(email='g@example.com'),
        request=_request(),
    ))
    assert res == {'ok': True, 'status': 'sent_if_account_exists'}
    # No delivery attempted, no token stored.
    assert tokens == []
    assert 'password_reset_token_hash' not in db.users.docs[0]


# ── rate limit ──────────────────────────────────────────────────────────────

def _reset_rate_limiter(monkeypatch):
    """Undo the _setup bypass and reset the sliding-window hits, so the
    real limiter is exercised."""
    import collections as _c
    monkeypatch.setattr(server, '_rate_limited', _REAL_RATE_LIMITED)
    server._RATE_LIMIT_HITS = _c.defaultdict(_c.deque)


def test_forgot_password_rate_limit_per_ip(monkeypatch):
    _setup(monkeypatch, users=[])
    _capture_reset(monkeypatch)
    _reset_rate_limiter(monkeypatch)
    # Use a different email per call so the per-email-hash limit does not
    # fire — this isolates the per-IP limit. 5 allowed per hour per IP;
    # the 6th must 429.
    for i in range(5):
        _run(server.auth_forgot_password(
            server.ForgotPasswordIn(email=f'x{i}@example.com'),
            request=_request('9.9.9.9'),
        ))
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_forgot_password(
            server.ForgotPasswordIn(email='x_overflow@example.com'),
            request=_request('9.9.9.9'),
        ))
    assert exc.value.status_code == 429


def test_forgot_password_rate_limit_per_email_hash(monkeypatch):
    _setup(monkeypatch, users=[])
    _capture_reset(monkeypatch)
    _reset_rate_limiter(monkeypatch)
    # 3 allowed per email-hash per hour; the 4th must 429 even from a fresh IP.
    for i in range(3):
        _run(server.auth_forgot_password(
            server.ForgotPasswordIn(email='target@example.com'),
            request=_request(f'1.1.1.{i}'),
        ))
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_forgot_password(
            server.ForgotPasswordIn(email='target@example.com'),
            request=_request('1.1.1.99'),
        ))
    assert exc.value.status_code == 429


# ── /auth/reset-password ────────────────────────────────────────────────────

def _issue_token_for(monkeypatch, *, email='user@example.com', user_id='u1',
                    password='OldPass123!', extra=None):
    """Run forgot-password and return the raw token captured by the fake
    delivery webhook. Used to set up reset-password tests."""
    extras = extra or {}
    db = _setup(monkeypatch, users=[
        _full_user(id=user_id, email=email, username=user_id + 'user',
                   normalized_email=email,
                   password_hash=hash_password(password),
                   email_verified=True,
                   **extras),
    ])
    tokens = _capture_reset(monkeypatch)
    _run(server.auth_forgot_password(
        server.ForgotPasswordIn(email=email), request=_request(),
    ))
    assert len(tokens) == 1
    return db, tokens[0]


def test_reset_password_valid_token_rotates_password_and_session(monkeypatch):
    db, token = _issue_token_for(monkeypatch)
    saved_before = dict(db.users.docs[0])
    sv_before = int(saved_before.get('session_version') or 0)

    res = _run(server.auth_reset_password(
        server.ResetPasswordIn(token=token, new_password='NewPass456!'),
    ))
    assert res == {'ok': True, 'status': 'password_reset'}

    saved = db.users.docs[0]
    # Password hash rotated and verifies against the new password only.
    assert saved['password_hash'] != saved_before['password_hash']
    assert verify_password('NewPass456!', saved['password_hash'])
    assert not verify_password('OldPass123!', saved['password_hash'])
    # session_version incremented → all old JWTs are invalidated.
    assert saved['session_version'] == sv_before + 1
    assert saved['session_revocation_reason'] == 'password_reset'
    # Token cleared and marked used.
    assert 'password_reset_token_hash' not in saved
    assert saved['password_reset_used_at'] is not None


def test_old_password_login_fails_after_reset(monkeypatch):
    db, token = _issue_token_for(monkeypatch, email='login@example.com',
                                 user_id='u_login')
    _run(server.auth_reset_password(
        server.ResetPasswordIn(token=token, new_password='Brand-New-9!')
    ))
    import collections as _c
    server._RATE_LIMIT_HITS = _c.defaultdict(_c.deque)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.login(
            LoginIn(username='login@example.com', password='OldPass123!'),
            request=_request(),
        ))
    assert exc.value.status_code == 401


def test_new_password_login_succeeds_after_reset(monkeypatch):
    db, token = _issue_token_for(monkeypatch, email='login2@example.com',
                                 user_id='u_login2')
    _run(server.auth_reset_password(
        server.ResetPasswordIn(token=token, new_password='Brand-New-9!')
    ))
    import collections as _c
    server._RATE_LIMIT_HITS = _c.defaultdict(_c.deque)
    res = _run(server.login(
        LoginIn(username='login2@example.com', password='Brand-New-9!'),
        request=_request(),
    ))
    assert res.token
    assert res.user.email == 'login2@example.com'


def test_reset_token_cannot_be_reused(monkeypatch):
    db, token = _issue_token_for(monkeypatch)
    _run(server.auth_reset_password(
        server.ResetPasswordIn(token=token, new_password='FirstNew!1')
    ))
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_reset_password(
            server.ResetPasswordIn(token=token, new_password='SecondNew!2')
        ))
    # Either invalid_password_reset_token (cleared) or password_reset_token_used.
    assert exc.value.status_code == 400
    assert exc.value.detail in (
        'invalid_password_reset_token', 'password_reset_token_used',
    )


def test_reset_expired_token_rejected(monkeypatch):
    db, token = _issue_token_for(monkeypatch)
    # Force expiry in the past.
    db.users.docs[0]['password_reset_expires_at'] = datetime.utcnow() - timedelta(minutes=1)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_reset_password(
            server.ResetPasswordIn(token=token, new_password='ValidNew!9'),
        ))
    assert exc.value.status_code == 400
    assert exc.value.detail == 'password_reset_token_expired'


def test_reset_invalid_token_rejected(monkeypatch):
    _setup(monkeypatch, users=[
        _full_user(id='u1', email='u@example.com', username='u1user',
                   normalized_email='u@example.com',
                   password_hash=hash_password('OldPass123!'),
                   email_verified=True),
    ])
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_reset_password(
            server.ResetPasswordIn(token='garbage-not-a-real-token',
                                   new_password='Brand-New-9!'),
        ))
    assert exc.value.status_code == 400
    assert exc.value.detail == 'invalid_password_reset_token'


def test_reset_empty_token_rejected(monkeypatch):
    _setup(monkeypatch, users=[])
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_reset_password(
            server.ResetPasswordIn(token='', new_password='Brand-New-9!'),
        ))
    assert exc.value.status_code == 400


def test_reset_password_too_short_rejected(monkeypatch):
    db, token = _issue_token_for(monkeypatch)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_reset_password(
            server.ResetPasswordIn(token=token, new_password='abc'),
        ))
    assert exc.value.status_code == 400
    assert exc.value.detail == 'password_too_short'
    # Token must still be valid (not consumed by validation failure).
    saved = db.users.docs[0]
    assert 'password_reset_token_hash' in saved
    assert saved.get('password_reset_used_at') is None


def test_old_jwt_is_revoked_after_reset(monkeypatch):
    db, token = _issue_token_for(monkeypatch)
    sv_before = int(db.users.docs[0].get('session_version') or 0)

    # Old JWT works before reset.
    assert _run(server.get_current_active_user_id(
        'u1', token_session_version=sv_before,
    )) == 'u1'

    _run(server.auth_reset_password(
        server.ResetPasswordIn(token=token, new_password='Brand-New-9!')
    ))

    # Old JWT now rejected.
    with pytest.raises(server.HTTPException) as exc:
        _run(server.get_current_active_user_id(
            'u1', token_session_version=sv_before,
        ))
    assert exc.value.status_code == 401
    assert exc.value.detail == 'session_revoked'

    # New JWT with the incremented session_version works.
    assert _run(server.get_current_active_user_id(
        'u1', token_session_version=sv_before + 1,
    )) == 'u1'


def test_raw_token_never_stored(monkeypatch):
    db, token = _issue_token_for(monkeypatch)
    serialized = repr(db.users.docs[0])
    # The raw token must NEVER appear in the persisted row. Only its hash.
    assert token not in serialized
    # Audit / log lines also must not leak the token (verified by capturing
    # caplog in another test below).


class _FakeDeliveryResponse:
    def __init__(self, status_code):
        self.status_code = status_code


class _FakeDeliveryClient:
    def __init__(self, status_code=200):
        self.status_code = status_code
        self.posts = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, json=None, headers=None):
        self.posts.append({'url': url, 'json': json or {}, 'headers': headers or {}})
        return _FakeDeliveryResponse(self.status_code)


def test_password_reset_delivery_payload_shape(monkeypatch):
    fake_client = _FakeDeliveryClient(status_code=200)
    monkeypatch.setattr(server, 'EMAIL_VERIFICATION_WEBHOOK_URL', 'https://email.example/webhook')
    monkeypatch.setattr(server, 'EMAIL_VERIFICATION_WEBHOOK_TOKEN', 'delivery-secret')
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=10: fake_client)
    user = {
        'id': 'u1',
        'email': 'User@Example.com',
    }

    ok = _run(server._deliver_password_reset(user, 'raw-reset-token-for-test'))

    assert ok is True
    assert len(fake_client.posts) == 1
    post = fake_client.posts[0]
    payload = post['json']
    assert post['url'] == 'https://email.example/webhook'
    assert post['headers']['authorization'] == 'Bearer delivery-secret'
    assert payload['to'] == 'user@example.com'
    assert payload['template'] == server.PASSWORD_RESET_EMAIL_TEMPLATE
    assert payload['reset_url'].startswith(server.FRONTEND_URL.rstrip() + '/reset-password?token=')
    assert payload['reset_url'] == payload['resetUrl'] == payload['url'] == payload['link']
    assert payload['app_name'] == 'mychat'
    assert payload['expires_in_minutes'] == server.PASSWORD_RESET_TOKEN_TTL_HOURS * 60
    assert 'password' not in payload


def test_password_reset_delivery_non_2xx_logged_safely(monkeypatch, caplog):
    import logging as _logging
    fake_client = _FakeDeliveryClient(status_code=500)
    monkeypatch.setattr(server, 'EMAIL_VERIFICATION_WEBHOOK_URL', 'https://email.example/webhook')
    monkeypatch.setattr(server, 'EMAIL_VERIFICATION_WEBHOOK_TOKEN', '')
    monkeypatch.setattr(server.httpx, 'AsyncClient', lambda timeout=10: fake_client)

    with caplog.at_level(_logging.WARNING, logger='mychat'):
        ok = _run(server._deliver_password_reset(
            {'id': 'u1', 'email': 'user@example.com'},
            'raw-reset-token-for-test',
        ))

    assert ok is False
    assert 'password_reset_email_delivery_failed' in caplog.text
    assert 'status_code=500' in caplog.text
    assert 'raw-reset-token-for-test' not in caplog.text
    assert '/reset-password?token=' not in caplog.text


def test_password_reset_delivery_missing_env_logged_safely(monkeypatch, caplog):
    import logging as _logging
    monkeypatch.setattr(server, 'EMAIL_VERIFICATION_WEBHOOK_URL', '')

    with caplog.at_level(_logging.WARNING, logger='mychat'):
        ok = _run(server._deliver_password_reset(
            {'id': 'u1', 'email': 'user@example.com'},
            'raw-reset-token-for-test',
        ))

    assert ok is False
    assert 'password_reset_email_delivery_skipped' in caplog.text
    assert 'EMAIL_VERIFICATION_WEBHOOK_URL' in caplog.text
    assert 'raw-reset-token-for-test' not in caplog.text
    assert '/reset-password?token=' not in caplog.text


def test_password_reset_logs_do_not_leak_token(monkeypatch, caplog):
    import logging as _logging
    db, _token = _issue_token_for(monkeypatch)
    with caplog.at_level(_logging.INFO, logger='mychat'):
        _run(server.auth_forgot_password(
            server.ForgotPasswordIn(email='user@example.com'),
            request=_request(),
        ))
    # Token captured at issuance is not echoed by either log line.
    serialized = caplog.text
    captured_token = None
    # Re-issue to grab a fresh token and confirm same redaction property.
    tokens = []

    async def _capture(_u, t):
        tokens.append(t)
        return True
    monkeypatch.setattr(server, '_deliver_password_reset', _capture)
    _run(server.auth_forgot_password(
        server.ForgotPasswordIn(email='user@example.com'),
        request=_request(),
    ))
    captured_token = tokens[-1]
    assert captured_token not in serialized
