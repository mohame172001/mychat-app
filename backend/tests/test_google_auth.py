"""Phase 2.7: Google Sign-In endpoint tests.

Mocks the Google ID token verifier so tests never touch the network.
Locks the privacy contract: the credential and decoded payload are
never logged, never echoed, never stored.
"""
import asyncio
import logging
import os
import sys
from datetime import datetime
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

from test_instagram_token_refresh import FakeDB, _account, _run, _user  # noqa: E402


PRIVATE_CREDENTIAL = 'eyJzZWNyZXQiOiJkby1ub3QtbG9nLW1lLTk5OTk5In0=.signature.private'


def _full_user(**overrides):
    """Test user with the keys _public_user expects."""
    doc = {
        'id': 'u1',
        'username': 'testuser',
        'name': 'Test User',
        'email': 'test@example.com',
        'ig_user_id': 'igA',
        'meta_access_token': 'token-a',
        'active_instagram_account_id': 'accA',
        'instagramConnected': True,
        'instagram_connection_valid': True,
    }
    doc.update(overrides)
    return doc


def _setup(monkeypatch, *, users=None):
    fake_db = FakeDB(
        _account(id='accA', userId=(users or [{}])[0].get('id', 'u1'),
                 instagramAccountId='igA'),
        users or [],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'GOOGLE_CLIENT_ID', 'test-google-client-id')
    # Per-test rate-limiter bypass — the in-memory limiter persists across
    # tests in this file and would otherwise 429 the second/third call.
    monkeypatch.setattr(server, '_rate_limited', lambda *a, **k: False)
    return fake_db


def _stub_verify(monkeypatch, claims):
    def fake_verify(credential):
        if credential is None or not isinstance(credential, str):
            from fastapi import HTTPException
            raise HTTPException(400, 'invalid_google_credential')
        # Mimic the real shape returned by google-auth's verify_oauth2_token.
        return {
            'iss': 'https://accounts.google.com',
            'aud': 'test-google-client-id',
            **claims,
        }
    monkeypatch.setattr(server, 'verify_google_credential', fake_verify)


# ── disabled when GOOGLE_CLIENT_ID missing ──────────────────────────────────

def test_google_auth_returns_503_when_not_configured(monkeypatch):
    fake_db = FakeDB(_account(id='accA', userId='u1', instagramAccountId='igA'))
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'GOOGLE_CLIENT_ID', None)
    # We bypass _stub_verify so the real verify_google_credential runs and
    # hits the GOOGLE_CLIENT_ID check first.
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_google({'credential': 'anything'}, request=None))
    assert exc.value.status_code == 503
    assert 'google_auth_not_configured' in str(exc.value.detail)


# ── new user path ───────────────────────────────────────────────────────────

def test_google_auth_creates_new_user(monkeypatch):
    db = _setup(monkeypatch)
    _stub_verify(monkeypatch, {
        'sub': 'g_sub_1', 'email': 'new@example.com', 'email_verified': True,
        'name': 'New User', 'picture': 'https://example.com/p.png',
    })
    res = _run(server.auth_google({'credential': PRIVATE_CREDENTIAL}, request=None))
    assert res.token
    assert res.user.email == 'new@example.com'
    saved = db.users.docs[0]
    assert saved['google_sub'] == 'g_sub_1'
    assert saved['email_verified'] is True
    assert saved['linked_providers'] == ['google']
    assert saved['username'].startswith('new')
    assert saved['avatar'] == 'https://example.com/p.png'


# ── existing google_sub login ───────────────────────────────────────────────

def test_google_auth_logs_in_existing_google_sub(monkeypatch):
    db = _setup(monkeypatch, users=[
        _full_user(id='u_known', email='known@example.com', google_sub='g_sub_known'),
    ])
    _stub_verify(monkeypatch, {
        'sub': 'g_sub_known', 'email': 'known@example.com', 'email_verified': True,
    })
    res = _run(server.auth_google({'credential': PRIVATE_CREDENTIAL}, request=None))
    assert res.user.id == 'u_known'
    # No second user created.
    assert len(db.users.docs) == 1


# ── existing email — link path ──────────────────────────────────────────────

def test_google_auth_links_existing_email_user(monkeypatch):
    db = _setup(monkeypatch, users=[
        _full_user(id='u_exist', email='exist@example.com', google_sub=None),
    ])
    _stub_verify(monkeypatch, {
        'sub': 'g_sub_link', 'email': 'exist@example.com',
        'email_verified': True, 'name': 'Existing',
        'picture': 'https://example.com/exist.png',
    })
    res = _run(server.auth_google({'credential': PRIVATE_CREDENTIAL}, request=None))
    assert res.user.id == 'u_exist'
    saved = next(u for u in db.users.docs if u.get('id') == 'u_exist')
    assert saved['google_sub'] == 'g_sub_link'
    assert saved['email_verified'] is True
    assert 'google' in (saved.get('linked_providers') or [])


def test_google_auth_link_preserves_plan_and_admin_role(monkeypatch):
    """Linking must NOT modify plan, admin role, or Instagram accounts."""
    db = FakeDB(
        _account(id='accA', userId='u_pro', instagramAccountId='igA',
                 connectionValid=True),
        _full_user(id='u_pro', email='pro@example.com', google_sub=None),
        user_plans=[{'id': 'up1', 'user_id': 'u_pro', 'plan_key': 'pro'}],
        admin_members=[{'id': 'm1', 'user_id': 'u_pro', 'email': 'pro@example.com',
                         'role': 'admin', 'disabled_at': None}],
    )
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'GOOGLE_CLIENT_ID', 'test-google-client-id')
    monkeypatch.setattr(server, '_rate_limited', lambda *a, **k: False)
    _stub_verify(monkeypatch, {
        'sub': 'g_sub_pro', 'email': 'pro@example.com', 'email_verified': True,
    })
    _run(server.auth_google({'credential': PRIVATE_CREDENTIAL}, request=None))

    # Plan unchanged.
    plan_row = db.user_plans.docs[0]
    assert plan_row['plan_key'] == 'pro'
    # Admin row unchanged.
    member = db.admin_members.docs[0]
    assert member['role'] == 'admin'
    # IG account row unchanged.
    assert db.instagram_accounts.docs[0]['connectionValid'] is True


# ── error paths ─────────────────────────────────────────────────────────────

def test_google_auth_rejects_unverified_email(monkeypatch):
    _setup(monkeypatch)
    _stub_verify(monkeypatch, {
        'sub': 'g_sub_x', 'email': 'unverified@example.com',
        'email_verified': False,
    })
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_google({'credential': PRIVATE_CREDENTIAL}, request=None))
    assert exc.value.status_code == 403
    assert 'google_email_not_verified' in str(exc.value.detail)


def test_google_auth_rejects_missing_email(monkeypatch):
    _setup(monkeypatch)
    _stub_verify(monkeypatch, {
        'sub': 'g_sub_x', 'email': '', 'email_verified': True,
    })
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_google({'credential': PRIVATE_CREDENTIAL}, request=None))
    assert exc.value.status_code == 401
    assert 'missing_email' in str(exc.value.detail)


def test_google_auth_rejects_missing_credential(monkeypatch):
    _setup(monkeypatch)
    # Don't stub — real verifier must reject.
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_google({'credential': None}, request=None))
    assert exc.value.status_code == 400


def test_google_auth_rejects_existing_user_with_other_google_sub(monkeypatch):
    """Existing user already has google_sub=A; a different sub for the same
    email is rejected with 409 google_account_already_linked."""
    db = _setup(monkeypatch, users=[
        _full_user(id='u_exist', email='exist@example.com', google_sub='g_sub_A'),
    ])
    _stub_verify(monkeypatch, {
        'sub': 'g_sub_B', 'email': 'exist@example.com', 'email_verified': True,
    })
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_google({'credential': PRIVATE_CREDENTIAL}, request=None))
    assert exc.value.status_code == 409
    assert 'google_account_already_linked' in str(exc.value.detail)


def test_google_auth_rejects_cross_user_conflict(monkeypatch):
    """sub=X owned by user_A but email=foo@x owned by user_B → 409."""
    db = _setup(monkeypatch, users=[
        _full_user(id='u_A', email='a@example.com', google_sub='g_sub_X'),
        _full_user(id='u_B', email='b@example.com', google_sub=None),
    ])
    _stub_verify(monkeypatch, {
        'sub': 'g_sub_X', 'email': 'b@example.com', 'email_verified': True,
    })
    with pytest.raises(server.HTTPException) as exc:
        _run(server.auth_google({'credential': PRIVATE_CREDENTIAL}, request=None))
    assert exc.value.status_code == 409
    assert 'google_account_conflict' in str(exc.value.detail)


# ── verify_google_credential reasons (real path through the helper) ─────────

def test_verify_google_credential_no_sdk_returns_503(monkeypatch):
    monkeypatch.setattr(server, 'GOOGLE_CLIENT_ID', 'test-id')
    # Force the lazy import to fail.
    import builtins
    real_import = builtins.__import__
    def boom(name, *a, **kw):
        if name.startswith('google.'):
            raise ImportError('mocked missing google sdk')
        return real_import(name, *a, **kw)
    monkeypatch.setattr(builtins, '__import__', boom)
    with pytest.raises(server.HTTPException) as exc:
        server.verify_google_credential('any.token.value')
    assert exc.value.status_code == 503
    assert 'google_auth_sdk_not_installed' in str(exc.value.detail)


# ── privacy contract ────────────────────────────────────────────────────────

def test_google_auth_does_not_log_credential(monkeypatch, caplog):
    _setup(monkeypatch)
    _stub_verify(monkeypatch, {
        'sub': 'g_sub_p', 'email': 'p@example.com', 'email_verified': True,
        'name': 'P', 'picture': 'https://example.com/p.png',
    })
    with caplog.at_level(logging.INFO, logger='mychat'):
        _run(server.auth_google({'credential': PRIVATE_CREDENTIAL}, request=None))
    # The token must not appear in any log line.
    serialized = caplog.text
    assert PRIVATE_CREDENTIAL not in serialized
    assert 'do-not-log-me-99999' not in serialized
    # The success log line should still record the (non-sensitive) user id.
    assert 'google_auth_success' in serialized


def test_google_auth_response_does_not_leak_google_sub(monkeypatch):
    db = _setup(monkeypatch)
    _stub_verify(monkeypatch, {
        'sub': 'g_super_secret_sub', 'email': 'p@example.com',
        'email_verified': True,
    })
    res = _run(server.auth_google({'credential': PRIVATE_CREDENTIAL}, request=None))
    serialized = repr(res)
    # The Google sub must not appear in the auth response — it's stored
    # in the DB but not echoed back to the client.
    assert 'g_super_secret_sub' not in serialized


# ── existing email/password login still works ───────────────────────────────

def test_existing_email_password_login_unaffected(monkeypatch):
    """Sanity: even after Google login changes, /auth/login still works."""
    from auth_utils import hash_password as _hash
    fake_db = FakeDB(
        _account(id='accA', userId='u_pwd', instagramAccountId='igA'),
        _full_user(id='u_pwd', email='pwd@example.com', username='pwduser',
              password_hash=_hash('Hunter22!!')),
    )
    monkeypatch.setattr(server, 'db', fake_db)

    async def no_rate(*_a, **_kw):
        return False
    monkeypatch.setattr(server, '_rate_limited', lambda *a, **k: False)

    class _LoginIn:
        username = 'pwduser'
        password = 'Hunter22!!'

    # Avoid pydantic — call inner function directly.
    res = _run(server.login(_LoginIn(), request=SimpleNamespace(client=SimpleNamespace(host='1.1.1.1'), headers={})))
    assert res.token
    assert res.user.email == 'pwd@example.com'
