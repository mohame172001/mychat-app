"""Phase 2.12G auth/session lifecycle hardening tests."""
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
import admin_roles as _ar  # noqa: E402
from models import LoginIn, SignupIn  # noqa: E402
from test_instagram_token_refresh import FakeDB, _account, _run, _user  # noqa: E402
from test_google_auth import _full_user, _stub_verify, PRIVATE_CREDENTIAL  # noqa: E402


def _request(ip='1.1.1.1'):
    return SimpleNamespace(client=SimpleNamespace(host=ip), headers={})


def _reset_rate_limits():
    server._RATE_LIMIT_HITS.clear()


async def _no_seed(*_a, **_k):
    return None


def _capture_verification(monkeypatch):
    tokens = []
    monkeypatch.setattr(server, '_email_verification_delivery_configured', lambda: True)

    async def _deliver(_user, token):
        tokens.append(token)
        return True

    monkeypatch.setattr(server, '_deliver_email_verification', _deliver)
    return tokens


def _setup(monkeypatch, *, users=None, members=None, admin_emails=None):
    db = FakeDB(
        _account(id='accA', userId=(users or [{}])[0].get('id', 'u1'),
                 instagramAccountId='igA'),
        users or [],
        admin_members=members or [],
    )
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', admin_emails or set())
    return db


def test_existing_jwt_blocked_after_suspend_and_delete(monkeypatch):
    db = _setup(monkeypatch, users=[_user(id='u1', email='user@example.com')])

    assert _run(server.get_current_active_user_id('u1')) == 'u1'

    db.users.docs[0]['status'] = 'suspended'
    with pytest.raises(server.HTTPException) as exc:
        _run(server.get_current_active_user_id('u1'))
    assert exc.value.status_code == 403
    assert exc.value.detail == 'account_suspended'

    db.users.docs[0]['status'] = 'deleted'
    with pytest.raises(server.HTTPException) as exc:
        _run(server.get_current_active_user_id('u1'))
    assert exc.value.status_code == 403
    assert exc.value.detail == 'account_deleted'


def test_email_signup_normalizes_and_blocks_case_duplicate(monkeypatch):
    _reset_rate_limits()
    db = _setup(monkeypatch)
    monkeypatch.setattr(server, '_seed_user', _no_seed)
    monkeypatch.setattr(server, 'RATE_LIMIT_SIGNUP_PER_HOUR', 100)
    _capture_verification(monkeypatch)

    with pytest.raises(server.HTTPException) as exc:
        _run(server.signup(
            SignupIn(username='caseuser', email='Case@Example.COM', password='Password123!'),
            _request('1.1.1.1'),
        ))
    assert exc.value.status_code == 403
    assert exc.value.detail == 'email_verification_required'
    assert db.users.docs[0]['normalized_email'] == 'case@example.com'
    assert db.users.docs[0]['email_verified'] is False

    with pytest.raises(server.HTTPException) as exc:
        _run(server.signup(
            SignupIn(username='caseuser2', email='case@example.com', password='Password123!'),
            _request('2.2.2.2'),
        ))
    assert exc.value.status_code == 400
    assert 'Email already registered' in str(exc.value.detail)


def test_login_accepts_email_case_insensitively(monkeypatch):
    from auth_utils import hash_password

    _reset_rate_limits()
    _setup(monkeypatch, users=[
        _full_user(
            id='u1',
            username='caseuser',
            email='Case@Example.COM',
            normalized_email='case@example.com',
            password_hash=hash_password('Password123!'),
        )
    ])
    monkeypatch.setattr(server, 'RATE_LIMIT_LOGIN_PER_MIN', 100)

    res = _run(server.login(
        LoginIn(username='CASE@example.com', password='Password123!'),
        _request('3.3.3.3'),
    ))
    assert res.user.id == 'u1'


def test_google_links_existing_email_case_insensitively(monkeypatch):
    db = _setup(monkeypatch, users=[
        _full_user(id='u1', email='Mixed@Example.COM', google_sub=None),
    ])
    monkeypatch.setattr(server, 'GOOGLE_CLIENT_ID', 'test-google-client-id')
    monkeypatch.setattr(server, '_rate_limited', lambda *a, **k: False)
    _stub_verify(monkeypatch, {
        'sub': 'g_sub_case',
        'email': 'mixed@example.com',
        'email_verified': True,
    })

    res = _run(server.auth_google({'credential': PRIVATE_CREDENTIAL}, request=None))

    assert res.user.id == 'u1'
    saved = db.users.docs[0]
    assert saved['google_sub'] == 'g_sub_case'
    assert saved['normalized_email'] == 'mixed@example.com'


def test_admin_role_change_takes_effect_without_new_jwt(monkeypatch):
    db = _setup(
        monkeypatch,
        users=[
            _user(id='owner_u', email='owner@example.com'),
            _user(id='admin_u', email='admin@example.com'),
            _user(id='target_u', email='target@example.com'),
        ],
        members=[
            {'id': 'm0', 'user_id': 'owner_u', 'email': 'owner@example.com',
             'role': _ar.ROLE_OWNER, 'disabled_at': None},
            {'id': 'm1', 'user_id': 'admin_u', 'email': 'admin@example.com',
             'role': _ar.ROLE_ADMIN, 'disabled_at': None},
        ],
    )

    _run(server.admin_members_update(
        'admin_u',
        body={'role': _ar.ROLE_SUPPORT, 'reason': 'scope change'},
        user_id='owner_u',
    ))

    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_assign_user_plan(
            'target_u',
            body={'plan_key': 'pro', 'reason': 'not allowed after demotion'},
            user_id='admin_u',
        ))
    assert exc.value.status_code == 403
    assert any(row.get('action') == 'admin_member_role_changed'
               for row in db.admin_audit_logs.docs)


def test_admin_me_does_not_expose_admin_emails_env(monkeypatch):
    _setup(
        monkeypatch,
        users=[_user(id='owner_u', email='owner@example.com')],
        admin_emails={'owner@example.com', 'other-owner@example.com'},
    )

    res = _run(server.admin_me(user_id='owner_u'))

    serialized = repr(res)
    assert res['bootstrap_owner'] is True
    assert 'other-owner@example.com' not in serialized
    assert 'ADMIN_EMAILS' not in serialized


def test_login_rate_limit_uses_identifier_hash_across_ips(monkeypatch):
    _reset_rate_limits()
    _setup(monkeypatch, users=[])
    monkeypatch.setattr(server, 'RATE_LIMIT_LOGIN_PER_MIN', 2)

    for ip in ('10.0.0.1', '10.0.0.2'):
        with pytest.raises(server.HTTPException) as exc:
            _run(server.login(
                LoginIn(username='target@example.com', password='bad'),
                _request(ip),
            ))
        assert exc.value.status_code == 401

    with pytest.raises(server.HTTPException) as exc:
        _run(server.login(
            LoginIn(username='TARGET@example.com', password='bad'),
            _request('10.0.0.3'),
        ))
    assert exc.value.status_code == 429


def test_signup_rate_limit_uses_normalized_email_hash_across_ips(monkeypatch):
    _reset_rate_limits()
    _setup(monkeypatch)
    monkeypatch.setattr(server, '_seed_user', _no_seed)
    monkeypatch.setattr(server, 'RATE_LIMIT_SIGNUP_PER_HOUR', 1)
    _capture_verification(monkeypatch)

    with pytest.raises(server.HTTPException) as first:
        _run(server.signup(
            SignupIn(username='first', email='limit@example.com', password='Password123!'),
            _request('20.0.0.1'),
        ))
    assert first.value.status_code == 403

    with pytest.raises(server.HTTPException) as exc:
        _run(server.signup(
            SignupIn(username='second', email='LIMIT@example.com', password='Password123!'),
            _request('20.0.0.2'),
        ))
    assert exc.value.status_code == 429
