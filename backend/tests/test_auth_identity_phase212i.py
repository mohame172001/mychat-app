"""Phase 2.12I session revocation and email verification controls."""
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
from auth_utils import create_token, decode_token_payload, hash_password  # noqa: E402
from models import LoginIn, SignupIn  # noqa: E402
from test_instagram_token_refresh import FakeDB, _account, _run, _user  # noqa: E402
from test_google_auth import _full_user, _stub_verify, PRIVATE_CREDENTIAL  # noqa: E402


def _request(ip='1.2.3.4'):
    return SimpleNamespace(client=SimpleNamespace(host=ip), headers={})


def _setup(monkeypatch, *, users=None, members=None):
    db = FakeDB(
        _account(id='accA', userId=(users or [{}])[0].get('id', 'u1'), instagramAccountId='igA'),
        users or [],
        admin_members=members or [],
    )
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())
    return db


def _capture_verification(monkeypatch):
    tokens = []
    monkeypatch.setattr(server, '_email_verification_delivery_configured', lambda: True)

    async def _deliver(_user, token):
        tokens.append(token)
        return True

    monkeypatch.setattr(server, '_deliver_email_verification', _deliver)
    return tokens


async def _no_seed(*_a, **_k):
    return None


def test_jwt_session_version_mismatch_revokes_old_token(monkeypatch):
    _setup(monkeypatch, users=[
        _user(id='u1', email='user@example.com', session_version=0, email_verified=True),
    ])

    assert _run(server.get_current_active_user_id('u1', token_session_version=0)) == 'u1'

    _run(server._increment_user_session_version('u1', reason='test_revoke'))
    with pytest.raises(server.HTTPException) as exc:
        _run(server.get_current_active_user_id('u1', token_session_version=0))
    assert exc.value.status_code == 401
    assert exc.value.detail == 'session_revoked'

    assert _run(server.get_current_active_user_id('u1', token_session_version=1)) == 'u1'


def test_login_issues_current_session_version_and_jwt_has_no_sensitive_claims(monkeypatch):
    server._RATE_LIMIT_HITS.clear()
    _setup(monkeypatch, users=[
        _full_user(
            id='u1',
            username='safeuser',
            email='safe@example.com',
            normalized_email='safe@example.com',
            password_hash=hash_password('Password123!'),
            session_version=3,
            email_verified=True,
            access_token='must-not-leak',
            meta_access_token='must-not-leak',
        )
    ])
    monkeypatch.setattr(server, 'RATE_LIMIT_LOGIN_PER_MIN', 100)

    result = _run(server.login(
        LoginIn(username='safe@example.com', password='Password123!'),
        _request(),
    ))
    payload = decode_token_payload(result.token)

    assert payload['sub'] == 'u1'
    assert payload['session_version'] == 3
    serialized = repr(payload)
    for forbidden in ('password', 'access_token', 'meta_access_token', 'google', 'credential'):
        assert forbidden not in serialized


def test_admin_revoke_sessions_invalidates_old_jwt_and_audits_safely(monkeypatch):
    db = _setup(
        monkeypatch,
        users=[
            _user(id='admin_u', email='admin@example.com', email_verified=True),
            _user(id='target_u', email='target@example.com', session_version=0, email_verified=True),
        ],
        members=[{
            'id': 'm1',
            'user_id': 'admin_u',
            'email': 'admin@example.com',
            'role': _ar.ROLE_ADMIN,
            'disabled_at': None,
        }],
    )

    result = _run(server.admin_revoke_user_sessions(
        'target_u',
        body={'reason': 'private reason should not be stored'},
        user_id='admin_u',
    ))

    assert result['session_version'] == 1
    with pytest.raises(server.HTTPException) as exc:
        _run(server.get_current_active_user_id('target_u', token_session_version=0))
    assert exc.value.detail == 'session_revoked'
    audit = db.admin_audit_logs.docs[-1]
    assert audit['action'] == 'user_sessions_revoked'
    assert audit['metadata']['reason_length'] == len('private reason should not be stored')
    assert 'private reason' not in repr(audit)


def test_non_admin_cannot_revoke_sessions(monkeypatch):
    _setup(monkeypatch, users=[
        _user(id='u1', email='u1@example.com', email_verified=True),
        _user(id='target_u', email='target@example.com', email_verified=True),
    ])

    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_revoke_user_sessions('target_u', body={}, user_id='u1'))
    assert exc.value.status_code == 403


def test_password_signup_creates_unverified_user_and_requires_verification(monkeypatch):
    server._RATE_LIMIT_HITS.clear()
    db = _setup(monkeypatch)
    monkeypatch.setattr(server, '_seed_user', _no_seed)
    monkeypatch.setattr(server, 'RATE_LIMIT_SIGNUP_PER_HOUR', 100)
    tokens = _capture_verification(monkeypatch)

    with pytest.raises(server.HTTPException) as exc:
        _run(server.signup(
            SignupIn(username='verifyme', email='verify@example.com', password='Password123!'),
            _request('2.2.2.2'),
        ))

    assert exc.value.status_code == 403
    assert exc.value.detail == 'email_verification_required'
    assert len(tokens) == 1
    user = db.users.docs[0]
    assert user['email_verified'] is False
    assert user['email_verification_required'] is True
    assert user.get('email_verification_token_hash')
    assert tokens[0] not in repr(user)
    with pytest.raises(server.HTTPException) as blocked:
        _run(server.get_current_active_user_id(user['id'], token_session_version=0))
    assert blocked.value.detail == 'email_verification_required'


def test_verify_email_token_is_hashed_single_use_and_new_login_works(monkeypatch):
    server._RATE_LIMIT_HITS.clear()
    db = _setup(monkeypatch)
    monkeypatch.setattr(server, '_seed_user', _no_seed)
    monkeypatch.setattr(server, 'RATE_LIMIT_SIGNUP_PER_HOUR', 100)
    monkeypatch.setattr(server, 'RATE_LIMIT_LOGIN_PER_MIN', 100)
    tokens = _capture_verification(monkeypatch)

    with pytest.raises(server.HTTPException):
        _run(server.signup(
            SignupIn(username='verifyok', email='verifyok@example.com', password='Password123!'),
            _request('3.3.3.3'),
        ))
    token = tokens[0]
    user_id = db.users.docs[0]['id']

    result = _run(server.verify_email({'token': token}))
    assert result['status'] == 'email_verified'
    user = db.users.docs[0]
    assert user['email_verified'] is True
    assert 'email_verification_token_hash' not in user
    assert user['session_version'] == 1

    with pytest.raises(server.HTTPException) as reused:
        _run(server.verify_email({'token': token}))
    assert reused.value.status_code == 400

    login = _run(server.login(
        LoginIn(username='VERIFYOK@example.com', password='Password123!'),
        _request('4.4.4.4'),
    ))
    assert decode_token_payload(login.token)['session_version'] == 1
    assert _run(server.get_current_active_user_id(user_id, token_session_version=1)) == user_id


def test_expired_email_verification_token_rejected(monkeypatch):
    db = _setup(monkeypatch, users=[
        _user(
            id='u1',
            email='expired@example.com',
            normalized_email='expired@example.com',
            password_hash=hash_password('Password123!'),
            email_verified=False,
            email_verification_required=True,
        )
    ])
    token = 'expired-token'
    db.users.docs[0]['email_verification_token_hash'] = server._hash_email_verification_token(token)
    db.users.docs[0]['email_verification_expires_at'] = server.datetime.utcnow() - server.timedelta(minutes=1)

    with pytest.raises(server.HTTPException) as exc:
        _run(server.verify_email({'token': token}))
    assert exc.value.detail == 'email_verification_token_expired'
    assert db.users.docs[0]['email_verified'] is False


def test_resend_verification_generic_for_unknown_and_rate_limited(monkeypatch):
    server._RATE_LIMIT_HITS.clear()
    _setup(monkeypatch, users=[])
    monkeypatch.setattr(server, 'RATE_LIMIT_SIGNUP_PER_HOUR', 100)
    _capture_verification(monkeypatch)

    first = _run(server.resend_email_verification(
        {'email': 'missing@example.com'},
        _request('5.5.5.5'),
    ))
    assert first == {'ok': True, 'status': 'sent_if_account_exists'}

    _run(server.resend_email_verification(
        {'email': 'missing@example.com'},
        _request('5.5.5.5'),
    ))
    _run(server.resend_email_verification(
        {'email': 'missing@example.com'},
        _request('5.5.5.5'),
    ))
    with pytest.raises(server.HTTPException) as limited:
        _run(server.resend_email_verification(
            {'email': 'missing@example.com'},
            _request('5.5.5.5'),
        ))
    assert limited.value.status_code == 429


def test_google_verified_user_bypasses_password_email_verification(monkeypatch):
    server._RATE_LIMIT_HITS.clear()
    db = _setup(monkeypatch, users=[])
    monkeypatch.setattr(server, 'GOOGLE_CLIENT_ID', 'test-google-client-id')
    monkeypatch.setattr(server, '_rate_limited', lambda *a, **k: False)
    _stub_verify(monkeypatch, {
        'sub': 'google-sub-verified',
        'email': 'googleverified@example.com',
        'email_verified': True,
    })

    result = _run(server.auth_google({'credential': PRIVATE_CREDENTIAL}, request=None))

    assert result.user.email == 'googleverified@example.com'
    assert db.users.docs[0]['email_verified'] is True
    assert db.users.docs[0]['email_verification_required'] is False
    assert _run(server.get_current_active_user_id(db.users.docs[0]['id'], token_session_version=0))


def test_normalized_email_diagnostics_and_backfill_return_hashes_only(monkeypatch):
    db = _setup(
        monkeypatch,
        users=[
            _user(id='admin_u', email='owner@example.com', normalized_email='owner@example.com', email_verified=True),
            _user(id='u1', email='Dup@Example.com', normalized_email='dup@example.com'),
            _user(id='u2', email='dup@example.com', normalized_email='dup@example.com'),
            _user(id='u3', email='Missing@Example.com'),
        ],
        members=[{
            'id': 'm1',
            'user_id': 'admin_u',
            'email': 'owner@example.com',
            'role': _ar.ROLE_OWNER,
            'disabled_at': None,
        }],
    )
    db.users.docs[-1].pop('normalized_email', None)

    diag = _run(server.admin_normalized_email_diagnostics(user_id='admin_u'))
    assert diag['duplicate_groups_count'] == 1
    assert diag['duplicate_normalized_email_count'] == 2
    assert diag['users_missing_normalized_email_count'] == 1
    assert 'dup@example.com' not in repr(diag)
    assert diag['raw_emails_returned'] is False

    dry = _run(server.admin_backfill_normalized_email({'dry_run': True}, user_id='admin_u'))
    assert dry['candidates_count'] == 1
    assert 'missing@example.com' not in repr(dry)
    assert 'normalized_email' not in db.users.docs[-1]

    applied = _run(server.admin_backfill_normalized_email({'dry_run': False}, user_id='admin_u'))
    assert applied['updated_count'] == 1
    assert db.users.docs[-1]['normalized_email'] == 'missing@example.com'


def test_non_admin_denied_normalized_email_diagnostics(monkeypatch):
    _setup(monkeypatch, users=[_user(id='u1', email='u1@example.com', email_verified=True)])

    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_normalized_email_diagnostics(user_id='u1'))
    assert exc.value.status_code == 403


def test_existing_suspended_deleted_login_errors_remain_safe(monkeypatch):
    server._RATE_LIMIT_HITS.clear()
    db = _setup(monkeypatch, users=[
        _full_user(
            id='s1',
            username='suspended',
            email='suspended@example.com',
            normalized_email='suspended@example.com',
            password_hash=hash_password('Correct123!'),
            status='suspended',
            email_verified=True,
        ),
        _full_user(
            id='d1',
            username='deleted',
            email='deleted@example.com',
            normalized_email='deleted@example.com',
            password_hash=hash_password('Correct123!'),
            status='deleted',
            email_verified=True,
        ),
    ])
    monkeypatch.setattr(server, 'RATE_LIMIT_LOGIN_PER_MIN', 100)

    with pytest.raises(server.HTTPException) as wrong:
        _run(server.login(LoginIn(username='suspended@example.com', password='wrong'), _request('6.6.6.1')))
    assert wrong.value.status_code == 401
    assert wrong.value.detail == 'Invalid username or password'

    with pytest.raises(server.HTTPException) as suspended:
        _run(server.login(LoginIn(username='suspended@example.com', password='Correct123!'), _request('6.6.6.2')))
    assert suspended.value.detail == 'account_suspended'

    with pytest.raises(server.HTTPException) as deleted:
        _run(server.login(LoginIn(username='deleted@example.com', password='Correct123!'), _request('6.6.6.3')))
    assert deleted.value.detail == 'account_deleted'

    assert 'Correct123!' not in repr(db.users.docs)
