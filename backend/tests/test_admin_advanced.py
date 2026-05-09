"""Phase 2.8: custom allowances + suspend/delete + reconciliation tests."""
import asyncio
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
import limit_overrides as _ov  # noqa: E402
import admin_roles as _ar  # noqa: E402

from test_instagram_token_refresh import FakeDB, _account, _run, _user  # noqa: E402


def _full_user(**overrides):
    doc = {
        'id': 'u1', 'username': 'testuser', 'name': 'Test User',
        'email': 'test@example.com',
        'instagramConnected': True, 'instagram_connection_valid': True,
        'ig_user_id': 'igA', 'meta_access_token': 'tok',
        'active_instagram_account_id': 'accA',
    }
    doc.update(overrides)
    return doc


def _setup(monkeypatch, *, owner_email='owner@mychat.app', users=None,
           overrides=None, comments=None, automations=None, monthly_usage=None,
           accounts=None):
    full_users = [
        _full_user(id='owner_u', email=owner_email, username='owner'),
        *(users or []),
    ]
    full_accounts = list(accounts or [])
    if not full_accounts:
        full_accounts.append(_account(id='accA', userId='owner_u',
                                      instagramAccountId='igA'))
    fake_db = FakeDB(
        full_accounts, full_users,
        admin_members=[
            {'id': 'm0', 'user_id': 'owner_u', 'email': owner_email,
             'role': 'owner', 'disabled_at': None},
        ],
        user_limit_overrides=overrides or [],
        comments=comments or [],
        automations=automations or [],
        monthly_usage=monthly_usage or [],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())
    monkeypatch.setattr(server, '_rate_limited', lambda *a, **k: False)
    return fake_db


# ── limit_overrides pure-logic tests ────────────────────────────────────────

def test_pure_active_filter_window_and_status():
    now = datetime(2026, 5, 8, 12, 0, 0)
    rows = [
        {'status': 'active', 'starts_at': datetime(2026, 5, 1),
         'ends_at': datetime(2026, 5, 31), 'type': 'additive_allowance',
         'metrics': {'comments_processed_extra': 1000}},
        {'status': 'active', 'starts_at': datetime(2026, 6, 1),
         'ends_at': None, 'type': 'additive_allowance', 'metrics': {}},
        {'status': 'revoked', 'starts_at': datetime(2026, 5, 1),
         'ends_at': None, 'type': 'additive_allowance', 'metrics': {}},
        {'status': 'active', 'starts_at': datetime(2026, 4, 1),
         'ends_at': datetime(2026, 5, 7), 'type': 'additive_allowance', 'metrics': {}},
    ]
    active = _ov.filter_active_overrides(rows, now)
    assert len(active) == 1


def test_pure_additive_extends_base_limit():
    base = {'monthly_comments_processed_limit': 250,
            'monthly_dms_sent_limit': 100,
            'max_active_automations': 2,
            'max_instagram_accounts': 1,
            'monthly_public_replies_sent_limit': 100,
            'monthly_links_clicked_limit': 100}
    rows = [{'type': 'additive_allowance',
             'metrics': {'comments_processed_extra': 1000}}]
    eff = _ov.compute_effective(base, rows)
    assert eff['monthly_comments_processed_limit'] == 1250
    assert eff['monthly_dms_sent_limit'] == 100  # untouched


def test_pure_limit_override_replaces_with_max():
    base = {'monthly_comments_processed_limit': 250}
    rows = [
        {'type': 'limit_override',
         'metrics': {'monthly_comments_processed_limit_override': 500}},
        {'type': 'limit_override',
         'metrics': {'monthly_comments_processed_limit_override': 2000}},
    ]
    eff = _ov.compute_effective(base, rows)
    assert eff['monthly_comments_processed_limit'] == 2000


def test_pure_trial_grant_adds_extras_on_top_of_override():
    base = {'monthly_comments_processed_limit': 250}
    rows = [
        {'type': 'trial_grant',
         'metrics': {
             'monthly_comments_processed_limit_override': 1000,
             'comments_processed_extra': 500,
         }},
    ]
    eff = _ov.compute_effective(base, rows)
    assert eff['monthly_comments_processed_limit'] == 1500


def test_pure_unlimited_base_unchanged_by_overrides():
    base = {'monthly_comments_processed_limit': None}
    rows = [
        {'type': 'limit_override',
         'metrics': {'monthly_comments_processed_limit_override': 100}},
        {'type': 'additive_allowance',
         'metrics': {'comments_processed_extra': 999}},
    ]
    eff = _ov.compute_effective(base, rows)
    assert eff['monthly_comments_processed_limit'] is None


# ── effective limits in admin endpoints ─────────────────────────────────────

def test_active_allowance_increases_check_plan_limit(monkeypatch):
    db = _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com', username='u1user')],
                monthly_usage=[
                    {'id': 'mu1', 'user_id': 'u1',
                     'event_month': datetime.utcnow().strftime('%Y-%m'),
                     'comments_processed': 250},
                ],
                overrides=[
                    {'id': 'ov1', 'user_id': 'u1', 'type': 'additive_allowance',
                     'status': 'active',
                     'starts_at': datetime.utcnow() - timedelta(hours=1),
                     'ends_at': datetime.utcnow() + timedelta(days=7),
                     'metrics': {'comments_processed_extra': 1000}},
                ])
    res = _run(server.check_plan_limit('u1', 'monthly_comments_processed_limit'))
    assert res['exceeded'] is False
    assert res['limit'] == 1250  # free 250 + 1000 extra


def test_expired_allowance_does_not_apply(monkeypatch):
    _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com')],
           monthly_usage=[
               {'id': 'mu1', 'user_id': 'u1',
                'event_month': datetime.utcnow().strftime('%Y-%m'),
                'comments_processed': 250},
           ],
           overrides=[
               {'id': 'ov1', 'user_id': 'u1', 'type': 'additive_allowance',
                'status': 'active',
                'starts_at': datetime.utcnow() - timedelta(days=60),
                'ends_at': datetime.utcnow() - timedelta(days=1),
                'metrics': {'comments_processed_extra': 1000}},
           ])
    res = _run(server.check_plan_limit('u1', 'monthly_comments_processed_limit'))
    assert res['exceeded'] is True
    assert res['limit'] == 250


def test_revoked_allowance_does_not_apply(monkeypatch):
    _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com')],
           monthly_usage=[
               {'id': 'mu1', 'user_id': 'u1',
                'event_month': datetime.utcnow().strftime('%Y-%m'),
                'comments_processed': 250},
           ],
           overrides=[
               {'id': 'ov1', 'user_id': 'u1', 'type': 'additive_allowance',
                'status': 'revoked',
                'starts_at': datetime.utcnow() - timedelta(hours=1),
                'ends_at': None,
                'metrics': {'comments_processed_extra': 1000}},
           ])
    res = _run(server.check_plan_limit('u1', 'monthly_comments_processed_limit'))
    assert res['exceeded'] is True


def test_trial_grant_inside_window_only(monkeypatch):
    now = datetime.utcnow()
    _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com')],
           monthly_usage=[
               {'id': 'mu1', 'user_id': 'u1',
                'event_month': now.strftime('%Y-%m'),
                'comments_processed': 100},
           ],
           overrides=[
               {'id': 'ov_future', 'user_id': 'u1', 'type': 'trial_grant',
                'status': 'active',
                'starts_at': now + timedelta(days=2),
                'ends_at': now + timedelta(days=10),
                'metrics': {'monthly_comments_processed_limit_override': 5000}},
           ])
    # Future-dated grant should NOT apply yet.
    res = _run(server.check_plan_limit('u1', 'monthly_comments_processed_limit'))
    assert res['limit'] == 250


# ── admin endpoints for overrides ───────────────────────────────────────────

def test_owner_can_create_additive_allowance(monkeypatch):
    db = _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com', username='u1user')])
    res = _run(server.admin_create_user_override(
        'u1',
        body={
            'type': 'additive_allowance',
            'grant_name': '5k extra trial',
            'metrics': {'comments_processed_extra': 5000},
            'reason': 'creator program',
        },
        user_id='owner_u',
    ))
    assert res['ok'] is True
    assert res['override']['type'] == 'additive_allowance'
    assert res['override']['metrics']['comments_processed_extra'] == 5000
    saved = db.user_limit_overrides.docs[0]
    assert saved['user_id'] == 'u1'
    assert saved['status'] == 'active'
    # Audit row written.
    audit = next(a for a in db.admin_audit_logs.docs
                 if a['action'] == 'user_limit_override_created')
    assert 'creator program' not in repr(audit)
    assert audit['metadata']['reason_length'] == len('creator program')
    assert audit['metadata']['type'] == 'additive_allowance'


def test_admin_role_can_create_allowance(monkeypatch):
    db = _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com', username='u1user'),
                                     _full_user(id='ad', email='ad@x.com', username='ad')])
    db.admin_members.docs.append({'id': 'm_ad', 'user_id': 'ad', 'email': 'ad@x.com',
                                  'role': 'admin', 'disabled_at': None})
    res = _run(server.admin_create_user_override(
        'u1', body={'type': 'limit_override',
                    'metrics': {'monthly_comments_processed_limit_override': 1500}},
        user_id='ad',
    ))
    assert res['ok'] is True


def test_support_cannot_create_allowance(monkeypatch):
    db = _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com', username='u1user'),
                                     _full_user(id='su', email='su@x.com', username='su')])
    db.admin_members.docs.append({'id': 'm_su', 'user_id': 'su', 'email': 'su@x.com',
                                  'role': 'support', 'disabled_at': None})
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_create_user_override(
            'u1', body={'type': 'additive_allowance',
                        'metrics': {'comments_processed_extra': 100}},
            user_id='su',
        ))
    assert exc.value.status_code == 403


def test_invalid_override_type_400(monkeypatch):
    _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com', username='u1user')])
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_create_user_override(
            'u1', body={'type': 'gift_card',
                        'metrics': {'comments_processed_extra': 100}},
            user_id='owner_u',
        ))
    assert exc.value.status_code == 400


def test_owner_can_revoke_allowance(monkeypatch):
    db = _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com', username='u1user')],
                overrides=[
                    {'id': 'ov_R', 'user_id': 'u1', 'type': 'additive_allowance',
                     'status': 'active', 'starts_at': datetime.utcnow() - timedelta(hours=1),
                     'ends_at': None,
                     'metrics': {'comments_processed_extra': 500}},
                ])
    res = _run(server.admin_revoke_user_override(
        'u1', 'ov_R', body={'reason': 'misuse'}, user_id='owner_u',
    ))
    assert res['ok'] is True
    saved = db.user_limit_overrides.docs[0]
    assert saved['status'] == 'revoked'
    assert saved['revoked_by_email'] == 'owner@mychat.app'
    audit = next(a for a in db.admin_audit_logs.docs
                 if a['action'] == 'user_limit_override_revoked')
    assert 'misuse' not in repr(audit)


def test_effective_limits_endpoint_shape(monkeypatch):
    _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com', username='u1user')],
           overrides=[
               {'id': 'ov1', 'user_id': 'u1', 'type': 'additive_allowance',
                'status': 'active', 'starts_at': datetime.utcnow() - timedelta(hours=1),
                'ends_at': None,
                'metrics': {'dms_sent_extra': 200}},
           ])
    res = _run(server.admin_user_effective_limits('u1', user_id='owner_u'))
    assert res['plan_key'] == 'free'
    assert res['active_overrides_count'] == 1
    # base 100 + extra 200 = 300
    assert res['limits']['monthly_dms_sent_limit'] == 300
    assert res['base_limits']['monthly_dms_sent_limit'] == 100
    expl = res['limits_explanation']['monthly_dms_sent_limit']
    assert expl['base'] == 100
    assert expl['additive_extra'] == 200
    assert expl['effective'] == 300


# ── /api/plan/current uses effective limits ─────────────────────────────────

def test_plan_current_includes_effective_limits(monkeypatch):
    _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com', username='u1user')],
           overrides=[
               {'id': 'ov1', 'user_id': 'u1', 'type': 'additive_allowance',
                'status': 'active', 'starts_at': datetime.utcnow() - timedelta(hours=1),
                'ends_at': None,
                'metrics': {'comments_processed_extra': 750}},
           ])
    res = _run(server.current_plan(user_id='u1'))
    assert res['limits']['monthly_comments_processed_limit'] == 1000  # 250+750
    assert res['base_limits']['monthly_comments_processed_limit'] == 250


# ── suspend / unsuspend / delete ───────────────────────────────────────────

def test_owner_can_suspend_user(monkeypatch):
    db = _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com', username='u1user')])
    res = _run(server.admin_suspend_user('u1', body={'reason': 'spam'}, user_id='owner_u'))
    assert res['status'] == 'suspended'
    saved = next(u for u in db.users.docs if u['id'] == 'u1')
    assert saved['status'] == 'suspended'
    assert saved['suspended_by'] == 'owner_u'
    assert saved['suspended_reason_length'] == len('spam')
    # Audit row.
    assert any(a['action'] == 'user_suspended' for a in db.admin_audit_logs.docs)


def test_suspended_user_cannot_login(monkeypatch):
    from auth_utils import hash_password as _hash
    db = _setup(monkeypatch, users=[
        _full_user(id='u1', email='u1@x.com', username='u1user',
                   password_hash=_hash('Hunter22!!'), status='suspended'),
    ])

    class _LoginIn:
        username = 'u1user'
        password = 'Hunter22!!'
    with pytest.raises(server.HTTPException) as exc:
        _run(server.login(_LoginIn(), request=SimpleNamespace(
            client=SimpleNamespace(host='1.1.1.1'), headers={})))
    assert exc.value.status_code == 403
    assert 'account_suspended' in str(exc.value.detail)


def test_suspended_user_wrong_password_gets_generic_login_error(monkeypatch):
    from auth_utils import hash_password as _hash
    _setup(monkeypatch, users=[
        _full_user(id='u1', email='u1@x.com', username='u1user',
                   password_hash=_hash('Hunter22!!'), status='suspended'),
    ])

    class _LoginIn:
        username = 'u1user'
        password = 'wrong-password'
    with pytest.raises(server.HTTPException) as exc:
        _run(server.login(_LoginIn(), request=SimpleNamespace(
            client=SimpleNamespace(host='1.1.1.1'), headers={})))
    assert exc.value.status_code == 401
    assert 'Invalid username or password' in str(exc.value.detail)
    assert 'account_suspended' not in str(exc.value.detail)


def test_unsuspend_restores_login(monkeypatch):
    from auth_utils import hash_password as _hash
    db = _setup(monkeypatch, users=[
        _full_user(id='u1', email='u1@x.com', username='u1user',
                   password_hash=_hash('Hunter22!!'), status='suspended'),
    ])
    _run(server.admin_unsuspend_user('u1', body={}, user_id='owner_u'))

    class _LoginIn:
        username = 'u1user'
        password = 'Hunter22!!'
    res = _run(server.login(_LoginIn(), request=SimpleNamespace(
        client=SimpleNamespace(host='1.1.1.1'), headers={})))
    assert res.token


def test_owner_can_soft_delete_user_and_pause_automations(monkeypatch):
    db = _setup(
        monkeypatch,
        users=[_full_user(id='u1', email='u1@x.com', username='u1user')],
        automations=[
            {'id': 'a1', 'user_id': 'u1', 'status': 'active'},
            {'id': 'a2', 'user_id': 'u1', 'status': 'active'},
        ],
        accounts=[
            _account(id='accU', userId='u1', user_id='u1',
                     instagramAccountId='igU', connectionValid=True),
            _account(id='accA', userId='owner_u', instagramAccountId='igA'),
        ],
    )
    res = _run(server.admin_soft_delete_user(
        'u1', body={'reason': 'fraud'}, user_id='owner_u'))
    assert res['status'] == 'deleted'
    saved = next(u for u in db.users.docs if u['id'] == 'u1')
    assert saved['status'] == 'deleted'
    # Automations paused.
    auto_a1 = next(a for a in db.automations.docs if a['id'] == 'a1')
    auto_a2 = next(a for a in db.automations.docs if a['id'] == 'a2')
    assert auto_a1['status'] == 'paused'
    assert auto_a2['status'] == 'paused'
    # IG account disconnected.
    saved_acc = next(a for a in db.instagram_accounts.docs if a['id'] == 'accU')
    assert saved_acc['connectionValid'] is False
    # Audit row written.
    assert any(a['action'] == 'user_soft_deleted' for a in db.admin_audit_logs.docs)


def test_deleted_user_cannot_login(monkeypatch):
    from auth_utils import hash_password as _hash
    _setup(monkeypatch, users=[
        _full_user(id='u1', email='u1@x.com', username='u1user',
                   password_hash=_hash('Hunter22!!'), status='deleted'),
    ])

    class _LoginIn:
        username = 'u1user'
        password = 'Hunter22!!'
    with pytest.raises(server.HTTPException) as exc:
        _run(server.login(_LoginIn(), request=SimpleNamespace(
            client=SimpleNamespace(host='1.1.1.1'), headers={})))
    assert exc.value.status_code == 403
    assert 'account_deleted' in str(exc.value.detail)


def test_admin_cannot_delete_user(monkeypatch):
    db = _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com', username='u1user'),
                                     _full_user(id='ad', email='ad@x.com', username='ad')])
    db.admin_members.docs.append({'id': 'm_ad', 'user_id': 'ad', 'email': 'ad@x.com',
                                  'role': 'admin', 'disabled_at': None})
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_soft_delete_user('u1', body={}, user_id='ad'))
    assert exc.value.status_code == 403


def test_owner_cannot_delete_self(monkeypatch):
    _setup(monkeypatch)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_soft_delete_user('owner_u', body={}, user_id='owner_u'))
    assert exc.value.status_code == 403
    assert 'cannot_delete_self' in str(exc.value.detail)


def test_cannot_delete_last_owner(monkeypatch):
    """Owner deletes target which is itself the only owner via member row.
    Last-owner invariant fires before self-check would (self-check would
    fire first for owner_u->owner_u, so we test target=second_owner)."""
    db = _setup(monkeypatch, users=[
        _full_user(id='other_owner', email='oo@x.com', username='oo'),
        _full_user(id='actor', email='actor@mychat.app', username='actor'),
    ])
    # Replace bootstrap member with two: actor=owner, other_owner=owner.
    db.admin_members.docs.clear()
    db.admin_members.docs.extend([
        {'id': 'm_a', 'user_id': 'actor', 'email': 'actor@mychat.app',
         'role': 'owner', 'disabled_at': None},
        {'id': 'm_oo', 'user_id': 'other_owner', 'email': 'oo@x.com',
         'role': 'owner', 'disabled_at': None},
    ])
    # Now suspend BOTH owners simultaneously: first should succeed, second
    # should fail because it would leave zero owners.
    _run(server.admin_suspend_user('other_owner', body={'reason': 'test'},
                                   user_id='actor'))
    # actor cannot delete itself (cannot_delete_self), but if actor is the
    # last owner, the `last owner` guard also blocks via members count.
    # Demote other_owner first.
    db.admin_members.docs[1]['role'] = 'admin'
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_suspend_user('actor', body={'reason': 'cycle'},
                                       user_id='actor'))
    # Self-suspend is allowed by current code; the invariant check catches
    # the last-owner removal. The expected status is 409 from the invariant.
    assert exc.value.status_code == 409


# ── reconciliation endpoint ────────────────────────────────────────────────

def test_reconciliation_returns_metric_rows(monkeypatch):
    month = datetime.utcnow().strftime('%Y-%m')
    db = _setup(
        monkeypatch,
        users=[_full_user(id='u1', email='u1@x.com', username='u1user')],
        automations=[
            {'id': 'a1', 'user_id': 'u1', 'status': 'active'},
            {'id': 'a2', 'user_id': 'u1', 'status': 'paused'},
        ],
        comments=[
            {'id': 'c1', 'user_id': 'u1', 'reply_provider_response_ok': True,
             'dm_status': 'success', 'action_status': 'success'},
            {'id': 'c2', 'user_id': 'u1', 'action_status': 'plan_limited'},
            {'id': 'c3', 'user_id': 'u1', 'action_status': 'failed_retryable'},
        ],
        monthly_usage=[
            {'id': 'mu1', 'user_id': 'u1', 'event_month': month,
             'public_replies_sent': 1, 'dms_sent': 1, 'links_clicked': 0},
        ],
    )
    res = _run(server.admin_metrics_reconciliation(month=None, user_id='owner_u'))
    assert 'items' in res
    names = [r['metric_name'] for r in res['items']]
    assert 'active_automations' in names
    assert 'public_replies_sent_month' in names
    assert 'plan_limited_counts' in names
    assert res['mismatch_count'] >= 0
    # Privacy: reconciliation must not return raw text.
    serialized = repr(res)
    assert 'comment_text' not in serialized
    assert 'access_token' not in serialized


def test_reconciliation_403_for_user(monkeypatch):
    fake_db = FakeDB(_account(id='accA', userId='u1', instagramAccountId='igA'),
                     _full_user(id='u1', email='u1@x.com', username='u1user'))
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_metrics_reconciliation(month=None, user_id='u1'))
    assert exc.value.status_code == 403


# ── audit log entries ──────────────────────────────────────────────────────

def test_create_revoke_workflow_writes_audit_pair(monkeypatch):
    db = _setup(monkeypatch, users=[_full_user(id='u1', email='u1@x.com', username='u1user')])
    res = _run(server.admin_create_user_override(
        'u1',
        body={'type': 'additive_allowance',
              'metrics': {'comments_processed_extra': 100}},
        user_id='owner_u',
    ))
    override_id = res['override']['id']
    _run(server.admin_revoke_user_override(
        'u1', override_id, body={'reason': 'reverted'}, user_id='owner_u',
    ))
    actions = [a['action'] for a in db.admin_audit_logs.docs]
    assert 'user_limit_override_created' in actions
    assert 'user_limit_override_revoked' in actions
