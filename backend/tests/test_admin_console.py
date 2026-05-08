"""Phase 2.4: Admin Console v0 endpoint tests.

Covers:
- /api/admin/me returns is_admin per email
- /api/admin/overview: 403 for non-admin, sanitized aggregates for admin
- /api/admin/users: pagination, search, plan filter, sanitization
- /api/admin/users/{id}/detail: profile/plan/usage/accounts/automations/failures
  (no raw text, no tokens)
- /api/admin/automations/{id}/disable: paused + audit log
- plan_assign records audit log
- Privacy: no raw comment / reply / DM / token fields in any admin response
"""
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
from test_instagram_token_refresh import FakeDB, _account, _run, _user  # noqa: E402


PRIVATE_TEXT = 'this private comment should never appear in admin responses'
PRIVATE_REPLY = 'this private reply should never appear'
PRIVATE_DM = 'this private DM should never appear'
PRIVATE_TOKEN = 'EAA-very-secret-token'


def _admin_setup(monkeypatch, *, extra_users=None, automations=None,
                 comments=None, monthly_usage=None, user_plans=None,
                 accounts=None):
    users = [
        {'id': 'admin_u', 'email': 'admin@mychat.app',
         'created_at': datetime.utcnow() - timedelta(days=1)},
    ] + list(extra_users or [])
    accounts_list = list(accounts or [])
    if not accounts_list:
        accounts_list.append(_account(id='accA', userId='admin_u',
                                      instagramAccountId='igA', connectionValid=True))
    fake_db = FakeDB(
        accounts_list,
        users,
        automations=automations or [],
        comments=comments or [],
        monthly_usage=monthly_usage or [],
        user_plans=user_plans or [],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', {'admin@mychat.app'})
    return fake_db


def _non_admin_setup(monkeypatch):
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA'),
        _user(id='u1', email='regular@example.com'),
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', {'admin@mychat.app'})
    return fake_db


# ── /admin/me ───────────────────────────────────────────────────────────────

def test_admin_me_admin_returns_true(monkeypatch):
    _admin_setup(monkeypatch)
    res = _run(server.admin_me(user_id='admin_u'))
    assert res['is_admin'] is True
    assert res['email'] == 'admin@mychat.app'


def test_admin_me_non_admin_returns_false(monkeypatch):
    _non_admin_setup(monkeypatch)
    res = _run(server.admin_me(user_id='u1'))
    assert res['is_admin'] is False
    # Never raises 403; the frontend uses the boolean to hide nav.


# ── /admin/overview ─────────────────────────────────────────────────────────

def test_admin_overview_403_for_non_admin(monkeypatch):
    _non_admin_setup(monkeypatch)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_overview(user_id='u1'))
    assert exc.value.status_code == 403


def test_admin_overview_returns_sanitized_aggregates(monkeypatch):
    month = datetime.utcnow().strftime('%Y-%m')
    db = _admin_setup(
        monkeypatch,
        extra_users=[
            {'id': 'u1', 'email': 'a@x.com', 'created_at': datetime.utcnow()},
            {'id': 'u2', 'email': 'b@x.com', 'created_at': datetime.utcnow()},
        ],
        automations=[
            {'id': 'a1', 'user_id': 'u1', 'status': 'active'},
            {'id': 'a2', 'user_id': 'u2', 'status': 'paused'},
        ],
        monthly_usage=[
            {'id': 'mu1', 'user_id': 'u1', 'event_month': month,
             'comments_processed': 10, 'public_replies_sent': 5,
             'dms_sent': 3, 'links_clicked': 1, 'queue_jobs_processed': 0,
             'retryable_failures': 0, 'permanent_failures': 0},
            {'id': 'mu2', 'user_id': 'u2', 'event_month': month,
             'comments_processed': 4, 'public_replies_sent': 2,
             'dms_sent': 2, 'links_clicked': 0, 'queue_jobs_processed': 0,
             'retryable_failures': 0, 'permanent_failures': 0},
        ],
        user_plans=[
            {'id': 'up1', 'user_id': 'u2', 'plan_key': 'pro'},
        ],
        comments=[
            {'id': 'c1', 'user_id': 'u1', 'action_status': 'plan_limited'},
            {'id': 'c2', 'user_id': 'u1', 'action_status': 'failed_retryable'},
            {'id': 'c3', 'user_id': 'u2', 'action_status': 'failed_permanent'},
        ],
    )
    res = _run(server.admin_overview(user_id='admin_u'))
    assert res['total_users'] == 3
    assert res['active_automations'] == 1
    assert res['total_automations'] == 2
    assert res['current_month_usage_totals']['comments_processed'] == 14
    assert res['current_month_usage_totals']['public_replies_sent'] == 7
    assert res['current_month_usage_totals']['dms_sent'] == 5
    assert res['plan_limited_counts'] == 1
    assert res['retryable_failure_counts'] == 1
    assert res['permanent_failure_counts'] == 1
    # u2 is on pro, the other 2 default to free.
    assert res['plan_distribution']['pro'] == 1
    assert res['plan_distribution']['free'] == 2
    assert res['billing_enabled'] is False
    # Privacy.
    serialized = repr(res)
    assert 'access_token' not in serialized
    assert PRIVATE_TEXT not in serialized


# ── /admin/users ────────────────────────────────────────────────────────────

def test_admin_users_list_paginated_and_sanitized(monkeypatch):
    extra = [
        {'id': f'u{i}', 'email': f'user{i}@x.com',
         'created_at': datetime.utcnow() - timedelta(days=i)}
        for i in range(1, 6)
    ]
    _admin_setup(monkeypatch, extra_users=extra)
    res = _run(server.admin_users_list(
        page=1, page_size=2, search=None, plan_key=None,
        sort=None, user_id='admin_u',
    ))
    assert res['pagination']['total'] == 6  # 5 extra + admin
    assert res['pagination']['page'] == 1
    assert res['pagination']['total_pages'] == 3
    assert len(res['items']) == 2
    for item in res['items']:
        assert 'access_token' not in item
        assert 'meta_access_token' not in item
        assert item['billing_enabled'] is False


def test_admin_users_list_search_filter(monkeypatch):
    _admin_setup(
        monkeypatch,
        extra_users=[
            {'id': 'u_alice', 'email': 'alice@example.com'},
            {'id': 'u_bob', 'email': 'bob@example.com'},
        ],
    )
    res = _run(server.admin_users_list(
        page=1, page_size=10, search='alice',
        plan_key=None, sort=None, user_id='admin_u',
    ))
    emails = [it['email'] for it in res['items']]
    assert 'alice@example.com' in emails
    assert 'bob@example.com' not in emails


def test_admin_users_list_plan_filter(monkeypatch):
    _admin_setup(
        monkeypatch,
        extra_users=[
            {'id': 'u_pro', 'email': 'pro@example.com'},
            {'id': 'u_free', 'email': 'free@example.com'},
        ],
        user_plans=[
            {'id': 'up1', 'user_id': 'u_pro', 'plan_key': 'pro'},
        ],
    )
    res = _run(server.admin_users_list(
        page=1, page_size=10, search=None, plan_key='pro',
        sort=None, user_id='admin_u',
    ))
    ids = [it['user_id'] for it in res['items']]
    assert 'u_pro' in ids
    assert 'u_free' not in ids
    assert 'admin_u' not in ids


def test_admin_users_list_invalid_plan_key_400(monkeypatch):
    _admin_setup(monkeypatch)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_users_list(
            page=1, page_size=10, search=None, plan_key='enterprise',
            sort=None, user_id='admin_u',
        ))
    assert exc.value.status_code == 400


def test_admin_users_list_403_for_non_admin(monkeypatch):
    _non_admin_setup(monkeypatch)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_users_list(
            page=1, page_size=10, search=None, plan_key=None,
            sort=None, user_id='u1',
        ))
    assert exc.value.status_code == 403


# ── /admin/users/{id}/detail ────────────────────────────────────────────────

def test_admin_user_detail_returns_sanitized_profile(monkeypatch):
    month = datetime.utcnow().strftime('%Y-%m')
    db = _admin_setup(
        monkeypatch,
        extra_users=[
            {'id': 'u_target', 'email': 't@x.com',
             'created_at': datetime.utcnow() - timedelta(days=3)},
        ],
        accounts=[
            _account(id='accA', userId='admin_u', instagramAccountId='igA'),
            {'id': 'accT', 'userId': 'u_target', 'user_id': 'u_target',
             'instagramAccountId': 'igT', 'username': 'target_handle',
             'connectionValid': True, 'tokenSource': 'long_lived',
             'accessToken': PRIVATE_TOKEN,  # MUST NOT leak
             'meta_access_token': PRIVATE_TOKEN,
             'created': datetime.utcnow()},
        ],
        automations=[
            {'id': 'auto_T', 'user_id': 'u_target', 'name': 'Test rule',
             'status': 'active', 'post_scope': 'specific',
             'media_id': 'm123', 'createdAt': datetime.utcnow(),
             'updated': datetime.utcnow()},
        ],
        comments=[
            {'id': 'c1', 'user_id': 'u_target', 'ig_comment_id': 'IGC1',
             'media_id': 'm123',
             'text': PRIVATE_TEXT,           # MUST NOT leak
             'reply_text': PRIVATE_REPLY,    # MUST NOT leak
             'dm_text': PRIVATE_DM,          # MUST NOT leak
             'reply_status': 'success', 'dm_status': 'failed',
             'action_status': 'partial_success',
             'dm_failure_reason': 'recipient_unavailable',
             'attempts': 2,
             'created': datetime.utcnow(), 'updated': datetime.utcnow()},
        ],
        monthly_usage=[
            {'id': 'mu1', 'user_id': 'u_target', 'event_month': month,
             'comments_processed': 5, 'public_replies_sent': 3,
             'dms_sent': 2, 'links_clicked': 1},
        ],
    )
    res = _run(server.admin_user_detail('u_target', user_id='admin_u'))
    assert res['user_id'] == 'u_target'
    assert res['profile']['email'] == 't@x.com'
    assert res['plan']['plan_key'] == 'free'
    assert res['plan']['billing_enabled'] is False
    assert len(res['instagram_accounts']) == 1
    assert res['instagram_accounts'][0]['username'] == 'target_handle'
    assert res['instagram_accounts'][0]['connectionValid'] is True
    assert len(res['automations']) == 1
    assert res['automations'][0]['active'] is True
    assert res['automations'][0]['post_scope'] == 'specific'
    assert res['automations'][0]['selected_media_id'] == 'm123'
    assert len(res['recent_failures']) == 1
    failure = res['recent_failures'][0]
    assert failure['ig_comment_id'] == 'IGC1'
    assert failure['action_status'] == 'partial_success'
    assert failure['dm_failure_reason'] == 'recipient_unavailable'
    # Privacy: no raw text or tokens anywhere in the response.
    serialized = repr(res)
    assert PRIVATE_TEXT not in serialized
    assert PRIVATE_REPLY not in serialized
    assert PRIVATE_DM not in serialized
    assert PRIVATE_TOKEN not in serialized
    assert 'accessToken' not in serialized.replace('access_token', '')  # field key check
    assert res['billing_enabled'] is False
    assert res['usage_current_month']['counters']['dms_sent'] == 2


def test_admin_user_detail_404_for_missing(monkeypatch):
    _admin_setup(monkeypatch)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_user_detail('does_not_exist', user_id='admin_u'))
    assert exc.value.status_code == 404


def test_admin_user_detail_403_for_non_admin(monkeypatch):
    _non_admin_setup(monkeypatch)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_user_detail('u1', user_id='u1'))
    assert exc.value.status_code == 403


# ── /admin/automations/{id}/disable + audit log ─────────────────────────────

def test_admin_disable_automation_pauses_and_logs(monkeypatch):
    db = _admin_setup(
        monkeypatch,
        automations=[
            {'id': 'auto_X', 'user_id': 'u_target', 'status': 'active',
             'name': 'Disable me'},
        ],
    )
    res = _run(server.admin_disable_automation(
        'auto_X', body={'reason': 'spam suspicion'}, user_id='admin_u',
    ))
    assert res['ok'] is True
    assert res['status'] == 'paused'
    saved = db.automations.docs[0]
    assert saved['status'] == 'paused'
    assert saved['admin_disabled_by'] == 'admin_u'
    # Audit log row written.
    audits = db.admin_audit_logs.docs
    assert len(audits) == 1
    assert audits[0]['action'] == 'automation_disable'
    assert audits[0]['target_automation_id'] == 'auto_X'
    assert audits[0]['target_user_id'] == 'u_target'
    # Audit metadata never contains raw reason text — only length/booleans.
    meta = audits[0].get('metadata') or {}
    assert meta.get('reason_length') == len('spam suspicion')
    assert 'spam suspicion' not in repr(audits[0])


def test_admin_disable_automation_404(monkeypatch):
    _admin_setup(monkeypatch)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_disable_automation(
            'nope', body={}, user_id='admin_u',
        ))
    assert exc.value.status_code == 404


def test_admin_disable_automation_403_for_non_admin(monkeypatch):
    db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA'),
        _user(id='u1', email='regular@example.com'),
        automations=[{'id': 'a1', 'user_id': 'u1', 'status': 'active'}],
    )
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', {'admin@mychat.app'})
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_disable_automation('a1', body={}, user_id='u1'))
    assert exc.value.status_code == 403


# ── plan_assign records audit ───────────────────────────────────────────────

def test_admin_plan_assign_records_audit_log(monkeypatch):
    db = _admin_setup(monkeypatch, extra_users=[{'id': 'u_target', 'email': 't@x.com'}])
    _run(server.admin_assign_user_plan(
        'u_target',
        body={'plan_key': 'starter', 'reason': 'beta tester'},
        user_id='admin_u',
    ))
    audits = [a for a in db.admin_audit_logs.docs if a.get('action') == 'plan_assign']
    assert len(audits) == 1
    assert audits[0]['target_user_id'] == 'u_target'
    assert audits[0]['admin_email'] == 'admin@mychat.app'
    assert (audits[0].get('metadata') or {}).get('plan_key') == 'starter'
    # Reason content not in metadata; only length.
    assert 'beta tester' not in repr(audits[0])


# ── /admin/audit-log feed ───────────────────────────────────────────────────

def test_admin_audit_log_returns_recent_actions(monkeypatch):
    db = _admin_setup(monkeypatch, extra_users=[{'id': 'u_target', 'email': 't@x.com'}])
    _run(server.admin_assign_user_plan(
        'u_target', body={'plan_key': 'pro'}, user_id='admin_u',
    ))
    res = _run(server.admin_audit_log(limit=50, user_id='admin_u'))
    assert res['count'] >= 1
    actions = [item['action'] for item in res['items']]
    assert 'plan_assign' in actions


def test_admin_audit_log_403_for_non_admin(monkeypatch):
    _non_admin_setup(monkeypatch)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_audit_log(limit=10, user_id='u1'))
    assert exc.value.status_code == 403
