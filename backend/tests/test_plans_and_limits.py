"""Phase 2.2: plan model + limit enforcement (no Stripe / no billing).

Covers:
A. missing user_plans row -> free plan
B. valid plans returned by GET /api/plans
C. GET /api/plan/current returns plan, usage, limits, remaining
D. admin POST /api/admin/users/{id}/plan assigns
E. non-admin caller is rejected by the admin endpoint (403)
F. invalid plan_key rejected (400)
G. instagram account connect blocked at max
H. automation activation blocked at max
I. comment processing blocked when limit exceeded -> skipped_plan_limit
J. public reply send blocked when limit exceeded -> reply_status=plan_limited
K. dm send blocked when limit exceeded -> dm_status=plan_limited
L. plan_limited does NOT call Meta on retry / does not aggressively retry
M. upgrading plan allows future processing
N. /api/usage/current includes limits + remaining
"""
import asyncio
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
import plans as _plans  # noqa: E402

from test_instagram_token_refresh import FakeDB, _account, _run, _user  # noqa: E402
from test_comment_dm_failure_status import (  # noqa: E402
    _automation,
    _comment_payload,
    _install_db,
    _reply_and_dm_automation,
    _reply_provider_ok,
    _user_doc,
)


# ── A. missing user_plans row -> free plan ──────────────────────────────────

def test_a_missing_user_plan_defaults_to_free(monkeypatch):
    fake_db = FakeDB(_user(id='u1'))
    monkeypatch.setattr(server, 'db', fake_db)
    plan = _run(server.get_user_plan('u1'))
    assert plan['plan_key'] == 'free'
    assert plan['display_name'] == 'Free'
    assert plan['_assignment']['billing_enabled'] is False


# ── B. GET /api/plans ───────────────────────────────────────────────────────

def test_b_list_plans_returns_all_tiers(monkeypatch):
    monkeypatch.setattr(server, 'db', FakeDB())
    res = _run(server.list_plans())
    keys = [p['plan_key'] for p in res['plans']]
    assert keys == ['free', 'starter', 'pro', 'business']
    assert res['billing_enabled'] is False
    for plan in res['plans']:
        assert plan['billing_enabled'] is False
        # Ensure no Stripe ids leaked through.
        assert 'stripe_price_id' not in plan
        assert 'stripe_product_id' not in plan


# ── C. GET /api/plan/current returns plan + limits + remaining ──────────────

def test_c_current_plan_returns_full_summary(monkeypatch):
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        _user(id='u1'),
        automations=[{'id': 'auto1', 'user_id': 'u1', 'status': 'active'}],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    _run(server.record_usage_event(user_id='u1', event_type='dm_sent'))
    res = _run(server.current_plan(user_id='u1'))
    assert res['plan_key'] == 'free'
    assert res['display_name'] == 'Free'
    assert res['billing_enabled'] is False
    assert res['limits']['monthly_dms_sent_limit'] == 100
    assert res['counters']['dms_sent'] == 1
    assert res['remaining']['monthly_dms_sent_limit'] == 99
    assert res['exceeded']['monthly_dms_sent_limit'] is False
    assert res['max_active_automations'] == 2
    assert res['max_instagram_accounts'] == 1


# ── D, E, F. admin assignment ───────────────────────────────────────────────

def test_d_admin_assigns_plan(monkeypatch):
    fake_db = FakeDB(
        _account(id='accA', userId='admin_u', instagramAccountId='igA'),
        _user(id='admin_u', email='admin@mychat.app'),
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', {'admin@mychat.app'})
    res = _run(server.admin_assign_user_plan(
        'u_target',
        body={'plan_key': 'pro', 'reason': 'manual'},
        user_id='admin_u',
    ))
    assert res['ok'] is True
    assert res['plan_key'] == 'pro'
    assert res['billing_enabled'] is False
    plan = _run(server.get_user_plan('u_target'))
    assert plan['plan_key'] == 'pro'
    assert plan['_assignment']['assigned_by'] == 'admin_u'
    assert plan['_assignment']['assignment_reason'] == 'manual'


def test_e_non_admin_cannot_assign_plan(monkeypatch):
    fake_db = FakeDB(_user(id='u1', email='u1@example.com'))
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_assign_user_plan(
            'u_target',
            body={'plan_key': 'pro'},
            user_id='u1',
        ))
    assert exc.value.status_code == 403


def test_f_invalid_plan_key_rejected(monkeypatch):
    fake_db = FakeDB(
        _account(id='accA', userId='admin_u', instagramAccountId='igA'),
        _user(id='admin_u', email='admin@mychat.app'),
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', {'admin@mychat.app'})
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_assign_user_plan(
            'u_target',
            body={'plan_key': 'enterprise'},
            user_id='admin_u',
        ))
    assert exc.value.status_code == 400


# ── G. instagram account connect cap ────────────────────────────────────────

def test_g_instagram_connect_blocked_at_max(monkeypatch):
    """auth-url with mode=add_account must return 402 when at cap."""
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        _user(id='u1'),
    )
    monkeypatch.setattr(server, 'db', fake_db)
    # Free plan cap is 1 IG account; user has 1 connected.
    monkeypatch.setattr(server, 'IG_APP_ID', 'test-app-id')
    monkeypatch.setattr(server, 'IG_APP_SECRET', 'test-app-secret')
    with pytest.raises(server.HTTPException) as exc:
        _run(server.instagram_auth_url(mode='add_account', returnTo='/app', user_id='u1'))
    assert exc.value.status_code == 402
    assert 'free' in str(exc.value.detail).lower() or '1' in str(exc.value.detail)


def test_g2_instagram_reconnect_not_blocked(monkeypatch):
    """mode=reconnect replaces existing -> not subject to add-account cap."""
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        _user(id='u1'),
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'IG_APP_ID', 'test-app-id')
    monkeypatch.setattr(server, 'IG_APP_SECRET', 'test-app-secret')
    # Should not raise.
    res = _run(server.instagram_auth_url(mode='reconnect', returnTo='/app', user_id='u1'))
    assert res['mode'] == 'reconnect'


# ── H. automation activation cap ────────────────────────────────────────────

def test_h_automation_activation_blocked_at_max(monkeypatch):
    """patch_automation flipping to 'active' returns 402 when at cap."""
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        _user(id='u1'),
        automations=[
            {'id': 'a1', 'user_id': 'u1', 'status': 'active'},
            {'id': 'a2', 'user_id': 'u1', 'status': 'active'},
            {'id': 'a3', 'user_id': 'u1', 'status': 'paused', 'instagramAccountId': 'igA'},
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    # Caller account scope helper.
    async def fake_account(_uid):
        return {'id': 'accA', 'userId': 'u1', 'instagramAccountId': 'igA'}
    monkeypatch.setattr(server, 'getActiveInstagramAccount', fake_account)
    monkeypatch.setattr(server, '_account_scoped_query', lambda _u, _a: {'user_id': _u})
    monkeypatch.setattr(
        server, 'ws_manager',
        SimpleNamespace(send=lambda *_a, **_kw: asyncio.sleep(0)),
    )
    # Pydantic patch model: only set status=active.
    patch = server.AutomationPatch(status='active')
    with pytest.raises(server.HTTPException) as exc:
        _run(server.patch_automation('a3', patch, user_id='u1'))
    assert exc.value.status_code == 402


# ── I. comment processing blocked when limit exceeded ───────────────────────

def test_i_comment_processing_blocked_when_limit_exceeded(monkeypatch):
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])
    # Pre-fill monthly_usage at exactly the free cap (250).
    event_month = datetime.utcnow().strftime('%Y-%m')
    db.monthly_usage.docs.append({
        'id': 'mu1',
        'user_id': 'u1',
        'event_month': event_month,
        'comments_processed': 250,
    })
    reply_called = []
    dm_called = []

    async def reply_should_not_run(*_a, **_kw):
        reply_called.append('reply')
        return _reply_provider_ok()

    async def dm_should_not_run(*_a, **_kw):
        dm_called.append('dm')
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_should_not_run)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_should_not_run)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert result['action_status'] == 'plan_limited'
    assert result['reason'] == 'skipped_plan_limit'
    assert saved['action_status'] == 'plan_limited'
    assert saved['skip_reason'] == 'skipped_plan_limit'
    # Critical: neither Meta call fired.
    assert reply_called == []
    assert dm_called == []
    # No retry scheduled.
    assert saved.get('next_retry_at') is None


# ── J / K. public reply / DM limit blocks per-step inside execute_flow ──────

def test_j_public_reply_send_blocked_when_limit_exceeded(monkeypatch):
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])
    event_month = datetime.utcnow().strftime('%Y-%m')
    # Comments allowed (not at cap), but public replies at cap.
    db.monthly_usage.docs.append({
        'id': 'mu1',
        'user_id': 'u1',
        'event_month': event_month,
        'comments_processed': 0,
        'public_replies_sent': 100,  # free cap
        'dms_sent': 0,
    })
    reply_called = []
    dm_called = []

    async def reply_should_not_run(*_a, **_kw):
        reply_called.append('reply')
        return _reply_provider_ok()

    async def dm_ok(*_a, **_kw):
        dm_called.append('dm')
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_should_not_run)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert reply_called == []
    assert dm_called == ['dm']  # DM still flows
    assert saved['reply_status'] == 'plan_limited'
    assert saved['reply_failure_reason'] == 'plan_limit_exceeded'
    assert saved['dm_status'] == 'success'


def test_k_dm_send_blocked_when_limit_exceeded(monkeypatch):
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])
    event_month = datetime.utcnow().strftime('%Y-%m')
    db.monthly_usage.docs.append({
        'id': 'mu1',
        'user_id': 'u1',
        'event_month': event_month,
        'comments_processed': 0,
        'public_replies_sent': 0,
        'dms_sent': 100,
    })
    reply_called = []
    dm_called = []

    async def reply_ok(*_a, **_kw):
        reply_called.append('reply')
        return _reply_provider_ok()

    async def dm_should_not_run(*_a, **_kw):
        dm_called.append('dm')
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_should_not_run)

    _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert reply_called == ['reply']  # reply flows
    assert dm_called == []            # DM blocked
    assert saved['reply_status'] == 'success'
    assert saved['dm_status'] == 'plan_limited'
    assert saved['dm_failure_reason'] == 'plan_limit_exceeded'


# ── L. plan_limited does not aggressively retry ─────────────────────────────

def test_l_plan_limited_comment_does_not_have_retry_at(monkeypatch):
    """Once a comment is marked skipped_plan_limit, the worker has no
    next_retry_at and dm_failure_retryable is False, so the queue won't
    pick it up."""
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])
    event_month = datetime.utcnow().strftime('%Y-%m')
    db.monthly_usage.docs.append({
        'id': 'mu1', 'user_id': 'u1', 'event_month': event_month,
        'comments_processed': 250,
    })

    async def reply_block(*_a, **_kw):
        raise AssertionError('reply must not be called')

    async def dm_block(*_a, **_kw):
        raise AssertionError('DM must not be called')

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_block)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_block)

    _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]
    assert saved['action_status'] == 'plan_limited'
    assert saved.get('next_retry_at') is None
    assert saved.get('dm_failure_retryable') is False or saved.get('dm_failure_retryable') is None
    assert saved.get('reply_failure_retryable') is False or saved.get('reply_failure_retryable') is None


# ── M. upgrading plan releases the cap on next call ─────────────────────────

def test_m_upgrading_plan_unblocks_future_processing(monkeypatch):
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])
    event_month = datetime.utcnow().strftime('%Y-%m')
    # At free cap (250).
    db.monthly_usage.docs.append({
        'id': 'mu1', 'user_id': 'u1', 'event_month': event_month,
        'comments_processed': 250,
    })

    # Pre-assign starter plan: limit becomes 2000.
    _run(server.assign_user_plan('u1', 'starter', assigned_by='admin', reason='upgrade'))

    sent = []

    async def reply_ok(*_a, **_kw):
        sent.append('reply')
        return _reply_provider_ok()

    async def dm_ok(*_a, **_kw):
        sent.append('dm')
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    # Now under the new (starter) cap; flow runs end-to-end.
    assert result['action_status'] == 'success'
    assert sent == ['reply', 'dm']


# ── N. /api/usage/current includes limits + remaining + plan ────────────────

def test_n_usage_current_includes_limits_and_remaining(monkeypatch):
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        _user(id='u1'),
    )
    monkeypatch.setattr(server, 'db', fake_db)
    _run(server.record_usage_event(user_id='u1', event_type='dm_sent'))
    res = _run(server.current_usage(user_id='u1'))
    assert res['plan'] == 'free'
    assert res['plan_key'] == 'free'
    assert res['billing_enabled'] is False
    assert res['limits']['monthly_dms_sent_limit'] == 100
    assert res['remaining']['monthly_dms_sent_limit'] == 99
    assert 'exceeded' in res
    # Backward-compat: existing keys still present.
    assert 'counters' in res
    assert 'event_month' in res


# ── compute_action_status with plan_limited ─────────────────────────────────

def test_compute_action_status_with_plan_limited():
    # Reply succeeded, DM plan_limited -> partial_success
    assert server._compute_comment_action_status('success', 'plan_limited') == 'partial_success'
    # Reply plan_limited, DM succeeded -> partial_success
    assert server._compute_comment_action_status('plan_limited', 'success') == 'partial_success'
    # Both plan_limited -> plan_limited
    assert server._compute_comment_action_status('plan_limited', 'plan_limited') == 'plan_limited'
    # plan_limited + disabled (no other step) -> plan_limited
    assert server._compute_comment_action_status('plan_limited', 'disabled') == 'plan_limited'
    # plan_limited + failed -> failed (failure dominates)
    assert server._compute_comment_action_status('plan_limited', 'failed') == 'failed'


# ── Direct unit tests for plans module ──────────────────────────────────────

def test_plans_module_helpers():
    assert _plans.is_valid_plan_key('free')
    assert _plans.is_valid_plan_key('starter')
    assert _plans.is_valid_plan_key('pro')
    assert _plans.is_valid_plan_key('business')
    assert not _plans.is_valid_plan_key('enterprise')
    assert not _plans.is_valid_plan_key(None)
    assert _plans.get_plan_limits('not_a_plan')['plan_key'] == 'free'
    # Limits ordering: free < starter < pro < business
    assert (_plans.get_plan_limits('free')['monthly_comments_processed_limit']
            < _plans.get_plan_limits('starter')['monthly_comments_processed_limit']
            < _plans.get_plan_limits('pro')['monthly_comments_processed_limit']
            < _plans.get_plan_limits('business')['monthly_comments_processed_limit'])
    assert _plans.remaining(100, 30) == 70
    assert _plans.remaining(100, 200) == 0
    assert _plans.remaining(None, 999) is None
    assert _plans.is_exceeded(100, 99, increment=1) is False
    assert _plans.is_exceeded(100, 100, increment=1) is True
    assert _plans.is_exceeded(None, 999999) is False
