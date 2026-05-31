"""Phase 2L — time-bound fresh-comment signal + fresh Graph verify.

The operator anchors a "fresh comment posted at time T" lower-bound
on the verification panel, and the backend computes per-account
verdicts using ONLY events at or after T. Old events (even those
inside the lookback window) are excluded — they cannot mask a
fresh-comment delivery failure.

Spec contract points (15):

  1.  Events before the test-comment time are ignored for the
      fresh-comment verdict.
  2.  Fresh external comment after timestamp with no webhook events for
      the account →
      ``fresh_external_comment_no_webhook_signal_after_comment_time``.
  3.  Non-comment webhook after timestamp → verdict is
      ``webhook_received_after_comment_time_but_no_comment_payload``.
  4.  Cached subscription says ready but zero events after timestamp →
      ``fresh_external_comment_no_webhook_signal_after_comment_time``.
  5.  Comment webhook after timestamp + automation_success →
      ``fresh_comment_webhook_completed``.
  6.  webhook_comment_detected after timestamp but no success yet →
      ``fresh_comment_webhook_detected``.
  7.  Only self-comment webhook after timestamp →
      ``only_bot_own_comment_seen_after_cutoff``.
  8-16. Unchanged-contract guards.
"""
import inspect
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://example.com')
os.environ.setdefault('FRONTEND_URL', 'https://example.com')
os.environ.setdefault('IG_APP_ID', '123')
os.environ.setdefault('IG_APP_SECRET', 'secret')
os.environ.setdefault('CRON_SECRET', 'cron-secret')

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server  # noqa: E402


def _ev(stage, *, source='webhook', t_offset_seconds=0, base=None,
        username='acct_x', extra=None):
    base = base or datetime.utcnow()
    return {
        'stage': stage,
        'source': source,
        'created_at': base + timedelta(seconds=t_offset_seconds),
        'username_key': username,
        'extra': extra or {},
    }


def _verdict_for(rows, after_time, username='acct_x', subscription_accounts=None):
    out = server._compute_fresh_comment_signal(
        rows, after_time, subscription_accounts or [],
    )
    by_user = {a['username']: a for a in out.get('accounts') or []}
    return by_user.get(username, {}).get('verdict')


def test_1_events_before_cutoff_are_ignored():
    base = datetime.utcnow()
    cutoff = base + timedelta(seconds=60)
    # Old, pre-cutoff events — must be ignored.
    rows = [
        _ev('webhook_received', t_offset_seconds=0, base=base),
        _ev('account_resolution_success', t_offset_seconds=1, base=base,
            extra={'has_comments_field': True}),
        _ev('webhook_comment_detected', t_offset_seconds=2, base=base),
        _ev('automation_success', t_offset_seconds=3, base=base),
    ]
    # Subscription cache says ready, but nothing happened after cutoff.
    subs = [{
        'username': 'acct_x',
        'comments_subscribed': True,
        'webhook_comment_delivery_configured': True,
    }]
    v = _verdict_for(rows, cutoff, subscription_accounts=subs)
    assert v == 'fresh_external_comment_no_webhook_signal_after_comment_time'


def test_2_no_events_after_cutoff_returns_fresh_no_signal():
    base = datetime.utcnow()
    cutoff = base + timedelta(seconds=60)
    rows: list = []
    v = _verdict_for(rows, cutoff, subscription_accounts=[])
    assert v is None or v == 'fresh_external_comment_no_webhook_signal_after_comment_time'
    # The signal block is also emitted when subscription is in cache:
    subs = [{'username': 'acct_x', 'comments_subscribed': False}]
    out = server._compute_fresh_comment_signal(rows, cutoff, subs)
    by_user = {a['username']: a for a in out.get('accounts') or []}
    assert by_user['acct_x']['verdict'] == 'fresh_external_comment_no_webhook_signal_after_comment_time'


def test_3_non_comment_webhook_after_cutoff_no_comment_payload():
    base = datetime.utcnow()
    cutoff = base
    rows = [
        _ev('webhook_received', t_offset_seconds=10, base=base),
        _ev('account_resolution_success', t_offset_seconds=11, base=base,
            extra={'via': 'instagram_accounts',
                   'has_comments_field': False,
                   'has_live_comments_field': False,
                   'has_messages_field': True}),
    ]
    v = _verdict_for(rows, cutoff)
    assert v == 'webhook_received_after_comment_time_but_no_comment_payload'


def test_4_subscription_ready_but_zero_events_returns_dedicated_label():
    base = datetime.utcnow()
    cutoff = base + timedelta(seconds=60)
    subs = [{
        'username': 'acct_x',
        'comments_subscribed': True,
        'webhook_comment_delivery_configured': True,
    }]
    v = _verdict_for([], cutoff, subscription_accounts=subs)
    assert v == 'fresh_external_comment_no_webhook_signal_after_comment_time'


def test_5_comment_webhook_after_cutoff_with_automation_success():
    base = datetime.utcnow()
    cutoff = base
    rows = [
        _ev('webhook_received', t_offset_seconds=10, base=base),
        _ev('account_resolution_success', t_offset_seconds=11, base=base,
            extra={'has_comments_field': True}),
        _ev('webhook_comment_detected', t_offset_seconds=12, base=base),
        _ev('automation_success', t_offset_seconds=13, base=base),
    ]
    v = _verdict_for(rows, cutoff)
    assert v == 'fresh_comment_webhook_completed'


def test_6_webhook_comment_detected_in_flight():
    base = datetime.utcnow()
    cutoff = base
    rows = [
        _ev('webhook_received', t_offset_seconds=10, base=base),
        _ev('account_resolution_success', t_offset_seconds=11, base=base,
            extra={'has_comments_field': True}),
        _ev('webhook_comment_detected', t_offset_seconds=12, base=base),
        # No automation_success yet — flow in progress.
    ]
    v = _verdict_for(rows, cutoff)
    assert v == 'fresh_comment_webhook_detected'


def test_7_only_bot_own_comment_after_cutoff_is_not_in_flight():
    base = datetime.utcnow()
    cutoff = base
    rows = [
        _ev('webhook_received', t_offset_seconds=10, base=base),
        _ev('account_resolution_success', t_offset_seconds=11, base=base,
            extra={'has_comments_field': True}),
        {
            **_ev('webhook_comment_detected', t_offset_seconds=12, base=base),
            'comment_id_partial': '181...233',
            'commenter_id_partial': '178...615',
        },
        {
            **_ev('automation_skipped', t_offset_seconds=13, base=base),
            'comment_id_partial': '181...233',
            'commenter_id_partial': '178...615',
            'skip_reason': 'bot_own_reply',
        },
    ]
    out = server._compute_fresh_comment_signal(rows, cutoff, [])
    by_user = {a['username']: a for a in out.get('accounts') or []}
    acct = by_user['acct_x']
    assert acct['verdict'] == 'only_bot_own_comment_seen_after_cutoff'
    assert acct['webhook_bot_own_skipped_count_after_cutoff'] == 1
    assert acct['webhook_bot_own_comment_id_partials_after_cutoff'] == ['181...233']
    assert acct['webhook_commenter_id_partials_after_cutoff'] == ['178...615']


def test_helper_returns_empty_when_no_cutoff_supplied():
    assert server._compute_fresh_comment_signal([], None, []) == {}


def test_endpoint_advertises_fresh_subscription_verify_route():
    src = inspect.getsource(server.admin_instagram_subscription_verify_fresh)
    # Read-only Graph GET (never POST in this endpoint).
    assert "graph.instagram.com" in src
    assert 'subscribed_apps' in src
    assert '.post(' not in src
    # Persists fresh fields into the cache.
    assert 'webhookSubscriptionFields' in src
    assert 'webhookSubscriptionLastCheckedAt' in src


# ---------------------------------------------------------------------------
# 7-15. Unchanged-contract guards
# ---------------------------------------------------------------------------


def test_no_username_specific_logic():
    src = inspect.getsource(server)
    for needle in (
        "username == 'muhammad_gehad'",
        "username == 'mogehad17'",
        "username_key == 'muhammad_gehad'",
        "username_key == 'mogehad17'",
    ):
        assert needle not in src


def test_polling_production_default_is_explicit():
    src = inspect.getsource(server)
    assert "IG_POLL_ENABLED = _env_bool(" in src
    assert "default=IS_PRODUCTION" in src


def test_hmac_unchanged():
    assert 'hmac.compare_digest' in inspect.getsource(server)


def test_billing_unchanged():
    assert hasattr(server, 'reserve_usage_limit')


def test_dedupe_unchanged():
    assert "'dedupe_checked'" in inspect.getsource(server)


def test_phase2d_cooldown_unchanged():
    assert 'opening_dm_already_sent_for_commenter_media' in inspect.getsource(server)


def test_quick_reply_copy_unchanged():
    src = inspect.getsource(server)
    assert 'public_reply_attempted' in src
    assert 'opening_dm_attempted' in src
