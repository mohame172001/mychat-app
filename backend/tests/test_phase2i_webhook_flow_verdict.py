"""Phase 2I — per-comment webhook flow verdict.

The Webhook Verification panel needs to answer "for this fresh
comment, where did the pipeline stop?" without the operator
scrolling through 30 flight-recorder events. The classifier under
test (``server._compute_webhook_flow_verdicts``) is a pure
function over the events list, so each verdict gets its own table
test.

Spec verdicts:

  1. ``webhook_completed`` — automation_success source=webhook reached.
  2. ``webhook_failed_at_account_resolution`` — webhook arrived but
     account_resolution_failed and no webhook_comment_detected.
  3. ``webhook_failed_at_rule_loading`` — rule_loading_started but no
     rule_loading_finished / rule_match_*.
  4. ``webhook_failed_at_rule_match`` — rule_match_failed without
     rule_match_success.
  5. ``webhook_failed_at_public_reply`` — public_reply_failed and no
     public_reply_success.
  6. ``webhook_failed_at_opening_dm`` — opening_dm_failed and no
     opening_dm_success (and the public reply succeeded).
  7. ``webhook_partial_success_missing_final_automation_success`` —
     reply or DM emitted success but neither automation_success nor
     automation_failed was recorded.
  8. ``webhook_in_flight`` — events present but no terminal state.
  9. ``webhook_polling_only`` — only polling saw this comment; no
     webhook event for it.

Plus unchanged-contract guards 10-15: HMAC, Billing, dedupe, Phase
2D cooldown, quick-reply copy, polling default OFF.
"""
import os
import sys
import inspect
from datetime import datetime, timedelta
from pathlib import Path

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://example.com')
os.environ.setdefault('FRONTEND_URL', 'https://example.com')
os.environ.setdefault('IG_APP_ID', '123')
os.environ.setdefault('IG_APP_SECRET', 'secret')
os.environ.setdefault('CRON_SECRET', 'cron-secret')

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server  # noqa: E402


def _ev(stage, *, source='webhook', cid='abc...xyz', t_offset=0,
        base=None):
    """Construct one flight-recorder row."""
    base = base or datetime.utcnow()
    return {
        'stage': stage,
        'source': source,
        'comment_id_partial': cid,
        'created_at': base + timedelta(seconds=t_offset),
    }


def _classify(rows, cid='abc...xyz'):
    result = server._compute_webhook_flow_verdicts(rows)
    assert cid in result, f'no verdict for {cid}: keys={list(result)}'
    return result[cid]


# ---------------------------------------------------------------------------
# 1-9 verdict cases
# ---------------------------------------------------------------------------


def test_1_webhook_completed():
    rows = [
        _ev('webhook_received', t_offset=0),
        _ev('account_resolution_success', t_offset=1),
        _ev('webhook_comment_detected', t_offset=2),
        _ev('rule_loading_started', t_offset=3),
        _ev('rule_loading_finished', t_offset=4),
        _ev('rule_match_success', t_offset=5),
        _ev('public_reply_attempted', t_offset=6),
        _ev('public_reply_success', t_offset=7),
        _ev('opening_dm_attempted', t_offset=8),
        _ev('opening_dm_success', t_offset=9),
        _ev('automation_success', t_offset=10),
    ]
    out = _classify(rows)
    assert out['flow_verdict'] == 'webhook_completed'
    assert 'webhook' in out['sources']


def test_2_webhook_failed_at_account_resolution():
    rows = [
        _ev('webhook_received', t_offset=0),
        _ev('account_resolution_failed', t_offset=1),
    ]
    out = _classify(rows)
    assert out['flow_verdict'] == 'webhook_failed_at_account_resolution'
    assert out['stop_stage'] == 'account_resolution_failed'


def test_3_webhook_failed_at_rule_loading():
    rows = [
        _ev('webhook_received', t_offset=0),
        _ev('account_resolution_success', t_offset=1),
        _ev('webhook_comment_detected', t_offset=2),
        _ev('rule_loading_started', t_offset=3),
        # rule_loading_finished missing
    ]
    out = _classify(rows)
    assert out['flow_verdict'] == 'webhook_failed_at_rule_loading'
    assert out['stop_stage'] == 'rule_loading_started'


def test_4_webhook_failed_at_rule_match():
    rows = [
        _ev('webhook_received', t_offset=0),
        _ev('account_resolution_success', t_offset=1),
        _ev('webhook_comment_detected', t_offset=2),
        _ev('rule_loading_started', t_offset=3),
        _ev('rule_loading_finished', t_offset=4),
        _ev('rule_match_failed', t_offset=5),
    ]
    out = _classify(rows)
    assert out['flow_verdict'] == 'webhook_failed_at_rule_match'
    assert out['stop_stage'] == 'rule_match_failed'


def test_5_webhook_failed_at_public_reply():
    rows = [
        _ev('webhook_received', t_offset=0),
        _ev('account_resolution_success', t_offset=1),
        _ev('webhook_comment_detected', t_offset=2),
        _ev('rule_match_success', t_offset=3),
        _ev('public_reply_attempted', t_offset=4),
        _ev('public_reply_failed', t_offset=5),
    ]
    out = _classify(rows)
    assert out['flow_verdict'] == 'webhook_failed_at_public_reply'
    assert out['stop_stage'] == 'public_reply_failed'


def test_6_webhook_failed_at_opening_dm():
    rows = [
        _ev('webhook_received', t_offset=0),
        _ev('account_resolution_success', t_offset=1),
        _ev('webhook_comment_detected', t_offset=2),
        _ev('rule_match_success', t_offset=3),
        _ev('public_reply_attempted', t_offset=4),
        _ev('public_reply_success', t_offset=5),
        _ev('opening_dm_attempted', t_offset=6),
        _ev('opening_dm_failed', t_offset=7),
    ]
    out = _classify(rows)
    assert out['flow_verdict'] == 'webhook_failed_at_opening_dm'
    assert out['stop_stage'] == 'opening_dm_failed'


def test_7_webhook_partial_success_missing_final_automation_success():
    rows = [
        _ev('webhook_received', t_offset=0),
        _ev('account_resolution_success', t_offset=1),
        _ev('webhook_comment_detected', t_offset=2),
        _ev('rule_match_success', t_offset=3),
        _ev('public_reply_success', t_offset=4),
        _ev('opening_dm_success', t_offset=5),
        # No automation_success / automation_failed.
    ]
    out = _classify(rows)
    assert out['flow_verdict'] == 'webhook_partial_success_missing_final_automation_success'


def test_8_webhook_in_flight():
    rows = [
        _ev('webhook_received', t_offset=0),
        _ev('account_resolution_success', t_offset=1),
        _ev('webhook_comment_detected', t_offset=2),
    ]
    out = _classify(rows)
    assert out['flow_verdict'] == 'webhook_in_flight'


def test_9_webhook_polling_only():
    rows = [
        _ev('poller_comment_seen', source='polling', t_offset=0),
    ]
    out = _classify(rows)
    assert out['flow_verdict'] == 'webhook_polling_only'


def test_classifier_keys_independent_comment_ids():
    """Two comments with different verdicts must be classified
    independently (no cross-talk between comment_id_partial keys)."""
    a = [
        _ev('webhook_received', cid='aaa...111', t_offset=0),
        _ev('automation_success', cid='aaa...111', t_offset=1),
    ]
    b = [
        _ev('webhook_received', cid='bbb...222', t_offset=0),
        _ev('account_resolution_failed', cid='bbb...222', t_offset=1),
    ]
    out = server._compute_webhook_flow_verdicts(a + b)
    assert out['aaa...111']['flow_verdict'] == 'webhook_completed'
    assert out['bbb...222']['flow_verdict'] == 'webhook_failed_at_account_resolution'


# ---------------------------------------------------------------------------
# 10-15 unchanged-contract guards
# ---------------------------------------------------------------------------


def test_10_polling_remains_disabled_by_default():
    """Re-asserts Phase 2H production posture."""
    src = inspect.getsource(server)
    assert "os.environ.get('IG_POLL_ENABLED', '0')" in src


def test_11_hmac_unchanged():
    src = inspect.getsource(server)
    assert 'hmac.compare_digest' in src


def test_12_billing_unchanged():
    assert hasattr(server, 'reserve_usage_limit')
    src = inspect.getsource(server._handle_new_comment)
    assert "'monthly_comments_processed_limit'" in src


def test_13_dedupe_unchanged():
    src = inspect.getsource(server)
    assert "'dedupe_checked'" in src
    assert "'namespace'" in src


def test_14_phase2d_cooldown_unchanged():
    src = inspect.getsource(server)
    assert 'opening_dm_already_sent_for_commenter_media' in src


def test_15_quick_reply_copy_unchanged():
    src = inspect.getsource(server)
    assert 'public_reply_attempted' in src
    assert 'opening_dm_attempted' in src
