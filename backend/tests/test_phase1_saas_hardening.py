"""Phase 1 SaaS hardening regression tests.

Covers four reporting/scoping fixes landed before opening MyChat
signups to real customers:

1. Single-tenant webhook fallback is disabled by default in production
   so the very first customer cannot have an unmapped webhook attributed
   to their account.
2. Story Reply automation loading is scoped by (owner_user_id,
   instagram_account_id) so a sibling-account rule cannot fire on a
   different account's story webhook.
3. TTL indexes for transient collections (webhook_processing_failures,
   comment_dm_sessions, link_click_events) exist with safe windows.
4. Stop Point reporting prefers automation_success over later dedupe
   re-scan labels, and includes the literal `historical` skip_reason
   in the no_fresh_comment_seen_in_poll override.

All tests are read-only and use fake collections — no real Mongo, no
network calls, no sending behavior touched.
"""
import asyncio
import importlib
import os
import sys
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


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Fix 1: single-tenant webhook fallback gate
# ---------------------------------------------------------------------------


def test_single_tenant_fallback_default_is_disabled_in_production(monkeypatch):
    """In production with the env var unset, the single-tenant
    auto-attribution must be DISABLED. This protects the very first
    SaaS customer from having an unmapped webhook silently attributed
    to their account."""
    monkeypatch.delenv('INSTAGRAM_SINGLE_TENANT_FALLBACK', raising=False)
    monkeypatch.setattr(server, 'IS_PRODUCTION', True)
    assert server._resolve_single_tenant_fallback_flag() is False


def test_single_tenant_fallback_default_is_enabled_in_non_production(monkeypatch):
    """Outside production (local dev, preview), the fallback stays ON
    by default so contributors don't have to configure anything to run
    a single-account demo."""
    monkeypatch.delenv('INSTAGRAM_SINGLE_TENANT_FALLBACK', raising=False)
    monkeypatch.setattr(server, 'IS_PRODUCTION', False)
    assert server._resolve_single_tenant_fallback_flag() is True


def test_single_tenant_fallback_explicit_opt_in_in_production(monkeypatch):
    """Operator can explicitly opt in to keep the legacy behavior in
    production for a single-tenant deployment via the env var."""
    monkeypatch.setattr(server, 'IS_PRODUCTION', True)
    for truthy in ('1', 'true', 'yes', 'on', 'TRUE'):
        monkeypatch.setenv('INSTAGRAM_SINGLE_TENANT_FALLBACK', truthy)
        assert server._resolve_single_tenant_fallback_flag() is True


def test_single_tenant_fallback_explicit_opt_out(monkeypatch):
    """Operator can explicitly opt out even in dev/local for SaaS
    parity testing."""
    monkeypatch.setattr(server, 'IS_PRODUCTION', False)
    for falsy in ('0', 'false', 'no', 'off', 'FALSE'):
        monkeypatch.setenv('INSTAGRAM_SINGLE_TENANT_FALLBACK', falsy)
        assert server._resolve_single_tenant_fallback_flag() is False


# ---------------------------------------------------------------------------
# Fix 2: Story Reply account scoping
# ---------------------------------------------------------------------------


def test_story_reply_automation_query_includes_account_scope():
    """Regression: Story Reply rule loading used to be `{user_id, status,
    trigger}` only, allowing a sibling-account rule to fire on a
    different account's story webhook. The fix wraps the load with
    `_account_scoped_query(user_id, ig_account_id)` so a rule belonging
    to account A's `instagramAccountId`/`igUserId` cannot match a
    webhook resolved to account B.

    Static check on the rendered query so the regression cannot be
    silently reverted later: the query MUST include at least one of the
    account-identity fields the resolver writes.
    """
    query = server._account_scoped_query('user_a', 'ig_account_a_id')
    # The scoped query must contain a `$or` covering instagramAccountId,
    # igUserId, ig_user_id, instagramAccountDbId, etc.
    assert query.get('user_id') == 'user_a'
    assert '$or' in query
    identity_fields = {next(iter(clause)) for clause in query['$or']}
    # Spot-check the canonical fields rules are stored under.
    assert 'instagramAccountId' in identity_fields
    assert 'igUserId' in identity_fields


def test_story_reply_code_path_uses_account_scoped_query():
    """Static-source check: the Story Reply branch in _process_webhook
    must call `_account_scoped_query` in its automation load. Without
    this, the regression silently re-opens — every test above could
    pass while production code reverts to user-only scoping."""
    server_source = Path(server.__file__).read_text(encoding='utf-8')
    # Find the story_insights branch
    story_anchor = "field == 'story_insights'"
    idx = server_source.find(story_anchor)
    assert idx >= 0, 'Story Reply branch anchor missing from server.py'
    # Look in the next 600 chars for both the trigger string and the
    # account-scoping helper. If the helper is removed, this test fails.
    window = server_source[idx:idx + 1200]
    assert "'trigger': 'Story Reply'" in window
    assert '_account_scoped_query' in window, (
        'Story Reply automation load is no longer scoped by Instagram '
        'account — multi-tenant regression risk. Re-add '
        '_account_scoped_query(user_id, ig_account_id) to the branch.'
    )


# ---------------------------------------------------------------------------
# Fix 3: TTL index declarations (static check on bootstrap source)
# ---------------------------------------------------------------------------


def test_webhook_dlq_ttl_index_declared():
    """The bootstrap must declare a TTL index on the DLQ's `terminal_at`
    field, with a safe default window. Active pending_retry rows have
    `terminal_at` unset so the TTL leaves them alone."""
    src = Path(server.__file__).read_text(encoding='utf-8')
    assert 'ttl_webhook_dlq_terminal_at' in src
    assert "WEBHOOK_DLQ_TERMINAL_TTL_SECONDS" in src
    assert "'terminal_at': datetime.utcnow()" in src


def test_comment_dm_sessions_ttl_index_declared():
    """30-day TTL past natural expiresAt on closed comment-DM sessions.
    Active pending sessions have expiresAt in the future so they are
    not pruned."""
    src = Path(server.__file__).read_text(encoding='utf-8')
    assert 'ttl_comment_dm_sessions_expires_at' in src
    assert "COMMENT_DM_SESSIONS_TTL_SECONDS" in src


def test_link_click_events_ttl_index_declared():
    """90-day raw retention on click events. Dashboard reads at most
    the 5000 most-recent clicks per user so this does not break
    aggregation."""
    src = Path(server.__file__).read_text(encoding='utf-8')
    assert 'ttl_link_click_events_clicked_at' in src
    assert "LINK_CLICK_EVENTS_TTL_SECONDS" in src


def test_phase1_ttl_indexes_do_not_touch_core_business_collections():
    """Defensive: the four NEW TTL indexes (instagram_automation_events,
    webhook_processing_failures, comment_dm_sessions, link_click_events)
    must NOT have been added to core business collections that store
    user-facing data, billing ledger entries, or dedupe proof.
    """
    src = Path(server.__file__).read_text(encoding='utf-8')
    # Disallowed: TTL on any of these collections
    forbidden = (
        'ttl_comments',
        'ttl_dm_logs',
        'ttl_usage_events',
        'ttl_monthly_usage',
        'ttl_users',
        'ttl_instagram_accounts',
        'ttl_automations',
        'ttl_contacts',
        'ttl_user_plans',
        'ttl_data_deletion_requests',
        'ttl_admin_audit_logs',
        'ttl_conversations',
    )
    for needle in forbidden:
        assert needle not in src, (
            f'TTL index name {needle!r} appears in server.py — core '
            f'business / dedupe / billing data must never be TTL\'d.'
        )


# ---------------------------------------------------------------------------
# Fix 4: Stop Point reporting — success wins over later dedupe re-scan
# ---------------------------------------------------------------------------


# Reuse the existing multi-account routing test helpers.
from test_multi_account_automation_routing import (  # noqa: E402
    _install_multi_account_db, _seed_flight_event,
)


def test_stop_point_prefers_automation_success_over_later_already_replied_success(monkeypatch):
    """Fix 4b: when `automation_success` exists for a comment AND no
    failure exists for it, the Stop Point summary must report
    `automation_success` even if a later polling re-scan recorded
    `automation_skipped(already_replied_success)` for the same comment.
    The dedupe re-scan is bookkeeping; the success is the meaningful
    outcome. Reporting-only — no sending or dedupe semantics change.
    """
    db = _install_multi_account_db(monkeypatch)
    now = datetime.utcnow()
    comment_id = server._safe_partial_identifier('success-then-dedupe')
    media_id = server._safe_partial_identifier('mediaB')

    _seed_flight_event(
        db, stage='poller_comment_seen', source='polling',
        comment_id_partial=comment_id, media_id_partial=media_id,
        created_at=now,
        extra={'effective_timestamp': (now - timedelta(seconds=10)).strftime('%Y-%m-%dT%H:%M:%S'),
               'timestamp_source': 'graph'},
    )
    _seed_flight_event(
        db, stage='rule_loading_finished', source='polling',
        comment_id_partial=comment_id, media_id_partial=media_id,
        created_at=now + timedelta(seconds=1), extra={'rules_count': 1},
    )
    _seed_flight_event(
        db, stage='rule_match_success', source='polling',
        comment_id_partial=comment_id, media_id_partial=media_id,
        created_at=now + timedelta(seconds=2),
    )
    _seed_flight_event(
        db, stage='automation_success', source='polling',
        comment_id_partial=comment_id, media_id_partial=media_id,
        created_at=now + timedelta(seconds=3),
    )
    # Later polling tick re-saw the same physical comment and recorded
    # the dedupe skip. The label-collapse bug was: this skip won the
    # exact_stop_reason slot, hiding the genuine success.
    _seed_flight_event(
        db, stage='automation_skipped', source='polling',
        skip_reason='already_replied_success',
        comment_id_partial=comment_id, media_id_partial=media_id,
        created_at=now + timedelta(seconds=20),
        extra={'classified_reason': 'comment_already_replied_success'},
    )

    summary = _run(server.summarize_account_automation_stop_point('account_b'))
    assert summary['exact_stop_reason'] == 'automation_success', (
        f"Expected success-wins-over-dedupe, got "
        f"{summary['exact_stop_reason']!r}. Stop Point regression."
    )


def test_stop_point_historical_label_collapses_to_no_fresh_when_no_fresh_comment(monkeypatch):
    """Fix 4a: an old polling re-scan whose skip_reason is the literal
    `historical` string (return value at server.py:17721) and where no
    fresh external comment exists in the window must surface
    `no_fresh_comment_seen_in_poll`, not `historical`.
    """
    db = _install_multi_account_db(monkeypatch)
    now = datetime.utcnow()
    old_comment_id = server._safe_partial_identifier('old-historical-1')

    _seed_flight_event(
        db, stage='poller_comment_seen', source='polling',
        comment_id_partial=old_comment_id,
        media_id_partial=server._safe_partial_identifier('old-media'),
        created_at=now,
        # Stale timestamp beyond the fresh window
        extra={'effective_timestamp': (now - timedelta(days=180)).strftime('%Y-%m-%dT%H:%M:%S'),
               'timestamp_source': 'payload'},
    )
    _seed_flight_event(
        db, stage='automation_skipped', source='polling',
        # Literal `historical` per server.py:17721
        skip_reason='historical',
        comment_id_partial=old_comment_id,
        media_id_partial=server._safe_partial_identifier('old-media'),
        created_at=now + timedelta(seconds=1),
        extra={'classified_reason': 'comment_skipped_historical'},
    )

    summary = _run(server.summarize_account_automation_stop_point('account_b'))
    assert summary['fresh_comment_seen_in_last_poll'] is False
    assert summary['exact_stop_reason'] == 'no_fresh_comment_seen_in_poll', (
        f"Expected no_fresh_comment_seen_in_poll for stale historical "
        f"re-scan, got {summary['exact_stop_reason']!r}. Fix 4a regression."
    )


def test_stop_point_success_does_not_override_failures(monkeypatch):
    """Defensive: if a genuine failure (opening_dm_failed, etc.) exists
    after a success, that failure must still surface — success-wins
    only applies when no failure event is present for the chosen
    comment.
    """
    db = _install_multi_account_db(monkeypatch)
    now = datetime.utcnow()
    comment_id = server._safe_partial_identifier('success-then-fail')
    media_id = server._safe_partial_identifier('mediaB')

    _seed_flight_event(
        db, stage='poller_comment_seen', source='polling',
        comment_id_partial=comment_id, media_id_partial=media_id,
        created_at=now,
        extra={'effective_timestamp': (now - timedelta(seconds=10)).strftime('%Y-%m-%dT%H:%M:%S'),
               'timestamp_source': 'graph'},
    )
    _seed_flight_event(
        db, stage='rule_loading_finished', source='polling',
        comment_id_partial=comment_id, media_id_partial=media_id,
        created_at=now + timedelta(seconds=1), extra={'rules_count': 1},
    )
    _seed_flight_event(
        db, stage='rule_match_success', source='polling',
        comment_id_partial=comment_id, media_id_partial=media_id,
        created_at=now + timedelta(seconds=2),
    )
    _seed_flight_event(
        db, stage='automation_success', source='polling',
        comment_id_partial=comment_id, media_id_partial=media_id,
        created_at=now + timedelta(seconds=3),
    )
    # A real failure on this comment must still win — success-wins-over-
    # dedupe must not mask genuine failures.
    _seed_flight_event(
        db, stage='opening_dm_failed', source='polling',
        skip_reason='messaging_window_expired',
        comment_id_partial=comment_id, media_id_partial=media_id,
        created_at=now + timedelta(seconds=4),
        error_code='10',
    )

    summary = _run(server.summarize_account_automation_stop_point('account_b'))
    assert summary['exact_stop_reason'] != 'automation_success', (
        'A genuine opening_dm_failed must not be masked by success-wins '
        'override. Got automation_success — failure-hiding regression.'
    )
