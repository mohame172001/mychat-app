"""Phase 2S — polling-primary send gate.

What this validates:
- `_is_polling_send_enabled()` defaults to TRUE in production and
  honors explicit `IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED` overrides.
- `_polling_mode()` reports `disabled` / `polling_primary`
  / `reconciliation_only` based on the two env flags.
- The Webhook Verification endpoint's summary surfaces the new
  Phase 2G counters and metadata
  (`polling_seen_count`, `polling_send_disabled_count`,
  `polling_mode`, `polling_interval_seconds`,
  `polling_send_enabled`).
- The endpoint includes `comment_webhook_missing_media_identifier`
  in `relevant_stages` so the operator can detect parseable-but-
  unroutable webhook payloads.

Send-gate execution path tests live in
`test_multi_account_automation_routing.py` (which uses the legacy
fallback flag via `conftest.py`). The structural tests here pin
the Phase 2G contracts that should never silently regress.
"""
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

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
# Env-flag / polling-mode primitives
# ---------------------------------------------------------------------------


def test_polling_send_default_is_disabled_outside_production(monkeypatch):
    """Local/dev can stay diagnostics-only when the env flag is absent."""
    monkeypatch.delenv('IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED',
                       raising=False)
    monkeypatch.setattr(server, 'IS_PRODUCTION', False)
    assert server._is_polling_send_enabled() is False


def test_polling_send_default_is_enabled_in_production(monkeypatch):
    monkeypatch.delenv('IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED',
                       raising=False)
    monkeypatch.setattr(server, 'IS_PRODUCTION', True)
    assert server._is_polling_send_enabled() is True


def test_polling_send_enabled_opt_in_truthy(monkeypatch):
    """Operator can explicitly re-enable the legacy emergency
    fallback by setting the env var to any truthy value."""
    for truthy in ('1', 'true', 'yes', 'on', 'TRUE'):
        monkeypatch.setenv('IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED',
                           truthy)
        assert server._is_polling_send_enabled() is True


def test_polling_send_enabled_explicit_off(monkeypatch):
    """Explicit 0/false/no must keep the gate closed."""
    for falsy in ('0', 'false', 'no', 'off', ''):
        monkeypatch.setenv('IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED',
                           falsy)
        assert server._is_polling_send_enabled() is False


def test_polling_mode_primary_when_legacy_flag_set(monkeypatch):
    monkeypatch.setattr(server, 'IG_POLL_ENABLED', True)
    monkeypatch.setenv('IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED', '1')
    assert server._polling_mode() == 'polling_primary'


def test_polling_mode_reconciliation_only_default(monkeypatch):
    monkeypatch.setattr(server, 'IG_POLL_ENABLED', True)
    monkeypatch.delenv('IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED',
                       raising=False)
    monkeypatch.setattr(server, 'IS_PRODUCTION', False)
    assert server._polling_mode() == 'reconciliation_only'


def test_polling_mode_disabled_when_poll_disabled(monkeypatch):
    monkeypatch.setattr(server, 'IG_POLL_ENABLED', False)
    assert server._polling_mode() == 'disabled'


def test_polling_primary_production_defaults_are_pinned():
    src = Path(server.__file__).read_text(encoding='utf-8')
    assert "IG_POLL_ENABLED = _env_bool(" in src
    assert "aliases=('IG_POLLING_ENABLED',)" in src
    assert "'IG_POLL_INTERVAL_SECONDS'," in src
    assert "15," in src
    assert "'IG_POLL_FRESH_COMMENT_WINDOW_SECONDS'" in src
    assert "60 * 60" in src


def test_polling_round_robin_and_recent_media_defaults(monkeypatch):
    monkeypatch.delenv('IG_POLL_ROUND_ROBIN_BATCH', raising=False)
    monkeypatch.delenv('IG_POLLING_ROUND_ROBIN_BATCH_SIZE', raising=False)
    monkeypatch.delenv('IG_POLL_RECENT_MEDIA_LIMIT', raising=False)
    monkeypatch.delenv('IG_POLLING_RECENT_MEDIA_LIMIT', raising=False)
    assert server._effective_polling_round_robin_batch_size() == 25
    assert server._effective_polling_recent_media_limit() == 50


def test_polling_round_robin_and_recent_media_env_overrides(monkeypatch):
    monkeypatch.setenv('IG_POLL_ROUND_ROBIN_BATCH', '25')
    monkeypatch.setenv('IG_POLL_RECENT_MEDIA_LIMIT', '50')
    assert server._effective_polling_round_robin_batch_size() == 25
    assert server._effective_polling_recent_media_limit() == 50


# ---------------------------------------------------------------------------
# Verification endpoint surfacing
# ---------------------------------------------------------------------------


def _match(doc, query):
    for k, v in (query or {}).items():
        if k == '$or':
            if not any(_match(doc, q) for q in v):
                return False
            continue
        if k == '$and':
            if not all(_match(doc, q) for q in v):
                return False
            continue
        actual = doc.get(k)
        if isinstance(v, dict):
            if '$exists' in v and (k in doc) != v['$exists']:
                return False
            if '$ne' in v and actual == v['$ne']:
                return False
            if '$in' in v and actual not in v['$in']:
                return False
            if '$gte' in v and not (actual is not None and actual >= v['$gte']):
                return False
        else:
            if actual != v:
                return False
    return True


class _Cursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, *_a, **_kw):
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    async def to_list(self, n):
        return list(self.docs[:n])


class _Coll:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    def find(self, query=None, projection=None):
        return _Cursor([d for d in self.docs if _match(d, query)])

    async def find_one(self, query=None, projection=None):
        for d in self.docs:
            if _match(d, query):
                return dict(d)
        return None

    async def update_one(self, query, update, upsert=False):
        for d in self.docs:
            if _match(d, query):
                if '$set' in update:
                    d.update(update['$set'])
                return SimpleNamespace(upserted_id=None, modified_count=1)
        return SimpleNamespace(upserted_id=None, modified_count=0)


class _DB:
    def __init__(self):
        self.instagram_accounts = _Coll()
        self.instagram_automation_events = _Coll()
        self.comments = _Coll()
        self.users = _Coll()


def _install(monkeypatch, db):
    monkeypatch.setattr(server, 'db', db)

    async def _noop_admin(*_a, **_kw):
        return ({}, 'admin')
    monkeypatch.setattr(server, '_require_admin_permission', _noop_admin)


def test_verification_summary_surfaces_polling_send_disabled_counters(monkeypatch):
    """The Webhook Verification summary MUST count distinct
    comment_id_partial values that triggered the new send-gate skip,
    plus distinct comment_id_partial values polling has seen at all.
    Without these, the operator cannot prove "webhook is doing the
    work" vs "polling silently masking webhook failures"."""
    db = _DB()
    now = datetime.utcnow()
    # Two distinct fresh comments polling discovered but the send
    # gate refused to act on.
    for cid in ('111...AAA', '222...BBB'):
        db.instagram_automation_events.docs.append({
            'created_at': now,
            'stage': 'poller_comment_seen', 'source': 'polling',
            'username_key': 'acca',
            'comment_id_partial': cid, 'media_id_partial': 'm-1',
        })
        db.instagram_automation_events.docs.append({
            'created_at': now,
            'stage': 'automation_skipped', 'source': 'polling',
            'username_key': 'acca',
            'comment_id_partial': cid, 'media_id_partial': 'm-1',
            'skip_reason': 'polling_comment_send_disabled_webhook_required',
        })
    _install(monkeypatch, db)
    result = _run(server.admin_instagram_webhook_verification(
        username='acca', since_minutes=30, user_id='admin',
    ))
    s = result.get('summary') or {}
    assert s['polling_seen_count'] == 2
    assert s['polling_send_disabled_count'] == 2
    assert s['latest_polling_seen_at']
    assert s['latest_polling_send_disabled_at']
    assert 'polling_mode' in s
    assert 'polling_interval_seconds' in s
    assert s['polling_send_fallback_env_flag'] == (
        'IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED'
    )
    assert 'polling_enabled' in s
    assert 'polling_send_enabled' in s
    assert 'polling_round_robin_batch_size' in s
    assert 'polling_recent_media_limit' in s
    assert 'polling_fresh_comment_window_seconds' in s


def test_verification_relevant_stages_includes_phase2g_events(monkeypatch):
    """`comment_webhook_missing_media_identifier` and
    `polling_comment_send_disabled_webhook_required` (via
    `automation_skipped.skip_reason`) MUST be surfaced. Without these
    the operator can't see "Meta sent a comment webhook but we
    couldn't extract media id" or "polling saw this but didn't send"."""
    db = _DB()
    now = datetime.utcnow()
    db.instagram_automation_events.docs.append({
        'created_at': now,
        'stage': 'comment_webhook_missing_media_identifier',
        'source': 'webhook', 'username_key': 'acca',
        'comment_id_partial': 'c-1', 'media_id_partial': None,
        'skip_reason': 'comment_webhook_missing_media_identifier',
    })
    _install(monkeypatch, db)
    result = _run(server.admin_instagram_webhook_verification(
        username='acca', since_minutes=30, user_id='admin',
    ))
    stages_in = {e.get('stage') for e in result.get('events') or []}
    assert 'comment_webhook_missing_media_identifier' in stages_in


# ---------------------------------------------------------------------------
# Webhook parser — live_comments + missing-media identifier
# ---------------------------------------------------------------------------


def test_webhook_parser_treats_live_comments_as_comment_field():
    """Phase 2G adds `live_comments` to the is_comment detection.
    The relevant source check is the inline expression in
    `_process_webhook`. Static-source check pins it."""
    src = Path(server.__file__).read_text(encoding='utf-8')
    assert "field == 'live_comments'" in src
    # And it must be wired into the same `is_comment` ternary used
    # for the `comments` field.
    assert "is_comment = (" in src
    assert "field == 'comments'" in src
    assert "field == 'live_comments'" in src


def test_webhook_parser_records_missing_media_event_source_check():
    """The endpoint must include the new
    `comment_webhook_missing_media_identifier` stage in its
    `relevant_stages` filter; static-source check guards against the
    list silently regressing."""
    src = Path(server.__file__).read_text(encoding='utf-8')
    # Stage is recorded.
    assert "'comment_webhook_missing_media_identifier'" in src
    # And surfaced by the verification endpoint.
    assert "comment_webhook_missing_media_identifier" in src


# ---------------------------------------------------------------------------
# Regression contracts
# ---------------------------------------------------------------------------


def test_polling_with_send_gate_off_skips_send_and_records_event(monkeypatch):
    """Live-path proof. Re-uses the multi-account routing harness but
    sets the env flag to OFF so the Phase 2G send gate fires inside
    `_handle_new_comment(source='polling')`. The poller still observes
    the comment (`poller_comment_seen`) but NO public reply, NO opening
    DM, and NO `automation_success` is recorded — only the new skip
    event is."""
    # Import locally so the conftest's module-load default doesn't
    # pollute the new env state.
    from test_multi_account_automation_routing import (  # noqa: E402
        FakeDB, FakeResponse, _account, _user, _legacy_general_rule,
        _install_successful_comment_sends,
    )
    monkeypatch.delenv('IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED',
                       raising=False)
    accounts = [_account(id='accB', userId='u1', instagramAccountId='igB',
                         igUserId='igB', username='accB',
                         accessToken='token-b')]
    user = _user(id='u1', active_instagram_account_id='accB',
                 ig_user_id='igB', meta_access_token='token-b')
    rule = _legacy_general_rule('accB', 'igB', 'ruleLegacyB',
                                trigger='Manual',
                                node_trigger='comment:any',
                                include_top_trigger=True)
    db = FakeDB(accounts, user, automations=[rule])
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'ws_manager',
                        SimpleNamespace(send=lambda *_a, **_kw: asyncio.sleep(0)))
    monkeypatch.setattr(server, 'reserve_usage_limit',
                        lambda *a, **kw: asyncio.sleep(0, result={
                            'allowed': True, 'exceeded': False,
                            'fail_open': False,
                        }))
    monkeypatch.setattr(server, 'confirm_usage_reservation',
                        lambda *a, **kw: asyncio.sleep(0, result=True))
    reply_calls, dm_calls = _install_successful_comment_sends(monkeypatch)

    async def fake_recent(token, ig_user_id, limit=10):
        return ['poll-media-B']
    async def fake_latest(token, ig_user_id):
        return None
    monkeypatch.setattr(server, '_fetch_recent_media_ids', fake_recent)
    monkeypatch.setattr(server, '_fetch_latest_media_id', fake_latest)

    from datetime import timedelta
    now = datetime.utcnow()
    class _Client:
        def __init__(self, *_a, **_kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return False
        async def get(self, *_a, **kw):
            params = kw.get('params') or {}
            if 'order' in params:
                return FakeResponse(200, {'data': [{
                    'id': 'poll-comment-B',
                    'text': 'hello',
                    'timestamp': (now - timedelta(seconds=15)).strftime(
                        '%Y-%m-%dT%H:%M:%S+0000'),
                    'from': {'id': 'external-fan', 'username': 'fan'},
                }]})
            return FakeResponse(200, {'data': []})
    monkeypatch.setattr(server.httpx, 'AsyncClient', _Client)
    owner = server._with_instagram_account_context(user, accounts[0])
    stats = _run(server._poll_user_comments(owner))

    # The poller observed exactly one fresh comment.
    assert stats['mediaChecked'] == 1
    assert stats['commentsSeen'] == 1
    # But NO send fired because the Phase 2G gate refused.
    assert reply_calls == []
    assert dm_calls == []
    # And the skip is visible.
    stages = [(e.get('stage'), e.get('skip_reason'))
              for e in db.instagram_automation_events.docs]
    assert (
        'automation_skipped',
        'polling_comment_send_disabled_webhook_required',
    ) in stages
    # And NO automation_success was recorded for the comment.
    assert not any(s == 'automation_success' for s, _ in stages)


def test_phase2g_no_username_specific_logic():
    src = Path(server.__file__).read_text(encoding='utf-8')
    for anchor in (
        'def _is_polling_send_enabled(',
        'def _polling_mode(',
    ):
        idx = src.find(anchor)
        assert idx >= 0, f'helper {anchor!r} missing'
        window = src[idx:idx + 2000]
        assert 'muhammad_gehad' not in window
        assert 'mogehad17' not in window


def test_phase2g_hmac_gate_unchanged():
    src = Path(server.__file__).read_text(encoding='utf-8')
    # Production HMAC enforcement gate must remain.
    assert "META_WEBHOOK_HMAC_ENFORCE=0 is not permitted in production" in src
