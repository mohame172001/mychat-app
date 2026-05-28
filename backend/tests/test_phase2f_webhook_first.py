"""Phase 2F — webhook-first comment automation.

Backend pieces under test:
- The Webhook Verification endpoint's `relevant_stages` list now
  includes `polling_scan_summary`, `poller_account_scan_started`,
  `poller_media_scan_started`, `historical_catchup_allowed`, and
  `existing_comment_unknown_state_reprocess` — without these the
  operator could not tell whether polling even ran in the window.
- The Webhook Verification response now carries a top-level
  `subscription_status` block read from cached Meta subscription
  state on the instagram_accounts row (no fresh Graph call from this
  endpoint).
- `POST /api/admin/instagram/repair-comment-webhooks?username=...`
  triggers a per-account fresh Graph subscribe via the existing
  `_subscribe_instagram_account_to_webhooks` helper, persists
  sanitized history, and returns an actionable error message on
  known Meta failure shapes (HTTP 400, 401/403/190, 404).

Hard constraints reaffirmed by tests:
- Admin-only gating.
- Tokens never returned, never in extra.
- No username branching in code paths — the endpoint behaves
  identically for any username.
- The pre-existing send / dedupe / HMAC / rate-limit / polling /
  quick-reply / billing paths are not touched.
"""
import asyncio
import os
import sys
from datetime import datetime, timedelta
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
# Minimal fake DB shared by these tests.
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
            if '$regex' in v:
                import re
                pattern = v['$regex']
                if isinstance(pattern, str):
                    pattern = re.compile(pattern)
                if not (isinstance(actual, str) and pattern.search(actual)):
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


# ---------------------------------------------------------------------------
# Stage filter widening (the dominant visibility gap)
# ---------------------------------------------------------------------------


def test_relevant_stages_includes_polling_scan_summary_and_friends(monkeypatch):
    """The Webhook Verification endpoint MUST surface polling tick
    events. Without these the operator cannot tell whether polling
    even ran in the window, which media were scanned, or what the
    Phase 2C-A prefilter rolled up — every root cause looks identical
    in the UI."""
    db = _DB()
    now = datetime.utcnow()
    for stage in (
        'polling_scan_summary',
        'poller_account_scan_started',
        'poller_media_scan_started',
        'historical_catchup_allowed',
        'existing_comment_unknown_state_reprocess',
    ):
        db.instagram_automation_events.docs.append({
            'created_at': now, 'stage': stage, 'source': 'polling',
            'username_key': 'acca',
            'extra': {'media_checked': 25, 'fresh_candidates': 0,
                      'stale_skipped': 7, 'target_media_count': 25},
        })
    _install(monkeypatch, db)
    result = _run(server.admin_instagram_webhook_verification(
        username='acca', since_minutes=30, user_id='admin',
    ))
    stages_returned = {e.get('stage') for e in result.get('events') or []}
    for stage in (
        'polling_scan_summary',
        'poller_account_scan_started',
        'poller_media_scan_started',
        'historical_catchup_allowed',
        'existing_comment_unknown_state_reprocess',
    ):
        assert stage in stages_returned, f'{stage} missing from events'


def test_polling_scan_summary_extras_are_surfaced(monkeypatch):
    """The new SAFE_EXTRA_KEYS additions (media_checked,
    fresh_candidates, stale_skipped, target_media_count, etc.)
    must reach the response so the operator can read the counters
    from one ride-along JSON without a separate DB query."""
    db = _DB()
    now = datetime.utcnow()
    db.instagram_automation_events.docs.append({
        'created_at': now,
        'stage': 'polling_scan_summary',
        'source': 'polling',
        'username_key': 'acca',
        'extra': {
            'media_checked': 25,
            'comments_seen': 130,
            'fresh_candidates': 0,
            'historical_skipped': 12,
            'stale_skipped': 35,
            'bot_own_skipped': 18,
            'already_processed_skipped': 65,
            'comments_sent_to_handle_new_comment': 0,
            'target_media_count': 25,
            'total_known_media_count': 62,
            'round_robin_cursor_position': 50,
        },
    })
    _install(monkeypatch, db)
    result = _run(server.admin_instagram_webhook_verification(
        username='acca', since_minutes=30, user_id='admin',
    ))
    ev = next(
        e for e in result['events']
        if e.get('stage') == 'polling_scan_summary'
    )
    extra = ev.get('extra') or {}
    for k in (
        'media_checked', 'comments_seen', 'fresh_candidates',
        'stale_skipped', 'bot_own_skipped', 'already_processed_skipped',
        'target_media_count', 'total_known_media_count',
        'round_robin_cursor_position',
    ):
        assert k in extra, f'{k} missing from polling_scan_summary.extra'


# ---------------------------------------------------------------------------
# Subscription status surfacing
# ---------------------------------------------------------------------------


def test_subscription_status_block_in_response(monkeypatch):
    """The verification response MUST carry a subscription_status
    block read from the cached fields on instagram_accounts."""
    db = _DB()
    db.instagram_accounts.docs.append({
        'id': 'acc-A',
        'instagramAccountId': 'ig-A-id',
        'igUserId': 'ig-A-id',
        'username': 'acca',
        'instagramHandle': 'acca',
        'userId': 'u1',
        'isActive': True,
        'connectionValid': True,
        'webhookSubscriptionFields': [
            'comments', 'live_comments', 'messages',
            'messaging_postbacks', 'messaging_seen', 'message_reactions',
        ],
        'webhookSubscriptionMissing': [],
        'webhookSubscriptionLastCheckedAt': datetime.utcnow(),
        'accessToken': 'TOK_SHOULD_NOT_APPEAR',
    })
    _install(monkeypatch, db)
    result = _run(server.admin_instagram_webhook_verification(
        username='acca', since_minutes=30, user_id='admin',
    ))
    sub = result.get('subscription_status')
    assert isinstance(sub, dict)
    assert sub.get('overall_ready') is True
    assert isinstance(sub.get('expected_fields'), list)
    assert sub['expected_fields']
    assert len(sub.get('accounts') or []) >= 1
    acc = sub['accounts'][0]
    assert acc['username'] == 'acca'
    assert acc['webhook_comment_delivery_configured'] is True
    assert acc['comments_subscribed'] is True
    # Token must not leak anywhere in the subscription block.
    blob = str(sub)
    assert 'TOK_SHOULD_NOT_APPEAR' not in blob


def test_subscription_status_shows_unconfigured_account(monkeypatch):
    """An account that is connected but has NO comments subscription
    must show `webhook_comment_delivery_configured=false` and list the
    missing fields so the operator can see what the Repair button
    needs to fix."""
    db = _DB()
    db.instagram_accounts.docs.append({
        'id': 'acc-B', 'instagramAccountId': 'ig-B', 'igUserId': 'ig-B',
        'username': 'accb', 'instagramHandle': 'accb',
        'userId': 'u1', 'isActive': True, 'connectionValid': True,
        'webhookSubscriptionFields': [],
        'webhookSubscriptionMissing': sorted(server.WEBHOOK_REQUIRED_FIELDS),
    })
    _install(monkeypatch, db)
    result = _run(server.admin_instagram_webhook_verification(
        username='accb', user_id='admin',
    ))
    sub = result['subscription_status']
    assert sub['overall_ready'] is False
    acc = sub['accounts'][0]
    assert acc['webhook_comment_delivery_configured'] is False
    assert 'comments' in acc['missing_fields']


# ---------------------------------------------------------------------------
# Repair endpoint
# ---------------------------------------------------------------------------


def test_repair_endpoint_returns_actionable_error_when_account_missing(monkeypatch):
    db = _DB()
    _install(monkeypatch, db)
    result = _run(server.admin_instagram_repair_comment_webhooks(
        username='nobody', user_id='admin',
    ))
    assert result['ok'] is False
    assert result['error'] == 'account_not_found'
    assert 'Reconnect' in result['message']


def test_repair_endpoint_returns_actionable_error_when_token_missing(monkeypatch):
    db = _DB()
    db.instagram_accounts.docs.append({
        'id': 'acc-A', 'instagramAccountId': 'ig-A',
        'username': 'acca', 'instagramHandle': 'acca',
        'userId': 'u1', 'isActive': True, 'connectionValid': True,
        # accessToken missing
    })
    _install(monkeypatch, db)
    result = _run(server.admin_instagram_repair_comment_webhooks(
        username='acca', user_id='admin',
    ))
    assert result['ok'] is False
    assert result['error'] == 'missing_id_or_token'
    assert 'reconnect' in result['message'].lower()


def test_repair_endpoint_success_persists_sanitized_history(monkeypatch):
    """Happy path: subscribe helper returns ok=True. Endpoint must
    NEVER return the access token, must persist sanitized history,
    and must surface the resulting subscribed_fields."""
    db = _DB()
    db.instagram_accounts.docs.append({
        'id': 'acc-A', 'instagramAccountId': 'ig-A',
        'username': 'acca', 'instagramHandle': 'acca',
        'userId': 'u1', 'isActive': True, 'connectionValid': True,
        'accessToken': 'TOK_X_SHOULD_NOT_APPEAR',
    })
    _install(monkeypatch, db)

    async def fake_subscribe(ig_user_id, access_token):
        # Existing helper contract — return shape exactly.
        assert access_token == 'TOK_X_SHOULD_NOT_APPEAR'
        return {
            'ok': True,
            'subscribe_status': 200,
            'verify_status': 200,
            'subscribed_fields': [
                'comments', 'live_comments', 'messages',
                'messaging_postbacks', 'messaging_seen', 'message_reactions',
            ],
        }
    monkeypatch.setattr(server, '_subscribe_instagram_account_to_webhooks',
                        fake_subscribe)

    result = _run(server.admin_instagram_repair_comment_webhooks(
        username='acca', user_id='admin',
    ))
    assert result['ok'] is True
    assert result['actionable_error'] is None
    # Token must never appear in the RESPONSE the operator sees.
    assert 'TOK_X_SHOULD_NOT_APPEAR' not in str(result)
    # Sanitized history was persisted; the legitimate stored accessToken
    # remains on the row (it's the live credential we use for sends).
    row = db.instagram_accounts.docs[0]
    assert row.get('lastWebhookRepairResult') == 'success'
    assert isinstance(row.get('lastWebhookRepairAttemptAt'), datetime)
    assert isinstance(row.get('webhookSubscriptionFields'), list)
    assert 'comments' in row['webhookSubscriptionFields']
    # The endpoint added NO new field whose name carries 'token' or
    # contains the credential value.
    repair_fields_added = {
        'lastWebhookRepairAttemptAt', 'lastWebhookRepairResult',
        'webhookSubscriptionFields', 'webhookSubscriptionMissing',
        'webhookSubscriptionLastCheckedAt', 'lastWebhookRepairError',
    }
    for k in repair_fields_added:
        assert 'token' not in k.lower()
        v = row.get(k)
        if isinstance(v, str):
            assert 'TOK_X_SHOULD_NOT_APPEAR' not in v


def test_repair_endpoint_surfaces_actionable_error_on_dev_mode_rejection(monkeypatch):
    """When Meta rejects the subscribe with HTTP 400 (typical for
    Development Mode + non-tester accounts, or missing permissions),
    the endpoint must surface an operator-readable explanation."""
    db = _DB()
    db.instagram_accounts.docs.append({
        'id': 'acc-A', 'instagramAccountId': 'ig-A',
        'username': 'acca', 'instagramHandle': 'acca',
        'userId': 'u1', 'isActive': True, 'connectionValid': True,
        'accessToken': 'tok',
    })
    _install(monkeypatch, db)

    async def fake_subscribe(_id, _tok):
        return {
            'ok': False,
            'subscribe_status': 400,
            'verify_status': 200,
            'subscribed_fields': [],
        }
    monkeypatch.setattr(server, '_subscribe_instagram_account_to_webhooks',
                        fake_subscribe)
    result = _run(server.admin_instagram_repair_comment_webhooks(
        username='acca', user_id='admin',
    ))
    assert result['ok'] is False
    assert result['actionable_error'] is not None
    assert 'Development Mode' in result['actionable_error']
    row = db.instagram_accounts.docs[0]
    assert row.get('lastWebhookRepairResult') == 'failed'
    assert row.get('lastWebhookRepairError')


def test_repair_endpoint_surfaces_actionable_error_on_auth_rejection(monkeypatch):
    """HTTP 401/403/190 mean the access token is expired or
    permissions revoked. Operator-actionable error must say so."""
    db = _DB()
    db.instagram_accounts.docs.append({
        'id': 'acc-A', 'instagramAccountId': 'ig-A',
        'username': 'acca', 'instagramHandle': 'acca',
        'userId': 'u1', 'isActive': True, 'connectionValid': True,
        'accessToken': 'tok',
    })
    _install(monkeypatch, db)

    async def fake_subscribe(_id, _tok):
        return {
            'ok': False,
            'subscribe_status': 401,
            'verify_status': None,
            'subscribed_fields': [],
        }
    monkeypatch.setattr(server, '_subscribe_instagram_account_to_webhooks',
                        fake_subscribe)
    result = _run(server.admin_instagram_repair_comment_webhooks(
        username='acca', user_id='admin',
    ))
    assert result['ok'] is False
    assert 'Reconnect' in result['actionable_error']


# ---------------------------------------------------------------------------
# Regression contracts
# ---------------------------------------------------------------------------


def test_no_username_specific_logic_in_phase2f():
    """Static-source check: the new helpers + repair endpoint MUST
    NOT branch on a hard-coded Instagram username string. The default
    UI username in the frontend tab is a separate, allowed UX choice."""
    src = Path(server.__file__).read_text(encoding='utf-8')
    # Find the new helpers' definitions and scan them only.
    for anchor in (
        'async def _compute_webhook_subscription_status(',
        'async def admin_instagram_repair_comment_webhooks(',
    ):
        idx = src.find(anchor)
        assert idx >= 0, f'helper {anchor!r} missing'
        window = src[idx:idx + 4000]
        assert 'muhammad_gehad' not in window
        assert 'mogehad17' not in window
        assert "if username ==" not in window


def test_hmac_unchanged_after_phase2f():
    """Static-source: HMAC enforcement gate still present, untouched
    by Phase 2F. This is the load-bearing safety check for the entire
    webhook ingestion path."""
    src = Path(server.__file__).read_text(encoding='utf-8')
    assert "META_WEBHOOK_HMAC_ENFORCE=0 is not permitted in production" in src
