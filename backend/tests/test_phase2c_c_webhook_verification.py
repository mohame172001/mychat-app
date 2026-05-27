"""Phase 2C-C — admin webhook-verification endpoint tests.

Endpoint:
  GET /api/admin/instagram/webhook-verification
       ?username=<optional>
       &since_minutes=30
       &comment_id_partial=<optional>
       &media_id_partial=<optional>
       &limit=200

Auth: admin-only via `_require_admin_permission(user_id, PERM_USERS_VIEW)`.

What it returns: sanitized summary + filtered list of recent
webhook/polling-related flight events. Nothing the recorder did not
already store — same first-3/last-3 partial identifier shape, no
tokens, no full payloads, no full message text.

These tests cover the spec requirements:
  - auth/admin required
  - redacts identifiers
  - excludes tokens / secrets / full payloads
  - filters by username
  - filters by comment_id_partial / media_id_partial
  - summarises webhook vs polling correctly
"""
import asyncio
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

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
# Minimal in-memory fakes.
# ---------------------------------------------------------------------------


def _match(doc, query):
    for k, v in query.items():
        if k == '$or':
            if not any(_match(doc, q) for q in v):
                return False
            continue
        actual = doc.get(k)
        if isinstance(v, dict):
            if '$in' in v and actual not in v['$in']:
                return False
            if '$gte' in v:
                if not isinstance(actual, datetime) or actual < v['$gte']:
                    return False
        elif actual != v:
            return False
    return True


class _Cursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, key, direction=-1):
        self.docs = sorted(
            self.docs,
            key=lambda d: d.get(key) or datetime.min,
            reverse=(direction == -1),
        )
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
        rows = [d for d in self.docs if not query or _match(d, query)]
        return _Cursor(rows)


class _DB:
    def __init__(self):
        self.instagram_automation_events = _Coll()


def _seed(db, **kwargs):
    db.instagram_automation_events.docs.append(dict(kwargs))


def _ev(stage, *, source='webhook', username='accA',
        comment='c-1', media='m-1', commenter='fan-1',
        when=None, skip_reason=None, extra=None,
        instagram_account_id_partial='ig-A-prt'):
    return {
        'created_at': when or datetime.utcnow(),
        'stage': stage,
        'source': source,
        'username_key': username,
        'instagram_account_id_partial': instagram_account_id_partial,
        'comment_id_partial': comment,
        'media_id_partial': media,
        'commenter_id_partial': commenter,
        'skip_reason': skip_reason,
        'error_code': None,
        'extra': extra or {},
    }


def _install(monkeypatch, db):
    monkeypatch.setattr(server, 'db', db)

    async def _noop_admin(*_a, **_kw):
        return ({'id': 'admin-user'}, 'admin')
    monkeypatch.setattr(server, '_require_admin_permission', _noop_admin)


# ---------------------------------------------------------------------------
# 1. Auth requirement
# ---------------------------------------------------------------------------


def test_endpoint_requires_admin_permission(monkeypatch):
    db = _DB()
    monkeypatch.setattr(server, 'db', db)

    raised = []

    async def deny(*_a, **_kw):
        from fastapi import HTTPException
        raised.append('called')
        raise HTTPException(status_code=403, detail='forbidden')
    monkeypatch.setattr(server, '_require_admin_permission', deny)

    from fastapi import HTTPException
    try:
        _run(server.admin_instagram_webhook_verification(user_id='non-admin'))
    except HTTPException as exc:
        assert exc.status_code == 403
    else:
        raise AssertionError('endpoint did not enforce admin permission')

    assert raised, '_require_admin_permission was not called'


# ---------------------------------------------------------------------------
# 2. Redacts identifiers (only partial fields passed through; full ids
#    that somehow leaked into stored docs must not appear).
# ---------------------------------------------------------------------------


def test_endpoint_only_returns_safe_event_fields(monkeypatch):
    db = _DB()
    now = datetime.utcnow()
    db.instagram_automation_events.docs.append({
        'created_at': now,
        'stage': 'webhook_comment_detected',
        'source': 'webhook',
        'username_key': 'accA',
        'instagram_account_id_partial': 'ig-A-prt',
        'comment_id_partial': 'c-1',
        'media_id_partial': 'm-1',
        'commenter_id_partial': 'fan-1',
        'skip_reason': None,
        'error_code': None,
        # Fields that MUST NOT pass through the endpoint serialiser.
        'access_token': 'SHOULD-NOT-LEAK-TOKEN',
        'full_payload': {'secret': 'SHOULD-NOT-LEAK-PAYLOAD'},
        'raw_text': 'full comment body that must never leak',
        'authorization': 'Bearer SHOULD-NOT-LEAK',
        'extra': {'via': 'media_owner_probe',
                  'access_token': 'ALSO-SHOULD-NOT-LEAK'},
    })
    _install(monkeypatch, db)

    result = _run(server.admin_instagram_webhook_verification(user_id='admin'))
    blob = repr(result)
    for forbidden in (
        'SHOULD-NOT-LEAK-TOKEN', 'SHOULD-NOT-LEAK-PAYLOAD',
        'full comment body that must never leak',
        'Bearer SHOULD-NOT-LEAK', 'ALSO-SHOULD-NOT-LEAK',
    ):
        assert forbidden not in blob, (
            f'token / payload / raw text leaked through endpoint: {forbidden!r}'
        )
    # Safe fields ARE preserved.
    assert result['events'][0]['stage'] == 'webhook_comment_detected'
    assert result['events'][0]['comment_id_partial'] == 'c-1'
    assert result['events'][0]['extra']['via'] == 'media_owner_probe'


# ---------------------------------------------------------------------------
# 3. Excludes tokens / secrets / full payloads (covered by test #2);
#    additionally check that connected_account_samples is bounded and
#    only carries the four documented keys.
# ---------------------------------------------------------------------------


def test_connected_account_samples_bounded_and_sanitized(monkeypatch):
    db = _DB()
    now = datetime.utcnow()
    db.instagram_automation_events.docs.append({
        'created_at': now,
        'stage': 'account_resolution_failed',
        'source': 'webhook',
        'username_key': '',
        'instagram_account_id_partial': 'unknown-id',
        'comment_id_partial': '', 'media_id_partial': '',
        'commenter_id_partial': '',
        'skip_reason': 'no_matching_instagram_account',
        'error_code': None,
        'extra': {
            'connected_account_samples': [
                {
                    'account_id_partial': f'acc-{i}',
                    'username': f'user-{i}',
                    'connection_valid': True,
                    'identity_partials': {'instagramAccountId': f'ig-{i}'},
                    # Fields that should NOT pass through.
                    'accessToken': f'token-{i}-SHOULD-NOT-LEAK',
                    'full_legacy_value': f'long secret {i}',
                }
                for i in range(20)
            ],
            'entry_id_partial': 'unknown-prt',
        },
    })
    _install(monkeypatch, db)

    result = _run(server.admin_instagram_webhook_verification(user_id='admin'))
    blob = repr(result)
    assert 'SHOULD-NOT-LEAK' not in blob
    assert 'long secret' not in blob
    samples = result['events'][0]['extra']['connected_account_samples']
    assert len(samples) <= 10, 'samples must be capped at 10'
    for s in samples:
        assert set(s.keys()) == {
            'account_id_partial', 'username',
            'connection_valid', 'identity_partials',
        }


# ---------------------------------------------------------------------------
# 4. Filters by username.
# ---------------------------------------------------------------------------


def test_filter_by_username(monkeypatch):
    db = _DB()
    now = datetime.utcnow()
    # Stored events use the normalised username_key (lowercased, no @).
    _seed(db, **_ev('webhook_comment_detected', username='acca',
                    comment='c-a', when=now))
    _seed(db, **_ev('webhook_comment_detected', username='accb',
                    comment='c-b', when=now))
    _install(monkeypatch, db)

    # Caller may pass any casing; endpoint normalises to username_key.
    result_a = _run(server.admin_instagram_webhook_verification(
        username='AccA', user_id='admin',
    ))
    result_b = _run(server.admin_instagram_webhook_verification(
        username='@accB', user_id='admin',
    ))
    a_comments = {e['comment_id_partial'] for e in result_a['events']}
    b_comments = {e['comment_id_partial'] for e in result_b['events']}
    assert a_comments == {'c-a'}
    assert b_comments == {'c-b'}


# ---------------------------------------------------------------------------
# 5. Filters by comment_id_partial / media_id_partial.
# ---------------------------------------------------------------------------


def test_filter_by_comment_and_media(monkeypatch):
    db = _DB()
    now = datetime.utcnow()
    _seed(db, **_ev('webhook_comment_detected', comment='c-keep',
                    media='m-keep', when=now))
    _seed(db, **_ev('webhook_comment_detected', comment='c-drop',
                    media='m-drop', when=now))
    _install(monkeypatch, db)

    by_comment = _run(server.admin_instagram_webhook_verification(
        comment_id_partial='c-keep', user_id='admin',
    ))
    assert {e['comment_id_partial'] for e in by_comment['events']} == {'c-keep'}

    by_media = _run(server.admin_instagram_webhook_verification(
        media_id_partial='m-keep', user_id='admin',
    ))
    assert {e['media_id_partial'] for e in by_media['events']} == {'m-keep'}


# ---------------------------------------------------------------------------
# 6. Summary distinguishes webhook vs polling successes and counts the
#    Phase 2C-B fallback / alias self-heal correctly.
# ---------------------------------------------------------------------------


def test_summary_distinguishes_webhook_vs_polling_paths(monkeypatch):
    db = _DB()
    base = datetime.utcnow() - timedelta(minutes=5)

    # Webhook-side: detected → mapped → reached handler → success
    _seed(db, **_ev('webhook_received', when=base, comment='', media=''))
    _seed(db, **_ev('account_resolution_success',
                    when=base + timedelta(seconds=1),
                    comment='', media='',
                    extra={'via': 'entry.id'}))
    _seed(db, **_ev('webhook_comment_detected',
                    when=base + timedelta(seconds=2), comment='c-w'))
    _seed(db, **_ev('rule_loading_finished',
                    when=base + timedelta(seconds=3), comment='c-w',
                    extra={'rules_count': 1}))
    _seed(db, **_ev('rule_match_success',
                    when=base + timedelta(seconds=4), comment='c-w'))
    _seed(db, **_ev('public_reply_sent',
                    when=base + timedelta(seconds=5), comment='c-w'))
    _seed(db, **_ev('opening_dm_sent',
                    when=base + timedelta(seconds=6), comment='c-w'))
    _seed(db, **_ev('automation_success',
                    when=base + timedelta(seconds=7), comment='c-w'))

    # Polling-side: separate comment, ends in success
    _seed(db, **_ev('poller_comment_seen', source='polling',
                    when=base + timedelta(seconds=8), comment='c-p'))
    _seed(db, **_ev('automation_success', source='polling',
                    when=base + timedelta(seconds=9), comment='c-p'))

    # Phase 2C-B fallback used once (media_owner_probe) and alias once.
    _seed(db, **_ev('account_resolution_success',
                    when=base + timedelta(seconds=10),
                    comment='', media='',
                    extra={'via': 'media_owner_probe'}))
    _seed(db, **_ev('account_resolution_success',
                    when=base + timedelta(seconds=11),
                    comment='', media='',
                    extra={'via': 'webhook_entry_id_alias'}))

    # An unmapped webhook (resolution failed)
    _seed(db, **_ev('account_resolution_failed', source='webhook',
                    when=base + timedelta(seconds=12),
                    comment='', media='',
                    skip_reason='no_matching_instagram_account'))

    _install(monkeypatch, db)
    result = _run(server.admin_instagram_webhook_verification(user_id='admin'))
    s = result['summary']

    assert s['webhook_comment_events_seen_count'] == 1
    assert s['webhook_comments_mapped_count'] == 1
    assert s['webhook_comments_unmapped_count'] == 1
    assert s['webhook_comments_reached_handle_count'] == 1
    assert s['webhook_comments_success_count'] == 1
    assert s['polling_success_count'] == 1
    assert s['phase2c_b_fallback_used_count'] == 1
    assert s['alias_self_heal_count'] == 1
    assert s['latest_webhook_comment_at'] is not None
    assert s['latest_polling_success_at'] is not None


# ---------------------------------------------------------------------------
# 7. since_minutes clamps to a safe range.
# ---------------------------------------------------------------------------


def test_since_minutes_clamps(monkeypatch):
    db = _DB()
    _install(monkeypatch, db)
    low = _run(server.admin_instagram_webhook_verification(
        since_minutes=0, user_id='admin',
    ))
    high = _run(server.admin_instagram_webhook_verification(
        since_minutes=999999, user_id='admin',
    ))
    assert low['summary']['since_minutes'] == 1
    assert high['summary']['since_minutes'] == 1440


# ---------------------------------------------------------------------------
# 8. since_minutes window actually filters.
# ---------------------------------------------------------------------------


def test_since_minutes_window_filters(monkeypatch):
    db = _DB()
    old = datetime.utcnow() - timedelta(hours=5)
    recent = datetime.utcnow() - timedelta(minutes=10)
    _seed(db, **_ev('webhook_comment_detected', comment='c-old', when=old))
    _seed(db, **_ev('webhook_comment_detected', comment='c-new', when=recent))
    _install(monkeypatch, db)

    result = _run(server.admin_instagram_webhook_verification(
        since_minutes=30, user_id='admin',
    ))
    comments = {e['comment_id_partial'] for e in result['events']}
    assert 'c-new' in comments
    assert 'c-old' not in comments


# ---------------------------------------------------------------------------
# 9. Sending behaviour is NOT touched.
# ---------------------------------------------------------------------------


def test_endpoint_does_not_call_send_or_dedupe_paths():
    """Static-source: the new endpoint must not call any send / dedupe
    / token-refresh helper. It is read-only diagnostics."""
    src = Path(server.__file__).read_text(encoding='utf-8')
    anchor = 'async def admin_instagram_webhook_verification('
    idx = src.find(anchor)
    assert idx >= 0, 'endpoint definition missing'
    # Body extends until the next `@api.get` or top-level def. ~8000
    # chars is more than enough.
    end_idx = src.find('@api.get(', idx + len(anchor))
    if end_idx < 0:
        end_idx = idx + 8000
    body = src[idx:end_idx]

    # Check for actual call-site shapes, not docstring mentions. The
    # narrative docstring legitimately references _handle_new_comment
    # to explain WHY the diagnostic exists.
    forbidden_calls = (
        'reply_to_ig_comment_detailed(',
        'send_ig_message(',
        'send_ig_dm(',
        'send_ig_quick_reply(',
        'await _handle_new_comment(',
        'execute_flow(',
        'reserve_usage_limit(',
        '_repair_legacy_reply',
        '.insert_one(',
        '.update_one(',
        '.delete_one(',
        '.delete_many(',
    )
    for name in forbidden_calls:
        assert name not in body, (
            f'webhook-verification endpoint must NOT contain {name!r} '
            f'(read-only contract violated)'
        )


# ---------------------------------------------------------------------------
# 10. No username-specific code in the new endpoint body.
# ---------------------------------------------------------------------------


def test_response_carries_diagnostic_metadata(monkeypatch):
    """Phase 2C-C frontend needs `server_now_utc`, `window_start_utc`,
    `window_end_utc`, and `applied_filters` to render the active
    query above the table and to convert timestamps to local time
    without server-clock drift."""
    db = _DB()
    _install(monkeypatch, db)
    result = _run(server.admin_instagram_webhook_verification(
        username='AccA', since_minutes=15,
        comment_id_partial=' c-x ', media_id_partial='m-x',
        limit=42, user_id='admin',
    ))
    for key in (
        'server_now_utc', 'window_start_utc',
        'window_end_utc', 'applied_filters',
    ):
        assert key in result, f'missing top-level {key!r}'
    af = result['applied_filters']
    # Username is normalised via _automation_flight_username_key (lowercased,
    # stripped). comment_id_partial trims whitespace.
    assert af['username'] == 'acca'
    assert af['since_minutes'] == 15
    assert af['comment_id_partial'] == 'c-x'
    assert af['media_id_partial'] == 'm-x'
    assert af['limit'] == 42


def test_no_username_specific_logic_in_endpoint():
    src = Path(server.__file__).read_text(encoding='utf-8')
    anchor = 'async def admin_instagram_webhook_verification('
    idx = src.find(anchor)
    end_idx = src.find('@api.get(', idx + len(anchor))
    if end_idx < 0:
        end_idx = idx + 8000
    body = src[idx:end_idx]
    for needle in ('muhammad_gehad', 'mogehad17', "if username == '",
                   'if username == "'):
        assert needle not in body, (
            f'forbidden username-specific token {needle!r} found in endpoint'
        )
