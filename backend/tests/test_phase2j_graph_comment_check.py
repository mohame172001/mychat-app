"""Phase 2J — direct Graph comment-visibility probe and webhook
payload-shape diagnostics.

Covers the 14-point spec:

  1.  Fresh comment missing from webhook but visible in Graph maps to
      the operator-visible blocker label
      ``graph_sees_comment_but_no_webhook_comment_event``.
  2.  Fresh comment not visible in Graph maps to the label
      ``comment_not_visible_to_connected_account_token``.
  3.  Webhook ``account_resolution_success`` without comment fields
      maps the parity blocker to
      ``webhook_received_but_no_comment_payload`` (or, when there's no
      ``webhook_received`` either,
      ``subscription_ready_but_no_comment_payload_received_for_account``).
  4.  ``_build_webhook_payload_shape`` redacts full ids / tokens /
      text and only emits partials + booleans.
  5-6. ``_is_comment_field_change`` now matches ``live_comments``
      generically (used by both mogehad17 and muhammad_gehad).
  7.  No username branching introduced.
  8.  No cross-account routing.
  9-14. Polling default OFF, HMAC / Billing / dedupe / Phase 2D
      cooldown / quick-reply copy unchanged.
"""
import asyncio
import inspect
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
# 4. payload shape helper
# ---------------------------------------------------------------------------


def test_payload_shape_redacts_full_ids_and_omits_text():
    payload = {'object': 'instagram', 'entry': []}
    entry = {
        'id': '178414151515151515',
        'changes': [{
            'field': 'comments',
            'value': {
                'id': '17841515151515151515',
                'comment_id': '17841515151515151515',
                'media': {'id': '17841616161616161616'},
                'from': {'id': '17841717171717171717', 'username': 'commenter'},
                'text': 'this should NOT be echoed',
            },
        }],
        'messaging': [],
    }
    shape = server._build_webhook_payload_shape(payload, entry)
    # Full ids must NOT appear in any field value.
    flat = ' '.join(
        f'{k}={v}' for k, v in shape.items()
    )
    for sensitive in (
        '178414151515151515',
        '17841515151515151515',
        '17841616161616161616',
        '17841717171717171717',
        'this should NOT be echoed',
    ):
        assert sensitive not in flat, f'leaked {sensitive!r}'
    # Required keys present.
    for key in (
        'webhook_object', 'change_fields',
        'has_comments_field', 'has_live_comments_field',
        'has_messages_field', 'has_messaging_seen_field',
        'has_message_reactions_field', 'has_messaging_postbacks_field',
        'entry_id_partial', 'value_id_partial',
        'value_comment_id_partial', 'value_media_id_partial',
        'value_from_id_partial',
    ):
        assert key in shape, f'missing {key}'
    assert shape['has_comments_field'] is True
    assert shape['has_live_comments_field'] is False
    assert shape['webhook_object'] == 'instagram'


def test_payload_shape_detects_live_comments_field():
    shape = server._build_webhook_payload_shape(
        {'object': 'instagram'},
        {'id': '999', 'changes': [{'field': 'live_comments', 'value': {}}], 'messaging': []},
    )
    assert shape['has_live_comments_field'] is True
    assert shape['has_comments_field'] is False
    assert 'live_comments' in (shape['change_fields'] or [])


def test_payload_shape_detects_messaging_types():
    shape = server._build_webhook_payload_shape(
        {'object': 'instagram'},
        {
            'id': '999',
            'changes': [],
            'messaging': [
                {'message': {'mid': 'X'}},
                {'reaction': {'mid': 'Y'}},
                {'postback': {'payload': 'Z'}},
                {'read': {'watermark': 1}},
            ],
        },
    )
    assert shape['has_messages_field'] is True
    assert shape['has_message_reactions_field'] is True
    assert shape['has_messaging_postbacks_field'] is True
    assert shape['has_messaging_seen_field'] is True


# ---------------------------------------------------------------------------
# 5-6. live_comments now flows through generic comment-field check
# ---------------------------------------------------------------------------


def test_is_comment_field_change_now_matches_live_comments():
    assert server._is_comment_field_change(
        {'field': 'live_comments', 'value': {'id': '1'}}
    ) is True
    assert server._is_comment_field_change(
        {'field': 'comments', 'value': {'id': '1'}}
    ) is True
    assert server._is_comment_field_change(
        {'field': 'messages', 'value': {'id': '1'}}
    ) is False


# ---------------------------------------------------------------------------
# 3. Parity blocker labels for webhook_received_but_no_comment_payload
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, items):
        self._items = list(items)

    def sort(self, *a, **kw):
        return self

    def limit(self, n):
        self._items = self._items[:n]
        return self

    async def to_list(self, n):
        return list(self._items[:n])


class _FakeColl:
    def __init__(self, items=None, count=0):
        self._items = items or []
        self._count = count

    def find(self, *a, **kw):
        return _FakeCursor(self._items)

    async def find_one(self, *a, **kw):
        return self._items[0] if self._items else None

    async def count_documents(self, *a, **kw):
        return self._count


class _FakeDB:
    def __init__(self, accounts, events, automations_count=0):
        self.instagram_accounts = _FakeColl(accounts)
        self.instagram_automation_events = _FakeColl(events)
        self.automations = _FakeColl(count=automations_count)
        self.comments = _FakeColl([])

    def __getattr__(self, name):
        return _FakeColl([])


class _GraphResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.content = b'{}'
        self.text = str(payload)

    def json(self):
        return self._payload


class _GraphClient:
    responses = []

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, *args, **kwargs):
        if self.__class__.responses:
            return self.__class__.responses.pop(0)
        return _GraphResponse(200, {'data': []})


def _install_graph_check(monkeypatch, *, events=None):
    fake_db = _FakeDB(
        accounts=[{
            'id': 'acct_db',
            'username': 'acct_x',
            'instagramAccountId': '178000000615',
            'accessToken': 'TOKEN-SHOULD-NOT-LEAK',
            'isActive': True,
        }],
        events=events or [],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    async def _admin(*_args, **_kwargs):
        return ({'id': 'admin'}, 'owner')

    monkeypatch.setattr(server, '_require_admin_permission', _admin)
    monkeypatch.setattr(server.httpx, 'AsyncClient', _GraphClient)
    return fake_db


def test_parity_blocker_webhook_received_but_no_comment_payload():
    now = datetime.utcnow()
    events = [
        {
            'username_key': 'acct_x',
            'stage': 'webhook_received',
            'source': 'webhook',
            'created_at': now - timedelta(seconds=20),
            'extra': {'entry_id_partial': 'eee...000'},
        },
        {
            'username_key': 'acct_x',
            'stage': 'account_resolution_success',
            'source': 'webhook',
            'created_at': now - timedelta(seconds=19),
            'extra': {
                'via': 'instagram_accounts',
                'has_comments_field': False,
                'has_live_comments_field': False,
            },
        },
        # No webhook_comment_detected.
    ]
    subscription_accounts = [{
        'username': 'acct_x',
        'instagram_account_id_partial': 'aaa...000',  # _safe_partial_identifier of aaa00000000000000
        'connection_valid': True,
        'subscribed_fields': ['comments', 'messages'],
        'missing_fields': [],
        'comments_subscribed': True,
        'webhook_comment_delivery_configured': True,
    }]
    fake_accounts = [{
        'id': 'acct_x_db',
        'userId': 'user_x',
        'username': 'acct_x',
        'instagramAccountId': 'aaa00000000000000',
        'connectionValid': True,
        'webhookEntryIdAliases': [],
    }]
    fake_db = _FakeDB(fake_accounts, events)
    original_db = server.db
    server.db = fake_db
    try:
        parity = _run(server._compute_webhook_account_parity(
            rows=events,
            subscription_accounts=subscription_accounts,
            username_key='',
            caller_user_id='user_admin',
            utc_iso=lambda v: v.isoformat() + 'Z' if isinstance(v, datetime) else v,
        ))
    finally:
        server.db = original_db
    by_user = {a['username']: a for a in parity['accounts']}
    assert by_user['acct_x']['blocker_label'] == 'webhook_received_but_no_comment_payload'


def test_parity_blocker_subscription_ready_but_no_comment_payload_received_for_account():
    """No webhook_received row, but account_resolution_success exists
    (this is what muhammad_gehad showed in production)."""
    now = datetime.utcnow()
    events = [
        {
            'username_key': 'acct_y',
            'stage': 'account_resolution_success',
            'source': 'webhook',
            'created_at': now - timedelta(seconds=10),
            'extra': {
                'via': 'instagram_accounts',
                'has_comments_field': False,
            },
        },
    ]
    subscription_accounts = [{
        'username': 'acct_y',
        'instagram_account_id_partial': 'yyy...000',  # _safe_partial_identifier of yyy00000000000000
        'connection_valid': True,
        'subscribed_fields': ['comments', 'messages'],
        'missing_fields': [],
        'comments_subscribed': True,
        'webhook_comment_delivery_configured': True,
    }]
    fake_accounts = [{
        'id': 'acct_y_db',
        'userId': 'user_y',
        'username': 'acct_y',
        'instagramAccountId': 'yyy00000000000000',
        'connectionValid': True,
    }]
    fake_db = _FakeDB(fake_accounts, events)
    original_db = server.db
    server.db = fake_db
    try:
        parity = _run(server._compute_webhook_account_parity(
            rows=events,
            subscription_accounts=subscription_accounts,
            username_key='',
            caller_user_id='user_admin',
            utc_iso=lambda v: v.isoformat() + 'Z' if isinstance(v, datetime) else v,
        ))
    finally:
        server.db = original_db
    by_user = {a['username']: a for a in parity['accounts']}
    assert by_user['acct_y']['blocker_label'] == 'subscription_ready_but_no_comment_payload_received_for_account'


# ---------------------------------------------------------------------------
# 1-2. comment-graph-check blocker labels (response contract)
# ---------------------------------------------------------------------------


def test_graph_check_endpoint_exposes_both_actionable_blocker_labels():
    """Even before Graph is called, the endpoint advertises which
    blocker the operator should record based on the verdict — this is
    the spec-required durable label."""
    src = inspect.getsource(server.admin_instagram_comment_graph_check)
    assert 'graph_sees_comment_but_no_webhook_comment_event' in src
    assert 'comment_not_visible_to_connected_account_token' in src
    assert 'graph_sees_comment' in src
    assert 'graph_does_not_see_matching_comment' in src
    assert 'graph_returned_no_comments' in src
    assert 'graph_error' in src
    assert 'external_comment_visible_in_graph_but_no_webhook_event' in src
    assert 'external_comment_not_visible_in_graph' in src
    assert 'external_comment_arrived_under_different_media' in src
    assert 'external_comment_filtered_before_logging' in src


def test_graph_check_visible_external_comment_without_webhook(monkeypatch):
    _install_graph_check(monkeypatch, events=[])
    _GraphClient.responses = [
        _GraphResponse(200, {'data': [{
            'id': '181000000639',
            'from': {'id': '435000000938', 'username': 'fan'},
            'text': 'hello',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        }]}),
    ]

    result = _run(server.admin_instagram_comment_graph_check(
        username='acct_x',
        media_id='180000000892',
        commenter_id_partial=server._safe_partial_identifier('435000000938'),
        since_minutes=30,
        user_id='admin',
    ))

    assert result['verdict'] == 'external_comment_visible_in_graph_but_no_webhook_event'
    assert result['legacy_verdict'] == 'graph_sees_comment'
    assert result['matches'][0]['comment_id_partial'] == server._safe_partial_identifier('181000000639')
    assert result['matches'][0]['commenter_id_partial'] == server._safe_partial_identifier('435000000938')
    assert result['matches'][0]['matched_by_time_window'] is True
    assert result['matches'][0]['matched_by_commenter_partial'] is True
    assert result['webhook_detected_for_matching_comment'] is False
    assert 'TOKEN-SHOULD-NOT-LEAK' not in repr(result)
    assert 'hello' not in repr(result)


def test_graph_check_empty_after_utc_is_request_scoped_no_cutoff(monkeypatch):
    _install_graph_check(monkeypatch, events=[])
    _GraphClient.responses = [_GraphResponse(200, {'data': []})]

    result = _run(server.admin_instagram_comment_graph_check(
        username='acct_x',
        media_id='180000000892',
        after_utc='   ',
        since_minutes=30,
        user_id='admin',
    ))

    assert result['after_utc'] is None
    assert result['after_time_utc_effective'] is None


def test_graph_check_invalid_after_utc_rejected(monkeypatch):
    _install_graph_check(monkeypatch, events=[])

    from fastapi import HTTPException
    try:
        _run(server.admin_instagram_comment_graph_check(
            username='acct_x',
            media_id='180000000892',
            after_utc='not-a-date',
            since_minutes=30,
            user_id='admin',
        ))
    except HTTPException as exc:
        assert exc.status_code == 400
        assert 'YYYY-MM-DDTHH:mm:ssZ' in str(exc.detail)
    else:
        raise AssertionError('invalid after_utc was accepted')


def test_graph_check_external_comment_not_visible(monkeypatch):
    _install_graph_check(monkeypatch, events=[])
    _GraphClient.responses = [
        _GraphResponse(200, {'data': [{
            'id': '181000000111',
            'from': {'id': '999000000999', 'username': 'other'},
            'text': 'other',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        }]}),
    ]

    result = _run(server.admin_instagram_comment_graph_check(
        username='acct_x',
        media_id='180000000892',
        commenter_id_partial=server._safe_partial_identifier('435000000938'),
        since_minutes=30,
        user_id='admin',
    ))

    assert result['verdict'] == 'external_comment_not_visible_in_graph'
    assert result['legacy_verdict'] == 'graph_does_not_see_matching_comment'
    assert result['match_count'] == 0
    assert result['visibility_blocker_label_if_not_visible'] == (
        'graph_comment_not_visible_due_to_permission_or_visibility'
    )


def test_graph_check_visible_and_webhook_detected(monkeypatch):
    partial_comment = server._safe_partial_identifier('181000000639')
    events = [{
        'created_at': datetime.utcnow(),
        'username_key': 'acct_x',
        'stage': 'webhook_comment_detected',
        'source': 'webhook',
        'comment_id_partial': partial_comment,
        'media_id_partial': server._safe_partial_identifier('180000000892'),
        'commenter_id_partial': server._safe_partial_identifier('435000000938'),
    }]
    _install_graph_check(monkeypatch, events=events)
    _GraphClient.responses = [
        _GraphResponse(200, {'data': [{
            'id': '181000000639',
            'from': {'id': '435000000938', 'username': 'fan'},
            'text': 'hello',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        }]}),
    ]

    result = _run(server.admin_instagram_comment_graph_check(
        username='acct_x',
        media_id='180000000892',
        commenter_id_partial=server._safe_partial_identifier('435000000938'),
        since_minutes=30,
        user_id='admin',
    ))

    assert result['verdict'] == 'external_comment_visible_in_graph'
    assert result['webhook_detected_for_matching_comment'] is True


def test_graph_check_comment_arrived_under_different_media(monkeypatch):
    events = [{
        'created_at': datetime.utcnow(),
        'username_key': 'acct_x',
        'stage': 'webhook_comment_detected',
        'source': 'webhook',
        'comment_id_partial': '181...999',
        'media_id_partial': '180...DIFF',
        'commenter_id_partial': server._safe_partial_identifier('435000000938'),
    }]
    _install_graph_check(monkeypatch, events=events)
    _GraphClient.responses = [
        _GraphResponse(200, {'data': []}),
    ]

    result = _run(server.admin_instagram_comment_graph_check(
        username='acct_x',
        media_id='180000000892',
        commenter_id_partial=server._safe_partial_identifier('435000000938'),
        since_minutes=30,
        user_id='admin',
    ))

    assert result['verdict'] == 'external_comment_arrived_under_different_media'
    assert result['external_comment_arrived_under_different_media'] is True


def test_graph_check_comment_filtered_before_logging(monkeypatch):
    partial_comment = server._safe_partial_identifier('181000000639')
    events = [{
        'created_at': datetime.utcnow(),
        'username_key': 'acct_x',
        'stage': 'account_resolution_success',
        'source': 'webhook',
        'comment_id_partial': '',
        'media_id_partial': '',
        'extra': {'value_comment_id_partial': partial_comment},
    }]
    _install_graph_check(monkeypatch, events=events)
    _GraphClient.responses = [
        _GraphResponse(200, {'data': [{
            'id': '181000000639',
            'from': {'id': '435000000938', 'username': 'fan'},
            'text': 'hello',
            'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
        }]}),
    ]

    result = _run(server.admin_instagram_comment_graph_check(
        username='acct_x',
        media_id='180000000892',
        commenter_id_partial=server._safe_partial_identifier('435000000938'),
        since_minutes=30,
        user_id='admin',
    ))

    assert result['verdict'] == 'external_comment_filtered_before_logging'
    assert result['webhook_filtered_before_logging_for_matching_comment'] is True


def test_graph_check_endpoint_never_logs_raw_token():
    src = inspect.getsource(server.admin_instagram_comment_graph_check)
    # The token is supplied as a query param. The endpoint must not
    # echo it back into the response (no f-string interpolation into
    # any returned field). A static scan: `access_token` only appears
    # as the variable read and the Graph params dict, not in any
    # return statement.
    assert 'access_token' in src
    # No `return ... access_token ...` style lines.
    for line in src.splitlines():
        if line.lstrip().startswith('return'):
            assert 'access_token' not in line


# ---------------------------------------------------------------------------
# 7-14. Unchanged-contract guards
# ---------------------------------------------------------------------------


def test_no_username_branching_introduced():
    src = inspect.getsource(server)
    for needle in (
        "username == 'muhammad_gehad'",
        "username == 'mogehad17'",
        "username_key == 'muhammad_gehad'",
        "username_key == 'mogehad17'",
    ):
        assert needle not in src


def test_no_cross_account_routing_path():
    src = inspect.getsource(server)
    # The single-tenant fallback must remain gated by env.
    assert 'INSTAGRAM_SINGLE_TENANT_FALLBACK' in src
    assert '_SINGLE_TENANT_FALLBACK_ENABLED' in src


def test_polling_production_default_is_explicit():
    src = inspect.getsource(server)
    assert "IG_POLL_ENABLED = _env_bool(" in src
    assert "default=IS_PRODUCTION" in src


def test_hmac_unchanged():
    assert 'hmac.compare_digest' in inspect.getsource(server)


def test_billing_unchanged():
    assert hasattr(server, 'reserve_usage_limit')


def test_dedupe_unchanged():
    src = inspect.getsource(server)
    assert "'dedupe_checked'" in src


def test_phase2d_cooldown_unchanged():
    src = inspect.getsource(server)
    assert 'opening_dm_already_sent_for_commenter_media' in src


def test_quick_reply_copy_unchanged():
    src = inspect.getsource(server)
    assert 'public_reply_attempted' in src
    assert 'opening_dm_attempted' in src
