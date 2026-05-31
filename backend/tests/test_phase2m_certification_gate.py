"""Phase 2M — comment-webhook certification gate.

The product now refuses to activate a comment automation rule on an
Instagram account that has not been certified ready by the backend.
This file covers the 23-point spec:

  1.  Account connect runs certification (via the sync wire-in).
  2.  Account activation runs certification (via the activate wire-in).
  3.  Admin Repair runs certification (delegates to the helper).
  4.  Certification normalizes identity (delegates to Phase 2K helper).
  5.  Certification adds webhookEntryIdAliases generically.
  6.  Certification fresh-verifies subscribed fields (calls subscribe).
  7.  Certification repairs missing comments/live_comments (subscribe).
  8.  Certification repairs safe empty/null rule binding (Phase 2K).
  9.  Certification repairs media catalog binding (Phase 2K).
  10. Comment automation activation is blocked if account is not ready.
  11. Existing comment rule is paused if account flips to not ready.
  12. Ready account can activate comment automation.
  13. Working-shape account stays ready (idempotent).
  14. Missing-shape account is repaired and certified ready when subs ok.
  15. Meta-delivery-blocked status set when comments subscribed but only
      non-comment webhooks arrive in the lookback window.
  16-23. Unchanged-contract guards.
"""
import asyncio
import inspect
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

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
# In-memory fake DB
# ---------------------------------------------------------------------------


class _UR:
    def __init__(self, n):
        self.modified_count = n


class _Coll:
    def __init__(self, items=None):
        self.items = list(items or [])

    def find(self, query=None, projection=None):
        out = []
        for it in self.items:
            ok = True
            if isinstance(query, dict):
                for k, v in query.items():
                    if k.startswith('$'):
                        continue
                    if isinstance(v, dict) and '$gte' in v:
                        if it.get(k) is None or it.get(k) < v['$gte']:
                            ok = False
                            break
                    elif it.get(k) != v:
                        ok = False
                        break
            if ok:
                out.append(it)
        class _C:
            def __init__(self, rows): self._r = rows
            def sort(self, *a, **kw): return self
            def limit(self, n): self._r = self._r[:n]; return self
            async def to_list(self, n): return list(self._r[:n])
        return _C(out)

    async def find_one(self, query=None, projection=None):
        if not query:
            return self.items[0] if self.items else None
        for it in self.items:
            ok = True
            for k, v in query.items():
                if k.startswith('$'):
                    continue
                if isinstance(v, dict) and '$ne' in v:
                    if it.get(k) == v['$ne']:
                        ok = False
                        break
                elif it.get(k) != v:
                    ok = False
                    break
            if ok:
                return it
        return None

    async def update_one(self, query, update):
        for it in self.items:
            if all(
                it.get(k) == v for k, v in query.items() if not k.startswith('$')
            ):
                for k, v in (update.get('$set') or {}).items():
                    it[k] = v
                add = (update.get('$addToSet') or {}).get('webhookEntryIdAliases')
                if add:
                    existing = list(it.get('webhookEntryIdAliases') or [])
                    items_to_add = (
                        add.get('$each')
                        if isinstance(add, dict) and '$each' in add
                        else [add]
                    )
                    for v in items_to_add:
                        if v not in existing:
                            existing.append(v)
                    it['webhookEntryIdAliases'] = existing
                return _UR(1)
        return _UR(0)

    async def update_many(self, query, update):
        modified = 0
        for it in self.items:
            ok = True
            # match owner clauses
            for clause in query.get('$or') or []:
                pass  # fall through to detailed check below
            # owner: userId or user_id
            owner_ok = True
            top_or = query.get('$or')
            if top_or:
                owner_ok = any(it.get(k) == v for c in top_or for k, v in c.items())
            if not owner_ok:
                continue
            # status filter
            if 'status' in query and it.get('status') != query['status']:
                continue
            # nested $and clauses (instagram match + comment rule type)
            and_clauses = query.get('$and') or []
            and_ok = True
            for cl in and_clauses:
                inner_or = cl.get('$or')
                if not inner_or:
                    continue
                if not any(
                    (
                        (v == {'$in': ['', None]} and it.get(k) in ('', None))
                        or (v == {'$exists': False} and k not in it)
                        or (it.get(k) == v)
                    )
                    for o in inner_or for k, v in o.items()
                ):
                    and_ok = False
                    break
            if not and_ok:
                continue
            for k, v in (update.get('$set') or {}).items():
                it[k] = v
            modified += 1
        return _UR(modified)


class _DB:
    def __init__(self, accounts=None, automations=None, media=None, events=None, users=None):
        self.instagram_accounts = _Coll(accounts)
        self.automations = _Coll(automations)
        self.instagram_media_catalog = _Coll(media)
        self.instagram_automation_events = _Coll(events)
        self.users = _Coll(users)

    def __getattr__(self, name):
        return _Coll([])


class _SubStub:
    def __init__(
        self,
        subscribed=None,
        status=200,
        verify_status=None,
        readback_failure_reason=None,
        readback_response_keys=None,
    ):
        self.subscribed = list(
            subscribed if subscribed is not None
            else server.WEBHOOK_REQUIRED_FIELDS
        )
        self.status = status
        self.verify_status = status if verify_status is None else verify_status
        self.readback_failure_reason = readback_failure_reason
        self.readback_response_keys = list(readback_response_keys or [])

    async def __call__(self, ig_user_id, access_token):
        return {
            'ok': self.status == 200,
            'subscribe_status': self.status,
            'verify_status': self.verify_status,
            'subscribed_fields': list(self.subscribed),
            'readback_endpoint_kind': 'instagram_subscribed_apps',
            'readback_object_id_partial': server._safe_partial_identifier(ig_user_id),
            'readback_http_status': self.verify_status,
            'readback_failure_reason': self.readback_failure_reason,
            'readback_response_keys': list(self.readback_response_keys),
        }


class _FakeGraphResponse:
    def __init__(self, status_code=200, body=None, json_raises=False):
        self.status_code = status_code
        self._body = body
        self._json_raises = json_raises

    def json(self):
        if self._json_raises:
            raise ValueError('bad json')
        return self._body


class _FakeAsyncClient:
    calls = []
    post_response = _FakeGraphResponse(200, {'success': True})
    get_response = _FakeGraphResponse(200, {'data': []})

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def post(self, url, params=None):
        type(self).calls.append(('post', url, dict(params or {})))
        return type(self).post_response

    async def get(self, url, params=None):
        type(self).calls.append(('get', url, dict(params or {})))
        return type(self).get_response


# ---------------------------------------------------------------------------
# 4-9, 13, 14. Certification produces ready state for working-shape input
# ---------------------------------------------------------------------------


def test_13_working_shape_account_certified_ready(monkeypatch):
    account = {
        'id': 'acct_ready',
        'userId': 'owner_R',
        'username': 'working_acc',
        'instagramAccountId': '17841500000000777',
        'igUserId': '17841500000000777',
        'webhookEntryIdAliases': ['17841500000000777'],
        'accessToken': 'fake-token',
        'connectionValid': True,
    }
    db = _DB(accounts=[dict(account)])
    server.db = db
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(server.WEBHOOK_REQUIRED_FIELDS),
    )
    report = _run(
        server.certify_instagram_account_for_comment_webhooks(account, reason='sync')
    )
    assert report['comment_webhook_ready'] is True
    assert report['comment_webhook_status'] == 'ready'
    persisted = db.instagram_accounts.items[0]
    assert persisted['commentWebhookReady'] is True
    assert persisted['commentWebhookStatus'] == 'ready'
    assert persisted['commentWebhookCertifiedAt']


def test_14_missing_shape_account_is_repaired_then_certified_ready(monkeypatch):
    account = {
        'id': 'acct_missing',
        'userId': 'owner_M',
        'username': 'missing_acc',
        'instagramAccountId': '17841500000000999',
        # No igUserId mirror, no aliases.
        'accessToken': 'fake-token',
        'connectionValid': True,
    }
    db = _DB(accounts=[dict(account)])
    server.db = db
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(server.WEBHOOK_REQUIRED_FIELDS),
    )
    report = _run(
        server.certify_instagram_account_for_comment_webhooks(account, reason='sync')
    )
    assert report['comment_webhook_ready'] is True
    persisted = db.instagram_accounts.items[0]
    assert persisted['igUserId'] == '17841500000000999'
    assert '17841500000000999' in (persisted.get('webhookEntryIdAliases') or [])


def test_duplicate_account_rows_repaired_to_canonical_same_owner(monkeypatch):
    stale = {
        'id': 'acct_stale',
        'userId': 'owner_DUP',
        'username': 'dup_acc_old',
        'instagramAccountId': '17841500000001000',
        'igUserId': '17841500000001000',
        'accessToken': '',
        'connectionValid': False,
        'isActive': True,
    }
    canonical = {
        'id': 'acct_canonical',
        'userId': 'owner_DUP',
        'username': 'dup_acc_new',
        'instagramAccountId': '17841500000001000',
        'igUserId': '17841500000001000',
        'webhookEntryIdAliases': ['17841500000001000'],
        'accessToken': 'fresh-token',
        'connectionValid': True,
        'isActive': True,
    }
    user = {
        'id': 'owner_DUP',
        'active_instagram_account_id': 'acct_stale',
    }
    automations = [
        {
            'id': 'rule_stale',
            'userId': 'owner_DUP',
            'status': 'active',
            'event_type': 'comment',
            'instagramAccountDbId': 'acct_stale',
            'instagram_account_id': 'acct_stale',
            'instagramAccountId': '17841500000001000',
        },
    ]
    media = [
        {
            'id': 'media_stale',
            'userId': 'owner_DUP',
            'instagramAccountDbId': 'acct_stale',
            'instagram_account_id': 'acct_stale',
            'instagramAccountId': '17841500000001000',
        },
    ]
    db = _DB(
        accounts=[stale, canonical],
        automations=automations,
        media=media,
        users=[user],
    )
    server.db = db
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(server.WEBHOOK_REQUIRED_FIELDS),
    )
    report = _run(
        server.certify_instagram_account_for_comment_webhooks(stale, reason='sync')
    )
    assert report['comment_webhook_status'] == 'ready'
    assert report['duplicate_account_row_detected'] is True
    assert report['stale_account_row_detected'] is True
    assert report['canonical_account_id_partial'] == server._safe_partial_identifier('acct_canonical')
    by_id = {row['id']: row for row in db.instagram_accounts.items}
    assert by_id['acct_stale']['connectionValid'] is False
    assert by_id['acct_stale']['staleDuplicateOf'] == 'acct_canonical'
    assert db.users.items[0]['active_instagram_account_id'] == 'acct_canonical'
    assert db.automations.items[0]['instagramAccountDbId'] == 'acct_canonical'
    assert db.instagram_media_catalog.items[0]['instagramAccountDbId'] == 'acct_canonical'


def _admin_account(**overrides):
    base = {
        'id': 'acct_admin',
        'userId': 'owner_ADMIN',
        'username': 'admin_acc',
        'instagramAccountId': '17841500000001111',
        'igUserId': '17841500000001111',
        'webhookEntryIdAliases': ['17841500000001111'],
        'accessToken': 'active-token',
        'connectionValid': True,
        'isActive': True,
    }
    base.update(overrides)
    return base


async def _grant_admin(*args, **kwargs):
    return None


async def _fresh_scope_proof(account):
    scopes = [
        'instagram_business_basic',
        'instagram_business_manage_messages',
        'instagram_business_manage_comments',
    ]
    account['grantedScopes'] = scopes
    account['grantedScopesTokenPrefix'] = server._token_prefix(
        account.get('accessToken') or ''
    )
    account['grantedScopesDebugTokenWorks'] = True
    account['grantedScopesMatchesIgAppId'] = True
    return scopes


def test_admin_repair_http_200_readback_fields_present_returns_ready(monkeypatch):
    account = _admin_account()
    db = _DB(accounts=[account])
    server.db = db
    monkeypatch.setattr(server, '_require_admin_permission', _grant_admin)
    monkeypatch.setattr(server, '_refresh_account_granted_scopes_via_graph', _fresh_scope_proof)
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(server.WEBHOOK_REQUIRED_FIELDS, status=200, verify_status=200),
    )

    result = _run(
        server.admin_instagram_repair_comment_webhooks('admin_acc', user_id='owner_ADMIN')
    )

    assert result['ok'] is True
    assert result['comment_webhook_ready'] is True
    assert result['comment_webhook_status'] == 'ready'
    assert result['missing_fields'] == []
    persisted = db.instagram_accounts.items[0]
    assert persisted['commentWebhookReady'] is True
    assert persisted['webhookSubscriptionMissing'] == []


def test_admin_repair_http_200_readback_missing_is_not_success(monkeypatch):
    account = _admin_account()
    db = _DB(accounts=[account])
    server.db = db
    monkeypatch.setattr(server, '_require_admin_permission', _grant_admin)
    monkeypatch.setattr(server, '_refresh_account_granted_scopes_via_graph', _fresh_scope_proof)
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(['messages'], status=200, verify_status=200),
    )

    result = _run(
        server.admin_instagram_repair_comment_webhooks('admin_acc', user_id='owner_ADMIN')
    )

    assert result['ok'] is False
    assert result['subscribe_status'] == 200
    assert result['verify_status'] == 200
    assert result['comment_webhook_status'] == 'repair_required'
    assert (
        result['comment_webhook_blocker']
        == 'repair_http_200_but_graph_readback_missing_fields'
    )
    assert 'comments' in result['missing_fields']
    persisted = db.instagram_accounts.items[0]
    assert persisted['lastWebhookRepairResult'] == 'failed'
    assert persisted['commentWebhookBlocker'] == 'repair_http_200_but_graph_readback_missing_fields'


def test_admin_repair_graph_readback_failure_is_explicit(monkeypatch):
    account = _admin_account()
    db = _DB(accounts=[account])
    server.db = db
    monkeypatch.setattr(server, '_require_admin_permission', _grant_admin)
    monkeypatch.setattr(server, '_refresh_account_granted_scopes_via_graph', _fresh_scope_proof)
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(
            [],
            status=200,
            verify_status=500,
            readback_failure_reason='graph_readback_http_failed',
            readback_response_keys=['error'],
        ),
    )

    result = _run(
        server.admin_instagram_repair_comment_webhooks('admin_acc', user_id='owner_ADMIN')
    )

    assert result['ok'] is False
    assert result['subscribe_status'] == 200
    assert result['verify_status'] == 500
    assert result['comment_webhook_status'] == 'repair_required'
    assert result['comment_webhook_blocker'] == 'repair_graph_readback_failed:graph_readback_http_failed'
    assert result['readback_failure_reason'] == 'graph_readback_http_failed'
    assert result['readback_response_keys'] == ['error']


def test_admin_repair_graph_readback_permission_denied_is_precise(monkeypatch):
    account = _admin_account()
    db = _DB(accounts=[account])
    server.db = db
    monkeypatch.setattr(server, '_require_admin_permission', _grant_admin)
    monkeypatch.setattr(server, '_refresh_account_granted_scopes_via_graph', _fresh_scope_proof)
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(
            [],
            status=200,
            verify_status=403,
            readback_failure_reason='graph_readback_permission_denied',
            readback_response_keys=['error'],
        ),
    )

    result = _run(
        server.admin_instagram_repair_comment_webhooks('admin_acc', user_id='owner_ADMIN')
    )

    assert result['ok'] is False
    assert result['comment_webhook_blocker'] == (
        'repair_graph_readback_failed:graph_readback_permission_denied'
    )
    assert result['readback_failure_reason'] == 'graph_readback_permission_denied'
    assert 'readback failed: graph_readback_permission_denied' in result['actionable_error']


def test_admin_repair_graph_readback_wrong_object_is_precise(monkeypatch):
    account = _admin_account()
    db = _DB(accounts=[account])
    server.db = db
    monkeypatch.setattr(server, '_require_admin_permission', _grant_admin)
    monkeypatch.setattr(server, '_refresh_account_granted_scopes_via_graph', _fresh_scope_proof)
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(
            [],
            status=200,
            verify_status=404,
            readback_failure_reason='graph_readback_wrong_object_id',
            readback_response_keys=['error'],
        ),
    )

    result = _run(
        server.admin_instagram_repair_comment_webhooks('admin_acc', user_id='owner_ADMIN')
    )

    assert result['ok'] is False
    assert result['comment_webhook_blocker'] == (
        'repair_graph_readback_failed:graph_readback_wrong_object_id'
    )
    assert result['readback_failure_reason'] == 'graph_readback_wrong_object_id'


def test_admin_repair_graph_readback_unexpected_shape_is_precise(monkeypatch):
    account = _admin_account()
    db = _DB(accounts=[account])
    server.db = db
    monkeypatch.setattr(server, '_require_admin_permission', _grant_admin)
    monkeypatch.setattr(server, '_refresh_account_granted_scopes_via_graph', _fresh_scope_proof)
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(
            [],
            status=200,
            verify_status=200,
            readback_failure_reason='graph_readback_unexpected_shape',
            readback_response_keys=['unexpected'],
        ),
    )

    result = _run(
        server.admin_instagram_repair_comment_webhooks('admin_acc', user_id='owner_ADMIN')
    )

    assert result['ok'] is False
    assert result['comment_webhook_blocker'] == (
        'repair_graph_readback_failed:graph_readback_unexpected_shape'
    )
    assert result['readback_response_keys'] == ['unexpected']
    assert 'access_token' not in str(result)


def test_subscribe_helper_uses_same_object_and_token_for_post_and_readback(monkeypatch):
    _FakeAsyncClient.calls = []
    _FakeAsyncClient.post_response = _FakeGraphResponse(200, {'success': True})
    _FakeAsyncClient.get_response = _FakeGraphResponse(
        200,
        {'data': [{'subscribed_fields': list(server.WEBHOOK_REQUIRED_FIELDS)}]},
    )
    monkeypatch.setattr(server.httpx, 'AsyncClient', _FakeAsyncClient)

    result = _run(
        server._subscribe_instagram_account_to_webhooks('17841500000001111', 'active-token')
    )

    assert result['ok'] is True
    assert result['readback_failure_reason'] is None
    assert _FakeAsyncClient.calls[0][1] == _FakeAsyncClient.calls[1][1]
    assert _FakeAsyncClient.calls[0][2]['access_token'] == 'active-token'
    assert _FakeAsyncClient.calls[1][2]['access_token'] == 'active-token'
    assert '17841500000001111' in _FakeAsyncClient.calls[0][1]


def test_subscribe_helper_classifies_readback_403_without_payload_leak(monkeypatch):
    _FakeAsyncClient.calls = []
    _FakeAsyncClient.post_response = _FakeGraphResponse(200, {'success': True})
    _FakeAsyncClient.get_response = _FakeGraphResponse(
        403,
        {
            'error': {
                'code': 10,
                'message': 'Permission denied for this object',
                'fbtrace_id': 'secret-trace',
            },
            'access_token': 'must-not-leak',
        },
    )
    monkeypatch.setattr(server.httpx, 'AsyncClient', _FakeAsyncClient)

    result = _run(
        server._subscribe_instagram_account_to_webhooks('17841500000001111', 'active-token')
    )

    assert result['ok'] is False
    assert result['verify_status'] == 403
    assert result['readback_failure_reason'] == 'graph_readback_permission_denied'
    assert result['readback_response_keys'] == ['access_token', 'error']
    assert result['graph_error_code'] == 10
    assert 'must-not-leak' not in str(result)
    assert 'secret-trace' not in str(result)


def test_subscribe_helper_classifies_unexpected_readback_shape(monkeypatch):
    _FakeAsyncClient.calls = []
    _FakeAsyncClient.post_response = _FakeGraphResponse(200, {'success': True})
    _FakeAsyncClient.get_response = _FakeGraphResponse(200, {'unexpected': []})
    monkeypatch.setattr(server.httpx, 'AsyncClient', _FakeAsyncClient)

    result = _run(
        server._subscribe_instagram_account_to_webhooks('17841500000001111', 'active-token')
    )

    assert result['ok'] is False
    assert result['readback_failure_reason'] == 'graph_readback_missing_data_array'
    assert result['readback_response_keys'] == ['unexpected']


async def _scope_refresh_inconclusive(account):
    account['commentPermissionScopeProofFailureReason'] = 'debug_token_missing_scopes_fields'
    return None


def test_admin_repair_active_token_scope_inconclusive_not_missing_permission(monkeypatch):
    account = _admin_account()
    db = _DB(accounts=[account])
    server.db = db
    monkeypatch.setattr(server, '_require_admin_permission', _grant_admin)
    monkeypatch.setattr(server, '_refresh_account_granted_scopes_via_graph', _scope_refresh_inconclusive)
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(server.WEBHOOK_REQUIRED_FIELDS, status=200, verify_status=200),
    )

    result = _run(
        server.admin_instagram_repair_comment_webhooks('admin_acc', user_id='owner_ADMIN')
    )

    assert result['ok'] is True
    assert result['comment_webhook_ready'] is True
    assert (
        result['comment_webhook_status']
        == 'subscription_verified_scope_proof_inconclusive'
    )
    assert result['comment_webhook_blocker'] == 'active_token_scope_proof_inconclusive'
    assert result['comment_webhook_blocker'] != 'comment_permission_not_granted'
    assert result['actionable_error'] is None
    assert result['comment_permission_scope_proof_failure_reason'] == (
        'debug_token_missing_scopes_fields'
    )
    persisted = db.instagram_accounts.items[0]
    assert persisted['lastWebhookRepairResult'] == 'success'
    assert persisted['commentWebhookReady'] is True
    assert persisted['commentWebhookStatus'] == (
        'subscription_verified_scope_proof_inconclusive'
    )
    assert persisted['commentWebhookScopeWarning'] == (
        'scope_proof_inconclusive_but_graph_subscription_verified'
    )


def test_stale_cached_missing_scopes_cannot_override_fresh_readback(monkeypatch):
    account = _admin_account(
        grantedScopes=[
            'instagram_business_basic',
            'instagram_business_manage_messages',
        ],
        grantedScopesTokenPrefix=server._token_prefix('old-token'),
        grantedScopesDebugTokenWorks=True,
        grantedScopesMatchesIgAppId=True,
    )
    db = _DB(accounts=[account])
    server.db = db
    monkeypatch.setattr(server, '_require_admin_permission', _grant_admin)
    monkeypatch.setattr(server, '_refresh_account_granted_scopes_via_graph', _scope_refresh_inconclusive)
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(server.WEBHOOK_REQUIRED_FIELDS, status=200, verify_status=200),
    )

    result = _run(
        server.admin_instagram_repair_comment_webhooks('admin_acc', user_id='owner_ADMIN')
    )

    assert result['ok'] is True
    assert result['comment_webhook_blocker'] == 'active_token_scope_proof_inconclusive'
    assert result['comment_webhook_blocker'] != 'comment_permission_not_granted'


# ---------------------------------------------------------------------------
# 15. Meta-delivery-blocked verdict
# ---------------------------------------------------------------------------


def test_15_meta_delivery_blocked_when_only_non_comment_events_seen(monkeypatch):
    account = {
        'id': 'acct_mdb',
        'userId': 'owner_MDB',
        'username': 'mdb_acc',
        'instagramAccountId': '17841500000000222',
        'igUserId': '17841500000000222',
        'accessToken': 'fake-token',
        'connectionValid': True,
        'webhookEntryIdAliases': ['17841500000000222'],
    }
    # Non-comment events only.
    now = datetime.utcnow()
    events = [
        {
            'created_at': now - timedelta(minutes=5),
            'stage': 'webhook_received',
            'source': 'webhook',
            'username_key': 'mdb_acc',
            'extra': {},
        },
        {
            'created_at': now - timedelta(minutes=4),
            'stage': 'account_resolution_success',
            'source': 'webhook',
            'username_key': 'mdb_acc',
            'extra': {
                'via': 'instagram_accounts',
                'has_comments_field': False,
                'has_live_comments_field': False,
                'has_messages_field': True,
            },
        },
    ]
    db = _DB(accounts=[dict(account)], events=events)
    server.db = db
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(server.WEBHOOK_REQUIRED_FIELDS),
    )
    report = _run(
        server.certify_instagram_account_for_comment_webhooks(account, reason='sync')
    )
    assert report['comment_webhook_status'] == 'meta_delivery_blocked'
    assert report['comment_webhook_ready'] is False
    assert report['comment_webhook_meta_delivery_blocked'] is True


# ---------------------------------------------------------------------------
# 10-12. Activation gate
# ---------------------------------------------------------------------------


def test_10_activation_blocked_when_not_ready(monkeypatch):
    monkeypatch.setattr(server, '_IG_REQUIRE_COMMENT_WEBHOOK_CERT', True)
    account = {
        'id': 'acct_x', 'instagramAccountId': '17841500000000111',
        'igUserId': '17841500000000111', 'connectionValid': True,
        'commentWebhookReady': False,
        'commentWebhookStatus': 'repair_required',
    }
    automation = {
        'id': 'r1', 'trigger': 'comment', 'status': 'active',
        'instagramAccountId': '17841500000000111',
    }
    try:
        _run(server._validate_automation_integrity_for_account(
            'owner_X', account, automation, require_connected=True,
        ))
        assert False, 'expected HTTPException'
    except server.HTTPException as exc:
        assert exc.status_code == 400
        detail = exc.detail
        if isinstance(detail, dict):
            assert detail.get('code') == 'comment_webhook_not_ready'


def test_12_activation_allowed_when_ready(monkeypatch):
    monkeypatch.setattr(server, '_IG_REQUIRE_COMMENT_WEBHOOK_CERT', True)
    account = {
        'id': 'acct_y', 'instagramAccountId': '17841500000000111',
        'igUserId': '17841500000000111', 'connectionValid': True,
        'commentWebhookReady': True,
        'commentWebhookStatus': 'ready',
    }
    automation = {
        'id': 'r1', 'trigger': 'comment', 'status': 'active',
        'instagramAccountId': '17841500000000111',
    }
    # No exception → allowed.
    _run(server._validate_automation_integrity_for_account(
        'owner_Y', account, automation, require_connected=True,
    ))


def test_dm_rule_activation_not_blocked_by_comment_cert(monkeypatch):
    """Only comment rules are gated. DM rules continue to activate
    even when the comment cert is not ready."""
    monkeypatch.setattr(server, '_IG_REQUIRE_COMMENT_WEBHOOK_CERT', True)
    account = {
        'id': 'acct_z', 'instagramAccountId': '17841500000000111',
        'igUserId': '17841500000000111', 'connectionValid': True,
        'commentWebhookReady': False,
        'commentWebhookStatus': 'repair_required',
    }
    automation = {
        'id': 'r_dm', 'event_type': 'dm', 'status': 'active',
        'instagramAccountId': '17841500000000111',
    }
    _run(server._validate_automation_integrity_for_account(
        'owner_Z', account, automation, require_connected=True,
    ))


def test_11_existing_comment_rule_auto_paused_when_not_ready(monkeypatch):
    account = {
        'id': 'acct_p',
        'userId': 'owner_P',
        'instagramAccountId': '17841500000000333',
        'igUserId': '17841500000000333',
        'accessToken': 'fake-token',
        'connectionValid': True,
    }
    automations = [
        {'id': 'r_active', 'userId': 'owner_P', 'status': 'active',
         'event_type': 'comment',
         'instagramAccountId': '17841500000000333'},
        {'id': 'r_dm', 'userId': 'owner_P', 'status': 'active',
         'event_type': 'dm',
         'instagramAccountId': '17841500000000333'},
    ]
    db = _DB(accounts=[dict(account)], automations=automations)
    server.db = db
    # Force the subscribe stub to return MISSING fields so cert →
    # repair_required → auto-pause kicks in.
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(['messages']),  # comments + live_comments missing
    )
    report = _run(
        server.certify_instagram_account_for_comment_webhooks(account, reason='admin_repair')
    )
    assert report['comment_webhook_ready'] is False
    assert report['comment_webhook_status'] == 'repair_required'
    by_id = {r['id']: r for r in db.automations.items}
    assert by_id['r_active']['status'] == 'paused'
    assert by_id['r_active'].get('paused_reason') == 'comment_webhook_not_ready'
    # DM rule must NOT be touched.
    assert by_id['r_dm']['status'] == 'active'


# ---------------------------------------------------------------------------
# 1-3. Wire-in proof
# ---------------------------------------------------------------------------


def test_1_certify_called_from_sync_path():
    src = inspect.getsource(server._sync_user_instagram_account_doc)
    assert 'certify_instagram_account_for_comment_webhooks' in src


def test_2_certify_called_from_activate_endpoint():
    src = inspect.getsource(server.instagram_account_activate)
    assert 'certify_instagram_account_for_comment_webhooks' in src


def test_3_certify_called_from_admin_repair():
    src = inspect.getsource(server.admin_instagram_repair_comment_webhooks)
    assert 'certify_instagram_account_for_comment_webhooks' in src


# ---------------------------------------------------------------------------
# 16-23. Unchanged-contract guards
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


def test_no_cross_account_routing_changed():
    src = inspect.getsource(server)
    assert 'INSTAGRAM_SINGLE_TENANT_FALLBACK' in src
