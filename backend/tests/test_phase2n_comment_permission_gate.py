"""Phase 2N — comment-permission gate.

Root cause of the long-standing "subscribe says ok but Meta never
delivers comment webhooks" symptom: the account's access token was
granted WITHOUT the comment-management permission
(``instagram_business_manage_comments`` for Instagram Business Login,
or the legacy ``instagram_manage_comments``). Meta still ACCEPTS the
``/subscribed_apps`` POST for the ``comments`` field, so the
subscription looks healthy, but it never delivers comment webhooks
while messaging keeps working.

Certification now detects the missing scope and returns an actionable
``reconnect_required`` verdict with blocker ``comment_permission_not_
granted`` — account-agnostic, no per-username branches.

Contract covered here:
  1.  Known-missing comment scope → reconnect_required + blocker.
  2.  Granted comment scope → permission does not block (→ ready).
  3.  Unknown scopes (None) → permission does NOT block (back-compat).
  4.  Scopes read from the user-doc OAuth audit fallback.
  5.  granular_scopes dict-list is normalized correctly.
  6.  _scopes_include_comment_permission tri-state semantics.
  7.  grantedScopes persisted onto the account row by certify.
  8.  Legacy (Graph) comment scope name is also recognized.
  9.  Unchanged-contract guards (no username logic, no live call in
      the certify hot path).
"""
import asyncio
import inspect
import os
import sys
from datetime import datetime
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


class _UR:
    def __init__(self, n):
        self.modified_count = n


class _Coll:
    def __init__(self, items=None):
        self.items = list(items or [])

    def find(self, query=None, projection=None):
        out = list(self.items)

        class _C:
            def __init__(self, rows):
                self._r = rows

            def sort(self, *a, **kw):
                return self

            def limit(self, n):
                self._r = self._r[:n]
                return self

            async def to_list(self, n):
                return list(self._r[:n])
        return _C(out)

    async def find_one(self, query=None, projection=None):
        if not query:
            return self.items[0] if self.items else None
        for it in self.items:
            ok = True
            for k, v in query.items():
                if k.startswith('$'):
                    continue
                if it.get(k) != v:
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
        return _UR(0)


class _DB:
    def __init__(self, accounts=None, users=None):
        self.instagram_accounts = _Coll(accounts)
        self.users = _Coll(users)

    def __getattr__(self, name):
        return _Coll([])


class _SubStub:
    def __init__(self, subscribed=None, status=200):
        self.subscribed = list(
            subscribed if subscribed is not None
            else server.WEBHOOK_REQUIRED_FIELDS
        )
        self.status = status

    async def __call__(self, ig_user_id, access_token):
        return {
            'ok': self.status == 200,
            'subscribe_status': self.status,
            'verify_status': self.status,
            'subscribed_fields': list(self.subscribed),
        }


_FULL_SCOPES = [
    'instagram_business_basic',
    'instagram_business_manage_messages',
    'instagram_business_manage_comments',
]
_NO_COMMENT_SCOPES = [
    'instagram_business_basic',
    'instagram_business_manage_messages',
]


# ---------------------------------------------------------------------------
# 6. tri-state helper semantics
# ---------------------------------------------------------------------------


def test_scope_helper_tristate():
    assert server._scopes_include_comment_permission(_FULL_SCOPES) is True
    assert server._scopes_include_comment_permission(_NO_COMMENT_SCOPES) is False
    # Unknown → None
    assert server._scopes_include_comment_permission(None) is None
    assert server._scopes_include_comment_permission('not-a-list') is None
    # Known-empty list → False (we asked and got nothing)
    assert server._scopes_include_comment_permission([]) is False


def test_legacy_graph_comment_scope_recognized():
    assert server._scopes_include_comment_permission(
        ['instagram_basic', 'instagram_manage_comments']
    ) is True


# ---------------------------------------------------------------------------
# 5. granular_scopes dict-list normalization
# ---------------------------------------------------------------------------


def test_normalize_granular_scopes_dictlist():
    granular = [
        {'scope': 'instagram_business_basic', 'target_ids': ['x']},
        {'scope': 'instagram_business_manage_comments', 'target_ids': ['y']},
    ]
    norm = server._normalize_granted_scopes(granular)
    assert 'instagram_business_manage_comments' in norm
    assert server._scopes_include_comment_permission(granular) is True


def test_debug_token_parser_combines_scopes_and_granular_scopes():
    debug = {
        'data': {
            'scopes': ['instagram_business_basic'],
            'granular_scopes': [
                {
                    'scope': 'instagram_business_manage_comments',
                    'target_ids': ['1784'],
                },
            ],
        },
    }
    scopes = server._debug_token_scopes(debug)
    assert 'instagram_business_basic' in scopes
    assert 'instagram_business_manage_comments' in scopes
    assert server._scopes_include_comment_permission(scopes) is True


def test_debug_token_parser_reads_camel_case_granular_scopes():
    debug = {
        'data': {
            'scopes': ['instagram_business_basic'],
            'granularScopes': [
                {
                    'scope': 'instagram_business_manage_comments',
                    'target_ids': ['1784'],
                },
            ],
        },
    }
    scopes = server._debug_token_scopes(debug)
    assert 'instagram_business_manage_comments' in scopes
    assert server._scopes_include_comment_permission(scopes) is True


# ---------------------------------------------------------------------------
# 1. known-missing comment scope → reconnect_required
# ---------------------------------------------------------------------------


def test_missing_comment_scope_forces_reconnect(monkeypatch):
    account = {
        'id': 'acct_noperm',
        'userId': 'owner_NP',
        'username': 'noperm_acc',
        'instagramAccountId': '17841500000000444',
        'igUserId': '17841500000000444',
        'webhookEntryIdAliases': ['17841500000000444'],
        'accessToken': 'fake-token',
        'connectionValid': True,
        'grantedScopes': list(_NO_COMMENT_SCOPES),
        'grantedScopesTokenPrefix': server._token_prefix('fake-token'),
        'grantedScopesDebugTokenWorks': True,
        'grantedScopesMatchesIgAppId': True,
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
    assert report['comment_webhook_ready'] is False
    assert report['comment_webhook_status'] == 'reconnect_required'
    assert report['comment_webhook_blocker'] == 'comment_permission_not_granted'
    assert report['comment_webhook_reconnect_required'] is True
    assert report['comment_permission_granted'] is False
    persisted = db.instagram_accounts.items[0]
    assert persisted['commentWebhookStatus'] == 'reconnect_required'
    assert persisted['commentWebhookBlocker'] == 'comment_permission_not_granted'
    assert persisted['commentPermissionGranted'] is False
    assert persisted['commentPermissionScopeCheckProven'] is True


def test_unproven_missing_comment_scope_is_inconclusive(monkeypatch):
    account = {
        'id': 'acct_unproven',
        'userId': 'owner_UP',
        'username': 'unproven_acc',
        'instagramAccountId': '17841500000000445',
        'igUserId': '17841500000000445',
        'webhookEntryIdAliases': ['17841500000000445'],
        'accessToken': 'fake-token',
        'connectionValid': True,
        'grantedScopes': list(_NO_COMMENT_SCOPES),
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
    assert report['comment_webhook_ready'] is False
    assert report['comment_webhook_status'] == 'certification_scope_check_inconclusive'
    assert report['comment_webhook_blocker'] == 'active_token_scope_proof_inconclusive'
    assert report['comment_webhook_reconnect_required'] is False
    assert report['comment_permission_granted'] is False
    assert report['comment_permission_scope_check_proven'] is False


# ---------------------------------------------------------------------------
# 2. granted comment scope → permission does not block
# ---------------------------------------------------------------------------


def test_granted_comment_scope_allows_ready(monkeypatch):
    account = {
        'id': 'acct_perm',
        'userId': 'owner_P',
        'username': 'perm_acc',
        'instagramAccountId': '17841500000000555',
        'igUserId': '17841500000000555',
        'webhookEntryIdAliases': ['17841500000000555'],
        'accessToken': 'fake-token',
        'connectionValid': True,
        'grantedScopes': list(_FULL_SCOPES),
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
    assert report['comment_webhook_status'] == 'ready'
    assert report['comment_webhook_ready'] is True
    assert report['comment_permission_granted'] is True
    assert 'instagram_business_manage_comments' in (
        db.instagram_accounts.items[0].get('grantedScopes') or []
    )


# ---------------------------------------------------------------------------
# 3. unknown scopes do not block (backward compatibility)
# ---------------------------------------------------------------------------


def test_unknown_scopes_do_not_block(monkeypatch):
    account = {
        'id': 'acct_unknown',
        'userId': 'owner_U',
        'username': 'unknown_acc',
        'instagramAccountId': '17841500000000666',
        'igUserId': '17841500000000666',
        'webhookEntryIdAliases': ['17841500000000666'],
        'accessToken': 'fake-token',
        'connectionValid': True,
        # No grantedScopes, and no user doc audit → unknown.
    }
    db = _DB(accounts=[dict(account)], users=[])
    server.db = db
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(server.WEBHOOK_REQUIRED_FIELDS),
    )
    report = _run(
        server.certify_instagram_account_for_comment_webhooks(account, reason='sync')
    )
    # Unknown permission must NOT force reconnect; falls through to the
    # delivery logic (no events here → ready, since subscribe is ok and
    # there is no contrary delivery signal).
    assert report['comment_webhook_status'] != 'reconnect_required'
    assert report['comment_permission_granted'] is None


# ---------------------------------------------------------------------------
# 4. scopes read from user-doc OAuth audit fallback
# ---------------------------------------------------------------------------


def test_scopes_resolved_from_user_audit_fallback(monkeypatch):
    account = {
        'id': 'acct_fallback',
        'userId': 'owner_FB',
        'username': 'fallback_acc',
        'instagramAccountId': '17841500000000888',
        'igUserId': '17841500000000888',
        'webhookEntryIdAliases': ['17841500000000888'],
        'accessToken': 'fake-token',
        'connectionValid': True,
        # No grantedScopes on the row — resolver must fall back to the
        # owning user's OAuth audit.
    }
    user = {
        'id': 'owner_FB',
        'ig_oauth_last_audit': {
            'finalTokenPrefix': server._token_prefix('fake-token'),
            'debugToken': {
                'scopes': list(_NO_COMMENT_SCOPES),
                'debugTokenWorks': True,
                'matchesIgAppId': True,
            },
        },
    }
    db = _DB(accounts=[dict(account)], users=[user])
    server.db = db
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(server.WEBHOOK_REQUIRED_FIELDS),
    )
    report = _run(
        server.certify_instagram_account_for_comment_webhooks(account, reason='sync')
    )
    assert report['comment_webhook_status'] == 'reconnect_required'
    assert report['comment_webhook_blocker'] == 'comment_permission_not_granted'
    assert report['comment_permission_scope_check_proven'] is True


def test_stale_user_audit_scope_mismatch_is_inconclusive(monkeypatch):
    account = {
        'id': 'acct_stale_audit',
        'userId': 'owner_SA',
        'username': 'stale_audit_acc',
        'instagramAccountId': '17841500000000889',
        'igUserId': '17841500000000889',
        'webhookEntryIdAliases': ['17841500000000889'],
        'accessToken': 'active-token',
        'connectionValid': True,
    }
    user = {
        'id': 'owner_SA',
        'ig_oauth_last_audit': {
            'finalTokenPrefix': server._token_prefix('old-token'),
            'debugToken': {
                'scopes': list(_NO_COMMENT_SCOPES),
                'debugTokenWorks': True,
                'matchesIgAppId': True,
            },
        },
    }
    db = _DB(accounts=[dict(account)], users=[user])
    server.db = db
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks',
        _SubStub(server.WEBHOOK_REQUIRED_FIELDS),
    )
    report = _run(
        server.certify_instagram_account_for_comment_webhooks(account, reason='sync')
    )
    assert report['comment_webhook_status'] == 'certification_scope_check_inconclusive'
    assert report['comment_webhook_blocker'] == 'active_token_scope_proof_inconclusive'
    assert report['comment_permission_token_prefix_matches'] is False


# ---------------------------------------------------------------------------
# 9. unchanged-contract guards
# ---------------------------------------------------------------------------


def test_certify_hot_path_makes_no_live_scope_call():
    """The generic certify path must resolve scopes WITHOUT a live
    Graph call (so the mocked-httpx legacy suite is undisturbed). The
    only live refresh lives in the admin-repair endpoint."""
    src = inspect.getsource(server.certify_instagram_account_for_comment_webhooks)
    assert '_resolve_account_granted_scope_context' in src
    assert '_refresh_account_granted_scopes_via_graph' not in src


def test_admin_repair_refreshes_scopes_live():
    src = inspect.getsource(server.admin_instagram_repair_comment_webhooks)
    assert '_refresh_account_granted_scopes_via_graph' in src


def test_connect_persists_granted_scopes():
    src = inspect.getsource(server)
    assert "'grantedScopes': _connect_scopes" in src


def test_no_username_specific_logic():
    src = inspect.getsource(server)
    for needle in (
        "username == 'muhammad_gehad'",
        "username == 'mogehad17'",
        "username_key == 'muhammad_gehad'",
        "username_key == 'mogehad17'",
    ):
        assert needle not in src
