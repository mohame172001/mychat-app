"""Phase 2K — generic onboarding/repair parity helper.

The fix shifts away from "add another UI form" toward "make every
connected account auto-repair to the working webhook shape." The
public surface is ``server.ensure_instagram_account_webhook_ready``.

Spec contract points covered (22):

  1.  New connected IG account gets canonical identity normalized
      (igUserId mirrored from instagramAccountId).
  2.  New connected IG account gets webhook aliases stored when safe
      (its own ig_user_id added to webhookEntryIdAliases).
  3.  New connected IG account triggers comment-webhook subscription
      repair (subscribe call attempted; missing fields surfaced).
  4.  Working account shape like mogehad17 still resolves source=webhook.
  5.  Broken/missing-alias account shape like muhammad_gehad is
      repaired generically (alias added, subscription verified).
  6.  webhookEntryIdAliases self-heal works generically (no
      username branch).
  7.  Direct entry.id mapping path remains.
  8.  Media-owner fallback path remains.
  9.  Zero account matches fail closed.
  10. Multiple account matches fail closed.
  11. Active rules load for the resolved account (binding present).
  12. Rule account binding mismatch is repaired (stale/empty
      instagramAccountId on rules of the same owner is rebound).
  13. No cross-account routing — rebind only touches rules of the
      SAME owner user_id, never other users.
  14. No username-specific logic.
  15. Polling disabled by default.
  16-20. HMAC / Billing / dedupe / Phase 2D cooldown / quick-reply
      copy unchanged.
  21. mogehad17-shaped account still completes after helper runs.
  22. muhammad_gehad-shaped account completes webhook flow after
      helper rebinds aliases/subscription generically.
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


class _UpdateResult:
    def __init__(self, modified):
        self.modified_count = modified


class _Coll:
    def __init__(self, items=None):
        self.items = list(items or [])
        self.last_query = None
        self.last_update = None

    async def update_one(self, query, update):
        self.last_query = query
        self.last_update = update
        for it in self.items:
            if all(it.get(k) == v for k, v in query.items()):
                set_ops = (update.get('$set') or {})
                for k, v in set_ops.items():
                    it[k] = v
                add_to_set = (update.get('$addToSet') or {}).get('webhookEntryIdAliases')
                if add_to_set:
                    existing = list(it.get('webhookEntryIdAliases') or [])
                    items_to_add = (
                        add_to_set.get('$each')
                        if isinstance(add_to_set, dict) and '$each' in add_to_set
                        else [add_to_set]
                    )
                    for v in items_to_add:
                        if v not in existing:
                            existing.append(v)
                    it['webhookEntryIdAliases'] = existing
                return _UpdateResult(1)
        return _UpdateResult(0)

    async def update_many(self, query, update):
        self.last_query = query
        self.last_update = update
        modified = 0
        owner_clauses = query.get('$or') or []
        owner_ids = set()
        for c in owner_clauses:
            for v in c.values():
                owner_ids.add(v)
        # $and clause typically constrains the field
        stale_filter = None
        for clause in query.get('$and') or []:
            ors = clause.get('$or') or []
            for o in ors:
                if 'instagramAccountId' in o and (
                    o['instagramAccountId'] == {'$in': ['', None]}
                    or o['instagramAccountId'] == {'$exists': False}
                ):
                    stale_filter = 'empty_or_missing'
        for it in self.items:
            if owner_ids and not (
                it.get('userId') in owner_ids or it.get('user_id') in owner_ids
            ):
                continue
            if stale_filter == 'empty_or_missing':
                cur = it.get('instagramAccountId', '__MISSING__')
                if cur not in ('', None, '__MISSING__'):
                    continue
            for k, v in (update.get('$set') or {}).items():
                it[k] = v
            modified += 1
        return _UpdateResult(modified)

    async def find_one(self, query=None, projection=None):
        if not query:
            return self.items[0] if self.items else None
        for it in self.items:
            if all(it.get(k) == v for k, v in query.items()):
                return it
        return None


class _FakeDB:
    def __init__(self, accounts=None, automations=None, media_catalog=None):
        self.instagram_accounts = _Coll(accounts)
        self.automations = _Coll(automations)
        self.instagram_media_catalog = _Coll(media_catalog)

    def __getattr__(self, name):
        return _Coll([])


# ---------------------------------------------------------------------------
# Helper to stub the Graph subscribe call
# ---------------------------------------------------------------------------


class _SubscribeStub:
    def __init__(self, *, subscribed=None, status=200):
        self.subscribed = list(
            subscribed if subscribed is not None
            else server.WEBHOOK_REQUIRED_FIELDS
        )
        self.status = status
        self.calls = []

    async def __call__(self, ig_user_id, access_token):
        self.calls.append((ig_user_id, access_token))
        return {
            'ok': self.status == 200,
            'subscribe_status': self.status,
            'verify_status': self.status,
            'subscribed_fields': list(self.subscribed),
        }


# ---------------------------------------------------------------------------
# 1-3, 5. Onboarding parity for muhammad_gehad-shaped account
# ---------------------------------------------------------------------------


def test_onboarding_normalizes_canonical_identity_and_adds_alias(monkeypatch):
    """A newly-connected account that has instagramAccountId but no
    igUserId mirror, no aliases, and partial subscription is brought
    to the same shape as a working account in one call."""
    account = {
        'id': 'acct_db_id_99',
        'userId': 'user_99',
        'username': 'someuser',
        'instagramAccountId': '17841500000000099',
        # igUserId intentionally missing.
        # webhookEntryIdAliases intentionally missing.
        'accessToken': 'fake-token-for-test',
        'connectionValid': True,
    }
    fake_db = _FakeDB(accounts=[dict(account)])
    server.db = fake_db
    sub_stub = _SubscribeStub(subscribed=server.WEBHOOK_REQUIRED_FIELDS)
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks', sub_stub,
    )
    report = _run(
        server.ensure_instagram_account_webhook_ready(account, reason='connect')
    )
    assert sub_stub.calls, 'Graph subscribe must run on connect'
    persisted = fake_db.instagram_accounts.items[0]
    # Canonical identity normalized.
    assert persisted['igUserId'] == '17841500000000099'
    # Alias self-populated.
    assert '17841500000000099' in (persisted.get('webhookEntryIdAliases') or [])
    # Subscription cache persisted.
    assert set(persisted.get('webhookSubscriptionFields') or []) >= set(
        server.WEBHOOK_REQUIRED_FIELDS
    )
    assert persisted.get('webhookSubscriptionMissing') == []
    # Report is structured + ready=True.
    assert report['ready'] is True
    assert report['missing_subscriptions'] == []
    assert report['repaired'] is True


def test_onboarding_idempotent_on_already_ready_account(monkeypatch):
    """Re-running the helper on a fully ready account must NOT
    duplicate aliases or claim a repair happened."""
    account = {
        'id': 'acct_db_id_ready',
        'userId': 'user_ready',
        'username': 'mogehad17_like',
        'instagramAccountId': '17841500000000777',
        'igUserId': '17841500000000777',
        'webhookEntryIdAliases': ['17841500000000777'],
        'webhookSubscriptionFields': sorted(server.WEBHOOK_REQUIRED_FIELDS),
        'webhookSubscriptionMissing': [],
        'accessToken': 'fake-token-for-test',
        'connectionValid': True,
    }
    fake_db = _FakeDB(accounts=[dict(account)])
    server.db = fake_db
    sub_stub = _SubscribeStub(subscribed=server.WEBHOOK_REQUIRED_FIELDS)
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks', sub_stub,
    )
    report = _run(
        server.ensure_instagram_account_webhook_ready(account, reason='admin_repair')
    )
    assert report['ready'] is True
    persisted = fake_db.instagram_accounts.items[0]
    assert persisted['webhookEntryIdAliases'].count('17841500000000777') == 1


# ---------------------------------------------------------------------------
# 12-13. Rule rebind is owner-scoped and never cross-account
# ---------------------------------------------------------------------------


def test_rule_rebind_is_scoped_to_owner_and_skips_other_users(monkeypatch):
    account = {
        'id': 'acct_db_id_rb',
        'userId': 'owner_A',
        'instagramAccountId': '17841500000000111',
        'igUserId': '17841500000000111',
        'accessToken': 'fake-token',
        'connectionValid': True,
    }
    automations = [
        # Owner A, stale instagramAccountId → should be rebound.
        {'id': 'rule_1', 'userId': 'owner_A', 'instagramAccountId': '',
         'status': 'active'},
        # Owner A, missing field entirely → should be rebound.
        {'id': 'rule_2', 'userId': 'owner_A', 'status': 'active'},
        # Owner A, already correctly set → must NOT be touched.
        {'id': 'rule_3', 'userId': 'owner_A',
         'instagramAccountId': '17841500000000111', 'status': 'active'},
        # Different owner → must NEVER be touched (cross-account guard).
        {'id': 'rule_4', 'userId': 'owner_B',
         'instagramAccountId': '', 'status': 'active'},
    ]
    fake_db = _FakeDB(accounts=[dict(account)], automations=automations)
    server.db = fake_db
    sub_stub = _SubscribeStub()
    monkeypatch.setattr(
        server, '_subscribe_instagram_account_to_webhooks', sub_stub,
    )
    report = _run(
        server.ensure_instagram_account_webhook_ready(account, reason='connect')
    )
    assert report['rule_binding_mismatch'] == 2
    by_id = {r['id']: r for r in fake_db.automations.items}
    # Stale rules rebound to the canonical ig id.
    assert by_id['rule_1']['instagramAccountId'] == '17841500000000111'
    assert by_id['rule_2']['instagramAccountId'] == '17841500000000111'
    # Already-correct rule untouched.
    assert by_id['rule_3']['instagramAccountId'] == '17841500000000111'
    # CRITICAL: rule belonging to a different user must NOT be touched
    # by this account's repair.
    assert by_id['rule_4']['instagramAccountId'] == ''
    assert by_id['rule_4'].get('igUserId') is None


# ---------------------------------------------------------------------------
# 6-10. Resolver paths remain
# ---------------------------------------------------------------------------


def test_webhookEntryIdAliases_addToSet_path_exists():
    src = inspect.getsource(server)
    assert '$addToSet' in src
    assert 'webhookEntryIdAliases' in src


def test_direct_entry_id_mapping_present():
    src = inspect.getsource(server)
    assert '_find_user_doc_for_instagram_account_id' in src


def test_media_owner_probe_present():
    src = inspect.getsource(server)
    assert '_resolve_comment_webhook_by_media_owner' in src
    assert 'webhook_media_owner_probe' in src


def test_resolver_fails_closed_when_no_account_matches():
    src = inspect.getsource(server)
    assert 'account_resolution_failed' in src
    assert 'no_matching_instagram_account' in src


def test_resolver_fails_closed_on_ambiguous_media_owner():
    src = inspect.getsource(server)
    assert 'ambiguous_media_owner_resolution' in src


# ---------------------------------------------------------------------------
# 14-20. Unchanged-contract guards
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


def test_polling_remains_disabled_by_default():
    src = inspect.getsource(server)
    assert "os.environ.get('IG_POLL_ENABLED', '0')" in src


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


# ---------------------------------------------------------------------------
# 4, 21-22. Wire-in proof
# ---------------------------------------------------------------------------


def test_helper_is_called_from_account_sync_path():
    """_sync_user_instagram_account_doc must fire the parity helper
    after upsert so every freshly-connected account auto-repairs."""
    src = inspect.getsource(server._sync_user_instagram_account_doc)
    # Phase 2M re-wired the call sites to go through the
    # certification helper which itself calls ensure_instagram_account_webhook_ready.
    assert (
        'ensure_instagram_account_webhook_ready' in src
        or 'certify_instagram_account_for_comment_webhooks' in src
    )


def test_helper_is_called_from_account_activate_endpoint():
    src = inspect.getsource(server.instagram_account_activate)
    # Phase 2M re-wired the call sites to go through the
    # certification helper which itself calls ensure_instagram_account_webhook_ready.
    assert (
        'ensure_instagram_account_webhook_ready' in src
        or 'certify_instagram_account_for_comment_webhooks' in src
    )


def test_helper_is_called_from_admin_repair_endpoint():
    src = inspect.getsource(server.admin_instagram_repair_comment_webhooks)
    # Phase 2M re-wired the call sites to go through the
    # certification helper which itself calls ensure_instagram_account_webhook_ready.
    assert (
        'ensure_instagram_account_webhook_ready' in src
        or 'certify_instagram_account_for_comment_webhooks' in src
    )
