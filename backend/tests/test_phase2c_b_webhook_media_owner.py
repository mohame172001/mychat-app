"""Phase 2C-B1 + B2 — webhook comment delivery hardening.

B1: when `_process_webhook` cannot resolve an `entry.id` to any
    connected `instagram_accounts` row, the `account_resolution_failed`
    flight event now carries a redacted identifier matrix so the
    operator can compare what Meta sent against what the rows hold.

B2: comment-field webhooks now consult a bounded media-owner probe
    fallback. For each connected account, the resolver issues
    `GET /{value.media.id}` with that account's token; the account
    whose token returns 200 is the owner. Fails closed on ambiguity.
    On success, the entry.id is persisted as a webhook alias on the
    resolved row so the NEXT webhook with the same entry.id hits the
    primary resolver and skips the probe.

These tests cover the 13 cases the spec requires + adjacency
properties (HMAC untouched, polling unchanged, dedupe unchanged, no
username branching, no token in logs).
"""
import asyncio
import os
import sys
from datetime import datetime
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
# Minimal in-memory fake collection — supports the few ops these tests need.
# ---------------------------------------------------------------------------


def _match(doc, query):
    for k, v in query.items():
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
        else:
            # $addToSet semantics: webhookEntryIdAliases is a list — match if
            # the value appears in it.
            if isinstance(actual, list):
                if v not in actual:
                    return False
            elif actual != v:
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
        rows = [d for d in self.docs if not query or _match(d, query)]
        return _Cursor(rows)

    async def find_one(self, query=None, projection=None):
        for d in self.docs:
            if not query or _match(d, query):
                return dict(d)
        return None

    async def update_one(self, query, update, upsert=False):
        for d in self.docs:
            if _match(d, query):
                if '$set' in update:
                    d.update(update['$set'])
                if '$addToSet' in update:
                    for key, value in update['$addToSet'].items():
                        existing = d.get(key)
                        if not isinstance(existing, list):
                            existing = []
                        if value not in existing:
                            existing.append(value)
                        d[key] = existing
                return SimpleNamespace(upserted_id=None, modified_count=1)
        return SimpleNamespace(upserted_id=None, modified_count=0)

    async def insert_one(self, doc):
        self.docs.append(dict(doc))
        return SimpleNamespace(inserted_id=doc.get('id'))


class _DB:
    def __init__(self):
        self.instagram_accounts = _Coll()
        self.users = _Coll()
        self.instagram_automation_events = _Coll()
        self.automations = _Coll()
        self.comments = _Coll()
        self.dm_logs = _Coll()
        self.comment_dm_sessions = _Coll()
        self.conversations = _Coll()


class _FakeResp:
    def __init__(self, status, json_body=None):
        self.status_code = status
        self._json = json_body or {}
        self.text = ''

    def json(self):
        return self._json


class _FakeHttpxClient:
    """Pluggable HTTP client. `responses` maps token → status code."""

    def __init__(self, responses=None, capture=None):
        self.responses = responses or {}
        self.capture = capture if capture is not None else []
        # httpx.AsyncClient accepts arbitrary kwargs; ignore them.

    def __call__(self, *a, **kw):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def get(self, url, params=None):
        token = (params or {}).get('access_token')
        self.capture.append({'url': url, 'token_partial': (token or '')[:6]})
        status = self.responses.get(token, 404)
        return _FakeResp(status, {'id': 'media-x'} if status == 200 else {})


def _install_db(monkeypatch, db):
    monkeypatch.setattr(server, 'db', db)


def _seed_account(db, *, account_id, ig_user_id, user_id, username, token,
                  aliases=None, extra=None):
    row = {
        'id': account_id,
        'instagramAccountId': ig_user_id,
        'igUserId': ig_user_id,
        'userId': user_id,
        'user_id': user_id,
        'username': username,
        'instagramHandle': username,
        'accessToken': token,
        'isActive': True,
        'connectionValid': True,
    }
    if aliases is not None:
        row['webhookEntryIdAliases'] = list(aliases)
    if extra:
        row.update(extra)
    db.instagram_accounts.docs.append(row)


def _seed_user(db, *, user_id, ig_user_id, username):
    db.users.docs.append({
        'id': user_id,
        'ig_user_id': ig_user_id,
        'instagramHandle': username,
        'meta_access_token': '',  # token comes from account row via _with_instagram_account_context
    })


def _clear_probe_cache():
    server._WEBHOOK_MEDIA_OWNER_PROBE_CACHE.clear()


def _comment_webhook_payload(entry_id, comment_id, media_id, commenter_id,
                             text='hi'):
    return {
        'object': 'instagram',
        'entry': [{
            'id': entry_id,
            'time': 1716742450,
            'changes': [{
                'field': 'comments',
                'value': {
                    'id': comment_id,
                    'comment_id': comment_id,
                    'media': {'id': media_id},
                    'from': {'id': commenter_id, 'username': 'fan'},
                    'text': text,
                },
            }],
        }],
    }


# ---------------------------------------------------------------------------
# 1. Primary resolver: entry.id matches an account identity field.
# ---------------------------------------------------------------------------


def test_comment_webhook_maps_normally_using_entry_id(monkeypatch):
    db = _DB()
    _seed_user(db, user_id='u1', ig_user_id='ig-A', username='accA')
    _seed_account(db, account_id='accA-db', ig_user_id='ig-A',
                  user_id='u1', username='accA', token='tok-A')
    _install_db(monkeypatch, db)
    _clear_probe_cache()

    # Block the heavy comment handler — we only verify mapping resolution.
    async def fake_handle(*_a, **_kw):
        return {'processed': True, 'matched': False}
    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)

    payload = _comment_webhook_payload(
        entry_id='ig-A', comment_id='comment-1',
        media_id='media-1', commenter_id='external-fan',
    )
    _run(server._process_webhook(payload))

    stages = [e.get('stage') for e in db.instagram_automation_events.docs]
    assert 'account_resolution_success' in stages
    assert 'webhook_comment_detected' in stages
    assert 'account_resolution_failed' not in stages


# ---------------------------------------------------------------------------
# 2. Media-owner fallback when entry.id is unmapped.
# ---------------------------------------------------------------------------


def test_comment_webhook_unmapped_entry_id_resolves_via_media_owner(monkeypatch):
    db = _DB()
    _seed_user(db, user_id='u1', ig_user_id='ig-A', username='accA')
    _seed_account(db, account_id='accA-db', ig_user_id='ig-A',
                  user_id='u1', username='accA', token='tok-A')
    _install_db(monkeypatch, db)
    _clear_probe_cache()

    # Only accA-db's token can read the media.
    capture = []
    client = _FakeHttpxClient(responses={'tok-A': 200}, capture=capture)
    monkeypatch.setattr(server.httpx, 'AsyncClient', client)

    handle_calls = []
    async def fake_handle(user_doc, comment, source=None):
        handle_calls.append({
            'user_id': user_doc.get('id'),
            'ig_user_id': user_doc.get('ig_user_id'),
            'source': source,
        })
        return {'processed': True, 'matched': False}
    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)

    # entry.id = something Meta sends that doesn't match any stored field.
    payload = _comment_webhook_payload(
        entry_id='unknown-meta-page-id', comment_id='comment-1',
        media_id='media-1', commenter_id='external-fan',
    )
    _run(server._process_webhook(payload))

    stages = [e.get('stage') for e in db.instagram_automation_events.docs]
    assert 'account_resolution_success' in stages
    assert 'account_resolution_failed' not in stages
    # The success event must record mapping_via=media_owner_probe.
    success = next(e for e in db.instagram_automation_events.docs
                   if e.get('stage') == 'account_resolution_success')
    assert success.get('extra', {}).get('via') == 'media_owner_probe'
    # Probe issued exactly one Graph call (only one account exists).
    assert len(capture) == 1


# ---------------------------------------------------------------------------
# 3. Resolved fallback calls _handle_new_comment with source='webhook'.
# ---------------------------------------------------------------------------


def test_resolved_fallback_calls_handle_new_comment_with_webhook_source(monkeypatch):
    db = _DB()
    _seed_user(db, user_id='u1', ig_user_id='ig-A', username='accA')
    _seed_account(db, account_id='accA-db', ig_user_id='ig-A',
                  user_id='u1', username='accA', token='tok-A')
    _install_db(monkeypatch, db)
    _clear_probe_cache()

    client = _FakeHttpxClient(responses={'tok-A': 200})
    monkeypatch.setattr(server.httpx, 'AsyncClient', client)

    handle_calls = []
    async def fake_handle(user_doc, comment, source=None):
        handle_calls.append({'source': source,
                             'comment_id': comment.get('ig_comment_id')})
        return {'processed': True}
    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)

    payload = _comment_webhook_payload(
        entry_id='unknown-id', comment_id='c-1',
        media_id='m-1', commenter_id='fan',
    )
    _run(server._process_webhook(payload))

    assert handle_calls, '_handle_new_comment was not called'
    assert handle_calls[0]['source'] == 'webhook'
    assert handle_calls[0]['comment_id'] == 'c-1'


# ---------------------------------------------------------------------------
# 4. Successful fallback writes webhook alias idempotently.
# ---------------------------------------------------------------------------


def test_successful_fallback_writes_alias_idempotently(monkeypatch):
    db = _DB()
    _seed_user(db, user_id='u1', ig_user_id='ig-A', username='accA')
    _seed_account(db, account_id='accA-db', ig_user_id='ig-A',
                  user_id='u1', username='accA', token='tok-A')
    _install_db(monkeypatch, db)
    _clear_probe_cache()
    client = _FakeHttpxClient(responses={'tok-A': 200})
    monkeypatch.setattr(server.httpx, 'AsyncClient', client)
    async def fake_handle(*_a, **_kw): return {'processed': True}
    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)

    payload1 = _comment_webhook_payload(
        entry_id='unknown-meta-id', comment_id='c-1',
        media_id='m-1', commenter_id='fan',
    )
    _run(server._process_webhook(payload1))
    aliases_after_first = db.instagram_accounts.docs[0].get(
        'webhookEntryIdAliases', []
    )
    assert aliases_after_first == ['unknown-meta-id']

    # Re-issue the same entry.id — alias must NOT be duplicated.
    payload2 = _comment_webhook_payload(
        entry_id='unknown-meta-id', comment_id='c-2',
        media_id='m-1', commenter_id='fan',
    )
    _run(server._process_webhook(payload2))
    aliases_after_second = db.instagram_accounts.docs[0].get(
        'webhookEntryIdAliases', []
    )
    assert aliases_after_second == ['unknown-meta-id'], (
        f'alias duplicated: {aliases_after_second}'
    )


# ---------------------------------------------------------------------------
# 5. Next webhook with same entry.id resolves via primary alias path.
# ---------------------------------------------------------------------------


def test_next_webhook_with_same_entry_id_uses_alias_not_probe(monkeypatch):
    db = _DB()
    _seed_user(db, user_id='u1', ig_user_id='ig-A', username='accA')
    _seed_account(db, account_id='accA-db', ig_user_id='ig-A',
                  user_id='u1', username='accA', token='tok-A',
                  aliases=['unknown-meta-id'])  # already self-healed
    _install_db(monkeypatch, db)
    _clear_probe_cache()
    # If the probe is hit, the test fails — capture would be non-empty.
    capture = []
    client = _FakeHttpxClient(responses={'tok-A': 200}, capture=capture)
    monkeypatch.setattr(server.httpx, 'AsyncClient', client)
    async def fake_handle(*_a, **_kw): return {'processed': True}
    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)

    payload = _comment_webhook_payload(
        entry_id='unknown-meta-id', comment_id='c-3',
        media_id='m-1', commenter_id='fan',
    )
    _run(server._process_webhook(payload))

    assert capture == [], (
        'Graph probe should NOT have fired on subsequent webhook — '
        'alias path was supposed to short-circuit it.'
    )
    success = next(e for e in db.instagram_automation_events.docs
                   if e.get('stage') == 'account_resolution_success')
    assert success.get('extra', {}).get('via') == 'webhook_entry_id_alias'


# ---------------------------------------------------------------------------
# 6. Unowned media_id fails cleanly with account_resolution_failed.
# ---------------------------------------------------------------------------


def test_unowned_media_fails_cleanly_with_identifier_matrix(monkeypatch):
    db = _DB()
    _seed_user(db, user_id='u1', ig_user_id='ig-A', username='accA')
    _seed_account(db, account_id='accA-db', ig_user_id='ig-A',
                  user_id='u1', username='accA', token='tok-A')
    _install_db(monkeypatch, db)
    _clear_probe_cache()

    # No token returns 200 — media is owned by no connected account.
    client = _FakeHttpxClient(responses={'tok-A': 403})
    monkeypatch.setattr(server.httpx, 'AsyncClient', client)
    async def fake_handle(*_a, **_kw): return {'processed': True}
    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)

    payload = _comment_webhook_payload(
        entry_id='unknown-id', comment_id='c-1',
        media_id='m-foreign', commenter_id='fan',
    )
    _run(server._process_webhook(payload))

    fail_events = [e for e in db.instagram_automation_events.docs
                   if e.get('stage') == 'account_resolution_failed']
    assert fail_events, 'expected account_resolution_failed event'
    extra = fail_events[0].get('extra') or {}
    # Phase 2C-B1 diagnostic matrix
    assert extra.get('probe_outcome') == 'none'
    assert extra.get('has_comment_field') is True
    assert extra.get('entry_id_partial') is not None
    assert extra.get('connected_account_samples') is not None
    # No webhook_comment_detected since resolution failed.
    stages = [e.get('stage') for e in db.instagram_automation_events.docs]
    assert 'webhook_comment_detected' not in stages


# ---------------------------------------------------------------------------
# 7. Ambiguous media-owner resolution fails closed.
# ---------------------------------------------------------------------------


def test_ambiguous_media_owner_resolution_fails_closed(monkeypatch):
    db = _DB()
    _seed_user(db, user_id='u1', ig_user_id='ig-A', username='accA')
    _seed_user(db, user_id='u2', ig_user_id='ig-B', username='accB')
    _seed_account(db, account_id='accA-db', ig_user_id='ig-A',
                  user_id='u1', username='accA', token='tok-A')
    _seed_account(db, account_id='accB-db', ig_user_id='ig-B',
                  user_id='u2', username='accB', token='tok-B')
    _install_db(monkeypatch, db)
    _clear_probe_cache()

    # BOTH tokens return 200 → ambiguous. Fail closed.
    client = _FakeHttpxClient(responses={'tok-A': 200, 'tok-B': 200})
    monkeypatch.setattr(server.httpx, 'AsyncClient', client)
    async def fake_handle(*_a, **_kw): return {'processed': True}
    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)

    payload = _comment_webhook_payload(
        entry_id='unknown-id', comment_id='c-1',
        media_id='m-ambig', commenter_id='fan',
    )
    _run(server._process_webhook(payload))

    fail_events = [e for e in db.instagram_automation_events.docs
                   if e.get('stage') == 'account_resolution_failed']
    assert fail_events
    assert fail_events[0].get('skip_reason') == 'ambiguous_media_owner_resolution'
    # No alias written to either account row.
    for row in db.instagram_accounts.docs:
        assert not row.get('webhookEntryIdAliases'), (
            'must not self-heal alias on ambiguous resolution'
        )


# ---------------------------------------------------------------------------
# 8. Expired/invalid token during probe does not crash.
# ---------------------------------------------------------------------------


def test_expired_token_during_probe_does_not_crash(monkeypatch):
    db = _DB()
    _seed_user(db, user_id='u1', ig_user_id='ig-A', username='accA')
    _seed_account(db, account_id='accA-db', ig_user_id='ig-A',
                  user_id='u1', username='accA', token='tok-A')
    _install_db(monkeypatch, db)
    _clear_probe_cache()

    class _ExplodingClient(_FakeHttpxClient):
        async def get(self, url, params=None):
            self.capture.append({'url': url})
            raise RuntimeError('simulated TLS / token expiry')
    monkeypatch.setattr(server.httpx, 'AsyncClient', _ExplodingClient())
    async def fake_handle(*_a, **_kw): return {'processed': True}
    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)

    payload = _comment_webhook_payload(
        entry_id='unknown-id', comment_id='c-1',
        media_id='m-x', commenter_id='fan',
    )
    # Must NOT raise.
    _run(server._process_webhook(payload))

    fail_events = [e for e in db.instagram_automation_events.docs
                   if e.get('stage') == 'account_resolution_failed']
    assert fail_events, 'expected graceful failure event after probe exception'


# ---------------------------------------------------------------------------
# 9. Multi-account owner scenario chooses correct account by media ownership.
# ---------------------------------------------------------------------------


def test_multi_account_owner_chooses_correct_account_by_media(monkeypatch):
    db = _DB()
    _seed_user(db, user_id='u1', ig_user_id='ig-A', username='accA')
    _seed_account(db, account_id='accA-db', ig_user_id='ig-A',
                  user_id='u1', username='accA', token='tok-A')
    _seed_account(db, account_id='accB-db', ig_user_id='ig-B',
                  user_id='u1', username='accB', token='tok-B')
    _install_db(monkeypatch, db)
    _clear_probe_cache()

    # Only accB's token can read this media — the comment belongs to
    # accB even though both accounts share the same owner.
    client = _FakeHttpxClient(responses={'tok-A': 400, 'tok-B': 200})
    monkeypatch.setattr(server.httpx, 'AsyncClient', client)
    handle_calls = []
    async def fake_handle(user_doc, comment, source=None):
        handle_calls.append({'ig_user_id': user_doc.get('ig_user_id'),
                             'source': source})
        return {'processed': True}
    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)

    payload = _comment_webhook_payload(
        entry_id='unknown-id', comment_id='c-1',
        media_id='m-B-only', commenter_id='fan',
    )
    _run(server._process_webhook(payload))

    assert handle_calls and handle_calls[0]['ig_user_id'] == 'ig-B'
    assert handle_calls[0]['source'] == 'webhook'
    # Self-heal alias only on accB.
    rows_by_id = {r['id']: r for r in db.instagram_accounts.docs}
    assert rows_by_id['accB-db'].get('webhookEntryIdAliases') == ['unknown-id']
    assert not rows_by_id['accA-db'].get('webhookEntryIdAliases')


# ---------------------------------------------------------------------------
# 10. Polling fallback remains unchanged — symbol smoke test.
# ---------------------------------------------------------------------------


def test_polling_helpers_unchanged_symbols_exist():
    """Static smoke: the polling-side symbols Phase 2/2B/2C-A built on
    are NOT modified or removed by Phase 2C-B."""
    assert callable(getattr(server, '_poll_user_comments', None))
    assert callable(getattr(server, '_collect_target_media_ids', None))
    assert callable(getattr(server, '_fetch_recent_media_ids', None))
    assert callable(getattr(server, '_polling_prefilter_skip_reason', None))


# ---------------------------------------------------------------------------
# 11. HMAC verification unchanged.
# ---------------------------------------------------------------------------


def test_hmac_enforce_constants_unchanged():
    """Static check: HMAC enforcement constants and helpers are intact.
    Phase 2C-B does not touch HMAC."""
    assert server.META_WEBHOOK_HMAC_ENFORCE in (True, False)
    src = Path(server.__file__).read_text(encoding='utf-8')
    # The HMAC enforcement gate must still be present.
    assert "META_WEBHOOK_HMAC_ENFORCE=0 is not permitted in production" in src


# ---------------------------------------------------------------------------
# 12. Dedupe unchanged — opening_dedupe_key composition test.
# ---------------------------------------------------------------------------


def test_dedupe_key_composition_unchanged():
    """`_comment_opening_dedupe_key` still requires all five fields and
    still SHA-256s them. Phase 2C-B does not change dedupe."""
    key = server._comment_opening_dedupe_key(
        user_id='u', instagram_account_id='ig', automation_id='r',
        media_id='m', commenter_id='c',
    )
    assert isinstance(key, str) and len(key) == 64
    # Missing any field → None
    assert server._comment_opening_dedupe_key('', 'ig', 'r', 'm', 'c') is None
    assert server._comment_opening_dedupe_key('u', '', 'r', 'm', 'c') is None
    assert server._comment_opening_dedupe_key('u', 'ig', '', 'm', 'c') is None
    assert server._comment_opening_dedupe_key('u', 'ig', 'r', '', 'c') is None
    assert server._comment_opening_dedupe_key('u', 'ig', 'r', 'm', '') is None


# ---------------------------------------------------------------------------
# 13. No username-specific logic in new code.
# ---------------------------------------------------------------------------


def test_no_username_specific_logic_in_phase2c_b():
    """Static-source check: the new helpers and the modified
    _process_webhook code must never reference a literal Instagram
    username. This is the load-bearing rule for SaaS multi-tenancy."""
    src = Path(server.__file__).read_text(encoding='utf-8')
    forbidden = (
        "muhammad_gehad",
        "mogehad17",
        # also catch any if username == form
        "if username == ",
    )
    # The username strings legitimately appear in module-level docstrings
    # and historical comments — scope the check to NEW code only.
    new_helpers = (
        '_resolve_comment_webhook_by_media_owner',
        '_record_webhook_alias_for_account',
        '_find_account_by_webhook_alias',
        '_build_webhook_failure_identifier_matrix',
        '_sample_connected_account_identity_matrix',
    )
    for helper in new_helpers:
        # Locate function definition
        anchor = f'async def {helper}('
        if anchor not in src:
            anchor = f'def {helper}('
        idx = src.find(anchor)
        assert idx >= 0, f'helper {helper} missing'
        # Extract the function body — up to the next top-level `def` or
        # end of file. ~3000 chars is more than enough for these helpers.
        window = src[idx:idx + 3000]
        for needle in forbidden:
            assert needle not in window, (
                f'forbidden username-specific token {needle!r} found in '
                f'helper {helper}'
            )


# ---------------------------------------------------------------------------
# Adjacency: no tokens leak into recorded events.
# ---------------------------------------------------------------------------


def test_identifier_matrix_redacts_all_ids_and_omits_tokens(monkeypatch):
    """B1 sanity: the diagnostic matrix carries first-3/last-3 partials
    of identity fields and never contains a raw access token, even when
    token fields are present on the account doc."""
    db = _DB()
    _seed_user(db, user_id='u1', ig_user_id='ig-A', username='accA')
    _seed_account(db, account_id='accA-db', ig_user_id='ig-A-very-long-secret',
                  user_id='u1', username='accA',
                  token='SHOULD_NOT_APPEAR_IN_OUTPUT')
    _install_db(monkeypatch, db)
    _clear_probe_cache()
    client = _FakeHttpxClient(responses={})  # nothing resolves
    monkeypatch.setattr(server.httpx, 'AsyncClient', client)
    async def fake_handle(*_a, **_kw): return {'processed': True}
    monkeypatch.setattr(server, '_handle_new_comment', fake_handle)

    payload = _comment_webhook_payload(
        entry_id='unknown-id', comment_id='c-1',
        media_id='m-1', commenter_id='fan',
    )
    _run(server._process_webhook(payload))

    fail = next(e for e in db.instagram_automation_events.docs
                if e.get('stage') == 'account_resolution_failed')
    blob = str(fail.get('extra'))
    assert 'SHOULD_NOT_APPEAR_IN_OUTPUT' not in blob
    # Full identity values must not appear either.
    assert 'ig-A-very-long-secret' not in blob
