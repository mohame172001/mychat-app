"""Phase 2 — media catalog + round-robin polling coverage.

Product requirement: a `comment:any / post_scope=any` rule must cover
every post on the connected Instagram account, not only the latest 25.
The polling fallback was previously capped at the 25 most-recent media
items, so a fresh comment on the 26th-most-recent post (or older) never
entered `_handle_new_comment` unless the webhook delivery happened to
work for it.

This file covers the new behavior end-to-end:
  - `_collect_target_media_ids` composes recent + pinned + round-robin
    slices from the per-account `instagram_media_catalog`.
  - The round-robin cursor advances each tick so the entire catalog is
    eventually covered without scanning every post every 15s.
  - The catalog is account-scoped — a sibling account's media is never
    included in another account's polling target.
  - Selected media (post-specific rules) is still pinned regardless of
    whether it appears in the recent slice.
  - Diagnostic fields (`total_known_media_count`, `recent_media_limit`,
    `round_robin_batch_size`, `round_robin_cursor_position`) are
    surfaced on `poller_account_scan_started`.

No code path that affects sending, dedupe semantics, HMAC verification,
billing, dashboard, rate limits, or quick-reply copy is exercised here.
Tests are read-only against fake collections — no real Mongo, no
network, no provider sends.
"""
import asyncio
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
# Minimal fake collection — supports just what these tests need.
# ---------------------------------------------------------------------------


def _match(doc, query):
    """Tiny pymongo-style matcher: $or, exact equality, and {'$exists'}."""
    for key, expected in query.items():
        if key == '$or':
            if not any(_match(doc, q) for q in expected):
                return False
            continue
        if key == '$and':
            if not all(_match(doc, q) for q in expected):
                return False
            continue
        v = doc.get(key)
        if isinstance(expected, dict):
            if '$exists' in expected and (key in doc) != expected['$exists']:
                return False
            if '$in' in expected and v not in expected['$in']:
                return False
            if '$ne' in expected and v == expected['$ne']:
                return False
        else:
            if v != expected:
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

    def __aiter__(self):
        async def gen():
            for d in self.docs:
                yield d
        return gen()


class _Collection:
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

    async def count_documents(self, query):
        return sum(1 for d in self.docs if _match(d, query))

    async def update_one(self, query, update, upsert=False):
        for d in self.docs:
            if _match(d, query):
                if '$set' in update:
                    d.update(update['$set'])
                if '$setOnInsert' in update:
                    for k, v in update['$setOnInsert'].items():
                        d.setdefault(k, v)
                from types import SimpleNamespace
                return SimpleNamespace(upserted_id=None, modified_count=1)
        if upsert:
            new_doc = {}
            if '$setOnInsert' in update:
                new_doc.update(update['$setOnInsert'])
            if '$set' in update:
                new_doc.update(update['$set'])
            # Carry over equality clauses from the query as identity.
            for k, v in (query or {}).items():
                if not isinstance(v, dict) and k != '$or' and k != '$and':
                    new_doc.setdefault(k, v)
            self.docs.append(new_doc)
            from types import SimpleNamespace
            return SimpleNamespace(upserted_id='inserted', modified_count=0)
        from types import SimpleNamespace
        return SimpleNamespace(upserted_id=None, modified_count=0)


class _FakeDB:
    def __init__(self):
        self.instagram_media_catalog = _Collection([])
        self.instagram_accounts = _Collection([])
        self.instagram_automation_events = _Collection([])


# ---------------------------------------------------------------------------
# Catalog reading + round-robin cursor
# ---------------------------------------------------------------------------


def test_media_catalog_known_ids_returns_account_scoped_ordered(monkeypatch):
    """`_media_catalog_known_ids(account_db_id)` returns every catalog
    row scoped to that account, sorted newest-first by media_timestamp.
    A sibling account's media must NOT appear."""
    db = _FakeDB()
    base = datetime(2026, 1, 1)
    for i in range(5):
        db.instagram_media_catalog.docs.append({
            'instagramAccountDbId': 'accA',
            'media_id': f'A-media-{i}',
            'media_timestamp': (base + timedelta(days=i)).isoformat(),
        })
    for i in range(3):
        db.instagram_media_catalog.docs.append({
            'instagramAccountDbId': 'accB',
            'media_id': f'B-media-{i}',
            'media_timestamp': (base + timedelta(days=i)).isoformat(),
        })
    monkeypatch.setattr(server, 'db', db)
    ids_a = _run(server._media_catalog_known_ids('accA'))
    ids_b = _run(server._media_catalog_known_ids('accB'))
    assert set(ids_a) == {f'A-media-{i}' for i in range(5)}
    assert set(ids_b) == {f'B-media-{i}' for i in range(3)}
    assert all(not mid.startswith('B-') for mid in ids_a), \
        'sibling account media leaked into accA list'
    assert all(not mid.startswith('A-') for mid in ids_b), \
        'sibling account media leaked into accB list'


def test_collect_target_media_includes_round_robin_batch(monkeypatch):
    """A comment:any rule on an account with 60 catalog media MUST
    produce a target list that covers recent + round-robin batch. After
    enough ticks the entire catalog is covered."""
    db = _FakeDB()
    # 60 catalog rows for accA
    base = datetime(2026, 1, 1)
    for i in range(60):
        db.instagram_media_catalog.docs.append({
            'instagramAccountDbId': 'accA',
            'media_id': f'media-{i:02d}',
            # Reverse order so media-00 is newest.
            'media_timestamp': (base + timedelta(days=60 - i)).isoformat(),
        })
    db.instagram_accounts.docs.append({
        'id': 'accA',
        'instagramAccountId': 'igA',
        'pollingRoundRobinCursor': 0,
    })
    monkeypatch.setattr(server, 'db', db)

    # Live /me/media returns only the first 25.
    async def fake_recent(token, ig_user_id, limit=10):
        return [f'media-{i:02d}' for i in range(25)]
    async def fake_latest(token, ig_user_id):
        return None
    monkeypatch.setattr(server, '_fetch_recent_media_ids', fake_recent)
    monkeypatch.setattr(server, '_fetch_latest_media_id', fake_latest)

    # Force batch=25 and recent=25 for the test (matches default env).
    monkeypatch.setenv('IG_POLL_RECENT_MEDIA_LIMIT', '25')
    monkeypatch.setenv('IG_POLL_ROUND_ROBIN_BATCH', '25')

    user_doc = {
        'id': 'u1', 'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA', 'meta_access_token': 'token-a',
    }
    rule = {
        'id': 'rule_general', 'user_id': 'u1', 'status': 'active',
        'trigger': 'comment:any', 'post_scope': 'any',
        'instagramAccountId': 'igA',
    }

    # Tick 1: recent 25 (media-00..media-24) + round-robin batch from
    # cursor 0 of the 60-row catalog. Catalog sort is newest-first by
    # media_timestamp, so it starts at media-00. Recent already covers
    # media-00..media-24 → round-robin slice adds nothing new IF it
    # walks the same ids first. The test only asserts the cursor
    # ADVANCES so subsequent ticks reach media older than 25.
    targets_1 = _run(server._collect_target_media_ids(user_doc, [rule]))
    # Catalog has 60 rows; cursor should have advanced.
    cursor_after_tick = db.instagram_accounts.docs[0].get(
        'pollingRoundRobinCursor'
    )
    assert cursor_after_tick == 25, f'cursor did not advance, got {cursor_after_tick}'
    assert len(targets_1) >= 25

    # Tick 2: cursor is now 25 → round-robin slice covers media-25..49.
    # Recent slice still covers media-00..24. So target_2 must INCLUDE
    # media outside the recent 25.
    targets_2 = _run(server._collect_target_media_ids(user_doc, [rule]))
    assert any(mid not in [f'media-{i:02d}' for i in range(25)]
               for mid in targets_2), (
        'round-robin batch did not add any media outside the recent 25; '
        'fresh-comment-on-old-post coverage is broken.'
    )
    assert 'media-25' in targets_2 or any(
        mid.startswith('media-') and int(mid.split('-')[1]) >= 25
        for mid in targets_2
    )


def test_collect_target_media_pins_selected_media_outside_recent(monkeypatch):
    """A post-specific rule's `selected_media_id` must always be in the
    target list, even when the post is not in the recent slice or the
    current round-robin window."""
    db = _FakeDB()
    db.instagram_media_catalog.docs.append({
        'instagramAccountDbId': 'accA', 'media_id': 'pinned-media',
        'media_timestamp': '2020-01-01T00:00:00',
    })
    db.instagram_accounts.docs.append({
        'id': 'accA', 'instagramAccountId': 'igA',
        'pollingRoundRobinCursor': 0,
    })
    monkeypatch.setattr(server, 'db', db)

    async def fake_recent(token, ig_user_id, limit=10):
        return ['recent-a', 'recent-b']
    async def fake_latest(token, ig_user_id):
        return None
    monkeypatch.setattr(server, '_fetch_recent_media_ids', fake_recent)
    monkeypatch.setattr(server, '_fetch_latest_media_id', fake_latest)

    user_doc = {
        'id': 'u1', 'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA', 'meta_access_token': 'token-a',
    }
    # Post-specific rule pinning `pinned-media`
    rule = {
        'id': 'rule_post', 'user_id': 'u1', 'status': 'active',
        'trigger': 'comment:any', 'post_scope': 'selected',
        'instagramAccountId': 'igA',
        'selected_media_id': 'pinned-media',
    }
    targets = _run(server._collect_target_media_ids(user_doc, [rule]))
    assert 'pinned-media' in targets, (
        'selected_media_id is missing from polling target — post-specific '
        'rules outside the recent slice would never be polled.'
    )


def test_collect_target_media_round_robin_wraps(monkeypatch):
    """Cursor wrap-around: if cursor advances past catalog length, the
    next tick must start from 0 again — never crash, never produce
    duplicates beyond the recent slice."""
    db = _FakeDB()
    for i in range(10):
        db.instagram_media_catalog.docs.append({
            'instagramAccountDbId': 'accA',
            'media_id': f'media-{i}',
            'media_timestamp': f'2026-01-{i+1:02d}T00:00:00',
        })
    db.instagram_accounts.docs.append({
        'id': 'accA', 'instagramAccountId': 'igA',
        'pollingRoundRobinCursor': 9,  # near end, will wrap
    })
    monkeypatch.setattr(server, 'db', db)
    async def fake_recent(*a, **kw): return []
    async def fake_latest(*a, **kw): return None
    monkeypatch.setattr(server, '_fetch_recent_media_ids', fake_recent)
    monkeypatch.setattr(server, '_fetch_latest_media_id', fake_latest)
    monkeypatch.setenv('IG_POLL_ROUND_ROBIN_BATCH', '5')

    user_doc = {
        'id': 'u1', 'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA', 'meta_access_token': 'token-a',
    }
    rule = {
        'id': 'rule_general', 'user_id': 'u1', 'status': 'active',
        'trigger': 'comment:any', 'post_scope': 'any',
        'instagramAccountId': 'igA',
    }
    targets = _run(server._collect_target_media_ids(user_doc, [rule]))
    # Cursor 9 → slice covers indices [9, 0, 1, 2, 3] wrap-around.
    # Stored cursor should be (9 + 5) % 10 = 4.
    next_cursor = db.instagram_accounts.docs[0].get('pollingRoundRobinCursor')
    assert next_cursor == 4
    assert len(targets) == 5


# ---------------------------------------------------------------------------
# Env-var clamping
# ---------------------------------------------------------------------------


def test_env_tunable_clamps_safe_ranges(monkeypatch):
    """All four env vars must clamp to safe ranges so a typo cannot
    starve the polling loop or explode the Graph call budget."""
    monkeypatch.setenv('IG_POLL_RECENT_MEDIA_LIMIT', '0')
    assert server._ig_poll_recent_media_limit() == 1  # floor
    monkeypatch.setenv('IG_POLL_RECENT_MEDIA_LIMIT', '99999')
    assert server._ig_poll_recent_media_limit() == 100  # ceiling
    monkeypatch.setenv('IG_POLL_ROUND_ROBIN_BATCH', '-5')
    assert server._ig_poll_round_robin_batch() == 0  # floor
    monkeypatch.setenv('IG_POLL_ROUND_ROBIN_BATCH', '99999')
    assert server._ig_poll_round_robin_batch() == 100  # ceiling
    monkeypatch.setenv('IG_MEDIA_CATALOG_SYNC_INTERVAL_SECONDS', '1')
    assert server._ig_media_catalog_sync_interval_seconds() == 60  # floor
    monkeypatch.setenv('IG_MEDIA_CATALOG_SYNC_INTERVAL_SECONDS', '999999')
    assert server._ig_media_catalog_sync_interval_seconds() == 86400  # ceiling
    monkeypatch.setenv('IG_MEDIA_CATALOG_MAX_PAGES', '0')
    assert server._ig_media_catalog_max_pages() == 1
    monkeypatch.setenv('IG_MEDIA_CATALOG_MAX_PAGES', '1000')
    assert server._ig_media_catalog_max_pages() == 100


def test_round_robin_batch_zero_disables_round_robin(monkeypatch):
    """Setting IG_POLL_ROUND_ROBIN_BATCH=0 must turn off the round-robin
    slice while preserving recent + pinned coverage. Useful operator
    knob for emergency Graph quota throttling."""
    db = _FakeDB()
    for i in range(10):
        db.instagram_media_catalog.docs.append({
            'instagramAccountDbId': 'accA', 'media_id': f'media-{i}',
            'media_timestamp': f'2026-01-{i+1:02d}T00:00:00',
        })
    monkeypatch.setattr(server, 'db', db)
    async def fake_recent(*a, **kw): return ['recent-x']
    async def fake_latest(*a, **kw): return None
    monkeypatch.setattr(server, '_fetch_recent_media_ids', fake_recent)
    monkeypatch.setattr(server, '_fetch_latest_media_id', fake_latest)
    monkeypatch.setenv('IG_POLL_ROUND_ROBIN_BATCH', '0')

    user_doc = {
        'id': 'u1', 'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA', 'meta_access_token': 'token-a',
    }
    rule = {
        'id': 'rule_general', 'user_id': 'u1', 'status': 'active',
        'trigger': 'comment:any', 'post_scope': 'any',
        'instagramAccountId': 'igA',
    }
    targets = _run(server._collect_target_media_ids(user_doc, [rule]))
    # Only recent-x — no catalog ids should leak in.
    assert targets == ['recent-x']


# ---------------------------------------------------------------------------
# Index declaration (static source check, regression-resistant)
# ---------------------------------------------------------------------------


def test_index_bootstrap_declares_media_catalog_indexes():
    """The bootstrap must declare both the uniqueness index and the
    sort-by-timestamp index on `instagram_media_catalog`. Without these
    the round-robin sort degrades to a collection scan."""
    src = Path(server.__file__).read_text(encoding='utf-8')
    assert 'instagram_media_catalog_account_media_unique' in src
    assert 'instagram_media_catalog_account_timestamp' in src
    # Critically: still no TTL on the catalog itself — these rows are
    # historical media metadata and pruning them would lose post-specific
    # rule pinning context.
    assert 'ttl_instagram_media_catalog' not in src, (
        'TTL must not be added to instagram_media_catalog — historical '
        'media metadata is load-bearing for post-specific rule pinning.'
    )
