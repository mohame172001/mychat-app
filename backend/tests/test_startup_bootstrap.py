"""Tests for the non-blocking startup bootstrap.

Production constraint: Railway healthcheck times out if the FastAPI
startup hook awaits all index creation + per-user migrations inline on
a cold Mongo. The fix moves that heavy work into ``_index_bootstrap``
scheduled via ``create_tracked_task`` so the startup hook returns
quickly. These tests pin that contract.
"""
import asyncio
import os
import sys
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


class _Cursor:
    def __init__(self, docs):
        self.docs = list(docs)
        self._limit = None

    def limit(self, n):
        self._limit = n
        return self

    async def to_list(self, n):
        cap = self._limit if self._limit is not None else n
        return list(self.docs[:cap])

    def __aiter__(self):
        capped = list(self.docs)[: self._limit] if self._limit is not None else list(self.docs)
        async def _gen():
            for d in capped:
                yield d
        return _gen()


class _FakeCollection:
    """Slow-on-purpose so we can detect blocking vs non-blocking."""
    def __init__(self, slow_ms=0):
        self.created_indexes = []
        self.dropped_indexes = []
        self.slow_ms = slow_ms

    async def create_index(self, *args, **kwargs):
        if self.slow_ms:
            await asyncio.sleep(self.slow_ms / 1000)
        self.created_indexes.append((args, kwargs))
        return kwargs.get('name') or 'idx'

    async def drop_index(self, name):
        self.dropped_indexes.append(name)
        return None

    async def update_many(self, *_a, **_k):
        from types import SimpleNamespace
        return SimpleNamespace(modified_count=0)

    async def find_one(self, *_a, **_k):
        return None

    def find(self, *_a, **_k):
        return _Cursor([])


class _FakeDB:
    async def command(self, *_a, **_k):
        # collMod fallback path is exercised when an index already exists
        # with conflicting options. Tests can override this on individual
        # instances when they need to assert it was called.
        return {'ok': 1}

    def __init__(self, slow_ms=0):
        # Every collection touched by _index_bootstrap gets a stub.
        for name in [
            'comments', 'dm_logs', 'dm_rules', 'automations', 'conversations',
            'comment_dm_sessions', 'data_deletion_requests', 'contacts',
            'tracked_links', 'link_click_events', 'usage_events',
            'usage_reservations', 'usage_reservation_buckets',
            'webhook_processing_failures', 'dashboard_summaries',
            'instagram_automation_events', 'monthly_usage', 'user_plans',
            'admin_audit_logs', 'users', 'user_notification_preferences',
            'user_limit_overrides', 'admin_members', 'instagram_accounts',
            'instagram_account_trial_claims', 'oauth_code_consumed',
        ]:
            setattr(self, name, _FakeCollection(slow_ms=slow_ms))


def _reset_bg_state():
    server._BG_TASKS.clear()
    server._BG_FACTORIES.clear()
    server._INFLIGHT_TASKS.clear()
    server.IS_SHUTTING_DOWN = False


def test_startup_schedules_bootstrap_instead_of_awaiting_it(monkeypatch):
    """_startup must NOT await the heavy bootstrap inline. It schedules
    a tracked task and returns quickly even when the underlying DB is
    slow to create indexes."""
    _reset_bg_state()
    fake_db = _FakeDB(slow_ms=20)
    monkeypatch.setattr(server, 'db', fake_db)

    # Replace the bootstrap factory with a deliberately slow coroutine
    # so we can detect blocking. If _startup awaits it inline the test
    # will time out beyond 500ms.
    real_bootstrap = server._index_bootstrap
    bootstrap_started = asyncio.Event()
    bootstrap_can_finish = asyncio.Event()

    async def slow_bootstrap():
        bootstrap_started.set()
        await bootstrap_can_finish.wait()

    monkeypatch.setattr(server, '_index_bootstrap', slow_bootstrap)

    # Stub out the bg task registrations so we don't spawn real loops
    # that would try to call methods on the fake DB.
    registered = []
    def _fake_register(name, factory):
        registered.append(name)
        return {'task': None}

    monkeypatch.setattr(server, '_register_bg_task', _fake_register)
    monkeypatch.setattr(server, 'IG_POLL_ENABLED', False)

    async def runner():
        start = asyncio.get_event_loop().time()
        await server._startup()
        elapsed = asyncio.get_event_loop().time() - start
        # Give the scheduler one tick so the bootstrap task actually
        # starts running — proving it WAS scheduled, not awaited.
        await asyncio.sleep(0)
        is_running = bootstrap_started.is_set()
        bootstrap_can_finish.set()
        # Drain the pending bootstrap task so pytest doesn't flag it.
        pending = [t for t in server._INFLIGHT_TASKS if not t.done()]
        if pending:
            await asyncio.wait(pending, timeout=2)
        return elapsed, is_running

    elapsed, started_during_run = asyncio.run(runner())

    assert elapsed < 0.5, f'_startup blocked for {elapsed:.3f}s'
    assert started_during_run, 'bootstrap task was scheduled but never ran'
    # Bg tasks still registered (real production behaviour).
    assert 'automation_queue' in registered
    assert 'watchdog' in registered

    # Sanity: the real bootstrap is unchanged after monkeypatch revert.
    assert server._index_bootstrap is slow_bootstrap  # still patched in test
    _ = real_bootstrap  # silence unused-var


def test_index_bootstrap_creates_oauth_code_consumed_indexes(monkeypatch):
    _reset_bg_state()
    fake_db = _FakeDB(slow_ms=0)
    monkeypatch.setattr(server, 'db', fake_db)

    async def _noop_repair():
        return None
    monkeypatch.setattr(server, '_repair_legacy_reply_success_without_provider_proof', _noop_repair)

    async def _noop_migrate():
        return 0
    monkeypatch.setattr(server, '_ensure_instagram_account_docs_for_connected_users', _noop_migrate)
    monkeypatch.setattr(server, '_ensure_automation_account_scope_for_user',
                        lambda _u: _noop_migrate())

    asyncio.run(server._index_bootstrap())

    names = [kwargs.get('name') for (_args, kwargs) in fake_db.oauth_code_consumed.created_indexes]
    assert 'oauth_code_consumed_provider_hash_unique' in names
    assert 'oauth_code_consumed_ttl' in names
    # TTL index must declare expireAfterSeconds.
    ttl_kwargs = next(kw for (_a, kw) in fake_db.oauth_code_consumed.created_indexes
                      if kw.get('name') == 'oauth_code_consumed_ttl')
    assert ttl_kwargs.get('expireAfterSeconds') == 600


def test_index_bootstrap_creates_instagram_automation_events_ttl_index(monkeypatch):
    """Disk-pressure regression: instagram_automation_events grew to
    ~465MB and filled Atlas's 512MB free-tier quota, blocking writes
    across the whole database. The fix is a TTL index so Atlas's
    background TTL monitor prunes expired diagnostic events. Verify
    the bootstrap declares it with expireAfterSeconds=604800 (7d
    default) and that the indexed field is created_at."""
    _reset_bg_state()
    fake_db = _FakeDB(slow_ms=0)
    monkeypatch.setattr(server, 'db', fake_db)

    async def _noop_repair():
        return None
    monkeypatch.setattr(server, '_repair_legacy_reply_success_without_provider_proof', _noop_repair)

    async def _noop_migrate():
        return 0
    monkeypatch.setattr(server, '_ensure_instagram_account_docs_for_connected_users', _noop_migrate)
    monkeypatch.setattr(server, '_ensure_automation_account_scope_for_user',
                        lambda _u: _noop_migrate())
    monkeypatch.delenv('IG_AUTOMATION_EVENTS_TTL_SECONDS', raising=False)

    asyncio.run(server._index_bootstrap())

    ttl_calls = [
        (args, kwargs) for (args, kwargs)
        in fake_db.instagram_automation_events.created_indexes
        if kwargs.get('name') == 'ttl_instagram_automation_events_created'
    ]
    assert ttl_calls, 'TTL index was not declared on instagram_automation_events'
    args, kwargs = ttl_calls[0]
    # The index must be on created_at and carry expireAfterSeconds.
    assert args[0] == [('created_at', 1)]
    assert kwargs.get('expireAfterSeconds') == 604800


def test_index_bootstrap_clamps_ttl_within_safe_bounds(monkeypatch):
    """A misconfigured env var must not (a) empty the collection in one
    polling cycle, nor (b) silently allow another overflow incident.
    Lower bound: 1 day. Upper bound: 90 days."""
    _reset_bg_state()

    # Below the floor.
    monkeypatch.setenv('IG_AUTOMATION_EVENTS_TTL_SECONDS', '1')
    fake_db_low = _FakeDB(slow_ms=0)
    monkeypatch.setattr(server, 'db', fake_db_low)

    async def _noop_repair():
        return None
    monkeypatch.setattr(server, '_repair_legacy_reply_success_without_provider_proof', _noop_repair)

    async def _noop_migrate():
        return 0
    monkeypatch.setattr(server, '_ensure_instagram_account_docs_for_connected_users', _noop_migrate)
    monkeypatch.setattr(server, '_ensure_automation_account_scope_for_user',
                        lambda _u: _noop_migrate())

    asyncio.run(server._index_bootstrap())
    low_kwargs = next(kw for (_a, kw) in fake_db_low.instagram_automation_events.created_indexes
                      if kw.get('name') == 'ttl_instagram_automation_events_created')
    assert low_kwargs.get('expireAfterSeconds') == 86400  # 1 day floor

    # Above the ceiling.
    _reset_bg_state()
    monkeypatch.setenv('IG_AUTOMATION_EVENTS_TTL_SECONDS', '999999999')
    fake_db_high = _FakeDB(slow_ms=0)
    monkeypatch.setattr(server, 'db', fake_db_high)
    asyncio.run(server._index_bootstrap())
    high_kwargs = next(kw for (_a, kw) in fake_db_high.instagram_automation_events.created_indexes
                       if kw.get('name') == 'ttl_instagram_automation_events_created')
    assert high_kwargs.get('expireAfterSeconds') == 7776000  # 90 day ceiling


def test_index_bootstrap_ttl_failure_does_not_crash_other_indexes(monkeypatch):
    """If the TTL create raises (e.g. IndexOptionsConflict) the bootstrap
    must keep running. Atlas's existing TTL keeps pruning; the operator
    can resolve manually."""
    _reset_bg_state()
    fake_db = _FakeDB(slow_ms=0)
    original_create = fake_db.instagram_automation_events.create_index
    captured = {'collmod': False}

    async def collmod(*_a, **_k):
        captured['collmod'] = True
        return {'ok': 1}
    fake_db.command = collmod  # type: ignore[assignment]

    async def maybe_boom(*args, **kwargs):
        if kwargs.get('name') == 'ttl_instagram_automation_events_created':
            raise RuntimeError('simulated IndexOptionsConflict')
        return await original_create(*args, **kwargs)
    fake_db.instagram_automation_events.create_index = maybe_boom  # type: ignore[assignment]

    monkeypatch.setattr(server, 'db', fake_db)

    async def _noop_repair():
        return None
    monkeypatch.setattr(server, '_repair_legacy_reply_success_without_provider_proof', _noop_repair)

    async def _noop_migrate():
        return 0
    monkeypatch.setattr(server, '_ensure_instagram_account_docs_for_connected_users', _noop_migrate)
    monkeypatch.setattr(server, '_ensure_automation_account_scope_for_user',
                        lambda _u: _noop_migrate())

    # Must NOT raise.
    asyncio.run(server._index_bootstrap())

    # collMod fallback was invoked to update the existing TTL's window.
    assert captured['collmod'] is True
    # Downstream indexes (oauth_code_consumed) were still attempted.
    names = [kwargs.get('name') for (_a, kwargs) in fake_db.oauth_code_consumed.created_indexes]
    assert 'oauth_code_consumed_provider_hash_unique' in names


def test_index_bootstrap_swallows_inner_errors(monkeypatch, caplog):
    """If an inner index create raises, the bootstrap must log and
    continue — it cannot crash the app."""
    _reset_bg_state()
    fake_db = _FakeDB(slow_ms=0)

    async def boom(*_a, **_k):
        raise RuntimeError('simulated index failure')

    fake_db.comments.create_index = boom  # type: ignore[assignment]
    monkeypatch.setattr(server, 'db', fake_db)

    async def _noop_repair():
        return None
    monkeypatch.setattr(server, '_repair_legacy_reply_success_without_provider_proof', _noop_repair)

    async def _noop_migrate():
        return 0
    monkeypatch.setattr(server, '_ensure_instagram_account_docs_for_connected_users', _noop_migrate)
    monkeypatch.setattr(server, '_ensure_automation_account_scope_for_user',
                        lambda _u: _noop_migrate())

    import logging
    caplog.set_level(logging.WARNING, logger=server.logger.name)

    # Must NOT raise.
    asyncio.run(server._index_bootstrap())

    # OAuth indexes still attempted even though earlier indexes failed.
    names = [kwargs.get('name') for (_args, kwargs) in fake_db.oauth_code_consumed.created_indexes]
    assert 'oauth_code_consumed_provider_hash_unique' in names
