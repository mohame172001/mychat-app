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


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])
        self.find_one_calls = 0

    def find(self, query=None):  # noqa: ARG002
        return _Cursor(self.docs)

    async def find_one(self, query):  # noqa: ARG002
        self.find_one_calls += 1
        return self.docs[0] if self.docs else None

    async def count_documents(self, query):  # noqa: ARG002
        return 0


class _Cursor:
    def __init__(self, docs):
        self.docs = docs
    async def to_list(self, n):
        return list(self.docs[:n])


class FakeDB:
    def __init__(self):
        self.users = FakeCollection([])
        self.comment_dm_sessions = FakeCollection([])
        self.automations = FakeCollection([])
        self.comments = FakeCollection([])
        self.tracked_links = FakeCollection([])


def _run(coro):
    return asyncio.run(coro)


def _reset_bg_state():
    server._BG_TASKS.clear()
    server._BG_FACTORIES.clear()
    server._INFLIGHT_TASKS.clear()
    server.IS_SHUTTING_DOWN = False
    server.SHUTDOWN_EVENT = asyncio.Event()


def test_register_bg_task_starts_and_records_metadata():
    """_register_bg_task must launch the loop and record started_at."""
    _reset_bg_state()

    async def harness():
        ticks = []
        async def short_loop():
            ticks.append(1)
            await asyncio.sleep(60)  # park
        info = server._register_bg_task('harness_a', short_loop)
        # Yield once so the loop runs.
        await asyncio.sleep(0.05)
        assert info['task'] is not None
        assert info['started_at'] is not None
        assert ticks, 'loop did not run'
        # Cleanup
        info['task'].cancel()
        try:
            await info['task']
        except (Exception, asyncio.CancelledError):
            pass

    _run(harness())


def test_watchdog_restarts_crashed_loop(monkeypatch):
    """A loop that raises must be restarted by the watchdog."""
    _reset_bg_state()
    # Speed the watchdog up for the test.
    monkeypatch.setattr(server, '_WATCHDOG_INTERVAL_SECONDS', 0.05)

    runs = []

    async def crashy_loop():
        runs.append(datetime.utcnow())
        # First two runs crash; third one parks so we can observe.
        if len(runs) < 3:
            raise RuntimeError('boom')
        await asyncio.sleep(60)

    async def harness():
        server._register_bg_task('crashy', crashy_loop)
        server._register_bg_task('watchdog', server._watchdog_loop)
        # Give the watchdog enough time to detect + restart twice.
        for _ in range(40):
            await asyncio.sleep(0.05)
            if len(runs) >= 3:
                break
        # Cleanup
        for info in list(server._BG_TASKS.values()):
            info['task'].cancel()
        await asyncio.sleep(0.05)

    _run(harness())

    assert len(runs) >= 3, f'watchdog did not restart loop; runs={len(runs)}'
    crashy = server._BG_TASKS.get('crashy') or {}
    assert int(crashy.get('restarts') or 0) >= 2, \
        f'restarts not recorded; restarts={crashy.get("restarts")}'


def test_watchdog_does_not_restart_during_shutdown(monkeypatch):
    """During shutdown, the watchdog must not resurrect cancelled loops."""
    _reset_bg_state()
    monkeypatch.setattr(server, '_WATCHDOG_INTERVAL_SECONDS', 0.01)

    runs = []

    async def loop_once():
        runs.append(1)
        raise RuntimeError('boom')

    async def harness():
        server._register_bg_task('crashy', loop_once)
        await asyncio.sleep(0.02)
        server.IS_SHUTTING_DOWN = True
        server.SHUTDOWN_EVENT.set()
        await server._watchdog_loop()

    _run(harness())
    assert len(runs) == 1, 'watchdog restarted work after shutdown began'


def test_shutdown_waits_for_inflight_before_closing_client(monkeypatch):
    """Short-lived background tasks must finish/cancel before Mongo closes."""
    _reset_bg_state()
    order = []

    class FakeClient:
        def close(self):
            order.append('mongo_closed')

    monkeypatch.setattr(server, 'client', FakeClient())

    async def inflight():
        await server.SHUTDOWN_EVENT.wait()
        order.append('inflight_finished')

    async def harness():
        server.create_tracked_task(inflight(), 'test_inflight')
        await asyncio.sleep(0.01)
        await server.shutdown_db_client()

    _run(harness())
    assert order == ['inflight_finished', 'mongo_closed']


def test_bg_tick_records_success_and_failure():
    """_bg_tick must update last_success_at on success and reset
    consecutive_failures."""
    _reset_bg_state()
    server._BG_TASKS['foo'] = {
        'task': None, 'started_at': datetime.utcnow(),
        'last_tick_at': None, 'last_success_at': None,
        'last_error_at': None, 'last_error_type': None,
        'restarts': 0, 'consecutive_failures': 0,
    }
    server._bg_tick('foo', success=False, error=RuntimeError('x'))
    assert server._BG_TASKS['foo']['last_error_type'] == 'RuntimeError'
    assert server._BG_TASKS['foo']['consecutive_failures'] == 1
    server._bg_tick('foo', success=False, error=ValueError('y'))
    assert server._BG_TASKS['foo']['consecutive_failures'] == 2
    server._bg_tick('foo', success=True)
    assert server._BG_TASKS['foo']['consecutive_failures'] == 0
    assert server._BG_TASKS['foo']['last_success_at'] is not None
    assert server._BG_TASKS['foo']['last_error_type'] is None


def test_comment_poller_loop_survives_one_user_exception(monkeypatch):
    """A single bad user must not stop other users from being polled
    nor stop the loop."""
    _reset_bg_state()
    monkeypatch.setattr(server, 'IG_POLL_INTERVAL_SECONDS', 0.05)

    users = [{'id': 'u1', 'ig_user_id': 'a'}, {'id': 'u2', 'ig_user_id': 'b'}]

    class _UserColl:
        def find(self, q):  # noqa: ARG002
            return _Cursor(users)

    class _DB:
        def __init__(self):
            self.users = _UserColl()

    monkeypatch.setattr(server, 'db', _DB())

    seen = []

    async def fake_poll(u):
        seen.append(u['id'])
        if u['id'] == 'u1':
            raise RuntimeError('bad account')
        return {'newComments': 1, 'matched': 1, 'actionsSucceeded': 1, 'actionsFailed': 0}

    monkeypatch.setattr(server, '_poll_user_comments', fake_poll)

    async def harness():
        task = asyncio.create_task(server._comment_poller_loop())
        # Run a few cycles.
        for _ in range(20):
            await asyncio.sleep(0.05)
            if seen.count('u1') >= 2 and seen.count('u2') >= 2:
                break
        task.cancel()
        try:
            await task
        except (Exception, asyncio.CancelledError):
            pass

    _run(harness())

    assert seen.count('u1') >= 2, 'bad user must not block subsequent ticks'
    assert seen.count('u2') >= 2, 'good user must keep being polled'


def test_comment_poller_loop_survives_db_failure(monkeypatch):
    """A Mongo error during the outer cycle must not kill the loop."""
    _reset_bg_state()
    monkeypatch.setattr(server, 'IG_POLL_INTERVAL_SECONDS', 0.05)

    cycles = []

    class _BrokenUsers:
        def find(self, q):  # noqa: ARG002
            cycles.append(1)
            if len(cycles) <= 2:
                raise RuntimeError('mongo down')
            return _Cursor([])

    class _DB:
        users = _BrokenUsers()

    monkeypatch.setattr(server, 'db', _DB())

    async def harness():
        task = asyncio.create_task(server._comment_poller_loop())
        for _ in range(30):
            await asyncio.sleep(0.05)
            if len(cycles) >= 4:
                break
        task.cancel()
        try:
            await task
        except (Exception, asyncio.CancelledError):
            pass

    _run(harness())

    assert len(cycles) >= 4, \
        f'loop did not survive Mongo failures; cycles={len(cycles)}'


def test_health_endpoint_returns_no_tokens_and_runs():
    """/api/instagram/automation-health must return JSON with task
    statuses and never any token field. Pure unit test against the
    handler — no real DB."""
    _reset_bg_state()
    # Seed registry with a fake task.
    server._BG_TASKS['comment_poller'] = {
        'task': None, 'started_at': datetime.utcnow(),
        'last_tick_at': None, 'last_success_at': None,
        'last_error_at': None, 'last_error_type': None,
        'restarts': 0, 'consecutive_failures': 0,
    }

    class _Coll:
        async def find_one(self, q):
            return {'id': 'u1', 'instagramConnected': True,
                    'meta_access_token': 'SECRET-TOKEN', 'ig_user_id': 'igX'}
        async def count_documents(self, q):
            return 0

    class _DB:
        users = _Coll()
        comment_dm_sessions = _Coll()

    import server as srv
    srv.db = _DB()

    out = asyncio.run(srv.instagram_automation_health(user_id='u1'))
    serialized = str(out)
    assert 'SECRET-TOKEN' not in serialized, 'token leaked into health response'
    assert 'tasks' in out
    assert 'comment_poller' in out['tasks']
    assert out['accounts'][0]['instagramAccountId'] == 'igX'
    assert out['accounts'][0]['tokenPresent'] is True
