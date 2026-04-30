"""Tests for graceful shutdown plumbing.

Verifies that:
1. SHUTDOWN_EVENT + IS_SHUTTING_DOWN prevent new Mongo writes.
2. create_tracked_task refuses work once shutdown has begun.
3. Background loops (_comment_poller_loop, _follow_verifier_loop) exit
   promptly when SHUTDOWN_EVENT is set instead of sleeping for the full interval.
4. The shutdown hook runs the ordered sequence: flags set → bg tasks
   cancelled → in-flight drained → Mongo closed.
"""
import asyncio
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

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


# ── 1. create_tracked_task refuses when shutting down ────────────────────────

def test_create_tracked_task_refused_during_shutdown():
    """create_tracked_task returns None and closes the coroutine if IS_SHUTTING_DOWN."""
    server.IS_SHUTTING_DOWN = True
    server.SHUTDOWN_EVENT.set()
    try:
        closed = []

        async def _noop():
            pass

        coro = _noop()
        result = server.create_tracked_task(coro, 'test_task')
        assert result is None, 'should return None during shutdown'
    finally:
        server.IS_SHUTTING_DOWN = False
        server.SHUTDOWN_EVENT.clear()
        server._INFLIGHT_TASKS.clear()


def test_create_tracked_task_registers_when_running():
    """create_tracked_task registers the task in _INFLIGHT_TASKS."""
    server.IS_SHUTTING_DOWN = False
    server._INFLIGHT_TASKS.clear()

    async def _inner():
        server.SHUTDOWN_EVENT.clear()

        async def _noop():
            pass

        task = server.create_tracked_task(_noop(), 'my_task')
        assert task is not None
        assert task in server._INFLIGHT_TASKS
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        server._INFLIGHT_TASKS.clear()

    _run(_inner())


# ── 2. _handle_new_comment bails when IS_SHUTTING_DOWN ───────────────────────

def test_handle_new_comment_skips_during_shutdown(monkeypatch):
    """_handle_new_comment returns immediately with reason=shutting_down."""
    server.IS_SHUTTING_DOWN = True
    user = {'id': 'u1', 'ig_user_id': 'biz1', 'meta_access_token': 'tok',
            'instagramConnected': True}
    comment = {'ig_comment_id': 'c99', 'commenter_id': 'fan1', 'text': 'hi',
               'media_id': 'm1', 'timestamp': '2024-01-01T00:00:00+0000'}
    try:
        res = _run(server._handle_new_comment(user, comment, source='polling'))
        assert res.get('reason') == 'shutting_down'
        assert res.get('processed') is False
    finally:
        server.IS_SHUTTING_DOWN = False


# ── 3. Comment poller exits promptly on SHUTDOWN_EVENT ───────────────────────

def test_comment_poller_exits_on_shutdown_event(monkeypatch):
    """The poller exits within 0.5 s once SHUTDOWN_EVENT is set."""
    server.IS_SHUTTING_DOWN = False
    server.SHUTDOWN_EVENT.clear()

    tick_calls = []

    async def fake_poll_user(u):
        tick_calls.append(u.get('id'))
        return {}

    monkeypatch.setattr(server, '_poll_user_comments', fake_poll_user)

    class FakeCursor:
        async def to_list(self, n):
            return []

    class FakeDB:
        class users:
            @staticmethod
            def find(q):
                return FakeCursor()

    monkeypatch.setattr(server, 'db', FakeDB())
    monkeypatch.setattr(server, 'IG_POLL_INTERVAL_SECONDS', 60)

    async def _run_test():
        server.SHUTDOWN_EVENT = asyncio.Event()
        task = asyncio.create_task(server._comment_poller_loop())
        # Let one tick run then signal shutdown
        await asyncio.sleep(0.05)
        server.IS_SHUTTING_DOWN = True
        server.SHUTDOWN_EVENT.set()
        try:
            await asyncio.wait_for(task, timeout=0.5)
        except asyncio.TimeoutError:
            task.cancel()
            raise AssertionError('comment_poller_loop did not exit within 0.5s')
        except asyncio.CancelledError:
            pass

    try:
        _run(_run_test())
    finally:
        server.IS_SHUTTING_DOWN = False
        server.SHUTDOWN_EVENT = asyncio.Event()


# ── 4. Follow verifier exits promptly on SHUTDOWN_EVENT ──────────────────────

def test_follow_verifier_exits_on_shutdown_event(monkeypatch):
    """The follow verifier exits within 0.5 s once SHUTDOWN_EVENT is set."""
    server.IS_SHUTTING_DOWN = False

    class FakeCursor:
        async def to_list(self, n):
            return []

    class FakeSessionsCollection:
        def find(self, q):
            return FakeCursor()

    class FakeDB:
        comment_dm_sessions = FakeSessionsCollection()

    monkeypatch.setattr(server, 'db', FakeDB())
    monkeypatch.setattr(server, 'FOLLOW_BACKGROUND_VERIFIER_INTERVAL_SECONDS', 60)

    async def _run_test():
        # Recreate the event bound to the current loop so wait() works
        server.SHUTDOWN_EVENT = asyncio.Event()
        task = asyncio.create_task(server._follow_verifier_loop())
        await asyncio.sleep(0.05)
        server.IS_SHUTTING_DOWN = True
        server.SHUTDOWN_EVENT.set()
        try:
            await asyncio.wait_for(task, timeout=0.5)
        except asyncio.TimeoutError:
            task.cancel()
            raise AssertionError('_follow_verifier_loop did not exit within 0.5s')
        except asyncio.CancelledError:
            pass

    try:
        _run(_run_test())
    finally:
        server.IS_SHUTTING_DOWN = False
        server.SHUTDOWN_EVENT = asyncio.Event()


# ── 5. Shutdown hook ordered sequence ────────────────────────────────────────

def test_shutdown_hook_sets_flags_and_closes_mongo(monkeypatch):
    """shutdown_db_client sets IS_SHUTTING_DOWN, sets SHUTDOWN_EVENT, then
    closes the Mongo client."""
    server.IS_SHUTTING_DOWN = False
    server.SHUTDOWN_EVENT = asyncio.Event()
    server._INFLIGHT_TASKS.clear()
    server._BG_TASKS.clear()

    close_called = []
    fake_client = MagicMock()
    fake_client.close = MagicMock(side_effect=lambda: close_called.append(True))
    monkeypatch.setattr(server, 'client', fake_client)

    _run(server.shutdown_db_client())

    assert server.IS_SHUTTING_DOWN is True
    assert server.SHUTDOWN_EVENT.is_set()
    assert close_called, 'Mongo client.close() must be called'

    # Reset for other tests
    server.IS_SHUTTING_DOWN = False
    server.SHUTDOWN_EVENT = asyncio.Event()


def test_shutdown_hook_waits_for_inflight_tasks(monkeypatch):
    """In-flight tasks are awaited before Mongo is closed."""
    server.IS_SHUTTING_DOWN = False
    server.SHUTDOWN_EVENT = asyncio.Event()
    server._INFLIGHT_TASKS.clear()
    server._BG_TASKS.clear()

    sequence = []

    async def slow_task():
        await asyncio.sleep(0.05)
        sequence.append('task_done')

    fake_client = MagicMock()
    fake_client.close = MagicMock(side_effect=lambda: sequence.append('mongo_closed'))
    monkeypatch.setattr(server, 'client', fake_client)

    async def _run_test():
        task = asyncio.create_task(slow_task())
        server._INFLIGHT_TASKS.add(task)
        task.add_done_callback(server._INFLIGHT_TASKS.discard)
        await server.shutdown_db_client()

    _run(_run_test())

    assert 'task_done' in sequence, 'inflight task must finish'
    assert 'mongo_closed' in sequence, 'mongo must close'
    assert sequence.index('task_done') < sequence.index('mongo_closed'), \
        'task_done must precede mongo_closed'

    server.IS_SHUTTING_DOWN = False
    server.SHUTDOWN_EVENT = asyncio.Event()
    server._INFLIGHT_TASKS.clear()
