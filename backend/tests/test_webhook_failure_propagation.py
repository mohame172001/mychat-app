import asyncio
import inspect
from unittest.mock import AsyncMock

import pytest
import server


def test_processing_failure_reaches_retry_supervisor(monkeypatch):
    monkeypatch.setattr(server, '_record_instagram_automation_event', AsyncMock())
    monkeypatch.setattr(server, '_find_user_doc_for_instagram_account_id',
                        AsyncMock(side_effect=RuntimeError('database unavailable')))
    with pytest.raises(RuntimeError, match='database unavailable'):
        asyncio.run(server._process_webhook({'entry': [{'id': 'test-account'}]}))


def test_webhook_flows_are_not_detached():
    source = inspect.getsource(server._process_webhook)
    assert 'create_tracked_task(execute_flow(' not in source
    assert source.count('await execute_flow(') == 3
