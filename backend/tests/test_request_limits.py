import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from request_limits import RequestBodyLimitMiddleware


@pytest.mark.parametrize('headers', [[], [(b'content-length', b'1')], [(b'content-length', b'invalid')]])
def test_actual_chunked_size_is_enforced(headers):
    app = AsyncMock()
    send = AsyncMock()
    receive = AsyncMock(side_effect=[
        {'type': 'http.request', 'body': b'123', 'more_body': True},
        {'type': 'http.request', 'body': b'456', 'more_body': False},
    ])
    asyncio.run(RequestBodyLimitMiddleware(app, 5)({'type': 'http', 'headers': headers}, receive, send))
    app.assert_not_awaited()
    assert send.await_args_list[0].args[0]['status'] == 413


def test_webhook_bytes_are_preserved_exactly():
    captured = []

    async def app(scope, receive, send):
        captured.append(await receive())

    receive = AsyncMock(side_effect=[
        {'type': 'http.request', 'body': b'{"a":', 'more_body': True},
        {'type': 'http.request', 'body': b'1}', 'more_body': False},
    ])
    asyncio.run(RequestBodyLimitMiddleware(app, 7)({'type': 'http', 'headers': []}, receive, AsyncMock()))
    assert captured == [{'type': 'http.request', 'body': b'{"a":1}', 'more_body': False}]


def test_declared_oversize_rejected_without_read():
    app, receive, send = AsyncMock(), AsyncMock(), AsyncMock()
    asyncio.run(RequestBodyLimitMiddleware(app, 5)(
        {'type': 'http', 'headers': [(b'content-length', b'6')]}, receive, send))
    receive.assert_not_awaited()
    app.assert_not_awaited()
    assert send.await_args_list[0].args[0]['status'] == 413


def test_websocket_passthrough():
    app, receive, send = AsyncMock(), AsyncMock(), AsyncMock()
    scope = {'type': 'websocket'}
    asyncio.run(RequestBodyLimitMiddleware(app, 5)(scope, receive, send))
    app.assert_awaited_once_with(scope, receive, send)
