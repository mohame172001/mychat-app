import asyncio
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException, WebSocketDisconnect

os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server


@pytest.fixture
def socket_context(monkeypatch):
    ws = SimpleNamespace(close=AsyncMock(), receive_json=AsyncMock())
    manager = SimpleNamespace(connect=AsyncMock(), send=AsyncMock(), disconnect=Mock())
    guard = AsyncMock(return_value='user-a')
    monkeypatch.setattr(server, 'ws_manager', manager)
    monkeypatch.setattr(server, 'get_current_active_user_id', guard)
    monkeypatch.setattr(server, 'decode_token_payload', lambda token: {
        'sub': 'user-a', 'session_version': 2,
    })
    return ws, manager, guard


@pytest.mark.parametrize('reason', ['session_revoked', 'account_suspended', 'account_deleted', 'email_verification_required'])
def test_rejected_session_never_connects(socket_context, reason):
    ws, manager, guard = socket_context
    guard.side_effect = HTTPException(403, reason)
    asyncio.run(server.websocket_endpoint(ws, 'user-a', 'token'))
    manager.connect.assert_not_awaited()
    ws.close.assert_awaited_once_with(code=4001)


def test_other_user_cannot_connect(socket_context):
    ws, manager, guard = socket_context
    asyncio.run(server.websocket_endpoint(ws, 'user-b', 'token'))
    manager.connect.assert_not_awaited()
    guard.assert_not_awaited()
    ws.close.assert_awaited_once_with(code=4003)


def test_revocation_after_connect_stops_messages(socket_context):
    ws, manager, guard = socket_context
    guard.side_effect = ['user-a', HTTPException(401, 'session_revoked')]
    ws.receive_json.return_value = {'type': 'ping'}
    asyncio.run(server.websocket_endpoint(ws, 'user-a', 'token'))
    manager.send.assert_not_awaited()
    manager.disconnect.assert_called_once_with('user-a')
    ws.close.assert_awaited_once_with(code=4001)


def test_valid_session_can_ping(socket_context):
    ws, manager, guard = socket_context
    ws.receive_json.side_effect = [{'type': 'ping'}, WebSocketDisconnect()]
    asyncio.run(server.websocket_endpoint(ws, 'user-a', 'token'))
    manager.send.assert_awaited_once_with('user-a', {'type': 'pong'})
    assert guard.await_count == 2
    manager.disconnect.assert_called_once_with('user-a')


def test_non_object_message_is_rejected(socket_context):
    ws, manager, guard = socket_context
    ws.receive_json.return_value = []
    asyncio.run(server.websocket_endpoint(ws, 'user-a', 'token'))
    ws.close.assert_awaited_once_with(code=1008)
    manager.send.assert_not_awaited()
