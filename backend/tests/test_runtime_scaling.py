import asyncio
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from cryptography.fernet import Fernet
from fastapi import HTTPException

import runtime_scaling as scaling


@pytest.fixture
def inbox(monkeypatch):
    monkeypatch.setenv('WEBHOOK_INBOX_ENCRYPTION_KEY', Fernet.generate_key().decode())
    collection = SimpleNamespace(update_one=AsyncMock(), find_one_and_update=AsyncMock(), create_index=AsyncMock())
    return scaling.WebhookInbox(SimpleNamespace(webhook_inbox=collection))


def test_enqueue_encrypts_and_deduplicates(inbox):
    async def run():
        first = await inbox.enqueue({'text': 'private', 'id': 1})
        second = await inbox.enqueue({'id': 1, 'text': 'private'})
        assert first == second
        call = inbox.collection.update_one.call_args
        assert call.kwargs['upsert'] is True
        fields = call.args[1]['$setOnInsert']
        assert 'private' not in fields['payload_encrypted']
        assert b'private' in inbox.cipher.decrypt(fields['payload_encrypted'].encode())
    asyncio.run(run())


@pytest.mark.parametrize('attempts,expected', [(1, 'pending'), (5, 'failed')])
def test_retry_is_bounded_and_redacts_errors(inbox, attempts, expected):
    async def run():
        inbox.collection.find_one_and_update.return_value = {
            '_id': 'event', 'attempts': attempts,
            'payload_encrypted': inbox.cipher.encrypt(b'{}').decode(),
        }
        process = AsyncMock(side_effect=ValueError('sensitive token'))
        assert await inbox.run_one(process)
        fields = inbox.collection.update_one.call_args.args[1]['$set']
        assert fields['status'] == expected
        assert fields['error_type'] == 'ValueError'
        assert 'sensitive' not in str(fields)
        assert ('expires_at' in fields) == (attempts == 5)
    asyncio.run(run())


def test_success_removes_payload_and_uses_owned_lease(inbox):
    async def run():
        inbox.collection.find_one_and_update.return_value = {
            '_id': 'event', 'attempts': 1,
            'payload_encrypted': inbox.cipher.encrypt(b'{"id":1}').decode(),
        }
        process = AsyncMock()
        assert await inbox.run_one(process)
        process.assert_awaited_once_with({'id': 1})
        claim = inbox.collection.find_one_and_update.call_args.args
        assert claim[0]['status']['$in'] == ['pending', 'processing']
        assert claim[1]['$set']['available_at'] > datetime.utcnow()
        query, update = inbox.collection.update_one.call_args.args
        assert query['owner'] == claim[1]['$set']['owner']
        assert update['$set']['status'] == 'completed'
        assert 'payload_encrypted' in update['$unset']
    asyncio.run(run())


def test_empty_queue_does_not_execute(inbox):
    inbox.collection.find_one_and_update.return_value = None
    process = AsyncMock()
    assert asyncio.run(inbox.run_one(process)) is False
    process.assert_not_called()


def test_crashed_worker_cannot_exceed_retry_budget(inbox):
    inbox.collection.find_one_and_update.return_value = {'_id': 'event', 'attempts': 6}
    process = AsyncMock()
    assert asyncio.run(inbox.run_one(process)) is True
    process.assert_not_called()
    assert inbox.collection.update_one.call_args.args[1]['$set']['status'] == 'failed'


def test_redis_failure_fails_closed(monkeypatch):
    monkeypatch.setenv('REDIS_URL', 'redis://unused')
    monkeypatch.setattr(scaling, '_redis_client', SimpleNamespace(eval=AsyncMock(side_effect=OSError('secret'))))
    with pytest.raises(HTTPException) as error:
        asyncio.run(scaling.shared_rate_limited('login', 'person', limit=2, window_seconds=60, fallback=lambda *a, **k: False))
    assert error.value.status_code == 503
    assert 'secret' not in error.value.detail


def test_redis_limit_and_private_key(monkeypatch):
    monkeypatch.setenv('REDIS_URL', 'redis://unused')
    redis = SimpleNamespace(eval=AsyncMock(return_value=3))
    monkeypatch.setattr(scaling, '_redis_client', redis)
    assert asyncio.run(scaling.shared_rate_limited('login', 'person@example.com', limit=2, window_seconds=60, fallback=None))
    assert 'person' not in redis.eval.call_args.args[2]


def test_legacy_fallback(monkeypatch):
    monkeypatch.delenv('REDIS_URL', raising=False)
    assert asyncio.run(scaling.shared_rate_limited('login', 'person', limit=2, window_seconds=60, fallback=lambda *a, **k: True))


def test_invalid_role_rejected(monkeypatch):
    monkeypatch.setenv('MYCHAT_PROCESS_ROLE', 'typo')
    with pytest.raises(RuntimeError):
        scaling.runtime_role()
