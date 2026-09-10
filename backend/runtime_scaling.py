"""Explicit process roles and durable, bounded webhook execution."""
import asyncio
import hashlib
import json
import os
import secrets
from datetime import datetime, timedelta

from cryptography.fernet import Fernet
from pymongo import ReturnDocument


def runtime_role():
    role = os.getenv('MYCHAT_PROCESS_ROLE', 'combined').strip().lower()
    if role not in {'combined', 'api', 'worker', 'scheduler'}:
        raise RuntimeError('Invalid MYCHAT_PROCESS_ROLE')
    return role


def queue_enabled():
    return os.getenv('WEBHOOK_INBOX_ENABLED', '0').lower() in {'1', 'true'}


class WebhookInbox:
    def __init__(self, db):
        self.collection = db.webhook_inbox
        self.cipher = Fernet(os.environ['WEBHOOK_INBOX_ENCRYPTION_KEY'].encode())

    async def ensure_indexes(self):
        await self.collection.create_index([('status', 1), ('available_at', 1)])
        await self.collection.create_index('expires_at', expireAfterSeconds=0)

    async def enqueue(self, payload):
        body = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
        event_id = hashlib.sha256(body).hexdigest()
        now = datetime.utcnow()
        await self.collection.update_one({'_id': event_id}, {'$setOnInsert': {
            'payload_encrypted': self.cipher.encrypt(body).decode(),
            'status': 'pending', 'attempts': 0, 'available_at': now,
            'created_at': now,
        }}, upsert=True)
        return event_id

    async def run_one(self, process):
        now = datetime.utcnow()
        owner = secrets.token_hex(16)
        row = await self.collection.find_one_and_update({
            'status': {'$in': ['pending', 'processing']},
            'available_at': {'$lte': now},
        }, {'$set': {'status': 'processing', 'owner': owner,
                     'available_at': now + timedelta(seconds=180)},
            '$inc': {'attempts': 1}},
            sort=[('available_at', 1)], return_document=ReturnDocument.AFTER)
        if row is None:
            return False
        query = {'_id': row['_id'], 'owner': owner}
        # A worker can die without reaching the exception handler. Its expired
        # lease must still count toward the retry budget.
        if row['attempts'] > 5:
            await self.collection.update_one(query, {'$set': {
                'status': 'failed', 'error_type': 'AttemptsExhausted',
                'expires_at': now + timedelta(days=30),
            }, '$unset': {'owner': ''}})
            return True
        try:
            payload = json.loads(self.cipher.decrypt(row['payload_encrypted'].encode()))
            # Finish or cancel before the lease expires; a crashed process can
            # be reclaimed. Existing action idempotency handles partial retries.
            await asyncio.wait_for(process(payload), timeout=120)
        except Exception as exc:
            failed = row['attempts'] >= 5
            fields = {'status': 'failed' if failed else 'pending',
                      'error_type': type(exc).__name__,
                      'available_at': datetime.utcnow() + timedelta(seconds=min(1800, 15 * 2 ** row['attempts']))}
            if failed:
                fields['expires_at'] = datetime.utcnow() + timedelta(days=30)
            await self.collection.update_one(query, {'$set': fields})
        else:
            await self.collection.update_one(query, {
                '$set': {'status': 'completed', 'completed_at': datetime.utcnow(),
                         'expires_at': datetime.utcnow() + timedelta(days=7)},
                '$unset': {'payload_encrypted': '', 'owner': ''},
            })
        return True


_redis_client = None


async def shared_rate_limited(bucket, key, *, limit, window_seconds, fallback):
    url = os.getenv('REDIS_URL', '').strip()
    if not url:
        return fallback(bucket, key, limit=limit, window_seconds=window_seconds)
    if not key:
        return False
    global _redis_client
    if _redis_client is None:
        from redis.asyncio import Redis
        _redis_client = Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)
    digest = hashlib.sha256(f'{bucket}:{key}'.encode()).hexdigest()
    # Atomic fixed window shared by every API replica. Redis time/TTL is
    # authoritative, and identifiers never appear in Redis keys.
    script = """
    local n = redis.call('INCR', KEYS[1])
    if n == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
    return n
    """
    from fastapi import HTTPException
    try:
        count = await _redis_client.eval(script, 1, 'mychat:rate:' + digest, window_seconds)
    except Exception:
        raise HTTPException(503, 'Please try again shortly.', headers={'Retry-After': '5'}) from None
    return count > limit


async def close_redis():
    global _redis_client
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
