"""Encrypted PostgreSQL inbox; explicitly constructed, not a runtime switch."""
import asyncio
import hashlib
import json
import os
import secrets
from datetime import timedelta

from cryptography.fernet import Fernet
from psycopg import AsyncConnection
from psycopg.rows import dict_row


class PostgresWebhookInbox:
    def __init__(self, database_url, *, encryption_key=None):
        if not database_url:
            raise ValueError("DATABASE_URL is required")
        self.database_url = database_url
        key = encryption_key or os.environ.get("WEBHOOK_INBOX_ENCRYPTION_KEY", "")
        if not key:
            raise ValueError("WEBHOOK_INBOX_ENCRYPTION_KEY is required")
        self.cipher = Fernet(key.encode())

    async def enqueue(self, payload):
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        digest = hashlib.sha256(body).hexdigest()
        encrypted = self.cipher.encrypt(body).decode()
        async with await AsyncConnection.connect(self.database_url) as connection:
            await connection.execute(
                "INSERT INTO mychat.webhook_inbox (event_digest,payload_encrypted) "
                "VALUES (%s,%s) ON CONFLICT (event_digest) DO NOTHING", (digest, encrypted),
            )
        return digest

    async def run_one(self, process):
        owner, fence = secrets.token_hex(16), secrets.token_hex(16)
        # Commit the claim before calling external APIs; do not hold SQL locks
        # throughout processing. The fencing token rejects obsolete workers.
        async with await AsyncConnection.connect(self.database_url, row_factory=dict_row) as connection:
            row = await (await connection.execute(
                "SELECT * FROM mychat.claim_webhook_job(%s,%s)", (owner, fence),
            )).fetchone()
        if row is None:
            return False
        try:
            payload = json.loads(self.cipher.decrypt(row["payload_encrypted"].encode()))
            await asyncio.wait_for(process(payload), timeout=120)
        except Exception as exc:
            # Only store the exception class, not messages containing payloads,
            # bearer credentials, or external API response bodies.
            async with await AsyncConnection.connect(self.database_url) as connection:
                await connection.execute(
                    "SELECT mychat.fail_webhook_job(%s,%s,%s,%s)",
                    (row["event_digest"], fence, type(exc).__name__,
                     timedelta(seconds=min(1800, 15 * 2 ** row["attempts"]))),
                )
        else:
            async with await AsyncConnection.connect(self.database_url) as connection:
                await connection.execute(
                    "SELECT mychat.complete_webhook_job(%s,%s)", (row["event_digest"], fence),
                )
        return True

    async def purge_expired(self, *, limit=1000):
        if not 1 <= limit <= 10000:
            raise ValueError("Cleanup limit must be between 1 and 10000")
        async with await AsyncConnection.connect(self.database_url) as connection:
            result = await connection.execute(
                "WITH expired AS (SELECT event_digest FROM mychat.webhook_inbox "
                "WHERE status IN ('completed','failed') AND expires_at<=clock_timestamp() "
                "ORDER BY expires_at LIMIT %s FOR UPDATE SKIP LOCKED) "
                "DELETE FROM mychat.webhook_inbox j USING expired e WHERE j.event_digest=e.event_digest",
                (limit,),
            )
        return result.rowcount
