import asyncio
import unittest
from uuid import uuid4

from cryptography.fernet import Fernet
from psycopg import AsyncConnection
from psycopg.rows import dict_row

import test_postgres_users_repository as user_tests
from app.repositories.postgres_webhook_inbox import PostgresWebhookInbox


class PostgresWebhookInboxTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        user_tests.PostgresUsersRepositoryTests.setUpClass()

    async def asyncSetUp(self):
        self.ids = []
        self.inbox = PostgresWebhookInbox(user_tests.DATABASE_URL, encryption_key=Fernet.generate_key().decode())
        async with await AsyncConnection.connect(user_tests.DATABASE_URL) as connection:
            count = (await (await connection.execute("SELECT count(*) FROM mychat.webhook_inbox")).fetchone())[0]
        if count:
            self.skipTest("Inbox tests require an empty development inbox; existing events are not consumed")

    async def asyncTearDown(self):
        if self.ids:
            async with await AsyncConnection.connect(user_tests.DATABASE_URL) as connection:
                await connection.execute("DELETE FROM mychat.webhook_inbox WHERE event_digest=ANY(%s)", (self.ids,))

    async def enqueue(self):
        payload = {"object": "instagram", "synthetic_test_id": str(uuid4())}
        digest = await self.inbox.enqueue(payload)
        self.ids.append(digest)
        return digest, payload

    async def row(self, digest):
        async with await AsyncConnection.connect(user_tests.DATABASE_URL, row_factory=dict_row) as connection:
            return await (await connection.execute("SELECT * FROM mychat.webhook_inbox WHERE event_digest=%s", (digest,))).fetchone()

    async def test_duplicate_encryption_and_completion(self):
        digest, payload = await self.enqueue()
        self.assertEqual(await self.inbox.enqueue(payload), digest)
        self.assertNotIn(payload["synthetic_test_id"], (await self.row(digest))["payload_encrypted"])
        received = []

        async def process(event):
            received.append(event)

        self.assertTrue(await self.inbox.run_one(process))
        self.assertEqual(received, [payload])
        row = await self.row(digest)
        self.assertEqual(row["status"], "completed")
        self.assertIsNone(row["payload_encrypted"])
        self.assertFalse(await self.inbox.run_one(process))

    async def test_concurrent_workers_only_one_processes_event(self):
        await self.enqueue()
        received = []

        async def process(event):
            received.append(event)

        results = await asyncio.gather(*(self.inbox.run_one(process) for _ in range(3)))
        self.assertEqual(sum(results), 1)
        self.assertEqual(len(received), 1)

    async def test_retry_budget_and_safe_error(self):
        digest, _ = await self.enqueue()

        async def process(event):
            raise ValueError("secret payload must not be stored")

        for attempt in range(1, 6):
            self.assertTrue(await self.inbox.run_one(process))
            row = await self.row(digest)
            self.assertEqual(row["attempts"], attempt)
            self.assertEqual(row["last_error"], "ValueError")
            self.assertEqual(row["status"], "failed" if attempt == 5 else "pending")
            async with await AsyncConnection.connect(user_tests.DATABASE_URL) as connection:
                await connection.execute("UPDATE mychat.webhook_inbox SET available_at=clock_timestamp() WHERE event_digest=%s", (digest,))
        self.assertIsNone((await self.row(digest))["payload_encrypted"])
        self.assertFalse(await self.inbox.run_one(process))

    async def test_expired_lease_recovery_rejects_old_worker(self):
        digest, payload = await self.enqueue()
        async with await AsyncConnection.connect(user_tests.DATABASE_URL) as connection:
            await connection.execute("SELECT * FROM mychat.claim_webhook_job(%s,%s)", ("old-worker", "old-fence"))
            await connection.execute("UPDATE mychat.webhook_inbox SET lease_until=clock_timestamp()-interval '1 second' WHERE event_digest=%s", (digest,))

        async def process(event):
            self.assertEqual(event, payload)
            async with await AsyncConnection.connect(user_tests.DATABASE_URL) as connection:
                stale_result = (await (await connection.execute(
                    "SELECT mychat.complete_webhook_job(%s,%s)", (digest, "old-fence"),
                )).fetchone())[0]
                self.assertFalse(stale_result)

        self.assertTrue(await self.inbox.run_one(process))
        row = await self.row(digest)
        self.assertEqual(row["attempts"], 2)
        self.assertEqual(row["status"], "completed")

    async def test_retention_cleanup_keeps_pending_jobs(self):
        completed, _ = await self.enqueue()

        async def process(event):
            pass

        await self.inbox.run_one(process)
        pending, _ = await self.enqueue()
        async with await AsyncConnection.connect(user_tests.DATABASE_URL) as connection:
            await connection.execute("UPDATE mychat.webhook_inbox SET expires_at=clock_timestamp()-interval '1 day' WHERE event_digest=ANY(%s)", (self.ids,))
        self.assertEqual(await self.inbox.purge_expired(limit=1), 1)
        self.assertIsNone(await self.row(completed))
        self.assertEqual((await self.row(pending))["status"], "pending")
