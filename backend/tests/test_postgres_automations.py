import asyncio
from uuid import uuid4

from psycopg import AsyncConnection

import test_postgres_users_repository as user_tests
from app.repositories.postgres_instagram_accounts import PostgresInstagramAccountRepository
from app.repositories.postgres_automations import AutomationAccountNotFound, PostgresAutomationRepository


class PostgresAutomationTests(user_tests.PostgresUsersRepositoryTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.accounts = PostgresInstagramAccountRepository(user_tests.DATABASE_URL, cipher=self.repository._cipher)
        self.rules = PostgresAutomationRepository(user_tests.DATABASE_URL)

    async def asyncTearDown(self):
        if self.user_ids:
            async with await AsyncConnection.connect(user_tests.DATABASE_URL) as connection:
                await connection.execute("DELETE FROM mychat.automations WHERE user_id=ANY(%s)", (self.user_ids,))
                await connection.execute("DELETE FROM mychat.instagram_accounts WHERE user_id=ANY(%s)", (self.user_ids,))
        await super().asyncTearDown()

    async def account(self, owner):
        return await self.accounts.save_connection(owner["id"], str(uuid4()), {"username": "synthetic"})

    async def test_round_trip_post_rule_and_status(self):
        owner = await self.repository.create(self._user())
        account = await self.account(owner)
        values = {"name": "Post keyword", "trigger": "Comment", "keyword": "info",
                  "media_id": "synthetic-post", "comment_reply": "Thanks", "dm_text": "Details",
                  "nodes": [{"id": "one", "type": "trigger"}], "edges": []}
        rule = await self.rules.create(owner["id"], account["id"], values)
        for key, value in values.items():
            self.assertEqual(rule[key], value)
        self.assertEqual(rule["instagramAccountId"], account["instagramAccountId"])
        self.assertEqual(rule["instagram_account_id"], account["id"])
        self.assertNotEqual(rule["instagramAccountId"], rule["instagram_account_id"])
        updated = await self.rules.update(owner["id"], account["id"], rule["id"], {"status": "active"})
        self.assertEqual(updated["createdAt"], rule["createdAt"])
        self.assertEqual(updated["media_id"], "synthetic-post")
        self.assertEqual(len(await self.rules.list_for_account(owner["id"], account["id"], status="active")), 1)
        self.assertEqual(await self.rules.list_for_account(owner["id"], account["id"], status="draft"), [])

    async def test_tenant_and_second_account_isolation(self):
        owner = await self.repository.create(self._user())
        other = await self.repository.create(self._user())
        account, second = await self.account(owner), await self.account(owner)
        rule = await self.rules.create(owner["id"], account["id"], {"name": "Private"})
        for user_id, account_id in ((other["id"], account["id"]), (owner["id"], second["id"])):
            self.assertIsNone(await self.rules.get(user_id, account_id, rule["id"]))
            self.assertIsNone(await self.rules.update(user_id, account_id, rule["id"], {"status": "active"}))
            self.assertEqual(await self.rules.list_for_account(user_id, account_id), [])
        with self.assertRaises(AutomationAccountNotFound):
            await self.rules.create(other["id"], account["id"], {"name": "Forged"})
        with self.assertRaises(ValueError):
            await self.rules.create(owner["id"], account["id"], {"user_id": other["id"]})

    async def test_concurrent_patches_preserve_distinct_configuration(self):
        owner = await self.repository.create(self._user())
        account = await self.account(owner)
        rule = await self.rules.create(owner["id"], account["id"], {"name": "Concurrent"})
        await asyncio.gather(
            self.rules.update(owner["id"], account["id"], rule["id"], {"keyword": "info"}),
            self.rules.update(owner["id"], account["id"], rule["id"], {"dm_text": "Details"}),
        )
        saved = await self.rules.get(owner["id"], account["id"], rule["id"])
        self.assertEqual(saved["keyword"], "info")
        self.assertEqual(saved["dm_text"], "Details")
