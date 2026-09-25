import asyncio
from uuid import uuid4

from psycopg import AsyncConnection, errors
from psycopg.types.json import Jsonb

import test_postgres_users_repository as user_tests
from app.repositories.postgres_instagram_accounts import (
    AccountOwnershipError, PostgresInstagramAccountRepository,
)
from app.repositories.postgres_users import PostgresUserRepositoryError
from app.security.instagram_tokens import TOKEN_ENVELOPE_PREFIX


class PostgresInstagramAccountTests(user_tests.PostgresUsersRepositoryTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.accounts = PostgresInstagramAccountRepository(
            user_tests.DATABASE_URL, cipher=self.repository._cipher,
        )

    async def asyncTearDown(self):
        if self.user_ids:
            async with await AsyncConnection.connect(user_tests.DATABASE_URL) as connection:
                await connection.execute(
                    "DELETE FROM mychat.instagram_accounts WHERE user_id=ANY(%s)", (self.user_ids,),
                )
        await super().asyncTearDown()

    async def test_two_accounts_and_reconnect_preserve_identity_and_encrypt_tokens(self):
        user = await self.repository.create(self._user())
        first = await self.accounts.save_connection(user["id"], str(uuid4()), {
            "accessToken": "synthetic-first", "username": "first", "connectionValid": True,
            "webhookEntryIdAliases": ["alias-one"],
        })
        await self.accounts.save_connection(user["id"], str(uuid4()), {
            "accessToken": "synthetic-second", "username": "second", "connectionValid": True,
        })
        reconnected = await self.accounts.save_connection(user["id"], first["instagramAccountId"], {
            "accessToken": "synthetic-rotated", "connectionValid": True,
        })
        self.assertEqual(first["id"], reconnected["id"])
        self.assertEqual(first["createdAt"], reconnected["createdAt"])
        self.assertEqual(reconnected["webhookEntryIdAliases"], ["alias-one"])
        self.assertEqual(reconnected["accessToken"], "synthetic-rotated")
        self.assertEqual(len(await self.accounts.list_for_user(user["id"])), 2)
        async with await AsyncConnection.connect(user_tests.DATABASE_URL) as connection:
            token = (await (await connection.execute(
                "SELECT access_token FROM mychat.instagram_accounts WHERE id=%s", (first["id"],)
            )).fetchone())[0]
        self.assertTrue(token.startswith(TOKEN_ENVELOPE_PREFIX))
        self.assertNotIn("synthetic-rotated", token)

    async def test_account_ownership_and_disconnect(self):
        owner = await self.repository.create(self._user())
        other = await self.repository.create(self._user())
        account = await self.accounts.save_connection(owner["id"], str(uuid4()), {
            "accessToken": "synthetic-token", "refreshToken": "synthetic-refresh", "connectionValid": True,
        })
        self.assertIsNone(await self.accounts.get(other["id"], account["id"]))
        self.assertFalse(await self.accounts.disconnect(other["id"], account["id"]))
        with self.assertRaises(AccountOwnershipError):
            await self.accounts.save_connection(other["id"], account["instagramAccountId"], {})
        self.assertTrue(await self.accounts.disconnect(owner["id"], account["id"]))
        disconnected = await self.accounts.get(owner["id"], account["id"])
        self.assertFalse(disconnected["connectionValid"])
        self.assertIsNone(disconnected["accessToken"])
        self.assertIsNone(disconnected["refreshToken"])

    async def test_concurrent_callbacks_create_one_account(self):
        user = await self.repository.create(self._user())
        instagram_id = str(uuid4())
        rows = await asyncio.gather(*[
            self.accounts.save_connection(user["id"], instagram_id, {"accessToken": "synthetic-token"})
            for _ in range(3)
        ])
        self.assertEqual(len({row["id"] for row in rows}), 1)

    async def test_metadata_cannot_hide_tokens(self):
        user = await self.repository.create(self._user())
        with self.assertRaises(PostgresUserRepositoryError):
            await self.accounts.save_connection(user["id"], str(uuid4()), {
                "metadata": {"access_token": "synthetic-raw-token"},
            })
        row = await self.accounts.save_connection(user["id"], str(uuid4()), {})
        for field, value in (("access_token", "synthetic-raw-token"),
                             ("refresh_token", "synthetic-raw-token"),
                             ("provider_metadata", Jsonb({"token": "synthetic-raw-token"}))):
            from psycopg import sql
            with self.assertRaises(errors.CheckViolation):
                async with await AsyncConnection.connect(user_tests.DATABASE_URL) as connection:
                    await connection.execute(
                        sql.SQL("UPDATE mychat.instagram_accounts SET {}=%s WHERE id=%s").format(sql.Identifier(field)),
                        (value, row["id"]),
                    )
