"""Development PostgreSQL integration tests for the isolated users repository."""
from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import unittest
import uuid
import asyncio
from datetime import datetime

from cryptography.fernet import Fernet
from psycopg import AsyncConnection
from psycopg.rows import dict_row


ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "backend"))

from app.repositories.postgres_users import (  # noqa: E402
    DuplicateUserError,
    PostgresUserRepository,
    PostgresUserRepositoryError,
)
from app.security.instagram_tokens import (  # noqa: E402
    InstagramTokenCipher,
    TOKEN_ENVELOPE_PREFIX,
)


DATABASE_URL = os.environ.get("DATABASE_URL", "")
RUN_DB_TESTS = os.environ.get("RUN_REPLIT_DB_TESTS") == "1"
TEST_SCHEMA_ACK = os.environ.get("REPLIT_DB_TEST_SCHEMA_ACK", "")
MIGRATION_RUNNER = ROOT / "backend/db/apply_migrations.sh"


class PostgresUsersRepositoryTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not DATABASE_URL:
            raise unittest.SkipTest("DATABASE_URL is not available")
        if not RUN_DB_TESTS:
            raise unittest.SkipTest(
                "set RUN_REPLIT_DB_TESTS=1 for development DB tests"
            )
        if TEST_SCHEMA_ACK != "mychat":
            raise unittest.SkipTest(
                "set REPLIT_DB_TEST_SCHEMA_ACK=mychat to confirm cleanup scope"
            )
        if not (os.environ.get("REPL_ID") or os.environ.get("REPL_SLUG")):
            raise unittest.SkipTest("tests require a Replit workspace")
        markers = (
            os.environ.get("APP_ENV", ""),
            os.environ.get("ENVIRONMENT", ""),
            os.environ.get("ENV", ""),
            os.environ.get("RAILWAY_ENVIRONMENT", ""),
        )
        if os.environ.get("REPLIT_DEPLOYMENT") == "1" or any(
            marker.lower() in {"production", "prod"} for marker in markers
        ):
            raise unittest.SkipTest("tests refuse deployment databases")
        subprocess.run(
            [str(MIGRATION_RUNNER)],
            check=True,
            text=True,
            capture_output=True,
            env={
                **os.environ,
                "DATABASE_URL": DATABASE_URL,
                "ALLOW_REPLIT_DEV_MIGRATIONS": "1",
            },
        )

    async def asyncSetUp(self) -> None:
        self.key = Fernet.generate_key().decode()
        self.repository = PostgresUserRepository(
            DATABASE_URL,
            cipher=InstagramTokenCipher(self.key),
        )
        self.user_ids: list[str] = []

    async def asyncTearDown(self) -> None:
        if not self.user_ids:
            return
        connection = await AsyncConnection.connect(DATABASE_URL)
        try:
            await connection.execute(
                "DELETE FROM mychat.users WHERE id = ANY(%s)",
                (self.user_ids,),
            )
            await connection.commit()
        finally:
            await connection.close()

    def _user(self, suffix: str = "") -> dict:
        unique = uuid.uuid4().hex
        user_id = f"repo-user-{unique}"
        self.user_ids.append(user_id)
        return {
            "id": user_id,
            "email": f"person{suffix}-{unique}@example.test",
            "username": f"person{suffix}_{unique}",
            "password_hash": "synthetic-password-hash",
            "name": "Repository User",
            "avatar": "https://example.test/avatar.png",
            "session_version": 0,
            "email_verified": True,
            "linked_providers": ["password"],
            "created": datetime.utcnow(),
        }

    async def test_create_get_and_update_auth_document(self) -> None:
        source = self._user()
        created = await self.repository.create(source)

        self.assertEqual(source["id"], created["id"])
        self.assertEqual(source["email"], created["normalized_email"])
        self.assertEqual(0, created["session_version"])
        self.assertEqual("Repository User", created["name"])
        self.assertIsNotNone(
            await self.repository.get_by_email(source["email"].upper())
        )
        self.assertIsNotNone(
            await self.repository.get_by_username(source["username"].upper())
        )

        result = await self.repository.update(
            source["id"],
            set_fields={
                "name": "Updated User",
                "username": f"updated_{uuid.uuid4().hex}",
                "session_version": 1,
                "updated_at": datetime.utcnow(),
            },
            unset_fields=("avatar",),
        )
        self.assertEqual(1, result.matched_count)
        self.assertEqual(1, result.modified_count)
        updated = await self.repository.get_by_id(source["id"])
        self.assertEqual("Updated User", updated["name"])
        self.assertEqual(1, updated["session_version"])
        self.assertNotIn("avatar", updated)

        missing = await self.repository.update(
            f"missing-{uuid.uuid4()}",
            set_fields={"name": "Nobody"},
        )
        self.assertEqual(0, missing.matched_count)

    async def test_repository_enforces_case_insensitive_uniqueness(self) -> None:
        first = self._user("unique")
        await self.repository.create(first)

        duplicate_email = self._user("email")
        duplicate_email["email"] = first["email"].upper()
        with self.assertRaises(DuplicateUserError) as email_error:
            await self.repository.create(duplicate_email)
        self.assertEqual("email", email_error.exception.field)

        duplicate_username = self._user("username")
        duplicate_username["username"] = first["username"].upper()
        with self.assertRaises(DuplicateUserError) as username_error:
            await self.repository.create(duplicate_username)
        self.assertEqual("username", username_error.exception.field)

    async def test_concurrent_create_serializes_email_uniqueness(self) -> None:
        first = self._user("race-a")
        second = self._user("race-b")
        second["email"] = first["email"].upper()

        results = await asyncio.gather(
            self.repository.create(first),
            self.repository.create(second),
            return_exceptions=True,
        )

        self.assertEqual(
            1,
            sum(isinstance(result, dict) for result in results),
        )
        duplicates = [
            result
            for result in results
            if isinstance(result, DuplicateUserError)
        ]
        self.assertEqual(1, len(duplicates))
        self.assertEqual("email", duplicates[0].field)

    async def test_conditional_update_increment_and_replay_protection(self) -> None:
        source = self._user("conditional")
        source["email_verification_token_hash"] = "synthetic-hash"
        await self.repository.create(source)

        mismatch = await self.repository.update_one(
            {
                "id": source["id"],
                "email_verification_token_hash": "wrong-hash",
            },
            set_fields={"email_verified": True},
        )
        self.assertEqual(0, mismatch.matched_count)

        consumed = await self.repository.update_one(
            {
                "id": source["id"],
                "email_verification_token_hash": "synthetic-hash",
            },
            set_fields={
                "email_verified": True,
                "email": f"changed-{uuid.uuid4().hex}@example.test",
            },
            unset_fields=("email_verification_token_hash",),
            increments={"session_version": 1},
        )
        self.assertEqual(1, consumed.matched_count)
        self.assertEqual(1, consumed.modified_count)

        replay = await self.repository.update_one(
            {
                "id": source["id"],
                "email_verification_token_hash": "synthetic-hash",
            },
            set_fields={"email_verified": True},
        )
        self.assertEqual(0, replay.matched_count)

        updated = await self.repository.get_by_id(source["id"])
        self.assertEqual(1, updated["session_version"])
        self.assertTrue(updated["email_verified"])
        self.assertNotIn("email_verification_token_hash", updated)
        self.assertEqual(updated["email"], updated["normalized_email"])
        self.assertIsNotNone(
            await self.repository.get_by_email(updated["email"].upper())
        )

        unchanged = await self.repository.update(
            source["id"],
            set_fields={"name": updated["name"]},
        )
        self.assertEqual(1, unchanged.matched_count)
        self.assertEqual(0, unchanged.modified_count)

    async def test_token_keys_cannot_enter_jsonb(self) -> None:
        source = self._user("nested-token")
        source["provider_metadata"] = {
            "nested": [{"instagram_token": "synthetic-raw-token"}]
        }
        with self.assertRaises(PostgresUserRepositoryError) as error:
            await self.repository.create(source)
        self.assertEqual(
            "token_field_not_allowed_in_postgres_json",
            str(error.exception),
        )

    async def test_tokens_are_encrypted_at_rest_and_blocked_with_wrong_key(self) -> None:
        source = self._user("token")
        source["meta_access_token"] = "synthetic-instagram-token"
        source["fb_page_access_token"] = "synthetic-page-token"
        await self.repository.create(source)

        connection = await AsyncConnection.connect(
            DATABASE_URL,
            row_factory=dict_row,
        )
        try:
            row = await (
                await connection.execute(
                    """
                    SELECT meta_access_token, fb_page_access_token, source_extra
                    FROM mychat.users WHERE id = %s
                    """,
                    (source["id"],),
                )
            ).fetchone()
        finally:
            await connection.close()

        self.assertTrue(row["meta_access_token"].startswith(TOKEN_ENVELOPE_PREFIX))
        self.assertTrue(
            row["fb_page_access_token"].startswith(TOKEN_ENVELOPE_PREFIX)
        )
        self.assertNotIn("synthetic-instagram-token", row["meta_access_token"])
        self.assertNotIn("synthetic-page-token", row["fb_page_access_token"])
        self.assertNotIn("meta_access_token", row["source_extra"])
        self.assertNotIn("fb_page_access_token", row["source_extra"])

        readable = await self.repository.get_by_id(source["id"])
        self.assertEqual(
            "synthetic-instagram-token",
            readable["meta_access_token"],
        )
        self.assertEqual(
            "synthetic-page-token",
            readable["fb_page_access_token"],
        )

        wrong_key_repository = PostgresUserRepository(
            DATABASE_URL,
            cipher=InstagramTokenCipher(Fernet.generate_key().decode()),
        )
        blocked = await wrong_key_repository.get_by_id(source["id"])
        self.assertEqual("", blocked["meta_access_token"])
        self.assertEqual("", blocked["fb_page_access_token"])
        self.assertEqual("blocked", blocked["token_security_status"])
        self.assertEqual(
            "instagram_token_decryption_failed",
            blocked["token_security_blocker"],
        )