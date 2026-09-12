import asyncio
import logging
import os
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet

from app.security.instagram_tokens import (
    EncryptedTokenUnavailable,
    InstagramTokenCipher,
    PlaintextTokenMigrationRequired,
    TOKEN_ENVELOPE_PREFIX,
    TokenEncryptionNotConfigured,
    TokenProtectedDatabase,
    TokenRedactionFilter,
    protect_document_for_read,
    redact_log_value,
    redact_token_text,
)
from app.services.instagram.account_resolver import (
    _with_instagram_account_context,
)


class FakeCollection:
    def __init__(self):
        self.documents = []

    async def insert_one(self, document, *args, **kwargs):
        self.documents.append(document)
        return object()

    async def find_one(self, query, *args, **kwargs):
        return self.documents[0] if self.documents else None

    async def update_one(self, query, update, *args, **kwargs):
        if not self.documents:
            self.documents.append({})
        self.documents[0].update(update.get("$set", {}))
        return object()


class FakeDatabase:
    def __init__(self):
        self.collections = {
            "users": FakeCollection(),
            "instagram_accounts": FakeCollection(),
        }

    def __getitem__(self, name):
        return self.collections.setdefault(name, FakeCollection())

    def __getattr__(self, name):
        return self[name]


class InstagramTokenEncryptionTests(unittest.TestCase):
    def test_authenticated_encryption_round_trip_and_wrong_key_failure(self):
        plaintext = "synthetic-meta-token"
        cipher = InstagramTokenCipher(Fernet.generate_key().decode())
        encrypted = cipher.encrypt(plaintext)

        self.assertTrue(encrypted.startswith(TOKEN_ENVELOPE_PREFIX))
        self.assertNotIn(plaintext, encrypted)
        self.assertEqual(plaintext, cipher.decrypt(encrypted))

        wrong_cipher = InstagramTokenCipher(Fernet.generate_key().decode())
        with self.assertRaises(EncryptedTokenUnavailable) as raised:
            wrong_cipher.decrypt(encrypted)
        self.assertEqual(
            "instagram_token_decryption_failed", str(raised.exception)
        )
        self.assertNotIn(plaintext, str(raised.exception))

    def test_missing_key_and_plaintext_are_explicitly_blocked(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("INSTAGRAM_TOKEN_ENCRYPTION_KEY", None)
            cipher = InstagramTokenCipher()
            with self.assertRaises(TokenEncryptionNotConfigured) as missing:
                cipher.encrypt("synthetic-token")
        self.assertEqual(
            "instagram_token_encryption_not_configured",
            str(missing.exception),
        )

        with self.assertRaises(PlaintextTokenMigrationRequired) as legacy:
            cipher.decrypt("legacy-plaintext-token")
        self.assertEqual(
            "instagram_token_migration_required", str(legacy.exception)
        )
        self.assertNotIn("legacy-plaintext-token", str(legacy.exception))

    def test_plaintext_record_is_not_returned_as_usable_token(self):
        document = protect_document_for_read(
            "instagram_accounts",
            {"id": "account-1", "accessToken": "legacy-plaintext-token"},
            InstagramTokenCipher(Fernet.generate_key().decode()),
        )

        self.assertEqual("", document["accessToken"])
        self.assertEqual("migration_needed", document["token_security_status"])
        self.assertEqual(
            "instagram_token_migration_required",
            document["token_security_blocker"],
        )
        self.assertEqual(
            "instagram_token_migration_required",
            document["instagram_connection_blocker"],
        )
        self.assertFalse(document["connectionValid"])
        self.assertNotIn("legacy-plaintext-token", repr(document))

    def test_mongo_boundary_encrypts_new_writes_and_decrypts_reads(self):
        raw_db = FakeDatabase()
        cipher = InstagramTokenCipher(Fernet.generate_key().decode())
        db = TokenProtectedDatabase(raw_db, cipher)

        asyncio.run(
            db.users.insert_one(
                {"id": "user-1", "meta_access_token": "synthetic-meta-token"}
            )
        )
        stored = raw_db.users.documents[0]["meta_access_token"]
        self.assertTrue(stored.startswith(TOKEN_ENVELOPE_PREFIX))
        self.assertNotIn("synthetic-meta-token", stored)

        user = asyncio.run(db.users.find_one({"id": "user-1"}))
        self.assertEqual("synthetic-meta-token", user["meta_access_token"])
        self.assertEqual("encrypted", user["token_security_status"])

        asyncio.run(
            db.instagram_accounts.update_one(
                {"id": "account-1"},
                {"$set": {"accessToken": "synthetic-account-token"}},
                upsert=True,
            )
        )
        account_stored = raw_db.instagram_accounts.documents[0]["accessToken"]
        self.assertTrue(account_stored.startswith(TOKEN_ENVELOPE_PREFIX))
        self.assertNotIn("synthetic-account-token", account_stored)

        asyncio.run(
            db.users.update_one(
                {"id": "user-1"},
                {"$set": {"fb_page_access_token": "synthetic-page-token"}},
            )
        )
        page_stored = raw_db.users.documents[0]["fb_page_access_token"]
        self.assertTrue(page_stored.startswith(TOKEN_ENVELOPE_PREFIX))
        self.assertNotIn("synthetic-page-token", page_stored)

        for field in (
            "accessToken",
            "access_token",
            "longLivedAccessToken",
            "long_lived_access_token",
            "refreshToken",
            "refresh_token",
            "token",
        ):
            raw_db.instagram_accounts.documents.clear()
            asyncio.run(
                db.instagram_accounts.insert_one(
                    {"id": field, field: f"synthetic-{field}"}
                )
            )
            alias_stored = raw_db.instagram_accounts.documents[0][field]
            self.assertTrue(
                alias_stored.startswith(TOKEN_ENVELOPE_PREFIX),
                field,
            )
            alias_read = asyncio.run(
                db.instagram_accounts.find_one({"id": field})
            )
            self.assertEqual(f"synthetic-{field}", alias_read[field], field)

    def test_blocked_account_token_never_falls_back_to_user_token(self):
        cipher = InstagramTokenCipher(Fernet.generate_key().decode())
        account = protect_document_for_read(
            "instagram_accounts",
            {"id": "account-1", "accessToken": "legacy-plaintext-token"},
            cipher,
        )
        merged = _with_instagram_account_context(
            {"id": "user-1", "meta_access_token": "usable-user-token"},
            account,
        )

        self.assertEqual("", merged["meta_access_token"])
        self.assertEqual(
            "instagram_token_migration_required",
            merged["instagram_token_blocker"],
        )

    def test_token_redaction_covers_logs_mappings_urls_and_envelopes(self):
        sentinel = "synthetic-super-secret"
        envelope = TOKEN_ENVELOPE_PREFIX + "gAAAAA-synthetic-ciphertext"
        values = {
            "accessToken": sentinel,
            "nested": [
                f"https://graph.example/me?access_token={sentinel}&fields=id",
                f"Authorization: Bearer {sentinel}",
                envelope,
            ],
        }

        redacted = redact_log_value(values)
        self.assertNotIn(sentinel, repr(redacted))
        self.assertNotIn(envelope, repr(redacted))
        self.assertIn("***REDACTED***", repr(redacted))
        self.assertNotIn(
            sentinel,
            redact_token_text(
                f"https://graph.example/me?access_token={sentinel}"
            ),
        )

        record = logging.LogRecord(
            "test",
            logging.ERROR,
            __file__,
            1,
            "request failed: %s",
            (values,),
            None,
        )
        self.assertTrue(TokenRedactionFilter().filter(record))
        self.assertNotIn(sentinel, record.getMessage())

        try:
            raise RuntimeError(
                f"https://graph.example/me?access_token={sentinel}"
            )
        except RuntimeError:
            import sys

            exception_record = logging.LogRecord(
                "test",
                logging.ERROR,
                __file__,
                1,
                "provider request failed",
                (),
                sys.exc_info(),
            )
        self.assertTrue(TokenRedactionFilter().filter(exception_record))
        formatted = logging.Formatter().format(exception_record)
        self.assertNotIn(sentinel, formatted)
        self.assertIn("exception_type=RuntimeError", formatted)


if __name__ == "__main__":
    unittest.main()