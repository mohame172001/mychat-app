import unittest

from cryptography.fernet import Fernet

from app.repositories.postgres_instagram_accounts import PostgresInstagramAccountRepository
from app.repositories.postgres_users import PostgresUserRepositoryError
from app.security.instagram_tokens import InstagramTokenCipher, TOKEN_ENVELOPE_PREFIX


class InstagramAccountMappingTests(unittest.TestCase):
    def setUp(self):
        self.repository = PostgresInstagramAccountRepository(
            "unused-unit-test-dsn", cipher=InstagramTokenCipher(Fernet.generate_key().decode()),
        )

    def test_token_is_encrypted_and_input_is_not_mutated(self):
        values = {"accessToken": "synthetic-token", "webhookEntryIdAliases": ["123"]}
        columns, extra = self.repository._split(values)
        self.assertTrue(columns["access_token"].startswith(TOKEN_ENVELOPE_PREFIX))
        self.assertEqual(self.repository.cipher.decrypt(columns["access_token"]), "synthetic-token")
        self.assertEqual(extra, {"webhookEntryIdAliases": ["123"]})
        self.assertEqual(values["accessToken"], "synthetic-token")

    def test_rejects_ambiguous_credentials_and_identity(self):
        for values in ({"accessToken": "one", "access_token": "two"}, {"userId": "other"}):
            with self.assertRaises(ValueError):
                self.repository._split(values)

    def test_rejects_nested_token_metadata(self):
        with self.assertRaises(PostgresUserRepositoryError):
            self.repository._split({"metadata": [{"refresh_token": "synthetic"}]})

    def test_server_document_restores_encrypted_token_and_account_identity(self):
        row = {
            "id": "internal", "user_id": "owner", "instagram_account_id": "external",
            "ig_user_id": "external", "access_token": self.repository.cipher.encrypt("synthetic"),
            "source_extra": {"webhookEntryIdAliases": ["123"]},
            "provider_metadata": {}, "connection_valid": True,
        }
        result = self.repository._document(row)
        self.assertEqual(result["accessToken"], "synthetic")
        self.assertEqual(result["userId"], "owner")
        self.assertEqual(result["instagramAccountId"], "external")
        self.assertEqual(result["webhookEntryIdAliases"], ["123"])
        self.assertTrue(result["connectionValid"])
