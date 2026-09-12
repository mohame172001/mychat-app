import asyncio
import os
import sys
import unittest
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.security.mongo_token_guard import TokenGuardedCollection
from app.security.token_security import (
    LEGACY_TOKEN_BLOCKED_FLAG,
    TOKEN_PREFIX,
    decrypt_token,
    decrypt_token_document,
    encrypt_token,
)


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    async def to_list(self, *_args, **_kwargs):
        return list(self.rows)

    def __aiter__(self):
        async def _gen():
            for row in self.rows:
                yield row
        return _gen()


class FakeCollection:
    def __init__(self):
        self.rows = []
        self.last_update = None

    async def insert_one(self, doc, *_args, **_kwargs):
        self.rows.append(deepcopy(doc))

    async def find_one(self, *_args, **_kwargs):
        return deepcopy(self.rows[0]) if self.rows else None

    def find(self, *_args, **_kwargs):
        return FakeCursor(deepcopy(self.rows))

    async def update_one(self, query, update, *_args, **_kwargs):
        self.last_update = deepcopy(update)


class InstagramTokenSecurityTests(unittest.TestCase):
    def setUp(self):
        self.old_key = os.environ.get('INSTAGRAM_TOKEN_ENCRYPTION_KEY')
        os.environ['INSTAGRAM_TOKEN_ENCRYPTION_KEY'] = 'test-token-key-for-mychat-replit-readiness'

    def tearDown(self):
        if self.old_key is None:
            os.environ.pop('INSTAGRAM_TOKEN_ENCRYPTION_KEY', None)
        else:
            os.environ['INSTAGRAM_TOKEN_ENCRYPTION_KEY'] = self.old_key

    def test_encrypt_token_round_trip(self):
        encrypted = encrypt_token('secret-token')

        self.assertTrue(encrypted.startswith(TOKEN_PREFIX))
        self.assertNotIn('secret-token', encrypted)
        self.assertEqual(decrypt_token(encrypted), 'secret-token')

    def test_plaintext_document_is_blocked_when_key_is_configured(self):
        doc = decrypt_token_document('instagram_accounts', {
            'id': 'acc1',
            'accessToken': 'legacy-plain-token',
        })

        self.assertEqual(doc['accessToken'], '')
        self.assertTrue(doc[LEGACY_TOKEN_BLOCKED_FLAG])
        self.assertEqual(
            doc['instagramTokenMigrationBlockedReason'],
            'legacy_plaintext_instagram_token',
        )

    def test_collection_insert_encrypts_sensitive_fields(self):
        fake = FakeCollection()
        guarded = TokenGuardedCollection(fake, 'instagram_accounts')

        asyncio.run(guarded.insert_one({
            'id': 'acc1',
            'accessToken': 'token-to-store',
        }))

        raw = fake.rows[0]['accessToken']
        self.assertTrue(raw.startswith(TOKEN_PREFIX))
        self.assertNotIn('token-to-store', raw)
        self.assertEqual(asyncio.run(guarded.find_one({'id': 'acc1'}))['accessToken'], 'token-to-store')

    def test_collection_update_encrypts_set_fields(self):
        fake = FakeCollection()
        guarded = TokenGuardedCollection(fake, 'users')

        asyncio.run(guarded.update_one(
            {'id': 'u1'},
            {'$set': {'meta_access_token': 'new-token'}},
        ))

        stored = fake.last_update['$set']['meta_access_token']
        self.assertTrue(stored.startswith(TOKEN_PREFIX))
        self.assertNotIn('new-token', stored)


if __name__ == '__main__':
    unittest.main()
