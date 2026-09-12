from typing import Any, Dict, Optional

from .token_security import (
    decrypt_token_document,
    encrypt_token_document,
    encrypt_token_update,
)


TOKEN_PROTECTED_COLLECTIONS = {'users', 'instagram_accounts'}


class TokenGuardedCursor:
    def __init__(self, cursor: Any, collection_name: str):
        self._cursor = cursor
        self._collection_name = collection_name

    def sort(self, *args, **kwargs):
        self._cursor = self._cursor.sort(*args, **kwargs)
        return self

    def limit(self, *args, **kwargs):
        self._cursor = self._cursor.limit(*args, **kwargs)
        return self

    def skip(self, *args, **kwargs):
        self._cursor = self._cursor.skip(*args, **kwargs)
        return self

    async def to_list(self, *args, **kwargs):
        rows = await self._cursor.to_list(*args, **kwargs)
        return [decrypt_token_document(self._collection_name, row) for row in rows]

    def __aiter__(self):
        async def _gen():
            async for row in self._cursor:
                yield decrypt_token_document(self._collection_name, row)
        return _gen()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cursor, name)


class TokenGuardedCollection:
    def __init__(self, collection: Any, collection_name: str):
        self._collection = collection
        self._collection_name = collection_name

    async def find_one(self, *args, **kwargs):
        row = await self._collection.find_one(*args, **kwargs)
        return decrypt_token_document(self._collection_name, row)

    def find(self, *args, **kwargs):
        return TokenGuardedCursor(
            self._collection.find(*args, **kwargs),
            self._collection_name,
        )

    async def insert_one(self, doc: Dict[str, Any], *args, **kwargs):
        return await self._collection.insert_one(
            encrypt_token_document(self._collection_name, doc),
            *args,
            **kwargs,
        )

    async def update_one(self, query: Dict[str, Any], update: Dict[str, Any], *args, **kwargs):
        return await self._collection.update_one(
            query,
            encrypt_token_update(self._collection_name, update),
            *args,
            **kwargs,
        )

    async def update_many(self, query: Dict[str, Any], update: Dict[str, Any], *args, **kwargs):
        return await self._collection.update_many(
            query,
            encrypt_token_update(self._collection_name, update),
            *args,
            **kwargs,
        )

    async def find_one_and_update(
        self,
        query: Dict[str, Any],
        update: Dict[str, Any],
        *args,
        **kwargs,
    ):
        row = await self._collection.find_one_and_update(
            query,
            encrypt_token_update(self._collection_name, update),
            *args,
            **kwargs,
        )
        return decrypt_token_document(self._collection_name, row)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._collection, name)


class TokenGuardedDatabase:
    def __init__(self, database: Any):
        self._database = database
        self._cache: Dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        collection = getattr(self._database, name)
        if name not in TOKEN_PROTECTED_COLLECTIONS:
            return collection
        if name not in self._cache:
            self._cache[name] = TokenGuardedCollection(collection, name)
        return self._cache[name]

    def __getitem__(self, name: str) -> Any:
        collection = self._database[name]
        if name not in TOKEN_PROTECTED_COLLECTIONS:
            return collection
        if name not in self._cache:
            self._cache[name] = TokenGuardedCollection(collection, name)
        return self._cache[name]
