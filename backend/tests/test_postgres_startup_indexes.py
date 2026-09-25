from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from app.repositories.postgres_documents import (
    COLLECTIONS, SCHEMA, PostgresDocumentDatabase, valid_index_exists,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
@pytest.mark.parametrize("row,exists", [(None, False), ((True, True, False, "users", SCHEMA), True)])
async def test_catalog_lookup(row, exists):
    connection = AsyncMock()
    connection.execute.return_value.fetchone.return_value = row
    assert await valid_index_exists(connection, "users", "users_doc_gin") is exists
    assert connection.execute.call_args.args[1] == (SCHEMA, "users_doc_gin")


@pytest.mark.anyio
@pytest.mark.parametrize("row", [
    (False, True, False, "users", SCHEMA),
    (True, False, False, "users", SCHEMA),
    (True, True, True, "users", SCHEMA),
    (True, True, False, "contacts", SCHEMA),
    (True, True, False, "users", "public"),
    (None, None, None, None, None),
])
async def test_invalid_or_conflicting_index_is_not_accepted(row):
    connection = AsyncMock()
    connection.execute.return_value.fetchone.return_value = row
    with pytest.raises(ValueError, match="invalid or belongs"):
        await valid_index_exists(connection, "users", "users_doc_gin")


def fake_database(monkeypatch, *, existing, unique=False):
    database = PostgresDocumentDatabase("postgresql://unused")
    statements = []

    async def execute(query, params=None):
        text = query if isinstance(query, str) else query.as_string()
        statements.append((text, params))
        cursor = AsyncMock()
        if "FROM pg_class c" in text:
            name = params[1]
            collection = name.removesuffix("_doc_gin") if name.endswith("_doc_gin") else "users"
            cursor.fetchone.return_value = (True, True, unique, collection, SCHEMA) if existing else None
        if existing and "CREATE" in text and "INDEX IF NOT EXISTS" in text:
            raise AssertionError("Existing index must not acquire DDL table locks")
        return cursor

    connection = AsyncMock()
    connection.execute.side_effect = execute

    @asynccontextmanager
    async def connect():
        yield connection

    monkeypatch.setattr(database, "connection", connect)
    return database, statements


@pytest.mark.anyio
@pytest.mark.parametrize("existing", [True, False])
async def test_initialize_creates_only_missing_indexes(monkeypatch, existing):
    database, statements = fake_database(monkeypatch, existing=existing)
    await database.initialize()
    ddl = [text for text, _ in statements if "CREATE INDEX" in text]
    assert len(ddl) == (0 if existing else len(COLLECTIONS))
    assert any("pg_advisory_xact_lock" in text for text, _ in statements)
    assert any("CREATE OR REPLACE FUNCTION" in text for text, _ in statements)


@pytest.mark.anyio
@pytest.mark.parametrize("existing", [True, False])
async def test_unique_and_ttl_metadata_are_preserved(monkeypatch, existing):
    database, statements = fake_database(monkeypatch, existing=existing, unique=True)
    result = await database.users.create_index("expiresAt", name="expiry", unique=True, expireAfterSeconds=60)
    assert result == "expiry"
    assert sum("CREATE UNIQUE INDEX" in text for text, _ in statements) == (0 if existing else 1)
    assert any(params == ("users", "expiry", "expiresAt", 60) for _, params in statements)


@pytest.mark.anyio
async def test_index_validation_failure_remains_visible(monkeypatch):
    database, _ = fake_database(monkeypatch, existing=True, unique=False)
    with pytest.raises(ValueError):
        await database.users.create_index("email", name="email_unique", unique=True)
    assert database.index_failures == [("users", "email_unique")]
