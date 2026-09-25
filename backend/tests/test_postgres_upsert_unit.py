from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from app.repositories.postgres_documents import PostgresDocumentDatabase


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_existing_usage_bucket_ignores_insert_only_identity_and_defaults():
    collection = PostgresDocumentDatabase("postgresql://unused").usage_reservation_buckets
    original = {"_id": "existing", "confirmed_amount": 4, "reserved_amount": 2}
    update = {"$setOnInsert": {"_id": "new", "confirmed_amount": 0, "reserved_amount": 0}}
    assert await collection._apply(AsyncMock(), original, update) == original
    assert original["_id"] == "existing"


@pytest.mark.anyio
async def test_monthly_usage_can_increment_repeatedly_with_insert_only_defaults():
    collection = PostgresDocumentDatabase("postgresql://unused").monthly_usage
    update = {"$setOnInsert": {"_id": "first", "user_id": "owner"}, "$inc": {"comments_processed": 1}}
    first = await collection._apply(AsyncMock(), {"_id": "generated"}, update, inserting=True)
    next_update = deepcopy(update)
    next_update["$setOnInsert"]["_id"] = "later"
    second = await collection._apply(AsyncMock(), first, next_update)
    assert first == {"_id": "first", "user_id": "owner", "comments_processed": 1}
    assert second == {"_id": "first", "user_id": "owner", "comments_processed": 2}


@pytest.mark.anyio
@pytest.mark.parametrize("operator,value", [("$set", "replacement"), ("$unset", ""), ("$inc", 1)])
async def test_real_identity_mutations_are_still_rejected(operator, value):
    collection = PostgresDocumentDatabase("postgresql://unused").users
    with pytest.raises(ValueError, match="_id is immutable"):
        await collection._apply(AsyncMock(), {"_id": "existing"}, {operator: {"_id": value}})
