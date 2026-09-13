import asyncio
import unittest
from datetime import datetime, timedelta
from uuid import uuid4

from pymongo.errors import DuplicateKeyError
from app.repositories.postgres_documents import PostgresDocumentDatabase, encode, decode
from . import test_postgres_users_repository as guards


class CodecTests(unittest.TestCase):
    def test_nested_dates(self):
        value={"created":datetime(2026,1,1),"nodes":[{"at":datetime(2026,1,2)}]}
        self.assertEqual(decode(encode(value)),value)


class DocumentDatabaseTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        guards.PostgresUsersRepositoryTests.setUpClass()

    async def asyncSetUp(self):
        self.db=PostgresDocumentDatabase(guards.DATABASE_URL)
        await self.db.initialize()
        self.collection=self.db.webhook_log
        self.tag=uuid4().hex
        self.filter={"_test_run":self.tag}

    async def asyncTearDown(self):
        await self.collection.delete_many(self.filter)

    async def insert(self, **values):
        doc={**self.filter,"id":uuid4().hex,**values}
        await self.collection.insert_one(doc)
        return doc

    def query(self, **values):
        return {**self.filter,**values}

    async def test_crud_projection_and_dates(self):
        now=datetime.utcnow()
        doc=await self.insert(name="first",created=now,nested={"n":3})
        found=await self.collection.find_one(self.query(id=doc["id"]))
        self.assertEqual(found["created"],now)
        self.assertEqual(await self.collection.find_one(self.query(),{"name":1,"_id":0}),{"name":"first"})
        result=await self.collection.update_one(self.query(),{"$set":{"nested.n":4},"$unset":{"name":""}})
        self.assertEqual(result.modified_count,1)
        self.assertEqual(await self.collection.count_documents(self.query(created={"$gte":now-timedelta(seconds=1)})),1)
        self.assertEqual((await self.collection.find_one(self.query()))["nested"]["n"],4)
        self.assertEqual((await self.collection.delete_one(self.query())).deleted_count,1)

    async def test_query_operators_and_graph_arrays(self):
        await self.insert(n=5,name="HELLO",tags=["one","two"],nodes=[{"data":{"trigger":"Comment"},"type":"trigger"}])
        for criteria in [{"n":{"$gte":5,"$lt":6}}, {"missing":None}, {"missing":{"$exists":False}},
                         {"missing":{"$ne":"value"}}, {"tags":"two"}, {"tags":{"$in":["two"]}},
                         {"name":{"$regex":"^hello$","$options":"i"}}, {"nodes.data.trigger":"Comment"},
                         {"nodes":{"$elemMatch":{"type":"trigger"}}},
                         {"$or":[{"n":5},{"n":100}]}, {"n":{"$nin":[1,2]}}]:
            self.assertEqual(await self.collection.count_documents({**self.filter,**criteria}),1,criteria)
        self.assertEqual(await self.collection.count_documents(self.query(missing={"$ne":None})),0)
        with self.assertRaises(ValueError):
            await self.collection.find_one(self.query(n={"$unknown":1}))

    async def test_atomic_upsert_and_counter(self):
        key=self.query(id="counter-"+self.tag)
        await asyncio.gather(*(self.collection.update_one(key,{"$inc":{"n":1},"$setOnInsert":{"status":"pending"}},upsert=True) for _ in range(12)))
        self.assertEqual(await self.collection.count_documents(key),1)
        self.assertEqual((await self.collection.find_one(key))["n"],12)
        before=await self.collection.find_one_and_update(key,{"$inc":{"n":1}},return_document=False)
        self.assertEqual(before["n"],12)
        after=await self.collection.find_one_and_update(key,{"$inc":{"n":1}},return_document=True)
        self.assertEqual(after["n"],14)

    async def test_array_updates_and_expr_reservation(self):
        await self.insert(n=2,tags=["a"],records=[{"id":"a"},{"id":"b"}])
        await self.collection.update_one(self.filter,{"$addToSet":{"tags":{"$each":["a","b"]}},"$pull":{"records":{"id":"a"}}})
        await self.collection.update_one(self.filter,{"$push":{"tags":{"$each":["c"],"$slice":-2}},"$max":{"n":4},"$min":{"minimum":1}})
        row=await self.collection.find_one(self.filter)
        self.assertEqual(row["tags"],["b","c"])
        self.assertEqual(row["records"],[{"id":"b"}])
        self.assertEqual(row["n"],4)
        self.assertEqual(await self.collection.count_documents({**self.filter,"$expr":{"$lte":[{"$add":[{"$ifNull":["$n",0]},1]},5]}}),1)

    async def test_sort_stream_and_aggregation(self):
        for n in [2,1,3]:
            await self.insert(n=n,user_id=self.tag,status="active")
        rows=await self.collection.find(self.filter).sort("n",-1).skip(1).to_list(1)
        self.assertEqual([x["n"] for x in rows],[2])
        streamed=[x["n"] async for x in self.collection.find(self.filter).sort("n",1).limit(2)]
        self.assertEqual(streamed,[1,2])
        groups=await self.collection.aggregate([{"$match":self.filter},{"$group":{"_id":"$user_id","count":{"$sum":1}}}]).to_list(10)
        self.assertEqual(groups,[{"_id":self.tag,"count":3}])

    async def test_unique_partial_index(self):
        name="test_unique_"+self.tag
        await self.collection.create_index("id",unique=True,name=name,partialFilterExpression=self.filter)
        try:
            await self.insert(id=self.tag)
            with self.assertRaises(DuplicateKeyError):
                await self.insert(id=self.tag)
        finally:
            await self.collection.drop_index(name)
