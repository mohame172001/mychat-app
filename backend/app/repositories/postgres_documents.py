"""PostgreSQL document persistence for the application's existing collection API.

All filtering runs in PostgreSQL. Writes lock matching rows and serialize upserts
per collection so counters, claims and unique keys are not read/write races.
Unsupported operators raise rather than silently changing application behavior.
"""
import asyncio
import hashlib
import json
import re
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from psycopg import AsyncConnection, errors, sql
from psycopg.types.json import Jsonb
from pymongo.errors import DuplicateKeyError


SCHEMA = "mychat_runtime"
COLLECTIONS = """admin_audit_logs admin_members automation_rate_limits automations broadcasts comment_dm_sessions
comments contacts conversations dashboard_summaries data_deletion_requests dm_logs dm_rules
instagram_account_trial_claims instagram_accounts instagram_automation_events instagram_media_catalog
invoices link_click_events monthly_usage oauth_code_consumed subscriptions tracked_links usage_events
usage_reservation_buckets usage_reservations user_limit_overrides user_notification_preferences
user_plans users webhook_inbox webhook_log webhook_processing_failures""".split()


async def valid_index_exists(connection, collection, name, *, unique=False):
    # Even CREATE INDEX IF NOT EXISTS can wait for table locks during a rollout.
    cursor = await connection.execute("""
        SELECT i.indisvalid, i.indisready, i.indisunique, t.relname, tn.nspname
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_index i ON i.indexrelid=c.oid
        LEFT JOIN pg_class t ON t.oid=i.indrelid
        LEFT JOIN pg_namespace tn ON tn.oid=t.relnamespace
        WHERE n.nspname=%s AND c.relname=%s
    """, (SCHEMA, name))
    row = await cursor.fetchone()
    if row is None:
        return False
    if row != (True, True, bool(unique), collection, SCHEMA):
        raise ValueError(f"Index {name} is invalid or belongs to an unexpected table/constraint")
    return True


def encode(value):
    if isinstance(value, datetime):
        return {"$date": value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc).isoformat()}
    if isinstance(value, dict):
        return {str(k): encode(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [encode(v) for v in value]
    return value


def decode(value):
    if isinstance(value, dict):
        if set(value) == {"$date"}:
            return datetime.fromisoformat(value["$date"]).astimezone(timezone.utc).replace(tzinfo=None)
        return {k: decode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [decode(v) for v in value]
    return value


def lit(value):
    return sql.Literal(Jsonb(encode(value)))


def direct(root, key):
    return sql.SQL("({} #> {})").format(root, sql.Literal(key.split(".")))


def join(parts, operator="AND"):
    return sql.SQL("({})").format(sql.SQL(f" {operator} ").join(parts)) if parts else sql.SQL("TRUE" if operator == "AND" else "FALSE")


def expression(value, root):
    if isinstance(value, str) and value.startswith("$"):
        return direct(root, value[1:])
    if not isinstance(value, dict):
        return lit(value)
    if len(value) != 1:
        raise ValueError("Unsupported expression")
    op, args = next(iter(value.items()))
    if op == "$ifNull":
        return sql.SQL("COALESCE(NULLIF({},'null'::jsonb),{})").format(expression(args[0], root), expression(args[1], root))
    if op == "$add":
        return sql.SQL("to_jsonb({})").format(sql.SQL("+").join(sql.SQL("({} #>> '{{}}')::numeric").format(expression(x, root)) for x in args))
    comparisons = {"$lte": "<=", "$lt": "<", "$gte": ">=", "$gt": ">", "$eq": "=", "$ne": "<>"}
    if op in comparisons:
        return sql.SQL("({} " + comparisons[op] + " {})").format(expression(args[0], root), expression(args[1], root))
    raise ValueError(f"Unsupported expression operator: {op}")


def condition(query, root=sql.SQL("document"), *, index=False):
    parts = []
    for field, value in query.items():
        if field in {"$and", "$or", "$nor"}:
            clause = join([condition(q, root, index=index) for q in value], "AND" if field == "$and" else "OR")
            parts.append(sql.SQL("NOT {}").format(clause) if field == "$nor" else clause)
        elif field == "$expr":
            parts.append(expression(value, root))
        elif field.startswith("$"):
            raise ValueError(f"Unsupported query operator: {field}")
        else:
            node = direct(root, field)
            # SQL expression indexes need predicates without subqueries.
            if index:
                candidates = None
            else:
                candidates = sql.SQL("{}.document_values({}, {})").format(sql.Identifier(SCHEMA), root, sql.Literal(field.split(".")))

            def any_value(predicate):
                if index:
                    return predicate(node)
                return sql.SQL("EXISTS (SELECT 1 FROM {} AS dv(v) WHERE {})").format(candidates, predicate(sql.SQL("dv.v")))

            def equals(expected):
                if expected is None:
                    return sql.SQL("({} IS NULL OR {})").format(node, any_value(lambda v: sql.SQL("{}='null'::jsonb").format(v)))
                return any_value(lambda v: sql.SQL("({}={} OR (jsonb_typeof({})='array' AND {} @> {}))").format(v, lit(expected), v, v, lit([expected])))

            operators = value if isinstance(value, dict) and any(k.startswith("$") for k in value) else {"$eq": value}
            for op, operand in operators.items():
                if op == "$options":
                    continue
                if op in {"$eq", "$ne"}:
                    clause = equals(operand)
                    parts.append(sql.SQL("NOT ({})").format(clause) if op == "$ne" else clause)
                elif op == "$exists":
                    clause = sql.SQL("{} IS NOT NULL").format(node) if index else sql.SQL("EXISTS (SELECT 1 FROM {})").format(candidates)
                    parts.append(clause if operand else sql.SQL("NOT ({})").format(clause))
                elif op in {"$in", "$nin"}:
                    clause = join([equals(x) for x in operand], "OR")
                    parts.append(sql.SQL("NOT ({})").format(clause) if op == "$nin" else clause)
                elif op in {"$lt", "$lte", "$gt", "$gte"}:
                    operator = {"$lt": "<", "$lte": "<=", "$gt": ">", "$gte": ">="}[op]
                    parts.append(any_value(lambda v: sql.SQL("(jsonb_typeof({})=jsonb_typeof({}) AND {} " + operator + " {})").format(v, lit(operand), v, lit(operand))))
                elif op == "$regex":
                    options = operators.get("$options", "")
                    if set(options) - {"i"}:
                        raise ValueError("Unsupported regex options")
                    pattern = operand.pattern if hasattr(operand, "pattern") else operand
                    parts.append(any_value(lambda v: sql.SQL("(jsonb_typeof({})='string' AND ({} #>> '{{}}') " + ("~*" if "i" in options else "~") + " {})").format(v, v, sql.Literal(pattern))))
                elif op == "$type":
                    types = {"string": "string", "object": "object", "array": "array", "bool": "boolean", "number": "number"}
                    if operand not in types:
                        raise ValueError("Unsupported type predicate")
                    parts.append(any_value(lambda v: sql.SQL("jsonb_typeof({})={}").format(v, sql.Literal(types[operand]))))
                elif op == "$elemMatch":
                    parts.append(any_value(lambda v: sql.SQL("EXISTS (SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof({})='array' THEN {} ELSE '[]'::jsonb END) elem WHERE {})").format(v, v, condition(operand, sql.SQL("elem")))))
                else:
                    raise ValueError(f"Unsupported field operator: {op}")
    return join(parts)


def get_path(doc, path, default=None):
    value = doc
    for key in path.split("."):
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def set_path(doc, path, value, *, remove=False):
    keys = path.split(".")
    target = doc
    for key in keys[:-1]:
        if key not in target:
            if remove:
                return
            target[key] = {}
        if not isinstance(target[key], dict):
            raise ValueError("Cannot update a child of a non-object field")
        target = target[key]
    if remove:
        target.pop(keys[-1], None)
    else:
        target[keys[-1]] = deepcopy(value)


def project(doc, projection):
    if not projection:
        return doc
    if isinstance(projection, (list, tuple)):
        projection = {k: 1 for k in projection}
    includes = [k for k, v in projection.items() if v and k != "_id"]
    if includes:
        result = {}
        sentinel = object()
        for key in includes:
            value = get_path(doc, key, sentinel)
            if value is not sentinel:
                set_path(result, key, value)
        if projection.get("_id", 1) and "_id" in doc:
            result["_id"] = doc["_id"]
        return result
    result = deepcopy(doc)
    for key, value in projection.items():
        if not value:
            set_path(result, key, None, remove=True)
    return result


class Cursor:
    def __init__(self, collection, query, projection=None):
        self.collection, self.query, self.projection = collection, deepcopy(query), projection
        self.order, self.maximum, self.offset = [], 0, 0

    def sort(self, keys, direction=1):
        self.order = [(keys, direction)] if isinstance(keys, str) else list(keys)
        return self

    def limit(self, count):
        self.maximum = abs(int(count))
        return self

    def skip(self, count):
        self.offset = max(0, int(count))
        return self

    async def to_list(self, length=None):
        maximum = min(self.maximum, length) if self.maximum and length is not None else self.maximum or length
        query = sql.SQL("SELECT document FROM {} WHERE {}").format(self.collection.table, condition(self.query))
        if self.order:
            query += sql.SQL(" ORDER BY ") + sql.SQL(",").join(sql.SQL("{} "+("DESC" if direction < 0 else "ASC")+" NULLS FIRST").format(direct(sql.SQL("document"), key)) for key, direction in self.order)
        if maximum is not None and maximum > 0:
            query += sql.SQL(" LIMIT {} ").format(sql.Literal(maximum))
        if length == 0:
            return []
        query += sql.SQL(" OFFSET {} ").format(sql.Literal(self.offset))
        async with self.collection.db.connection() as connection:
            rows = await (await connection.execute(query)).fetchall()
        return [project(decode(row[0]), self.projection) for row in rows]

    def __aiter__(self):
        async def iterate():
            # Keyset paging is inappropriate with arbitrary source sorts. A
            # server cursor streams the consistent query without loading all rows.
            query = sql.SQL("SELECT document FROM {} WHERE {}").format(self.collection.table, condition(self.query))
            if self.order:
                query += sql.SQL(" ORDER BY ") + sql.SQL(",").join(sql.SQL("{} "+("DESC" if direction < 0 else "ASC")).format(direct(sql.SQL("document"), key)) for key, direction in self.order)
            if self.maximum:
                query += sql.SQL(" LIMIT {}").format(sql.Literal(self.maximum))
            query += sql.SQL(" OFFSET {}").format(sql.Literal(self.offset))
            async with self.collection.db.connection() as connection:
                async with connection.cursor(name="stream_"+uuid4().hex) as cursor:
                    await cursor.execute(query)
                    async for row in cursor:
                        yield project(decode(row[0]), self.projection)
        return iterate()


class Collection:
    def __init__(self, db, name):
        self.db, self.name = db, name
        self.table = sql.Identifier(SCHEMA, name)

    def find(self, query=None, projection=None):
        return Cursor(self, query or {}, projection)

    async def find_one(self, query=None, projection=None, sort=None):
        cursor = self.find(query, projection)
        if sort:
            cursor.sort(sort)
        rows = await cursor.to_list(1)
        return rows[0] if rows else None

    async def count_documents(self, query, **kwargs):
        if kwargs:
            raise ValueError("Unsupported count options")
        async with self.db.connection() as connection:
            return (await (await connection.execute(sql.SQL("SELECT count(*) FROM {} WHERE {}").format(self.table, condition(query)))).fetchone())[0]

    async def insert_one(self, doc):
        data = deepcopy(doc)
        data.setdefault("_id", str(uuid4()))
        async with self.db.connection() as connection:
            await self._lock(connection)
            await connection.execute(sql.SQL("INSERT INTO {} (id,document) VALUES (%s,%s)").format(self.table), (str(data["_id"]), Jsonb(encode(data))))
        doc.setdefault("_id", data["_id"])
        return SimpleNamespace(inserted_id=data["_id"], acknowledged=True)

    async def _lock(self, connection):
        await connection.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("mychat-doc:"+self.name,))

    async def _apply(self, connection, doc, update, inserting=False):
        result = deepcopy(doc)
        for operator, fields in update.items():
            if operator not in {"$set", "$unset", "$inc", "$setOnInsert", "$push", "$addToSet", "$pull", "$max", "$min"}:
                raise ValueError(f"Unsupported update operator: {operator}")
            if operator == "$setOnInsert" and not inserting:
                continue
            for key, value in fields.items():
                if key == "_id" and (operator != "$setOnInsert" or not inserting):
                    raise ValueError("_id is immutable")
                old = get_path(result, key)
                if operator == "$set" or (operator == "$setOnInsert" and inserting):
                    set_path(result, key, value)
                elif operator == "$unset":
                    set_path(result, key, None, remove=True)
                elif operator == "$inc":
                    set_path(result, key, (old or 0) + value)
                elif operator in {"$max", "$min"}:
                    if old is None or (value > old if operator == "$max" else value < old):
                        set_path(result, key, value)
                elif operator in {"$push", "$addToSet"}:
                    items = deepcopy(old) if old is not None else []
                    if not isinstance(items, list):
                        raise ValueError("Array update requires array")
                    if isinstance(value, dict) and "$each" in value:
                        if set(value) - {"$each", "$slice", "$position"}:
                            raise ValueError("Unsupported array modifier")
                        additions = value["$each"]
                    else:
                        additions = [value]
                    if operator == "$addToSet":
                        for item in additions:
                            if item not in items:
                                items.append(item)
                    else:
                        position = value.get("$position", len(items)) if isinstance(value, dict) else len(items)
                        items[position:position] = additions
                        if isinstance(value, dict) and "$slice" in value:
                            size = value["$slice"]
                            items = items[:size] if size >= 0 else items[size:]
                    set_path(result, key, items)
                elif operator == "$pull":
                    retained = []
                    for item in old or []:
                        query = value if isinstance(value, dict) and not any(k.startswith("$") for k in value) else {"value": value}
                        candidate = item if query is value else {"value": item}
                        matches = (await (await connection.execute(sql.SQL("SELECT {}").format(condition(query, lit(candidate))))).fetchone())[0]
                        if not matches:
                            retained.append(item)
                    set_path(result, key, retained)
        return result

    async def _update(self, query, update, *, many=False, upsert=False, sort=None, after=False, document_result=False):
        async with self.db.connection() as connection:
            await self._lock(connection)
            statement = sql.SQL("SELECT id,document FROM {} WHERE {}").format(self.table, condition(query))
            if sort:
                statement += sql.SQL(" ORDER BY ") + sql.SQL(",").join(sql.SQL("{} "+("DESC" if direction < 0 else "ASC")).format(direct(sql.SQL("document"), key)) for key, direction in sort)
            if not many:
                statement += sql.SQL(" LIMIT 1")
            rows = await (await connection.execute(statement + sql.SQL(" FOR UPDATE"))).fetchall()
            modified, returned, inserted = 0, None, None
            for identity, raw in rows:
                previous = decode(raw)
                current = await self._apply(connection, previous, update)
                if current != previous:
                    await connection.execute(sql.SQL("UPDATE {} SET document=%s WHERE id=%s").format(self.table), (Jsonb(encode(current)), identity))
                    modified += 1
                returned = current if after else previous
            if not rows and upsert:
                seed = {}
                for key, value in query.items():
                    if not key.startswith("$") and not (isinstance(value, dict) and any(k.startswith("$") for k in value)):
                        set_path(seed, key, value)
                seed.setdefault("_id", str(uuid4()))
                current = await self._apply(connection, seed, update, inserting=True)
                inserted = current["_id"]
                await connection.execute(sql.SQL("INSERT INTO {} (id,document) VALUES (%s,%s)").format(self.table), (str(inserted), Jsonb(encode(current))))
                returned = current if after else None
        return returned if document_result else SimpleNamespace(matched_count=len(rows), modified_count=modified, upserted_id=inserted, acknowledged=True)

    async def update_one(self, query, update, upsert=False):
        return await self._update(query, update, upsert=upsert)

    async def update_many(self, query, update, upsert=False):
        return await self._update(query, update, many=True, upsert=upsert)

    async def find_one_and_update(self, query, update, upsert=False, return_document=False, sort=None, projection=None):
        result = await self._update(query, update, upsert=upsert, sort=sort, after=bool(return_document), document_result=True)
        return project(result, projection) if result is not None else None

    async def _delete(self, query, many):
        async with self.db.connection() as connection:
            await self._lock(connection)
            selected = sql.SQL("SELECT id FROM {} WHERE {}").format(self.table, condition(query))
            if not many:
                selected += sql.SQL(" LIMIT 1")
            result = await connection.execute(sql.SQL("DELETE FROM {} WHERE id IN ({})").format(self.table, selected))
        return SimpleNamespace(deleted_count=result.rowcount, acknowledged=True)

    async def delete_one(self, query):
        return await self._delete(query, False)

    async def delete_many(self, query):
        return await self._delete(query, True)

    def aggregate(self, pipeline):
        return AggregateCursor(self, pipeline)

    async def create_index(self, keys, **options):
        try:
            return await self._create_index(keys, **options)
        except Exception:
            self.db.index_failures.append((self.name, options.get("name", str(keys))))
            raise

    async def _create_index(self, keys, **options):
        if isinstance(keys, str):
            keys = [(keys, 1)]
        if set(options) - {"name", "unique", "sparse", "partialFilterExpression", "expireAfterSeconds", "background"}:
            raise ValueError("Unsupported index options")
        name = options.get("name") or "_".join(f"{k}_{d}" for k,d in keys)
        physical = "ix_" + hashlib.sha256((self.name+":"+name).encode()).hexdigest()[:32]
        predicate = condition(options.get("partialFilterExpression", {}), index=True)
        if options.get("sparse"):
            predicate = join([predicate, join([sql.SQL("{} IS NOT NULL").format(direct(sql.SQL("document"), k)) for k,d in keys], "OR")])
        terms = [sql.SQL("(COALESCE({},'null'::jsonb)) "+("DESC" if d < 0 else "ASC")).format(direct(sql.SQL("document"), k)) for k,d in keys]
        async with self.db.connection() as connection:
            if not await valid_index_exists(connection, self.name, physical, unique=options.get("unique", False)):
                await connection.execute(sql.SQL("CREATE "+("UNIQUE " if options.get("unique") else "")+"INDEX IF NOT EXISTS {} ON {} ({}) WHERE {}").format(sql.Identifier(physical), self.table, sql.SQL(",").join(terms), predicate))
            if "expireAfterSeconds" in options:
                if len(keys) != 1:
                    raise ValueError("TTL requires one field")
                await connection.execute(sql.SQL("INSERT INTO {} (collection,index_name,field,seconds) VALUES (%s,%s,%s,%s) ON CONFLICT (collection,index_name) DO UPDATE SET field=EXCLUDED.field,seconds=EXCLUDED.seconds").format(sql.Identifier(SCHEMA,"ttl_indexes")), (self.name,name,keys[0][0],int(options["expireAfterSeconds"])))
        return name

    async def drop_index(self, name):
        physical = "ix_" + hashlib.sha256((self.name+":"+name).encode()).hexdigest()[:32]
        async with self.db.connection() as connection:
            await connection.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(SCHEMA,physical)))
            await connection.execute(sql.SQL("DELETE FROM {} WHERE collection=%s AND index_name=%s").format(sql.Identifier(SCHEMA,"ttl_indexes")), (self.name,name))


class AggregateCursor:
    def __init__(self, collection, pipeline):
        self.collection, self.pipeline = collection, pipeline

    async def to_list(self, length=None):
        query = sql.SQL("SELECT document FROM {}").format(self.collection.table)
        for stage in self.pipeline:
            if len(stage) != 1:
                raise ValueError("Invalid aggregation stage")
            op, value = next(iter(stage.items()))
            if op == "$match":
                query = sql.SQL("SELECT document FROM ({}) source WHERE {}").format(query,condition(value))
            elif op == "$group":
                group = expression(value["_id"],sql.SQL("document"))
                fields = [sql.SQL("'_id',{}").format(group)]
                for field, accumulator in value.items():
                    if field == "_id":
                        continue
                    if accumulator != {"$sum": 1}:
                        raise ValueError("Unsupported accumulator")
                    fields.append(sql.SQL("{},count(*)").format(sql.Literal(field)))
                query = sql.SQL("SELECT jsonb_build_object({}) AS document FROM ({}) source GROUP BY {}").format(sql.SQL(",").join(fields),query,group)
            elif op == "$limit":
                query = sql.SQL("SELECT document FROM ({}) source LIMIT {}").format(query,sql.Literal(int(value)))
            else:
                raise ValueError(f"Unsupported aggregate stage: {op}")
        if length is not None:
            query = sql.SQL("SELECT document FROM ({}) source LIMIT {}").format(query,sql.Literal(length))
        async with self.collection.db.connection() as connection:
            rows = await (await connection.execute(query)).fetchall()
        return [decode(row[0]) for row in rows]

    def __aiter__(self):
        async def iterate():
            for item in await self.to_list():
                yield item
        return iterate()


class PostgresDocumentDatabase:
    def __init__(self, database_url):
        if not database_url:
            raise ValueError("DATABASE_URL is required")
        self.database_url = database_url
        self._gate = asyncio.Semaphore(10)
        self.index_failures = []

    def __getattr__(self, name):
        return self[name]

    def __getitem__(self, name):
        if name not in COLLECTIONS:
            raise ValueError("Unknown collection: "+name)
        return Collection(self,name)

    @asynccontextmanager
    async def connection(self):
        async with self._gate:
            try:
                async with await AsyncConnection.connect(self.database_url) as connection:
                    await connection.execute("SET LOCAL statement_timeout='30s'")
                    yield connection
            except errors.UniqueViolation:
                raise DuplicateKeyError("Duplicate application key") from None

    async def initialize(self):
        async with self.connection() as connection:
            await connection.execute("SELECT pg_advisory_xact_lock(hashtext('mychat-runtime-schema'))")
            await connection.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))
            for name in COLLECTIONS:
                await connection.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (id text PRIMARY KEY,document jsonb NOT NULL CHECK(jsonb_typeof(document)='object'))").format(sql.Identifier(SCHEMA,name)))
                if not await valid_index_exists(connection, name, name+"_doc_gin"):
                    await connection.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING gin(document jsonb_path_ops)").format(sql.Identifier(name+"_doc_gin"),sql.Identifier(SCHEMA,name)))
            await connection.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (collection text,index_name text,field text NOT NULL,seconds bigint NOT NULL CHECK(seconds>=0),PRIMARY KEY(collection,index_name))").format(sql.Identifier(SCHEMA,"ttl_indexes")))
            await connection.execute('''CREATE OR REPLACE FUNCTION mychat_runtime.document_values(doc jsonb, path text[])
RETURNS SETOF jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE item jsonb;
BEGIN
 IF doc IS NULL THEN RETURN; END IF;
 IF cardinality(path)=0 THEN RETURN NEXT doc; RETURN; END IF;
 IF jsonb_typeof(doc)='array' THEN
  IF path[1] ~ '^[0-9]+$' THEN
   RETURN QUERY SELECT * FROM mychat_runtime.document_values(doc->(path[1]::integer),path[2:]);
  ELSE
   FOR item IN SELECT value FROM jsonb_array_elements(doc) LOOP
    RETURN QUERY SELECT * FROM mychat_runtime.document_values(item,path);
   END LOOP;
  END IF;
 ELSIF jsonb_typeof(doc)='object' AND doc ? path[1] THEN
  RETURN QUERY SELECT * FROM mychat_runtime.document_values(doc->path[1],path[2:]);
 END IF;
END $$''')

    async def command(self, command, *args, **kwargs):
        if command == "ping":
            async with self.connection() as connection:
                await connection.execute("SELECT 1")
            return {"ok":1}
        if command == "collMod" and args and set(kwargs)=={"index"}:
            spec=kwargs["index"]
            async with self.connection() as connection:
                result=await connection.execute(sql.SQL("UPDATE {} SET seconds=%s WHERE collection=%s AND index_name=%s").format(sql.Identifier(SCHEMA,"ttl_indexes")),(int(spec["expireAfterSeconds"]),args[0],spec["name"]))
                if not result.rowcount:
                    raise ValueError("TTL index is not registered")
            return {"ok":1}
        raise ValueError("Unsupported database command")

    async def purge_expired(self):
        async with self.connection() as connection:
            rules=await (await connection.execute(sql.SQL("SELECT collection,field,seconds FROM {}").format(sql.Identifier(SCHEMA,"ttl_indexes")))).fetchall()
            for collection,field,seconds in rules:
                if collection not in COLLECTIONS:
                    raise ValueError("Invalid TTL collection")
                date=direct(sql.SQL("document"),field+".$date")
                await connection.execute(sql.SQL("DELETE FROM {} WHERE id IN (SELECT id FROM {} WHERE ({} #>> '{{}}')::timestamptz + (%s * interval '1 second') < clock_timestamp() LIMIT 1000)").format(sql.Identifier(SCHEMA,collection),sql.Identifier(SCHEMA,collection),date),(seconds,))
