"""PostgreSQL automation persistence, isolated until the complete runtime cutover.

Account parameters are internal database IDs; provider IDs are resolved from the
owned account, never trusted from caller-supplied rule configuration.
"""
from copy import deepcopy
from datetime import datetime, timezone
from uuid import uuid4

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.repositories.postgres_users import _json_restore, _json_safe


FIELDS = {
    "name": "name", "status": "status", "automation_type": "automation_type",
    "trigger": "trigger_type", "run_count": "run_count", "createdAt": "created_at",
    "updatedAt": "updated_at", "nodes": "nodes", "edges": "edges",
}
IDENTITY_FIELDS = {
    "id", "user_id", "instagramAccountDbId", "instagram_account_id",
    "instagramAccountId", "igUserId", "instagramUsername",
}
SELECT = """SELECT a.*, i.instagram_account_id AS provider_id, i.username AS account_username
    FROM mychat.automations a JOIN mychat.instagram_accounts i
    ON i.id=a.instagram_account_id AND i.user_id=a.user_id
    WHERE a.user_id=%s AND a.instagram_account_id=%s"""


class AutomationAccountNotFound(ValueError):
    pass


def split_values(values):
    values = deepcopy(dict(values))
    if IDENTITY_FIELDS.intersection(values):
        raise ValueError("Automation identity must be supplied through explicit parameters")
    columns = {}
    for field, column in FIELDS.items():
        if field in values:
            value = values.pop(field)
            columns[column] = Jsonb(_json_safe(value)) if field in {"nodes", "edges"} else value
    return columns, values


def document(row):
    if row is None:
        return None
    result = _json_restore(row.get("source_extra") or {})
    result.update(_json_restore(row.get("config") or {}))
    for field, column in FIELDS.items():
        if row.get(column) is not None:
            result[field] = _json_restore(row[column])
    result.update(id=row["id"], user_id=row["user_id"],
                  instagramAccountDbId=row["instagram_account_id"],
                  instagram_account_id=row["instagram_account_id"],
                  instagramAccountId=row["provider_id"], igUserId=row["provider_id"],
                  instagramUsername=row["account_username"] or "")
    return result


class PostgresAutomationRepository:
    def __init__(self, database_url):
        if not database_url:
            raise ValueError("DATABASE_URL is required")
        self.database_url = database_url

    async def create(self, user_id, account_id, values):
        columns, config = split_values(values)
        now = datetime.now(timezone.utc)
        columns.setdefault("created_at", now)
        columns["updated_at"] = now
        columns.setdefault("status", "draft")
        async with await AsyncConnection.connect(self.database_url, row_factory=dict_row) as connection:
            account = await (await connection.execute(
                "SELECT id FROM mychat.instagram_accounts WHERE id=%s AND user_id=%s FOR UPDATE",
                (account_id, user_id),
            )).fetchone()
            if not account:
                raise AutomationAccountNotFound("Connected account does not belong to this user")
            rule_id = str(uuid4())
            columns.update(id=rule_id, user_id=user_id, instagram_account_id=account_id,
                           config=Jsonb(_json_safe(config)))
            await connection.execute(
                sql.SQL("INSERT INTO mychat.automations ({}) VALUES ({})").format(
                    sql.SQL(",").join(map(sql.Identifier, columns)),
                    sql.SQL(",").join(sql.Placeholder() for _ in columns),
                ), tuple(columns.values()),
            )
            row = await (await connection.execute(SELECT + " AND a.id=%s", (user_id, account_id, rule_id))).fetchone()
        return document(row)

    async def get(self, user_id, account_id, rule_id):
        async with await AsyncConnection.connect(self.database_url, row_factory=dict_row) as connection:
            row = await (await connection.execute(SELECT + " AND a.id=%s", (user_id, account_id, rule_id))).fetchone()
        return document(row)

    async def list_for_account(self, user_id, account_id, *, status=None, limit=100):
        if not user_id or not account_id or not 1 <= limit <= 1000:
            raise ValueError("User, account and bounded limit are required")
        query, parameters = SELECT, [user_id, account_id]
        if status is not None:
            query += " AND a.status=%s"
            parameters.append(status)
        query += " ORDER BY a.updated_at DESC NULLS LAST, a.id LIMIT %s"
        parameters.append(limit)
        async with await AsyncConnection.connect(self.database_url, row_factory=dict_row) as connection:
            rows = await (await connection.execute(query, parameters)).fetchall()
        return [document(row) for row in rows]

    async def update(self, user_id, account_id, rule_id, values):
        columns, config = split_values(values)
        columns.pop("created_at", None)
        columns["updated_at"] = datetime.now(timezone.utc)
        async with await AsyncConnection.connect(self.database_url, row_factory=dict_row) as connection:
            row = await (await connection.execute(
                SELECT + " AND a.id=%s FOR UPDATE OF a", (user_id, account_id, rule_id),
            )).fetchone()
            if row is None:
                return None
            merged = _json_restore(row["config"] or {})
            merged.update(config)
            columns["config"] = Jsonb(_json_safe(merged))
            await connection.execute(
                sql.SQL("UPDATE mychat.automations SET {} WHERE id=%s AND user_id=%s AND instagram_account_id=%s").format(
                    sql.SQL(",").join(sql.SQL("{}=%s").format(sql.Identifier(key)) for key in columns),
                ), (*columns.values(), rule_id, user_id, account_id),
            )
            row = await (await connection.execute(SELECT + " AND a.id=%s", (user_id, account_id, rule_id))).fetchone()
        return document(row)
