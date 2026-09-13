"""Tenant-scoped PostgreSQL Instagram persistence; not yet selected by server.py."""
from copy import deepcopy
from datetime import datetime, timezone
from uuid import uuid4

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.repositories.postgres_users import _assert_json_has_no_token_keys, _json_restore, _json_safe
from app.security.instagram_tokens import InstagramTokenCipher, protect_document_for_read


FIELDS = {
    "username": "username", "isActive": "is_active",
    "connectionValid": "connection_valid", "refreshStatus": "refresh_status",
    "tokenExpiresAt": "token_expires_at", "createdAt": "created_at",
    "updatedAt": "updated_at",
}
TOKEN_ALIASES = {
    "accessToken": "access_token", "access_token": "access_token",
    "longLivedAccessToken": "access_token", "long_lived_access_token": "access_token",
    "token": "access_token", "refreshToken": "refresh_token", "refresh_token": "refresh_token",
}
IDENTITY_KEYS = {"id", "userId", "user_id", "instagramAccountId", "instagram_account_id", "igUserId"}


class AccountOwnershipError(ValueError):
    pass


class PostgresInstagramAccountRepository:
    def __init__(self, database_url, *, cipher=None):
        if not database_url:
            raise ValueError("DATABASE_URL is required")
        self.database_url = database_url
        self.cipher = cipher or InstagramTokenCipher()

    def _split(self, values):
        values = deepcopy(dict(values))
        if IDENTITY_KEYS.intersection(values):
            raise ValueError("Identity must be supplied through explicit account parameters")
        columns = {}
        for key, column in FIELDS.items():
            if key in values:
                columns[column] = values.pop(key)
        for key, column in TOKEN_ALIASES.items():
            if key in values:
                token = values.pop(key)
                if column in columns:
                    raise ValueError("Multiple aliases supplied for one token")
                columns[column] = self.cipher.encrypt(token) if token else None
        _assert_json_has_no_token_keys(values)
        return columns, values

    def _document(self, row):
        if row is None:
            return None
        result = _json_restore(row.get("source_extra") or {})
        result.update(_json_restore(row.get("provider_metadata") or {}))
        result.update({"id": row["id"], "userId": row["user_id"],
                       "instagramAccountId": row["instagram_account_id"],
                       "igUserId": row["ig_user_id"]})
        for key, column in FIELDS.items():
            if row.get(column) is not None:
                result[key] = row[column]
        result["accessToken"] = row.get("access_token")
        result["refreshToken"] = row.get("refresh_token")
        return protect_document_for_read("instagram_accounts", result, self.cipher)

    async def get(self, user_id, account_id):
        async with await AsyncConnection.connect(self.database_url, row_factory=dict_row) as connection:
            row = await (await connection.execute(
                "SELECT * FROM mychat.instagram_accounts WHERE id=%s AND user_id=%s",
                (account_id, user_id),
            )).fetchone()
        return self._document(row)

    async def list_for_user(self, user_id, limit=100):
        if not user_id or not 1 <= limit <= 1000:
            raise ValueError("A user and a bounded limit are required")
        async with await AsyncConnection.connect(self.database_url, row_factory=dict_row) as connection:
            rows = await (await connection.execute(
                "SELECT * FROM mychat.instagram_accounts WHERE user_id=%s "
                "ORDER BY updated_at DESC NULLS LAST, id LIMIT %s", (user_id, limit),
            )).fetchall()
        return [self._document(row) for row in rows]

    async def save_connection(self, user_id, instagram_id, values):
        if not user_id or not instagram_id:
            raise ValueError("User and Instagram account identifiers are required")
        columns, extra = self._split(values)
        columns["updated_at"] = datetime.now(timezone.utc)
        async with await AsyncConnection.connect(self.database_url, row_factory=dict_row) as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (f"mychat:instagram:{instagram_id}",),
                )
                rows = await (await connection.execute(
                    "SELECT * FROM mychat.instagram_accounts WHERE instagram_account_id=%s FOR UPDATE",
                    (instagram_id,),
                )).fetchall()
                if any(row["user_id"] != user_id for row in rows):
                    raise AccountOwnershipError("Instagram account already belongs to another user")
                row = rows[0] if rows else None
                if row:
                    columns.pop("created_at", None)
                    metadata = _json_restore(row["source_extra"] or {})
                    metadata.update(extra)
                    columns["source_extra"] = Jsonb(_json_safe(metadata))
                    assignments = sql.SQL(", ").join(
                        sql.SQL("{}=%s").format(sql.Identifier(key)) for key in columns
                    )
                    row = await (await connection.execute(
                        sql.SQL("UPDATE mychat.instagram_accounts SET {} WHERE id=%s AND user_id=%s RETURNING *").format(assignments),
                        (*columns.values(), row["id"], user_id),
                    )).fetchone()
                else:
                    columns.update(id=str(uuid4()), user_id=user_id,
                                   instagram_account_id=instagram_id, ig_user_id=instagram_id,
                                   source_extra=Jsonb(_json_safe(extra)))
                    columns.setdefault("created_at", columns["updated_at"])
                    row = await (await connection.execute(
                        sql.SQL("INSERT INTO mychat.instagram_accounts ({}) VALUES ({}) RETURNING *").format(
                            sql.SQL(",").join(map(sql.Identifier, columns)),
                            sql.SQL(",").join(sql.Placeholder() for _ in columns),
                        ), tuple(columns.values()),
                    )).fetchone()
        return self._document(row)

    async def disconnect(self, user_id, account_id):
        async with await AsyncConnection.connect(self.database_url) as connection:
            result = await connection.execute(
                "UPDATE mychat.instagram_accounts SET access_token=NULL, refresh_token=NULL, "
                "connection_valid=false, is_active=false, updated_at=clock_timestamp() "
                "WHERE id=%s AND user_id=%s", (account_id, user_id),
            )
        return result.rowcount == 1
