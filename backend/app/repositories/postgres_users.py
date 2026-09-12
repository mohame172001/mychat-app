"""Isolated PostgreSQL users repository for Replit development parity tests.

This module does not replace the application's Mongo ``db.users`` collection.
Callers must explicitly construct it, and environment-based construction is
allowed only for an opted-in Replit development process.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Iterable, Mapping, Optional

from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.security.instagram_tokens import (
    InstagramTokenCipher,
    protect_document_for_read,
    protect_update_for_write,
)


POSTGRES_USERS_OPT_IN = "REPLIT_POSTGRES_USERS_ENABLED"
_PRODUCTION_VALUES = {"prod", "production"}
_TOKEN_ALIASES = {
    "accessToken": "meta_access_token",
    "access_token": "meta_access_token",
    "page_access_token": "fb_page_access_token",
}
_COLUMN_FIELDS = {
    "id": "id",
    "normalized_email": "normalized_email",
    "email": "email",
    "username": "username",
    "status": "status",
    "google_sub": "google_sub",
    "password_hash": "password_hash",
    "email_verification_token_hash": "email_verification_token_hash",
    "email_verification_expires_at": "email_verification_expires_at",
    "password_reset_token_hash": "password_reset_token_hash",
    "password_reset_expires_at": "password_reset_expires_at",
    "meta_access_token": "meta_access_token",
    "fb_page_access_token": "fb_page_access_token",
    "created_at": "created_at",
    "updated_at": "updated_at",
}
_PROFILE_FIELDS = {"name", "avatar"}
_JSON_DATE_MARKER = "__mychat_datetime__"
_JSON_FORBIDDEN_TOKEN_KEYS = {
    "token",
    "authorization",
    "accesstoken",
    "metaaccesstoken",
    "fbpageaccesstoken",
    "pageaccesstoken",
    "longlivedaccesstoken",
    "refreshtoken",
}
_FILTER_COLUMNS = {
    "id": "id",
    "email_verification_token_hash": "email_verification_token_hash",
    "password_reset_token_hash": "password_reset_token_hash",
    "google_sub": "google_sub",
}
_INCREMENT_FIELDS = {"session_version"}


class PostgresUserRepositoryError(RuntimeError):
    """Base repository error with no document or credential values."""


class DuplicateUserError(PostgresUserRepositoryError):
    def __init__(self, field: str) -> None:
        self.field = field
        super().__init__(f"duplicate_user_{field}")


class UnsafePostgresUsersEnvironment(PostgresUserRepositoryError):
    pass


@dataclass(frozen=True)
class UserUpdateResult:
    matched_count: int
    modified_count: int


def _normalize_email(value: Any) -> Optional[str]:
    normalized = str(value or "").strip().lower()
    return normalized or None


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return {_JSON_DATE_MARKER: value.isoformat()}
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _json_restore(value: Any) -> Any:
    if isinstance(value, Mapping):
        if set(value) == {_JSON_DATE_MARKER}:
            return datetime.fromisoformat(str(value[_JSON_DATE_MARKER]))
        return {key: _json_restore(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_restore(item) for item in value]
    return value


def _normalized_key(value: Any) -> str:
    return "".join(character for character in str(value).lower() if character.isalnum())


def _assert_json_has_no_token_keys(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = _normalized_key(key)
            if (
                normalized in _JSON_FORBIDDEN_TOKEN_KEYS
                or normalized.endswith("token")
            ):
                raise PostgresUserRepositoryError(
                    "token_field_not_allowed_in_postgres_json"
                )
            _assert_json_has_no_token_keys(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _assert_json_has_no_token_keys(item)


class PostgresUserRepository:
    def __init__(
        self,
        database_url: str,
        *,
        cipher: Optional[InstagramTokenCipher] = None,
    ) -> None:
        if not str(database_url or "").strip():
            raise PostgresUserRepositoryError("postgres_database_url_missing")
        self._database_url = database_url
        self._cipher = cipher or InstagramTokenCipher()

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[AsyncConnection]:
        connection = await AsyncConnection.connect(
            self._database_url,
            row_factory=dict_row,
        )
        try:
            yield connection
        finally:
            await connection.close()

    @staticmethod
    def _canonicalize(document: Mapping[str, Any]) -> dict:
        result = deepcopy(dict(document))
        if "created_at" not in result and "created" in result:
            result["created_at"] = result["created"]
        if "updated_at" not in result and "updated" in result:
            result["updated_at"] = result["updated"]
        for alias, canonical in _TOKEN_ALIASES.items():
            if canonical not in result and alias in result:
                result[canonical] = result[alias]
            result.pop(alias, None)
        if result.get("email") and not result.get("normalized_email"):
            result["normalized_email"] = _normalize_email(result["email"])
        elif result.get("normalized_email"):
            result["normalized_email"] = _normalize_email(
                result["normalized_email"]
            )
        return result

    def _protect_for_storage(self, document: Mapping[str, Any]) -> dict:
        canonical = self._canonicalize(document)
        return protect_update_for_write("users", canonical, self._cipher)

    @staticmethod
    def _split_document(document: Mapping[str, Any]) -> tuple[dict, dict, dict]:
        columns = {
            column: document.get(field)
            for field, column in _COLUMN_FIELDS.items()
        }
        profile = {
            key: document[key]
            for key in _PROFILE_FIELDS
            if key in document
        }
        excluded = (
            set(_COLUMN_FIELDS)
            | set(_PROFILE_FIELDS)
            | set(_TOKEN_ALIASES)
            | {"created", "updated", "profile", "source_extra"}
        )
        source_extra = {
            key: value
            for key, value in document.items()
            if key not in excluded
        }
        _assert_json_has_no_token_keys(profile)
        _assert_json_has_no_token_keys(source_extra)
        return columns, profile, source_extra

    def _row_to_document(self, row: Optional[Mapping[str, Any]]) -> Optional[dict]:
        if row is None:
            return None
        document = dict(_json_restore(row.get("source_extra") or {}))
        document.update(_json_restore(row.get("profile") or {}))
        for field, column in _COLUMN_FIELDS.items():
            value = row.get(column)
            if value is not None:
                document[field] = value
        if document.get("created_at") is not None:
            document["created"] = document["created_at"]
        if document.get("updated_at") is not None:
            document["updated"] = document["updated_at"]
        return protect_document_for_read("users", document, self._cipher)

    @staticmethod
    async def _lock_unique_values(
        connection: AsyncConnection,
        *,
        normalized_email: Optional[str],
        username: Optional[str],
    ) -> None:
        lock_keys = []
        if normalized_email:
            lock_keys.append(f"mychat:users:email:{normalized_email}")
        if username:
            lock_keys.append(f"mychat:users:username:{username.lower()}")
        for key in sorted(lock_keys):
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (key,),
            )

    @staticmethod
    async def _assert_unique(
        connection: AsyncConnection,
        *,
        normalized_email: Optional[str],
        username: Optional[str],
        excluding_id: Optional[str] = None,
    ) -> None:
        if normalized_email:
            row = await (
                await connection.execute(
                    """
                    SELECT id
                    FROM mychat.users
                    WHERE (
                        lower(normalized_email) = %s
                        OR (
                            normalized_email IS NULL
                            AND lower(email) = %s
                        )
                    )
                    AND (%s::text IS NULL OR id <> %s)
                    LIMIT 1
                    """,
                    (
                        normalized_email,
                        normalized_email,
                        excluding_id,
                        excluding_id,
                    ),
                )
            ).fetchone()
            if row:
                raise DuplicateUserError("email")
        if username:
            row = await (
                await connection.execute(
                    """
                    SELECT id
                    FROM mychat.users
                    WHERE lower(username) = lower(%s)
                      AND (%s::text IS NULL OR id <> %s)
                    LIMIT 1
                    """,
                    (username, excluding_id, excluding_id),
                )
            ).fetchone()
            if row:
                raise DuplicateUserError("username")

    @staticmethod
    async def _write_insert(
        connection: AsyncConnection,
        columns: Mapping[str, Any],
        profile: Mapping[str, Any],
        source_extra: Mapping[str, Any],
    ) -> None:
        await connection.execute(
            """
            INSERT INTO mychat.users (
                id, normalized_email, email, username, status, google_sub,
                password_hash, email_verification_token_hash,
                email_verification_expires_at, password_reset_token_hash,
                password_reset_expires_at, meta_access_token,
                fb_page_access_token, created_at, updated_at, profile,
                source_extra
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            """,
            (
                columns["id"],
                columns["normalized_email"],
                columns["email"],
                columns["username"],
                columns["status"],
                columns["google_sub"],
                columns["password_hash"],
                columns["email_verification_token_hash"],
                columns["email_verification_expires_at"],
                columns["password_reset_token_hash"],
                columns["password_reset_expires_at"],
                columns["meta_access_token"],
                columns["fb_page_access_token"],
                columns["created_at"],
                columns["updated_at"],
                Jsonb(_json_safe(profile)),
                Jsonb(_json_safe(source_extra)),
            ),
        )

    async def create(self, document: Mapping[str, Any]) -> dict:
        protected = self._protect_for_storage(document)
        if not protected.get("id"):
            raise PostgresUserRepositoryError("user_id_required")
        columns, profile, source_extra = self._split_document(protected)
        async with self._connection() as connection:
            try:
                async with connection.transaction():
                    await self._lock_unique_values(
                        connection,
                        normalized_email=columns["normalized_email"],
                        username=columns["username"],
                    )
                    await self._assert_unique(
                        connection,
                        normalized_email=columns["normalized_email"],
                        username=columns["username"],
                    )
                    await self._write_insert(
                        connection, columns, profile, source_extra
                    )
            except errors.UniqueViolation as exc:
                constraint = str(exc.diag.constraint_name or "")
                if "google_sub" in constraint:
                    raise DuplicateUserError("google_sub") from None
                if "username" in constraint:
                    raise DuplicateUserError("username") from None
                raise DuplicateUserError("id") from None
        created = await self.get_by_id(str(protected["id"]))
        if created is None:
            raise PostgresUserRepositoryError("user_create_readback_failed")
        return created

    async def get_by_id(self, user_id: str) -> Optional[dict]:
        return await self._get_one("id = %s", (user_id,))

    async def get_by_email(self, email: str) -> Optional[dict]:
        normalized = _normalize_email(email)
        if not normalized:
            return None
        return await self._get_one(
            """
            lower(normalized_email) = %s
            OR (normalized_email IS NULL AND lower(email) = %s)
            """,
            (normalized, normalized),
        )

    async def get_by_username(self, username: str) -> Optional[dict]:
        return await self._get_one(
            "lower(username) = lower(%s)",
            (str(username or "").strip(),),
        )

    async def get_by_google_sub(self, google_sub: str) -> Optional[dict]:
        return await self._get_one("google_sub = %s", (google_sub,))

    async def get_by_email_verification_hash(
        self, token_hash: str
    ) -> Optional[dict]:
        return await self._get_one(
            "email_verification_token_hash = %s",
            (token_hash,),
        )

    async def get_by_password_reset_hash(
        self, token_hash: str
    ) -> Optional[dict]:
        return await self._get_one(
            "password_reset_token_hash = %s",
            (token_hash,),
        )

    async def _get_one(
        self, fixed_predicate: str, parameters: tuple[Any, ...]
    ) -> Optional[dict]:
        async with self._connection() as connection:
            row = await (
                await connection.execute(
                    f"""
                    SELECT id, normalized_email, email, username, status,
                           google_sub, password_hash,
                           email_verification_token_hash,
                           email_verification_expires_at,
                           password_reset_token_hash,
                           password_reset_expires_at, meta_access_token,
                           fb_page_access_token, created_at, updated_at,
                           profile, source_extra
                    FROM mychat.users
                    WHERE {fixed_predicate}
                    LIMIT 1
                    """,
                    parameters,
                )
            ).fetchone()
        return self._row_to_document(row)

    async def update(
        self,
        user_id: str,
        *,
        set_fields: Optional[Mapping[str, Any]] = None,
        unset_fields: Iterable[str] = (),
    ) -> UserUpdateResult:
        return await self.update_one(
            {"id": user_id},
            set_fields=set_fields,
            unset_fields=unset_fields,
        )

    async def update_one(
        self,
        filters: Mapping[str, Any],
        *,
        set_fields: Optional[Mapping[str, Any]] = None,
        unset_fields: Iterable[str] = (),
        increments: Optional[Mapping[str, int]] = None,
    ) -> UserUpdateResult:
        updates = dict(set_fields or {})
        unsets = tuple(str(field) for field in unset_fields)
        increment_values = dict(increments or {})
        if not filters:
            raise PostgresUserRepositoryError("user_update_filter_required")
        unknown_filters = set(filters) - set(_FILTER_COLUMNS)
        if unknown_filters:
            raise PostgresUserRepositoryError("unsupported_user_update_filter")
        if any(not isinstance(value, int) for value in increment_values.values()):
            raise PostgresUserRepositoryError("user_increment_must_be_integer")
        if set(increment_values) - _INCREMENT_FIELDS:
            raise PostgresUserRepositoryError("unsupported_user_increment")
        if not updates and not unsets and not increment_values:
            return UserUpdateResult(matched_count=0, modified_count=0)
        predicates = []
        parameters = []
        for field, value in filters.items():
            predicates.append(f"{_FILTER_COLUMNS[field]} = %s")
            parameters.append(value)
        where_clause = " AND ".join(predicates)
        async with self._connection() as connection:
            async with connection.transaction():
                row = await (
                    await connection.execute(
                        f"""
                        SELECT id, normalized_email, email, username, status,
                               google_sub, password_hash,
                               email_verification_token_hash,
                               email_verification_expires_at,
                               password_reset_token_hash,
                               password_reset_expires_at, meta_access_token,
                               fb_page_access_token, created_at, updated_at,
                               profile, source_extra
                        FROM mychat.users
                        WHERE {where_clause}
                        LIMIT 1
                        FOR UPDATE
                        """,
                        tuple(parameters),
                    )
                ).fetchone()
                if row is None:
                    return UserUpdateResult(0, 0)
                stored_document = dict(_json_restore(row.get("source_extra") or {}))
                stored_document.update(_json_restore(row.get("profile") or {}))
                for field, column in _COLUMN_FIELDS.items():
                    if row.get(column) is not None:
                        stored_document[field] = row[column]
                candidate = self._canonicalize(stored_document)
                candidate.update(updates)
                if "email" in updates and "normalized_email" not in updates:
                    candidate["normalized_email"] = _normalize_email(
                        updates["email"]
                    )
                for field, amount in increment_values.items():
                    current = candidate.get(field, 0)
                    if not isinstance(current, (int, float)):
                        raise PostgresUserRepositoryError(
                            "user_increment_target_not_numeric"
                        )
                    candidate[field] = current + amount
                for field in unsets:
                    candidate.pop(field, None)
                changed = candidate != self._canonicalize(stored_document)
                if not changed:
                    return UserUpdateResult(1, 0)
                protected = self._protect_for_storage(candidate)
                columns, profile, source_extra = self._split_document(protected)
                await self._lock_unique_values(
                    connection,
                    normalized_email=columns["normalized_email"],
                    username=columns["username"],
                )
                await self._assert_unique(
                    connection,
                    normalized_email=columns["normalized_email"],
                    username=columns["username"],
                    excluding_id=str(row["id"]),
                )
                try:
                    await connection.execute(
                        """
                        UPDATE mychat.users SET
                            normalized_email = %s, email = %s, username = %s,
                            status = %s, google_sub = %s, password_hash = %s,
                            email_verification_token_hash = %s,
                            email_verification_expires_at = %s,
                            password_reset_token_hash = %s,
                            password_reset_expires_at = %s,
                            meta_access_token = %s, fb_page_access_token = %s,
                            created_at = %s, updated_at = %s, profile = %s,
                            source_extra = %s
                        WHERE id = %s
                        """,
                        (
                            columns["normalized_email"],
                            columns["email"],
                            columns["username"],
                            columns["status"],
                            columns["google_sub"],
                            columns["password_hash"],
                            columns["email_verification_token_hash"],
                            columns["email_verification_expires_at"],
                            columns["password_reset_token_hash"],
                            columns["password_reset_expires_at"],
                            columns["meta_access_token"],
                            columns["fb_page_access_token"],
                            columns["created_at"],
                            columns["updated_at"],
                            Jsonb(_json_safe(profile)),
                            Jsonb(_json_safe(source_extra)),
                            row["id"],
                        ),
                    )
                except errors.UniqueViolation as exc:
                    constraint = str(exc.diag.constraint_name or "")
                    if "google_sub" in constraint:
                        raise DuplicateUserError("google_sub") from None
                    if "username" in constraint:
                        raise DuplicateUserError("username") from None
                    raise DuplicateUserError("id") from None
        return UserUpdateResult(matched_count=1, modified_count=1)


def development_postgres_users_repository(
    env: Optional[Mapping[str, str]] = None,
) -> Optional[PostgresUserRepository]:
    values = os.environ if env is None else env
    if values.get(POSTGRES_USERS_OPT_IN) != "1":
        return None
    markers = (
        values.get("APP_ENV", ""),
        values.get("ENVIRONMENT", ""),
        values.get("ENV", ""),
        values.get("RAILWAY_ENVIRONMENT", ""),
    )
    if (
        values.get("REPLIT_DEPLOYMENT") == "1"
        or any(marker.lower() in _PRODUCTION_VALUES for marker in markers)
        or not any(marker.lower() == "development" for marker in markers)
        or not (values.get("REPL_ID") or values.get("REPL_SLUG"))
    ):
        raise UnsafePostgresUsersEnvironment(
            "postgres_users_repository_requires_replit_development"
        )
    return PostgresUserRepository(str(values.get("DATABASE_URL") or ""))