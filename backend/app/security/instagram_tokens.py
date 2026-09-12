"""Authenticated encryption and Mongo boundary protection for Meta tokens."""
from __future__ import annotations

import logging
import os
import re
from copy import deepcopy
from typing import Any, Mapping, Optional

from cryptography.fernet import Fernet, InvalidToken


TOKEN_KEY_ENV = "INSTAGRAM_TOKEN_ENCRYPTION_KEY"
TOKEN_ENVELOPE_PREFIX = "mychat:ig-token:v1:"
TOKEN_REDACTED = "***REDACTED***"

_COLLECTION_TOKEN_FIELDS = {
    "users": frozenset(
        {
            "meta_access_token",
            "accessToken",
            "access_token",
            "fb_page_access_token",
            "page_access_token",
        }
    ),
    "instagram_accounts": frozenset(
        {
            "accessToken",
            "access_token",
            "longLivedAccessToken",
            "long_lived_access_token",
            "refreshToken",
            "refresh_token",
            "token",
        }
    ),
}
_SENSITIVE_ASSIGNMENT = re.compile(
    r"(?i)((?:access_?token|accesstoken|meta_access_token|refresh_?token|"
    r"refreshToken)\s*[\"']?\s*[:=]\s*[\"']?)([^&,\s\"'}]+)"
)
_AUTHORIZATION_ASSIGNMENT = re.compile(
    r"(?i)(authorization\s*[\"']?\s*[:=]\s*[\"']?)(?:Bearer\s+)?"
    r"([^&,\s\"'}]+)"
)
_BEARER = re.compile(r"(?i)(\bBearer\s+)[A-Za-z0-9._~+/\-=]+")
_ENVELOPE = re.compile(
    re.escape(TOKEN_ENVELOPE_PREFIX) + r"[A-Za-z0-9_\-=]+"
)


class InstagramTokenSecurityError(RuntimeError):
    """Base error whose message is always safe to expose or log."""

    status = "instagram_token_security_error"
    http_status = 500

    def __init__(self) -> None:
        super().__init__(self.status)


class TokenEncryptionNotConfigured(InstagramTokenSecurityError):
    status = "instagram_token_encryption_not_configured"
    http_status = 503


class PlaintextTokenMigrationRequired(InstagramTokenSecurityError):
    status = "instagram_token_migration_required"
    http_status = 409


class EncryptedTokenUnavailable(InstagramTokenSecurityError):
    status = "instagram_token_decryption_failed"
    http_status = 503


def redact_token_text(value: Any) -> Any:
    """Redact token-bearing assignments, bearer credentials, and envelopes."""
    if not isinstance(value, str):
        return value
    redacted = _AUTHORIZATION_ASSIGNMENT.sub(
        r"\1" + TOKEN_REDACTED, value
    )
    redacted = _SENSITIVE_ASSIGNMENT.sub(r"\1" + TOKEN_REDACTED, redacted)
    redacted = _BEARER.sub(r"\1" + TOKEN_REDACTED, redacted)
    return _ENVELOPE.sub(TOKEN_REDACTED, redacted)


def redact_log_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            key: (
                TOKEN_REDACTED
                if str(key).lower()
                in {
                    "access_token",
                    "accesstoken",
                    "meta_access_token",
                    "refresh_token",
                    "refreshtoken",
                    "fb_page_access_token",
                    "page_access_token",
                    "longlivedaccesstoken",
                    "long_lived_access_token",
                    "authorization",
                }
                else redact_log_value(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, tuple):
        return tuple(redact_log_value(item) for item in value)
    if isinstance(value, list):
        return [redact_log_value(item) for item in value]
    return redact_token_text(value)


class TokenRedactionFilter(logging.Filter):
    """Last-line defense for application and HTTP-client log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact_token_text(record.msg)
        if record.args:
            record.args = redact_log_value(record.args)
        if record.exc_info:
            exception_type = record.exc_info[0].__name__
            record.msg = f"{record.msg} exception_type={exception_type}"
            record.exc_info = None
            record.exc_text = None
        if record.stack_info:
            record.stack_info = redact_token_text(record.stack_info)
        return True


def install_token_log_redaction(logger: Optional[logging.Logger] = None) -> None:
    target = logger or logging.getLogger()
    if not any(isinstance(item, TokenRedactionFilter) for item in target.filters):
        target.addFilter(TokenRedactionFilter())
    for handler in target.handlers:
        if not any(
            isinstance(item, TokenRedactionFilter) for item in handler.filters
        ):
            handler.addFilter(TokenRedactionFilter())


class InstagramTokenCipher:
    def __init__(self, key: Optional[str] = None) -> None:
        self._configured_key = key

    def _fernet(self) -> Fernet:
        key = self._configured_key
        if key is None:
            key = (os.environ.get(TOKEN_KEY_ENV) or "").strip()
        if not key:
            raise TokenEncryptionNotConfigured()
        try:
            return Fernet(key.encode())
        except (TypeError, ValueError):
            raise TokenEncryptionNotConfigured() from None

    def encrypt(self, plaintext: Any) -> Any:
        if plaintext is None or plaintext == "":
            return plaintext
        value = str(plaintext)
        if value.startswith(TOKEN_ENVELOPE_PREFIX):
            self.decrypt(value)
            return value
        encrypted = self._fernet().encrypt(value.encode()).decode()
        return TOKEN_ENVELOPE_PREFIX + encrypted

    def ensure_configured(self) -> None:
        self._fernet()

    def decrypt(self, stored_value: Any) -> Any:
        if stored_value is None or stored_value == "":
            return stored_value
        value = str(stored_value)
        if not value.startswith(TOKEN_ENVELOPE_PREFIX):
            raise PlaintextTokenMigrationRequired()
        ciphertext = value[len(TOKEN_ENVELOPE_PREFIX) :]
        try:
            return self._fernet().decrypt(ciphertext.encode()).decode()
        except InvalidToken:
            raise EncryptedTokenUnavailable() from None


def _status_for_error(exc: InstagramTokenSecurityError) -> tuple[str, str]:
    if isinstance(exc, PlaintextTokenMigrationRequired):
        return "migration_needed", exc.status
    if isinstance(exc, TokenEncryptionNotConfigured):
        return "blocked", exc.status
    return "blocked", exc.status


def protect_document_for_read(
    collection_name: str,
    document: Optional[Mapping[str, Any]],
    cipher: InstagramTokenCipher,
) -> Optional[dict]:
    """Decrypt encrypted fields; replace unusable fields with a safe blocked status."""
    if document is None:
        return None
    result = dict(document)
    statuses = []
    for field in _COLLECTION_TOKEN_FIELDS.get(collection_name, ()):
        stored = result.get(field)
        if stored is None or stored == "":
            continue
        try:
            result[field] = cipher.decrypt(stored)
            statuses.append(("encrypted", None))
        except InstagramTokenSecurityError as exc:
            result[field] = ""
            statuses.append(_status_for_error(exc))
    if statuses:
        blocked = next((item for item in statuses if item[0] != "encrypted"), None)
        status, blocker = blocked or ("encrypted", None)
        result["token_security_status"] = status
        result["token_security_blocker"] = blocker
        result["tokenSecurityStatus"] = status
        result["tokenSecurityBlocker"] = blocker
        if blocker:
            result["instagram_token_status"] = status
            result["instagram_token_blocker"] = blocker
            result["instagram_connection_blocker"] = blocker
            result["instagram_connection_valid"] = False
            result["instagramConnectionValid"] = False
            if collection_name == "instagram_accounts":
                result["connectionValid"] = False
    return result


def _encrypt_token_fields(
    collection_name: str, document: Mapping[str, Any], cipher: InstagramTokenCipher
) -> dict:
    result = deepcopy(dict(document))
    token_fields = _COLLECTION_TOKEN_FIELDS.get(collection_name, ())
    for key, value in list(result.items()):
        base_key = str(key).rsplit(".", 1)[-1]
        if base_key in token_fields:
            result[key] = cipher.encrypt(value)
    return result


def protect_update_for_write(
    collection_name: str, update: Any, cipher: InstagramTokenCipher
) -> Any:
    if isinstance(update, list):
        return [
            protect_update_for_write(collection_name, stage, cipher)
            for stage in update
        ]
    if not isinstance(update, Mapping):
        return update
    result = {}
    for key, value in update.items():
        if str(key).startswith("$") and isinstance(value, Mapping):
            result[key] = _encrypt_token_fields(collection_name, value, cipher)
        else:
            result[key] = value
    if not any(str(key).startswith("$") for key in result):
        return _encrypt_token_fields(collection_name, result, cipher)
    return result


class _ProtectedCursor:
    def __init__(
        self, cursor: Any, collection_name: str, cipher: InstagramTokenCipher
    ) -> None:
        self._cursor = cursor
        self._collection_name = collection_name
        self._cipher = cipher

    def __aiter__(self):
        return self

    async def __anext__(self):
        document = await self._cursor.__anext__()
        return protect_document_for_read(
            self._collection_name, document, self._cipher
        )

    async def to_list(self, *args, **kwargs):
        documents = await self._cursor.to_list(*args, **kwargs)
        return [
            protect_document_for_read(self._collection_name, item, self._cipher)
            for item in documents
        ]

    def __getattr__(self, name: str):
        attribute = getattr(self._cursor, name)
        if name in {"sort", "limit", "skip", "batch_size", "hint", "collation"}:
            def chained(*args, **kwargs):
                self._cursor = attribute(*args, **kwargs)
                return self

            return chained
        return attribute


class _ProtectedCollection:
    def __init__(
        self, collection: Any, collection_name: str, cipher: InstagramTokenCipher
    ) -> None:
        self._collection = collection
        self._collection_name = collection_name
        self._cipher = cipher

    async def find_one(self, *args, **kwargs):
        document = await self._collection.find_one(*args, **kwargs)
        return protect_document_for_read(
            self._collection_name, document, self._cipher
        )

    def find(self, *args, **kwargs):
        return _ProtectedCursor(
            self._collection.find(*args, **kwargs),
            self._collection_name,
            self._cipher,
        )

    def aggregate(self, *args, **kwargs):
        return _ProtectedCursor(
            self._collection.aggregate(*args, **kwargs),
            self._collection_name,
            self._cipher,
        )

    async def insert_one(self, document, *args, **kwargs):
        protected = _encrypt_token_fields(
            self._collection_name, document, self._cipher
        )
        return await self._collection.insert_one(protected, *args, **kwargs)

    async def insert_many(self, documents, *args, **kwargs):
        protected = [
            _encrypt_token_fields(self._collection_name, item, self._cipher)
            for item in documents
        ]
        return await self._collection.insert_many(protected, *args, **kwargs)

    async def replace_one(self, query, replacement, *args, **kwargs):
        protected = _encrypt_token_fields(
            self._collection_name, replacement, self._cipher
        )
        return await self._collection.replace_one(
            query, protected, *args, **kwargs
        )

    async def update_one(self, query, update, *args, **kwargs):
        protected = protect_update_for_write(
            self._collection_name, update, self._cipher
        )
        return await self._collection.update_one(
            query, protected, *args, **kwargs
        )

    async def update_many(self, query, update, *args, **kwargs):
        protected = protect_update_for_write(
            self._collection_name, update, self._cipher
        )
        return await self._collection.update_many(
            query, protected, *args, **kwargs
        )

    async def find_one_and_update(self, query, update, *args, **kwargs):
        protected = protect_update_for_write(
            self._collection_name, update, self._cipher
        )
        document = await self._collection.find_one_and_update(
            query, protected, *args, **kwargs
        )
        return protect_document_for_read(
            self._collection_name, document, self._cipher
        )

    def __getattr__(self, name: str):
        return getattr(self._collection, name)


class TokenProtectedDatabase:
    """Delegate all Mongo behavior except token-bearing collection boundaries."""

    def __init__(self, database: Any, cipher: Optional[InstagramTokenCipher] = None):
        self._database = database
        self._cipher = cipher or InstagramTokenCipher()
        self._collections = {
            name: _ProtectedCollection(database[name], name, self._cipher)
            for name in _COLLECTION_TOKEN_FIELDS
        }

    def __getitem__(self, name: str):
        return self._collections.get(name, self._database[name])

    def __getattr__(self, name: str):
        if name in self._collections:
            return self._collections[name]
        return getattr(self._database, name)