import base64
import hashlib
import os
from copy import deepcopy
from typing import Any, Dict, Iterable, Optional

from cryptography.fernet import Fernet, InvalidToken


TOKEN_PREFIX = 'igenc:v1:'
TOKEN_KEY_ENV = 'INSTAGRAM_TOKEN_ENCRYPTION_KEY'
LEGACY_TOKEN_BLOCKED_FLAG = 'instagramTokenMigrationBlocked'
LEGACY_TOKEN_BLOCKED_REASON = 'legacy_plaintext_instagram_token'

USER_TOKEN_FIELDS = ('meta_access_token', 'fb_page_access_token')
INSTAGRAM_ACCOUNT_TOKEN_FIELDS = ('accessToken', 'access_token', 'refreshToken')


class TokenEncryptionConfigError(RuntimeError):
    pass


class TokenDecryptionError(RuntimeError):
    pass


def _raw_key() -> str:
    return (os.environ.get(TOKEN_KEY_ENV) or '').strip()


def token_encryption_configured() -> bool:
    return bool(_raw_key())


def _fernet_key(raw: str) -> bytes:
    candidate = raw.encode('utf-8')
    try:
        decoded = base64.urlsafe_b64decode(candidate)
        if len(decoded) == 32:
            return candidate
    except Exception:
        pass
    return base64.urlsafe_b64encode(hashlib.sha256(candidate).digest())


def _fernet() -> Fernet:
    raw = _raw_key()
    if not raw:
        raise TokenEncryptionConfigError(
            f'{TOKEN_KEY_ENV} is required before storing Instagram tokens'
        )
    return Fernet(_fernet_key(raw))


def is_encrypted_token(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(TOKEN_PREFIX)


def encrypt_token(value: Any) -> Any:
    if value is None or value == '' or is_encrypted_token(value):
        return value
    token = str(value)
    return TOKEN_PREFIX + _fernet().encrypt(token.encode('utf-8')).decode('ascii')


def decrypt_token(value: Any) -> Any:
    if value is None or value == '':
        return value
    if not is_encrypted_token(value):
        return value
    payload = value[len(TOKEN_PREFIX):].encode('ascii')
    try:
        return _fernet().decrypt(payload).decode('utf-8')
    except (InvalidToken, ValueError) as exc:
        raise TokenDecryptionError('invalid_instagram_token_ciphertext') from exc


def redact_token(value: Any) -> str:
    if not value:
        return ''
    text = str(value)
    if len(text) <= 8:
        return '***REDACTED***'
    return f'{text[:4]}...{text[-4:]}'


def token_fields_for_collection(collection_name: str) -> Iterable[str]:
    if collection_name == 'users':
        return USER_TOKEN_FIELDS
    if collection_name == 'instagram_accounts':
        return INSTAGRAM_ACCOUNT_TOKEN_FIELDS
    return ()


def encrypt_token_document(collection_name: str, doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(doc, dict):
        return doc
    protected = deepcopy(doc)
    for field in token_fields_for_collection(collection_name):
        if field in protected and protected.get(field):
            protected[field] = encrypt_token(protected[field])
    return protected


def decrypt_token_document(collection_name: str, doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(doc, dict):
        return doc
    protected = deepcopy(doc)
    fields = tuple(token_fields_for_collection(collection_name))
    for field in fields:
        value = protected.get(field)
        if not value:
            continue
        if is_encrypted_token(value):
            protected[field] = decrypt_token(value)
        elif token_encryption_configured():
            protected[field] = ''
            protected[LEGACY_TOKEN_BLOCKED_FLAG] = True
            protected['instagramTokenMigrationBlockedFields'] = sorted(
                set(protected.get('instagramTokenMigrationBlockedFields') or []) | {field}
            )
            protected['instagramTokenMigrationBlockedReason'] = LEGACY_TOKEN_BLOCKED_REASON
    return protected


def encrypt_token_update(collection_name: str, update: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(update, dict):
        return update
    protected = deepcopy(update)
    fields = set(token_fields_for_collection(collection_name))
    for op in ('$set', '$setOnInsert'):
        values = protected.get(op)
        if not isinstance(values, dict):
            continue
        for field in fields:
            if field in values and values.get(field):
                values[field] = encrypt_token(values[field])
    return protected
