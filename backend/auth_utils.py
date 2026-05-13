import os
import jwt
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from passlib.context import CryptContext
from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

JWT_SECRET = os.environ.get('JWT_SECRET', '')
if not JWT_SECRET:
    raise RuntimeError('JWT_SECRET environment variable is required')
JWT_ALGO = 'HS256'
JWT_TTL_DAYS = 30

pwd_ctx = CryptContext(schemes=['bcrypt'], deprecated='auto')
security = HTTPBearer()


def hash_password(pw: str) -> str:
    return pwd_ctx.hash(pw)


def verify_password(pw: str, hashed: str) -> bool:
    try:
        return pwd_ctx.verify(pw, hashed)
    except Exception:
        return False


def create_token(user_id: str, session_version: int = 0) -> str:
    payload = {
        'sub': user_id,
        'session_version': int(session_version or 0),
        'exp': datetime.now(timezone.utc) + timedelta(days=JWT_TTL_DAYS),
        'iat': datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


def decode_token_payload(token: str) -> Dict[str, Any]:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Token expired')
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid token')


def decode_token(token: str) -> str:
    payload = decode_token_payload(token)
    return payload['sub']


async def get_current_user_id(creds: HTTPAuthorizationCredentials = Depends(security)) -> str:
    return decode_token(creds.credentials)


async def get_current_session_version(
    creds: HTTPAuthorizationCredentials = Depends(security),
) -> int:
    payload = decode_token_payload(creds.credentials)
    return int(payload.get('session_version') or 0)
