import os
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import re
import secrets
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
from urllib.parse import parse_qs, urlencode, urlparse

from fastapi import FastAPI, APIRouter, HTTPException, Depends, Query, Request, Response, BackgroundTasks, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse
from pydantic import BaseModel
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError
import httpx

from models import (
    SignupIn, LoginIn, AuthOut, UserPublic, ProfileUpdateIn,
    NotificationPreferencesIn,
    AutomationIn, AutomationPatch, Automation,
    ContactIn, ContactPatch, Contact,
    BroadcastIn, BroadcastPatch, Broadcast,
    MessageIn, Conversation,
    DmRuleIn, DmRulePatch, DmTestIn,
)
from auth_utils import (
    hash_password,
    verify_password,
    create_token,
    get_current_user_id,
    get_current_session_version,
    decode_token,
    JWT_SECRET,
)

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

MONGO_URL = os.environ['MONGO_URL']
DB_NAME = os.environ.get('DB_NAME', 'mychat')
META_APP_ID = os.environ.get('META_APP_ID', '')
META_APP_SECRET = os.environ.get('META_APP_SECRET', '')
# Instagram API with Business Login uses a SEPARATE App ID/Secret from the
# Facebook App. Resolution priority for the Instagram credential pair:
#   INSTAGRAM_APP_ID > IG_APP_ID > META_APP_ID
#   INSTAGRAM_APP_SECRET > IG_APP_SECRET > META_APP_SECRET
# We track which env name actually supplied each value so the credentials
# diagnostic endpoint can show what was used (without exposing the value).
def _resolve_env(*names):
    for n in names:
        v = os.environ.get(n, '')
        if v:
            return v, n
    return '', None


INSTAGRAM_APP_ID, INSTAGRAM_APP_ID_SOURCE = _resolve_env(
    'INSTAGRAM_APP_ID', 'IG_APP_ID', 'META_APP_ID')
INSTAGRAM_APP_SECRET, INSTAGRAM_APP_SECRET_SOURCE = _resolve_env(
    'INSTAGRAM_APP_SECRET', 'IG_APP_SECRET', 'META_APP_SECRET')

# Backward-compat aliases used by the rest of the codebase.
IG_APP_ID = INSTAGRAM_APP_ID
IG_APP_SECRET = INSTAGRAM_APP_SECRET

# Webhook GET verification token.
META_VERIFY_TOKEN, META_VERIFY_TOKEN_SOURCE = _resolve_env(
    'META_WEBHOOK_VERIFY_TOKEN', 'META_VERIFY_TOKEN')
# Hardcoded fallback is allowed ONLY outside production. Production must
# fail fast if no verify token is set so a public webhook URL can never
# be subscribed by a third party that knows the default value.
if not META_VERIFY_TOKEN:
    _is_prod_for_token = (
        os.environ.get('APP_ENV') or os.environ.get('ENV')
        or os.environ.get('RAILWAY_ENVIRONMENT') or ''
    ).strip().lower() in ('production', 'prod')
    if _is_prod_for_token:
        raise RuntimeError(
            'META_WEBHOOK_VERIFY_TOKEN is required in production. '
            'Set it in the deployment environment before starting the server.'
        )
    META_VERIFY_TOKEN = 'mychat_verify_dev_only'
    META_VERIFY_TOKEN_SOURCE = 'default_dev_only'

# Webhook X-Hub-Signature-256 secret. If META_WEBHOOK_APP_SECRET is set we use
# it; otherwise we fall back to META_APP_SECRET.
META_WEBHOOK_APP_SECRET, META_WEBHOOK_APP_SECRET_SOURCE = _resolve_env(
    'META_WEBHOOK_APP_SECRET', 'META_APP_SECRET')
# When enforce=True, webhooks with bad or missing signatures are rejected with
# 403. Production defaults to enforced verification; local/dev can explicitly
# set META_WEBHOOK_HMAC_ENFORCE=0 when testing unsigned webhook payloads.
META_WEBHOOK_HMAC_ENFORCE = os.environ.get('META_WEBHOOK_HMAC_ENFORCE', '1') != '0'
FRONTEND_URL = os.environ.get('FRONTEND_URL', 'http://localhost:3000')
BACKEND_PUBLIC_URL = os.environ.get('BACKEND_PUBLIC_URL', 'http://localhost:8001')
CRON_SECRET = os.environ.get('CRON_SECRET', '')

# Production hardening config -----------------------------------------------
# IS_PRODUCTION is true when ENV is one of the standard production names.
# Some Railway-style providers set RAILWAY_ENVIRONMENT=production instead.
ENV_NAME = (
    os.environ.get('APP_ENV')
    or os.environ.get('ENV')
    or os.environ.get('RAILWAY_ENVIRONMENT')
    or 'development'
).strip().lower()
IS_PRODUCTION = ENV_NAME in ('production', 'prod')

# Phase 2.18G security: in production we never want to accept an unsigned
# Instagram webhook. The HMAC enforce flag is on by default, but we also
# require the secret to actually be configured so we can compute the
# expected signature. Fail fast so a misconfigured production deploy
# never silently downgrades to warn-only.
if IS_PRODUCTION and not META_WEBHOOK_APP_SECRET:
    raise RuntimeError(
        'META_WEBHOOK_APP_SECRET (or META_APP_SECRET) is required in '
        'production so the Instagram webhook HMAC signature can be '
        'verified. Refusing to start without it.'
    )
if IS_PRODUCTION and not META_WEBHOOK_HMAC_ENFORCE:
    raise RuntimeError(
        'META_WEBHOOK_HMAC_ENFORCE=0 is not permitted in production. '
        'Unsigned Instagram webhooks must never be accepted in prod.'
    )

# Comma-separated list of explicit allowed origins. In production we ALSO
# always include FRONTEND_URL (and any RAILWAY_PUBLIC_DOMAIN values) so the
# main frontend cannot be accidentally locked out by a bad env value.
def _split_origins(raw: str):
    return [o.strip().rstrip('/') for o in (raw or '').split(',') if o.strip()]

CORS_ALLOWED_ORIGINS_ENV = _split_origins(os.environ.get('CORS_ALLOWED_ORIGINS', ''))
RAILWAY_PUBLIC_DOMAINS = _split_origins(os.environ.get('RAILWAY_PUBLIC_DOMAINS', ''))


def _resolved_cors_origins():
    """Decide the final allowed-origins list at startup.

    In production: only the explicit allowlist (env + FRONTEND_URL +
    Railway public domains). No wildcard, no localhost.
    Outside production: the env list plus a localhost dev set, so
    contributors don't need to configure anything.
    """
    explicit = list(CORS_ALLOWED_ORIGINS_ENV)
    if FRONTEND_URL and FRONTEND_URL.rstrip('/') not in explicit:
        explicit.append(FRONTEND_URL.rstrip('/'))
    for d in RAILWAY_PUBLIC_DOMAINS:
        if d not in explicit:
            explicit.append(d)
    if IS_PRODUCTION:
        return [o for o in explicit if o] or [FRONTEND_URL.rstrip('/')]
    dev_default = [
        'http://localhost:3000', 'http://127.0.0.1:3000',
        'http://localhost:5173', 'http://127.0.0.1:5173',
    ]
    return explicit + [o for o in dev_default if o not in explicit]


# Rate-limit thresholds. Env-tunable so ops can dial them in production.
def _rl_int(name, default):
    try:
        return max(1, int(os.environ.get(name, default)))
    except (TypeError, ValueError):
        return default

RATE_LIMIT_LOGIN_PER_MIN = _rl_int('RATE_LIMIT_LOGIN_PER_MIN', 10)
RATE_LIMIT_SIGNUP_PER_HOUR = _rl_int('RATE_LIMIT_SIGNUP_PER_HOUR', 5)
RATE_LIMIT_POLL_NOW_PER_MIN = _rl_int('RATE_LIMIT_POLL_NOW_PER_MIN', 5)
RATE_LIMIT_PROCESS_UNREPLIED_PER_MIN = _rl_int('RATE_LIMIT_PROCESS_UNREPLIED_PER_MIN', 5)
RATE_LIMIT_INSTAGRAM_CONNECT_PER_MIN = _rl_int('RATE_LIMIT_INSTAGRAM_CONNECT_PER_MIN', 5)
RATE_LIMIT_RETRY_REPLY_PER_MIN = _rl_int('RATE_LIMIT_RETRY_REPLY_PER_MIN', 10)
RATE_LIMIT_ADMIN_HEAVY_PER_MIN = _rl_int('RATE_LIMIT_ADMIN_HEAVY_PER_MIN', 10)
RATE_LIMIT_DATA_DELETION_PER_HOUR = _rl_int('RATE_LIMIT_DATA_DELETION_PER_HOUR', 20)

# Admin allowlist for ops-only endpoints (webhook-log etc.). Comma-separated
# emails. In production this MUST be set or those endpoints stay locked.
ADMIN_EMAILS = {e.strip().lower() for e in (os.environ.get('ADMIN_EMAILS') or '').split(',') if e.strip()}

# Phase 1.4H repair tools: admin endpoints under /api/admin/comments/* are
# gated by ENABLE_ADMIN_REPAIR_TOOLS=true. Even when the flag is off, an
# email in ADMIN_EMAILS can still call them. The flag is meant to be
# turned on briefly during a repair window and turned off again.
ENABLE_ADMIN_REPAIR_TOOLS = (os.environ.get('ENABLE_ADMIN_REPAIR_TOOLS') or '').strip().lower() == 'true'

# Phase 2.7 Google Sign-In. Optional — when GOOGLE_CLIENT_ID is unset
# the /api/auth/google endpoint returns 503 with 'google_auth_not_configured'.
# Existing email/password login is unaffected.
GOOGLE_CLIENT_ID = (os.environ.get('GOOGLE_CLIENT_ID') or '').strip() or None
TOKEN_REFRESH_LOOKAHEAD_DAYS = int(os.environ.get('IG_TOKEN_REFRESH_LOOKAHEAD_DAYS', '15'))
TOKEN_REFRESH_MIN_AGE_HOURS = int(os.environ.get('IG_TOKEN_REFRESH_MIN_AGE_HOURS', '24'))
TOKEN_REFRESH_LOCK_MINUTES = int(os.environ.get('IG_TOKEN_REFRESH_LOCK_MINUTES', '5'))
PASSWORD_EMAIL_VERIFICATION_REQUIRED = (
    os.environ.get('PASSWORD_EMAIL_VERIFICATION_REQUIRED', 'true').strip().lower()
    not in ('0', 'false', 'no', 'off')
)
EMAIL_VERIFICATION_TOKEN_TTL_HOURS = int(os.environ.get('EMAIL_VERIFICATION_TOKEN_TTL_HOURS', '24'))
EMAIL_VERIFICATION_WEBHOOK_URL = (os.environ.get('EMAIL_VERIFICATION_WEBHOOK_URL') or '').strip()
EMAIL_VERIFICATION_WEBHOOK_TOKEN = (os.environ.get('EMAIL_VERIFICATION_WEBHOOK_TOKEN') or '').strip()

# Phase 2.14 password reset. Reuses the email-verification webhook
# transport — same env vars, different template name. TTL defaults to 1h.
PASSWORD_RESET_TOKEN_TTL_HOURS = int(os.environ.get('PASSWORD_RESET_TOKEN_TTL_HOURS', '1'))
PASSWORD_RESET_EMAIL_TEMPLATE = (
    os.environ.get('PASSWORD_RESET_EMAIL_TEMPLATE', 'mychat_password_reset').strip()
    or 'mychat_password_reset'
)

client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]

_FASTAPI_KW = {'title': 'mychat API'}
if IS_PRODUCTION:
    # Disable interactive docs and OpenAPI spec in production. Anyone who
    # actually needs them in prod can flip an env flag temporarily.
    if os.environ.get('ENABLE_DOCS_IN_PRODUCTION', '').lower() not in ('1', 'true', 'yes'):
        _FASTAPI_KW.update(docs_url=None, redoc_url=None, openapi_url=None)
app = FastAPI(**_FASTAPI_KW)
api = APIRouter(prefix='/api')

logger = logging.getLogger('mychat')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Silence libraries that log full request URLs at INFO. httpx/httpcore otherwise
# emit lines like "HTTP Request: GET .../comments?access_token=IGAA..." which
# leaks the user's IG long-lived token into Railway log retention.
for _noisy in ('httpx', 'httpcore', 'httpcore.http11', 'httpcore.connection'):
    logging.getLogger(_noisy).setLevel(logging.WARNING)


def _user_session_version(user: Optional[dict]) -> int:
    try:
        return int((user or {}).get('session_version') or 0)
    except (TypeError, ValueError):
        return 0


def _email_verification_required(user: Optional[dict]) -> bool:
    if not PASSWORD_EMAIL_VERIFICATION_REQUIRED:
        return False
    if not user:
        return False
    providers = set(user.get('linked_providers') or [])
    if user.get('auth_provider') == GOOGLE_AUTH_PROVIDER_KEY or GOOGLE_AUTH_PROVIDER_KEY in providers:
        return False
    if user.get('google_sub'):
        return False
    # Legacy password users may not have email verification fields yet.
    # Enforce only when the row is explicitly marked as requiring verification,
    # which new password signups are.
    return bool(user.get('email_verification_required')) and not bool(user.get('email_verified'))


async def _increment_user_session_version(user_id: str, *, reason: str) -> int:
    now = datetime.utcnow()
    await db.users.update_one(
        {'id': user_id},
        {'$inc': {'session_version': 1}, '$set': {
            'session_revoked_at': now,
            'session_revocation_reason': str(reason or 'security_update')[:80],
            'updated_at': now,
        }},
    )
    refreshed = await db.users.find_one({'id': user_id}) or {}
    return _user_session_version(refreshed)


def _hash_email_verification_token(token: str) -> str:
    return hmac.new(
        JWT_SECRET.encode('utf-8'),
        str(token or '').encode('utf-8'),
        hashlib.sha256,
    ).hexdigest()


def _email_verification_delivery_configured() -> bool:
    return bool(EMAIL_VERIFICATION_WEBHOOK_URL)


def _email_verification_url(token: str) -> str:
    return f"{BACKEND_PUBLIC_URL.rstrip('/')}/api/auth/verify-email?token={token}"


async def _deliver_email_verification(user: dict, token: str) -> bool:
    """Deliver verification through a configured webhook without logging the token."""
    if not EMAIL_VERIFICATION_WEBHOOK_URL:
        return False
    payload = {
        'to': _normalize_email_value(user.get('email')),
        'template': 'mychat_email_verification',
        'verification_url': _email_verification_url(token),
    }
    headers = {'content-type': 'application/json'}
    if EMAIL_VERIFICATION_WEBHOOK_TOKEN:
        headers['authorization'] = f'Bearer {EMAIL_VERIFICATION_WEBHOOK_TOKEN}'
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(EMAIL_VERIFICATION_WEBHOOK_URL, json=payload, headers=headers)
        return 200 <= response.status_code < 300
    except Exception as exc:
        logger.warning('email_verification_delivery_failed reason=%s', type(exc).__name__)
        return False


async def _issue_email_verification(user: dict, *, reason: str = 'signup') -> dict:
    token = secrets.token_urlsafe(32)
    now = datetime.utcnow()
    expires_at = now + timedelta(hours=EMAIL_VERIFICATION_TOKEN_TTL_HOURS)
    await db.users.update_one(
        {'id': user.get('id')},
        {'$set': {
            'email_verified': False,
            'email_verification_required': True,
            'email_verification_token_hash': _hash_email_verification_token(token),
            'email_verification_sent_at': now,
            'email_verification_expires_at': expires_at,
            'email_verification_used_at': None,
            'email_verification_reason': str(reason or 'signup')[:40],
            'updated_at': now,
        }},
    )
    sent = await _deliver_email_verification(user, token)
    return {'sent': sent, 'expires_at': expires_at}


async def get_current_active_user_id(
    user_id: str = Depends(get_current_user_id),
    token_session_version: Any = Depends(get_current_session_version),
) -> str:
    """JWT dependency for normal app/admin APIs.

    Auth login already blocks suspended/deleted users, but long-lived JWTs can
    survive a later admin status change. This central guard closes that gap
    without preventing admins from viewing suspended/deleted *target* users.
    """
    user = await db.users.find_one({'id': user_id})
    if not user:
        raise HTTPException(401, 'Invalid token')
    try:
        token_sv = int(token_session_version or 0)
    except (TypeError, ValueError):
        token_sv = 0
    current_sv = _user_session_version(user)
    if token_sv != current_sv:
        raise HTTPException(401, 'session_revoked')
    status_value = str(user.get('status') or 'active').lower()
    if status_value == 'suspended':
        raise HTTPException(403, 'account_suspended')
    if status_value == 'deleted':
        raise HTTPException(403, 'account_deleted')
    if _email_verification_required(user):
        raise HTTPException(403, 'email_verification_required')
    return user_id


def _redact_secrets(value):
    """Recursively redact obvious credential keys from dicts/strings before
    they hit the log stream or HTTP response bodies."""
    SECRET_KEYS = {'access_token', 'accesstoken', 'meta_access_token',
                   'client_secret', 'app_secret', 'refresh_token',
                   'credential', 'google_credential', 'id_token',
                   'google_id_token', 'token', 'authorization'}
    if isinstance(value, dict):
        return {k: ('***REDACTED***' if k.lower() in SECRET_KEYS else _redact_secrets(v))
                for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_secrets(v) for v in value]
    return value


USAGE_EVENT_TYPES = {
    'comment_processed',
    'public_reply_sent',
    'dm_sent',
    'link_clicked',
    'queue_job_processed',
    'retryable_failure',
    'permanent_failure',
    'automation_created',
    'automation_activated',
    'instagram_account_connected',
}

USAGE_COUNTER_BY_EVENT = {
    'comment_processed': 'comments_processed',
    'public_reply_sent': 'public_replies_sent',
    'dm_sent': 'dms_sent',
    'link_clicked': 'links_clicked',
    'queue_job_processed': 'queue_jobs_processed',
    'retryable_failure': 'retryable_failures',
    'permanent_failure': 'permanent_failures',
}

USAGE_COUNTER_FIELDS = (
    'comments_processed',
    'public_replies_sent',
    'dms_sent',
    'links_clicked',
    'queue_jobs_processed',
    'retryable_failures',
    'permanent_failures',
)

_USAGE_UNSAFE_METADATA_KEYS = {
    'access_token', 'accesstoken', 'meta_access_token', 'token', 'authorization',
    'client_secret', 'app_secret', 'secret', 'credential', 'google_credential',
    'id_token', 'google_id_token', 'refresh_token', 'jwt', 'code', 'raw', 'body',
    'payload', 'headers', 'graph_error', 'error_body', 'comment_text',
    'reply_text', 'dm_text', 'message_text', 'text', 'message', 'private_message',
}
_USAGE_UNSAFE_METADATA_FRAGMENTS = (
    'token', 'secret', 'authorization', 'credential', 'id_token',
    'refresh_token', 'raw', 'comment_text', 'reply_text', 'dm_text',
    'message_text', 'graph_error', 'error_body', 'webhook_body',
)


def _is_unsafe_usage_metadata_key(key: Any) -> bool:
    key_s = str(key or '').lower()
    return key_s in _USAGE_UNSAFE_METADATA_KEYS or any(
        fragment in key_s for fragment in _USAGE_UNSAFE_METADATA_FRAGMENTS
    )


def _usage_month(dt: datetime) -> str:
    return dt.strftime('%Y-%m')


def _monthly_usage_user_query(user_id: str, event_month: str) -> dict:
    return {
        'user_id': str(user_id),
        'event_month': event_month,
        '$or': [
            {'limit_subject_type': 'user'},
            {'limit_subject_type': {'$exists': False}},
            {'limit_subject_type': None},
        ],
    }


def _monthly_usage_user_scope_query(event_month: str) -> dict:
    return {
        'event_month': event_month,
        '$or': [
            {'limit_subject_type': 'user'},
            {'limit_subject_type': {'$exists': False}},
            {'limit_subject_type': None},
        ],
    }


def _monthly_usage_instagram_query(instagram_account_id: str, event_month: str) -> dict:
    return {
        'event_month': event_month,
        'limit_subject_type': 'instagram_account',
        'limit_subject_id': str(instagram_account_id),
    }


def _canonical_instagram_account_id(value: Any) -> str:
    return str(value or '').strip()


def _safe_partial_identifier(value: Any) -> str:
    raw = str(value or '').strip()
    if not raw:
        return ''
    if len(raw) <= 6:
        return f"***{raw[-2:]}"
    return f"{raw[:3]}...{raw[-3:]}"


def _sanitize_usage_metadata(metadata: Optional[dict]) -> dict:
    """Keep usage metadata useful without storing tokens or message content."""
    if not isinstance(metadata, dict):
        return {}

    def sanitize(value, depth: int = 0):
        if depth > 2:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, str):
            return value[:160]
        if isinstance(value, (int, float, bool)) or value is None:
            return value
        if isinstance(value, dict):
            cleaned = {}
            for key, child in value.items():
                key_s = str(key)
                if _is_unsafe_usage_metadata_key(key_s):
                    continue
                cleaned[key_s[:80]] = sanitize(child, depth + 1)
            return cleaned
        if isinstance(value, (list, tuple)):
            return [sanitize(item, depth + 1) for item in list(value)[:10]]
        return str(value)[:160]

    cleaned = {}
    for key, value in metadata.items():
        key_s = str(key)
        if _is_unsafe_usage_metadata_key(key_s):
            continue
        cleaned[key_s[:80]] = sanitize(value)
    return cleaned


async def _usage_snapshots_for_user(user_id: str) -> dict:
    snapshots = {}
    try:
        # Phase 2.18N: align the plan-cap snapshot with what the
        # account-switcher actually shows. The previous count of
        # connectionValid=True rows still included rows that
        # _is_public_switchable_instagram_account hides (rows missing
        # an access token, or rows marked auto_cleanup_*, replaced_by_
        # reconnect, force_disconnected_by_admin, disconnected). That
        # left users in the inconsistent state from the screenshot:
        # the switcher shows "No account connected" but the plan
        # guard still says "free allows 1 account; upgrade". Counting
        # only the rows that are actually visible to the user as
        # connected fixes both screens at once.
        snapshots['instagram_accounts_connected_snapshot'] = await db.instagram_accounts.count_documents({
            '$or': [{'userId': user_id}, {'user_id': user_id}],
            'connectionValid': True,
            'isActive': {'$ne': False},
            'accessToken': {'$exists': True, '$nin': [None, '']},
            'refreshStatus': {'$nin': [
                'disconnected',
                'auto_cleanup_users_disconnected',
                'auto_cleanup_single_account_plan',
                'force_disconnected_by_admin',
                'replaced_by_reconnect',
            ]},
        })
    except Exception:
        pass
    try:
        snapshots['active_automations_snapshot'] = await db.automations.count_documents({
            'user_id': user_id,
            'status': 'active',
        })
    except Exception:
        pass
    return snapshots


async def record_usage_event(
    user_id,
    event_type,
    instagram_account_id=None,
    automation_id=None,
    comment_id=None,
    queue_job_id=None,
    metadata=None,
    event_date=None,
):
    """Persist a sanitized usage event and atomically update monthly counters."""
    if event_type not in USAGE_EVENT_TYPES:
        raise ValueError(f'Invalid usage event type: {event_type}')
    if not user_id:
        raise ValueError('user_id is required')

    now = datetime.utcnow()
    event_dt = event_date if isinstance(event_date, datetime) else now
    event_month = _usage_month(event_dt)
    event = {
        '_id': secrets.token_urlsafe(12),
        'id': secrets.token_urlsafe(12),
        'user_id': str(user_id),
        'instagram_account_id': str(instagram_account_id) if instagram_account_id else None,
        'limit_subject_type': 'instagram_account' if instagram_account_id else 'user',
        'limit_subject_id': str(instagram_account_id) if instagram_account_id else str(user_id),
        'automation_id': str(automation_id) if automation_id else None,
        'comment_id': str(comment_id) if comment_id else None,
        'queue_job_id': str(queue_job_id) if queue_job_id else None,
        'event_type': event_type,
        'event_month': event_month,
        'event_date': event_dt,
        'metadata': _sanitize_usage_metadata(metadata),
        'created_at': now,
    }
    await db.usage_events.insert_one(event)

    set_on_insert = {
        '_id': secrets.token_urlsafe(12),
        'id': secrets.token_urlsafe(12),
        'user_id': str(user_id),
        'limit_subject_type': 'user',
        'limit_subject_id': str(user_id),
        'event_month': event_month,
        'created_at': now,
    }
    for field in USAGE_COUNTER_FIELDS:
        set_on_insert[field] = 0
    update = {
        '$setOnInsert': set_on_insert,
        '$set': {
            'updated_at': now,
            'limit_subject_type': 'user',
            'limit_subject_id': str(user_id),
            **await _usage_snapshots_for_user(str(user_id)),
        },
    }
    counter = USAGE_COUNTER_BY_EVENT.get(event_type)
    if counter:
        update['$inc'] = {counter: 1}
    await db.monthly_usage.update_one(
        _monthly_usage_user_query(str(user_id), event_month),
        update,
        upsert=True,
    )
    if instagram_account_id:
        account_set_on_insert = dict(set_on_insert)
        account_set_on_insert.update({
            '_id': secrets.token_urlsafe(12),
            'id': secrets.token_urlsafe(12),
            'limit_subject_type': 'instagram_account',
            'limit_subject_id': str(instagram_account_id),
            'instagram_account_id': str(instagram_account_id),
        })
        account_update = {
            '$setOnInsert': account_set_on_insert,
            '$set': {
                'updated_at': now,
                'user_id': str(user_id),
                'instagram_account_id': str(instagram_account_id),
            },
        }
        if counter:
            account_update['$inc'] = {counter: 1}
        await db.monthly_usage.update_one(
            _monthly_usage_instagram_query(str(instagram_account_id), event_month),
            account_update,
            upsert=True,
        )
    try:
        await invalidate_dashboard_summary(str(user_id), instagram_account_id=instagram_account_id, month=event_month)
    except NameError:
        # During isolated tests/imports the dashboard cache helper may not be
        # bound yet. Usage recording must remain the source of truth.
        pass
    except Exception as e:
        logger.warning(
            'dashboard_summary_invalidation_failed user_id=%s instagramAccountId=%s reason=%s',
            user_id, instagram_account_id, str(e)[:120],
        )
    return event


async def _safe_record_usage_event(*args, **kwargs) -> bool:
    try:
        await record_usage_event(*args, **kwargs)
        return True
    except Exception as e:
        event_type = kwargs.get('event_type') if kwargs else (args[1] if len(args) > 1 else None)
        user_id = kwargs.get('user_id') if kwargs else (args[0] if args else None)
        logger.warning('usage_event_record_failed event_type=%s user_id=%s reason=%s',
                       event_type, user_id, str(e)[:120])
        return False


async def _record_comment_usage_once(comment_doc_id: str, marker_field: str, **usage_kwargs) -> None:
    if not comment_doc_id:
        return
    comment = await db.comments.find_one({'id': comment_doc_id})
    if not comment or comment.get(marker_field):
        return
    recorded = await _safe_record_usage_event(**usage_kwargs)
    if recorded:
        await db.comments.update_one(
            {'id': comment_doc_id},
            {'$set': {marker_field: True, f'{marker_field}_at': datetime.utcnow()}},
        )


# ---- Phase 2.2: plans + limits ----------------------------------------------
# Plan definitions live in backend/plans.py. This block adds DB helpers and
# enforcement helpers. NO BILLING. NO STRIPE. All assignments are manual.
import plans as _plans  # noqa: E402


def _user_plan_assignment_timestamp(row: Optional[dict]) -> Optional[datetime]:
    if not row:
        return None
    raw = (
        row.get('updated_at')
        or row.get('updatedAt')
        or row.get('created_at')
        or row.get('createdAt')
    )
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace('Z', '+00:00'))
        except ValueError:
            return None
    return None


def _select_user_plan_assignment(rows: List[dict]) -> Optional[dict]:
    """Pick the effective manual plan assignment from possibly duplicate rows.

    `user_plans` is indexed unique by user_id now, but old production data can
    still contain duplicates from before that guarantee. Using `find_one` made
    limits depend on arbitrary Mongo row order. The newest assignment wins; if
    legacy rows lack timestamps, the latest returned row wins as a deterministic
    fallback instead of silently over/under-granting by chance.
    """
    candidates = [
        (idx, row)
        for idx, row in enumerate(rows or [])
        if _plans.is_valid_plan_key((row or {}).get('plan_key'))
    ]
    if not candidates:
        return None

    def _sort_key(item):
        idx, row = item
        ts = _user_plan_assignment_timestamp(row)
        return (
            1 if ts else 0,
            ts.timestamp() if ts else float('-inf'),
            idx,
        )

    return max(candidates, key=_sort_key)[1]


async def _get_user_plan_assignment(user_id: str) -> Optional[dict]:
    if not user_id:
        return None
    try:
        rows = await db.user_plans.find({'user_id': str(user_id)}).to_list(50)
    except Exception:
        try:
            row = await db.user_plans.find_one({'user_id': str(user_id)})
        except Exception:
            row = None
        return row if _plans.is_valid_plan_key((row or {}).get('plan_key')) else None

    selected = _select_user_plan_assignment(rows)
    valid_count = sum(
        1
        for row in rows or []
        if _plans.is_valid_plan_key((row or {}).get('plan_key'))
    )
    if selected and valid_count > 1:
        logger.warning(
            'duplicate_user_plan_rows_resolved user_id=%s row_count=%s selected_plan=%s',
            user_id, valid_count, selected.get('plan_key'),
        )
    return selected


async def _effective_user_plan_key_map() -> Dict[str, str]:
    grouped: Dict[str, List[dict]] = {}
    try:
        cursor = db.user_plans.find({})
        async for row in cursor:
            uid = row.get('user_id')
            if uid:
                grouped.setdefault(str(uid), []).append(row)
    except Exception:
        return {}

    effective: Dict[str, str] = {}
    for uid, rows in grouped.items():
        row = _select_user_plan_assignment(rows)
        if row:
            effective[uid] = row.get('plan_key') or _plans.DEFAULT_PLAN_KEY
    return effective


async def _effective_plan_distribution(total_users: Optional[int] = None) -> dict:
    dist = {key: 0 for key in _plans.PLAN_KEYS}
    effective = await _effective_user_plan_key_map()
    for plan_key in effective.values():
        if _plans.is_valid_plan_key(plan_key):
            dist[plan_key] += 1
    if total_users is not None:
        dist[_plans.DEFAULT_PLAN_KEY] += max(0, int(total_users or 0) - len(effective))
    return dist


async def get_user_plan(user_id: str) -> dict:
    """Return the user's plan definition. Missing row -> free plan."""
    if not user_id:
        return _plans.get_plan_limits(_plans.DEFAULT_PLAN_KEY)
    row = await _get_user_plan_assignment(str(user_id))
    plan_key = (row or {}).get('plan_key')
    if not _plans.is_valid_plan_key(plan_key):
        plan_key = _plans.DEFAULT_PLAN_KEY
    plan = _plans.get_plan_limits(plan_key)
    plan['_assignment'] = {
        'assigned_by': (row or {}).get('assigned_by'),
        'assignment_reason': (row or {}).get('assignment_reason'),
        'billing_status': (row or {}).get('billing_status') or 'manual',
        'billing_enabled': False,
        'updated_at': (row or {}).get('updated_at'),
    }
    return plan


async def assign_user_plan(
    user_id: str,
    plan_key: str,
    assigned_by: str,
    reason: Optional[str] = None,
) -> dict:
    """Manually assign a plan to a user. Idempotent upsert.

    Phase 2.18J: also clears the dashboard_summaries read-through
    snapshot for the target user so the next /api/dashboard/summary
    (and /auth/bootstrap) call rebuilds with the new plan limits
    rather than serving cached numbers tied to the old plan.
    """
    if not _plans.is_valid_plan_key(plan_key):
        raise HTTPException(400, f'Invalid plan_key: {plan_key}')
    if not user_id:
        raise HTTPException(400, 'user_id is required')
    now = datetime.utcnow()
    await db.user_plans.update_one(
        {'user_id': str(user_id)},
        {
            '$set': {
                'user_id': str(user_id),
                'plan_key': plan_key,
                'assigned_by': str(assigned_by) if assigned_by else None,
                'assignment_reason': (reason or '')[:200] or None,
                'billing_status': 'manual',
                'billing_enabled': False,
                'updated_at': now,
            },
            '$setOnInsert': {
                'id': secrets.token_urlsafe(12),
                'created_at': now,
            },
        },
        upsert=True,
    )
    # Invalidate every dashboard snapshot belonging to this user so the
    # plan_key + limits in the next response reflect the new plan
    # everywhere (Dashboard, Billing, /api/usage/current, the
    # plan-limit guards on /instagram/auth-url and POST /automations).
    try:
        await invalidate_dashboard_summary(str(user_id))
    except Exception:
        pass
    return await get_user_plan(user_id)


async def _user_monthly_counters(user_id: str, month: Optional[str] = None) -> dict:
    event_month = (month or _usage_month(datetime.utcnow())).strip()
    usage = await db.monthly_usage.find_one(
        _monthly_usage_user_query(str(user_id), event_month)
    ) or {}
    return {field: int(usage.get(field) or 0) for field in USAGE_COUNTER_FIELDS}


async def _instagram_monthly_counters(instagram_account_id: str, month: Optional[str] = None) -> dict:
    event_month = (month or _usage_month(datetime.utcnow())).strip()
    if not instagram_account_id:
        return {field: 0 for field in USAGE_COUNTER_FIELDS}
    usage = await db.monthly_usage.find_one(
        _monthly_usage_instagram_query(str(instagram_account_id), event_month)
    ) or {}
    return {field: int(usage.get(field) or 0) for field in USAGE_COUNTER_FIELDS}


import limit_overrides as _overrides  # Phase 2.8: custom allowances


async def get_active_user_limit_overrides(user_id: str,
                                          now: Optional[datetime] = None) -> List[dict]:
    """Return active override rows for a user. Empty list on errors."""
    if not user_id:
        return []
    try:
        rows = []
        cursor = db.user_limit_overrides.find({
            'user_id': str(user_id),
            'status': _overrides.STATUS_ACTIVE,
        })
        async for r in cursor:
            rows.append(r)
        return _overrides.filter_active_overrides(rows, now)
    except Exception:
        return []


async def compute_effective_limits(user_id: str, *,
                                   now: Optional[datetime] = None) -> dict:
    """Effective limits = base plan limits + active overrides.

    Returns a dict with the same keys as a plan limit dict so callers
    that previously passed the plan dict to limit-check helpers can
    pass this instead.
    """
    plan = await get_user_plan(user_id)
    base = {key: plan.get(key) for key in _overrides.ALL_METRIC_KEYS}
    overrides = await get_active_user_limit_overrides(user_id, now)
    eff = _overrides.compute_effective(base, overrides)
    return eff


async def get_current_usage_with_limits(user_id: str, month: Optional[str] = None) -> dict:
    """Return the user's current-month usage, EFFECTIVE limits, and remaining.

    Phase 2.8: 'limits' / 'remaining' / 'exceeded' / max_* values now
    incorporate user_limit_overrides. The base plan is exposed
    separately via 'base_limits' so the UI can show 'X added by your
    free trial' etc.
    """
    plan = await get_user_plan(user_id)
    counters = await _user_monthly_counters(user_id, month)
    snapshots = await _usage_snapshots_for_user(str(user_id))
    base_limits = {key: plan.get(key) for key in _plans.LIMIT_COUNTER_KEYS}
    overrides = await get_active_user_limit_overrides(user_id)
    effective = _overrides.compute_effective(
        {key: plan.get(key) for key in _overrides.ALL_METRIC_KEYS},
        overrides,
    )
    limits = {key: effective.get(key) for key in _plans.LIMIT_COUNTER_KEYS}
    remaining_map = {}
    exceeded = {}
    for limit_key, counter_field in _plans.LIMIT_TO_COUNTER_FIELD.items():
        used = int(counters.get(counter_field) or 0)
        limit_value = effective.get(limit_key)
        remaining_map[limit_key] = _plans.remaining(limit_value, used)
        exceeded[limit_key] = _plans.is_exceeded(limit_value, used, increment=0) or (
            limit_value is not None and used >= int(limit_value)
        )
    explanation = _overrides.explain_effective(
        {key: plan.get(key) for key in _overrides.ALL_METRIC_KEYS},
        overrides,
    )
    return {
        'plan_key': plan['plan_key'],
        'display_name': plan['display_name'],
        'billing_enabled': False,
        'limits': limits,
        'base_limits': base_limits,
        'counters': counters,
        'remaining': remaining_map,
        'exceeded': exceeded,
        'connectedInstagramAccountsCount': int(snapshots.get('instagram_accounts_connected_snapshot') or 0),
        'activeAutomationsCount': int(snapshots.get('active_automations_snapshot') or 0),
        'max_instagram_accounts': effective.get('max_instagram_accounts'),
        'max_active_automations': effective.get('max_active_automations'),
        'base_max_instagram_accounts': plan.get('max_instagram_accounts'),
        'base_max_active_automations': plan.get('max_active_automations'),
        'active_overrides_count': len(overrides),
        'limits_explanation': explanation,
        'event_month': month or _usage_month(datetime.utcnow()),
    }


ACCOUNT_USAGE_LIMIT_KEYS = {
    'monthly_comments_processed_limit',
    'monthly_public_replies_sent_limit',
    'monthly_dms_sent_limit',
}


RESERVABLE_LIMIT_EVENTS = {
    'monthly_comments_processed_limit': 'comment_processed',
    'monthly_public_replies_sent_limit': 'public_reply_sent',
    'monthly_dms_sent_limit': 'dm_sent',
    'monthly_links_clicked_limit': 'link_clicked',
}

USAGE_RESERVATION_ACTIVE_STATUSES = {'reserved'}
USAGE_RESERVATION_TTL_SECONDS = int(os.environ.get('USAGE_RESERVATION_TTL_SECONDS', '1800'))


def _usage_reservation_key(*parts: Any) -> str:
    basis = '|'.join(str(p or '') for p in parts)
    return hashlib.sha256(basis.encode('utf-8', errors='ignore')).hexdigest()


async def check_plan_limit(user_id: str, limit_key: str, increment: int = 1,
                           instagram_account_id: Optional[str] = None) -> dict:
    """Return {exceeded, remaining, limit, used, plan_key}. Never raises.

    Phase 2.8: limits considered here are EFFECTIVE limits (base plan
    plus active user_limit_overrides), so additive grants and trial
    grants extend the cap before any Meta call is gated.

    Errors fail OPEN so a monitoring blip on monthly_usage / overrides
    never blocks the automation flow.
    """
    try:
        plan = await get_user_plan(user_id)
        counter_field = _plans.LIMIT_TO_COUNTER_FIELD.get(limit_key)
        if not counter_field:
            return {'exceeded': False, 'remaining': None, 'limit': None,
                    'used': 0, 'plan_key': plan['plan_key'], 'fail_open': True}
        canonical_ig_id = _canonical_instagram_account_id(instagram_account_id)
        if canonical_ig_id and limit_key in ACCOUNT_USAGE_LIMIT_KEYS:
            account_counters = await _instagram_monthly_counters(canonical_ig_id)
            # Backward compatibility: before Phase 2.12, some production rows
            # were user-scoped only. Until the safe backfill maps them, enforce
            # the stricter value so reconnects never receive a fresh allowance.
            legacy_user_counters = await _user_monthly_counters(user_id)
            counters = {
                field: max(int(account_counters.get(field) or 0),
                           int(legacy_user_counters.get(field) or 0))
                for field in USAGE_COUNTER_FIELDS
            }
            subject_type = 'instagram_account'
            subject_id = canonical_ig_id
        else:
            counters = await _user_monthly_counters(user_id)
            subject_type = 'user'
            subject_id = str(user_id)
        used = int(counters.get(counter_field) or 0)
        overrides = await get_active_user_limit_overrides(user_id)
        effective = _overrides.compute_effective(
            {key: plan.get(key) for key in _overrides.ALL_METRIC_KEYS},
            overrides,
        )
        limit_value = effective.get(limit_key)
        return {
            'exceeded': _plans.is_exceeded(limit_value, used, increment),
            'remaining': _plans.remaining(limit_value, used),
            'limit': limit_value,
            'used': used,
            'plan_key': plan['plan_key'],
            'fail_open': False,
            'limit_subject_type': subject_type,
            'limit_subject_id': subject_id,
        }
    except Exception as e:
        logger.warning('plan_limit_check_failed user_id=%s limit_key=%s reason=%s',
                       user_id, limit_key, str(e)[:120])
        return {'exceeded': False, 'remaining': None, 'limit': None,
                'used': 0, 'plan_key': _plans.DEFAULT_PLAN_KEY, 'fail_open': True}


async def reserve_usage_limit(
    user_id: str,
    limit_key: str,
    *,
    increment: int = 1,
    instagram_account_id: Optional[str] = None,
    source: str = 'runtime',
    automation_id: Optional[str] = None,
    ig_comment_id: Optional[str] = None,
    action_id: Optional[str] = None,
) -> dict:
    """Atomically reserve capacity for a limited action.

    The reservation bucket is the concurrency gate. monthly_usage remains the
    reporting counter and is incremented only when a reservation is confirmed.
    """
    amount = max(1, int(increment or 1))
    event_type = RESERVABLE_LIMIT_EVENTS.get(limit_key)
    check = await check_plan_limit(
        user_id,
        limit_key,
        increment=amount,
        instagram_account_id=instagram_account_id,
    )
    counter_field = _plans.LIMIT_TO_COUNTER_FIELD.get(limit_key)
    if not event_type or not counter_field:
        return {**check, 'allowed': True, 'reserved': False, 'reservation_required': False}
    if check.get('fail_open'):
        return {**check, 'allowed': True, 'reserved': False, 'reservation_required': False}
    limit_value = check.get('limit')
    if limit_value is None:
        return {
            **check,
            'allowed': True,
            'reserved': False,
            'reservation_required': False,
            'unlimited': True,
            'event_type': event_type,
            'metric': counter_field,
        }
    subject_type = check.get('limit_subject_type') or 'user'
    subject_id = str(check.get('limit_subject_id') or user_id)
    month = _usage_month(datetime.utcnow())
    idempotency_key = _usage_reservation_key(
        subject_type,
        subject_id,
        month,
        counter_field,
        automation_id,
        ig_comment_id,
        action_id or event_type,
    )
    existing = await db.usage_reservations.find_one({'idempotency_key': idempotency_key})
    if existing:
        status = existing.get('status')
        allowed = status in ('reserved', 'confirmed')
        return {
            **check,
            'allowed': allowed,
            'reserved': status == 'reserved',
            'duplicate': True,
            'reservation_required': True,
            'reservation': existing,
            'reservation_id': existing.get('reservation_id'),
            'event_type': existing.get('event_type') or event_type,
            'metric': existing.get('metric') or counter_field,
        }

    now = datetime.utcnow()
    reservation_id = secrets.token_urlsafe(12)
    reservation = {
        '_id': reservation_id,
        'reservation_id': reservation_id,
        'idempotency_key': idempotency_key,
        'user_id': str(user_id),
        'instagram_account_id': str(instagram_account_id) if instagram_account_id else None,
        'limit_subject_type': subject_type,
        'limit_subject_id': subject_id,
        'metric': counter_field,
        'event_type': event_type,
        'amount': amount,
        'month': month,
        'status': 'pending',
        'source': str(source or 'runtime')[:40],
        'automation_id': str(automation_id) if automation_id else None,
        'ig_comment_id': str(ig_comment_id) if ig_comment_id else None,
        'action_id': str(action_id) if action_id else None,
        'created_at': now,
        'updated_at': now,
        'expires_at': now + timedelta(seconds=USAGE_RESERVATION_TTL_SECONDS),
    }
    try:
        await db.usage_reservations.insert_one(reservation)
    except DuplicateKeyError:
        existing = await db.usage_reservations.find_one({'idempotency_key': idempotency_key})
        if existing:
            status = existing.get('status')
            return {
                **check,
                'allowed': status in ('reserved', 'confirmed'),
                'reserved': status == 'reserved',
                'duplicate': True,
                'reservation_required': True,
                'reservation': existing,
                'reservation_id': existing.get('reservation_id'),
                'event_type': existing.get('event_type') or event_type,
                'metric': existing.get('metric') or counter_field,
            }
        raise

    bucket_query = {
        'limit_subject_type': subject_type,
        'limit_subject_id': subject_id,
        'month': month,
        'metric': counter_field,
    }
    await db.usage_reservation_buckets.update_one(
        bucket_query,
        {'$setOnInsert': {
            '_id': secrets.token_urlsafe(12),
            'id': secrets.token_urlsafe(12),
            **bucket_query,
            'user_id': str(user_id),
            'instagram_account_id': str(instagram_account_id) if instagram_account_id else None,
            'confirmed_amount': int(check.get('used') or 0),
            'reserved_amount': 0,
            'created_at': now,
        }},
        upsert=True,
    )
    updated_bucket = await db.usage_reservation_buckets.find_one_and_update(
        {
            **bucket_query,
            '$expr': {
                '$lte': [
                    {
                        '$add': [
                            {'$ifNull': ['$confirmed_amount', 0]},
                            {'$ifNull': ['$reserved_amount', 0]},
                            amount,
                        ],
                    },
                    int(limit_value),
                ],
            },
        },
        {'$inc': {'reserved_amount': amount}, '$set': {'updated_at': now}},
        return_document=ReturnDocument.AFTER,
    )
    if not updated_bucket:
        denied_at = datetime.utcnow()
        await db.usage_reservations.update_one(
            {'reservation_id': reservation_id},
            {'$set': {
                'status': 'failed',
                'failure_reason_sanitized': 'usage_limit_exceeded',
                'updated_at': denied_at,
                'released_at': denied_at,
            }},
        )
        logger.info(
            'reservation_denied_limit user_id=%s metric=%s subject_type=%s subject_id=%s month=%s used=%s limit=%s source=%s',
            user_id, counter_field, subject_type, _safe_partial_identifier(subject_id),
            month, check.get('used'), limit_value, source,
        )
        return {
            **check,
            'allowed': False,
            'reserved': False,
            'reservation_required': True,
            'event_type': event_type,
            'metric': counter_field,
        }

    reserved_at = datetime.utcnow()
    await db.usage_reservations.update_one(
        {'reservation_id': reservation_id},
        {'$set': {'status': 'reserved', 'reserved_at': reserved_at, 'updated_at': reserved_at}},
    )
    reservation.update({'status': 'reserved', 'reserved_at': reserved_at, 'updated_at': reserved_at})
    logger.info(
        'reservation_created user_id=%s metric=%s subject_type=%s subject_id=%s month=%s amount=%s source=%s',
        user_id, counter_field, subject_type, _safe_partial_identifier(subject_id),
        month, amount, source,
    )
    return {
        **check,
        'allowed': True,
        'reserved': True,
        'reservation_required': True,
        'reservation': reservation,
        'reservation_id': reservation_id,
        'event_type': event_type,
        'metric': counter_field,
    }


async def confirm_usage_reservation(
    reservation_result: Optional[dict],
    *,
    user_id: str,
    event_type: Optional[str] = None,
    instagram_account_id: Optional[str] = None,
    automation_id: Optional[str] = None,
    comment_id: Optional[str] = None,
    queue_job_id: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> bool:
    """Confirm a reservation once and write monthly usage exactly once."""
    result = reservation_result or {}
    if not result.get('reservation_required'):
        event = event_type or result.get('event_type')
        if event:
            return await _safe_record_usage_event(
                user_id=user_id,
                event_type=event,
                instagram_account_id=instagram_account_id,
                automation_id=automation_id,
                comment_id=comment_id,
                queue_job_id=queue_job_id,
                metadata={**(metadata or {}), 'usage_reservation': 'bypassed'},
            )
        return False
    reservation = result.get('reservation') or {}
    reservation_id = result.get('reservation_id') or reservation.get('reservation_id')
    if not reservation_id:
        return False
    current = await db.usage_reservations.find_one({'reservation_id': reservation_id}) or reservation
    if current.get('status') == 'confirmed':
        return False
    if current.get('status') != 'reserved':
        return False
    now = datetime.utcnow()
    updated = await db.usage_reservations.find_one_and_update(
        {'reservation_id': reservation_id, 'status': 'reserved'},
        {'$set': {
            'status': 'confirmed',
            'confirmed_at': now,
            'updated_at': now,
        }},
        return_document=ReturnDocument.AFTER,
    )
    if not updated:
        return False
    amount = int(updated.get('amount') or 1)
    bucket_query = {
        'limit_subject_type': updated.get('limit_subject_type'),
        'limit_subject_id': updated.get('limit_subject_id'),
        'month': updated.get('month'),
        'metric': updated.get('metric'),
    }
    await db.usage_reservation_buckets.update_one(
        bucket_query,
        {'$inc': {'reserved_amount': -amount, 'confirmed_amount': amount},
         '$set': {'updated_at': now}},
    )
    ok = await _safe_record_usage_event(
        user_id=user_id,
        event_type=event_type or updated.get('event_type'),
        instagram_account_id=instagram_account_id or updated.get('instagram_account_id'),
        automation_id=automation_id or updated.get('automation_id'),
        comment_id=comment_id or updated.get('action_id'),
        queue_job_id=queue_job_id,
        metadata={
            **(metadata or {}),
            'reservation_id': reservation_id,
            'reservation_status': 'confirmed',
        },
    )
    logger.info(
        'reservation_confirmed user_id=%s metric=%s subject_type=%s subject_id=%s month=%s amount=%s recorded=%s',
        user_id, updated.get('metric'), updated.get('limit_subject_type'),
        _safe_partial_identifier(updated.get('limit_subject_id')), updated.get('month'),
        amount, ok,
    )
    return ok


async def release_usage_reservation(
    reservation_result: Optional[dict],
    *,
    reason: str = 'released_before_send',
) -> bool:
    result = reservation_result or {}
    if not result.get('reservation_required'):
        return False
    reservation = result.get('reservation') or {}
    reservation_id = result.get('reservation_id') or reservation.get('reservation_id')
    if not reservation_id:
        return False
    current = await db.usage_reservations.find_one({'reservation_id': reservation_id}) or reservation
    if current.get('status') != 'reserved':
        return False
    now = datetime.utcnow()
    updated = await db.usage_reservations.find_one_and_update(
        {'reservation_id': reservation_id, 'status': 'reserved'},
        {'$set': {
            'status': 'released',
            'released_at': now,
            'updated_at': now,
            'failure_reason_sanitized': str(reason or 'released')[:80],
        }},
        return_document=ReturnDocument.AFTER,
    )
    if not updated:
        return False
    amount = int(updated.get('amount') or 1)
    await db.usage_reservation_buckets.update_one(
        {
            'limit_subject_type': updated.get('limit_subject_type'),
            'limit_subject_id': updated.get('limit_subject_id'),
            'month': updated.get('month'),
            'metric': updated.get('metric'),
        },
        {'$inc': {'reserved_amount': -amount}, '$set': {'updated_at': now}},
    )
    logger.info(
        'reservation_released user_id=%s metric=%s subject_type=%s subject_id=%s month=%s reason=%s',
        updated.get('user_id'), updated.get('metric'), updated.get('limit_subject_type'),
        _safe_partial_identifier(updated.get('limit_subject_id')), updated.get('month'), reason,
    )
    return True


TRACKED_LINK_TTL_DAYS = int(os.environ.get('TRACKED_LINK_TTL_DAYS', '90'))
_HTTP_URL_RE = re.compile(r'https?://[^\s<>"\']+', re.IGNORECASE)


# ---- In-memory rate limiter --------------------------------------------------
# Simple sliding-window counter keyed by (bucket, key). Sufficient for a
# single-replica deployment; multi-replica deployments should layer Redis
# in front of these endpoints. No external dependency.
import collections as _collections
import threading as _threading

_RATE_LIMIT_LOCK = _threading.Lock()
_RATE_LIMIT_HITS: dict = _collections.defaultdict(_collections.deque)


def _rate_limited(bucket: str, key: str, *, limit: int, window_seconds: int) -> bool:
    """Return True if (bucket,key) has exceeded ``limit`` hits in the
    current sliding window of ``window_seconds`` seconds."""
    if not key:
        return False
    import time as _t
    now = _t.monotonic()
    cutoff = now - window_seconds
    with _RATE_LIMIT_LOCK:
        dq = _RATE_LIMIT_HITS[(bucket, key)]
        while dq and dq[0] < cutoff:
            dq.popleft()
        if len(dq) >= limit:
            return True
        dq.append(now)
        return False


def _client_ip(request) -> str:
    """Best-effort client IP for rate-limit keying. Trusts the leftmost
    X-Forwarded-For entry when present (Railway/CDN setups)."""
    try:
        xff = request.headers.get('x-forwarded-for') or ''
        if xff:
            return xff.split(',')[0].strip() or 'unknown'
        return getattr(request.client, 'host', 'unknown') or 'unknown'
    except Exception:
        return 'unknown'


def _rate_limit_response(retry_after_seconds: int = 60):
    return JSONResponse(
        status_code=429,
        content={'error': 'rate_limited',
                 'retry_after_seconds': retry_after_seconds},
        headers={'Retry-After': str(retry_after_seconds)},
    )


def _hash_text(text: str) -> str:
    """Stable short hash for safe-to-log text fingerprints — never the text itself."""
    import hashlib
    return hashlib.sha256((text or '').encode('utf-8', errors='ignore')).hexdigest()[:12]


def _is_valid_original_url(url: str) -> bool:
    try:
        raw = str(url or '').strip()
        if any(ch in raw for ch in ('\r', '\n', '\t', '\x00')):
            return False
        parsed = urlparse(raw)
        return parsed.scheme in {'http', 'https'} and bool(parsed.netloc)
    except Exception:
        return False


def _reject_unsafe_mongo_keys(value: Any, path: str = 'payload') -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            key_text = str(key)
            if key_text.startswith('$') or '.' in key_text:
                raise HTTPException(400, f'Invalid field in {path}')
            _reject_unsafe_mongo_keys(nested, f'{path}.{key_text}')
    elif isinstance(value, list):
        for idx, nested in enumerate(value):
            _reject_unsafe_mongo_keys(nested, f'{path}[{idx}]')


def _bounded_search_text(value: Optional[str], *, field: str = 'search',
                         max_len: int = 80) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) > max_len:
        raise HTTPException(400, f'{field} is too long')
    return text or None


def _extract_first_url(text: str) -> str:
    match = _HTTP_URL_RE.search(str(text or ''))
    return match.group(0).rstrip(').,؛،') if match else ''


def _tracked_link_url(short_code: str) -> str:
    return f"{BACKEND_PUBLIC_URL.rstrip('/')}/r/{short_code}"


def _hash_tracking_value(value: str) -> str:
    raw = str(value or '').strip()
    if not raw:
        return ''
    return hashlib.sha256(f'{JWT_SECRET}:{raw}'.encode()).hexdigest()


# ---------------- WebSocket manager ----------------
class ConnectionManager:
    def __init__(self):
        self.active: Dict[str, WebSocket] = {}

    async def connect(self, user_id: str, ws: WebSocket):
        await ws.accept()
        self.active[user_id] = ws
        logger.info('WS connected: %s', user_id)

    def disconnect(self, user_id: str):
        self.active.pop(user_id, None)
        logger.info('WS disconnected: %s', user_id)

    async def send(self, user_id: str, data: dict):
        ws = self.active.get(user_id)
        if ws:
            try:
                await ws.send_json(data)
            except Exception:
                self.disconnect(user_id)


ws_manager = ConnectionManager()


# ---------------- Meta messaging helper ----------------
def _safe_provider_error_payload(error: Any) -> dict:
    if isinstance(error, dict):
        payload = error
    else:
        try:
            payload = json.loads(str(error or '{}'))
        except Exception:
            payload = {'message': str(error or '')[:300]}
    if isinstance(payload, dict) and isinstance(payload.get('error'), dict):
        payload = payload['error']
    if not isinstance(payload, dict):
        payload = {'message': str(payload)[:300]}
    return _redact_secrets(payload)


def classify_instagram_send_error(error: Any, status_code: Optional[int] = None) -> dict:
    """Classify Graph send failures without exposing raw provider payloads."""
    payload = _safe_provider_error_payload(error)
    message = str(
        payload.get('message')
        or payload.get('error_user_msg')
        or payload.get('error_user_title')
        or error
        or ''
    ).lower()
    code = payload.get('code')
    subcode = payload.get('error_subcode') or payload.get('subcode')

    reason = 'unknown_graph_error'
    retryable = False
    if status_code in (429,) or code in (4, 17, 32, 613) or 'rate limit' in message or 'too many' in message:
        reason = 'rate_limited'
        retryable = True
    elif status_code and status_code >= 500:
        reason = 'temporary_graph_error'
        retryable = True
    elif any(term in message for term in (
        'cannot receive',
        "can't receive",
        'recipient is unavailable',
        'recipient unavailable',
        'not available',
        'unavailable recipient',
        # Graph variants for "no thread / outside window" — the simple
        # string wrapper _classify_graph_send_error treats these as
        # recipient_unavailable; keep both classifiers consistent.
        'no message thread',
        'outside the messaging window',
        'cannot reply',
    )):
        reason = 'recipient_unavailable'
    elif any(term in message for term in (
        'messaging not allowed',
        'cannot send',
        'not allowed to message',
        'outside allowed window',
        'recipient has not',
        'messaging is disabled',
        'cannot message users',
        'messages must be initiated',
    )):
        reason = 'messaging_not_allowed'
    elif any(term in message for term in ('blocked', 'block messages', 'privacy')):
        reason = 'user_blocked_messages'
    elif code in (10, 190, 200):
        reason = 'permission_error'
    elif status_code in (408, 409, 425, 502, 503, 504) or 'timeout' in message or 'temporar' in message:
        reason = 'temporary_graph_error'
        retryable = True

    if reason in {'recipient_unavailable', 'messaging_not_allowed', 'user_blocked_messages', 'permission_error'}:
        retryable = False
    return {
        'failure_reason': reason,
        'retryable': retryable,
        'provider_code': code,
        'provider_subcode': subcode,
        'status_code': status_code,
        'safe_label': reason,
    }


def _detailed_send_result(ok: bool, status_code: Optional[int] = None,
                          body: Optional[dict] = None, error: Any = None) -> dict:
    if ok:
        return {
            'ok': True,
            'status': 'success',
            'status_code': status_code,
            'body': _redact_secrets(body or {}),
            'failure_reason': None,
            'retryable': False,
        }
    classified = classify_instagram_send_error(error or body or {}, status_code)
    return {
        'ok': False,
        'status': 'failed',
        'status_code': status_code,
        'error': _redact_secrets(_safe_provider_error_payload(error or body or {})),
        **classified,
    }


# Permanent DM/reply failures: do NOT retry on a later poll/webhook.
PERMANENT_GRAPH_FAILURE_REASONS = frozenset({
    'recipient_unavailable',
    'messaging_not_allowed',
    'user_blocked_messages',
    'permission_error',
})

# Transient failures: safe to retry on the next poll or webhook.
TRANSIENT_GRAPH_FAILURE_REASONS = frozenset({
    'rate_limited',
    'temporary_graph_error',
})


def _classify_graph_send_error(status_code, body_text):
    """Classify an Instagram Graph send-error into a stable, low-cardinality
    reason string for action_status decisions and dashboards.

    Never logs or returns the raw body. Inspects only normalized substrings.
    Returns one of the values in PERMANENT/TRANSIENT_GRAPH_FAILURE_REASONS,
    or 'unknown_graph_error' as a safe default.

    NOTE: this is the lightweight string wrapper used by tests and by
    code paths that just need a category. It deliberately implements its
    own pattern matching rather than delegating to classify_instagram_send_error,
    because the test suite locks in this exact pattern set.
    """
    if status_code is None:
        return 'temporary_graph_error'
    if status_code == 200:
        return None
    text = (body_text or '').lower()
    if status_code in (401, 403):
        return 'permission_error'
    if status_code == 429:
        return 'rate_limited'
    if 500 <= status_code < 600:
        return 'temporary_graph_error'
    # Pattern match on known Graph API error phrases. Order matters — the
    # first match wins, so put the most specific phrases first.
    if 'has blocked' in text or 'user has blocked' in text:
        return 'user_blocked_messages'
    if (
        'messaging is disabled' in text
        or 'cannot message users' in text
        or 'messages must be initiated' in text
        or '"code":10' in text
        or 'not authorized' in text
        or 'permission' in text
    ):
        return 'messaging_not_allowed'
    if (
        'no message thread' in text
        or 'outside the messaging window' in text
        or 'cannot reply' in text
        or '"code":551' in text
        or 'recipient' in text
    ):
        return 'recipient_unavailable'
    return 'unknown_graph_error'


from contextvars import ContextVar as _DMContextVar  # used by both send paths below

# Out-of-band channel for the classified Graph error reason of the most
# recent DM send. All send helpers ultimately call send_ig_message which
# writes this contextvar, so every downstream path can read the classified
# reason without changing function signatures.
_LAST_DM_FAILURE: _DMContextVar = _DMContextVar('_LAST_DM_FAILURE', default={})


async def send_ig_message(access_token: str, ig_user_id: str, recipient_ig_id: str,
                          message: dict) -> dict:
    """Send a raw Instagram message object. Tokens are never returned.

    On failure, writes the classified Graph error reason to
    _LAST_DM_FAILURE so any caller (simple DM path or comment-DM flow
    path) can read the reason without changing function signatures.
    """
    if not access_token or not ig_user_id:
        logger.warning('send_ig_message: missing access_token or ig_user_id')
        _LAST_DM_FAILURE.set({'failure_reason': 'missing_access_token_or_ig_user_id',
                              'status_code': None})
        return _detailed_send_result(
            False, None, error={'message': 'missing_access_token_or_ig_user_id'}
        )
    url = f'https://graph.instagram.com/{ig_user_id}/messages'
    payload = {
        'recipient': {'id': recipient_ig_id},
        'message': message,
    }
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(url, json=payload, params={'access_token': access_token})
            if r.status_code == 200:
                try:
                    body = r.json()
                except Exception:
                    body = {}
                # Reset _LAST_DM_FAILURE on success.
                _LAST_DM_FAILURE.set({})
                return _detailed_send_result(True, r.status_code, body=body)
            safe_error = _safe_provider_error_payload(r.text[:500])
            classified = classify_instagram_send_error(safe_error, r.status_code)
            logger.error('send_ig_message_failed status=%s reason=%s retryable=%s',
                         r.status_code, classified['failure_reason'], classified['retryable'])
            _LAST_DM_FAILURE.set({
                'failure_reason': classified.get('failure_reason') or 'unknown_graph_error',
                'status_code': r.status_code,
            })
            return _detailed_send_result(False, r.status_code, error=safe_error)
    except Exception as e:
        logger.exception('send_ig_message exception: %s', e)
        _LAST_DM_FAILURE.set({'failure_reason': 'temporary_graph_error', 'status_code': None})
        return _detailed_send_result(False, None, error={'message': str(e)[:300]})


async def send_ig_dm_detailed(access_token: str, ig_user_id: str,
                              recipient_ig_id: str, text: str) -> dict:
    """Send a text DM and return a safe detailed result."""
    return await send_ig_message(access_token, ig_user_id, recipient_ig_id, {'text': text})


async def send_ig_dm(access_token: str, ig_user_id: str, recipient_ig_id: str, text: str) -> bool:
    """Send a text DM via Instagram Graph API. Returns True on success."""
    result = await send_ig_dm_detailed(access_token, ig_user_id, recipient_ig_id, text)
    return bool(result.get('ok'))


_ORIGINAL_SEND_IG_DM_DETAILED = send_ig_dm_detailed
_ORIGINAL_SEND_IG_DM = send_ig_dm


async def _call_send_ig_dm_detailed(access_token: str, ig_user_id: str,
                                    recipient_ig_id: str, text: str) -> dict:
    """Call detailed DM helper, while preserving old tests that patch send_ig_dm.

    When tests monkey-patch the bool wrapper send_ig_dm and set
    _LAST_DM_FAILURE manually to a classified reason, we must surface
    THAT reason in the returned dict — not overwrite it with a generic
    'unknown_graph_error'.
    """
    if send_ig_dm_detailed is not _ORIGINAL_SEND_IG_DM_DETAILED:
        return await send_ig_dm_detailed(access_token, ig_user_id, recipient_ig_id, text)
    if send_ig_dm is not _ORIGINAL_SEND_IG_DM:
        # Reset contextvar so we read THIS call's failure, not a stale one.
        _LAST_DM_FAILURE.set({})
        ok = await send_ig_dm(access_token, ig_user_id, recipient_ig_id, text)
        if ok:
            return _detailed_send_result(True, 200, body={})
        # Read the classified reason the patched send_ig_dm wrote (if any).
        last = _LAST_DM_FAILURE.get() or {}
        reason = last.get('failure_reason') or 'unknown_graph_error'
        status_code = last.get('status_code')
        retryable = reason in TRANSIENT_GRAPH_FAILURE_REASONS
        return {
            'ok': False,
            'status': 'failed',
            'status_code': status_code,
            'failure_reason': reason,
            'retryable': retryable,
            'provider_code': None,
            'provider_subcode': None,
            'safe_label': reason,
            'error': {'message': 'patched_send_ig_dm_failed'},
        }
    return await send_ig_dm_detailed(access_token, ig_user_id, recipient_ig_id, text)


def _send_failure_fields(prefix: str, result: dict) -> dict:
    """Return safe failure fields for a comment/DM send result."""
    if result.get('ok'):
        return {
            f'{prefix}_failure_reason': None,
            f'{prefix}_failure_retryable': False,
            f'{prefix}_provider_code': None,
            f'{prefix}_provider_subcode': None,
        }
    return {
        f'{prefix}_failure_reason': result.get('failure_reason') or 'unknown_graph_error',
        f'{prefix}_failure_retryable': bool(result.get('retryable')),
        f'{prefix}_provider_code': result.get('provider_code'),
        f'{prefix}_provider_subcode': result.get('provider_subcode'),
    }


def _status_is_success(value: Any) -> bool:
    return str(value or '').lower() in ('success', 'sent', 'replied')


def _status_is_disabled(value: Any) -> bool:
    return str(value or '').lower() in ('disabled', 'skipped', 'not_applicable', 'na')


_FAILED_LIKE_STATUSES = ('failed', 'failed_retryable', 'failed_permanent', 'failed_retry_exhausted')


def _status_is_failed_like(value: Any) -> bool:
    return str(value or '').lower() in _FAILED_LIKE_STATUSES


def _status_is_plan_limited(value: Any) -> bool:
    return str(value or '').lower() == 'plan_limited'


def _compute_comment_action_status(reply_status: Any, dm_status: Any) -> str:
    """Compute the overall action status from independent reply/DM statuses.

    failed_retryable / failed_permanent are treated like failed for the
    success/partial_success math, so a 'failed_retryable' reply alongside
    a 'success' DM correctly reports 'partial_success'.

    plan_limited is a third category (Phase 2.2): the step was deliberately
    not attempted because the user's plan limit is exceeded. plan_limited
    + success on the other step -> partial_success. plan_limited alone (or
    with disabled) -> 'plan_limited'.
    """
    statuses = [
        str(reply_status or 'disabled').lower(),
        str(dm_status or 'disabled').lower(),
    ]
    enabled_statuses = [s for s in statuses if not _status_is_disabled(s)]
    if not enabled_statuses:
        return 'skipped'
    has_success = any(_status_is_success(s) for s in enabled_statuses)
    has_failed = any(_status_is_failed_like(s) for s in enabled_statuses)
    has_plan_limited = any(_status_is_plan_limited(s) for s in enabled_statuses)
    if has_success and (has_failed or has_plan_limited):
        return 'partial_success'
    if has_failed:
        return 'failed'
    if has_plan_limited:
        return 'plan_limited'
    if all(_status_is_success(s) for s in enabled_statuses):
        return 'success'
    return 'failed'


def _has_retryable_step_failure(doc: dict) -> bool:
    return bool(doc.get('reply_failure_retryable') or doc.get('dm_failure_retryable'))


def _failure_category_from_doc(doc: dict) -> Optional[str]:
    return (
        doc.get('reply_failure_reason')
        or doc.get('dm_failure_reason')
        or doc.get('skip_reason')
        or doc.get('action_status')
    )


def _next_retry_time(attempts: int) -> datetime:
    exponent = max(0, min(int(attempts or 0), 5))
    delay = AUTOMATION_TEMP_RETRY_BASE_SECONDS * (2 ** exponent)
    return datetime.utcnow() + timedelta(seconds=delay)


async def _respect_account_send_spacing(user_id: str, instagram_account_id: str,
                                        send_kind: str) -> Optional[str]:
    """Apply small per-account send spacing without exposing or storing content."""
    spacing = COMMENT_REPLY_MIN_SPACING_SECONDS if send_kind == 'comment_reply' else DM_SEND_MIN_SPACING_SECONDS
    if spacing <= 0:
        return None
    collection = getattr(db, 'automation_rate_limits', None)
    if collection is None:
        return None
    now = datetime.utcnow()
    key = f'{user_id}:{instagram_account_id}:{send_kind}'
    try:
        current = await collection.find_one({'id': key})
        last_sent = _parse_graph_datetime((current or {}).get('last_sent_at')) if current else None
        if last_sent:
            elapsed = (now - last_sent).total_seconds()
            if elapsed < spacing:
                return f'{send_kind}_spacing'
        await collection.update_one(
            {'id': key},
            {'$set': {
                'id': key,
                'user_id': user_id,
                'instagramAccountId': instagram_account_id,
                'send_kind': send_kind,
                'last_sent_at': now,
                'updated': now,
            }},
            upsert=True,
        )
    except Exception:
        logger.exception('automation_rate_limit_check_failed kind=%s user_id=%s account=%s',
                         send_kind, user_id, instagram_account_id)
    return None


def _automation_has_node_type(automation: dict, node_type: str) -> bool:
    if node_type == 'reply_comment' and _automation_public_reply_texts(automation):
        return True
    return any((node or {}).get('type') == node_type for node in automation.get('nodes') or [])


def _automation_public_reply_texts(automation: dict) -> List[str]:
    replies: List[str] = []
    for node in automation.get('nodes') or []:
        if (node or {}).get('type') != 'reply_comment':
            continue
        data = node.get('data') or {}
        node_replies = data.get('replies')
        if isinstance(node_replies, list):
            replies.extend(str(item or '').strip() for item in node_replies)
        replies.append(str(data.get('text') or data.get('message') or '').strip())
    replies = [reply for reply in replies if reply]
    if replies:
        return replies
    if automation.get('reply_under_post') is False:
        return []
    top_level = [
        automation.get('comment_reply'),
        automation.get('comment_reply_2'),
        automation.get('comment_reply_3'),
    ]
    return [str(reply or '').strip() for reply in top_level if str(reply or '').strip()]


def _automation_public_reply_required(automation: dict) -> bool:
    """True if the rule has any public reply text in any persisted shape.

    This is the canonical pre-flight signal: if it returns True, the
    flow run MUST attempt a public reply. A reply_status='disabled'
    outcome is invalid for such a rule. Used by execute_flow,
    _run_and_record_action, dedup recovery, and the repair script.
    """
    if not isinstance(automation, dict):
        return False
    if automation.get('reply_under_post') is False:
        return False
    return bool(_automation_public_reply_texts(automation))


def _automation_public_reply_source(automation: dict) -> str:
    for node in automation.get('nodes') or []:
        if (node or {}).get('type') != 'reply_comment':
            continue
        data = node.get('data') or {}
        node_replies = data.get('replies')
        if isinstance(node_replies, list) and any(str(item or '').strip() for item in node_replies):
            return 'graph_node'
        if str(data.get('text') or data.get('message') or '').strip():
            return 'graph_node'
    for key in ('comment_reply', 'comment_reply_2', 'comment_reply_3'):
        if str(automation.get(key) or '').strip():
            return key
    return 'none'


def _automation_dm_text_for_diagnostics(automation: dict) -> str:
    for node in automation.get('nodes') or []:
        if (node or {}).get('type') == 'message':
            data = node.get('data') or {}
            return str(
                data.get('text')
                or data.get('message')
                or data.get('opening_dm_text')
                or data.get('link_dm_text')
                or automation.get('dm_text')
                or ''
            ).strip()
    return str(
        automation.get('opening_dm_text')
        or automation.get('link_dm_text')
        or automation.get('dm_text')
        or ''
    ).strip()


def _safe_text_hash(text: str) -> str:
    value = str(text or '')
    if not value:
        return ''
    return hashlib.sha256(value.encode('utf-8', errors='ignore')).hexdigest()[:12]


def _normalize_public_reply_for_persistence(update: dict, existing: Optional[dict] = None) -> dict:
    """Keep top-level reply fields and reply_comment graph nodes in sync.

    The UI and older rules may store public replies either in top-level
    comment_reply fields or in a graph node. Normalizing before persistence
    prevents specific-post rules from becoming DM-only when one shape is absent.
    """
    normalized = dict(update or {})
    existing = existing or {}

    explicit_disable = normalized.get('reply_under_post') is False
    update_has_reply_keys = any(
        key in normalized for key in ('comment_reply', 'comment_reply_2', 'comment_reply_3')
    )
    update_reply_values = [
        str(normalized.get('comment_reply') or '').strip(),
        str(normalized.get('comment_reply_2') or '').strip(),
        str(normalized.get('comment_reply_3') or '').strip(),
    ]
    update_reply_values = [value for value in update_reply_values if value]
    update_nodes_replies = _automation_public_reply_texts({'nodes': normalized.get('nodes') or []})
    existing_replies = _automation_public_reply_texts(existing)

    # If an older graph-only rule is edited for DM settings, preserve its
    # public reply unless the payload includes a non-empty replacement.
    if explicit_disable and not update_reply_values and not update_nodes_replies and existing_replies:
        dm_related_update = any(key in normalized for key in (
            'dm_text', 'opening_dm_enabled', 'opening_dm_text',
            'opening_dm_button_text', 'link_dm_text', 'link_button_text',
            'link_url', 'follow_request_enabled', 'follow_request_message',
            'follow_request_button_text', 'follow_confirmation_keywords',
            'follow_gate_fallback_message', 'verify_actual_follow',
            'follow_not_detected_message', 'follow_verification_failed_message',
            'follow_retry_button_text', 'follow_cooldown_message',
            'max_follow_verification_attempts', 'email_request_enabled',
            'follow_up_enabled', 'follow_up_text', 'nodes', 'edges',
        ))
        if dm_related_update:
            normalized['reply_under_post'] = True
            update_reply_values = existing_replies

    replies = update_reply_values or update_nodes_replies
    if not replies and not explicit_disable:
        replies = existing_replies if existing and not update_has_reply_keys else []
    if replies:
        normalized['reply_under_post'] = True
        normalized['comment_reply'] = replies[0]
        normalized['comment_reply_2'] = replies[1] if len(replies) > 1 else ''
        normalized['comment_reply_3'] = replies[2] if len(replies) > 2 else ''
        normalized = _ensure_public_reply_node(normalized)
    elif explicit_disable:
        normalized['comment_reply'] = ''
        normalized['comment_reply_2'] = ''
        normalized['comment_reply_3'] = ''
        nodes = [
            node for node in (normalized.get('nodes') or [])
            if (node or {}).get('type') != 'reply_comment'
        ]
        if 'nodes' in normalized:
            normalized['nodes'] = nodes
            normalized['edges'] = [
                edge for edge in (normalized.get('edges') or [])
                if (edge or {}).get('target') not in {'n_reply'}
            ]
    return normalized


def _ensure_public_reply_node(automation: dict) -> dict:
    replies = _automation_public_reply_texts(automation)
    if not replies:
        return automation
    nodes = [dict(node or {}) for node in automation.get('nodes') or []]
    if not nodes:
        return automation
    trigger = next((node for node in nodes if node.get('type') == 'trigger'), None)
    if not trigger:
        return automation
    reply_nodes = [node for node in nodes if node.get('type') == 'reply_comment']
    if reply_nodes:
        changed = False
        for node in reply_nodes:
            data = dict(node.get('data') or {})
            existing = data.get('replies')
            existing_replies = [str(item or '').strip() for item in existing] if isinstance(existing, list) else []
            if not any(existing_replies) and not str(data.get('text') or data.get('message') or '').strip():
                data['text'] = replies[0]
                data['replies'] = replies
                node['data'] = data
                changed = True
        if not changed:
            return automation
        updated = dict(automation)
        updated['nodes'] = nodes
        return updated
    reply_node_id = 'n_reply'
    existing_ids = {str(node.get('id')) for node in nodes}
    if reply_node_id in existing_ids:
        suffix = 1
        while f'n_reply_synth_{suffix}' in existing_ids:
            suffix += 1
        reply_node_id = f'n_reply_synth_{suffix}'
    nodes.append({
        'id': reply_node_id,
        'type': 'reply_comment',
        'data': {'text': replies[0], 'replies': replies, 'source': 'top_level_comment_reply'},
    })
    edges = [dict(edge or {}) for edge in automation.get('edges') or []]
    edges.append({
        'id': f'e_public_reply_synth_{reply_node_id}',
        'source': trigger.get('id'),
        'target': reply_node_id,
    })
    updated = dict(automation)
    updated['nodes'] = nodes
    updated['edges'] = edges
    return updated


def _dm_failure_retryable_from_doc(doc: dict) -> bool:
    if 'dm_failure_retryable' in doc:
        return bool(doc.get('dm_failure_retryable'))
    reason = doc.get('dm_failure_reason')
    return reason in ('rate_limited', 'temporary_graph_error')


def _reply_provider_proof_exists(doc: Optional[dict]) -> bool:
    doc = doc or {}
    return bool(doc.get('reply_provider_response_ok') is True)


def _reply_provider_comment_id_exists(doc: Optional[dict]) -> bool:
    doc = doc or {}
    return bool(doc.get('reply_provider_comment_id') or doc.get('reply_id'))


def _reply_marked_success_without_provider_proof(doc: Optional[dict]) -> bool:
    doc = doc or {}
    reply_status = doc.get('reply_status') or doc.get('replyStatus')
    return bool(
        (doc.get('replied') or _status_is_success(reply_status))
        and not _reply_provider_proof_exists(doc)
    )


def _reply_result_has_provider_proof(result: Optional[dict]) -> bool:
    result = result or {}
    return bool(result.get('ok') and result.get('provider_response_ok') is True)


def _normalize_reply_result_for_provider_proof(result: Optional[dict]) -> dict:
    result = dict(result or {})
    if _reply_result_has_provider_proof(result):
        return result
    if result.get('ok'):
        # The public comment reply path must only be treated as success after
        # Meta confirms the /replies request. Older wrappers and tests returned
        # ok=True without provider proof; keep that retryable instead of
        # creating another false success.
        result['ok'] = False
        result.setdefault('failure_reason', 'missing_provider_confirmation')
        result.setdefault('retryable', True)
        result.setdefault('safe_label', 'Missing public reply provider confirmation')
    return result


def _env_int_clamped(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except Exception:
        value = default
    return max(minimum, min(value, maximum))


def _quick_reply_title(title: str, fallback: str = 'Send me the link') -> str:
    """Instagram quick reply labels are short; keep custom labels usable."""
    value = (title or fallback or '').strip() or fallback
    return value[:20]


DEFAULT_FOLLOW_GATE_MESSAGE = (
    'فرحان إنك مهتم 😊\n'
    'تابع الحساب الأول، وبعدها اضغط على الزر عشان أبعتلك الرابط.'
)
DEFAULT_FOLLOW_GATE_BUTTON_TEXT = 'تمت المتابعة'
DEFAULT_FOLLOW_GATE_CONFIRMATION_KEYWORDS = [
    'following',
    'i followed',
    'تمت المتابعة',
    'تابعت',
]
# When verify_actual_follow is on, the bot calls Meta's User Profile API to
# read is_user_follow_business after every confirmation. The defaults below
# are the messages the bot sends when verification reports the user is NOT
# actually following, when permission/consent makes verification impossible,
# and the maximum number of times we will re-check before pausing.
DEFAULT_FOLLOW_NOT_DETECTED_MESSAGE = (
    'لسه مش ظاهر عندي إنك تابعت الحساب 😊\n'
    'تابع الحساب الأول وبعدها اضغط الزر تاني وهبعتلك الرابط فورًا.'
)
DEFAULT_FOLLOW_VERIFICATION_FAILED_MESSAGE = (
    'مش قادر أتأكد من المتابعة دلوقتي. جرّب تتابع الحساب واضغط الزر مرة تانية.'
)
DEFAULT_FOLLOW_RETRY_BUTTON_TEXT = DEFAULT_FOLLOW_GATE_BUTTON_TEXT
DEFAULT_MAX_FOLLOW_VERIFICATION_ATTEMPTS = 3


def _follow_verification_cooldown_seconds() -> int:
    try:
        configured = int(os.getenv('FOLLOW_VERIFICATION_COOLDOWN_SECONDS', '2'))
    except (TypeError, ValueError):
        configured = 2
    return max(2, min(configured, 30))


# Short concurrency-dedup window. Two webhook events triggered by the same
# tap arrive milliseconds apart; only the first should call Meta. Anything
# longer than this would block legitimate retries (a user who follows
# between two taps must succeed on the second tap).
FOLLOW_VERIFICATION_COOLDOWN_SECONDS = _follow_verification_cooldown_seconds()
# Background verifier wakes up at this cadence and re-checks pending
# sessions whose user hasn't been verified yet — covers the case where
# Meta updates is_user_follow_business a few seconds AFTER the user follows.
FOLLOW_BACKGROUND_VERIFIER_INTERVAL_SECONDS = 25
FOLLOW_BACKGROUND_VERIFIER_MAX_AGE_MINUTES = 30
# Sent once per cooldown window when the user taps again before the
# cooldown elapses. Rate-limited by lastCooldownNoticeAt on the session.
DEFAULT_FOLLOW_COOLDOWN_MESSAGE = (
    'بحاول أتأكد من المتابعة 😊 جرّب تضغط الزر مرة تانية خلال ثواني.'
)


def _split_follow_keywords(value) -> list:
    if isinstance(value, list):
        raw_items = value
    else:
        raw_items = str(value or '').split(',')
    seen = set()
    out = []
    for item in raw_items:
        keyword = str(item or '').strip()
        key = keyword.casefold()
        if keyword and key not in seen:
            seen.add(key)
            out.append(keyword)
    return out


def _normalize_follow_gate_config(data: dict) -> dict:
    enabled = bool(data.get('follow_request_enabled', data.get('followGateEnabled', False)))
    message = (
        data.get('follow_request_message')
        or data.get('follow_gate_message')
        or data.get('followGateMessage')
        or DEFAULT_FOLLOW_GATE_MESSAGE
    )
    button = (
        data.get('follow_request_button_text')
        or data.get('follow_gate_button_text')
        or data.get('followGateButtonText')
        or DEFAULT_FOLLOW_GATE_BUTTON_TEXT
    )
    keywords = _split_follow_keywords(
        data.get('follow_confirmation_keywords')
        if 'follow_confirmation_keywords' in data
        else data.get('followGateConfirmationKeywords')
    )
    if not keywords:
        keywords = DEFAULT_FOLLOW_GATE_CONFIRMATION_KEYWORDS.copy()
    expires_raw = data.get('follow_gate_expires_after_minutes', data.get('followGateExpiresAfterMinutes', 1440))
    try:
        expires_minutes = int(expires_raw)
    except (TypeError, ValueError):
        expires_minutes = 1440
    expires_minutes = max(5, min(expires_minutes, 10080))
    fallback = (
        data.get('follow_gate_fallback_message')
        or data.get('followGateFallbackMessage')
        or ''
    )
    # Verify-actual-follow defaults to True for any new rule, but we honor
    # explicit False so admins can opt out and keep the legacy click-only gate.
    raw_verify = data.get('verify_actual_follow')
    if raw_verify is None:
        raw_verify = data.get('verifyActualFollow')
    verify_actual_follow = True if raw_verify is None else bool(raw_verify)
    not_detected = (
        data.get('follow_not_detected_message')
        or data.get('followNotDetectedMessage')
        or DEFAULT_FOLLOW_NOT_DETECTED_MESSAGE
    )
    verification_failed = (
        data.get('follow_verification_failed_message')
        or data.get('followVerificationFailedMessage')
        or DEFAULT_FOLLOW_VERIFICATION_FAILED_MESSAGE
    )
    retry_button = (
        data.get('follow_retry_button_text')
        or data.get('followRetryButtonText')
        or str(button or '').strip()
        or DEFAULT_FOLLOW_RETRY_BUTTON_TEXT
    )
    max_attempts_raw = (
        data.get('max_follow_verification_attempts')
        if 'max_follow_verification_attempts' in data
        else data.get('maxFollowVerificationAttempts',
                      DEFAULT_MAX_FOLLOW_VERIFICATION_ATTEMPTS)
    )
    try:
        max_attempts = int(max_attempts_raw)
    except (TypeError, ValueError):
        max_attempts = DEFAULT_MAX_FOLLOW_VERIFICATION_ATTEMPTS
    max_attempts = max(1, min(max_attempts, 10))
    cooldown_message = (
        data.get('follow_cooldown_message')
        or data.get('followCooldownMessage')
        or DEFAULT_FOLLOW_COOLDOWN_MESSAGE
    )
    return {
        'follow_request_enabled': enabled,
        'follow_request_message': str(message or '').strip(),
        'follow_request_button_text': str(button or '').strip(),
        'follow_confirmation_keywords': keywords,
        'follow_gate_expires_after_minutes': expires_minutes,
        'follow_gate_fallback_message': str(fallback or '').strip(),
        'verify_actual_follow': verify_actual_follow,
        'follow_not_detected_message': str(not_detected or '').strip(),
        'follow_verification_failed_message': str(verification_failed or '').strip(),
        'follow_retry_button_text': str(retry_button or '').strip(),
        'follow_cooldown_message': str(cooldown_message or '').strip(),
        'max_follow_verification_attempts': max_attempts,
    }


async def send_ig_quick_reply(access_token: str, ig_user_id: str, recipient_ig_id: str,
                              text: str, title: str, payload: str) -> dict:
    return await send_ig_message(
        access_token,
        ig_user_id,
        recipient_ig_id,
        {
            'text': text,
            'quick_replies': [{
                'content_type': 'text',
                'title': _quick_reply_title(title),
                'payload': payload[:1000],
            }],
        },
    )


async def send_ig_url_button(access_token: str, ig_user_id: str, recipient_ig_id: str,
                             text: str, button_title: str, url: str) -> dict:
    return await send_ig_message(
        access_token,
        ig_user_id,
        recipient_ig_id,
        {
            'attachment': {
                'type': 'template',
                'payload': {
                    'template_type': 'button',
                    'text': text,
                    'buttons': [{
                        'type': 'web_url',
                        'url': url,
                        'title': (button_title or 'Open link').strip()[:20],
                    }],
                },
            },
        },
    )


async def get_instagram_messaging_user_profile(access_token: str, ig_scoped_id: str) -> dict:
    """Fetch the messaging user's profile, including follow relationship."""
    if not access_token or not ig_scoped_id:
        return {'ok': False, 'status_code': None, 'error': 'missing_access_token_or_igsid'}
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f'https://graph.instagram.com/{ig_scoped_id}',
                params={
                    'fields': (
                        'name,username,profile_pic,follower_count,'
                        'is_user_follow_business,is_business_follow_user'
                    ),
                    'access_token': access_token,
                },
            )
            try:
                body = r.json()
            except Exception:
                body = {'raw': r.text[:500]}
            if r.status_code == 200:
                return {'ok': True, 'status_code': r.status_code, 'profile': _redact_secrets(body)}
            return {'ok': False, 'status_code': r.status_code, 'error': _redact_secrets(body)}
    except Exception as e:
        logger.exception('instagram_user_profile_fetch_exception: %s', e)
        return {'ok': False, 'status_code': None, 'error': str(e)[:500]}


# Meta Graph error codes that indicate the app or page lacks the permissions
# required to read is_user_follow_business. Anything else is treated as a
# transient API error so we don't permanently mark the session as failed
# for a one-off network blip.
_PERMISSION_ERROR_CODES = {10, 100, 200, 4, 17, 190}


def _classify_profile_error(error: Any) -> str:
    """Map a Graph error body to a stable reason code."""
    if not isinstance(error, dict):
        return 'temporary_api_error'
    inner = error.get('error') if isinstance(error.get('error'), dict) else error
    code = inner.get('code') if isinstance(inner, dict) else None
    subcode = inner.get('error_subcode') if isinstance(inner, dict) else None
    msg = (inner.get('message') if isinstance(inner, dict) else '') or ''
    msg_l = str(msg).lower()
    if code in _PERMISSION_ERROR_CODES:
        return 'permission_or_consent_required'
    if subcode in {2018028, 2018032}:
        return 'permission_or_consent_required'
    if 'permission' in msg_l or 'not authorized' in msg_l or 'consent' in msg_l:
        return 'permission_or_consent_required'
    return 'temporary_api_error'


async def verify_instagram_user_follows_business(access_token: str,
                                                 ig_scoped_id: str) -> dict:
    """High-level wrapper around the User Profile API.

    Always returns a structured dict — never raises into webhook processing.
        { ok: True,  follows: bool,  raw_status: int|None, profile_excerpt }
        { ok: False, reason: 'permission_or_consent_required'|'temporary_api_error'|
                              'missing_token_or_id',
          raw_status: int|None }
    """
    if not access_token or not ig_scoped_id:
        return {'ok': False, 'reason': 'missing_token_or_id', 'raw_status': None}
    import time as _time
    _start = _time.monotonic()
    raw = await get_instagram_messaging_user_profile(access_token, ig_scoped_id)
    logger.info('graph_api_call_duration_ms call=user_profile ms=%s status=%s',
                int((_time.monotonic() - _start) * 1000), raw.get('status_code'))
    if raw.get('ok'):
        profile = raw.get('profile') or {}
        return {
            'ok': True,
            'follows': bool(profile.get('is_user_follow_business')),
            'raw_status': raw.get('status_code'),
            'profile_excerpt': {
                'username': profile.get('username'),
                'is_user_follow_business': profile.get('is_user_follow_business'),
                'is_business_follow_user': profile.get('is_business_follow_user'),
            },
        }
    reason = _classify_profile_error(raw.get('error'))
    return {
        'ok': False,
        'reason': reason,
        'raw_status': raw.get('status_code'),
    }


def _comment_dm_flow_enabled(automation: dict) -> bool:
    if (automation.get('mode') or '') != 'reply_and_dm':
        return False
    return any([
        automation.get('opening_dm_text'),
        automation.get('opening_dm_button_text'),
        automation.get('link_dm_text'),
        automation.get('link_url'),
        automation.get('follow_request_enabled'),
        automation.get('email_request_enabled'),
        automation.get('follow_up_enabled') and automation.get('follow_up_text'),
    ])


def _normalize_comment_text(value: Any) -> str:
    if value is None:
        return ''
    return str(value).strip()


def _automation_keywords(rule: dict) -> list:
    raw = rule.get('keywords')
    if isinstance(raw, list):
        parts = raw
    else:
        parts = str(rule.get('keyword') or '').split(',')
    return [str(item or '').strip() for item in parts if str(item or '').strip()]


def _keyword_in_text(keyword: str, text: str) -> bool:
    keyword_norm = str(keyword or '').strip().casefold()
    text_norm = str(text or '').strip().casefold()
    return bool(keyword_norm and text_norm and keyword_norm in text_norm)


def matchesAutomationRule(rule: dict, comment_text: Any, context: Optional[dict] = None) -> dict:
    """Unicode-safe comment matcher used by webhook and polling paths.

    Reply-all rules match any non-empty trimmed text, including Arabic,
    emoji-only, punctuation-only, and one-word comments. Keyword rules remain
    keyword-specific and use case-insensitive contains matching.
    """
    text = _normalize_comment_text(comment_text)
    if not text:
        return {'matches': False, 'mode': 'empty', 'reason': 'skipped_empty_comment'}

    raw_trigger = str(rule.get('trigger') or '')
    trigger = raw_trigger.casefold()
    match_mode = str(
        rule.get('match') or rule.get('matchMode') or rule.get('match_mode') or 'any'
    ).strip().casefold()

    if trigger.startswith('keyword:'):
        trigger_keyword = raw_trigger.split(':', 1)[1].strip()
        return {
            'matches': _keyword_in_text(trigger_keyword, text),
            'mode': 'keyword',
            'keywords': [trigger_keyword] if trigger_keyword else [],
            'reason': None if _keyword_in_text(trigger_keyword, text) else 'keyword_no_match',
        }

    if match_mode in ('any', 'all', 'any_comment', 'all_comments', 'reply_all', 'any word'):
        return {'matches': True, 'mode': 'any', 'reason': None}

    keywords = _automation_keywords(rule)
    if match_mode in ('keyword', 'keywords', 'specific', 'specific_words', 'specific word or words'):
        matched = any(_keyword_in_text(kw, text) for kw in keywords)
        return {
            'matches': matched,
            'mode': 'keyword',
            'keywords': keywords,
            'reason': None if matched else 'keyword_no_match',
        }

    # Conservative fallback: unknown modes behave like existing "any" rules.
    return {'matches': True, 'mode': match_mode or 'any', 'reason': None}


def _conversion_tracking_enabled(source: dict, url: str = '') -> bool:
    if 'conversionTrackingEnabled' in source:
        return bool(source.get('conversionTrackingEnabled'))
    if 'conversion_tracking_enabled' in source:
        return bool(source.get('conversion_tracking_enabled'))
    return bool((url or source.get('link_url') or source.get('finalLinkUrl') or '').strip())


async def _create_tracked_link(user_doc: dict, session: dict, original_url: str) -> Optional[dict]:
    original_url = (original_url or '').strip()
    if not _is_valid_original_url(original_url):
        logger.warning('tracked_link_invalid_original_url user_id=%s session=%s',
                       user_doc.get('id'), session.get('id'))
        return None

    now = datetime.utcnow()
    ctx = {
        **_current_instagram_context(user_doc),
        'instagramAccountDbId': (
            session.get('instagramAccountDbId')
            or session.get('instagram_account_id')
            or user_doc.get('active_instagram_account_id')
            or ''
        ),
        'instagram_account_id': (
            session.get('instagram_account_id')
            or session.get('instagramAccountDbId')
            or user_doc.get('active_instagram_account_id')
            or ''
        ),
        'instagramAccountId': (
            session.get('instagramAccountId')
            or session.get('igUserId')
            or user_doc.get('ig_user_id')
            or ''
        ),
        'igUserId': (
            session.get('igUserId')
            or session.get('instagramAccountId')
            or user_doc.get('ig_user_id')
            or ''
        ),
    }
    base_doc = {
        'id': '',
        'shortCode': '',
        'user_id': user_doc.get('id'),
        'userId': user_doc.get('id'),
        **ctx,
        'ruleId': session.get('automation_id') or session.get('ruleId'),
        'automation_id': session.get('automation_id') or session.get('ruleId'),
        'instagramUserId': session.get('recipient_id') or session.get('instagramUserId'),
        'recipient_id': session.get('recipient_id') or session.get('instagramUserId'),
        'originalUrl': original_url,
        'relatedCommentId': session.get('ig_comment_id') or session.get('relatedCommentId'),
        'comment_doc_id': session.get('comment_doc_id'),
        'relatedMessageId': None,
        'clicksCount': 0,
        'firstClickedAt': None,
        'lastClickedAt': None,
        'createdAt': now,
        'created': now,
        'updatedAt': now,
        'updated': now,
        'expiresAt': now + timedelta(days=TRACKED_LINK_TTL_DAYS),
        'isActive': False,
        'status': 'created',
    }
    for _ in range(5):
        short_code = secrets.token_urlsafe(6).replace('_', '').replace('-', '')[:10]
        doc = {**base_doc, 'id': short_code, 'shortCode': short_code}
        try:
            await db.tracked_links.insert_one(doc)
            return doc
        except Exception:
            continue
    logger.warning('tracked_link_create_failed user_id=%s session=%s',
                   user_doc.get('id'), session.get('id'))
    return None


async def _activate_tracked_link(tracked_link: Optional[dict], related_message_id: Optional[str] = None):
    if not tracked_link:
        return
    update = {
        'isActive': True,
        'status': 'sent',
        'updatedAt': datetime.utcnow(),
        'updated': datetime.utcnow(),
    }
    if related_message_id:
        update['relatedMessageId'] = related_message_id
    await db.tracked_links.update_one(
        {'shortCode': tracked_link.get('shortCode')},
        {'$set': update},
    )


async def _mark_tracked_link_unused(tracked_link: Optional[dict], status: str = 'send_failed'):
    if not tracked_link:
        return
    await db.tracked_links.update_one(
        {'shortCode': tracked_link.get('shortCode')},
        {'$set': {
            'isActive': False,
            'status': status,
            'updatedAt': datetime.utcnow(),
            'updated': datetime.utcnow(),
        }},
    )


async def _send_text_dm_with_optional_tracking(user_doc: dict, session: dict, text: str) -> bool:
    text_to_send = text
    tracked_link = None
    original_url = _extract_first_url(text)
    if original_url and _conversion_tracking_enabled(session, original_url):
        tracked_link = await _create_tracked_link(user_doc, session, original_url)
        if tracked_link:
            text_to_send = text.replace(original_url, _tracked_link_url(tracked_link['shortCode']))
    ok = await send_ig_dm(
        user_doc.get('meta_access_token', ''),
        user_doc.get('ig_user_id', ''),
        session.get('recipient_id'),
        text_to_send,
    )
    if ok:
        await _activate_tracked_link(tracked_link)
    else:
        await _mark_tracked_link_unused(tracked_link)
    return ok


async def _create_comment_dm_session(user_doc: dict, automation: dict, recipient_ig_id: str,
                                     comment_context: Optional[dict], payload: str) -> dict:
    import uuid as _uuid
    now = datetime.utcnow()
    follow_gate = _normalize_follow_gate_config(automation)
    session = {
        'id': payload.split(':')[1] if ':' in payload else str(_uuid.uuid4()),
        'user_id': user_doc['id'],
        **_current_instagram_context(user_doc),
        'ig_user_id': user_doc.get('ig_user_id') or '',
        'recipient_id': recipient_ig_id,
        'automation_id': automation.get('id'),
        'automation_name': automation.get('name'),
        'comment_doc_id': (comment_context or {}).get('comment_doc_id'),
        'ig_comment_id': (comment_context or {}).get('ig_comment_id'),
        'payload': payload,
        'status': 'pending',
        'stage': 'awaiting_user_action',
        'link_dm_text': (automation.get('link_dm_text') or '').strip(),
        'link_button_text': (automation.get('link_button_text') or '').strip(),
        'link_url': (automation.get('link_url') or '').strip(),
        'conversionTrackingEnabled': _conversion_tracking_enabled(
            automation, (automation.get('link_url') or '').strip()
        ),
        **follow_gate,
        'follow_confirmed': False,
        'follow_confirmation_attempts': 0,
        'follow_verified': False,
        'follow_verification_attempts': 0,
        'followLastCheckedAt': None,
        'followReminderCount': 0,
        'lastFollowVerificationError': None,
        'finalDmSentAt': None,
        'expiresAt': now + timedelta(minutes=follow_gate['follow_gate_expires_after_minutes']),
        'email_request_enabled': bool(automation.get('email_request_enabled')),
        'follow_up_enabled': bool(automation.get('follow_up_enabled')),
        'follow_up_text': (automation.get('follow_up_text') or '').strip(),
        'created': now,
        'updated': now,
    }
    await db.comment_dm_sessions.insert_one(session)
    return session


def _comment_dm_follow_confirmation_matches(session: dict, text: str = '',
                                            payload: Optional[str] = None) -> bool:
    if not session.get('follow_request_enabled'):
        return True
    if session.get('follow_confirmed'):
        return True
    payload_value = str(payload or '')
    follow_payload = str(session.get('follow_payload') or '')
    if follow_payload and payload_value == follow_payload:
        return True
    if payload_value.endswith(':followed'):
        return True
    normalized_text = str(text or '').strip().casefold()
    if not normalized_text:
        return False
    candidates = [
        session.get('follow_request_button_text'),
        *list(session.get('follow_confirmation_keywords') or []),
    ]
    return any(normalized_text == str(item or '').strip().casefold() for item in candidates if item)


async def _mark_comment_dm_follow_confirmed(session: dict) -> None:
    if not session.get('id') or session.get('follow_confirmed'):
        return
    now = datetime.utcnow()
    await db.comment_dm_sessions.update_one(
        {'id': session['id'], 'status': 'pending'},
        {'$set': {
            'follow_confirmed': True,
            'stage': 'follow_confirmed',
            'confirmedAt': now,
            'updated': now,
        }},
    )
    session['follow_confirmed'] = True
    session['stage'] = 'follow_confirmed'
    session['confirmedAt'] = now


async def _send_comment_dm_follow_gate_prompt(user_doc: dict, session: dict) -> bool:
    access_token = user_doc.get('meta_access_token', '')
    ig_user_id = user_doc.get('ig_user_id', '')
    recipient_id = session.get('recipient_id')
    if not recipient_id:
        return False
    now = datetime.utcnow()
    expires_at = session.get('expiresAt')
    if expires_at and isinstance(expires_at, datetime) and expires_at <= now:
        fallback = (session.get('follow_gate_fallback_message') or '').strip()
        sent_fallback = False
        if fallback:
            sent_fallback = await send_ig_dm(access_token, ig_user_id, recipient_id, fallback)
        await db.comment_dm_sessions.update_one(
            {'id': session.get('id')},
            {'$set': {'status': 'expired', 'stage': 'expired', 'updated': now}},
        )
        logger.info('follow_gate_expired session=%s fallback_sent=%s',
                    session.get('id'), sent_fallback)
        return sent_fallback

    if session.get('stage') == 'awaiting_follow_confirmation' and session.get('followPromptSentAt'):
        logger.info('duplicate_skipped follow_gate_prompt session=%s', session.get('id'))
        return True

    prompt = (session.get('follow_request_message') or DEFAULT_FOLLOW_GATE_MESSAGE).strip()
    button = (session.get('follow_request_button_text') or DEFAULT_FOLLOW_GATE_BUTTON_TEXT).strip()
    payload = session.get('follow_payload') or f'comment_flow:{session.get("id")}:followed'
    result = await send_ig_quick_reply(
        access_token,
        ig_user_id,
        recipient_id,
        prompt,
        button,
        payload,
    )
    prompt_sent = bool(result.get('ok'))
    if not prompt_sent:
        prompt_sent = await send_ig_dm(access_token, ig_user_id, recipient_id, prompt)
    await db.comment_dm_sessions.update_one(
        {'id': session.get('id')},
        {'$set': {
            'status': 'pending',
            'stage': 'awaiting_follow_confirmation',
            'follow_payload': payload,
            'followPromptSentAt': now,
            'updated': now,
        }},
    )
    logger.info('follow_gate_started session=%s recipient=%s prompt_sent=%s',
                session.get('id'), recipient_id, prompt_sent)
    return prompt_sent


async def _send_comment_dm_flow_entry(user_doc: dict, automation: dict, recipient_ig_id: str,
                                      comment_context: Optional[dict] = None) -> bool:
    """Send the first DM only. The link step waits for the recipient response."""
    import uuid as _uuid
    access_token = user_doc.get('meta_access_token', '')
    ig_user_id = user_doc.get('ig_user_id', '')
    opening_text = (automation.get('opening_dm_text') or automation.get('dm_text') or '').strip()
    button_text = (automation.get('opening_dm_button_text') or 'Send me the link').strip()
    has_deferred_step = any([
        automation.get('link_dm_text'),
        automation.get('link_url'),
        automation.get('follow_request_enabled'),
        automation.get('email_request_enabled'),
        automation.get('follow_up_enabled') and automation.get('follow_up_text'),
    ])

    if opening_text and has_deferred_step:
        payload = f'comment_flow:{str(_uuid.uuid4())}:continue'
        await _create_comment_dm_session(user_doc, automation, recipient_ig_id, comment_context, payload)
        result = await send_ig_quick_reply(
            access_token, ig_user_id, recipient_ig_id,
            opening_text, button_text, payload,
        )
        if result.get('ok'):
            logger.info('comment_dm_opening_quick_reply_sent rule_id=%s recipient=%s',
                        automation.get('id'), recipient_ig_id)
            return True
        logger.warning('comment_dm_quick_reply_failed rule_id=%s err=%s; falling back to text',
                       automation.get('id'), result.get('error'))
        # Keep the pending session active. If quick replies are not accepted by
        # Meta for this account, the user can still type any response to continue.
        return await send_ig_dm(access_token, ig_user_id, recipient_ig_id, opening_text)

    if opening_text:
        return await _send_text_dm_with_optional_tracking(
            user_doc,
            {
                'user_id': user_doc['id'],
                **_current_instagram_context(user_doc),
                'recipient_id': recipient_ig_id,
                'automation_id': automation.get('id'),
                'ig_comment_id': (comment_context or {}).get('ig_comment_id'),
                'comment_doc_id': (comment_context or {}).get('comment_doc_id'),
                'conversionTrackingEnabled': _conversion_tracking_enabled(
                    automation, _extract_first_url(opening_text)
                ),
            },
            opening_text,
        )

    if has_deferred_step:
        payload = f'comment_flow:{str(_uuid.uuid4())}:continue'
        session = await _create_comment_dm_session(
            user_doc, automation, recipient_ig_id, comment_context, payload
        )
        return await _send_comment_dm_flow_completion(user_doc, session)

    return await _send_comment_dm_flow_completion(
        user_doc,
        {
            'user_id': user_doc['id'],
            'ig_user_id': ig_user_id,
            'recipient_id': recipient_ig_id,
            'automation_id': automation.get('id'),
            'link_dm_text': (automation.get('link_dm_text') or '').strip(),
            'link_button_text': (automation.get('link_button_text') or '').strip(),
            'link_url': (automation.get('link_url') or '').strip(),
            'conversionTrackingEnabled': _conversion_tracking_enabled(
                automation, (automation.get('link_url') or '').strip()
            ),
            'follow_request_enabled': bool(automation.get('follow_request_enabled')),
            'follow_verified': False,
            'email_request_enabled': bool(automation.get('email_request_enabled')),
            'follow_up_enabled': bool(automation.get('follow_up_enabled')),
            'follow_up_text': (automation.get('follow_up_text') or '').strip(),
        },
    )


async def _send_follow_reminder(user_doc: dict, session: dict, message: str,
                                stage: str) -> bool:
    """Send the not-detected / verification-failed message with a retry button."""
    access_token = user_doc.get('meta_access_token', '')
    ig_user_id = user_doc.get('ig_user_id', '')
    recipient_id = session.get('recipient_id')
    if not (recipient_id and message):
        return False
    button = (session.get('follow_retry_button_text')
              or session.get('follow_request_button_text')
              or DEFAULT_FOLLOW_RETRY_BUTTON_TEXT).strip()
    payload = session.get('follow_payload') or f'comment_flow:{session.get("id")}:followed'
    result = await send_ig_quick_reply(
        access_token, ig_user_id, recipient_id, message, button, payload,
    )
    sent = bool(result.get('ok'))
    if not sent:
        sent = await send_ig_dm(access_token, ig_user_id, recipient_id, message)
    now = datetime.utcnow()
    if session.get('id'):
        await db.comment_dm_sessions.update_one(
            {'id': session['id']},
            {
                '$set': {
                    'stage': stage,
                    'lastFollowReminderAt': now,
                    'updated': now,
                    # Reset follow_confirmed so the user must click again to
                    # trigger another verification attempt.
                    'follow_confirmed': False,
                },
                '$inc': {'followReminderCount': 1},
            },
        )
    if sent:
        logger.info('follow_reminder_sent session=%s stage=%s',
                    session.get('id'), stage)
    return sent


async def _verify_comment_dm_follow_gate(user_doc: dict, session: dict) -> dict:
    """Decide whether the final link may be sent for this session.

    Behavior:
      - If the gate is disabled → allow.
      - If verify_actual_follow is False (admin opt-out) → fall back to the
        legacy click-only gate using session.follow_confirmed.
      - If the user clicked confirmation, call Meta's User Profile API and
        check is_user_follow_business. The click alone is never enough.
      - Cooldown protects against confirmation spam.
      - max_follow_verification_attempts caps the number of API checks.
    """
    if not session.get('follow_request_enabled'):
        return {'allowed': True, 'checked': False}

    # Idempotency: if the link has already been delivered for this session,
    # never resend.
    if session.get('finalDmSentAt') or session.get('stage') == 'final_sent':
        return {'allowed': False, 'checked': True, 'reason': 'final_already_sent'}

    verify_enabled = session.get('verify_actual_follow')
    if verify_enabled is None:
        verify_enabled = True

    # Legacy click-only gate when admin disabled API verification.
    if not verify_enabled:
        if session.get('follow_confirmed') is True:
            return {'allowed': True, 'checked': True, 'confirmed': True,
                    'reason': 'click_only_gate'}
        prompt_sent = await _send_comment_dm_follow_gate_prompt(user_doc, session)
        return {'allowed': False, 'checked': True, 'prompt_sent': prompt_sent,
                'reason': 'awaiting_confirmation'}

    # Cached previous success.
    if session.get('follow_verified') is True:
        return {'allowed': True, 'checked': True, 'cached': True}

    # If the user has not actually pressed/typed confirmation yet, send the
    # initial follow request once and wait.
    if not session.get('follow_confirmed'):
        prompt_sent = await _send_comment_dm_follow_gate_prompt(user_doc, session)
        return {'allowed': False, 'checked': True, 'prompt_sent': prompt_sent,
                'reason': 'awaiting_confirmation'}

    # max_attempts caps how many REMINDER messages we send, not verification
    # itself. A user who follows the account after 5 failed taps must still
    # succeed on the 6th tap — we just stop spamming the not-detected reminder.
    max_attempts = int(session.get('max_follow_verification_attempts')
                       or DEFAULT_MAX_FOLLOW_VERIFICATION_ATTEMPTS)
    attempts_so_far = int(session.get('follow_verification_attempts') or 0)
    reminder_budget_exhausted = attempts_so_far >= max_attempts

    # Cooldown — protects against confirmation-button spam. We never call
    # Meta during the cooldown window, but we MUST still respond to the user
    # so the bot doesn't appear stuck. The notice is rate-limited to once
    # per cooldown window via lastCooldownNoticeAt.
    last_check = session.get('followLastCheckedAt')
    if isinstance(last_check, datetime):
        elapsed = (datetime.utcnow() - last_check).total_seconds()
        if elapsed < FOLLOW_VERIFICATION_COOLDOWN_SECONDS:
            logger.info('follow_verification_cooldown session=%s elapsed=%ss',
                        session.get('id'), int(elapsed))
            last_notice = session.get('lastCooldownNoticeAt')
            should_notify = True
            if isinstance(last_notice, datetime):
                if (datetime.utcnow() - last_notice).total_seconds() < FOLLOW_VERIFICATION_COOLDOWN_SECONDS:
                    should_notify = False
            if should_notify:
                cooldown_msg = (session.get('follow_cooldown_message') or
                                DEFAULT_FOLLOW_COOLDOWN_MESSAGE)
                sent = await _send_follow_reminder(user_doc, session, cooldown_msg,
                                                   session.get('stage') or 'awaiting_actual_follow')
                if sent and session.get('id'):
                    await db.comment_dm_sessions.update_one(
                        {'id': session['id']},
                        {'$set': {'lastCooldownNoticeAt': datetime.utcnow()}},
                    )
                return {'allowed': False, 'checked': True, 'reason': 'cooldown',
                        'prompt_sent': sent}
            return {'allowed': False, 'checked': True, 'reason': 'cooldown',
                    'prompt_sent': True}

    access_token = user_doc.get('meta_access_token', '')
    ig_scoped_id = session.get('recipient_id')
    igsid_present = bool(ig_scoped_id)
    logger.info('follow_verification_started session=%s igsid_present=%s attempt=%s/%s',
                session.get('id'), igsid_present, attempts_so_far + 1, max_attempts)

    result = await verify_instagram_user_follows_business(access_token, ig_scoped_id)
    now = datetime.utcnow()
    update_set = {
        'followLastCheckedAt': now,
        'lastFollowCheckOk': bool(result.get('ok')),
        'lastFollowCheckStatus': result.get('raw_status'),
        'updated': now,
    }
    if result.get('ok'):
        update_set['lastFollowerProfile'] = result.get('profile_excerpt')
        update_set['lastFollowVerificationError'] = None
    else:
        update_set['lastFollowVerificationError'] = result.get('reason')
    await db.comment_dm_sessions.update_one(
        {'id': session.get('id')},
        {'$set': update_set, '$inc': {'follow_verification_attempts': 1}},
    )
    # Mirror locally so the rest of this request sees the new counters.
    session['follow_verification_attempts'] = attempts_so_far + 1
    session['followLastCheckedAt'] = now

    if not result.get('ok'):
        reason = result.get('reason')
        if reason == 'permission_or_consent_required':
            failed_msg = (session.get('follow_verification_failed_message') or
                          DEFAULT_FOLLOW_VERIFICATION_FAILED_MESSAGE)
            await _send_follow_reminder(user_doc, session, failed_msg,
                                        'verification_failed')
            await db.comment_dm_sessions.update_one(
                {'id': session.get('id')},
                {'$set': {'status': 'verification_failed',
                          'stage': 'verification_failed',
                          'updated': datetime.utcnow()}},
            )
            logger.warning('follow_verification_failed session=%s reason=%s',
                           session.get('id'), reason)
            return {'allowed': False, 'checked': True, 'reason': reason}
        # temporary_api_error / missing_token_or_id — keep session pending,
        # send the verification-failed message but do not lock the state.
        soft_msg = (session.get('follow_verification_failed_message') or
                    DEFAULT_FOLLOW_VERIFICATION_FAILED_MESSAGE)
        await _send_follow_reminder(user_doc, session, soft_msg,
                                    'awaiting_actual_follow')
        logger.warning('follow_verification_failed session=%s reason=%s',
                       session.get('id'), reason)
        return {'allowed': False, 'checked': True, 'reason': reason or 'temporary_api_error'}

    if result.get('follows') is True:
        await db.comment_dm_sessions.update_one(
            {'id': session.get('id')},
            {'$set': {
                'follow_verified': True,
                'verifiedFollowAt': now,
                'stage': 'follow_verified',
                'updated': now,
            }},
        )
        session['follow_verified'] = True
        session['verifiedFollowAt'] = now
        logger.info('follow_verified_true session=%s attempt=%s',
                    session.get('id'), attempts_so_far + 1)
        return {'allowed': True, 'checked': True, 'verified': True,
                'profile': result.get('profile_excerpt')}

    # follows == False. Send the reminder only while we still have budget;
    # once the budget is exhausted we send a final fallback ONCE so the
    # user knows the bot heard them, then stay quiet on further taps.
    # The session remains retryable: a future tap that returns follows=True
    # will still send the link.
    if not reminder_budget_exhausted:
        not_detected = (session.get('follow_not_detected_message') or
                        DEFAULT_FOLLOW_NOT_DETECTED_MESSAGE)
        await _send_follow_reminder(user_doc, session, not_detected,
                                    'awaiting_actual_follow')
        logger.info('follow_verified_false session=%s attempt=%s/%s',
                    session.get('id'), attempts_so_far + 1, max_attempts)
        return {'allowed': False, 'checked': True, 'reason': 'not_following'}
    # Budget exhausted — send the verification-failed fallback once.
    if not session.get('verificationFailedFallbackSentAt'):
        failed_msg = (session.get('follow_verification_failed_message') or
                      DEFAULT_FOLLOW_VERIFICATION_FAILED_MESSAGE)
        await _send_follow_reminder(user_doc, session, failed_msg,
                                    'awaiting_actual_follow')
        await db.comment_dm_sessions.update_one(
            {'id': session.get('id')},
            {'$set': {'verificationFailedFallbackSentAt': now}},
        )
        logger.info('follow_verification_attempts_exceeded session=%s', session.get('id'))
    else:
        logger.info('follow_verification_attempts_exceeded_silent session=%s', session.get('id'))
    return {'allowed': False, 'checked': True, 'reason': 'not_following_budget_exhausted'}


async def _send_comment_dm_flow_completion(user_doc: dict, session: dict) -> bool:
    import time as _time
    _flow_start = _time.monotonic()
    access_token = user_doc.get('meta_access_token', '')
    ig_user_id = user_doc.get('ig_user_id', '')
    recipient_id = session.get('recipient_id')
    if not recipient_id:
        return False

    # Idempotency: never resend the final link if it has already been sent
    # for this session — even on duplicate webhook events. We check both the
    # in-memory session and the persisted record before doing anything.
    if session.get('finalDmSentAt') or session.get('stage') == 'final_sent':
        logger.info('final_link_already_sent session=%s', session.get('id'))
        return True
    if session.get('id'):
        persisted = await db.comment_dm_sessions.find_one({'id': session['id']})
        if persisted and (persisted.get('finalDmSentAt') or
                          persisted.get('stage') == 'final_sent'):
            logger.info('final_link_already_sent session=%s (db check)', session.get('id'))
            return True

    follow_gate = await _verify_comment_dm_follow_gate(user_doc, session)
    if not follow_gate.get('allowed'):
        return bool(follow_gate.get('prompt_sent') or follow_gate.get('reason')
                    in ('cooldown', 'final_already_sent'))

    ok_all = True
    sent_steps = ['follow_confirmed'] if session.get('follow_request_enabled') else []
    link_text = (session.get('link_dm_text') or '').strip()
    link_url = (session.get('link_url') or '').strip()
    link_button = (session.get('link_button_text') or 'Open link').strip()
    tracked_link = None
    tracked_url = ''
    url_to_send = link_url
    text_to_send = link_text
    should_track = _conversion_tracking_enabled(session, link_url or _extract_first_url(link_text))
    if should_track:
        original_url = link_url or _extract_first_url(link_text)
        if original_url:
            tracked_link = await _create_tracked_link(user_doc, session, original_url)
            if tracked_link:
                tracked_url = _tracked_link_url(tracked_link['shortCode'])
                if link_url:
                    url_to_send = tracked_url
                if text_to_send:
                    text_to_send = text_to_send.replace(original_url, tracked_url)

    if link_url:
        text_for_button = text_to_send or 'Here is the link'
        result = await send_ig_url_button(
            access_token, ig_user_id, recipient_id,
            text_for_button, link_button, url_to_send,
        )
        if result.get('ok'):
            await _activate_tracked_link(
                tracked_link,
                (result.get('body') or {}).get('message_id') or (result.get('body') or {}).get('id'),
            )
            sent_steps.append('link_button')
        else:
            fallback_text = f'{text_for_button}\n\n{url_to_send}'.strip()
            ok = await send_ig_dm(access_token, ig_user_id, recipient_id, fallback_text)
            ok_all = ok_all and ok
            sent_steps.append('link_text_fallback')
            if ok:
                await _activate_tracked_link(tracked_link)
            else:
                await _mark_tracked_link_unused(tracked_link)
            if not ok:
                logger.warning('comment_dm_link_fallback_failed session=%s err=%s',
                               session.get('id'), result.get('error'))
    elif link_text:
        ok = await send_ig_dm(access_token, ig_user_id, recipient_id, text_to_send)
        ok_all = ok_all and ok
        sent_steps.append('link_text')
        if ok:
            await _activate_tracked_link(tracked_link)
        else:
            await _mark_tracked_link_unused(tracked_link)

    extra_messages = []
    if session.get('email_request_enabled'):
        extra_messages.append('Reply with your email and we will send the details.')
    if session.get('follow_up_enabled') and session.get('follow_up_text'):
        extra_messages.append((session.get('follow_up_text') or '').strip())

    for text in [m for m in extra_messages if m]:
        ok = await send_ig_dm(access_token, ig_user_id, recipient_id, text)
        ok_all = ok_all and ok
        sent_steps.append('extra_message')

    if session.get('id'):
        await db.comment_dm_sessions.update_one(
            {'id': session['id']},
            {'$set': {
                'status': 'completed' if ok_all else 'failed',
                'stage': 'final_sent' if ok_all else 'failed',
                'completedAt': datetime.utcnow(),
                'finalDmSentAt': datetime.utcnow() if ok_all else None,
                'updated': datetime.utcnow(),
                'sentSteps': sent_steps,
            }},
        )
    if ok_all:
        logger.info('final_link_sent session=%s recipient=%s', session.get('id'), recipient_id)
        if session.get('follow_request_enabled') and session.get('verify_actual_follow') is not False:
            logger.info('final_link_sent_after_verified_follow session=%s', session.get('id'))
    flow_ms = int((_time.monotonic() - _flow_start) * 1000)
    logger.info('comment_dm_flow_completed session=%s ok=%s steps=%s total_processing_ms=%s',
                session.get('id'), ok_all, sent_steps, flow_ms)
    return ok_all


async def _find_pending_comment_dm_session(user_doc: dict, sender_id: str,
                                           payload: Optional[str] = None) -> Optional[dict]:
    if not sender_id:
        return None
    q = {
        'user_id': user_doc['id'],
        'recipient_id': sender_id,
        'status': 'pending',
    }
    if payload and str(payload).startswith('comment_flow:'):
        parts = str(payload).split(':')
        if len(parts) >= 2 and parts[1]:
            q['id'] = parts[1]
    return await db.comment_dm_sessions.find_one(q, sort=[('created', -1)])


# ---------------- Comment reply helper ----------------
from contextvars import ContextVar

# Per-call channel for surfacing classified failure reason of the last
# reply_to_ig_comment_detailed call. Kept for backward-compat with tests.
_LAST_REPLY_FAILURE: ContextVar[dict] = ContextVar('_LAST_REPLY_FAILURE', default={})


async def reply_to_ig_comment_detailed(access_token: str, ig_comment_id: str, text: str) -> dict:
    """Reply to an Instagram comment via Graph API and return a safe detailed result."""
    _LAST_REPLY_FAILURE.set({})
    if not access_token or not ig_comment_id:
        _LAST_REPLY_FAILURE.set({'failure_reason': 'missing_access_token_or_comment_id',
                                 'status_code': None})
        return _detailed_send_result(
            False, None, error={'message': 'missing_access_token_or_comment_id'}
        )
    url = f'https://graph.instagram.com/{ig_comment_id}/replies'
    import time as _time
    _start = _time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(url, data={'message': text, 'access_token': access_token})
            ms = int((_time.monotonic() - _start) * 1000)
            if r.status_code == 200:
                logger.info('comment_reply_send_ms ms=%s comment_id=%s', ms, ig_comment_id)
                try:
                    body = r.json()
                except Exception:
                    body = {}
                result = _detailed_send_result(True, r.status_code, body=body)
                result['provider_response_ok'] = True
                # Sync _LAST_REPLY_FAILURE for backward-compat.
                _LAST_REPLY_FAILURE.set({})
                result['provider_comment_id'] = body.get('id') or body.get('comment_id')
                return result
            safe_error = _safe_provider_error_payload(r.text[:500])
            classified = classify_instagram_send_error(safe_error, r.status_code)
            logger.error('reply_to_ig_comment_failed status=%s ms=%s reason=%s retryable=%s',
                         r.status_code, ms, classified['failure_reason'], classified['retryable'])
            return _detailed_send_result(False, r.status_code, error=safe_error)
    except Exception as e:
        logger.exception('reply_to_ig_comment exception: %s', e)
        _LAST_REPLY_FAILURE.set({'failure_reason': 'temporary_graph_error', 'status_code': None})
        return _detailed_send_result(False, None, error={'message': str(e)[:300]})


async def reply_to_ig_comment(access_token: str, ig_comment_id: str, text: str) -> bool:
    """Reply to an Instagram comment via Graph API."""
    result = await reply_to_ig_comment_detailed(access_token, ig_comment_id, text)
    return bool(result.get('ok'))


_ORIGINAL_REPLY_TO_IG_COMMENT_DETAILED = reply_to_ig_comment_detailed
_ORIGINAL_REPLY_TO_IG_COMMENT = reply_to_ig_comment


async def _call_reply_to_ig_comment_detailed(access_token: str, ig_comment_id: str,
                                             text: str) -> dict:
    """Call detailed reply helper, while preserving old tests that patch the bool wrapper."""
    if reply_to_ig_comment_detailed is not _ORIGINAL_REPLY_TO_IG_COMMENT_DETAILED:
        return await reply_to_ig_comment_detailed(access_token, ig_comment_id, text)
    if reply_to_ig_comment is not _ORIGINAL_REPLY_TO_IG_COMMENT:
        ok = await reply_to_ig_comment(access_token, ig_comment_id, text)
        result = _detailed_send_result(bool(ok), 200 if ok else None,
                                       error={'message': 'patched_reply_to_ig_comment_failed'})
        if ok:
            result['provider_response_ok'] = True
        # Sync _LAST_REPLY_FAILURE for backward-compat with tests.
        _LAST_REPLY_FAILURE.set({} if ok else {
            'failure_reason': result.get('failure_reason') or 'unknown_graph_error',
            'status_code': result.get('status_code'),
        })
        return result
    return await reply_to_ig_comment_detailed(access_token, ig_comment_id, text)


# ---------------- Automation engine ----------------
async def execute_flow(user: dict, automation: dict, sender_ig_id: str,
                       trigger_text: str = '', comment_context: Optional[dict] = None,
                       flow_results: Optional[dict] = None):
    """Walk the flow graph and execute each node in order.

    comment_context: when the trigger was an Instagram comment, holds
        {'ig_comment_id': ..., 'comment_doc_id': ...} so reply_comment nodes work.

    flow_results: optional dict the caller provides to capture per-step
        outcomes. Mutated during the walk:
          {
            'reply_status': 'success' | 'failed' | 'skipped' | 'disabled',
            'reply_failure_reason': str | None,
            'replied_at': datetime | None,
            'dm_status':    'success' | 'failed' | 'skipped' | 'disabled',
            'dm_failure_reason': str | None,
            'dm_sent_at': datetime | None,
          }
        Defaults are 'disabled' so a node-type that never appeared stays
        distinguishable from a node that ran but failed.
    """
    automation = _ensure_public_reply_node(automation)
    nodes = automation.get('nodes', [])
    edges = automation.get('edges', [])
    if flow_results is None:
        flow_results = {}
    flow_results.setdefault('reply_status', 'disabled')
    flow_results.setdefault('reply_failure_reason', None)
    flow_results.setdefault('replied_at', None)
    flow_results.setdefault('dm_status', 'disabled')
    flow_results.setdefault('dm_failure_reason', None)
    flow_results.setdefault('dm_sent_at', None)
    # Pre-flight invariant: if the rule is configured with a public reply
    # in any shape, reply_status='disabled' at the end is illegal. The
    # caller (_run_and_record_action) and the dedup recovery path use
    # this flag to convert an unattempted public reply into a retryable
    # failure with skip_reason='public_reply_required_not_attempted'.
    flow_results['public_reply_required'] = _automation_public_reply_required(automation)
    if not nodes:
        return False

    node_map = {n['id']: n for n in nodes}
    edge_map: Dict[str, list] = {}
    for e in edges:
        edge_map.setdefault(e['source'], []).append(e['target'])

    start = next((n for n in nodes if n.get('type') == 'trigger'), None)
    if not start:
        return False

    access_token = user.get('meta_access_token', '')
    ig_user_id = user.get('ig_user_id', '')
    flow_source = (comment_context or {}).get('source') if comment_context else None
    flow_received_monotonic = (comment_context or {}).get('received_monotonic') if comment_context else None
    flow_comment_id = (comment_context or {}).get('ig_comment_id') if comment_context else None
    flow_comment_doc_id = (comment_context or {}).get('comment_doc_id') if comment_context else None
    public_replies = _automation_public_reply_texts(automation)
    public_reply_text = public_replies[0] if public_replies else ''
    dm_diag_text = _automation_dm_text_for_diagnostics(automation)
    public_reply_source = _automation_public_reply_source(automation)
    before_status_doc = {}
    if flow_comment_doc_id:
        before_status_doc = await db.comments.find_one({'id': flow_comment_doc_id}) or {}
    logger.info(
        'automation_action_plan comment_id=%s user_id=%s instagram_account_id=%s media_id=%s '
        'matched_rule_id=%s matched_rule_scope=%s matched_rule_priority=%s '
        'broad_rules_skipped_due_specific_match=%s rule_has_public_reply=%s '
        'public_reply_text_length=%s public_reply_text_hash=%s public_reply_source=%s rule_has_dm=%s '
        'dm_text_length=%s dm_text_hash=%s reply_status_before=%s dm_status_before=%s '
        'action_status_before=%s',
        flow_comment_id,
        user.get('id'),
        ig_user_id,
        before_status_doc.get('media_id') or before_status_doc.get('mediaId'),
        automation.get('id'),
        before_status_doc.get('matched_rule_scope'),
        before_status_doc.get('matched_rule_priority'),
        before_status_doc.get('broad_rules_skipped_due_specific_match'),
        bool(public_reply_text),
        len(public_reply_text),
        _safe_text_hash(public_reply_text),
        public_reply_source,
        _automation_has_node_type(automation, 'message'),
        len(dm_diag_text),
        _safe_text_hash(dm_diag_text),
        before_status_doc.get('reply_status') or before_status_doc.get('replyStatus'),
        before_status_doc.get('dm_status') or before_status_doc.get('dmStatus'),
        before_status_doc.get('action_status') or before_status_doc.get('actionStatus'),
    )

    def webhook_elapsed_ms() -> Optional[int]:
        if flow_source != 'webhook' or flow_received_monotonic is None:
            return None
        import time as _time
        return int((_time.monotonic() - flow_received_monotonic) * 1000)

    # Process reply_comment nodes before message/DM nodes so a public
    # comment reply is always attempted (and persisted) BEFORE the DM
    # step has a chance to fail. This is the key invariant that lets us
    # report partial_success cleanly when DM is unavailable.
    REPLY_NODE_TYPES = {'reply_comment'}

    def _node_priority(_id):
        n = node_map.get(_id) or {}
        return 0 if n.get('type') in REPLY_NODE_TYPES else 1

    current_ids = [start['id']]
    visited: set = set()
    action_attempted = False
    ok_all = True

    while current_ids:
        # Always pull the highest-priority node next (reply_comment wins).
        current_ids.sort(key=_node_priority)
        nid = current_ids.pop(0)
        if nid in visited:
            continue
        visited.add(nid)
        node = node_map.get(nid)
        if not node:
            continue
        ntype = node.get('type', '')
        data = node.get('data', {})

        if ntype == 'message':
            msg_text = data.get('text') or data.get('message', '')
            if msg_text and sender_ig_id:
                action_attempted = True
                dm_failure_reason = None
                # Reset the contextvar so we read THIS send's reason, not
                # a stale value from an earlier send in the same task.
                _LAST_DM_FAILURE.set({})
                dm_reservation = None
                existing_dm_success = False
                if comment_context and comment_context.get('comment_doc_id'):
                    existing_comment = await db.comments.find_one({'id': comment_context['comment_doc_id']})
                    existing_dm_success = bool(
                        existing_comment
                        and _status_is_success(existing_comment.get('dm_status') or existing_comment.get('dmStatus'))
                    )
                if existing_dm_success:
                    ok = True
                    dm_result = {'ok': True, 'failure_reason': None, 'retryable': False}
                    logger.info(
                        'automation_step_diagnostics comment_id=%s matched_rule_id=%s dm_attempted=%s dm_skip_reason=%s',
                        flow_comment_id, automation.get('id'), False, 'already_dm_success'
                    )
                    logger.info('dm_duplicate_skipped comment_id=%s rule_id=%s',
                                flow_comment_id, automation.get('id'))
                elif _comment_dm_flow_enabled(automation):
                    limit_reason = await _respect_account_send_spacing(
                        user.get('id', ''), ig_user_id, 'dm'
                    )
                    if limit_reason:
                        ok = False
                        dm_result = {
                            'ok': False,
                            'failure_reason': 'rate_limited',
                            'retryable': True,
                            'safe_label': limit_reason,
                        }
                        logger.info(
                            'automation_step_diagnostics comment_id=%s matched_rule_id=%s dm_attempted=%s dm_skip_reason=%s',
                            flow_comment_id, automation.get('id'), False, limit_reason
                        )
                    else:
                        dm_reservation = await reserve_usage_limit(
                            user.get('id', ''), 'monthly_dms_sent_limit', increment=1,
                            instagram_account_id=ig_user_id,
                            source=flow_source or 'runtime',
                            automation_id=automation.get('id'),
                            ig_comment_id=flow_comment_id,
                            action_id=f"{comment_context.get('comment_doc_id')}:dm" if comment_context else None,
                        )
                        if not dm_reservation.get('allowed') or (
                            dm_reservation.get('exceeded') and not dm_reservation.get('fail_open')
                        ):
                            ok = False
                            dm_result = {
                                'ok': False,
                                'failure_reason': 'plan_limit_exceeded',
                                'retryable': False,
                            }
                            flow_results['dm_status'] = 'plan_limited'
                            flow_results['dm_failure_reason'] = 'plan_limit_exceeded'
                            if comment_context and comment_context.get('comment_doc_id'):
                                now_dl = datetime.utcnow()
                                await db.comments.update_one(
                                    {'id': comment_context['comment_doc_id']},
                                    {'$set': {
                                        'dm_status': 'plan_limited',
                                        'dmStatus': 'plan_limited',
                                        'dm_failure_reason': 'plan_limit_exceeded',
                                        'dm_failure_retryable': False,
                                        'last_attempt_at': now_dl,
                                        'updated': now_dl,
                                    }},
                                )
                            logger.warning(
                                'dm_skipped_plan_limit ig_comment_id=%s rule_id=%s plan_key=%s used=%s limit=%s',
                                flow_comment_id, automation.get('id'),
                                dm_reservation.get('plan_key'),
                                dm_reservation.get('used'),
                                dm_reservation.get('limit'),
                            )
                            ok_all = False
                            continue
                        logger.info(
                            'automation_step_diagnostics comment_id=%s matched_rule_id=%s dm_attempted=%s dm_skip_reason=%s',
                            flow_comment_id, automation.get('id'), True, None
                        )
                        ok = await _send_comment_dm_flow_entry(
                            user, automation, sender_ig_id, comment_context
                        )
                        last = _LAST_DM_FAILURE.get() or {}
                        dm_result = {
                            'ok': bool(ok),
                            'failure_reason': None if ok else (last.get('failure_reason') or 'unknown_graph_error'),
                            'retryable': False,
                        }
                    if not ok:
                        last = _LAST_DM_FAILURE.get() or {}
                        dm_failure_reason = (
                            dm_result.get('failure_reason')
                            or last.get('failure_reason')
                            or 'unknown_graph_error'
                        )
                    logger.info('Flow comment DM entry to %s rule=%s ok=%s',
                                sender_ig_id, automation.get('id'), ok)
                else:
                    limit_reason = await _respect_account_send_spacing(
                        user.get('id', ''), ig_user_id, 'dm'
                    )
                    if limit_reason:
                        dm_result = {
                            'ok': False,
                            'failure_reason': 'rate_limited',
                            'retryable': True,
                            'safe_label': limit_reason,
                        }
                        logger.info(
                            'automation_step_diagnostics comment_id=%s matched_rule_id=%s dm_attempted=%s dm_skip_reason=%s',
                            flow_comment_id, automation.get('id'), False, limit_reason
                        )
                    else:
                        dm_reservation = await reserve_usage_limit(
                            user.get('id', ''), 'monthly_dms_sent_limit', increment=1,
                            instagram_account_id=ig_user_id,
                            source=flow_source or 'runtime',
                            automation_id=automation.get('id'),
                            ig_comment_id=flow_comment_id,
                            action_id=f"{comment_context.get('comment_doc_id')}:dm" if comment_context else None,
                        )
                        if not dm_reservation.get('allowed') or (
                            dm_reservation.get('exceeded') and not dm_reservation.get('fail_open')
                        ):
                            ok = False
                            dm_result = {
                                'ok': False,
                                'failure_reason': 'plan_limit_exceeded',
                                'retryable': False,
                            }
                            flow_results['dm_status'] = 'plan_limited'
                            flow_results['dm_failure_reason'] = 'plan_limit_exceeded'
                            if comment_context and comment_context.get('comment_doc_id'):
                                now_dl = datetime.utcnow()
                                await db.comments.update_one(
                                    {'id': comment_context['comment_doc_id']},
                                    {'$set': {
                                        'dm_status': 'plan_limited',
                                        'dmStatus': 'plan_limited',
                                        'dm_failure_reason': 'plan_limit_exceeded',
                                        'dm_failure_retryable': False,
                                        'last_attempt_at': now_dl,
                                        'updated': now_dl,
                                    }},
                                )
                            logger.warning(
                                'dm_skipped_plan_limit ig_comment_id=%s rule_id=%s plan_key=%s used=%s limit=%s',
                                flow_comment_id, automation.get('id'),
                                dm_reservation.get('plan_key'),
                                dm_reservation.get('used'),
                                dm_reservation.get('limit'),
                            )
                            ok_all = False
                            continue
                        logger.info(
                            'automation_step_diagnostics comment_id=%s matched_rule_id=%s dm_attempted=%s dm_skip_reason=%s',
                            flow_comment_id, automation.get('id'), True, None
                        )
                        dm_result = await _call_send_ig_dm_detailed(access_token, ig_user_id, sender_ig_id, msg_text)
                    ok = bool(dm_result.get('ok'))
                    if not ok:
                        # Capture the classified DM failure for flow_results.
                        last = _LAST_DM_FAILURE.get() or {}
                        dm_failure_reason = (
                            dm_result.get('failure_reason')
                            or last.get('failure_reason')
                            or 'unknown_graph_error'
                        )
                    logger.info('Flow message to %s rule=%s ok=%s reason=%s',
                                sender_ig_id, automation.get('id'), ok,
                                dm_result.get('failure_reason'))
                if comment_context and comment_context.get('comment_doc_id'):
                    now = datetime.utcnow()
                    update = {
                        'dm_status': 'success' if ok else 'failed',
                        'dmStatus': 'success' if ok else 'failed',
                        'last_attempt_at': now,
                        'updated': now,
                        **_send_failure_fields('dm', dm_result),
                    }
                    if ok:
                        update['dm_sent_at'] = now
                        update['dmSentAt'] = now
                    await db.comments.update_one(
                        {'id': comment_context['comment_doc_id']},
                        {'$set': update},
                    )
                    if ok and not existing_dm_success:
                        recorded_dm_usage = await confirm_usage_reservation(
                            dm_reservation,
                            user_id=user.get('id', ''),
                            event_type='dm_sent',
                            instagram_account_id=ig_user_id,
                            automation_id=automation.get('id'),
                            comment_id=comment_context['comment_doc_id'],
                            metadata={
                                'source': flow_source or 'runtime',
                                'ig_comment_id': flow_comment_id,
                            },
                        )
                        if recorded_dm_usage:
                            await db.comments.update_one(
                                {'id': comment_context['comment_doc_id']},
                                {'$set': {
                                    'usage_dm_sent_recorded': True,
                                    'usage_dm_sent_recorded_at': datetime.utcnow(),
                                }},
                            )
                if flow_source == 'webhook':
                    logger.info(
                        'total_webhook_to_dm_ms=%s ig_comment_id=%s rule_id=%s ok=%s reason=%s',
                        webhook_elapsed_ms(), flow_comment_id, automation.get('id'),
                        bool(ok), dm_failure_reason,
                    )
                # Record per-step outcome so callers can compute action_status
                # without conflating reply success with DM failure.
                if ok:
                    flow_results['dm_status'] = 'success'
                    flow_results['dm_failure_reason'] = None
                    flow_results['dm_sent_at'] = datetime.utcnow()
                else:
                    flow_results['dm_status'] = 'failed'
                    flow_results['dm_failure_reason'] = dm_failure_reason
                if comment_context and comment_context.get('comment_doc_id'):
                    await db.comments.update_one(
                        {'id': comment_context['comment_doc_id']},
                        {'$set': {
                            'dm_status': flow_results['dm_status'],
                            'dm_failure_reason': flow_results['dm_failure_reason'],
                            'dm_sent_at': flow_results['dm_sent_at'],
                            'updated': datetime.utcnow(),
                        }},
                    )
                ok_all = ok_all and bool(ok)
        elif ntype == 'reply_comment':
            replies = data.get('replies')
            if replies and isinstance(replies, list) and len(replies) > 0:
                import random
                msg_text = random.choice(replies)
            else:
                msg_text = data.get('text') or data.get('message', '')
            if msg_text and comment_context and comment_context.get('ig_comment_id'):
                action_attempted = True
                reply_reservation = None
                already_replied = False
                if comment_context.get('comment_doc_id'):
                    existing_comment = await db.comments.find_one({'id': comment_context['comment_doc_id']})
                    already_replied = bool(
                        existing_comment
                        and _reply_provider_proof_exists(existing_comment)
                    )
                if already_replied:
                    ok = True
                    reply_result = {'ok': True, 'provider_response_ok': True,
                                    'failure_reason': None, 'retryable': False}
                    logger.info(
                        'automation_step_diagnostics comment_id=%s matched_rule_id=%s public_reply_attempted=%s public_reply_skip_reason=%s',
                        flow_comment_id, automation.get('id'), False, 'already_provider_confirmed'
                    )
                    logger.info('comment_reply_duplicate_skipped ig_comment_id=%s rule_id=%s',
                                comment_context['ig_comment_id'], automation.get('id'))
                else:
                    limit_reason = await _respect_account_send_spacing(
                        user.get('id', ''), ig_user_id, 'comment_reply'
                    )
                    if limit_reason:
                        reply_result = {
                            'ok': False,
                            'failure_reason': 'rate_limited',
                            'retryable': True,
                            'safe_label': limit_reason,
                        }
                        logger.info(
                            'automation_step_diagnostics comment_id=%s matched_rule_id=%s public_reply_attempted=%s public_reply_skip_reason=%s',
                            flow_comment_id, automation.get('id'), False, limit_reason
                        )
                    else:
                        reply_reservation = await reserve_usage_limit(
                            user.get('id', ''), 'monthly_public_replies_sent_limit', increment=1,
                            instagram_account_id=ig_user_id,
                            source=flow_source or 'runtime',
                            automation_id=automation.get('id'),
                            ig_comment_id=comment_context['ig_comment_id'],
                            action_id=f"{comment_context.get('comment_doc_id')}:reply",
                        )
                        if not reply_reservation.get('allowed') or (
                            reply_reservation.get('exceeded') and not reply_reservation.get('fail_open')
                        ):
                            flow_results['reply_status'] = 'plan_limited'
                            flow_results['reply_failure_reason'] = 'plan_limit_exceeded'
                            if comment_context.get('comment_doc_id'):
                                now_rl = datetime.utcnow()
                                await db.comments.update_one(
                                    {'id': comment_context['comment_doc_id']},
                                    {'$set': {
                                        'reply_status': 'plan_limited',
                                        'replyStatus': 'plan_limited',
                                        'reply_failure_reason': 'plan_limit_exceeded',
                                        'reply_failure_retryable': False,
                                        'last_attempt_at': now_rl,
                                        'updated': now_rl,
                                    }},
                                )
                            logger.warning(
                                'public_reply_skipped_plan_limit ig_comment_id=%s rule_id=%s plan_key=%s used=%s limit=%s',
                                comment_context['ig_comment_id'], automation.get('id'),
                                reply_reservation.get('plan_key'),
                                reply_reservation.get('used'),
                                reply_reservation.get('limit'),
                            )
                            ok_all = False
                            continue
                        logger.info(
                            'automation_step_diagnostics comment_id=%s matched_rule_id=%s public_reply_attempted=%s public_reply_skip_reason=%s',
                            flow_comment_id, automation.get('id'), True, None
                        )
                        reply_result = await _call_reply_to_ig_comment_detailed(
                            access_token, comment_context['ig_comment_id'], msg_text
                        )
                        reply_result = _normalize_reply_result_for_provider_proof(reply_result)
                    ok = bool(reply_result.get('ok'))
                logger.info('Flow comment reply on %s rule=%s ok=%s reason=%s',
                            comment_context['ig_comment_id'], automation.get('id'), ok,
                            reply_result.get('failure_reason'))
                if ok:
                    logger.info(
                        'comment_reply_sent source=%s ig_comment_id=%s rule_id=%s total_webhook_to_reply_ms=%s',
                        flow_source or 'unknown',
                        comment_context['ig_comment_id'],
                        automation.get('id'),
                        webhook_elapsed_ms(),
                    )
                else:
                    logger.warning(
                        'comment_reply_failed source=%s ig_comment_id=%s rule_id=%s '
                        'reason=%s total_webhook_to_reply_ms=%s',
                        flow_source or 'unknown',
                        comment_context['ig_comment_id'],
                        automation.get('id'),
                        reply_result.get('failure_reason'),
                        webhook_elapsed_ms(),
                    )
                ok_all = ok_all and bool(ok)
                now_ts = datetime.utcnow()
                provider_ok_fr = _reply_result_has_provider_proof(reply_result)
                if provider_ok_fr:
                    flow_results['reply_status'] = 'success'
                    flow_results['reply_failure_reason'] = None
                    flow_results['replied_at'] = now_ts
                else:
                    flow_results['reply_status'] = 'failed'
                    flow_results['reply_failure_reason'] = reply_result.get('failure_reason')
                if comment_context.get('comment_doc_id'):
                    now = datetime.utcnow()
                    provider_ok = _reply_result_has_provider_proof(reply_result)
                    update = {
                        'reply_status': 'success' if provider_ok else 'failed',
                        'replyStatus': 'success' if provider_ok else 'failed',
                        'reply_attempted_at': now,
                        'reply_provider_status': reply_result.get('status_code'),
                        'reply_provider_response_ok': bool(provider_ok),
                        'reply_provider_comment_id': reply_result.get('provider_comment_id'),
                        'reply_success_source': (flow_source or 'runtime') if provider_ok else None,
                        'last_attempt_at': now,
                        'updated': now,
                        **_send_failure_fields('reply', reply_result),
                    }
                    if provider_ok:
                        update.update({
                            'replied': True,
                            'reply_text': msg_text,
                            'replySentAt': now,
                            'replied_at': now,
                        })
                    await db.comments.update_one(
                        {'id': comment_context['comment_doc_id']},
                        {'$set': update}
                    )
                    if provider_ok and not already_replied:
                        recorded_reply_usage = await confirm_usage_reservation(
                            reply_reservation,
                            user_id=user.get('id', ''),
                            event_type='public_reply_sent',
                            instagram_account_id=ig_user_id,
                            automation_id=automation.get('id'),
                            comment_id=comment_context['comment_doc_id'],
                            metadata={
                                'source': flow_source or 'runtime',
                                'provider_status': reply_result.get('status_code'),
                                'provider_comment_id_exists': bool(reply_result.get('provider_comment_id')),
                            },
                        )
                        if recorded_reply_usage:
                            await db.comments.update_one(
                                {'id': comment_context['comment_doc_id']},
                                {'$set': {
                                    'usage_public_reply_sent_recorded': True,
                                    'usage_public_reply_sent_recorded_at': datetime.utcnow(),
                                }},
                            )
        elif ntype == 'delay':
            secs = int(data.get('seconds', 0) or data.get('delay', 0))
            if secs > 0:
                await asyncio.sleep(min(secs, 30))
        elif ntype == 'condition':
            keyword = (data.get('value') or '').lower()
            match = keyword in trigger_text.lower() if keyword else True
            # Take 'yes' edge if match, 'no' edge otherwise
            for edge in edges:
                if edge['source'] == nid:
                    label = (edge.get('label') or '').lower()
                    if match and label in ('yes', 'true', ''):
                        current_ids.append(edge['target'])
                    elif not match and label in ('no', 'false'):
                        current_ids.append(edge['target'])
            continue

        next_ids = list(edge_map.get(nid, []))
        if comment_context:
            next_ids.sort(key=lambda next_id: 0 if (node_map.get(next_id) or {}).get('type') == 'reply_comment' else 1)
        for next_id in next_ids:
            current_ids.append(next_id)

    if action_attempted and ok_all:
        await db.automations.update_one(
            {'id': automation['id']},
            {'$inc': {'sent': 1}, '$set': {'updated': datetime.utcnow()}}
        )
    else:
        await db.automations.update_one(
            {'id': automation['id']},
            {'$set': {'updated': datetime.utcnow()}}
        )
    if flow_comment_doc_id:
        after_status_doc = await db.comments.find_one({'id': flow_comment_doc_id}) or {}
        logger.info(
            'automation_action_result comment_id=%s matched_rule_id=%s reply_status_after=%s '
            'dm_status_after=%s action_status_after=%s reply_provider_response_ok=%s',
            flow_comment_id,
            automation.get('id'),
            after_status_doc.get('reply_status') or after_status_doc.get('replyStatus'),
            after_status_doc.get('dm_status') or after_status_doc.get('dmStatus'),
            after_status_doc.get('action_status') or after_status_doc.get('actionStatus'),
            bool(after_status_doc.get('reply_provider_response_ok') is True),
        )
    return bool(action_attempted and ok_all)


# ---------------- helpers ----------------
def _strip_mongo(doc):
    if doc and '_id' in doc:
        doc.pop('_id', None)
    return doc


def _public_user(u: dict) -> UserPublic:
    instagram_valid = _has_valid_instagram_connection(u)
    return UserPublic(
        id=u['id'], username=u['username'], name=u['name'], email=u['email'],
        avatar=u.get('avatar') or f"https://i.pravatar.cc/150?u={u['username']}",
        instagramConnected=instagram_valid,
        instagramHandle=u.get('instagramHandle'),
        instagramProfilePictureUrl=u.get('instagram_profile_picture_url'),
        instagramConnectionValid=instagram_valid,
        instagramAccountType=u.get('instagram_account_type'),
        activeInstagramAccountId=u.get('active_instagram_account_id'),
        activeInstagramIgUserId=u.get('ig_user_id'),
    )


def _has_valid_instagram_connection(u: Optional[dict]) -> bool:
    return bool(
        u
        and u.get('instagramConnected')
        and u.get('instagram_connection_valid')
        and u.get('meta_access_token')
        and u.get('ig_user_id')
    )


def _instagram_connection_error(u: Optional[dict]) -> str:
    if not u:
        return 'Instagram not connected'
    blocker = u.get('instagram_connection_blocker') or 'token_cannot_call_graph_me'
    if u.get('instagramConnected') and not u.get('instagram_connection_valid'):
        return f'Instagram reconnect required: {blocker}'
    return 'Instagram not connected'


async def _seed_user(user_id: str):
    """No fake data. New users start with a clean slate — contacts,
    conversations, automations and comments will be populated by real
    Instagram webhook events once the account is connected."""
    return


def _hash_identifier(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return hashlib.sha256(value.strip().lower().encode('utf-8')).hexdigest()


def _data_deletion_confirmation_code() -> str:
    return f"mychat-del-{secrets.token_hex(8)}"


async def _parse_data_deletion_payload(request: Request) -> dict:
    raw = await request.body()
    if not raw:
        return {}
    content_type = (request.headers.get('content-type') or '').lower()
    if 'application/json' in content_type:
        try:
            payload = json.loads(raw.decode('utf-8'))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}
    if 'application/x-www-form-urlencoded' in content_type:
        parsed = parse_qs(raw.decode('utf-8'), keep_blank_values=False)
        return {k: v[-1] for k, v in parsed.items() if v}
    return {}


def _verify_meta_signed_request(signed_request: str) -> bool:
    signed_request = str(signed_request or '').strip()
    secret = META_APP_SECRET or META_WEBHOOK_APP_SECRET
    if not signed_request or not secret or '.' not in signed_request:
        return False
    try:
        encoded_sig, payload = signed_request.split('.', 1)
        expected = hmac.new(
            secret.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256,
        ).digest()
        padding = '=' * (-len(encoded_sig) % 4)
        provided = base64.urlsafe_b64decode((encoded_sig + padding).encode('utf-8'))
        return hmac.compare_digest(provided, expected)
    except Exception:
        return False


@api.post('/meta/data-deletion')
async def meta_data_deletion_callback(request: Request):
    """Public Meta data-deletion callback.

    Stores only hashed request metadata and returns a confirmation code plus
    public deletion-status instructions. It never returns stored user data and
    never logs signed_request contents.
    """
    ip = _client_ip(request)
    if _rate_limited('data_deletion', ip,
                     limit=RATE_LIMIT_DATA_DELETION_PER_HOUR, window_seconds=3600):
        logger.warning('rate_limit_hit bucket=data_deletion ip=%s', ip)
        raise HTTPException(429, 'Too many data deletion requests. Try again later.')
    payload = await _parse_data_deletion_payload(request)
    confirmation_code = _data_deletion_confirmation_code()
    signed_request = str(payload.get('signed_request') or '')
    email = str(payload.get('email') or payload.get('user_email') or '')
    signed_request_valid = _verify_meta_signed_request(signed_request)
    doc = {
        'confirmation_code': confirmation_code,
        'source': 'meta_callback',
        'signed_request_present': bool(signed_request),
        'signed_request_valid': bool(signed_request_valid),
        'signed_request_sha256': _hash_identifier(signed_request),
        'email_sha256': _hash_identifier(email),
        'ip_hash': _hash_identifier(_client_ip(request)),
        'created_at': datetime.utcnow(),
        'status': 'received',
    }
    try:
        await db.data_deletion_requests.insert_one(doc)
    except Exception as e:
        logger.warning('data_deletion_request_store_failed reason=%s',
                       str(e)[:80])
    logger.info(
        'data_deletion_request_received source=meta_callback '
        'signed_request_present=%s confirmation_code=%s',
        bool(signed_request),
        confirmation_code,
    )
    return {
        'url': f"{FRONTEND_URL.rstrip('/')}/data-deletion?confirmation_code={confirmation_code}",
        'confirmation_code': confirmation_code,
    }


# ---------------- auth ----------------

# Phase 2.18G security: centralize password policy in one place so signup,
# password change, and password reset all enforce the same rules. The
# previous code allowed a 1-character signup password because SignupIn
# had no length check, and 6-character passwords on change/reset.
PASSWORD_MIN_LENGTH = 8
PASSWORD_MAX_LENGTH = 256


def _enforce_password_policy(candidate: object) -> str:
    """Validate a new password. Raises HTTPException with a stable detail
    string the frontend already translates. Returns the validated string."""
    if not isinstance(candidate, str):
        raise HTTPException(400, 'password_required')
    if len(candidate) < PASSWORD_MIN_LENGTH:
        raise HTTPException(400, 'password_too_short')
    if len(candidate) > PASSWORD_MAX_LENGTH:
        raise HTTPException(400, 'password_too_long')
    return candidate


@api.post('/auth/signup', response_model=AuthOut)
async def signup(data: SignupIn, request: Request):
    ip = _client_ip(request)
    _enforce_password_policy(data.password)
    normalized_email = _normalize_email_value(data.email)
    if _rate_limited('signup', ip,
                     limit=RATE_LIMIT_SIGNUP_PER_HOUR, window_seconds=3600):
        logger.warning('rate_limit_hit bucket=signup ip=%s', ip)
        raise HTTPException(429, 'Too many signups from this IP. Try again later.')
    email_hash = _hash_identifier(normalized_email)
    if email_hash and _rate_limited('signup_email', email_hash,
                                    limit=RATE_LIMIT_SIGNUP_PER_HOUR, window_seconds=3600):
        logger.warning('rate_limit_hit bucket=signup_email email_hash=%s', email_hash[:12])
        raise HTTPException(429, 'Too many signups for this email. Try again later.')
    if await db.users.find_one({'username': data.username}):
        raise HTTPException(400, 'Username already taken')
    if await _find_user_by_email(normalized_email):
        raise HTTPException(400, 'Email already registered')
    if PASSWORD_EMAIL_VERIFICATION_REQUIRED and not _email_verification_delivery_configured():
        raise HTTPException(503, 'email_verification_not_configured')
    import uuid
    user_id = str(uuid.uuid4())
    user_doc = {
        'id': user_id, 'username': data.username, 'email': normalized_email,
        'normalized_email': normalized_email,
        'name': data.username.capitalize(),
        'password_hash': hash_password(data.password),
        'avatar': f'https://i.pravatar.cc/150?u={data.username}',
        'instagramConnected': False, 'instagramHandle': None,
        'session_version': 0,
        'email_verified': not PASSWORD_EMAIL_VERIFICATION_REQUIRED,
        'email_verification_required': bool(PASSWORD_EMAIL_VERIFICATION_REQUIRED),
        'auth_provider': 'password',
        'linked_providers': ['password'],
        'created': datetime.utcnow(),
    }
    await db.users.insert_one(user_doc)
    await _seed_user(user_id)
    if PASSWORD_EMAIL_VERIFICATION_REQUIRED:
        issued = await _issue_email_verification(user_doc, reason='signup')
        if not issued.get('sent'):
            raise HTTPException(503, 'email_verification_not_configured')
        raise HTTPException(403, 'email_verification_required')
    return AuthOut(token=create_token(user_id, session_version=0), user=_public_user(user_doc))


@api.post('/auth/login', response_model=AuthOut)
async def login(data: LoginIn, request: Request):
    ip = _client_ip(request)
    normalized_identifier = _normalize_email_value(data.username)
    if _rate_limited('login', ip,
                     limit=RATE_LIMIT_LOGIN_PER_MIN, window_seconds=60):
        logger.warning('rate_limit_hit bucket=login ip=%s', ip)
        raise HTTPException(429, 'Too many login attempts. Try again in a minute.')
    identifier_hash = _hash_identifier(normalized_identifier)
    if identifier_hash and _rate_limited('login_identifier', identifier_hash,
                                         limit=RATE_LIMIT_LOGIN_PER_MIN, window_seconds=60):
        logger.warning('rate_limit_hit bucket=login_identifier identifier_hash=%s', identifier_hash[:12])
        raise HTTPException(429, 'Too many login attempts. Try again in a minute.')
    if '@' in normalized_identifier:
        u = await _find_user_by_email(normalized_identifier)
    else:
        u = await db.users.find_one({'username': data.username})
    if not u or not verify_password(data.password, u['password_hash']):
        raise HTTPException(401, 'Invalid username or password')
    # Phase 2.8: block login for suspended/deleted users.
    _ensure_user_active(u)
    if _email_verification_required(u):
        raise HTTPException(403, 'email_verification_required')
    return AuthOut(token=create_token(u['id'], session_version=_user_session_version(u)), user=_public_user(u))


class PasswordChangeIn(BaseModel):
    current_password: str
    new_password: str


@api.post('/auth/password')
async def change_password(
    data: PasswordChangeIn,
    user_id: str = Depends(get_current_active_user_id),
):
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    if not u.get('password_hash'):
        raise HTTPException(400, 'Password login not configured for this account')
    if not verify_password(data.current_password, u['password_hash']):
        raise HTTPException(401, 'Current password is incorrect')
    new_password = _enforce_password_policy(data.new_password)
    new_hashed = hash_password(new_password)
    result = await db.users.update_one(
        {'id': user_id},
        {'$set': {
            'password_hash': new_hashed,
            'session_version': _user_session_version(u) + 1,
            'updated_at': datetime.utcnow(),
        }},
    )
    if result.matched_count == 0:
        raise HTTPException(500, 'Failed to update password — user not found')
    if result.modified_count == 0:
        logger.warning('password_change_no_modify user_id=%s', user_id)
    logger.info('password_changed user_id=%s', user_id)
    return {'ok': True, 'detail': 'Password changed successfully'}


@api.get('/auth/me', response_model=UserPublic)
async def me(user_id: str = Depends(get_current_active_user_id)):
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    # Phase 2.18T: reconcile stale legacy Instagram flags before
    # returning so Topbar / Sidebar always see a consistent state
    # even when the user goes through /auth/me directly (bypassing
    # /auth/bootstrap).
    u = await _reconcile_user_instagram_flags(user_id, u) or u
    return _public_user(u)


# Phase 2.18U: profile editing. Email changes intentionally NOT
# supported here — they need a verification-link flow first. Only
# display fields (name, username) are mutable in this endpoint.
PROFILE_NAME_MIN = 1
PROFILE_NAME_MAX = 80
PROFILE_USERNAME_MIN = 3
PROFILE_USERNAME_MAX = 32
_PROFILE_USERNAME_RE = re.compile(r'^[a-zA-Z0-9_.-]+$')


@api.patch('/auth/me', response_model=UserPublic)
async def update_me(
    data: ProfileUpdateIn = Body(...),
    user_id: str = Depends(get_current_active_user_id),
):
    """Update a user's editable profile fields (name + username).

    Validation:
      - name: 1..80 chars after trim
      - username: 3..32 chars, [a-zA-Z0-9_.-] only, unique
    Username uniqueness is enforced by a final upsert race-safe
    pre-check, and the response is the refreshed UserPublic.
    """
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    updates: dict = {}
    raw_name = data.name
    raw_username = data.username
    if raw_name is not None:
        if not isinstance(raw_name, str):
            raise HTTPException(400, 'name_must_be_string')
        name = raw_name.strip()
        if len(name) < PROFILE_NAME_MIN or len(name) > PROFILE_NAME_MAX:
            raise HTTPException(400, 'name_length_out_of_range')
        updates['name'] = name
    if raw_username is not None:
        if not isinstance(raw_username, str):
            raise HTTPException(400, 'username_must_be_string')
        username = raw_username.strip()
        if not _PROFILE_USERNAME_RE.fullmatch(username):
            raise HTTPException(400, 'username_invalid_characters')
        if len(username) < PROFILE_USERNAME_MIN or len(username) > PROFILE_USERNAME_MAX:
            raise HTTPException(400, 'username_length_out_of_range')
        # Only check uniqueness if it actually changes (case-insensitive
        # because the existing signup flow does not lowercase).
        if username.lower() != str(u.get('username') or '').lower():
            existing = await db.users.find_one({
                'username': {'$regex': f'^{re.escape(username)}$', '$options': 'i'},
                'id': {'$ne': user_id},
            })
            if existing:
                raise HTTPException(409, 'username_already_taken')
        updates['username'] = username
    if not updates:
        # Nothing to do — return the current shape so the client gets
        # a consistent UserPublic without thinking it's an error.
        u = await _reconcile_user_instagram_flags(user_id, u) or u
        return _public_user(u)
    updates['updated_at'] = datetime.utcnow()
    await db.users.update_one({'id': user_id}, {'$set': updates})
    # Phase 2.18J cache hygiene: profile fields appear inside the
    # dashboard summary's user identity block, so wipe the snapshot.
    try:
        await invalidate_dashboard_summary(user_id)
    except Exception:
        pass
    logger.info(
        'profile_updated user_id=%s fields=%s',
        user_id, sorted([k for k in updates.keys() if k != 'updated_at']),
    )
    refreshed = await db.users.find_one({'id': user_id})
    refreshed = await _reconcile_user_instagram_flags(user_id, refreshed) or refreshed
    return _public_user(refreshed)


# Phase 2.18V: notification preferences. Critical security mail
# (password reset, plan change, account suspended) is OUT OF scope
# here — those bypass user preferences. This endpoint only governs
# the opt-in/transactional surface.
_NOTIFICATION_DEFAULTS = {
    'email': True,    # Activity digest / new automation events
    'push': False,    # Browser push, off by default
    'weekly': False,  # Weekly summary, off by default
}


def _notification_preferences_payload(row: Optional[dict]) -> dict:
    row = row or {}
    return {
        key: bool(row.get(key, default))
        for key, default in _NOTIFICATION_DEFAULTS.items()
    }


@api.get('/notifications/preferences')
async def get_notification_preferences(
    user_id: str = Depends(get_current_active_user_id),
):
    """Return the caller's notification preferences. Returns the
    documented defaults if the user has never saved any."""
    row = await db.user_notification_preferences.find_one({'user_id': user_id})
    return {
        'preferences': _notification_preferences_payload(row),
        'defaults': dict(_NOTIFICATION_DEFAULTS),
    }


@api.patch('/notifications/preferences')
async def update_notification_preferences(
    data: NotificationPreferencesIn = Body(...),
    user_id: str = Depends(get_current_active_user_id),
):
    """Patch — only the keys the caller sends are written. Unknown
    keys are ignored. Idempotent."""
    raw = data.model_dump(exclude_none=True) if hasattr(data, 'model_dump') else data.dict(exclude_none=True)
    accepted: dict = {}
    for key in _NOTIFICATION_DEFAULTS.keys():
        if key in raw:
            accepted[key] = bool(raw[key])
    if not accepted:
        # Nothing to update — return current view.
        row = await db.user_notification_preferences.find_one({'user_id': user_id})
        return {
            'preferences': _notification_preferences_payload(row),
            'defaults': dict(_NOTIFICATION_DEFAULTS),
        }
    now = datetime.utcnow()
    await db.user_notification_preferences.update_one(
        {'user_id': user_id},
        {
            '$set': {**accepted, 'user_id': user_id, 'updated_at': now},
            '$setOnInsert': {
                'id': secrets.token_urlsafe(12),
                'created_at': now,
            },
        },
        upsert=True,
    )
    logger.info(
        'notification_preferences_updated user_id=%s keys=%s',
        user_id, sorted(accepted.keys()),
    )
    row = await db.user_notification_preferences.find_one({'user_id': user_id})
    return {
        'preferences': _notification_preferences_payload(row),
        'defaults': dict(_NOTIFICATION_DEFAULTS),
    }


# Phase 2.18I: single round-trip session bootstrap. The frontend used
# to need 4-5 sequential network round-trips after login (auth/me,
# dashboard/summary, automations/summary, instagram/accounts, plus
# admin overview for admins) before the dashboard could render real
# numbers. This endpoint collapses all of them into one parallel call
# so by the time the user sees /app, every important payload is
# already in the response and the client seeds its SWR cache directly
# — every page renders from a hot cache on first visit.
async def _reconcile_user_instagram_flags(user_id: str, u: Optional[dict] = None) -> Optional[dict]:
    """Phase 2.18T: keep users.instagramConnected/instagramHandle in sync
    with the authoritative instagram_accounts collection.

    Live tester pass found a case where:
      - users.instagramConnected = True (legacy)
      - 0 instagram_accounts rows with connectionValid=True for this user
    Result: Topbar shows 'Connected' badge while Admin Users shows
    IG=0 and Settings shows 'No account connected'. The fields drift
    because older code paths set users.instagramConnected without
    also writing to instagram_accounts, and historical disconnects
    cleared the latter without clearing the former.

    This helper is the one-shot reconciler. Cheap to run on every
    authenticated read:
      - 1 count_documents
      - 0 or 1 update_one (only when actually drifted)
    It is idempotent — a clean user-doc is left untouched.

    Returns the (possibly updated) users document so callers can avoid
    a second find_one round-trip.
    """
    if not user_id:
        return u
    if u is None:
        u = await db.users.find_one({'id': user_id})
    if not u:
        return None
    try:
        valid_count = await db.instagram_accounts.count_documents({
            '$or': [{'userId': user_id}, {'user_id': user_id}],
            'connectionValid': True,
            'isActive': {'$ne': False},
        })
    except Exception:
        return u
    legacy_connected = bool(
        u.get('instagramConnected')
        or u.get('instagram_connection_valid')
        or u.get('instagramConnectionValid')
    )
    drift_to_disconnected = legacy_connected and valid_count == 0
    if not drift_to_disconnected:
        return u
    # The user document still claims connected, but there is no live
    # IG row backing it. Authoritative source is instagram_accounts —
    # clear the stale legacy flags so Topbar / Sidebar / Admin agree.
    now = datetime.utcnow()
    await db.users.update_one(
        {'id': user_id},
        {
            '$set': {
                'instagramConnected': False,
                'instagram_connection_valid': False,
                'instagramConnectionValid': False,
                'instagram_connection_blocker': 'reconciled_no_live_account',
                'instagramHandle': None,
                'active_instagram_account_id': None,
                'updated_at': now,
            },
            '$unset': {
                'meta_access_token': '',
                'ig_user_id': '',
                'instagram_account_type': '',
                'instagram_graph_me_id': '',
                'instagram_graph_me_user_id': '',
                'instagram_profile_picture_url': '',
            },
        },
    )
    try:
        await invalidate_dashboard_summary(user_id)
    except Exception:
        pass
    logger.info(
        'instagram_flags_reconciled_user_doc user_id=%s valid_count=%s',
        user_id, valid_count,
    )
    # Re-read so the caller sees the corrected state without another
    # round-trip later.
    refreshed = await db.users.find_one({'id': user_id})
    return refreshed or u


@api.get('/auth/bootstrap')
async def auth_bootstrap(user_id: str = Depends(get_current_active_user_id)):
    started = datetime.utcnow()
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    # Phase 2.18T: reconcile stale users.instagramConnected flags
    # against the authoritative instagram_accounts table BEFORE
    # building the bootstrap payload so the Topbar / Sidebar /
    # dashboard counters all see the same state.
    u = await _reconcile_user_instagram_flags(user_id, u) or u

    # Resolve the active Instagram account once and reuse it across all
    # the data probes below so we don't pay for that lookup five times.
    try:
        account = await getActiveInstagramAccount(user_id)
    except HTTPException as e:
        if e.status_code != 400:
            raise
        account = None

    # admin? compute once.
    try:
        role, _bootstrap_owner = await _resolve_admin_role(u)
        is_admin = _admin_roles.is_admin_role(role)
    except Exception:
        role = None
        is_admin = False

    # Build the parallel task list. The dashboard summary uses the
    # read-through cache (snapshot table) so this stays cheap even
    # under load.
    async def _safe(coro):
        try:
            return await coro
        except Exception as e:
            return {'error': str(e)[:120]}

    async def _safe_dashboard():
        try:
            data, _meta = await _get_dashboard_summary_readthrough(user_id, account)
            return data
        except Exception as e:
            return {'error': str(e)[:120]}

    async def _safe_automations():
        # Phase 2.18Q: use the same _dashboard_scoped_docs semantics
        # the dashboard's automations counter uses so bootstrap, the
        # /automations/summary endpoint, and the dashboard agree on
        # which rows exist.
        try:
            include_unscoped = await _dashboard_include_unscoped(user_id)
            rows = await _dashboard_scoped_docs('automations', user_id, account, include_unscoped, 500)
            rows.sort(
                key=lambda r: (
                    r.get('updated') or r.get('updatedAt')
                    or r.get('created') or r.get('createdAt') or datetime.min
                ),
                reverse=True,
            )
            missing = [
                r for r in rows
                if r.get('media_id')
                and not ((r.get('media_preview') or {}).get('thumbnail_url'))
                and not ((r.get('media_preview') or {}).get('media_url'))
            ][:12]
            if missing and account is not None:
                refreshed = await asyncio.gather(
                    *(_backfill_automation_media_preview(r, account) for r in missing)
                )
                by_id = {r.get('id'): r for r in refreshed}
                rows = [by_id.get(r.get('id'), r) for r in rows]
            items = [_automation_summary_row(row) for row in rows]
            return {'items': items, 'count': len(items),
                    'lastUpdatedAt': datetime.utcnow().isoformat()}
        except Exception as e:
            return {'items': [], 'count': 0, 'error': str(e)[:120],
                    'lastUpdatedAt': datetime.utcnow().isoformat()}

    async def _safe_accounts():
        try:
            await _sync_user_instagram_account_doc(u)
            active_id = (account or {}).get('id') or u.get('active_instagram_account_id') or ''
            rows = await db.instagram_accounts.find({'userId': user_id}).sort('updatedAt', -1).to_list(100)
            missing = [r for r in rows
                       if not (r.get('profilePictureUrl') or r.get('profile_picture_url'))]
            if missing:
                refreshed = await asyncio.gather(*(_backfill_account_profile_picture(r) for r in missing))
                by_id = {r.get('id'): r for r in refreshed}
                rows = [by_id.get(r.get('id'), r) for r in rows]
            return {
                'accounts': [_instagram_account_public_row(row, active_id) for row in rows],
                'activeInstagramAccountId': active_id or None,
                'count': len(rows),
            }
        except Exception as e:
            return {'accounts': [], 'count': 0, 'error': str(e)[:120]}

    async def _safe_admin_overview():
        if not is_admin:
            return None
        try:
            # Reuse the same code path as /api/admin/overview by calling
            # the read paths in parallel via the existing helper.
            now = datetime.utcnow()
            today_start = datetime(now.year, now.month, now.day)
            seven_days = today_start - timedelta(days=7)
            thirty_days = today_start - timedelta(days=30)
            month = _usage_month(now)

            async def _safe_usage_totals():
                totals = {field: 0 for field in USAGE_COUNTER_FIELDS}
                try:
                    cursor = db.monthly_usage.find(_monthly_usage_user_scope_query(month))
                    async for row in cursor:
                        for field in USAGE_COUNTER_FIELDS:
                            totals[field] += int(row.get(field) or 0)
                except Exception:
                    pass
                return totals

            async def _safe_plan_distribution():
                try:
                    return await _effective_plan_distribution()
                except Exception:
                    return {key: 0 for key in _plans.PLAN_KEYS}

            (
                total_users, active_users, suspended_users, deleted_users,
                users_today, users_7d, users_30d,
                total_ig, connected_ig, total_autos, active_autos,
                usage_totals, plan_distribution,
                plan_limited, retryable_failures, permanent_failures, queue_pending,
            ) = await asyncio.gather(
                db.users.count_documents({}),
                db.users.count_documents({'status': {'$nin': ['suspended', 'deleted']}}),
                db.users.count_documents({'status': 'suspended'}),
                db.users.count_documents({'status': 'deleted'}),
                db.users.count_documents({'created_at': {'$gte': today_start}}),
                db.users.count_documents({'created_at': {'$gte': seven_days}}),
                db.users.count_documents({'created_at': {'$gte': thirty_days}}),
                db.instagram_accounts.count_documents({}),
                db.instagram_accounts.count_documents({'connectionValid': True}),
                db.automations.count_documents({}),
                # Phase 2.18Q: align with _automation_active() — legacy rows
        # may have status missing and use enabled=true / isActive=true
        # instead, which the dashboard's per-user counter already
        # honors. Count them as active here too, otherwise the admin
        # overview undercounts and disagrees with the dashboard.
        db.automations.count_documents({'$or': [
            {'status': 'active'},
            {'status': {'$exists': False}, 'enabled': True},
            {'status': None, 'enabled': True},
            {'status': '', 'enabled': True},
        ]}),
                _safe_usage_totals(),
                _safe_plan_distribution(),
                db.comments.count_documents({'action_status': 'plan_limited'}),
                db.comments.count_documents({'action_status': 'failed_retryable'}),
                db.comments.count_documents({
                    'action_status': {'$in': ['failed_permanent', 'failed_retry_exhausted']},
                }),
                db.comments.count_documents({'queued': True}),
            )
            user_plan_rows = sum(plan_distribution.values())
            plan_distribution['free'] += max(0, total_users - user_plan_rows)
            return {
                'total_users': total_users,
                'active_users': active_users,
                'suspended_users': suspended_users,
                'deleted_users': deleted_users,
                'users_created_today': users_today,
                'users_created_7d': users_7d,
                'users_created_30d': users_30d,
                'total_instagram_accounts': total_ig,
                'connected_instagram_accounts': connected_ig,
                'total_automations': total_autos,
                'active_automations': active_autos,
                'event_month': month,
                'current_month_usage_totals': usage_totals,
                'plan_distribution': plan_distribution,
                'plan_limited_counts': plan_limited,
                'retryable_failures_count': retryable_failures,
                'permanent_failures_count': permanent_failures,
                'queue_pending_count': queue_pending,
            }
        except Exception as e:
            return {'error': str(e)[:120]}

    (
        dashboard_summary,
        automations_summary,
        instagram_accounts_payload,
        admin_overview_payload,
    ) = await asyncio.gather(
        _safe_dashboard(),
        _safe_automations(),
        _safe_accounts(),
        _safe_admin_overview(),
    )

    duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
    logger.info(
        'auth_bootstrap user_id=%s is_admin=%s durationMs=%s',
        user_id, is_admin, duration_ms,
    )

    return {
        'user': _public_user(u),
        'isAdmin': is_admin,
        'role': role,
        'dashboard_summary': dashboard_summary,
        'automations_summary': automations_summary,
        'instagram_accounts': instagram_accounts_payload,
        'admin_overview': admin_overview_payload,
        'bootstrap_duration_ms': duration_ms,
    }


@api.post('/auth/resend-verification')
async def resend_email_verification(body: dict = Body(...), request: Request = None):
    ip = _client_ip(request) if request is not None else 'unknown'
    email = _normalize_email_value((body or {}).get('email'))
    email_hash = _hash_identifier(email)
    if _rate_limited('email_verification_resend_ip', ip, limit=5, window_seconds=3600):
        raise HTTPException(429, 'Too many verification requests. Try again later.')
    if email_hash and _rate_limited('email_verification_resend_email', email_hash, limit=3, window_seconds=3600):
        raise HTTPException(429, 'Too many verification requests. Try again later.')
    # Unknown/verified accounts get the same generic success to prevent enumeration.
    generic = {'ok': True, 'status': 'sent_if_account_exists'}
    user = await _find_user_by_email(email)
    if not user or user.get('email_verified') or not _email_verification_required(user):
        return generic
    if not _email_verification_delivery_configured():
        raise HTTPException(503, 'email_verification_not_configured')
    issued = await _issue_email_verification(user, reason='resend')
    if not issued.get('sent'):
        raise HTTPException(503, 'email_verification_not_configured')
    return generic


async def _verify_email_token_value(token: str) -> dict:
    if not token or not isinstance(token, str) or len(token) > 256:
        raise HTTPException(400, 'invalid_email_verification_token')
    token_hash = _hash_email_verification_token(token)
    user = await db.users.find_one({'email_verification_token_hash': token_hash})
    if not user:
        raise HTTPException(400, 'invalid_email_verification_token')
    if user.get('email_verification_used_at'):
        raise HTTPException(400, 'email_verification_token_used')
    expires_at = user.get('email_verification_expires_at')
    if isinstance(expires_at, datetime) and expires_at < datetime.utcnow():
        raise HTTPException(400, 'email_verification_token_expired')
    now = datetime.utcnow()
    await db.users.update_one(
        {'id': user.get('id'), 'email_verification_token_hash': token_hash},
        {'$set': {
            'email_verified': True,
            'email_verification_required': False,
            'email_verified_at': now,
            'email_verification_used_at': now,
            'updated_at': now,
        }, '$unset': {
            'email_verification_token_hash': '',
        }},
    )
    await _increment_user_session_version(user.get('id'), reason='email_verified')
    return {'ok': True, 'status': 'email_verified'}


@api.post('/auth/verify-email')
async def verify_email(body: dict = Body(...)):
    return await _verify_email_token_value((body or {}).get('token'))


@api.get('/auth/verify-email')
async def verify_email_get(token: str = Query('')):
    return await _verify_email_token_value(token)


# ---------------- Phase 2.14 password reset ----------------
# Mirrors the email-verification token primitive: cryptographically
# random token, only the HMAC-SHA256 hash is stored on the user row,
# single-use, expiring, generic responses so no user enumeration.
# Reuses EMAIL_VERIFICATION_WEBHOOK_URL transport with a distinct
# template name. Raw token NEVER logged, returned, or stored.

GENERIC_FORGOT_PASSWORD_RESPONSE = {'ok': True, 'status': 'sent_if_account_exists'}


def _hash_password_reset_token(token: str) -> str:
    return hmac.new(
        JWT_SECRET.encode('utf-8'),
        str(token or '').encode('utf-8'),
        hashlib.sha256,
    ).hexdigest()


def _password_reset_link(token: str) -> str:
    base = (FRONTEND_URL or '').rstrip('/')
    return f"{base}/reset-password?token={token}"


async def _deliver_password_reset(user: dict, token: str) -> bool:
    """Send the reset link via the same webhook the verification flow uses.
    Never logs the token. Returns True iff webhook responded 2xx."""
    user_id = user.get('id')
    if not EMAIL_VERIFICATION_WEBHOOK_URL:
        logger.warning(
            'password_reset_email_delivery_skipped user_id=%s reason=missing_env env=EMAIL_VERIFICATION_WEBHOOK_URL',
            user_id,
        )
        return False
    reset_link = _password_reset_link(token)
    payload = {
        'to': _normalize_email_value(user.get('email')),
        'template': PASSWORD_RESET_EMAIL_TEMPLATE,
        # Keep reset_url for the original contract, and include common
        # aliases so webhook/template adapters do not silently drop the link.
        'reset_url': reset_link,
        'resetUrl': reset_link,
        'url': reset_link,
        'link': reset_link,
        'app_name': 'mychat',
        'expires_in_minutes': PASSWORD_RESET_TOKEN_TTL_HOURS * 60,
    }
    headers = {'content-type': 'application/json'}
    if EMAIL_VERIFICATION_WEBHOOK_TOKEN:
        headers['authorization'] = f'Bearer {EMAIL_VERIFICATION_WEBHOOK_TOKEN}'
    try:
        logger.info(
            'password_reset_email_delivery_attempt user_id=%s provider=EMAIL_VERIFICATION_WEBHOOK_URL template=%s',
            user_id, PASSWORD_RESET_EMAIL_TEMPLATE,
        )
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                EMAIL_VERIFICATION_WEBHOOK_URL, json=payload, headers=headers,
            )
        ok = 200 <= response.status_code < 300
        if ok:
            logger.info(
                'password_reset_email_delivery_success user_id=%s status_code=%s template=%s',
                user_id, response.status_code, PASSWORD_RESET_EMAIL_TEMPLATE,
            )
        else:
            logger.warning(
                'password_reset_email_delivery_failed user_id=%s reason=non_2xx status_code=%s template=%s',
                user_id, response.status_code, PASSWORD_RESET_EMAIL_TEMPLATE,
            )
        return ok
    except Exception as exc:
        # Never echo the request body — token is in there.
        logger.warning(
            'password_reset_email_delivery_exception user_id=%s reason=%s template=%s',
            user_id, type(exc).__name__, PASSWORD_RESET_EMAIL_TEMPLATE,
        )
        return False


async def _issue_password_reset(user: dict) -> dict:
    """Generate, hash, and persist a one-shot reset token. Never returns
    the raw token to anyone except the email-delivery layer."""
    token = secrets.token_urlsafe(32)
    now = datetime.utcnow()
    expires_at = now + timedelta(hours=PASSWORD_RESET_TOKEN_TTL_HOURS)
    await db.users.update_one(
        {'id': user.get('id')},
        {'$set': {
            'password_reset_token_hash': _hash_password_reset_token(token),
            'password_reset_sent_at': now,
            'password_reset_expires_at': expires_at,
            'password_reset_used_at': None,
            'updated_at': now,
        }},
    )
    sent = await _deliver_password_reset(user, token)
    return {'sent': sent, 'expires_at': expires_at}


class ForgotPasswordIn(BaseModel):
    email: str


@api.post('/auth/forgot-password')
async def auth_forgot_password(
    data: ForgotPasswordIn = Body(...),
    request: Request = None,
):
    """Start a password reset. Always returns the same generic response so
    callers cannot enumerate registered emails. Rate-limited per IP and
    per email-hash, mirroring /auth/resend-verification."""
    ip = _client_ip(request) if request is not None else 'unknown'
    if _rate_limited('password_reset_request_ip', ip, limit=5, window_seconds=3600):
        # Generic — 429 is acceptable; the response shape never reveals
        # whether the email is known.
        raise HTTPException(429, 'Too many password reset requests. Try again later.')
    email = _normalize_email_value(data.email)
    email_hash = _hash_identifier(email)
    if email_hash and _rate_limited('password_reset_request_email', email_hash,
                                    limit=3, window_seconds=3600):
        raise HTTPException(429, 'Too many password reset requests. Try again later.')
    if not email:
        return GENERIC_FORGOT_PASSWORD_RESPONSE
    user = await _find_user_by_email(email)
    if not user:
        # Unknown account — generic success, no enumeration. Sanitized log.
        logger.info('password_reset_request_unknown email_hash=%s',
                    (email_hash or '')[:12])
        return GENERIC_FORGOT_PASSWORD_RESPONSE
    if not user.get('password_hash'):
        # Google-only account: no password to reset. Same generic response.
        # Documented behaviour: users in this state should sign in via
        # /auth/google and set a password from Settings later (out of
        # scope for this phase).
        logger.info('password_reset_request_no_password_user_id=%s', user.get('id'))
        return GENERIC_FORGOT_PASSWORD_RESPONSE
    issued = await _issue_password_reset(user)
    logger.info(
        'password_reset_issued user_id=%s sent=%s',
        user.get('id'), bool(issued.get('sent')),
    )
    return GENERIC_FORGOT_PASSWORD_RESPONSE


class ResetPasswordIn(BaseModel):
    token: str
    new_password: str


@api.post('/auth/reset-password')
async def auth_reset_password(data: ResetPasswordIn = Body(...)):
    """Consume a one-shot reset token and set a new password.

    Token verification rules:
      - lookup by hashed token only (raw never compared)
      - not previously used
      - not expired
    On success:
      - hash + store new password
      - clear token fields
      - increment session_version (all old JWTs become 401 session_revoked)
    """
    token = data.token if isinstance(data.token, str) else ''
    if not token or len(token) > 256:
        raise HTTPException(400, 'invalid_password_reset_token')
    new_password = _enforce_password_policy(data.new_password)
    token_hash = _hash_password_reset_token(token)
    user = await db.users.find_one({'password_reset_token_hash': token_hash})
    if not user:
        raise HTTPException(400, 'invalid_password_reset_token')
    if user.get('password_reset_used_at'):
        raise HTTPException(400, 'password_reset_token_used')
    expires_at = user.get('password_reset_expires_at')
    if isinstance(expires_at, datetime) and expires_at < datetime.utcnow():
        raise HTTPException(400, 'password_reset_token_expired')
    now = datetime.utcnow()
    new_hashed = hash_password(new_password)
    result = await db.users.update_one(
        {'id': user.get('id'), 'password_reset_token_hash': token_hash},
        {'$set': {
            'password_hash': new_hashed,
            'password_reset_used_at': now,
            'session_version': _user_session_version(user) + 1,
            'session_revoked_at': now,
            'session_revocation_reason': 'password_reset',
            'updated_at': now,
        }, '$unset': {
            'password_reset_token_hash': '',
        }},
    )
    if result.matched_count == 0:
        # Race: token was used between SELECT and UPDATE.
        raise HTTPException(400, 'password_reset_token_used')
    logger.info('password_reset_completed user_id=%s', user.get('id'))
    return {'ok': True, 'status': 'password_reset'}


# ---------------- Phase 2.7 Google Sign-In ----------------
# Login + signup via Google ID token. Email/password remains the
# primary auth flow; Google is purely additive. Issued JWT shape is
# identical to /auth/login so the frontend session bootstrap is the
# same code path.

GOOGLE_AUTH_PROVIDER_KEY = 'google'


def _normalize_email_value(email: Optional[str]) -> str:
    """Canonical email identity for lookup and admin-member matching.

    We intentionally do not strip plus-address tags here. Plus-addressing
    remains a distinct mailbox identity for now; this only closes casing and
    surrounding-whitespace duplicates without silently merging accounts.
    """
    return str(email or '').strip().lower()


async def _find_user_by_email(email: Optional[str]) -> Optional[dict]:
    normalized = _normalize_email_value(email)
    if not normalized:
        return None
    user = await db.users.find_one({'normalized_email': normalized})
    if user:
        return user
    user = await db.users.find_one({'email': normalized})
    if user:
        return user
    # Legacy rows may have mixed-case email and no normalized_email field.
    return await db.users.find_one({
        'email': {'$regex': f'^{re.escape(normalized)}$', '$options': 'i'}
    })


@api.get('/auth/google/config')
async def google_auth_config():
    """Public Google Sign-In browser config.

    The OAuth web client ID is intentionally public; it is required by
    Google Identity Services in the browser. No credentials, ID tokens,
    access tokens, or decoded Google payloads are returned here.
    """
    return {
        'enabled': bool(GOOGLE_CLIENT_ID),
        'client_id': GOOGLE_CLIENT_ID or '',
    }


def verify_google_credential(credential: str) -> dict:
    """Verify a Google ID token. Returns the claims dict.

    Raises HTTPException on any failure. NEVER logs the credential or
    the decoded payload — only sanitized failure reasons.

    The implementation lazy-imports `google.oauth2.id_token` so the
    server boots fine without the SDK; tests monkeypatch this whole
    function to avoid any network call.
    """
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(503, 'google_auth_not_configured')
    if not credential or not isinstance(credential, str):
        raise HTTPException(400, 'invalid_google_credential')
    try:
        from google.oauth2 import id_token as _gid  # type: ignore
        from google.auth.transport import requests as _greq  # type: ignore
    except Exception:
        logger.warning('google_auth_sdk_missing')
        raise HTTPException(503, 'google_auth_sdk_not_installed')
    try:
        info = _gid.verify_oauth2_token(
            credential, _greq.Request(), GOOGLE_CLIENT_ID
        )
    except ValueError as e:
        # google-auth raises ValueError for any verification failure:
        # bad signature, expired, wrong audience, malformed JWT.
        # Log only the class + sanitized prefix (no token contents).
        msg = str(e)
        reason = 'invalid_token'
        low = msg.lower()
        if 'expired' in low:
            reason = 'token_expired'
        elif 'audience' in low or 'aud' in low:
            reason = 'wrong_audience'
        elif 'issuer' in low:
            reason = 'wrong_issuer'
        logger.info('google_credential_verify_failed reason=%s', reason)
        raise HTTPException(401, f'google_credential_invalid:{reason}')
    if not isinstance(info, dict):
        raise HTTPException(401, 'google_credential_invalid:unexpected_payload')
    iss = (info.get('iss') or '').lower()
    if iss not in ('accounts.google.com', 'https://accounts.google.com'):
        raise HTTPException(401, 'google_credential_invalid:wrong_issuer')
    return info


def _google_claims_safe(claims: dict) -> dict:
    """Strict allow-list extraction. Drops anything else."""
    return {
        'sub': str(claims.get('sub') or ''),
        'email': _normalize_email_value(claims.get('email')),
        'email_verified': bool(claims.get('email_verified')),
        'name': claims.get('name'),
        'picture': claims.get('picture'),
    }


@api.post('/auth/google', response_model=AuthOut)
async def auth_google(data: dict = Body(...), request: Request = None):
    """Login or sign up via Google ID token.

    Flow:
      A. existing google_sub -> log that user in.
      B. existing email      -> link google_sub to that user (only if
                                Google reports email_verified=true).
      C. otherwise           -> create new user, set google_sub.

    Always returns the same {token, user} shape as /auth/login.
    """
    ip = _client_ip(request) if request is not None else 'unknown'
    if _rate_limited('login', ip,
                     limit=RATE_LIMIT_LOGIN_PER_MIN, window_seconds=60):
        logger.warning('rate_limit_hit bucket=google_auth ip=%s', ip)
        raise HTTPException(429, 'Too many login attempts. Try again in a minute.')
    credential = (data or {}).get('credential')
    claims = verify_google_credential(credential)
    safe = _google_claims_safe(claims)
    if not safe['sub']:
        raise HTTPException(401, 'google_credential_invalid:missing_sub')
    if not safe['email']:
        raise HTTPException(401, 'google_credential_invalid:missing_email')
    if not safe['email_verified']:
        raise HTTPException(403, 'google_email_not_verified')

    now = datetime.utcnow()
    is_new_user = False

    by_sub = await db.users.find_one({'google_sub': safe['sub']})
    by_email = await _find_user_by_email(safe['email'])

    if by_sub and by_email and by_sub.get('id') != by_email.get('id'):
        # Different user_id rows for sub vs email — never auto-merge.
        logger.warning(
            'google_account_conflict_cross_user sub_user_id=%s email_user_id=%s',
            by_sub.get('id'), by_email.get('id'),
        )
        raise HTTPException(409, 'google_account_conflict')

    if by_sub:
        user = by_sub
        # Already linked. Refresh non-secret display fields if present.
        update_set = {'updated_at': now, 'last_seen_at': now}
        if safe.get('name') and not user.get('name'):
            update_set['name'] = safe['name']
        if safe.get('picture') and not user.get('avatar'):
            update_set['avatar'] = safe['picture']
        await db.users.update_one({'id': user['id']}, {'$set': update_set})
    elif by_email:
        # Link path. Preserve existing user_id, plan, automations,
        # admin role, Instagram accounts.
        user = by_email
        existing_sub = user.get('google_sub')
        if existing_sub and existing_sub != safe['sub']:
            logger.warning(
                'google_account_already_linked user_id=%s', user.get('id'),
            )
            raise HTTPException(409, 'google_account_already_linked')
        providers = user.get('linked_providers') or []
        if GOOGLE_AUTH_PROVIDER_KEY not in providers:
            providers = providers + [GOOGLE_AUTH_PROVIDER_KEY]
        await db.users.update_one(
            {'id': user['id']},
            {'$inc': {'session_version': 1}, '$set': {
                'google_sub': safe['sub'],
                'normalized_email': safe['email'],
                'email_verified': True,
                'email_verification_required': False,
                'email_verified_at': now,
                'linked_providers': providers,
                'updated_at': now,
                'last_seen_at': now,
                **({'avatar': safe['picture']} if safe.get('picture') and not user.get('avatar') else {}),
                **({'name': safe['name']} if safe.get('name') and not user.get('name') else {}),
            }, '$unset': {'email_verification_token_hash': ''}},
        )
        user = await db.users.find_one({'id': user['id']}) or user
    else:
        # New user path. Pick a username from the email local part.
        import uuid
        local = safe['email'].split('@')[0] if '@' in safe['email'] else safe['email']
        base_username = ''.join(ch for ch in local if ch.isalnum() or ch == '_')[:24] or 'user'
        username = base_username
        suffix = 0
        while await db.users.find_one({'username': username}):
            suffix += 1
            username = f'{base_username}{suffix}'[:32]
        user_id = str(uuid.uuid4())
        user = {
            'id': user_id,
            'username': username,
            'email': safe['email'],
            'normalized_email': safe['email'],
            'name': safe.get('name') or base_username.capitalize(),
            'password_hash': None,
            'avatar': safe.get('picture') or f'https://i.pravatar.cc/150?u={username}',
            'instagramConnected': False,
            'instagramHandle': None,
            'created': now,
            'created_at': now,
            'updated_at': now,
            'google_sub': safe['sub'],
            'email_verified': True,
            'email_verification_required': False,
            'email_verified_at': now,
            'session_version': 0,
            'auth_provider': GOOGLE_AUTH_PROVIDER_KEY,
            'linked_providers': [GOOGLE_AUTH_PROVIDER_KEY],
        }
        await db.users.insert_one(user)
        await _seed_user(user_id)
        is_new_user = True

    # Phase 2.8: block Google login for suspended/deleted users.
    _ensure_user_active(user)
    # Sanitized log line: id only, no email or token contents.
    logger.info(
        'google_auth_success user_id=%s new_user=%s',
        user.get('id'), is_new_user,
    )
    return AuthOut(token=create_token(user['id'], session_version=_user_session_version(user)), user=_public_user(user))


# ---------------- automations ----------------
def _automation_summary_row(auto: dict) -> dict:
    media_preview = auto.get('media_preview') or {}
    post_scope = auto.get('post_scope')
    if not post_scope:
        trigger = str(auto.get('trigger') or '').lower()
        if trigger == 'comment:any':
            post_scope = 'any'
        elif trigger == 'comment:latest' or auto.get('latest'):
            post_scope = 'next'
        else:
            post_scope = 'specific'
    dm_text = _automation_dm_text_for_diagnostics(auto)
    public_replies = _automation_public_reply_texts(auto)
    status = auto.get('status') or ('active' if auto.get('enabled') else 'draft')
    updated = auto.get('updatedAt') or auto.get('updated') or auto.get('createdAt') or auto.get('created')
    created = auto.get('createdAt') or auto.get('created')
    return {
        'id': auto.get('id'),
        'automation_id': auto.get('id'),
        'name': auto.get('name') or 'Untitled automation',
        'active': str(status or '').lower() == 'active',
        'status': status,
        'scope': post_scope,
        'post_scope': post_scope,
        'selected_media_id': auto.get('media_id') or '',
        'media_id': auto.get('media_id') or '',
        'selected_media_label': str(media_preview.get('caption') or '')[:80],
        'media_preview': {
            'caption': str(media_preview.get('caption') or '')[:120],
            'thumbnail_url': media_preview.get('thumbnail_url') or media_preview.get('media_url') or '',
            'media_type': media_preview.get('media_type') or '',
        },
        'trigger_type': auto.get('trigger') or 'comment',
        'trigger': auto.get('trigger') or 'comment',
        'match': auto.get('match') or ('keyword' if auto.get('keyword') else 'any'),
        'keyword': auto.get('keyword') or '',
        'mode': auto.get('mode') or ('reply_and_dm' if dm_text else 'reply_only'),
        'latest': bool(auto.get('latest')),
        'has_public_reply': bool(public_replies),
        'reply_under_post': bool(public_replies),
        'has_dm': bool(dm_text),
        'has_follow_gate': bool(auto.get('follow_request_enabled')),
        'process_existing_unreplied_comments': bool(auto.get('process_existing_unreplied_comments')),
        'processExistingComments': bool(auto.get('processExistingComments')),
        'created_at': created.isoformat() if isinstance(created, datetime) else created,
        'updated_at': updated.isoformat() if isinstance(updated, datetime) else updated,
        'createdAt': created.isoformat() if isinstance(created, datetime) else created,
        'updatedAt': updated.isoformat() if isinstance(updated, datetime) else updated,
        'activationStartedAt': (
            auto.get('activationStartedAt').isoformat()
            if isinstance(auto.get('activationStartedAt'), datetime)
            else auto.get('activationStartedAt')
        ),
        'last_run_at': (
            auto.get('last_run_at') or auto.get('lastRunAt') or auto.get('lastProcessedAt')
        ),
        'counters': {
            'comments_processed': int(auto.get('comments_processed') or 0),
            'replies_sent': int(auto.get('public_replies_sent') or auto.get('replies_sent') or 0),
            'dms_sent': int(auto.get('dms_sent') or 0),
            'failures': int(auto.get('failures') or 0),
        },
        'sent': int(auto.get('sent') or 0),
    }


async def _backfill_automation_media_preview(auto: dict, account: dict) -> dict:
    """Phase 2.18H: lazy-fetch missing media_preview.thumbnail_url for
    automations bound to a specific media_id. Older automations
    created before the wizard cached the preview show a blank
    placeholder otherwise. Each fetch is one Instagram Graph call and
    we run them in parallel via gather upstream."""
    if not isinstance(auto, dict):
        return auto
    media_id = auto.get('media_id') or ''
    if not media_id:
        return auto
    preview = auto.get('media_preview') or {}
    if preview.get('thumbnail_url') or preview.get('media_url'):
        return auto
    token = (account or {}).get('accessToken') or ''
    if not token:
        return auto
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(
                f'https://graph.instagram.com/{media_id}',
                params={
                    'access_token': token,
                    'fields': 'id,caption,media_type,media_url,thumbnail_url,permalink',
                },
            )
            if r.status_code != 200:
                return auto
            body = r.json() or {}
            thumbnail = body.get('thumbnail_url') or body.get('media_url') or ''
            if not thumbnail:
                return auto
            new_preview = {
                'caption': body.get('caption') or preview.get('caption') or '',
                'thumbnail_url': thumbnail,
                'media_url': body.get('media_url') or thumbnail,
                'media_type': body.get('media_type') or preview.get('media_type') or '',
                'permalink': body.get('permalink') or preview.get('permalink') or '',
            }
            await db.automations.update_one(
                {'id': auto.get('id')},
                {'$set': {'media_preview': new_preview, 'updated': datetime.utcnow()}},
            )
            return {**auto, 'media_preview': new_preview}
    except Exception:
        # Never fail the parent listing because a media preview backfill
        # could not reach Instagram Graph.
        pass
    return auto


@api.get('/automations/summary')
async def list_automations_summary(user_id: str = Depends(get_current_active_user_id)):
    started = datetime.utcnow()
    # Phase 2.18Q: align scoping with /dashboard/summary so the
    # 'Active Automations' counter on the dashboard never disagrees
    # with the row count on /app/automations. The previous behaviour
    # was: dashboard used _dashboard_scoped_docs (account-scoped +
    # unscoped legacy rows when the workspace has <=1 IG account)
    # while this endpoint used _account_scoped_query only. An
    # automation with post_scope='any' that was saved without an IG
    # account id (legacy data or a workspace that disconnected) was
    # therefore visible on the dashboard count but missing from the
    # actual list — the inconsistency the live tester pass hit.
    try:
        account = await getActiveInstagramAccount(user_id)
    except HTTPException as e:
        if e.status_code != 400:
            raise
        account = None
    include_unscoped = await _dashboard_include_unscoped(user_id)
    rows = await _dashboard_scoped_docs('automations', user_id, account, include_unscoped, 500)
    # Stable ordering: newest updated first.
    rows.sort(
        key=lambda r: (
            r.get('updated') or r.get('updatedAt')
            or r.get('created') or r.get('createdAt') or datetime.min
        ),
        reverse=True,
    )
    # Phase 2.18H: parallel-backfill missing media_preview thumbnails
    # for automations that target a specific post but were created
    # before the wizard cached the preview. Limit to 12 concurrent
    # Graph calls so a workspace with hundreds of legacy automations
    # does not stall the listing — anything beyond that keeps the
    # placeholder until next listing.
    missing = [
        r for r in rows
        if r.get('media_id')
        and not ((r.get('media_preview') or {}).get('thumbnail_url'))
        and not ((r.get('media_preview') or {}).get('media_url'))
    ][:12]
    if missing:
        refreshed = await asyncio.gather(*(_backfill_automation_media_preview(r, account) for r in missing))
        by_id = {r.get('id'): r for r in refreshed}
        rows = [by_id.get(r.get('id'), r) for r in rows]
    items = [_automation_summary_row(row) for row in rows]
    duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
    logger.info(
        'automations_summary_calculated user_id=%s instagramAccountId=%s count=%s durationMs=%s',
        user_id,
        account.get('instagramAccountId') or account.get('igUserId'),
        len(items),
        duration_ms,
    )
    return {
        'items': items,
        'count': len(items),
        'lastUpdatedAt': datetime.utcnow().isoformat(),
    }


@api.get('/automations')
async def list_automations(user_id: str = Depends(get_current_active_user_id)):
    try:
        account = await getActiveInstagramAccount(user_id)
    except HTTPException as e:
        if e.status_code == 400:
            return []
        raise
    cursor = db.automations.find(_account_scoped_query(user_id, account)).sort('updated', -1)
    return [_strip_mongo(d) for d in await cursor.to_list(1000)]


@api.post('/automations')
async def create_automation(data: AutomationIn, user_id: str = Depends(get_current_active_user_id)):
    automation_data = data.model_dump()
    for server_owned_field in ('user_id', 'instagramAccountId', 'igUserId', 'instagramUsername'):
        automation_data.pop(server_owned_field, None)
    account = await getActiveInstagramAccount(user_id)
    ctx = _instagram_context_from_account(account)
    if ctx['instagramAccountId']:
        automation_data.update(ctx)
    now = datetime.utcnow()
    # Handle None values for nodes and edges
    if automation_data.get('nodes') is None:
        automation_data['nodes'] = []
    if automation_data.get('edges') is None:
        automation_data['edges'] = []
    automation_data['createdAt'] = now
    automation_data['updatedAt'] = now
    historical_catchup = _normalize_historical_catchup_flag(automation_data)
    if not historical_catchup and (
        automation_data.get('process_existing_unreplied_comments')
        or automation_data.get('processExistingUnrepliedComments')
        or automation_data.get('processExistingComments')
    ):
        logger.info('process_existing_unreplied_comments_ignored_reason=broad_scope user_id=%s',
                    user_id)
    automation_data['processExistingComments'] = historical_catchup
    automation_data['process_existing_unreplied_comments'] = historical_catchup
    if _is_comment_automation_rule(automation_data):
        automation_data = _normalize_public_reply_for_persistence(automation_data)
        automation_data['activationStartedAt'] = now
    await _validate_automation_integrity_for_account(
        user_id,
        account,
        automation_data,
        require_connected=(automation_data.get('status') or '').lower() == 'active',
    )
    # Phase 2.2 plan enforcement: if creating directly as active, count it.
    if (automation_data.get('status') or '').lower() == 'active':
        plan = await get_user_plan(user_id)
        effective = await compute_effective_limits(user_id)
        max_active = effective.get('max_active_automations')
        if max_active is not None:
            active_count = await db.automations.count_documents({
                'user_id': user_id, 'status': 'active',
            })
            if active_count >= int(max_active):
                raise HTTPException(
                    402,
                    f'Plan {plan["plan_key"]} allows {max_active} active '
                    f'automation(s); save as draft or upgrade.',
                )
    a = Automation(user_id=user_id, **automation_data)
    await db.automations.insert_one(a.model_dump())
    await invalidate_dashboard_summary(
        user_id,
        instagram_account_id=automation_data.get('instagramAccountId') or ctx.get('instagramAccountId'),
    )
    await _safe_record_usage_event(
        user_id=user_id,
        event_type='automation_created',
        instagram_account_id=automation_data.get('instagramAccountId') or ctx.get('instagramAccountId'),
        automation_id=a.id,
        metadata={'status': automation_data.get('status') or 'draft'},
    )
    if (automation_data.get('status') or '').lower() == 'active':
        await _safe_record_usage_event(
            user_id=user_id,
            event_type='automation_activated',
            instagram_account_id=automation_data.get('instagramAccountId') or ctx.get('instagramAccountId'),
            automation_id=a.id,
            metadata={'source': 'create_automation'},
        )
    return a.model_dump()


@api.get('/automations/{aid}')
async def get_automation(aid: str, user_id: str = Depends(get_current_active_user_id)):
    account = await getActiveInstagramAccount(user_id)
    d = await db.automations.find_one({'id': aid, **_account_scoped_query(user_id, account)})
    if not d:
        raise HTTPException(404, 'Not found')
    return _strip_mongo(d)


@api.patch('/automations/{aid}')
async def patch_automation(aid: str, data: AutomationPatch, user_id: str = Depends(get_current_active_user_id)):
    update = {k: v for k, v in data.model_dump().items() if v is not None}
    for server_owned_field in ('user_id', 'instagramAccountId', 'igUserId', 'instagramUsername'):
        update.pop(server_owned_field, None)
    camel_aliases = {
        'followGateEnabled': 'follow_request_enabled',
        'followGateMessage': 'follow_request_message',
        'followGateButtonText': 'follow_request_button_text',
        'followGateConfirmationKeywords': 'follow_confirmation_keywords',
        'followGateExpiresAfterMinutes': 'follow_gate_expires_after_minutes',
        'followGateFallbackMessage': 'follow_gate_fallback_message',
    }
    if 'processExistingUnrepliedComments' in update and 'process_existing_unreplied_comments' not in update:
        update['process_existing_unreplied_comments'] = update.pop('processExistingUnrepliedComments')
    for src, dest in camel_aliases.items():
        if src in update:
            update[dest] = update.pop(src)
    follow_keys = {
        'follow_request_enabled',
        'follow_request_message',
        'follow_request_button_text',
        'follow_confirmation_keywords',
        'follow_gate_expires_after_minutes',
        'follow_gate_fallback_message',
        'verify_actual_follow',
        'follow_not_detected_message',
        'follow_verification_failed_message',
        'follow_retry_button_text',
        'follow_cooldown_message',
        'max_follow_verification_attempts',
    }
    if any(key in update for key in follow_keys):
        normalized_follow = _normalize_follow_gate_config(update)
        for key in follow_keys:
            if key in update or update.get('follow_request_enabled'):
                update[key] = normalized_follow[key]
        if update.get('follow_request_enabled'):
            if not update.get('follow_request_message'):
                raise HTTPException(400, 'Follow request message is required')
            if not update.get('follow_request_button_text'):
                raise HTTPException(400, 'Follow confirmation button text is required')
    account = await getActiveInstagramAccount(user_id)
    scoped = _account_scoped_query(user_id, account)
    existing = await db.automations.find_one({'id': aid, **scoped})
    if not existing:
        raise HTTPException(404, 'Not found')
    now = datetime.utcnow()
    update = _normalize_public_reply_for_persistence(update, existing)
    update['updated'] = now
    update['updatedAt'] = now
    # If any of the comment reply variations changed, rebuild the n_reply
    # node on the persisted flow graph so execute_flow's random.choice
    # actually picks from the updated list. Without this, edits to
    # comment_reply_2 / _3 would land on the document but never reach
    # the runtime, since the runtime reads from nodes[i].data.replies.
    reply_keys_changed = any(
        k in update for k in ('comment_reply', 'comment_reply_2', 'comment_reply_3')
    )
    if reply_keys_changed:
        merged_replies = [
            (update.get('comment_reply')
             if 'comment_reply' in update
             else existing.get('comment_reply') or ''),
            (update.get('comment_reply_2')
             if 'comment_reply_2' in update
             else existing.get('comment_reply_2') or ''),
            (update.get('comment_reply_3')
             if 'comment_reply_3' in update
             else existing.get('comment_reply_3') or ''),
        ]
        merged_replies = [str(r or '').strip() for r in merged_replies]
        merged_replies = [r for r in merged_replies if r]
        existing_nodes = list(existing.get('nodes') or [])
        rebuilt_nodes = []
        for node in existing_nodes:
            if node.get('id') == 'n_reply' and node.get('type') == 'reply_comment':
                node = {
                    **node,
                    'data': {
                        **(node.get('data') or {}),
                        'text': merged_replies[0] if merged_replies else '',
                        'replies': merged_replies,
                    },
                }
            rebuilt_nodes.append(node)
        # Only persist the rebuilt graph; if the FE is already sending
        # nodes/edges in the same patch, prefer their version.
        if 'nodes' not in update:
            update['nodes'] = rebuilt_nodes
    if update.get('reply_under_post') and _automation_public_reply_texts(update):
        update = _ensure_public_reply_node(update)
    prospective = {**existing, **update}
    status_reenabled = (
        update.get('status') == 'active' and existing.get('status') != 'active'
    )
    await _validate_automation_integrity_for_account(
        user_id,
        account,
        prospective,
        require_connected=status_reenabled,
    )
    historical_catchup = _normalize_historical_catchup_flag(prospective)
    if not historical_catchup and (
        update.get('process_existing_unreplied_comments')
        or update.get('processExistingComments')
    ):
        logger.info('process_existing_unreplied_comments_ignored_reason=broad_scope rule_id=%s user_id=%s',
                    aid, user_id)
    update['process_existing_unreplied_comments'] = historical_catchup
    update['processExistingComments'] = historical_catchup
    reset_fields = {
        'trigger', 'nodes', 'edges', 'match', 'keyword', 'media_id', 'latest',
        'mode', 'comment_reply', 'comment_reply_2', 'comment_reply_3',
        'dm_text', 'media_preview', 'keywords',
        'post_scope', 'reply_under_post', 'opening_dm_enabled',
        'opening_dm_text', 'opening_dm_button_text', 'link_dm_text',
        'link_button_text', 'link_url', 'conversionTrackingEnabled',
        'follow_request_enabled', 'follow_request_message',
        'follow_request_button_text', 'follow_confirmation_keywords',
        'follow_gate_expires_after_minutes', 'follow_gate_fallback_message',
        'verify_actual_follow', 'follow_not_detected_message',
        'follow_verification_failed_message', 'follow_retry_button_text',
        'follow_cooldown_message', 'max_follow_verification_attempts',
        'email_request_enabled', 'follow_up_enabled', 'follow_up_text',
    }
    # Phase 2.2 plan enforcement: block activation if at active-automation cap.
    if status_reenabled:
        plan = await get_user_plan(user_id)
        effective = await compute_effective_limits(user_id)
        max_active = effective.get('max_active_automations')
        if max_active is not None:
            active_count = await db.automations.count_documents({
                'user_id': user_id, 'status': 'active',
            })
            if active_count >= int(max_active):
                raise HTTPException(
                    402,
                    f'Plan {plan["plan_key"]} allows {max_active} active '
                    f'automation(s); deactivate one or upgrade.',
                )
    rule_shape_changed = any(field in update for field in reset_fields)
    if _is_comment_automation_rule(prospective) and (status_reenabled or rule_shape_changed):
        update['activationStartedAt'] = now
        logger.info('comment_rule_activation_reset rule_id=%s user_id=%s reason=%s',
                    aid, user_id, 'status_reenabled' if status_reenabled else 'rule_changed')
    res = await db.automations.update_one({'id': aid, **scoped}, {'$set': update})
    if res.matched_count == 0:
        raise HTTPException(404, 'Not found')
    if status_reenabled:
        await _safe_record_usage_event(
            user_id=user_id,
            event_type='automation_activated',
            instagram_account_id=existing.get('instagramAccountId') or account.get('instagramAccountId'),
            automation_id=aid,
            metadata={'source': 'patch_automation'},
        )
    await invalidate_dashboard_summary(
        user_id,
        instagram_account_id=existing.get('instagramAccountId') or (account or {}).get('instagramAccountId'),
    )
    d = await db.automations.find_one({'id': aid, **scoped})
    return _strip_mongo(d)


@api.delete('/automations/{aid}')
async def delete_automation(aid: str, user_id: str = Depends(get_current_active_user_id)):
    account = await getActiveInstagramAccount(user_id)
    res = await db.automations.delete_one({'id': aid, **_account_scoped_query(user_id, account)})
    if res.deleted_count == 0:
        raise HTTPException(404, 'Not found')
    await invalidate_dashboard_summary(user_id, instagram_account_id=account.get('instagramAccountId'))
    return {'ok': True}


@api.post('/automations/{aid}/duplicate')
async def duplicate_automation(aid: str, user_id: str = Depends(get_current_active_user_id)):
    account = await getActiveInstagramAccount(user_id)
    d = await db.automations.find_one({'id': aid, **_account_scoped_query(user_id, account)})
    if not d:
        raise HTTPException(404, 'Not found')
    import uuid
    copy = _strip_mongo({**d})
    copy['id'] = str(uuid.uuid4())
    copy['name'] = d['name'] + ' (Copy)'
    copy['status'] = 'draft'
    copy['sent'] = 0
    copy['clicks'] = 0
    now = datetime.utcnow()
    copy['created'] = now
    copy['updated'] = now
    copy['createdAt'] = now
    copy['updatedAt'] = now
    copy['processExistingComments'] = False
    copy['process_existing_unreplied_comments'] = False
    if _is_comment_automation_rule(copy):
        copy['activationStartedAt'] = now
    await db.automations.insert_one(copy)
    await invalidate_dashboard_summary(
        user_id,
        instagram_account_id=copy.get('instagramAccountId') or account.get('instagramAccountId'),
    )
    await _safe_record_usage_event(
        user_id=user_id,
        event_type='automation_created',
        instagram_account_id=copy.get('instagramAccountId') or account.get('instagramAccountId'),
        automation_id=copy.get('id'),
        metadata={'status': copy.get('status') or 'draft', 'source': 'duplicate'},
    )
    return _strip_mongo(copy)


@api.post('/automations/quick-comment-rule')
async def create_quick_comment_rule(
    data: dict = Body(...),
    user_id: str = Depends(get_current_active_user_id),
):
    """Create an automation that watches comments on an IG media (specific or latest).

    Body:
      media_id?: str
      latest?: bool                         - watch whatever is currently newest
      media_preview?: {caption, thumbnail_url, media_type}  - cached for display
      mode?: 'reply_and_dm' | 'reply_only'  - default 'reply_and_dm'
      match?: 'any' | 'keyword'             - default 'any'
      keyword?: str                         - required if match=='keyword'
      comment_reply: str                    - public reply text
      dm_text?: str                         - DM text (ignored when mode=='reply_only')
      name?: str
    """
    import uuid

    def _split_keywords(value) -> list:
        raw = value
        if isinstance(raw, list):
            parts = raw
        else:
            parts = str(raw or '').split(',')
        seen = set()
        out = []
        for item in parts:
            kw = str(item or '').strip()
            key = kw.lower()
            if kw and key not in seen:
                seen.add(key)
                out.append(kw)
        return out

    media_id = (data.get('media_id') or '').strip() or None
    post_scope = (data.get('post_scope') or '').strip().lower()
    if not post_scope:
        post_scope = 'latest' if bool(data.get('latest')) else 'specific'
    if post_scope not in ('specific', 'any', 'latest', 'next'):
        raise HTTPException(400, "post_scope must be 'specific', 'any', 'latest', or 'next'")
    latest = post_scope in ('latest', 'next') or bool(data.get('latest'))
    if post_scope == 'specific' and not media_id:
        raise HTTPException(400, 'Provide media_id for a specific post')

    mode = (data.get('mode') or 'reply_and_dm').strip()
    if mode not in ('reply_and_dm', 'reply_only'):
        raise HTTPException(400, "mode must be 'reply_and_dm' or 'reply_only'")

    match = (data.get('match') or 'any').strip()
    keywords = _split_keywords(data.get('keywords') if 'keywords' in data else data.get('keyword'))
    keyword = ', '.join(keywords)
    if match == 'keyword' and not keywords:
        raise HTTPException(400, 'keyword is required when match=keyword')
    if match not in ('any', 'keyword'):
        raise HTTPException(400, "match must be 'any' or 'keyword'")

    reply_under_post = bool(data.get('reply_under_post', True))
    comment_reply = (data.get('comment_reply') or '').strip()
    comment_reply_2 = (data.get('comment_reply_2') or '').strip()
    comment_reply_3 = (data.get('comment_reply_3') or '').strip()
    opening_dm_enabled = bool(data.get('opening_dm_enabled', mode == 'reply_and_dm'))
    opening_dm_text = (data.get('opening_dm_text') or data.get('dm_text') or '').strip()
    opening_dm_button_text = (data.get('opening_dm_button_text') or '').strip()
    link_dm_text = (data.get('link_dm_text') or '').strip()
    link_button_text = (data.get('link_button_text') or '').strip()
    link_url = (data.get('link_url') or '').strip()
    conversion_tracking_enabled = (
        bool(data.get('conversionTrackingEnabled'))
        if 'conversionTrackingEnabled' in data
        else bool(link_url)
    )
    follow_gate = _normalize_follow_gate_config(data)
    follow_request_enabled = follow_gate['follow_request_enabled']
    if follow_request_enabled:
        if not follow_gate['follow_request_message']:
            raise HTTPException(400, 'Follow request message is required')
        if not follow_gate['follow_request_button_text']:
            raise HTTPException(400, 'Follow confirmation button text is required')
    email_request_enabled = bool(data.get('email_request_enabled', False))
    follow_up_enabled = bool(data.get('follow_up_enabled', False))
    follow_up_text = (data.get('follow_up_text') or '').strip()
    has_dm_action = any([
        opening_dm_enabled and opening_dm_text,
        opening_dm_button_text,
        link_dm_text,
        link_button_text,
        link_url,
        follow_request_enabled,
        email_request_enabled,
        follow_up_enabled and follow_up_text,
    ])
    dm_text = ''
    if mode == 'reply_and_dm':
        dm_text = (
            opening_dm_text if opening_dm_enabled and opening_dm_text
            else (link_dm_text or link_url or '')
        ).strip()
    if mode == 'reply_and_dm' and has_dm_action and not dm_text:
        dm_text = 'Thanks for your comment.'
    process_existing_unreplied = bool(
        data.get('process_existing_unreplied_comments')
        or data.get('processExistingUnrepliedComments')
        or data.get('processExistingComments')
    ) and post_scope == 'specific' and bool(media_id)
    process_existing_comments = process_existing_unreplied
    if not process_existing_unreplied and (
        data.get('process_existing_unreplied_comments')
        or data.get('processExistingUnrepliedComments')
        or data.get('processExistingComments')
    ):
        logger.info('process_existing_unreplied_comments_ignored_reason=broad_scope user_id=%s post_scope=%s',
                    user_id, post_scope)
    if reply_under_post and not (comment_reply or comment_reply_2 or comment_reply_3):
        raise HTTPException(400, 'At least one comment reply is required')
    if mode == 'reply_and_dm' and not dm_text:
        dm_text = 'Thanks for your comment.'

    if mode == 'reply_and_dm' and not has_dm_action:
        dm_text = 'Thanks for your comment.'
    if not reply_under_post and not dm_text:
        raise HTTPException(400, 'Enable a public reply or a DM message')

    account = await getActiveInstagramAccount(user_id)
    ctx = _instagram_context_from_account(account)
    await _validate_automation_integrity_for_account(
        user_id,
        account,
        {
            **ctx,
            'post_scope': post_scope,
            'media_id': media_id,
            'latest': latest,
            'trigger': f'comment:{media_id}' if media_id else ('comment:latest' if latest else 'comment:any'),
            'status': 'active',
        },
        require_connected=True,
    )

    if post_scope == 'any':
        trigger = 'comment:any'
    elif latest:
        trigger = 'comment:latest'
    else:
        trigger = f'comment:{media_id}'
    preview = data.get('media_preview') or {}
    if post_scope == 'any':
        default_name = 'Any post - ' + (f'keywords "{keyword}"' if match == 'keyword' else 'any comment')
    elif latest:
        default_name = 'Latest post — ' + (f'keyword "{keyword}"' if match == 'keyword' else 'any comment')
    else:
        label = (preview.get('caption') or '')[:30] or (media_id[:10] if media_id else '')
        default_name = f'{label} — ' + (f'keyword "{keyword}"' if match == 'keyword' else 'any comment')
    name = (data.get('name') or default_name).strip()

    nodes = [{'id': 'n_trigger', 'type': 'trigger',
              'data': {'label': 'Comment trigger', 'trigger': trigger,
                       'match': match, 'keyword': keyword, 'keywords': keywords}}]
    edges = []
    prev = 'n_trigger'
    if reply_under_post and (comment_reply or comment_reply_2 or comment_reply_3):
        replies = [r for r in [comment_reply, comment_reply_2, comment_reply_3] if r]
        nodes.append({'id': 'n_reply', 'type': 'reply_comment',
                      'data': {'text': replies[0] if replies else '', 'replies': replies}})
        edges.append({'id': 'e1', 'source': prev, 'target': 'n_reply'})
        prev = 'n_reply'
    if dm_text:
        nodes.append({'id': 'n_dm', 'type': 'message', 'data': {
            'text': dm_text,
            'opening_dm_text': opening_dm_text,
            'opening_dm_button_text': opening_dm_button_text,
            'link_dm_text': link_dm_text,
            'link_button_text': link_button_text,
            'link_url': link_url,
            'conversionTrackingEnabled': conversion_tracking_enabled,
            'follow_request_enabled': follow_request_enabled,
            'follow_request_message': follow_gate['follow_request_message'],
            'follow_request_button_text': follow_gate['follow_request_button_text'],
            'follow_confirmation_keywords': follow_gate['follow_confirmation_keywords'],
            'follow_gate_expires_after_minutes': follow_gate['follow_gate_expires_after_minutes'],
            'follow_gate_fallback_message': follow_gate['follow_gate_fallback_message'],
            'verify_actual_follow': follow_gate['verify_actual_follow'],
            'follow_not_detected_message': follow_gate['follow_not_detected_message'],
            'follow_verification_failed_message': follow_gate['follow_verification_failed_message'],
            'follow_retry_button_text': follow_gate['follow_retry_button_text'],
            'follow_cooldown_message': follow_gate['follow_cooldown_message'],
            'max_follow_verification_attempts': follow_gate['max_follow_verification_attempts'],
            'email_request_enabled': email_request_enabled,
            'follow_up_enabled': follow_up_enabled,
            'follow_up_text': follow_up_text,
        }})
        edges.append({'id': f'e{len(edges)+1}', 'source': prev, 'target': 'n_dm'})

    # Phase 2.2 plan enforcement: this endpoint always creates active rules.
    plan = await get_user_plan(user_id)
    effective = await compute_effective_limits(user_id)
    max_active = effective.get('max_active_automations')
    if max_active is not None:
        active_count = await db.automations.count_documents({
            'user_id': user_id, 'status': 'active',
        })
        if active_count >= int(max_active):
            raise HTTPException(
                402,
                f'Plan {plan["plan_key"]} allows {max_active} active '
                f'automation(s); deactivate one or upgrade.',
            )

    now = datetime.utcnow()
    doc = {
        'id': str(uuid.uuid4()),
        'user_id': user_id,
        **({} if not ctx['instagramAccountId'] else ctx),
        'name': name,
        'status': 'active',
        'trigger': trigger,
        'match': match,
        'keyword': keyword,
        'keywords': keywords,
        'mode': mode,
        'post_scope': post_scope,
        'reply_under_post': reply_under_post,
        'comment_reply': comment_reply,
        'comment_reply_2': comment_reply_2,
        'comment_reply_3': comment_reply_3,
        'dm_text': dm_text,
        'opening_dm_enabled': opening_dm_enabled,
        'opening_dm_text': opening_dm_text,
        'opening_dm_button_text': opening_dm_button_text,
        'link_dm_text': link_dm_text,
        'link_button_text': link_button_text,
        'link_url': link_url,
        'conversionTrackingEnabled': conversion_tracking_enabled,
        'follow_request_enabled': follow_request_enabled,
        'follow_request_message': follow_gate['follow_request_message'],
        'follow_request_button_text': follow_gate['follow_request_button_text'],
        'follow_confirmation_keywords': follow_gate['follow_confirmation_keywords'],
        'follow_gate_expires_after_minutes': follow_gate['follow_gate_expires_after_minutes'],
        'follow_gate_fallback_message': follow_gate['follow_gate_fallback_message'],
        'verify_actual_follow': follow_gate['verify_actual_follow'],
        'follow_not_detected_message': follow_gate['follow_not_detected_message'],
        'follow_verification_failed_message': follow_gate['follow_verification_failed_message'],
        'follow_retry_button_text': follow_gate['follow_retry_button_text'],
        'follow_cooldown_message': follow_gate['follow_cooldown_message'],
        'max_follow_verification_attempts': follow_gate['max_follow_verification_attempts'],
        'email_request_enabled': email_request_enabled,
        'follow_up_enabled': follow_up_enabled,
        'follow_up_text': follow_up_text,
        'media_id': media_id,
        'latest': latest,
        'media_preview': preview,
        'nodes': nodes,
        'edges': edges,
        'sent': 0,
        'clicks': 0,
        'processExistingComments': process_existing_comments,
        'process_existing_unreplied_comments': process_existing_unreplied,
        'activationStartedAt': now,
        'createdAt': now,
        'updatedAt': now,
        'created': now,
        'updated': now,
    }
    doc = _normalize_public_reply_for_persistence(doc)
    await db.automations.insert_one(doc)
    await invalidate_dashboard_summary(user_id, instagram_account_id=doc.get('instagramAccountId'))
    await _safe_record_usage_event(
        user_id=user_id,
        event_type='automation_created',
        instagram_account_id=doc.get('instagramAccountId'),
        automation_id=doc.get('id'),
        metadata={'status': doc.get('status'), 'source': 'quick_comment_rule'},
    )
    await _safe_record_usage_event(
        user_id=user_id,
        event_type='automation_activated',
        instagram_account_id=doc.get('instagramAccountId'),
        automation_id=doc.get('id'),
        metadata={'source': 'quick_comment_rule'},
    )
    return _strip_mongo({**doc})


# ---------------- contacts ----------------
@api.get('/contacts')
async def list_contacts(search: Optional[str] = None, tag: Optional[str] = None,
                        user_id: str = Depends(get_current_active_user_id)):
    q = {'user_id': user_id}
    if search:
        q['$or'] = [
            {'name': {'$regex': search, '$options': 'i'}},
            {'username': {'$regex': search, '$options': 'i'}},
        ]
    if tag:
        q['tags'] = tag
    docs = await db.contacts.find(q).sort('created', -1).to_list(2000)
    return [_strip_mongo(d) for d in docs]


@api.post('/contacts')
async def create_contact(data: ContactIn, user_id: str = Depends(get_current_active_user_id)):
    import uuid
    doc = {
        'id': str(uuid.uuid4()), 'user_id': user_id,
        'name': data.name, 'username': data.username,
        'avatar': data.avatar or f'https://i.pravatar.cc/150?u={data.username}',
        'tags': data.tags, 'subscribed': data.subscribed,
        'lastActive': datetime.utcnow(), 'created': datetime.utcnow(),
    }
    await db.contacts.insert_one(doc)
    return _strip_mongo(doc)


@api.patch('/contacts/{cid}')
async def patch_contact(cid: str, data: ContactPatch, user_id: str = Depends(get_current_active_user_id)):
    update = {k: v for k, v in data.model_dump().items() if v is not None}
    res = await db.contacts.update_one({'id': cid, 'user_id': user_id}, {'$set': update})
    if res.matched_count == 0:
        raise HTTPException(404, 'Not found')
    d = await db.contacts.find_one({'id': cid, 'user_id': user_id})
    return _strip_mongo(d)


@api.delete('/contacts/{cid}')
async def delete_contact(cid: str, user_id: str = Depends(get_current_active_user_id)):
    res = await db.contacts.delete_one({'id': cid, 'user_id': user_id})
    if res.deleted_count == 0:
        raise HTTPException(404, 'Not found')
    return {'ok': True}


# ---------------- broadcasts ----------------
@api.get('/broadcasts')
async def list_broadcasts(user_id: str = Depends(get_current_active_user_id)):
    docs = await db.broadcasts.find({'user_id': user_id}).sort('created', -1).to_list(500)
    return [_strip_mongo(d) for d in docs]


@api.post('/broadcasts')
async def create_broadcast(data: BroadcastIn, user_id: str = Depends(get_current_active_user_id)):
    import uuid
    total_audience = await db.contacts.count_documents({'user_id': user_id, 'subscribed': True})
    doc = {
        'id': str(uuid.uuid4()), 'user_id': user_id,
        'name': data.name, 'message': data.message,
        'status': 'draft',
        'audience': data.audience_size or total_audience or 0,
        'openRate': '-', 'clickRate': '-', 'date': '-',
        'created': datetime.utcnow(),
    }
    await db.broadcasts.insert_one(doc)
    return _strip_mongo(doc)


@api.patch('/broadcasts/{bid}')
async def patch_broadcast(bid: str, data: BroadcastPatch, user_id: str = Depends(get_current_active_user_id)):
    update = {k: v for k, v in data.model_dump().items() if v is not None}
    res = await db.broadcasts.update_one({'id': bid, 'user_id': user_id}, {'$set': update})
    if res.matched_count == 0:
        raise HTTPException(404, 'Not found')
    d = await db.broadcasts.find_one({'id': bid, 'user_id': user_id})
    return _strip_mongo(d)


@api.post('/broadcasts/{bid}/send')
async def send_broadcast(bid: str, user_id: str = Depends(get_current_active_user_id)):
    """Send a broadcast to all subscribed contacts via Meta API (or mock if IG not connected)."""
    broadcast = await db.broadcasts.find_one({'id': bid, 'user_id': user_id})
    if not broadcast:
        raise HTTPException(404, 'Not found')
    if broadcast.get('status') == 'sent':
        raise HTTPException(400, 'Already sent')

    user_doc = await db.users.find_one({'id': user_id})
    contacts = await db.contacts.find({'user_id': user_id, 'subscribed': True}).to_list(5000)

    await db.broadcasts.update_one({'id': bid, 'user_id': user_id}, {'$set': {'status': 'sending'}})
    create_tracked_task(_send_broadcast_task(bid, broadcast, user_doc, contacts), 'broadcast')
    return {'ok': True, 'status': 'sending', 'recipients': len(contacts)}


async def _send_broadcast_task(bid: str, broadcast: dict, user_doc: dict, contacts: list):
    """Background task: send DMs to all contacts."""
    sent = 0
    failed = 0
    ig_connected = user_doc.get('instagramConnected') and user_doc.get('meta_access_token')
    access_token = user_doc.get('meta_access_token', '')
    ig_user_id = user_doc.get('ig_user_id', '')
    msg_text = broadcast.get('message', '')

    for contact in contacts:
        ig_id = contact.get('ig_id')
        if ig_connected and ig_id:
            ok = await send_ig_dm(access_token, ig_user_id, ig_id, msg_text)
            if ok:
                sent += 1
            else:
                failed += 1
            await asyncio.sleep(0.5)  # respect rate limits
        else:
            # Not connected to Instagram, or contact has no IG id — cannot deliver
            failed += 1

    total = sent + failed
    # Open/click rates require Meta Insights — not wired yet, so leave blank
    open_rate = '-'
    click_rate = '-'
    await db.broadcasts.update_one(
        {'id': bid, 'user_id': broadcast.get('user_id') or (user_doc or {}).get('id')},
        {'$set': {
            'status': 'sent',
            'audience': total,
            'openRate': open_rate,
            'clickRate': click_rate,
            'date': datetime.utcnow().strftime('%b %d, %Y'),
        }}
    )
    logger.info('Broadcast %s done: %s sent, %s failed', bid, sent, failed)


# ---------------- conversations ----------------
@api.get('/conversations')
async def list_conversations(user_id: str = Depends(get_current_active_user_id)):
    try:
        account = await getActiveInstagramAccount(user_id)
        query = _account_scoped_query(user_id, account)
    except HTTPException as e:
        if e.status_code != 400:
            raise
        query = {'user_id': user_id, 'instagramAccountId': {'$exists': False}}
    docs = await db.conversations.find(query).sort('created', -1).to_list(500)
    return [_strip_mongo(d) for d in docs]


@api.get('/conversations/{cid}')
async def get_conversation(cid: str, user_id: str = Depends(get_current_active_user_id)):
    account = await getActiveInstagramAccount(user_id)
    d = await db.conversations.find_one({'id': cid, **_account_scoped_query(user_id, account)})
    if not d:
        raise HTTPException(404, 'Not found')
    return _strip_mongo(d)


@api.post('/conversations/{cid}/messages')
async def send_message(cid: str, data: MessageIn, user_id: str = Depends(get_current_active_user_id)):
    """Send a message. If the conversation is tied to a real IG contact and the
    user is connected to Instagram, send via Graph API. No fake auto-reply."""
    import uuid
    account = await getActiveInstagramAccount(user_id)
    user_doc = _with_instagram_account_context(
        await db.users.find_one({'id': user_id}) or {},
        account,
    )
    conv = await db.conversations.find_one({'id': cid, **_account_scoped_query(user_id, account)})
    if not conv:
        raise HTTPException(404, 'Not found')
    text = (data.text or '').strip()
    if not text:
        raise HTTPException(400, 'Empty message')

    now = datetime.utcnow()
    msg_me = {
        'id': str(uuid.uuid4()),
        'from': 'me',
        'text': text,
        'time': now.strftime('%I:%M %p'),
        'createdAt': now,
        'created': now,
        'instagramAccountId': account.get('instagramAccountId') or account.get('igUserId'),
        'instagramAccountDbId': account.get('id'),
        'instagram_account_id': account.get('id'),
    }
    new_messages = conv['messages'] + [msg_me]

    # Try to deliver to Instagram if we have a real recipient
    delivered = False
    delivery_error = None
    ig_recipient = (conv.get('contact') or {}).get('ig_id')
    if user_doc and user_doc.get('instagramConnected') and ig_recipient:
        try:
            delivered = await send_ig_dm(
                user_doc.get('meta_access_token', ''),
                user_doc.get('ig_user_id', ''),
                ig_recipient, text,
            )
            if not delivered:
                delivery_error = 'Graph API rejected the message'
        except Exception as e:
            delivery_error = str(e)
            logger.exception('send_message graph call failed')

    msg_me['delivered'] = delivered
    msg_me['status'] = 'sent' if delivered else 'failed'
    if delivery_error:
        msg_me['error'] = delivery_error
    await db.conversations.update_one(
        {'id': cid, **_account_scoped_query(user_id, account)},
        {'$set': {'messages': new_messages, 'lastMessage': text,
                  'time': 'now', 'unread': 0}}
    )
    # Push to WS so other tabs stay in sync
    await ws_manager.send(user_id, {'type': 'message', 'conv_id': cid, 'message': msg_me})
    return {'messages': new_messages, 'delivered': delivered, 'error': delivery_error}


# ---------------- comments ----------------
@api.get('/comments')
async def list_comments(
    user_id: str = Depends(get_current_active_user_id),
    limit: int = Query(50, le=100),
    page: int = Query(1, ge=1),
    unreplied: bool = Query(False)
):
    try:
        account = await getActiveInstagramAccount(user_id)
        query = _account_scoped_query(user_id, account)
    except HTTPException as e:
        if e.status_code != 400:
            raise
        query = {'user_id': user_id, 'instagramAccountId': {'$exists': False}}
        
    if unreplied:
        query['replied'] = {'$ne': True}

    skip = (page - 1) * limit
    total = await db.comments.count_documents(query)
    docs = await db.comments.find(query).sort('created', -1).skip(skip).limit(limit).to_list(limit)
    
    return {
        'comments': [_strip_mongo(d) for d in docs],
        'total': total,
        'page': page,
        'limit': limit,
        'has_more': (skip + limit) < total
    }


@api.post('/comments/{cid}/reply')
async def reply_to_comment(cid: str, data: MessageIn, user_id: str = Depends(get_current_active_user_id)):
    """Reply to an Instagram comment via Graph API.
    POST /{comment-id}/replies with message=..."""
    account = await getActiveInstagramAccount(user_id)
    comment = await db.comments.find_one({'id': cid, **_account_scoped_query(user_id, account)})
    if not comment:
        raise HTTPException(404, 'Comment not found')
    if not account.get('connectionValid'):
        raise HTTPException(400, 'Instagram not connected')
    ig_comment_id = comment.get('ig_comment_id')
    if not ig_comment_id:
        raise HTTPException(400, 'Comment has no Instagram ID (seed data cannot be replied to)')
    access_token = account.get('accessToken', '')
    text = (data.text or '').strip()
    if not text:
        raise HTTPException(400, 'Empty reply')
    attempted_at = datetime.utcnow()
    result = _normalize_reply_result_for_provider_proof(
        await _call_reply_to_ig_comment_detailed(access_token, ig_comment_id, text)
    )
    if not _reply_result_has_provider_proof(result):
        await db.comments.update_one(
            {'id': cid, **_account_scoped_query(user_id, account)},
            {'$set': {
                'reply_status': 'failed',
                'replyStatus': 'failed',
                'reply_attempted_at': attempted_at,
                'reply_provider_status': result.get('status_code'),
                'reply_provider_response_ok': False,
                'reply_failure_reason': result.get('failure_reason') or 'comment_reply_failed',
                'reply_failure_retryable': bool(result.get('retryable')),
                'last_attempt_at': datetime.utcnow(),
                'updated': datetime.utcnow(),
            }}
        )
        raise HTTPException(
            502,
            f"Graph API reply failed: {result.get('failure_reason') or 'provider_error'}"
        )
    body = result.get('body') or {}
    await db.comments.update_one(
        {'id': cid, **_account_scoped_query(user_id, account)},
        {'$set': {
            'replied': True,
            'reply_text': text,
            'reply_id': body.get('id'),
            'reply_status': 'success',
            'replyStatus': 'success',
            'reply_provider_status': result.get('status_code'),
            'reply_provider_response_ok': True,
            'reply_provider_comment_id': result.get('provider_comment_id') or body.get('id') or body.get('comment_id'),
            'reply_success_source': 'manual_reply',
            'reply_attempted_at': attempted_at,
            'replied_at': datetime.utcnow(),
            'replySentAt': datetime.utcnow(),
        }}
    )
    await invalidate_dashboard_summary(
        user_id,
        instagram_account_id=comment.get('instagramAccountId') or comment.get('igUserId') or account.get('instagramAccountId'),
    )
    return {'ok': True, 'graph_reply_id': body.get('id')}


@api.get('/comments/{comment_id}/diagnostics')
async def comment_diagnostics(comment_id: str, user_id: str = Depends(get_current_active_user_id)):
    """Safe per-comment automation status diagnostics for the active IG account."""
    account = await getActiveInstagramAccount(user_id)
    scoped = _account_scoped_query(user_id, account)
    comment = None
    for field in ('id', 'ig_comment_id', 'igCommentId'):
        comment = await db.comments.find_one({**scoped, field: comment_id})
        if comment:
            break
    if not comment:
        raise HTTPException(404, 'Comment not found')

    def _iso(value):
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    text = comment.get('text') or ''
    return {
        # commentId is the internal stable doc id; igCommentId is the
        # Instagram-side comment id that appears in webhook/log lines.
        'commentId': comment.get('id') or comment.get('ig_comment_id') or comment.get('igCommentId'),
        'igCommentId': comment.get('ig_comment_id') or comment.get('igCommentId'),
        'mediaId': comment.get('media_id') or comment.get('mediaId'),
        'text_length': len(str(text)),
        'reply_status': comment.get('reply_status') or comment.get('replyStatus') or (
            'success' if comment.get('replied') else None
        ),
        'dm_status': comment.get('dm_status') or comment.get('dmStatus'),
        'action_status': comment.get('action_status') or comment.get('actionStatus'),
        'skip_reason': comment.get('skip_reason') or comment.get('skipReason'),
        'reply_failure_reason': comment.get('reply_failure_reason'),
        'dm_failure_reason': comment.get('dm_failure_reason'),
        'rule_id': comment.get('rule_id') or comment.get('ruleId'),
        'matched_rule_id': comment.get('matched_rule_id') or comment.get('rule_id') or comment.get('ruleId'),
        'matched_rule_priority': comment.get('matched_rule_priority'),
        'matched_rule_scope': comment.get('matched_rule_scope'),
        'broad_rules_skipped_due_specific_match': bool(comment.get('broad_rules_skipped_due_specific_match')),
        'source': comment.get('source'),
        'last_attempt_at': _iso(comment.get('last_attempt_at')),
        'replied_at': _iso(comment.get('replied_at') or comment.get('replySentAt')),
        'dm_sent_at': _iso(comment.get('dm_sent_at')),
        'reply_provider_response_ok': bool(comment.get('reply_provider_response_ok') is True),
        'reply_provider_confirmation_exists': _reply_provider_proof_exists(comment),
        'reply_provider_comment_id_exists': _reply_provider_comment_id_exists(comment),
        'reply_provider_status': comment.get('reply_provider_status'),
        'legacy_reply_success_without_provider_confirmation': _reply_marked_success_without_provider_proof(comment),
        'attempts': comment.get('attempts'),
        'next_retry_at': _iso(comment.get('next_retry_at')),
        'created': _iso(comment.get('created')),
        'updated': _iso(comment.get('updated')),
    }


@api.post('/comments/{cid}/retry-reply')
async def retry_comment_reply(cid: str, user_id: str = Depends(get_current_active_user_id)):
    """Safely retry the PUBLIC comment reply for one comment.

    Distinct from POST /comments/{cid}/reply (a manual operator-typed
    reply): this endpoint re-runs the rule's own reply step, with
    explicit guardrails that prevent duplicating a provider-proven
    successful reply.

    Allowed only when:
      • the comment is owned by the caller's user_id AND active IG
        account (account-scoped to prevent cross-tenant retry);
      • reply_provider_response_ok is NOT True (the canonical "Graph
        confirmed delivery" flag);
      • reply_status is one of: pending, failed, disabled, or empty
        (legacy doc with no proof);
      • reply_failure_reason is not a PERMANENT_GRAPH_FAILURE_REASONS
        value — those would deterministically fail again.

    Returns a SAFE summary:
      action_status, reply_status, dm_status, queued, next_retry_at,
      attempts, reason. Never includes the reply text, access token,
      or raw Graph error body.
    """
    if _rate_limited('retry_reply', user_id,
                     limit=RATE_LIMIT_RETRY_REPLY_PER_MIN, window_seconds=60):
        logger.warning('rate_limit_hit bucket=retry_reply user_id=%s', user_id)
        raise HTTPException(429, 'Too many retry requests. Try again in a minute.')
    account = await getActiveInstagramAccount(user_id)
    comment = await db.comments.find_one({
        'id': cid, **_account_scoped_query(user_id, account),
    })
    if not comment:
        raise HTTPException(404, 'Comment not found')

    # Guard 1: provider proof exists → never retry.
    if comment.get('reply_provider_response_ok') is True:
        return {
            'queued': False,
            'action_status': comment.get('action_status'),
            'reply_status': comment.get('reply_status') or 'success',
            'dm_status': comment.get('dm_status'),
            'attempts': int(comment.get('attempts') or 0),
            'next_retry_at': None,
            'reason': 'reply_already_proven',
        }

    # Guard 2: only certain prior reply states are retryable. Success
    # without proof (legacy) is allowed because there is no Graph
    # confirmation; we will set the proof flag if this attempt lands.
    prior_reply_status = (comment.get('reply_status') or '').lower()
    legacy_unproven_success = (
        prior_reply_status == 'success' and not comment.get('reply_provider_response_ok')
    )
    if prior_reply_status not in ('', 'pending', 'failed', 'disabled', 'failed_retryable') \
            and not legacy_unproven_success:
        raise HTTPException(409, f'Reply is not retryable from state={prior_reply_status}')

    # Guard 3: permanent failure reasons are not retried.
    prior_reason = comment.get('reply_failure_reason')
    if prior_reason in PERMANENT_GRAPH_FAILURE_REASONS:
        return {
            'queued': False,
            'action_status': comment.get('action_status'),
            'reply_status': prior_reply_status or 'failed',
            'dm_status': comment.get('dm_status'),
            'attempts': int(comment.get('attempts') or 0),
            'next_retry_at': None,
            'reason': f'permanent_failure:{prior_reason}',
        }

    # Resolve the reply text the rule wants to send. Prefer the text we
    # already chose on a prior attempt (so the same comment doesn't get
    # different worded replies on retry). Fall back to the rule's
    # n_reply node.replies list.
    reply_text = (comment.get('reply_text') or '').strip()
    if not reply_text:
        rule_id = comment.get('rule_id') or comment.get('ruleId')
        if rule_id:
            rule = await db.automations.find_one({
                'id': rule_id, **_account_scoped_query(user_id, account),
            })
            if rule:
                for node in (rule.get('nodes') or []):
                    if node.get('type') == 'reply_comment':
                        replies = (node.get('data') or {}).get('replies') or []
                        replies = [r for r in replies if r]
                        if replies:
                            import random as _random
                            reply_text = _random.choice(replies).strip()
                            break
                        legacy = (node.get('data') or {}).get('text') or ''
                        if legacy.strip():
                            reply_text = legacy.strip()
                            break
    if not reply_text:
        raise HTTPException(400, 'No reply text available for this comment')

    ig_comment_id = comment.get('ig_comment_id') or comment.get('igCommentId')
    if not ig_comment_id:
        raise HTTPException(400, 'Comment has no Instagram ID (seed data cannot be replied to)')

    user_doc = await db.users.find_one({'id': user_id}) or {}
    access_token = user_doc.get('meta_access_token') or ''
    if not access_token:
        raise HTTPException(400, 'Instagram not connected')

    attempts = int(comment.get('attempts') or 0) + 1
    now = datetime.utcnow()

    # Mark as in-flight pending BEFORE the network call so concurrent
    # retries see the elevated attempt count and the pending state.
    await db.comments.update_one(
        {'id': cid, **_account_scoped_query(user_id, account)},
        {'$set': {
            'reply_status': 'pending',
            'attempts': attempts,
            'last_attempt_at': now,
            'updated': now,
        }},
    )

    reply_reservation = await reserve_usage_limit(
        user_id,
        'monthly_public_replies_sent_limit',
        increment=1,
        instagram_account_id=comment.get('instagramAccountId') or comment.get('igUserId') or account.get('instagramAccountId'),
        source='retry',
        automation_id=comment.get('rule_id') or comment.get('ruleId'),
        ig_comment_id=ig_comment_id,
        action_id=f"{cid}:retry_reply",
    )
    if not reply_reservation.get('allowed') or (
        reply_reservation.get('exceeded') and not reply_reservation.get('fail_open')
    ):
        await db.comments.update_one(
            {'id': cid, **_account_scoped_query(user_id, account)},
            {'$set': {
                'reply_status': 'plan_limited',
                'reply_failure_reason': 'plan_limit_exceeded',
                'next_retry_at': None,
                'action_status': 'plan_limited' if not comment.get('replied') else comment.get('action_status'),
                'updated': datetime.utcnow(),
            }},
        )
        return {
            'queued': False,
            'action_status': 'plan_limited',
            'reply_status': 'plan_limited',
            'dm_status': comment.get('dm_status'),
            'attempts': attempts,
            'next_retry_at': None,
            'reason': 'plan_limit_exceeded',
        }

    result = await reply_to_ig_comment_detailed(access_token, ig_comment_id, reply_text)
    classified_reason = result.get('failure_reason')
    final_now = datetime.utcnow()

    if result.get('ok'):
        # Persist provider-proven success — including the proof flag
        # that future retries will check FIRST.
        await db.comments.update_one(
            {'id': cid, **_account_scoped_query(user_id, account)},
            {'$set': {
                'replied': True,
                'reply_text': reply_text,
                'reply_status': 'success',
                'replyStatus': 'sent',
                'replied_at': final_now,
                'replySentAt': final_now,
                'reply_provider_response_ok': True,
                'reply_failure_reason': None,
                'next_retry_at': None,
                # Recompute action_status from per-step results.
                'action_status': 'partial_success' if (
                    str(comment.get('dm_status') or '').lower() == 'failed'
                ) else 'success',
                'updated': final_now,
            }},
        )
        await confirm_usage_reservation(
            reply_reservation,
            user_id=user_id,
            event_type='public_reply_sent',
            instagram_account_id=comment.get('instagramAccountId') or comment.get('igUserId') or account.get('instagramAccountId'),
            automation_id=comment.get('rule_id') or comment.get('ruleId'),
            comment_id=cid,
            metadata={'source': 'retry', 'ig_comment_id': ig_comment_id},
        )
        await invalidate_dashboard_summary(
            user_id,
            instagram_account_id=comment.get('instagramAccountId') or comment.get('igUserId') or account.get('instagramAccountId'),
        )
        logger.info(
            'comment_retry_reply_success comment_id=%s ig_comment_id=%s attempts=%s',
            cid, ig_comment_id, attempts,
        )
        return {
            'queued': False,
            'action_status': 'partial_success' if (
                str(comment.get('dm_status') or '').lower() == 'failed'
            ) else 'success',
            'reply_status': 'success',
            'dm_status': comment.get('dm_status'),
            'attempts': attempts,
            'next_retry_at': None,
            'reason': 'reply_sent',
        }

    # Failure path. If the failure is permanent we lock the doc; if
    # transient we schedule a next_retry_at on a small backoff so
    # repeated user clicks don't hammer Graph.
    permanent = classified_reason in PERMANENT_GRAPH_FAILURE_REASONS
    next_retry_at = None
    if not permanent:
        # Exponential-ish backoff: 60s * 2^(attempts-1), capped at 30 min.
        delay_seconds = min(60 * (2 ** max(0, attempts - 1)), 1800)
        next_retry_at = final_now + timedelta(seconds=delay_seconds)
    new_reply_status = 'failed'
    await db.comments.update_one(
        {'id': cid, **_account_scoped_query(user_id, account)},
        {'$set': {
            'reply_status': new_reply_status,
            'reply_failure_reason': classified_reason or 'unknown_graph_error',
            'next_retry_at': next_retry_at,
            'action_status': 'failed' if not comment.get('replied') else comment.get('action_status'),
            'updated': final_now,
        }},
    )
    logger.warning(
        'comment_retry_reply_failed comment_id=%s ig_comment_id=%s attempts=%s reason=%s permanent=%s',
        cid, ig_comment_id, attempts, classified_reason, permanent,
    )
    return {
        'queued': False,
        'action_status': 'failed',
        'reply_status': new_reply_status,
        'dm_status': comment.get('dm_status'),
        'attempts': attempts,
        'next_retry_at': next_retry_at.isoformat() if next_retry_at else None,
        'reason': classified_reason or 'unknown_graph_error',
    }


# Queue-based retry: enqueue without invoking Graph immediately. Used by
# tooling and tests that prefer the background worker to send the reply.
# Distinct from retry_comment_reply (immediate) — tests reach this via
# server.comment_retry_reply directly.
async def comment_retry_reply(comment_id: str, user_id: str = Depends(get_current_active_user_id)):
    """Safely enqueue a public reply retry for a comment without provider proof."""
    account = await getActiveInstagramAccount(user_id)
    scoped = _account_scoped_query(user_id, account)
    comment = None
    for field in ('id', 'ig_comment_id', 'igCommentId'):
        comment = await db.comments.find_one({**scoped, field: comment_id})
        if comment:
            break
    if not comment:
        raise HTTPException(404, 'Comment not found')
    if _reply_provider_proof_exists(comment):
        raise HTTPException(409, 'Public reply already has provider confirmation')
    rule_id = comment.get('rule_id') or comment.get('ruleId')
    if not rule_id or not comment.get('matched'):
        raise HTTPException(400, 'Comment is not eligible for automation reply retry')
    now = datetime.utcnow()
    await db.comments.update_one(
        {'id': comment.get('id'), **scoped},
        {'$set': {
            'replied': False,
            'reply_status': 'pending',
            'replyStatus': 'pending',
            'reply_failure_reason': 'manual_retry_requested',
            'reply_failure_retryable': True,
            'reply_provider_response_ok': False,
            'reply_provider_comment_id': None,
            'reply_success_source': None,
            'action_status': 'pending',
            'actionStatus': 'pending',
            'skip_reason': 'manual_retry_requested',
            'skipReason': 'manual_retry_requested',
            'queued': True,
            'next_retry_at': now,
            'queue_lock_until': None,
            'updated': now,
        }}
    )
    logger.info(
        'manual_retry_reply_enqueued comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s',
        comment.get('ig_comment_id') or comment.get('igCommentId') or comment.get('id'),
        comment.get('media_id') or comment.get('mediaId'),
        user_id,
        account.get('instagramAccountId') or account.get('igUserId') or account.get('id'),
        rule_id,
    )
    return {
        'ok': True,
        'commentId': comment.get('ig_comment_id') or comment.get('igCommentId') or comment.get('id'),
        'action_status': 'pending',
        'reply_status': 'pending',
        'queued': True,
        'next_retry_at': now,
        'reason': 'manual_retry_requested',
    }


@api.get('/comments/{comment_id}/why-not-replied')
async def comment_why_not_replied(comment_id: str, user_id: str = Depends(get_current_active_user_id)):
    """Explain why a comment has not been fully replied to, without exposing content."""
    account = await getActiveInstagramAccount(user_id)
    scoped = _account_scoped_query(user_id, account)
    comment = None
    for field in ('id', 'ig_comment_id', 'igCommentId'):
        comment = await db.comments.find_one({**scoped, field: comment_id})
        if comment:
            break
    if not comment:
        raise HTTPException(404, 'Comment not found')
    action_status = comment.get('action_status') or comment.get('actionStatus')
    reply_status = comment.get('reply_status') or comment.get('replyStatus')
    dm_status = comment.get('dm_status') or comment.get('dmStatus')
    queued = bool(
        comment.get('queued')
        or action_status in ('pending', 'failed_retryable', 'processing')
        or (action_status == 'partial_success' and comment.get('dm_failure_retryable'))
    )
    retryable = bool(
        action_status in ('pending', 'failed_retryable')
        or comment.get('reply_failure_retryable')
        or comment.get('dm_failure_retryable')
    )
    provider_confirmation_exists = _reply_provider_proof_exists(comment)
    legacy_reply_success = _reply_marked_success_without_provider_proof(comment)
    thinks_replied_reason = None
    if provider_confirmation_exists:
        thinks_replied_reason = 'provider_confirmed_public_reply'
    elif legacy_reply_success:
        thinks_replied_reason = 'legacy_success_without_provider_confirmation'
    elif comment.get('replied') is True:
        thinks_replied_reason = 'legacy_replied_flag_without_provider_confirmation'
    manual_retry_allowed = bool(legacy_reply_success or (
        reply_status in ('failed', 'pending') and not provider_confirmation_exists
    ))
    return {
        'eligible': bool(comment.get('matched')) and action_status not in (
            'skipped', 'skipped_ineligible', 'failed_permanent', 'failed_retry_exhausted'
        ),
        'action_status': action_status,
        'reply_status': reply_status,
        'dm_status': dm_status,
        'skip_reason': comment.get('skip_reason') or comment.get('skipReason'),
        'queued': queued,
        'next_retry_at': comment.get('next_retry_at'),
        'attempts': int(comment.get('attempts') or 0),
        'retryable': retryable,
        'rate_limit_reason': (
            comment.get('skip_reason') if 'rate_limit' in str(comment.get('skip_reason') or '') else None
        ),
        'matched_rule_id': comment.get('matched_rule_id') or comment.get('rule_id') or comment.get('ruleId'),
        'matched_rule_priority': comment.get('matched_rule_priority'),
        'matched_rule_scope': comment.get('matched_rule_scope'),
        'broad_rules_skipped_due_specific_match': bool(comment.get('broad_rules_skipped_due_specific_match')),
        'mediaId': comment.get('media_id') or comment.get('mediaId'),
        'reply_failure_reason': comment.get('reply_failure_reason'),
        'dm_failure_reason': comment.get('dm_failure_reason'),
        'last_attempt_at': comment.get('last_attempt_at'),
        'last_queue_attempt_at': comment.get('last_queue_attempt_at'),
        'queue_lock_until': comment.get('queue_lock_until'),
        'failure_category': _failure_category_from_doc(comment),
        'replied_at': comment.get('replied_at') or comment.get('replySentAt'),
        'reply_attempted_at': comment.get('reply_attempted_at'),
        'reply_provider_status': comment.get('reply_provider_status'),
        'reply_provider_confirmation_exists': provider_confirmation_exists,
        'reply_provider_response_ok': bool(comment.get('reply_provider_response_ok') is True),
        'reply_provider_comment_id_exists': _reply_provider_comment_id_exists(comment),
        'legacy_reply_success_without_provider_confirmation': legacy_reply_success,
        'thinks_replied_reason': thinks_replied_reason,
        'manual_retry_allowed': manual_retry_allowed,
    }


@api.get('/comments/{comment_id}/diagnose-specific-rule-reply-plan')
async def diagnose_specific_rule_reply_plan(comment_id: str,
                                            user_id: str = Depends(get_current_active_user_id)):
    """Phase 1.4F debug helper.

    Returns the exact public-reply plan a comment would have, plus
    booleans/lengths/hashes (no raw text) describing the matched rule's
    public reply configuration shape. Use this to localise specific-rule
    bugs where reply_status=disabled despite dm_status=success.
    """
    account = await getActiveInstagramAccount(user_id)
    scoped = _account_scoped_query(user_id, account)
    comment = None
    for field in ('id', 'ig_comment_id', 'igCommentId'):
        comment = await db.comments.find_one({**scoped, field: comment_id})
        if comment:
            break
    if not comment:
        raise HTTPException(404, 'Comment not found')

    rule_id = comment.get('rule_id') or comment.get('ruleId')
    rule = await db.automations.find_one({'id': rule_id}) if rule_id else None

    def _len_hash(text: str) -> dict:
        text = str(text or '').strip()
        return {'present': bool(text), 'length': len(text), 'hash': _safe_text_hash(text)}

    public_reply_candidates = {
        'comment_reply': _len_hash(rule.get('comment_reply') if rule else ''),
        'comment_reply_2': _len_hash(rule.get('comment_reply_2') if rule else ''),
        'comment_reply_3': _len_hash(rule.get('comment_reply_3') if rule else ''),
        'graph_node': {'present': False, 'length': 0, 'hash': ''},
    }
    if rule:
        for node in rule.get('nodes') or []:
            if (node or {}).get('type') == 'reply_comment':
                data = node.get('data') or {}
                node_text = ''
                replies = data.get('replies')
                if isinstance(replies, list):
                    for r in replies:
                        s = str(r or '').strip()
                        if s:
                            node_text = s
                            break
                if not node_text:
                    node_text = str(data.get('text') or data.get('message') or '').strip()
                if node_text:
                    public_reply_candidates['graph_node'] = _len_hash(node_text)
                    break

    runtime_replies = _automation_public_reply_texts(rule or {})
    runtime_dm_text = _automation_dm_text_for_diagnostics(rule or {})

    public_reply_required = _automation_public_reply_required(rule or {})
    public_reply_source = _automation_public_reply_source(rule or {}) if rule else 'none'

    skip_reason = None
    if not rule:
        skip_reason = 'rule_not_found'
    elif rule.get('reply_under_post') is False:
        skip_reason = 'reply_under_post_false'
    elif not runtime_replies:
        skip_reason = 'no_public_reply_text_in_any_shape'

    return {
        'comment_id': comment.get('id'),
        'ig_comment_id': comment.get('ig_comment_id') or comment.get('igCommentId'),
        'media_id': comment.get('media_id') or comment.get('mediaId'),
        'matched_rule_id': rule_id,
        'matched_rule_scope': comment.get('matched_rule_scope') or comment.get('matchedRuleScope'),
        'rule_active': bool(rule and (rule.get('status') == 'active')) if rule else False,
        'rule_post_scope': (rule or {}).get('post_scope'),
        'rule_selected_media_id': (rule or {}).get('media_id'),
        'public_reply_configured_in_saved_rule': bool(rule and any(
            public_reply_candidates[k]['present'] for k in
            ('comment_reply', 'comment_reply_2', 'comment_reply_3', 'graph_node')
        )),
        'public_reply_source_candidates': public_reply_candidates,
        'runtime_rule_has_public_reply': bool(runtime_replies),
        'runtime_public_reply_text_length': len(runtime_replies[0]) if runtime_replies else 0,
        'runtime_public_reply_text_hash': _safe_text_hash(runtime_replies[0]) if runtime_replies else '',
        'public_reply_required': public_reply_required,
        'public_reply_source': public_reply_source,
        'public_reply_preflight_skip_reason': skip_reason,
        'dm_configured': bool(runtime_dm_text),
        'dm_source': 'graph_node' if any(
            (n or {}).get('type') == 'message' for n in (rule.get('nodes') or [])
        ) else ('top_level' if (rule or {}).get('dm_text') else 'none'),
        'dm_text_length': len(runtime_dm_text),
        'dm_text_hash': _safe_text_hash(runtime_dm_text),
        'status_before': {
            'reply_status': comment.get('reply_status') or comment.get('replyStatus'),
            'dm_status': comment.get('dm_status') or comment.get('dmStatus'),
            'action_status': comment.get('action_status') or comment.get('actionStatus'),
            'reply_provider_response_ok': bool(comment.get('reply_provider_response_ok') is True),
            'reply_skip_reason': comment.get('reply_skip_reason'),
            'attempts': comment.get('attempts'),
            'next_retry_at': (
                comment.get('next_retry_at').isoformat()
                if isinstance(comment.get('next_retry_at'), datetime)
                else comment.get('next_retry_at')
            ),
        },
    }


# ---------------- admin repair tools — TEMPORARY support tooling ----------------
# Phase 1.4H. NOT a product feature. NOT exposed to end users.
#
# These endpoints exist so an operator (no Railway shell) can diagnose
# and re-queue a single comment when investigating a specific-post-rule
# regression. They are DISABLED BY DEFAULT (ENABLE_ADMIN_REPAIR_TOOLS=false
# unless explicitly set in the Railway environment) and additionally gated
# by ADMIN_EMAILS. After Phase 1.4 closes, the flag should remain unset
# in production so all four endpoints return 404 to non-admin callers.
#
# Privacy contract: these endpoints return only ids, statuses, lengths,
# hashes, and timestamps — never raw comment / reply / DM text, never
# tokens, never raw Graph error bodies.

async def _require_admin_repair_access(user_id: str, comment_doc: Optional[dict] = None) -> dict:
    """Return user record after enforcing admin/owner+flag access.

    Allowed iff:
      - user.email is in ADMIN_EMAILS, OR
      - ENABLE_ADMIN_REPAIR_TOOLS=true AND user owns the target comment.
    Raises 403/404 otherwise. Never reveals existence of comments owned
    by other users.
    """
    user = await db.users.find_one({'id': user_id})
    if not user:
        raise HTTPException(403, 'Admin access required')
    email = (user.get('email') or '').lower()
    if email and email in ADMIN_EMAILS:
        return user
    if not ENABLE_ADMIN_REPAIR_TOOLS:
        # Hide existence — don't leak that a feature flag is off vs the user
        # is unauthorised vs no such comment.
        raise HTTPException(404, 'Not found')
    if comment_doc is not None:
        if str(comment_doc.get('user_id') or '') != str(user_id):
            raise HTTPException(404, 'Not found')
    return user


async def _find_comment_by_ig_or_internal(ig_comment_id: str) -> Optional[dict]:
    """Look up a comment by ig_comment_id first, then internal id.

    Account scoping is enforced by _require_admin_repair_access using
    comment.user_id, so this lookup is intentionally unscoped.
    """
    if not ig_comment_id:
        return None
    for field in ('ig_comment_id', 'igCommentId', 'id'):
        doc = await db.comments.find_one({field: ig_comment_id})
        if doc:
            return doc
    return None


def _safe_repair_diagnosis(comment: dict, rule: Optional[dict]) -> dict:
    """Build a sanitized diagnosis payload — no raw text fields."""
    rule = rule or {}
    runtime_replies = _automation_public_reply_texts(rule)
    runtime_dm_text = _automation_dm_text_for_diagnostics(rule)
    public_reply_required = _automation_public_reply_required(rule)
    reply_status = comment.get('reply_status') or comment.get('replyStatus') or ''
    dm_status = comment.get('dm_status') or comment.get('dmStatus') or ''
    action_status = comment.get('action_status') or comment.get('actionStatus') or ''
    has_proof = bool(comment.get('reply_provider_response_ok') is True)

    forbidden_state_detected = bool(
        public_reply_required
        and _status_is_disabled(reply_status)
        and _status_is_success(dm_status)
    )

    repair_reason = None
    repairable = False
    if has_proof:
        repair_reason = 'reply_provider_response_ok_already_true'
    elif not public_reply_required:
        repair_reason = 'rule_does_not_require_public_reply'
    elif _status_is_success(reply_status) and has_proof:
        repair_reason = 'reply_already_proven_success'
    else:
        # Repairable if the rule requires a reply, no proof exists, and the
        # current reply_status is disabled/skipped/missing or a non-permanent
        # failure.
        legacy_failure_reason = comment.get('reply_failure_reason')
        permanent = legacy_failure_reason in PERMANENT_GRAPH_FAILURE_REASONS
        if permanent:
            repair_reason = f'reply_permanent_failure:{legacy_failure_reason}'
        elif (
            _status_is_disabled(reply_status)
            or str(reply_status or '').lower() in ('failed', 'failed_retryable', 'pending', '')
        ):
            repairable = True
            repair_reason = 'public_reply_required_not_attempted' if forbidden_state_detected else 'reply_status_repairable'
        else:
            repair_reason = f'reply_status_not_repairable:{reply_status}'

    def _iso(value):
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    return {
        'comment_id': comment.get('id'),
        'ig_comment_id': comment.get('ig_comment_id') or comment.get('igCommentId'),
        'media_id': comment.get('media_id') or comment.get('mediaId'),
        'user_id': comment.get('user_id'),
        'instagram_account_id': comment.get('instagramAccountId') or comment.get('igUserId'),
        'automation_id': comment.get('rule_id') or comment.get('ruleId'),
        'matched_rule_id': comment.get('rule_id') or comment.get('ruleId'),
        'matched_rule_scope': comment.get('matched_rule_scope') or comment.get('matchedRuleScope') or rule.get('post_scope'),
        'reply_status': reply_status or None,
        'dm_status': dm_status or None,
        'action_status': action_status or None,
        'reply_attempted_at': _iso(comment.get('reply_attempted_at')),
        'replied_at': _iso(comment.get('replied_at') or comment.get('replySentAt')),
        'reply_provider_response_ok': has_proof,
        'reply_provider_comment_id_exists': _reply_provider_comment_id_exists(comment),
        'reply_skip_reason': comment.get('reply_skip_reason') or comment.get('skip_reason'),
        'dm_attempted_at': _iso(comment.get('dm_attempted_at')),
        'finalDmSentAt': _iso(comment.get('finalDmSentAt') or comment.get('dm_sent_at') or comment.get('dmSentAt')),
        'next_retry_at': _iso(comment.get('next_retry_at')),
        'attempts': int(comment.get('attempts') or 0),
        'queue_lock_until': _iso(comment.get('queue_lock_until')),
        'public_reply_required': public_reply_required,
        'public_reply_source': _automation_public_reply_source(rule) if rule else 'none',
        'public_reply_text_length': len(runtime_replies[0]) if runtime_replies else 0,
        'public_reply_text_hash': _safe_text_hash(runtime_replies[0]) if runtime_replies else '',
        'dm_required': bool(runtime_dm_text),
        'dm_text_length': len(runtime_dm_text),
        'dm_text_hash': _safe_text_hash(runtime_dm_text),
        'repairable': repairable,
        'repair_reason': repair_reason,
        'forbidden_state_detected': forbidden_state_detected,
    }


@api.get('/admin/tools-enabled')
async def admin_tools_enabled(user_id: str = Depends(get_current_active_user_id)):
    """Tell the frontend whether the admin repair page should render.

    Returns enabled=true if the caller is in ADMIN_EMAILS or if the
    server-side ENABLE_ADMIN_REPAIR_TOOLS flag is on. Never raises 403 —
    a logged-in non-admin in a non-flagged environment simply sees
    enabled=false and can hide the link.
    """
    user = await db.users.find_one({'id': user_id})
    email = ((user or {}).get('email') or '').lower()
    is_admin = bool(email and email in ADMIN_EMAILS)
    return {
        'enabled': bool(is_admin or ENABLE_ADMIN_REPAIR_TOOLS),
        'is_admin': is_admin,
        'flag': ENABLE_ADMIN_REPAIR_TOOLS,
    }


@api.get('/admin/comments/{ig_comment_id}/specific-reply-diagnosis')
async def admin_specific_reply_diagnosis(
    ig_comment_id: str,
    user_id: str = Depends(get_current_active_user_id),
):
    """Sanitized diagnosis for a single comment's specific-post-rule plan."""
    comment = await _find_comment_by_ig_or_internal(ig_comment_id)
    if not comment:
        # Owner-scoped 404 when flag is off; otherwise admin sees true 404.
        raise HTTPException(404, 'Not found')
    await _require_admin_repair_access(user_id, comment)
    rule_id = comment.get('rule_id') or comment.get('ruleId')
    rule = await db.automations.find_one({'id': rule_id}) if rule_id else None
    return _safe_repair_diagnosis(comment, rule)


@api.post('/admin/comments/{ig_comment_id}/repair-specific-public-reply')
async def admin_repair_specific_public_reply(
    ig_comment_id: str,
    user_id: str = Depends(get_current_active_user_id),
):
    """Re-queue a comment for public reply only. Never resends DM."""
    comment = await _find_comment_by_ig_or_internal(ig_comment_id)
    if not comment:
        raise HTTPException(404, 'Not found')
    await _require_admin_repair_access(user_id, comment)
    rule_id = comment.get('rule_id') or comment.get('ruleId')
    rule = await db.automations.find_one({'id': rule_id}) if rule_id else None
    before = _safe_repair_diagnosis(comment, rule)
    if not before['repairable']:
        return {
            'ok': False,
            'repaired': False,
            'reason': before['repair_reason'] or 'not_repairable',
            'before': before,
        }
    now = datetime.utcnow()
    await db.comments.update_one(
        {'id': comment.get('id')},
        {'$set': {
            'reply_status': 'failed_retryable',
            'replyStatus': 'failed_retryable',
            'reply_failure_reason': 'public_reply_required_not_attempted',
            'reply_failure_retryable': True,
            'reply_skip_reason': 'public_reply_required_not_attempted',
            'action_status': 'failed_retryable',
            'actionStatus': 'failed_retryable',
            'queued': True,
            'next_retry_at': now,
            'updated': now,
        }},
    )
    logger.warning(
        'admin_repair_specific_public_reply ig_comment_id=%s user_id=%s rule_id=%s',
        ig_comment_id, user_id, rule_id,
    )
    refreshed = await db.comments.find_one({'id': comment.get('id')}) or comment
    after = _safe_repair_diagnosis(refreshed, rule)
    return {'ok': True, 'repaired': True, 'before': before, 'after': after}


@api.post('/admin/comments/{ig_comment_id}/process-retry-now')
async def admin_process_retry_now(
    ig_comment_id: str,
    user_id: str = Depends(get_current_active_user_id),
):
    """Process the public-reply retry for ONE comment, immediately.

    Wraps the existing retry-reply (immediate) endpoint logic so the
    same provider-proof guards and DM-untouched invariants apply. Never
    sends a DM. Never resends a final link.
    """
    comment = await _find_comment_by_ig_or_internal(ig_comment_id)
    if not comment:
        raise HTTPException(404, 'Not found')
    await _require_admin_repair_access(user_id, comment)
    rule_id = comment.get('rule_id') or comment.get('ruleId')
    rule = await db.automations.find_one({'id': rule_id}) if rule_id else None
    before = _safe_repair_diagnosis(comment, rule)

    # Provider-proof short-circuit: if the public reply already has proof,
    # don't call Graph again.
    if _reply_provider_proof_exists(comment):
        return {
            'ok': True,
            'public_reply_attempted': False,
            'reply_status_after': 'success',
            'reply_provider_response_ok': True,
            'dm_attempted': False,
            'dm_skip_reason': 'provider_proof_exists',
            'dm_status_after': comment.get('dm_status') or comment.get('dmStatus'),
            'action_status_after': comment.get('action_status') or comment.get('actionStatus'),
            'before': before,
        }

    # Re-use the canonical immediate retry endpoint behaviour by calling its
    # internals — it already enforces account scope, permanent-failure
    # rejection, attempts++, next_retry_at backoff. Pass the internal id.
    cid = comment.get('id')
    try:
        result = await retry_comment_reply(cid, user_id=user_id)
    except HTTPException as e:
        return {
            'ok': False,
            'reason': str(e.detail),
            'status_code': e.status_code,
            'before': before,
        }

    refreshed = await db.comments.find_one({'id': cid}) or comment
    return {
        'ok': True,
        'public_reply_attempted': True,
        'reply_status_after': refreshed.get('reply_status') or refreshed.get('replyStatus'),
        'reply_provider_response_ok': bool(refreshed.get('reply_provider_response_ok') is True),
        'dm_attempted': False,
        'dm_skip_reason': 'never_resent_by_repair_tool',
        'dm_status_after': refreshed.get('dm_status') or refreshed.get('dmStatus'),
        'action_status_after': refreshed.get('action_status') or refreshed.get('actionStatus'),
        'retry_result': result,
        'before': before,
    }


# ---------------- dashboard ----------------
def _dashboard_dt(*values) -> Optional[datetime]:
    for value in values:
        parsed = _parse_graph_datetime(value)
        if parsed:
            return parsed
    return None


def _dashboard_key(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.replace('@', '').lower()


def _dashboard_is_unscoped(doc: dict) -> bool:
    return not any(doc.get(k) for k in (
        'instagramAccountId', 'igUserId', 'ig_user_id',
        'instagramAccountDbId', 'instagram_account_id', 'accountId'
    ))


async def _dashboard_include_unscoped(user_id: str) -> bool:
    try:
        count = await db.instagram_accounts.count_documents({
            'userId': user_id,
            'isActive': {'$ne': False},
        })
        return count <= 1
    except Exception:
        return False


async def _dashboard_scoped_docs(collection_name: str, user_id: str, account: Optional[dict],
                                 include_unscoped: bool, limit: int = 5000,
                                 projection: Optional[dict] = None) -> list:
    collection = getattr(db, collection_name)
    docs: list = []
    seen: set = set()

    async def add_many(query: dict):
        try:
            cursor = collection.find(query, projection=projection).sort('created', -1)
            rows = await cursor.to_list(limit)
        except Exception:
            cursor = collection.find(query, projection=projection)
            rows = await cursor.to_list(limit)
        for row in rows:
            key = row.get('id') or str(row.get('_id') or id(row))
            if key not in seen:
                seen.add(key)
                docs.append(row)

    if account:
        await add_many(_account_scoped_query(user_id, account))
    if include_unscoped:
        await add_many({
            'user_id': user_id,
            'instagramAccountId': {'$exists': False},
            'igUserId': {'$exists': False},
            'ig_user_id': {'$exists': False},
            'instagramAccountDbId': {'$exists': False},
            'instagram_account_id': {'$exists': False},
            'accountId': {'$exists': False},
        })
    if not account and not include_unscoped:
        await add_many({'user_id': user_id})
    return docs


def _automation_active(auto: dict) -> bool:
    status = str(auto.get('status') or '').lower()
    if status:
        return status == 'active'
    if 'enabled' in auto:
        return bool(auto.get('enabled'))
    if 'isActive' in auto:
        return bool(auto.get('isActive'))
    return False


def _sent_day(doc: dict) -> Optional[str]:
    ts = _dashboard_dt(
        doc.get('sentAt'), doc.get('sent_at'), doc.get('completedAt'),
        doc.get('processed_at'), doc.get('updated'), doc.get('updatedAt'),
        doc.get('created'), doc.get('createdAt'),
    )
    return ts.date().isoformat() if ts else None


_DASHBOARD_SUCCESS_STATUSES = {
    'success', 'sent', 'replied', 'completed', 'delivered', 'ok',
}


def _dashboard_success_status(value: Any) -> bool:
    return str(value or '').strip().lower() in _DASHBOARD_SUCCESS_STATUSES


DASHBOARD_METRIC_SOURCES = {
    'user_dashboard': {
        'comments_processed': {
            'meaning': 'Eligible comments processed for the current user and active Instagram account.',
            'scope': ['user_id', 'active_instagram_account_id', 'current_month'],
            'source': 'comments.action_status or usage_events.comment_processed fallback',
            'freshness': 'near real-time from comment processing writes',
            'limitations': 'Legacy comments without status fields are not counted unless usage events exist.',
        },
        'public_replies_sent': {
            'meaning': 'Public Instagram comment replies accepted by Meta.',
            'scope': ['user_id', 'active_instagram_account_id', 'current_month'],
            'source': 'comments.reply_status=success + reply_provider_response_ok=true',
            'freshness': 'provider-confirmed',
            'limitations': 'Legacy successes without provider proof are excluded.',
        },
        'dms_sent': {
            'meaning': 'Automation DMs accepted by Meta.',
            'scope': ['user_id', 'active_instagram_account_id', 'current_month'],
            'source': 'comments.dm_status=success or usage_events.dm_sent fallback',
            'freshness': 'provider-confirmed/status-confirmed',
            'limitations': 'Legacy unscoped counters are used only for single-account users.',
        },
        'messages_sent': {
            'meaning': 'Public replies plus automation DMs sent this month.',
            'scope': ['user_id', 'active_instagram_account_id', 'current_month'],
            'source': 'public_replies_sent + dms_sent',
            'freshness': 'same as component metrics',
            'limitations': 'False legacy successes are excluded unless repaired.',
        },
        'link_clicks': {
            'meaning': 'Tracked link clicks this month.',
            'scope': ['user_id', 'active_instagram_account_id', 'current_month'],
            'source': 'link_click_events',
            'freshness': 'near real-time from tracking endpoint',
            'limitations': 'Raw historical links sent without tracking cannot be inferred.',
        },
        'conversion_rate': {
            'meaning': 'Unique Instagram users who clicked tracked links divided by total contacts.',
            'scope': ['user_id', 'active_instagram_account_id', 'current_month'],
            'source': 'unique link_click_events users / contacts plus observed commenters',
            'freshness': 'near real-time',
            'limitations': 'Contacts with no stable Instagram user key cannot be deduped.',
        },
        'active_automations': {
            'meaning': 'Active automations scoped to the active Instagram account.',
            'scope': ['user_id', 'active_instagram_account_id'],
            'source': 'automations.status or enabled flags',
            'freshness': 'immediate after save/cache invalidation',
            'limitations': 'Legacy enabled-only rules are normalized as active when enabled=true.',
        },
        'queue_pending': {
            'meaning': 'Pending or processing eligible comment automation work for the active account.',
            'scope': ['user_id', 'active_instagram_account_id'],
            'source': 'comments.action_status in pending/processing',
            'freshness': 'queue write cadence',
            'limitations': 'Bounded dashboard sample; admin views provide broader reconciliation.',
        },
    },
    'admin_dashboard': {
        'total_users': {
            'meaning': 'All user records, including suspended and deleted users for audit continuity.',
            'scope': ['global'],
            'source': 'users collection',
            'freshness': 'immediate',
        },
        'active_users': {
            'meaning': 'Users not suspended or deleted.',
            'scope': ['global'],
            'source': 'users.status not in suspended/deleted',
            'freshness': 'immediate',
        },
        'connected_instagram_accounts': {
            'meaning': 'Instagram accounts currently marked connectionValid=true.',
            'scope': ['global'],
            'source': 'instagram_accounts.connectionValid',
            'freshness': 'immediate after OAuth/token refresh status writes',
        },
        'active_automations': {
            'meaning': 'Global active automation count.',
            'scope': ['global'],
            'source': 'automations.status=active',
            'freshness': 'immediate after automation mutation',
        },
        'current_month_usage': {
            'meaning': 'Summed monthly usage counters across users for the current month.',
            'scope': ['global', 'current_month'],
            'source': 'monthly_usage',
            'freshness': 'usage event write cadence',
            'limitations': 'Use reconciliation to detect drift from usage_events/provider status.',
        },
        'plan_limited_count': {
            'meaning': 'Comments currently blocked by plan limits.',
            'scope': ['global'],
            'source': 'comments.action_status=plan_limited',
            'freshness': 'immediate from automation processing',
        },
        'retryable_failure_count': {
            'meaning': 'Comments requiring retry.',
            'scope': ['global'],
            'source': 'comments.action_status=failed_retryable',
            'freshness': 'queue write cadence',
        },
        'permanent_failure_count': {
            'meaning': 'Comments that reached permanent failure/exhausted retry.',
            'scope': ['global'],
            'source': 'comments.action_status in failed_permanent/failed_retry_exhausted',
            'freshness': 'queue write cadence',
        },
    },
}


def _dashboard_month_bounds(month: Optional[str] = None) -> tuple:
    if month and re.fullmatch(r'\d{4}-\d{2}', str(month)):
        year, mon = [int(part) for part in str(month).split('-')]
        start = datetime(year, mon, 1)
    else:
        now = datetime.utcnow()
        start = datetime(now.year, now.month, 1)
    if start.month == 12:
        end = datetime(start.year + 1, 1, 1)
    else:
        end = datetime(start.year, start.month + 1, 1)
    return start, end


def _dashboard_comment_dt(comment: dict) -> Optional[datetime]:
    return _dashboard_dt(
        comment.get('replied_at'), comment.get('dm_sent_at'),
        comment.get('last_attempt_at'), comment.get('processed_at'),
        comment.get('updated'), comment.get('updatedAt'),
        comment.get('created'), comment.get('createdAt'),
        comment.get('timestamp'),
    )


def _dashboard_public_reply_confirmed(comment: dict) -> bool:
    return (
        str(comment.get('reply_status') or comment.get('replyStatus') or '').lower() == 'success'
        and comment.get('reply_provider_response_ok') is True
    )


def _dashboard_dm_confirmed(comment: dict) -> bool:
    return str(comment.get('dm_status') or comment.get('dmStatus') or '').lower() == 'success'


def _dashboard_comment_processed(comment: dict) -> bool:
    status = str(comment.get('action_status') or comment.get('actionStatus') or '').lower()
    if status in {
        'success', 'partial_success', 'pending', 'processing',
        'failed_retryable', 'failed_permanent', 'failed_retry_exhausted',
        'plan_limited',
    }:
        return True
    if comment.get('matched_rule_id') or comment.get('matchedRuleId'):
        return True
    return _dashboard_public_reply_confirmed(comment) or _dashboard_dm_confirmed(comment)


def _dashboard_comment_message_count(comment: dict) -> int:
    sent_count = 0
    if comment.get('replied') is True:
        sent_count += 1
    action_status = comment.get('action_status') or comment.get('actionStatus')
    if _dashboard_success_status(action_status) and not comment.get('replied'):
        sent_count += 1
    return sent_count


def _dashboard_conversation_message_sent(message: dict) -> bool:
    is_outgoing = message.get('from') == 'me' or message.get('from_') == 'me'
    if not is_outgoing:
        return False
    status = message.get('status')
    if status is not None:
        return _dashboard_success_status(status)
    if 'delivered' in message:
        return message.get('delivered') is True
    return False


def _dashboard_account_keys(account: Optional[dict]) -> set:
    keys = set()
    if not account:
        return keys
    for value in (
        account.get('id'),
        account.get('instagramAccountDbId'),
        account.get('instagram_account_id'),
        account.get('instagramAccountId'),
        account.get('igUserId'),
        account.get('accountId'),
    ):
        key = _dashboard_key(value)
        if key:
            keys.add(key)
    return keys


def _dashboard_doc_matches_account(doc: dict, account: Optional[dict], include_unscoped: bool) -> bool:
    account_keys = _dashboard_account_keys(account)
    doc_keys = set()
    for field in (
        'instagram_account_id', 'instagramAccountId', 'instagramAccountDbId',
        'igUserId', 'ig_user_id', 'accountId',
    ):
        key = _dashboard_key(doc.get(field))
        if key:
            doc_keys.add(key)
    if account_keys and doc_keys.intersection(account_keys):
        return True
    return include_unscoped and not doc_keys


def _dashboard_last_seven_buckets() -> dict:
    from collections import OrderedDict
    today = datetime.utcnow().date()
    buckets = OrderedDict()
    for i in range(6, -1, -1):
        d = today - timedelta(days=i)
        buckets[d.isoformat()] = {
            'day': d.strftime('%a'),
            'date': d.isoformat(),
            'messages': 0,
            'conversions': 0,
        }
    return buckets


def _dashboard_event_dt(event: dict) -> Optional[datetime]:
    return _dashboard_dt(event.get('event_date'), event.get('created_at'), event.get('createdAt'))


async def _dashboard_usage_events_for_window(user_id: str, event_types: list, start_date) -> list:
    months = sorted({
        (start_date + timedelta(days=i)).strftime('%Y-%m')
        for i in range(0, 8)
    })
    try:
        cursor = db.usage_events.find({
            'user_id': str(user_id),
            'event_month': {'$in': months},
            'event_type': {'$in': event_types},
        }, projection={
            'event_type': 1, 'event_date': 1, 'event_month': 1,
            'instagramAccountId': 1, 'instagram_account_id': 1,
            'igUserId': 1, 'ig_user_id': 1, 'accountId': 1,
        }).sort('event_date', -1).limit(5000)
        return await cursor.to_list(5000)
    except Exception:
        return []


def _dashboard_auto_out(auto: dict) -> dict:
    return {
        'id': auto.get('id'),
        'name': auto.get('name') or 'Untitled automation',
        'trigger': auto.get('trigger') or auto.get('keyword') or auto.get('match') or 'Comment trigger',
        'status': auto.get('status') or ('active' if auto.get('enabled') else 'draft'),
        'sent': int(auto.get('sent') or 0),
    }


DASHBOARD_SUMMARY_FRESH_SECONDS = 60
DASHBOARD_SUMMARY_MAX_STALE_SECONDS = 5 * 60
DASHBOARD_SUMMARY_METRICS_VERSION = 1


def _get_dashboard_summary_month(now: Optional[datetime] = None) -> str:
    return _usage_month(now or datetime.utcnow())


def _dashboard_summary_account_id(account: Optional[dict]) -> str:
    return str(
        (account or {}).get('instagramAccountId') or
        (account or {}).get('igUserId') or
        (account or {}).get('id') or
        'none'
    )


def _make_dashboard_summary_cache_key(user_id: str, instagram_account_id: str, month: str) -> dict:
    return {
        'user_id': str(user_id),
        'instagramAccountId': str(instagram_account_id or 'none'),
        'month': str(month),
    }


async def _get_dashboard_summary_snapshot(user_id: str, instagram_account_id: str, month: str) -> Optional[dict]:
    try:
        return await db.dashboard_summaries.find_one(
            _make_dashboard_summary_cache_key(user_id, instagram_account_id, month)
        )
    except Exception as exc:
        logger.warning(
            'dashboard_summary_snapshot_lookup_failed user_id=%s instagramAccountId=%s reason=%s',
            user_id,
            _token_prefix(instagram_account_id),
            type(exc).__name__,
        )
        return None


async def _store_dashboard_summary_snapshot(
    user_id: str,
    instagram_account_id: str,
    month: str,
    summary: dict,
    now: Optional[datetime] = None,
) -> None:
    now_dt = now or datetime.utcnow()
    key = _make_dashboard_summary_cache_key(user_id, instagram_account_id, month)
    doc = {
        **key,
        'summary': summary,
        'metrics_version': DASHBOARD_SUMMARY_METRICS_VERSION,
        'source': 'rebuilt',
        'computed_at': now_dt,
        'updated_at': now_dt,
        'expires_at': now_dt + timedelta(seconds=DASHBOARD_SUMMARY_FRESH_SECONDS),
        'max_stale_at': now_dt + timedelta(seconds=DASHBOARD_SUMMARY_MAX_STALE_SECONDS),
    }
    try:
        await db.dashboard_summaries.update_one(
            key,
            {
                '$set': doc,
                '$setOnInsert': {'created_at': now_dt},
            },
            upsert=True,
        )
    except Exception as exc:
        logger.warning(
            'dashboard_summary_snapshot_store_failed user_id=%s instagramAccountId=%s reason=%s',
            user_id,
            _token_prefix(instagram_account_id),
            type(exc).__name__,
        )


async def invalidate_dashboard_summary(
    user_id: str,
    instagram_account_id: Optional[str] = None,
    month: Optional[str] = None,
) -> None:
    query: Dict[str, Any] = {'user_id': str(user_id)}
    if instagram_account_id:
        query['instagramAccountId'] = str(instagram_account_id)
    if month:
        query['month'] = str(month)
    try:
        await db.dashboard_summaries.delete_many(query)
    except Exception as exc:
        logger.warning(
            'dashboard_summary_invalidation_failed user_id=%s reason=%s',
            user_id,
            type(exc).__name__,
        )


def _dashboard_snapshot_dt(snapshot: dict, field: str) -> Optional[datetime]:
    value = (snapshot or {}).get(field)
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=None)
        except Exception:
            return None
    return None


def _dashboard_snapshot_usable(snapshot: Optional[dict], field: str, now_dt: datetime) -> bool:
    marker = _dashboard_snapshot_dt(snapshot or {}, field)
    return bool(marker and now_dt <= marker and isinstance((snapshot or {}).get('summary'), dict))


async def _refresh_dashboard_summary_snapshot(user_id: str, account: Optional[dict], month: str) -> None:
    instagram_account_id = _dashboard_summary_account_id(account)
    try:
        summary, _meta = await _calculate_dashboard_summary_live(
            user_id,
            account=account,
            account_loaded=True,
        )
        await _store_dashboard_summary_snapshot(user_id, instagram_account_id, month, summary)
        logger.info(
            'dashboard_summary_background_refresh_success user_id=%s instagramAccountId=%s',
            user_id,
            _token_prefix(instagram_account_id),
        )
    except Exception as exc:
        logger.warning(
            'dashboard_summary_background_refresh_failed user_id=%s instagramAccountId=%s reason=%s',
            user_id,
            _token_prefix(instagram_account_id),
            type(exc).__name__,
        )


async def _get_dashboard_summary_readthrough(
    user_id: str,
    account: Optional[dict],
    background_tasks: Optional[BackgroundTasks] = None,
) -> tuple[dict, dict]:
    started = datetime.utcnow()
    now_dt = datetime.utcnow()
    month = _get_dashboard_summary_month(now_dt)
    instagram_account_id = _dashboard_summary_account_id(account)
    snapshot = await _get_dashboard_summary_snapshot(user_id, instagram_account_id, month)

    if _dashboard_snapshot_usable(snapshot, 'expires_at', now_dt):
        duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
        return snapshot['summary'], {
            'duration_ms': duration_ms,
            'source': 'read_model',
            'slowest': 'read_model',
        }

    if _dashboard_snapshot_usable(snapshot, 'max_stale_at', now_dt):
        if background_tasks is not None:
            background_tasks.add_task(_refresh_dashboard_summary_snapshot, user_id, account, month)
        duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
        return snapshot['summary'], {
            'duration_ms': duration_ms,
            'source': 'stale_read_model',
            'slowest': 'read_model',
        }

    try:
        summary, live_meta = await _calculate_dashboard_summary_live(
            user_id,
            account=account,
            account_loaded=True,
        )
        await _store_dashboard_summary_snapshot(user_id, instagram_account_id, month, summary, now_dt)
        duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
        return summary, {
            **live_meta,
            'duration_ms': duration_ms,
            'source': 'rebuilt',
            'slowest': live_meta.get('slowest') or 'live',
        }
    except Exception:
        if _dashboard_snapshot_usable(snapshot, 'max_stale_at', now_dt):
            duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
            logger.warning(
                'dashboard_summary_rebuild_failed_using_stale user_id=%s instagramAccountId=%s',
                user_id,
                _token_prefix(instagram_account_id),
            )
            return snapshot['summary'], {
                'duration_ms': duration_ms,
                'source': 'stale_fallback',
                'slowest': 'read_model',
            }
        raise


@api.get('/dashboard/metric-sources')
async def dashboard_metric_sources(user_id: str = Depends(get_current_active_user_id)):
    """Return the dashboard metric contract.

    This is intentionally metadata-only: no raw comments, messages, tokens, or
    provider payloads. It gives support/admin verification a stable source map
    for the numbers shown in the UI.
    """
    return {
        'metrics': DASHBOARD_METRIC_SOURCES,
        'billing_enabled': False,
    }


async def _calculate_dashboard_summary_live(
    user_id: str,
    account: Optional[dict] = None,
    account_loaded: bool = False,
) -> tuple[dict, dict]:
    """Compact dashboard payload for fast route transitions.

    The legacy /dashboard/stats endpoint intentionally remains available for
    backwards compatibility, but it scans several large collections. This
    summary uses monthly usage counters and bounded event windows so the app can
    render the dashboard shell and first numbers without waiting on heavy
    history aggregation.
    """
    started = datetime.utcnow()
    if not account_loaded:
        try:
            account = await getActiveInstagramAccount(user_id)
        except HTTPException as e:
            if e.status_code != 400:
                raise
            account = None
    t_after_account = datetime.utcnow()

    include_unscoped = await _dashboard_include_unscoped(user_id)
    instagram_account_id = (account or {}).get('instagramAccountId') or (account or {}).get('igUserId')
    buckets = _dashboard_last_seven_buckets()
    start_day = datetime.fromisoformat(next(iter(buckets.keys()))).date()
    current_month = _usage_month(datetime.utcnow())
    month_start, month_end = _dashboard_month_bounds(current_month)
    t_after_meta = datetime.utcnow()

    # Phase 2.18C performance fix: the 6 independent read paths below
    # (usage_summary, usage events, automations, contacts, link clicks,
    # comments) used to run sequentially, costing ~6× single-query
    # round-trip time. They are now executed in parallel via
    # asyncio.gather, which on a warm backend cuts the typical live
    # rebuild from 3-5s to ~1s. Exceptions on individual collections
    # are caught locally so the summary still renders the rest.
    async def _safe_clicks():
        try:
            return await db.link_click_events.find({
                '$or': [{'userId': user_id}, {'user_id': user_id}],
            }, projection={
                'clickedAt': 1, 'createdAt': 1, 'created': 1,
                'instagramUserId': 1, 'instagram_user_id': 1,
                'recipient_id': 1, 'sender_id': 1,
                'user_id': 1, 'userId': 1,
                'instagram_account_id': 1, 'instagramAccountId': 1,
                'instagramAccountDbId': 1, 'igUserId': 1, 'ig_user_id': 1,
                'accountId': 1,
            }).sort('clickedAt', -1).limit(5000).to_list(5000)
        except Exception:
            return []

    async def _safe_connected_accounts():
        try:
            return await db.instagram_accounts.count_documents({
                'userId': user_id,
                'isActive': {'$ne': False},
                'connectionValid': True,
            })
        except Exception:
            return None  # caller falls back to usage_summary counter

    (
        usage_summary,
        events,
        autos,
        contacts,
        clicks,
        recent_comments,
        connected_accounts_raw,
    ) = await asyncio.gather(
        get_current_usage_with_limits(user_id),
        _dashboard_usage_events_for_window(
            user_id,
            ['comment_processed', 'public_reply_sent', 'dm_sent', 'link_clicked'],
            start_day,
        ),
        _dashboard_scoped_docs(
            'automations', user_id, account, include_unscoped, 1000,
            projection={'id': 1, 'name': 1, 'status': 1, 'created': 1, 'updated': 1, 'sent': 1,
                        'post_scope': 1, 'media_id': 1, 'createdAt': 1},
        ),
        _dashboard_scoped_docs(
            'contacts', user_id, account, include_unscoped, 2000,
            projection={'id': 1, 'ig_id': 1, 'instagramUserId': 1, 'instagram_user_id': 1,
                        'username': 1, 'contact_id': 1, 'user_id': 1,
                        'instagram_account_id': 1, 'instagramAccountId': 1,
                        'instagramAccountDbId': 1, 'igUserId': 1, 'ig_user_id': 1,
                        'accountId': 1},
        ),
        _safe_clicks(),
        _dashboard_scoped_docs(
            'comments', user_id, account, include_unscoped, 5000,
            projection={'id': 1, 'commenter_id': 1, 'commenterId': 1, 'commenter_username': 1,
                        'sender_id': 1, 'instagramUserId': 1,
                        'reply_status': 1, 'replyStatus': 1, 'reply_provider_response_ok': 1,
                        'dm_status': 1, 'dmStatus': 1, 'dm_provider_response_ok': 1,
                        'action_status': 1, 'actionStatus': 1,
                        'created': 1, 'createdAt': 1, 'updated': 1, 'updatedAt': 1,
                        'ig_comment_id': 1, 'media_id': 1, 'mediaId': 1,
                        'attempts': 1, 'skip_reason': 1, 'skipReason': 1,
                        'reply_failure_reason': 1, 'dm_failure_reason': 1,
                        'next_retry_at': 1, 'replied': 1, 'reply_text': 1,
                        'user_id': 1,
                        'instagram_account_id': 1, 'instagramAccountId': 1,
                        'instagramAccountDbId': 1, 'igUserId': 1, 'ig_user_id': 1,
                        'accountId': 1},
        ),
        _safe_connected_accounts(),
    )
    counters = usage_summary.get('counters') or {}
    t_after_parallel_reads = datetime.utcnow()

    usage_comments_processed = 0
    usage_public_replies = 0
    usage_dms = 0
    usage_link_clicks = 0
    for event in events:
        if not _dashboard_doc_matches_account(event, account, include_unscoped):
            continue
        event_dt = _dashboard_event_dt(event)
        if not event_dt:
            continue
        event_type = event.get('event_type')
        day_key = event_dt.date().isoformat()
        in_current_month = (event.get('event_month') or event_dt.strftime('%Y-%m')) == current_month
        if event_type == 'comment_processed':
            if in_current_month:
                usage_comments_processed += 1
        elif event_type in ('public_reply_sent', 'dm_sent'):
            if day_key in buckets:
                buckets[day_key]['messages'] += 1
            if in_current_month:
                if event_type == 'public_reply_sent':
                    usage_public_replies += 1
                else:
                    usage_dms += 1
        elif event_type == 'link_clicked':
            if in_current_month:
                usage_link_clicks += 1

    # autos / contacts / clicks / recent_comments were fetched in the
    # asyncio.gather() above.
    active_autos = [auto for auto in autos if _automation_active(auto)]
    top_automations = [_dashboard_auto_out(auto) for auto in autos[:6]]

    ig_owner_id = _dashboard_key(instagram_account_id)
    contact_keys = set()
    for contact in contacts:
        if not _dashboard_doc_matches_account(contact, account, include_unscoped):
            continue
        key = _dashboard_key(
            contact.get('ig_id') or contact.get('instagramUserId') or
            contact.get('instagram_user_id') or contact.get('username') or
            contact.get('contact_id')
        )
        if key and key != ig_owner_id:
            contact_keys.add(key)

    converted_contacts = set()
    tracked_month_link_clicks = 0
    for click in clicks:
        if not _dashboard_doc_matches_account(click, account, include_unscoped):
            continue
        click_dt = _dashboard_dt(click.get('clickedAt'), click.get('createdAt'), click.get('created'))
        if click_dt and click_dt.strftime('%Y-%m') != current_month:
            continue
        tracked_month_link_clicks += 1
        key = _dashboard_key(
            click.get('instagramUserId') or click.get('instagram_user_id') or
            click.get('recipient_id') or click.get('sender_id')
        )
        if key and key != ig_owner_id:
            converted_contacts.add(key)
            contact_keys.add(key)
    # recent_comments was fetched in the asyncio.gather() above.
    provider_comments_processed = 0
    provider_public_replies = 0
    provider_dms = 0
    provider_weekly_messages = {key: 0 for key in buckets.keys()}
    for comment in recent_comments:
        if not _dashboard_doc_matches_account(comment, account, include_unscoped):
            continue
        key = _dashboard_key(
            comment.get('commenter_id') or comment.get('commenterId') or
            comment.get('sender_id') or comment.get('instagramUserId') or
            comment.get('commenter_username')
        )
        if key and key != ig_owner_id:
            contact_keys.add(key)

        comment_dt = _dashboard_comment_dt(comment)
        in_current_month = bool(comment_dt and month_start <= comment_dt < month_end)
        if in_current_month and _dashboard_comment_processed(comment):
            provider_comments_processed += 1
        if in_current_month and _dashboard_public_reply_confirmed(comment):
            provider_public_replies += 1
            day_key = comment_dt.date().isoformat() if comment_dt else None
            if day_key in provider_weekly_messages:
                provider_weekly_messages[day_key] += 1
        if in_current_month and _dashboard_dm_confirmed(comment):
            provider_dms += 1
            day_key = comment_dt.date().isoformat() if comment_dt else None
            if day_key in provider_weekly_messages:
                provider_weekly_messages[day_key] += 1

    # For workspaces that do not yet have account-scoped usage_events, fall back
    # to the monthly_usage counters only when there is a single account. This
    # preserves isolation for multi-account users.
    if include_unscoped:
        usage_comments_processed = max(usage_comments_processed, int(counters.get('comments_processed') or 0))
        usage_public_replies = max(usage_public_replies, int(counters.get('public_replies_sent') or 0))
        usage_dms = max(usage_dms, int(counters.get('dms_sent') or 0))
        usage_link_clicks = max(usage_link_clicks, int(counters.get('links_clicked') or 0))

    comments_processed = max(provider_comments_processed, usage_comments_processed)
    public_replies_sent = max(provider_public_replies, usage_public_replies)
    dms_sent = max(provider_dms, usage_dms)
    month_messages_sent = public_replies_sent + dms_sent

    total_contacts = len(contact_keys)
    converted_count = len(converted_contacts)
    conversion_rate = 0 if not total_contacts else round((converted_count / total_contacts) * 100, 1)
    month_link_clicks = max(tracked_month_link_clicks, usage_link_clicks)

    conversion_users_by_day = {key: set() for key in buckets.keys()}
    for click in clicks:
        if not _dashboard_doc_matches_account(click, account, include_unscoped):
            continue
        click_dt = _dashboard_dt(click.get('clickedAt'), click.get('createdAt'), click.get('created'))
        day_key = click_dt.date().isoformat() if click_dt else None
        if day_key not in conversion_users_by_day:
            continue
        user_key = _dashboard_key(
            click.get('instagramUserId') or click.get('instagram_user_id') or
            click.get('recipient_id') or click.get('sender_id') or click.get('id')
        )
        if user_key:
            conversion_users_by_day[day_key].add(user_key)
    if any(provider_weekly_messages.values()):
        for day_key, count in provider_weekly_messages.items():
            buckets[day_key]['messages'] = count
    for day_key, users in conversion_users_by_day.items():
        buckets[day_key]['conversions'] = len(users)

    # connected_accounts_raw was fetched in the asyncio.gather() above.
    # If the parallel call failed, _safe_connected_accounts returned None
    # and we fall back to the usage_summary counter so we still answer.
    if connected_accounts_raw is None:
        connected_accounts = int(usage_summary.get('connectedInstagramAccountsCount') or 0)
    else:
        connected_accounts = connected_accounts_raw

    queue_summary = {
        'pending': 0,
        'retryable': 0,
        'permanentFailures': 0,
        'partialSuccess': 0,
    }
    for comment in recent_comments:
        status = str(comment.get('action_status') or comment.get('actionStatus') or '').lower()
        if status in ('pending', 'processing'):
            queue_summary['pending'] += 1
        elif status in ('failed_retryable', 'retryable'):
            queue_summary['retryable'] += 1
        elif status in ('failed_permanent', 'failed_retry_exhausted'):
            queue_summary['permanentFailures'] += 1
        elif status == 'partial_success':
            queue_summary['partialSuccess'] += 1

    now = datetime.utcnow()
    duration_ms = int((now - started).total_seconds() * 1000)
    # Phase 2.18C: queries now run in parallel, so the per-collection
    # breakdown is no longer meaningful. We expose one parallel_reads
    # bucket plus the pre/post phases so logs still show where time
    # goes (network round-trip vs in-process processing).
    section_timings = {
        'active_account': int((t_after_account - started).total_seconds() * 1000),
        'meta': int((t_after_meta - t_after_account).total_seconds() * 1000),
        'parallel_reads': int((t_after_parallel_reads - t_after_meta).total_seconds() * 1000),
        'post_processing': int((now - t_after_parallel_reads).total_seconds() * 1000),
    }
    slowest_section = max(section_timings, key=section_timings.get)
    section_counts = {
        'usage_events': len(events),
        'automations': len(autos),
        'contacts': len(contacts),
        'link_clicks': len(clicks),
        'comments': len(recent_comments),
    }
    logger.info(
        'dashboard_summary_calculated user_id=%s activeAccountId=%s instagramAccountId=%s '
        'messagesSent=%s publicRepliesSent=%s dmsSent=%s activeAutomations=%s totalContacts=%s durationMs=%s',
        user_id,
        (account or {}).get('id'),
        instagram_account_id,
        month_messages_sent,
        public_replies_sent,
        dms_sent,
        len(active_autos),
        total_contacts,
        duration_ms,
    )
    logger.info(
        'dashboard_summary_breakdown user_id=%s '
        'accountMs=%s metaMs=%s parallelReadsMs=%s postProcessingMs=%s '
        'totalMs=%s slowest=%s counts=%s',
        user_id,
        section_timings['active_account'],
        section_timings['meta'],
        section_timings['parallel_reads'],
        section_timings['post_processing'],
        duration_ms,
        slowest_section,
        json.dumps(section_counts, sort_keys=True),
    )

    weekly_performance = list(buckets.values())
    summary = {
        'totalContacts': total_contacts,
        'activeAutomations': len(active_autos),
        'messagesSent': month_messages_sent,
        'conversionRate': conversion_rate,
        'weeklyPerformance': weekly_performance,
        'linkClicks': month_link_clicks,
        'convertedContacts': converted_count,
        'commentsProcessed': comments_processed,
        'publicRepliesSent': public_replies_sent,
        'dmsSent': dms_sent,
        'connectedAccounts': int(connected_accounts or 0),
        'queueSummary': queue_summary,
        'plan': usage_summary,
        'topAutomations': top_automations,
        'lastUpdatedAt': datetime.utcnow().isoformat(),
        'instagram': {
            'connected': bool(account and account.get('connectionValid')),
            'username': (account or {}).get('username') or None,
            'activeAccountId': (account or {}).get('id') or None,
            'instagramAccountId': instagram_account_id or None,
        },
        # Backward-compatible keys used by older frontend bundles.
        'total_contacts': total_contacts,
        'active_automations': len(active_autos),
        'messages_sent': month_messages_sent,
        'conversion_rate': conversion_rate,
        'weekly_chart': weekly_performance,
    }
    return summary, {
        'duration_ms': duration_ms,
        'source': 'live',
        'slowest': slowest_section,
        'section_timings': section_timings,
        'section_counts': section_counts,
    }


@api.get('/dashboard/summary')
async def dashboard_summary(
    user_id: str = Depends(get_current_active_user_id),
    response: Response = None,
    background_tasks: BackgroundTasks = None,
):
    try:
        account = await getActiveInstagramAccount(user_id)
    except HTTPException as e:
        if e.status_code != 400:
            raise
        account = None
    summary, meta = await _get_dashboard_summary_readthrough(
        user_id,
        account,
        background_tasks=background_tasks,
    )
    if response is not None:
        response.headers['X-Dashboard-Summary-Time'] = str(int(meta.get('duration_ms') or 0))
        response.headers['X-Dashboard-Summary-Slowest'] = str(meta.get('slowest') or 'unknown')
        response.headers['X-Dashboard-Summary-Source'] = str(meta.get('source') or 'unknown')
    return summary


@api.get('/dashboard/stats')
async def dashboard_stats(user_id: str = Depends(get_current_active_user_id)):
    started = datetime.utcnow()
    try:
        account = await getActiveInstagramAccount(user_id)
    except HTTPException as e:
        if e.status_code != 400:
            raise
        account = None

    include_unscoped = await _dashboard_include_unscoped(user_id)
    ig_owner_id = _dashboard_key(
        (account or {}).get('instagramAccountId') or (account or {}).get('igUserId')
    )

    autos = await _dashboard_scoped_docs('automations', user_id, account, include_unscoped, 5000)
    comments = await _dashboard_scoped_docs('comments', user_id, account, include_unscoped, 5000)
    conversations = await _dashboard_scoped_docs('conversations', user_id, account, include_unscoped, 5000)
    dm_logs = await _dashboard_scoped_docs('dm_logs', user_id, account, include_unscoped, 5000)
    sessions = await _dashboard_scoped_docs('comment_dm_sessions', user_id, account, include_unscoped, 5000)
    contacts = await _dashboard_scoped_docs('contacts', user_id, account, include_unscoped, 5000)
    click_events = await _dashboard_scoped_docs('link_click_events', user_id, account, include_unscoped, 10000)

    active_automations = sum(1 for auto in autos if _automation_active(auto))
    automation_sent = sum(int(auto.get('sent') or 0) for auto in autos)

    contacts_seen: set = set()

    def add_contact(*values):
        for value in values:
            key = _dashboard_key(value)
            if key and key != ig_owner_id:
                contacts_seen.add(key)
                return

    for c in comments:
        add_contact(c.get('commenter_id'), c.get('commenterId'), c.get('commenter_username'))
    for log in dm_logs:
        if not log.get('is_echo'):
            add_contact(log.get('sender_id'), log.get('senderId'))
    for session in sessions:
        add_contact(session.get('recipient_id'), session.get('instagramUserId'))
    for conv in conversations:
        contact = conv.get('contact') or {}
        add_contact(contact.get('ig_id'), contact.get('instagramUserId'), contact.get('username'))
    for contact in contacts:
        if account or include_unscoped or not _dashboard_is_unscoped(contact):
            add_contact(contact.get('ig_id'), contact.get('instagramUserId'), contact.get('username'))

    from collections import OrderedDict
    today = datetime.utcnow().date()
    buckets = OrderedDict()
    for i in range(6, -1, -1):
        d = today - timedelta(days=i)
        buckets[d.isoformat()] = {
            'day': d.strftime('%a'),
            'date': d.isoformat(),
            'messages': 0,
            'conversions': 0,
        }

    def add_message(day: Optional[str], count: int = 1):
        if day in buckets and count > 0:
            buckets[day]['messages'] += count

    event_messages = 0
    source_counts = {
        'comments': 0,
        'dm_logs': 0,
        'comment_dm_sessions': 0,
        'conversations': 0,
        'legacy_automation_sent': 0,
    }
    for c in comments:
        sent_count = _dashboard_comment_message_count(c)
        if sent_count:
            event_messages += sent_count
            source_counts['comments'] += sent_count
            add_message(_sent_day(c), sent_count)

    for log in dm_logs:
        if _dashboard_success_status(log.get('status')):
            event_messages += 1
            source_counts['dm_logs'] += 1
            add_message(_sent_day(log), 1)

    for session in sessions:
        if _dashboard_success_status(session.get('status')):
            event_messages += 1
            source_counts['comment_dm_sessions'] += 1
            add_message(_sent_day(session), 1)

    for conv in conversations:
        for m in conv.get('messages', []) or []:
            if _dashboard_conversation_message_sent(m):
                event_messages += 1
                source_counts['conversations'] += 1
                add_message(_sent_day(m), 1)

    # automation.sent is a legacy aggregate counter. It is safe only for old
    # single-account workspaces where no account-specific sent-event logs exist.
    if include_unscoped and automation_sent > event_messages:
        remaining = automation_sent - event_messages
        for auto in autos:
            if remaining <= 0:
                break
            count = min(int(auto.get('sent') or 0), remaining)
            add_message(_sent_day(auto), count)
            source_counts['legacy_automation_sent'] += count
            remaining -= count

    messages_sent = event_messages + source_counts['legacy_automation_sent']
    converted_contacts: set = set()
    click_events_count = 0
    for event in click_events:
        click_events_count += 1
        contact_key = _dashboard_key(
            event.get('instagramUserId')
            or event.get('recipient_id')
            or event.get('instagram_user_id')
        )
        if contact_key and contact_key != ig_owner_id:
            contacts_seen.add(contact_key)
            converted_contacts.add(contact_key)
            clicked_day = _dashboard_dt(event.get('clickedAt'), event.get('createdAt'), event.get('created'))
            if clicked_day:
                day_key = clicked_day.date().isoformat()
                if day_key in buckets:
                    bucket = buckets[day_key]
                    day_conversions = bucket.setdefault('_convertedUsers', set())
                    day_conversions.add(contact_key)

    for bucket in buckets.values():
        day_conversions = bucket.pop('_convertedUsers', set())
        bucket['conversions'] = len(day_conversions)

    conversions = len(converted_contacts)
    conversion_rate = 0 if not contacts_seen else round((conversions / len(contacts_seen)) * 100, 1)
    weekly_performance = list(buckets.values())
    instagram_account_id = (account or {}).get('instagramAccountId') or (account or {}).get('igUserId')

    duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
    logger.info(
        'dashboard_stats_calculated user_id=%s activeAccountId=%s instagramAccountId=%s '
        'messagesSent=%s sources=%s totalContacts=%s activeAutomations=%s '
        'linkClicks=%s convertedContacts=%s durationMs=%s',
        user_id,
        (account or {}).get('id'),
        instagram_account_id,
        messages_sent,
        source_counts,
        len(contacts_seen),
        active_automations,
        click_events_count,
        conversions,
        duration_ms,
    )

    response = {
        'totalContacts': len(contacts_seen),
        'activeAutomations': active_automations,
        'messagesSent': messages_sent,
        'conversionRate': conversion_rate,
        'weeklyPerformance': weekly_performance,
        'linkClicks': click_events_count,
        'convertedContacts': conversions,
        'instagram': {
            'connected': bool(account and account.get('connectionValid')),
            'username': (account or {}).get('username') or None,
            'activeAccountId': (account or {}).get('id') or None,
            'instagramAccountId': instagram_account_id or None,
        },
        'conversionTrackingImplemented': True,
        'commentsLogged': len(comments),
        # Backward-compatible keys used by older frontend bundles.
        'total_contacts': len(contacts_seen),
        'active_automations': active_automations,
        'messages_sent': messages_sent,
        'conversion_rate': conversion_rate,
        'weekly_chart': weekly_performance,
        'link_clicks': click_events_count,
        'converted_contacts': conversions,
        'activeInstagramAccountId': (account or {}).get('id') or None,
        'current_instagram_account_id': instagram_account_id or None,
        'comments_logged': len(comments),
    }
    return response


@api.get('/usage/current')
async def current_usage(user_id: str = Depends(get_current_active_user_id)):
    """Phase 2.1 + 2.2: monthly usage for the current user PLUS the plan
    limits and remaining counters. billing_enabled stays False until
    Phase 3 lands real billing."""
    summary = await get_current_usage_with_limits(user_id)
    # Backward-compatible top-level fields older frontend bundles may read.
    return {
        'event_month': summary['event_month'],
        'counters': summary['counters'],
        'connectedInstagramAccountsCount': summary['connectedInstagramAccountsCount'],
        'activeAutomationsCount': summary['activeAutomationsCount'],
        'plan': summary['plan_key'],
        'plan_key': summary['plan_key'],
        'display_name': summary['display_name'],
        'limits': summary['limits'],
        'remaining': summary['remaining'],
        'exceeded': summary['exceeded'],
        'max_instagram_accounts': summary['max_instagram_accounts'],
        'max_active_automations': summary['max_active_automations'],
        'billing_enabled': False,
    }


@api.get('/observability/status')
async def observability_status_endpoint(user_id: str = Depends(get_current_active_user_id)):
    """Phase 2.5: sanitized observability config for SystemHealth/admin.

    Auth required (any logged-in user). Never echoes the DSN. Tells the
    frontend whether Sentry is configured + the deployed build sha so
    the System Health card can render a green/red dot per service.
    """
    try:
        import observability as _observability  # noqa: WPS433
        status = _observability.observability_status()
    except Exception:
        status = {'sentry_configured': False, 'sentry_initialized': False,
                  'environment': 'unknown', 'build_sha': None, 'service': 'backend'}
    # Frontend signals layered on top — these are populated by the React
    # bundle at runtime; here we only echo back the build sha + env so
    # the admin can see they match.
    return {
        'backend': status,
        'frontend': {
            'sentry_dsn_env_var': 'REACT_APP_SENTRY_DSN',
            'posthog_key_env_var': 'REACT_APP_POSTHOG_KEY',
            'note': 'frontend reports its own configured-state at runtime',
        },
    }


@api.get('/plans')
async def list_plans():
    """Public list of plan tiers. No auth required: prices and limits are
    not secret. billing_enabled is False on every entry."""
    return {'plans': _plans.all_plan_summaries(), 'billing_enabled': False}


@api.get('/plan/current')
async def current_plan(user_id: str = Depends(get_current_active_user_id)):
    """The caller's current plan + its limits + this month's usage."""
    summary = await get_current_usage_with_limits(user_id)
    return summary


@api.post('/admin/users/{target_user_id}/plan')
async def admin_assign_user_plan(
    target_user_id: str,
    body: dict = Body(...),
    user_id: str = Depends(get_current_active_user_id),
):
    """Manually assign a plan to a user. Requires admin.plans.assign.
    Independent of ENABLE_ADMIN_REPAIR_TOOLS. No Stripe."""
    caller, _role = await _require_admin_permission(user_id, _admin_roles.PERM_PLANS_ASSIGN)
    plan_key = (body or {}).get('plan_key')
    reason = (body or {}).get('reason') or 'manual_admin_assignment'
    if not _plans.is_valid_plan_key(plan_key):
        raise HTTPException(400, f'plan_key must be one of: {", ".join(_plans.PLAN_KEYS)}')
    plan = await assign_user_plan(target_user_id, plan_key, assigned_by=user_id, reason=reason)
    logger.info(
        'admin_plan_assigned target_user_id=%s plan_key=%s assigned_by=%s',
        target_user_id, plan_key, user_id,
    )
    # Phase 2.4 admin audit log entry. Best-effort — never blocks the action.
    await _record_admin_action(
        caller,
        action='plan_assign',
        target_user_id=target_user_id,
        metadata={'plan_key': plan_key, 'reason_length': len(str(reason or ''))},
    )
    return {
        'ok': True,
        'user_id': target_user_id,
        'plan_key': plan['plan_key'],
        'display_name': plan['display_name'],
        'billing_enabled': False,
    }


@api.get('/admin/users/{target_user_id}/plan')
async def admin_get_user_plan(
    target_user_id: str,
    user_id: str = Depends(get_current_active_user_id),
):
    await _require_admin_permission(user_id, _admin_roles.PERM_USERS_VIEW)
    summary = await get_current_usage_with_limits(target_user_id)
    return {**summary, 'user_id': target_user_id}


@api.get('/admin/usage/{target_user_id}')
async def admin_user_usage(
    target_user_id: str,
    month: Optional[str] = None,
    user_id: str = Depends(get_current_active_user_id),
):
    """Admin-only: return monthly usage counters for any user.

    Requires admin.users.view (admin / support / owner). Independent of
    ENABLE_ADMIN_REPAIR_TOOLS — read-only usage stats are safe enough
    for any admin role.
    """
    await _require_admin_permission(user_id, _admin_roles.PERM_USERS_VIEW)
    event_month = (month or _usage_month(datetime.utcnow())).strip()
    if not re.fullmatch(r'\d{4}-\d{2}', event_month):
        raise HTTPException(400, 'month must be YYYY-MM')

    usage = await db.monthly_usage.find_one(
        _monthly_usage_user_query(target_user_id, event_month)
    ) or {}
    counters = {field: int(usage.get(field) or 0) for field in USAGE_COUNTER_FIELDS}
    snapshots = await _usage_snapshots_for_user(target_user_id)
    return {
        'user_id': target_user_id,
        'event_month': event_month,
        'counters': counters,
        'connectedInstagramAccountsCount': int(
            usage.get('instagram_accounts_connected_snapshot')
            or snapshots.get('instagram_accounts_connected_snapshot')
            or 0
        ),
        'activeAutomationsCount': int(
            usage.get('active_automations_snapshot')
            or snapshots.get('active_automations_snapshot')
            or 0
        ),
        'plan': 'free',
        'billing_enabled': False,
    }


# ---------------- admin console v0 (Phase 2.4) ----------------
# Authenticated, sanitized admin endpoints for the product owner.
# Auth: ADMIN_EMAILS only. Independent of ENABLE_ADMIN_REPAIR_TOOLS.
# Privacy: never returns raw comment / reply / DM text, tokens, or
# Authorization values. Returns counts, ids, statuses, hashes only.

import admin_roles as _admin_roles  # Phase 2.6: role + permission catalogue


async def _resolve_admin_role(user: Optional[dict]) -> tuple[Optional[str], bool]:
    """Return (role, bootstrap_owner) for a user record.

    Lookup order:
      1. ADMIN_EMAILS bootstrap → owner. On first call we lazily insert
         an admin_members row so the env list can be removed once the
         team is in place.
      2. admin_members row keyed by user_id (then email fallback).
      3. None → not an admin.

    Disabled (disabled_at != None) admin_members rows return None.
    """
    if not user:
        return None, False
    email = _normalize_email_value(user.get('normalized_email') or user.get('email'))
    user_id = user.get('id')
    bootstrap_owner = bool(email and email in ADMIN_EMAILS)
    member = None
    try:
        if user_id:
            member = await db.admin_members.find_one({'user_id': user_id})
        if not member and email:
            member = await db.admin_members.find_one({'email': email})
    except Exception:
        member = None
    if member and member.get('disabled_at'):
        # Disabled members lose admin access regardless of bootstrap, except
        # we still treat ADMIN_EMAILS as a recovery path so the owner can
        # re-enable themselves via the env list.
        if not bootstrap_owner:
            return None, False
        member = None
    if member and _admin_roles.is_admin_role(member.get('role')):
        return member.get('role'), bootstrap_owner
    if bootstrap_owner:
        # Lazy insert so future role lookups skip ADMIN_EMAILS.
        try:
            now = datetime.utcnow()
            await db.admin_members.update_one(
                {'user_id': user_id} if user_id else {'email': email},
                {
                    '$setOnInsert': {
                        'id': secrets.token_urlsafe(12),
                        'user_id': user_id,
                        'email': email,
                        'role': _admin_roles.ROLE_OWNER,
                        'added_by_user_id': None,
                        'added_by_email': 'ADMIN_EMAILS',
                        'created_at': now,
                        'updated_at': now,
                    },
                },
                upsert=True,
            )
        except Exception:
            pass
        return _admin_roles.ROLE_OWNER, True
    return None, False


async def _require_admin(user_id: str) -> dict:
    """Backward-compat: any admin role passes. Use _require_admin_permission
    for finer-grained gates."""
    user = await db.users.find_one({'id': user_id})
    role, _ = await _resolve_admin_role(user)
    if not _admin_roles.is_admin_role(role):
        raise HTTPException(403, 'Admin access required')
    return user


async def _require_admin_permission(user_id: str, permission: str) -> tuple[dict, str]:
    """Permission-based gate. Returns (user, role) on success, 403 otherwise."""
    user = await db.users.find_one({'id': user_id})
    role, _ = await _resolve_admin_role(user)
    if not _admin_roles.has_permission(role, permission):
        raise HTTPException(403, 'Admin permission required')
    return user, role


async def _record_admin_action(
    admin_user: dict,
    action: str,
    target_user_id: Optional[str] = None,
    target_automation_id: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> None:
    """Append a sanitized audit-log row. Failures never break the calling
    admin endpoint — audit log is best-effort but loud on failure."""
    try:
        await db.admin_audit_logs.insert_one({
            '_id': secrets.token_urlsafe(12),
            'id': secrets.token_urlsafe(12),
            'admin_user_id': str((admin_user or {}).get('id') or ''),
            'admin_email': ((admin_user or {}).get('email') or '').lower(),
            'action': action,
            'target_user_id': str(target_user_id) if target_user_id else None,
            'target_automation_id': str(target_automation_id) if target_automation_id else None,
            'metadata': _sanitize_usage_metadata(metadata or {}),
            'created_at': datetime.utcnow(),
        })
    except Exception as e:
        logger.warning(
            'admin_audit_log_failed action=%s admin=%s reason=%s',
            action, ((admin_user or {}).get('email') or '').lower(), str(e)[:120],
        )


@api.get('/admin/me')
async def admin_me(user_id: str = Depends(get_current_active_user_id)):
    """Phase 2.6: returns the caller's role + permission set.

    Never raises 403 — the frontend uses this to decide what to render.
    Non-admins simply see is_admin=False and admin nav stays hidden.
    """
    user = await db.users.find_one({'id': user_id})
    email = ((user or {}).get('email') or '').lower()
    role, bootstrap_owner = await _resolve_admin_role(user)
    return {
        'is_admin': _admin_roles.is_admin_role(role),
        'role': role,
        'permissions': sorted(_admin_roles.get_role_permissions(role)),
        'bootstrap_owner': bool(bootstrap_owner),
        'email': email or None,
        'user_id': (user or {}).get('id'),
    }


@api.get('/admin/overview')
async def admin_overview(user_id: str = Depends(get_current_active_user_id)):
    """Sanitized aggregate snapshot of the SaaS — totals + plan distribution
    + this month's usage roll-up + failure / queue health counts.

    Phase 2.18D: all read paths now run in parallel via asyncio.gather.
    Previously this endpoint ran 13 count_documents + 2 cursor iterations
    sequentially, paying one MongoDB round-trip per call. With ~20-50ms
    latency per call to MongoDB Atlas that meant 0.5-1.5s before any byte
    came back. The aggregate is now bounded by the slowest single read.
    """
    started = datetime.utcnow()
    await _require_admin_permission(user_id, _admin_roles.PERM_OVERVIEW_VIEW)
    now = datetime.utcnow()
    today_start = datetime(now.year, now.month, now.day)
    seven_days = today_start - timedelta(days=7)
    thirty_days = today_start - timedelta(days=30)
    month = _usage_month(now)

    async def _safe_usage_totals():
        totals = {field: 0 for field in USAGE_COUNTER_FIELDS}
        try:
            cursor = db.monthly_usage.find(_monthly_usage_user_scope_query(month))
            async for row in cursor:
                for field in USAGE_COUNTER_FIELDS:
                    totals[field] += int(row.get(field) or 0)
        except Exception:
            pass
        return totals

    async def _safe_plan_distribution():
        try:
            return await _effective_plan_distribution()
        except Exception:
            return {key: 0 for key in _plans.PLAN_KEYS}

    (
        total_users,
        active_users,
        suspended_users,
        deleted_users,
        users_today,
        users_7d,
        users_30d,
        total_ig,
        connected_ig,
        total_autos,
        active_autos,
        usage_totals,
        plan_distribution,
        plan_limited,
        retryable_failures,
        permanent_failures,
        queue_pending,
    ) = await asyncio.gather(
        db.users.count_documents({}),
        db.users.count_documents({'status': {'$nin': ['suspended', 'deleted']}}),
        db.users.count_documents({'status': 'suspended'}),
        db.users.count_documents({'status': 'deleted'}),
        db.users.count_documents({'created_at': {'$gte': today_start}}),
        db.users.count_documents({'created_at': {'$gte': seven_days}}),
        db.users.count_documents({'created_at': {'$gte': thirty_days}}),
        db.instagram_accounts.count_documents({}),
        db.instagram_accounts.count_documents({'connectionValid': True}),
        db.automations.count_documents({}),
        # Phase 2.18Q: align with _automation_active() — legacy rows
        # may have status missing and use enabled=true / isActive=true
        # instead, which the dashboard's per-user counter already
        # honors. Count them as active here too, otherwise the admin
        # overview undercounts and disagrees with the dashboard.
        db.automations.count_documents({'$or': [
            {'status': 'active'},
            {'status': {'$exists': False}, 'enabled': True},
            {'status': None, 'enabled': True},
            {'status': '', 'enabled': True},
        ]}),
        _safe_usage_totals(),
        _safe_plan_distribution(),
        db.comments.count_documents({'action_status': 'plan_limited'}),
        db.comments.count_documents({'action_status': 'failed_retryable'}),
        db.comments.count_documents({
            'action_status': {'$in': ['failed_permanent', 'failed_retry_exhausted']},
        }),
        db.comments.count_documents({'queued': True}),
    )

    user_plan_rows = sum(plan_distribution.values())
    plan_distribution['free'] += max(0, total_users - user_plan_rows)

    duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
    logger.info('admin_overview_calculated user_id=%s durationMs=%s', user_id, duration_ms)

    return {
        'total_users': total_users,
        'active_users': active_users,
        'suspended_users': suspended_users,
        'deleted_users': deleted_users,
        'users_created_today': users_today,
        'users_created_7d': users_7d,
        'users_created_30d': users_30d,
        'total_instagram_accounts': total_ig,
        'connected_instagram_accounts': connected_ig,
        'total_automations': total_autos,
        'active_automations': active_autos,
        'event_month': month,
        'current_month_usage_totals': usage_totals,
        'plan_distribution': plan_distribution,
        'plan_limited_counts': plan_limited,
        'retryable_failure_counts': retryable_failures,
        'permanent_failure_counts': permanent_failures,
        'queue_health': {
            'pending': queue_pending,
            'failed_retryable': retryable_failures,
            'failed_permanent': permanent_failures,
            'plan_limited': plan_limited,
        },
        'billing_enabled': False,
    }


@api.get('/admin/users')
async def admin_users_list(
    page: int = 1,
    page_size: int = 25,
    search: Optional[str] = None,
    plan_key: Optional[str] = None,
    sort: Optional[str] = None,
    user_id: str = Depends(get_current_active_user_id),
):
    """Paginated, sanitized user list with usage roll-ups."""
    started = datetime.utcnow()
    await _require_admin_permission(user_id, _admin_roles.PERM_USERS_VIEW)
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 25), 100))

    query: dict = {}
    s = _bounded_search_text(search, field='search', max_len=80)
    if s:
        # Match on email or id (sanitized regex).
        esc = re.escape(s)
        query['$or'] = [
            {'email': {'$regex': esc, '$options': 'i'}},
            {'id': s},
        ]

    # Plan filter requires a join — we resolve in two passes when filtering.
    plan_filter_user_ids: Optional[set] = None
    if plan_key:
        if not _plans.is_valid_plan_key(plan_key):
            raise HTTPException(400, f'Invalid plan_key: {plan_key}')
        plan_filter_user_ids = set()
        effective_plan_by_user = await _effective_user_plan_key_map()
        if plan_key == _plans.DEFAULT_PLAN_KEY:
            # 'free' = users with no effective plan row OR latest assignment is free.
            cursor2 = db.users.find({})
            async for row in cursor2:
                uid = row.get('id')
                if effective_plan_by_user.get(uid, _plans.DEFAULT_PLAN_KEY) == _plans.DEFAULT_PLAN_KEY:
                    plan_filter_user_ids.add(row.get('id'))
        else:
            plan_filter_user_ids = {
                uid for uid, effective_plan in effective_plan_by_user.items()
                if effective_plan == plan_key
            }
        if plan_filter_user_ids is not None:
            query['id'] = {'$in': list(plan_filter_user_ids)}

    sort_spec = [('created_at', -1)]
    if sort not in (None, '', 'created_at_desc', 'email', 'created_at_asc'):
        raise HTTPException(400, 'Invalid sort')
    if sort == 'email':
        sort_spec = [('email', 1)]
    elif sort == 'created_at_asc':
        sort_spec = [('created_at', 1)]

    total = await db.users.count_documents(query)
    skip = (page - 1) * page_size
    cursor = db.users.find(query).sort(sort_spec).skip(skip).limit(page_size)
    rows = await cursor.to_list(page_size)

    month = _usage_month(datetime.utcnow())
    items = []
    for u in rows:
        uid = u.get('id') or ''
        plan = await get_user_plan(uid)
        usage = await db.monthly_usage.find_one(
            _monthly_usage_user_query(uid, month)
        ) or {}
        snapshots = await _usage_snapshots_for_user(uid)
        counters = {f: int(usage.get(f) or 0) for f in USAGE_COUNTER_FIELDS}
        exceeded = {}
        for limit_key, counter_field in _plans.LIMIT_TO_COUNTER_FIELD.items():
            used = int(counters.get(counter_field) or 0)
            limit_value = plan.get(limit_key)
            exceeded[limit_key] = bool(
                limit_value is not None and used >= int(limit_value)
            )
        items.append({
            'user_id': uid,
            'email': u.get('email'),
            'created_at': (
                u.get('created_at').isoformat()
                if isinstance(u.get('created_at'), datetime) else u.get('created_at')
            ),
            'last_seen_at': (
                u.get('last_seen_at').isoformat()
                if isinstance(u.get('last_seen_at'), datetime) else u.get('last_seen_at')
            ),
            'plan_key': plan['plan_key'],
            'billing_status': plan['_assignment'].get('billing_status') or 'manual',
            'billing_enabled': False,
            'instagram_accounts_count': int(snapshots.get('instagram_accounts_connected_snapshot') or 0),
            'active_automations_count': int(snapshots.get('active_automations_snapshot') or 0),
            'current_month_usage': {
                'comments_processed': counters.get('comments_processed', 0),
                'public_replies_sent': counters.get('public_replies_sent', 0),
                'dms_sent': counters.get('dms_sent', 0),
                'links_clicked': counters.get('links_clicked', 0),
            },
            'exceeded': exceeded,
        })

    duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
    logger.info(
        'admin_users_list_calculated user_id=%s resultCount=%s durationMs=%s',
        user_id, len(items), duration_ms,
    )
    return {
        'items': items,
        'pagination': {
            'page': page,
            'page_size': page_size,
            'total': total,
            'total_pages': max(1, (total + page_size - 1) // page_size),
        },
        'event_month': month,
        'billing_enabled': False,
    }


@api.get('/admin/users/{target_user_id}/detail')
async def admin_user_detail(
    target_user_id: str,
    user_id: str = Depends(get_current_active_user_id),
):
    """Sanitized full profile of one user — plan, usage, accounts,
    automations, recent failures. Raw text is NEVER returned; comment/
    reply/DM bodies are not in this response."""
    started = datetime.utcnow()
    await _require_admin_permission(user_id, _admin_roles.PERM_USERS_VIEW)
    user = await db.users.find_one({'id': target_user_id})
    if not user:
        raise HTTPException(404, 'User not found')

    plan = await get_user_plan(target_user_id)
    usage_now = await get_current_usage_with_limits(target_user_id)
    last_month_dt = datetime.utcnow().replace(day=1) - timedelta(days=1)
    last_month_key = _usage_month(last_month_dt)
    last_month = await db.monthly_usage.find_one(
        _monthly_usage_user_query(target_user_id, last_month_key)
    ) or {}
    last_month_counters = {f: int(last_month.get(f) or 0) for f in USAGE_COUNTER_FIELDS}

    # Instagram accounts (no tokens)
    accounts = []
    cursor = db.instagram_accounts.find(
        {'$or': [{'userId': target_user_id}, {'user_id': target_user_id}]}
    )
    async for acc in cursor:
        canonical_ig_id = _canonical_instagram_account_id(
            acc.get('instagramAccountId') or acc.get('igUserId')
        )
        account_usage = await _instagram_monthly_counters(canonical_ig_id) if canonical_ig_id else {
            field: 0 for field in USAGE_COUNTER_FIELDS
        }
        trial_claim = None
        if canonical_ig_id:
            trial_claim = await db.instagram_account_trial_claims.find_one({
                'instagram_account_id': canonical_ig_id,
            })
        ownership_status = acc.get('ownershipStatus') or (
            'active_owner' if acc.get('connectionValid') and acc.get('isActive') is not False
            else 'disconnected'
        )
        accounts.append({
            'id': acc.get('id'),
            'instagram_account_id': canonical_ig_id,
            'instagram_account_id_partial': _safe_partial_identifier(canonical_ig_id),
            'instagram_account_id_hash': _hash_tracking_value(canonical_ig_id) if canonical_ig_id else None,
            'username': acc.get('username'),
            'connectionValid': bool(acc.get('connectionValid')),
            'tokenSource': acc.get('tokenSource'),
            'tokenExpiresAt': (
                acc.get('tokenExpiresAt').isoformat()
                if isinstance(acc.get('tokenExpiresAt'), datetime) else acc.get('tokenExpiresAt')
            ),
            'connected_at': (
                acc.get('created').isoformat()
                if isinstance(acc.get('created'), datetime) else acc.get('created')
            ),
            'active': bool(acc.get('active') or acc.get('isCurrent')),
            'ownership_status': ownership_status,
            'usage_subject': {
                'type': 'instagram_account',
                'id_partial': _safe_partial_identifier(canonical_ig_id),
            } if canonical_ig_id else None,
            'prior_usage_history': any(int(account_usage.get(field) or 0) > 0 for field in USAGE_COUNTER_FIELDS),
            'trial_claim_exists': bool(trial_claim),
        })

    # Automations
    automations = []
    auto_cursor = db.automations.find({'user_id': target_user_id}).sort('updated', -1).limit(50)
    async for a in auto_cursor:
        automations.append({
            'automation_id': a.get('id'),
            'name': a.get('name'),
            'status': a.get('status'),
            'active': (a.get('status') == 'active'),
            'post_scope': a.get('post_scope'),
            'selected_media_id': a.get('media_id'),
            'created_at': (
                a.get('createdAt').isoformat()
                if isinstance(a.get('createdAt'), datetime) else a.get('createdAt')
            ),
            'updated_at': (
                a.get('updated').isoformat()
                if isinstance(a.get('updated'), datetime) else a.get('updated')
            ),
        })

    # Recent failures (no raw text)
    fail_query = {
        'user_id': target_user_id,
        'action_status': {'$in': [
            'failed', 'failed_retryable', 'failed_permanent',
            'failed_retry_exhausted', 'partial_success', 'plan_limited',
        ]},
    }
    failures = []
    fail_cursor = db.comments.find(fail_query).sort('updated', -1).limit(25)
    async for c in fail_cursor:
        failures.append({
            'comment_id': c.get('id'),
            'ig_comment_id': c.get('ig_comment_id') or c.get('igCommentId'),
            'media_id': c.get('media_id') or c.get('mediaId'),
            'reply_status': c.get('reply_status') or c.get('replyStatus'),
            'dm_status': c.get('dm_status') or c.get('dmStatus'),
            'action_status': c.get('action_status') or c.get('actionStatus'),
            'skip_reason': c.get('skip_reason') or c.get('skipReason'),
            'reply_failure_reason': c.get('reply_failure_reason'),
            'dm_failure_reason': c.get('dm_failure_reason'),
            'attempts': int(c.get('attempts') or 0),
            'created_at': (
                c.get('created').isoformat()
                if isinstance(c.get('created'), datetime) else c.get('created')
            ),
            'updated_at': (
                c.get('updated').isoformat()
                if isinstance(c.get('updated'), datetime) else c.get('updated')
            ),
        })

    duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
    logger.info(
        'admin_user_detail_calculated user_id=%s target_user_id=%s accounts=%s automations=%s failures=%s durationMs=%s',
        user_id, target_user_id, len(accounts), len(automations), len(failures), duration_ms,
    )
    return {
        'user_id': target_user_id,
        'profile': {
            'user_id': target_user_id,
            'email': user.get('email'),
            'username': user.get('username'),
            'name': user.get('name'),
            # Phase 2.7: Google identity fields (boolean only, never sub).
            'google_linked': bool(user.get('google_sub')),
            'email_verified': bool(user.get('email_verified')),
            'auth_provider': user.get('auth_provider'),
            'linked_providers': user.get('linked_providers') or [],
            # Phase 2.8: status fields.
            'status': _user_status(user),
            'suspended_at': (
                user.get('suspended_at').isoformat()
                if isinstance(user.get('suspended_at'), datetime) else user.get('suspended_at')
            ),
            'suspended_by': user.get('suspended_by'),
            'suspended_reason_length': user.get('suspended_reason_length'),
            'deleted_at': (
                user.get('deleted_at').isoformat()
                if isinstance(user.get('deleted_at'), datetime) else user.get('deleted_at')
            ),
            'deleted_by': user.get('deleted_by'),
            'created_at': (
                user.get('created_at').isoformat()
                if isinstance(user.get('created_at'), datetime) else user.get('created_at')
            ),
            'last_seen_at': (
                user.get('last_seen_at').isoformat()
                if isinstance(user.get('last_seen_at'), datetime) else user.get('last_seen_at')
            ),
        },
        'plan': {
            'plan_key': plan['plan_key'],
            'display_name': plan['display_name'],
            'billing_enabled': False,
            **plan.get('_assignment', {}),
        },
        'active_overrides': [
            _overrides.safe_override_summary(r)
            for r in await get_active_user_limit_overrides(target_user_id)
        ],
        'usage_current_month': usage_now,
        'usage_last_month': {
            'event_month': last_month_key,
            'counters': last_month_counters,
        },
        'instagram_accounts': accounts,
        'automations': automations,
        'recent_failures': failures,
        'billing_enabled': False,
    }


@api.post('/admin/automations/{automation_id}/disable')
async def admin_disable_automation(
    automation_id: str,
    body: Optional[dict] = Body(None),
    user_id: str = Depends(get_current_active_user_id),
):
    """Set automation.status='paused' (does NOT delete). Logs an audit row."""
    admin_user, _role = await _require_admin_permission(user_id, _admin_roles.PERM_AUTOMATIONS_DISABLE)
    automation = await db.automations.find_one({'id': automation_id})
    if not automation:
        raise HTTPException(404, 'Automation not found')
    reason = ((body or {}).get('reason') or '')[:200]
    now = datetime.utcnow()
    await db.automations.update_one(
        {'id': automation_id},
        {'$set': {
            'status': 'paused',
            'admin_disabled_at': now,
            'admin_disabled_by': user_id,
            'admin_disable_reason': reason or None,
            'updated': now,
            'updatedAt': now,
        }},
    )
    # Phase 2.18O: the dashboard summary's activeAutomations count and
    # topAutomations list both embed status. Purge the snapshot so the
    # paused state shows on the next /dashboard/summary call.
    try:
        await invalidate_dashboard_summary(automation.get('user_id'))
    except Exception:
        pass
    await _record_admin_action(
        admin_user,
        action='automation_disable',
        target_user_id=automation.get('user_id'),
        target_automation_id=automation_id,
        metadata={'reason_present': bool(reason), 'reason_length': len(reason)},
    )
    return {
        'ok': True,
        'automation_id': automation_id,
        'status': 'paused',
        'admin_disabled_at': now.isoformat(),
    }


@api.get('/admin/audit-log')
async def admin_audit_log(
    limit: int = 50,
    user_id: str = Depends(get_current_active_user_id),
):
    """Recent admin actions (sanitized). Useful for the console activity feed."""
    await _require_admin_permission(user_id, _admin_roles.PERM_AUDIT_VIEW)
    limit = max(1, min(int(limit or 50), 200))
    cursor = db.admin_audit_logs.find().sort('created_at', -1).limit(limit)
    rows = await cursor.to_list(limit)
    items = []
    for r in rows:
        items.append({
            'id': r.get('id'),
            'admin_email': r.get('admin_email'),
            'action': r.get('action'),
            'target_user_id': r.get('target_user_id'),
            'target_automation_id': r.get('target_automation_id'),
            'metadata': r.get('metadata') or {},
            'created_at': (
                r.get('created_at').isoformat()
                if isinstance(r.get('created_at'), datetime) else r.get('created_at')
            ),
        })
    return {'items': items, 'count': len(items)}


# ---------------- admin members CRUD (Phase 2.6) ----------------

async def _admin_members_count_owners(exclude_user_id: Optional[str] = None) -> int:
    query = {'role': _admin_roles.ROLE_OWNER, 'disabled_at': None}
    cursor = db.admin_members.find(query)
    count = 0
    async for row in cursor:
        if exclude_user_id and row.get('user_id') == exclude_user_id:
            continue
        count += 1
    return count


async def _safe_member_row(row: dict) -> dict:
    if not row:
        return {}
    bootstrap = bool((row.get('email') or '').lower() in ADMIN_EMAILS)
    return {
        'user_id': row.get('user_id'),
        'email': row.get('email'),
        'role': row.get('role'),
        'added_by_user_id': row.get('added_by_user_id'),
        'added_by_email': row.get('added_by_email'),
        'created_at': (
            row.get('created_at').isoformat()
            if isinstance(row.get('created_at'), datetime) else row.get('created_at')
        ),
        'updated_at': (
            row.get('updated_at').isoformat()
            if isinstance(row.get('updated_at'), datetime) else row.get('updated_at')
        ),
        'disabled_at': (
            row.get('disabled_at').isoformat()
            if isinstance(row.get('disabled_at'), datetime) else row.get('disabled_at')
        ),
        'bootstrap_owner': bootstrap,
    }


@api.get('/admin/members')
async def admin_members_list(user_id: str = Depends(get_current_active_user_id)):
    """List admin members. Anyone with admin.members.view (admin/owner)."""
    await _require_admin_permission(user_id, _admin_roles.PERM_MEMBERS_VIEW)
    cursor = db.admin_members.find().sort('created_at', -1)
    rows = []
    async for r in cursor:
        rows.append(await _safe_member_row(r))
    return {'items': rows, 'count': len(rows)}


@api.post('/admin/members')
async def admin_members_add(
    body: dict = Body(...),
    user_id: str = Depends(get_current_active_user_id),
):
    """Add a user to the admin team. Owner-only (admin.members.manage)."""
    actor, actor_role = await _require_admin_permission(user_id, _admin_roles.PERM_MEMBERS_MANAGE)
    target_email = _normalize_email_value((body or {}).get('email'))
    new_role = (body or {}).get('role') or _admin_roles.ROLE_VIEWER
    reason = ((body or {}).get('reason') or '')[:200]
    if not target_email:
        raise HTTPException(400, 'email is required')
    if not _admin_roles.is_valid_assignable_role(new_role):
        raise HTTPException(400, f'role must be one of: {", ".join(_admin_roles.ASSIGNABLE_ROLE_KEYS)}')
    if not _admin_roles.can_manage_role(actor_role, None, new_role):
        raise HTTPException(403, 'Insufficient role to assign that role')
    target_user = await _find_user_by_email(target_email)
    if not target_user:
        raise HTTPException(404, 'User not found for that email')
    target_uid = target_user.get('id')
    existing = await db.admin_members.find_one({
        '$or': [{'user_id': target_uid}, {'email': target_email}],
    })
    now = datetime.utcnow()
    update = {
        '$set': {
            'user_id': target_uid,
            'email': target_email,
            'role': new_role,
            'added_by_user_id': user_id,
            'added_by_email': (actor.get('email') or '').lower(),
            'disabled_at': None,
            'updated_at': now,
        },
        '$setOnInsert': {
            'id': secrets.token_urlsafe(12),
            'created_at': now,
        },
    }
    await db.admin_members.update_one(
        {'user_id': target_uid} if target_uid else {'email': target_email},
        update,
        upsert=True,
    )
    await _record_admin_action(
        actor,
        action='admin_member_added',
        target_user_id=target_uid,
        metadata={
            'target_email_hash': _safe_text_hash(target_email)[:12],
            'old_role': (existing or {}).get('role'),
            'new_role': new_role,
            'reason_length': len(reason),
        },
    )
    saved = await db.admin_members.find_one(
        {'user_id': target_uid} if target_uid else {'email': target_email}
    )
    return {'ok': True, 'member': await _safe_member_row(saved)}


@api.patch('/admin/members/{target_user_id}')
async def admin_members_update(
    target_user_id: str,
    body: dict = Body(...),
    user_id: str = Depends(get_current_active_user_id),
):
    """Change a member's role. Owner-only (admin.members.manage)."""
    actor, actor_role = await _require_admin_permission(user_id, _admin_roles.PERM_MEMBERS_MANAGE)
    new_role = (body or {}).get('role')
    reason = ((body or {}).get('reason') or '')[:200]
    if not _admin_roles.is_valid_assignable_role(new_role):
        raise HTTPException(400, f'role must be one of: {", ".join(_admin_roles.ASSIGNABLE_ROLE_KEYS)}')
    member = await db.admin_members.find_one({'user_id': target_user_id})
    if not member:
        raise HTTPException(404, 'Member not found')
    if not _admin_roles.can_manage_role(actor_role, member.get('role'), new_role):
        raise HTTPException(403, 'Insufficient role to change that member')
    # Last-owner invariant.
    if member.get('role') == _admin_roles.ROLE_OWNER and new_role != _admin_roles.ROLE_OWNER:
        owners_left = await _admin_members_count_owners(exclude_user_id=target_user_id)
        if owners_left <= 0:
            raise HTTPException(409, 'Cannot demote the last owner')
    now = datetime.utcnow()
    await db.admin_members.update_one(
        {'user_id': target_user_id},
        {'$set': {
            'role': new_role,
            'disabled_at': None,
            'updated_at': now,
        }},
    )
    await _record_admin_action(
        actor,
        action='admin_member_role_changed',
        target_user_id=target_user_id,
        metadata={
            'old_role': member.get('role'),
            'new_role': new_role,
            'reason_length': len(reason),
        },
    )
    refreshed = await db.admin_members.find_one({'user_id': target_user_id})
    return {'ok': True, 'member': await _safe_member_row(refreshed)}


@api.delete('/admin/members/{target_user_id}')
async def admin_members_remove(
    target_user_id: str,
    user_id: str = Depends(get_current_active_user_id),
):
    """Soft-disable a member. Owner-only (admin.members.manage).

    Cannot remove the last owner.
    """
    actor, actor_role = await _require_admin_permission(user_id, _admin_roles.PERM_MEMBERS_MANAGE)
    member = await db.admin_members.find_one({'user_id': target_user_id})
    if not member:
        raise HTTPException(404, 'Member not found')
    if not _admin_roles.can_manage_role(actor_role, member.get('role')):
        raise HTTPException(403, 'Insufficient role to remove that member')
    if member.get('role') == _admin_roles.ROLE_OWNER:
        owners_left = await _admin_members_count_owners(exclude_user_id=target_user_id)
        if owners_left <= 0:
            raise HTTPException(409, 'Cannot remove the last owner')
    now = datetime.utcnow()
    await db.admin_members.update_one(
        {'user_id': target_user_id},
        {'$set': {'disabled_at': now, 'updated_at': now}},
    )
    await _record_admin_action(
        actor,
        action='admin_member_removed',
        target_user_id=target_user_id,
        metadata={'old_role': member.get('role')},
    )
    refreshed = await db.admin_members.find_one({'user_id': target_user_id})
    return {'ok': True, 'member': await _safe_member_row(refreshed)}


# ---------------- Phase 2.8 admin: allowances + suspend/delete + metrics ----

def _user_status(user: Optional[dict]) -> str:
    """Return user.status defaulting to 'active' for legacy rows."""
    if not user:
        return 'unknown'
    s = (user.get('status') or 'active').lower()
    return s


def _ensure_user_active(user: Optional[dict]) -> None:
    """Raise 403 if the user is suspended/deleted."""
    s = _user_status(user)
    if s == 'suspended':
        raise HTTPException(403, 'account_suspended')
    if s == 'deleted':
        raise HTTPException(403, 'account_deleted')


@api.get('/admin/users/{target_user_id}/limit-overrides')
async def admin_list_user_overrides(
    target_user_id: str,
    user_id: str = Depends(get_current_active_user_id),
):
    """Active + history overrides for a target user. View-only — anyone
    with admin.users.view can read."""
    await _require_admin_permission(user_id, _admin_roles.PERM_USERS_VIEW)
    rows = []
    cursor = db.user_limit_overrides.find({'user_id': target_user_id}).sort('created_at', -1)
    async for r in cursor:
        rows.append(_overrides.safe_override_summary(r))
    active = [r for r in rows if r.get('status') == _overrides.STATUS_ACTIVE]
    return {'items': rows, 'count': len(rows), 'active_count': len(active)}


@api.post('/admin/users/{target_user_id}/limit-overrides')
async def admin_create_user_override(
    target_user_id: str,
    body: dict = Body(...),
    user_id: str = Depends(get_current_active_user_id),
):
    """Create a custom allowance / limit_override / trial_grant.
    Requires admin.plans.assign (admin / owner)."""
    actor, _role = await _require_admin_permission(user_id, _admin_roles.PERM_PLANS_ASSIGN)
    target_user = await db.users.find_one({'id': target_user_id})
    if not target_user:
        raise HTTPException(404, 'User not found')

    body = body or {}
    override_type = body.get('type')
    if not _overrides.is_valid_override_type(override_type):
        raise HTTPException(
            400, f'type must be one of: {", ".join(sorted(_overrides.VALID_OVERRIDE_TYPES))}'
        )
    grant_name = (body.get('grant_name') or '').strip()[:120]
    reason = (body.get('reason') or '').strip()[:500]
    raw_metrics = body.get('metrics') or {}
    if not isinstance(raw_metrics, dict):
        raise HTTPException(400, 'metrics must be an object')
    allowed_keys = set(_overrides.ADDITIVE_KEYS) | set(_overrides.LIMIT_OVERRIDE_KEYS)
    metrics: dict = {}
    for k, v in raw_metrics.items():
        if k not in allowed_keys:
            continue
        try:
            n = int(v)
        except (TypeError, ValueError):
            continue
        if n < 0:
            continue
        metrics[k] = n
    if not metrics:
        raise HTTPException(400, 'metrics must contain at least one allowed numeric field')

    starts_at = _overrides._to_dt(body.get('starts_at')) or datetime.utcnow()
    ends_at = _overrides._to_dt(body.get('ends_at'))
    if ends_at and ends_at <= starts_at:
        raise HTTPException(400, 'ends_at must be after starts_at')
    instagram_trial_account_ids: List[str] = []
    if override_type == _overrides.OVERRIDE_TYPE_TRIAL:
        instagram_trial_account_ids = await _ensure_instagram_trial_claim_available(
            target_user_id,
            'trial_grant',
        )

    now = datetime.utcnow()
    row = {
        '_id': secrets.token_urlsafe(12),
        'id': secrets.token_urlsafe(12),
        'user_id': target_user_id,
        'type': override_type,
        'status': _overrides.STATUS_ACTIVE,
        'grant_name': grant_name or None,
        'reason': reason or None,
        'metrics': metrics,
        'starts_at': starts_at,
        'ends_at': ends_at,
        'created_by_user_id': user_id,
        'created_by_email': (actor.get('email') or '').lower(),
        'created_at': now,
        'updated_at': now,
    }
    await db.user_limit_overrides.insert_one(row)
    if override_type == _overrides.OVERRIDE_TYPE_TRIAL:
        for canonical in instagram_trial_account_ids:
            await _record_instagram_trial_claim(target_user_id, canonical, 'trial_grant')
    # Phase 2.18J: drop the dashboard snapshot so the target user
    # sees the new effective limits immediately on their next visit.
    try:
        await invalidate_dashboard_summary(target_user_id)
    except Exception:
        pass
    await _record_admin_action(
        actor,
        action='user_limit_override_created',
        target_user_id=target_user_id,
        metadata={
            'override_id': row['id'],
            'type': override_type,
            'metric_keys': sorted(list(metrics.keys())),
            'reason_length': len(reason),
        },
    )
    return {'ok': True, 'override': _overrides.safe_override_summary(row)}


@api.patch('/admin/users/{target_user_id}/limit-overrides/{override_id}/revoke')
async def admin_revoke_user_override(
    target_user_id: str,
    override_id: str,
    body: Optional[dict] = Body(None),
    user_id: str = Depends(get_current_active_user_id),
):
    actor, _role = await _require_admin_permission(user_id, _admin_roles.PERM_PLANS_ASSIGN)
    row = await db.user_limit_overrides.find_one({
        'id': override_id, 'user_id': target_user_id,
    })
    if not row:
        raise HTTPException(404, 'Override not found')
    reason = ((body or {}).get('reason') or '').strip()[:500]
    now = datetime.utcnow()
    await db.user_limit_overrides.update_one(
        {'id': override_id},
        {'$set': {
            'status': _overrides.STATUS_REVOKED,
            'revoked_by_user_id': user_id,
            'revoked_by_email': (actor.get('email') or '').lower(),
            'revoked_at': now,
            'updated_at': now,
        }},
    )
    # Phase 2.18J: drop the dashboard snapshot so the revoked override
    # disappears from the target user's effective limits immediately.
    try:
        await invalidate_dashboard_summary(target_user_id)
    except Exception:
        pass
    await _record_admin_action(
        actor,
        action='user_limit_override_revoked',
        target_user_id=target_user_id,
        metadata={'override_id': override_id, 'reason_length': len(reason)},
    )
    refreshed = await db.user_limit_overrides.find_one({'id': override_id})
    return {'ok': True, 'override': _overrides.safe_override_summary(refreshed or row)}


@api.get('/admin/instagram/accounts/by-handle')
async def admin_lookup_instagram_account_by_handle(
    username: str = Query('', description='Instagram username (without @)'),
    instagram_account_id: str = Query('', description='Instagram numeric user id'),
    user_id: str = Depends(get_current_active_user_id),
):
    """Phase 2.18K: admin diagnostic — find every instagram_accounts row
    for a given handle / IG account id, including rows still marked
    connectionValid=True that may be blocking a reconnect. Returns
    sanitized owner + row metadata. No access tokens. Requires
    admin.users.view."""
    await _require_admin_permission(user_id, _admin_roles.PERM_USERS_VIEW)
    needle_username = (username or '').strip().lstrip('@').lower()
    needle_id = (instagram_account_id or '').strip()
    if not (needle_username or needle_id):
        raise HTTPException(400, 'Provide username= or instagram_account_id=')
    query: dict = {'$or': []}
    if needle_username:
        # case-insensitive exact match on username
        query['$or'].append({
            'username': {'$regex': f'^{re.escape(needle_username)}$', '$options': 'i'},
        })
    if needle_id:
        canonical = _canonical_instagram_account_id(needle_id) or needle_id
        query['$or'].append({'instagramAccountId': canonical})
        query['$or'].append({'igUserId': canonical})
    rows = await db.instagram_accounts.find(query).sort('updatedAt', -1).to_list(50)
    out = []
    for row in rows:
        owner_id = row.get('userId') or row.get('user_id') or ''
        owner_email = None
        owner_status = None
        if owner_id:
            owner = await db.users.find_one(
                {'id': owner_id},
                projection={'email': 1, 'status': 1},
            )
            if owner:
                owner_email = (owner.get('email') or '').lower() or None
                owner_status = owner.get('status') or 'active'
        out.append({
            'row_id': row.get('id'),
            'instagramAccountId': row.get('instagramAccountId') or row.get('igUserId'),
            'username': row.get('username'),
            'owner_user_id': owner_id,
            'owner_email': owner_email,
            'owner_status': owner_status,
            'connectionValid': bool(row.get('connectionValid')),
            'isActive': bool(row.get('isActive')),
            'isCurrent': bool(row.get('isCurrent')),
            'refreshStatus': row.get('refreshStatus'),
            'createdAt': _iso_or_none(row.get('createdAt')),
            'updatedAt': _iso_or_none(row.get('updatedAt')),
            'tokenExpiresAt': _iso_or_none(row.get('tokenExpiresAt')),
        })
    return {'count': len(out), 'rows': out}


class AdminForceDisconnectIn(BaseModel):
    row_id: Optional[str] = None
    instagram_account_id: Optional[str] = None
    username: Optional[str] = None
    reason: Optional[str] = None


@api.post('/admin/instagram/accounts/force-disconnect')
async def admin_force_disconnect_instagram_account(
    body: AdminForceDisconnectIn = Body(...),
    user_id: str = Depends(get_current_active_user_id),
):
    """Phase 2.18K: admin force-disconnect.

    Lets an admin (admin.users.manage) clear an orphan / locked
    instagram_accounts row so the legitimate owner can re-OAuth and
    re-link without hitting the 'already connected to another MyChat
    account' guard. Identify the row by row_id, by instagram_account_id,
    or by username — at least one is required. Affected rows are set
    to isActive=False / connectionValid=False / isCurrent=False, the
    refresh lock is cleared, and the action is recorded in the admin
    audit log.
    """
    actor, _role = await _require_admin_permission(user_id, _admin_roles.PERM_USERS_MANAGE)
    if not body.row_id and not body.instagram_account_id and not body.username:
        raise HTTPException(400, 'Provide row_id, instagram_account_id, or username.')
    query: dict = {}
    if body.row_id:
        query = {'id': body.row_id}
    elif body.instagram_account_id:
        canonical = _canonical_instagram_account_id(body.instagram_account_id) or body.instagram_account_id
        query = {'$or': [{'instagramAccountId': canonical}, {'igUserId': canonical}]}
    elif body.username:
        clean_username = body.username.strip().lstrip('@').lower()
        query = {'username': {'$regex': f'^{re.escape(clean_username)}$', '$options': 'i'}}
    affected_rows = await db.instagram_accounts.find(query).to_list(100)
    if not affected_rows:
        raise HTTPException(404, 'No instagram_accounts row matched.')
    affected_user_ids = sorted({
        (r.get('userId') or r.get('user_id') or '') for r in affected_rows if (r.get('userId') or r.get('user_id'))
    })
    now = datetime.utcnow()
    result = await db.instagram_accounts.update_many(
        query,
        {'$set': {
            'isActive': False,
            'isCurrent': False,
            'connectionValid': False,
            'refreshStatus': 'force_disconnected_by_admin',
            'refreshLockedUntil': None,
            'updatedAt': now,
        }},
    )
    # Drop dashboard snapshots for every affected user so their plan
    # connected-account count refreshes the moment they hit /app again.
    for uid in affected_user_ids:
        try:
            await invalidate_dashboard_summary(uid)
        except Exception:
            pass
    await _record_admin_action(
        actor,
        action='instagram_account_force_disconnected',
        target_user_id=affected_user_ids[0] if affected_user_ids else None,
        metadata={
            'rows_modified': getattr(result, 'modified_count', 0),
            'affected_user_count': len(affected_user_ids),
            'identifier': 'row_id' if body.row_id else ('instagram_account_id' if body.instagram_account_id else 'username'),
            'reason_length': len(str(body.reason or '')),
        },
    )
    logger.info(
        'instagram_account_force_disconnected_by_admin admin=%s rows=%s users=%s',
        (actor.get('email') or '').lower(),
        getattr(result, 'modified_count', 0),
        len(affected_user_ids),
    )
    return {
        'ok': True,
        'rows_disconnected': getattr(result, 'modified_count', 0),
        'affected_user_ids': affected_user_ids,
    }


class AdminRestoreIn(BaseModel):
    row_id: Optional[str] = None
    target_user_id: Optional[str] = None
    instagram_account_id: Optional[str] = None
    username: Optional[str] = None
    reason: Optional[str] = None


@api.post('/admin/instagram/accounts/restore')
async def admin_restore_instagram_account(
    body: AdminRestoreIn = Body(...),
    user_id: str = Depends(get_current_active_user_id),
):
    """Phase 2.18L admin restore: re-activate a disconnected
    instagram_accounts row so the legitimate owner does not have to
    re-run OAuth. Useful after an accidental wipe or after the
    Phase 2.18K bulk disconnect dropped a row the user still wants.

    Identify the row by row_id, target_user_id + instagram_account_id,
    target_user_id + username, or just instagram_account_id /
    username when the row is uniquely identifiable. The row must
    still have a stored accessToken — we never invent credentials.

    Requires admin.users.manage. Sets connectionValid=True,
    isActive=True, refreshStatus='restored_by_admin' and clears
    refreshLockedUntil so the regular refresh job picks it up.
    """
    actor, _role = await _require_admin_permission(user_id, _admin_roles.PERM_USERS_MANAGE)
    query: dict = {}
    if body.row_id:
        query['id'] = body.row_id
    else:
        if body.target_user_id:
            query['$or'] = [
                {'userId': body.target_user_id},
                {'user_id': body.target_user_id},
            ]
        if body.instagram_account_id:
            canonical = _canonical_instagram_account_id(body.instagram_account_id) or body.instagram_account_id
            query.setdefault('$and', []).append({
                '$or': [{'instagramAccountId': canonical}, {'igUserId': canonical}],
            })
        if body.username:
            clean_username = body.username.strip().lstrip('@').lower()
            query.setdefault('$and', []).append({
                'username': {'$regex': f'^{re.escape(clean_username)}$', '$options': 'i'},
            })
    if not query:
        raise HTTPException(400, 'Provide row_id, or some combination of target_user_id / instagram_account_id / username.')
    rows = await db.instagram_accounts.find(query).to_list(50)
    if not rows:
        raise HTTPException(404, 'No matching instagram_accounts row.')
    if len(rows) > 1:
        raise HTTPException(
            409,
            f'{len(rows)} rows matched — narrow the filter (use row_id) so we restore exactly one.',
        )
    row = rows[0]
    if not (row.get('accessToken') or '').strip():
        raise HTTPException(
            400,
            'This row has no stored access token. The legitimate owner must run OAuth again from the Settings page.',
        )
    target_uid = row.get('userId') or row.get('user_id')
    if not target_uid:
        raise HTTPException(500, 'Stored row is missing a user_id; cannot restore safely.')
    # If another row is currently active+valid for the same IG account
    # under a DIFFERENT user, that's the existing-owner case — block.
    canonical = _canonical_instagram_account_id(row.get('instagramAccountId') or row.get('igUserId') or '')
    if canonical:
        conflict = await db.instagram_accounts.find_one({
            'instagramAccountId': canonical,
            'isActive': True,
            'connectionValid': True,
            'id': {'$ne': row.get('id')},
        })
        if conflict and (conflict.get('userId') or conflict.get('user_id')) != target_uid:
            raise HTTPException(
                409,
                {
                    'code': 'instagram_account_already_connected',
                    'message': 'Another MyChat account is currently the active owner of this Instagram account. Force-disconnect that one first.',
                },
            )
    now = datetime.utcnow()
    await db.instagram_accounts.update_one(
        {'id': row.get('id')},
        {'$set': {
            'isActive': True,
            'connectionValid': True,
            'refreshStatus': 'restored_by_admin',
            'refreshLockedUntil': None,
            'updatedAt': now,
        }},
    )
    # Mirror back onto users.* so the existing legacy code paths
    # (Sidebar, /api/instagram/profile, OAuth checks) see the user as
    # connected.
    await db.users.update_one(
        {'id': target_uid},
        {'$set': {
            'instagramConnected': True,
            'instagram_connection_valid': True,
            'instagramConnectionValid': True,
            'instagram_connection_blocker': None,
            'instagramHandle': row.get('username') or '',
            'ig_user_id': canonical or row.get('instagramAccountId') or '',
            'meta_access_token': row.get('accessToken') or '',
            'instagram_account_type': row.get('accountType'),
            'tokenExpiresAt': row.get('tokenExpiresAt'),
            'instagram_token_expires_at': row.get('tokenExpiresAt'),
            'active_instagram_account_id': row.get('id'),
        }},
    )
    try:
        await invalidate_dashboard_summary(target_uid)
    except Exception:
        pass
    await _record_admin_action(
        actor,
        action='instagram_account_restored',
        target_user_id=target_uid,
        metadata={
            'row_id': row.get('id'),
            'instagramAccountId_partial': _safe_partial_identifier(row.get('instagramAccountId') or ''),
            'reason_length': len(str(body.reason or '')),
        },
    )
    logger.info(
        'instagram_account_restored_by_admin admin=%s row_id=%s target_user_id=%s',
        (actor.get('email') or '').lower(),
        row.get('id'),
        target_uid,
    )
    return {
        'ok': True,
        'row_id': row.get('id'),
        'target_user_id': target_uid,
        'username': row.get('username'),
        'instagramAccountId': row.get('instagramAccountId'),
    }


@api.get('/admin/users/{target_user_id}/effective-limits')
async def admin_user_effective_limits(
    target_user_id: str,
    user_id: str = Depends(get_current_active_user_id),
):
    await _require_admin_permission(user_id, _admin_roles.PERM_USERS_VIEW)
    summary = await get_current_usage_with_limits(target_user_id)
    overrides = await get_active_user_limit_overrides(target_user_id)
    return {
        **summary,
        'user_id': target_user_id,
        'active_overrides': [_overrides.safe_override_summary(r) for r in overrides],
    }


@api.post('/admin/users/{target_user_id}/revoke-sessions')
async def admin_revoke_user_sessions(
    target_user_id: str,
    body: Optional[dict] = Body(None),
    user_id: str = Depends(get_current_active_user_id),
):
    actor, _role = await _require_admin_permission(user_id, _admin_roles.PERM_USERS_MANAGE)
    if user_id == target_user_id:
        raise HTTPException(403, 'cannot_revoke_self')
    target = await db.users.find_one({'id': target_user_id})
    if not target:
        raise HTTPException(404, 'User not found')
    reason = str((body or {}).get('reason') or '')[:500]
    new_version = await _increment_user_session_version(target_user_id, reason='admin_revoke_sessions')
    await _record_admin_action(
        actor,
        action='user_sessions_revoked',
        target_user_id=target_user_id,
        metadata={'reason_length': len(reason)},
    )
    return {'ok': True, 'user_id': target_user_id, 'session_version': new_version}


def _email_hash_for_diagnostics(email: str) -> str:
    return _hash_identifier(_normalize_email_value(email))[:16]


@api.get('/admin/auth/normalized-email-diagnostics')
async def admin_normalized_email_diagnostics(
    user_id: str = Depends(get_current_active_user_id),
):
    await _require_admin_permission(user_id, _admin_roles.PERM_AUDIT_VIEW)
    groups: Dict[str, int] = {}
    missing = 0
    cursor = db.users.find({}).limit(10000)
    async for user in cursor:
        normalized = _normalize_email_value(user.get('normalized_email') or '')
        if not normalized:
            missing += 1
            normalized = _normalize_email_value(user.get('email') or '')
        if normalized:
            groups[normalized] = groups.get(normalized, 0) + 1
    duplicate_emails = [email for email, count in groups.items() if count > 1]
    return {
        'duplicate_normalized_email_count': sum(groups[email] for email in duplicate_emails),
        'duplicate_groups_count': len(duplicate_emails),
        'sample_hashes': [_email_hash_for_diagnostics(email) for email in duplicate_emails[:10]],
        'users_missing_normalized_email_count': missing,
        'raw_emails_returned': False,
    }


@api.post('/admin/auth/backfill-normalized-email')
async def admin_backfill_normalized_email(
    body: Optional[dict] = Body(None),
    user_id: str = Depends(get_current_active_user_id),
):
    await _require_admin_permission(user_id, _admin_roles.PERM_USERS_MANAGE)
    dry_run = bool((body or {}).get('dry_run', True))
    candidates = []
    groups: Dict[str, int] = {}
    cursor = db.users.find({}).limit(10000)
    async for user in cursor:
        normalized = _normalize_email_value(user.get('normalized_email') or user.get('email') or '')
        if normalized:
            groups[normalized] = groups.get(normalized, 0) + 1
        if not user.get('normalized_email') and normalized:
            candidates.append({'id': user.get('id'), 'normalized_email': normalized})
    conflicts = [email for email, count in groups.items() if count > 1]
    updated = 0
    if not dry_run:
        for item in candidates:
            await db.users.update_one(
                {'id': item['id'], 'normalized_email': {'$exists': False}},
                {'$set': {'normalized_email': item['normalized_email'], 'updated_at': datetime.utcnow()}},
            )
            updated += 1
    return {
        'ok': True,
        'dry_run': dry_run,
        'candidates_count': len(candidates),
        'updated_count': updated,
        'conflict_groups_count': len(conflicts),
        'conflict_hashes': [_email_hash_for_diagnostics(email) for email in conflicts[:10]],
        'raw_emails_returned': False,
    }


# ---- suspend / soft delete ------------------------------------------------

async def _admin_user_status_change(
    actor: dict, target_user_id: str, *,
    status: str, reason: str, action_name: str,
    require_owner: bool = False,
) -> dict:
    target = await db.users.find_one({'id': target_user_id})
    if not target:
        raise HTTPException(404, 'User not found')
    # Cannot demote/remove the last owner via suspend/delete either.
    target_member = await db.admin_members.find_one({'user_id': target_user_id})
    target_role = (target_member or {}).get('role')
    if target_role == _admin_roles.ROLE_OWNER:
        owners_left = await _admin_members_count_owners(exclude_user_id=target_user_id)
        if owners_left <= 0:
            raise HTTPException(409, 'Cannot suspend or delete the last owner')
    now = datetime.utcnow()
    update: dict = {
        'status': status,
        'updated_at': now,
    }
    reason_field = 'suspended_reason_length' if status == 'suspended' else 'delete_reason_length'
    if status == 'suspended':
        update['suspended_at'] = now
        update['suspended_by'] = actor.get('id')
        update[reason_field] = len(reason or '')
    elif status == 'deleted':
        update['deleted_at'] = now
        update['deleted_by'] = actor.get('id')
        update[reason_field] = len(reason or '')
    elif status == 'active':
        # Unsuspend: clear suspension fields but keep history in audit log.
        update['suspended_at'] = None
        update['suspended_by'] = None
    if status == 'deleted':
        # Soft delete pauses all the user's automations to stop further
        # Meta calls. Comments / usage / audit are preserved.
        await db.automations.update_many(
            {'user_id': target_user_id, 'status': 'active'},
            {'$set': {'status': 'paused', 'updated': now, 'updatedAt': now,
                       'admin_disabled_at': now, 'admin_disable_reason': 'soft_delete'}},
        )
        await db.instagram_accounts.update_many(
            {'$or': [{'userId': target_user_id}, {'user_id': target_user_id}]},
            {'$set': {
                'connectionValid': False,
                'instagramConnected': False,
                'isActive': False,
                'ownershipStatus': 'released_soft_deleted',
                'updated': now,
                'updatedAt': now,
            }},
        )
    session_revoke_statuses = {'suspended', 'deleted', 'active'}
    update_op = {'$set': update}
    if status in session_revoke_statuses:
        update_op['$inc'] = {'session_version': 1}
    await db.users.update_one({'id': target_user_id}, update_op)
    # Phase 2.18O cache hygiene: every user-state change must purge
    # the dashboard snapshot for the affected user so the new status
    # (suspended / active / deleted), the paused automations, and the
    # disconnected Instagram accounts are visible on the very next
    # request.
    try:
        await invalidate_dashboard_summary(target_user_id)
    except Exception:
        pass
    await _record_admin_action(
        actor, action=action_name, target_user_id=target_user_id,
        metadata={'reason_length': len(reason or ''), 'new_status': status},
    )
    refreshed = await db.users.find_one({'id': target_user_id})
    return {
        'ok': True,
        'user_id': target_user_id,
        'status': (refreshed or {}).get('status') or status,
    }


@api.post('/admin/users/{target_user_id}/suspend')
async def admin_suspend_user(
    target_user_id: str,
    body: Optional[dict] = Body(None),
    user_id: str = Depends(get_current_active_user_id),
):
    actor, _role = await _require_admin_permission(user_id, _admin_roles.PERM_USERS_MANAGE)
    reason = ((body or {}).get('reason') or '')
    return await _admin_user_status_change(
        actor, target_user_id,
        status='suspended', reason=reason,
        action_name='user_suspended',
    )


@api.post('/admin/users/{target_user_id}/unsuspend')
async def admin_unsuspend_user(
    target_user_id: str,
    body: Optional[dict] = Body(None),
    user_id: str = Depends(get_current_active_user_id),
):
    actor, _role = await _require_admin_permission(user_id, _admin_roles.PERM_USERS_MANAGE)
    reason = ((body or {}).get('reason') or '')
    return await _admin_user_status_change(
        actor, target_user_id,
        status='active', reason=reason,
        action_name='user_unsuspended',
    )


@api.post('/admin/users/{target_user_id}/delete')
async def admin_soft_delete_user(
    target_user_id: str,
    body: Optional[dict] = Body(None),
    user_id: str = Depends(get_current_active_user_id),
):
    """Soft delete only. Pauses automations, disconnects IG accounts,
    blocks login. Owner-only (admin.owner.manage). Cannot delete the
    last owner. Cannot delete yourself. Hard delete is intentionally
    NOT implemented in Phase 2.8."""
    actor, _role = await _require_admin_permission(user_id, _admin_roles.PERM_OWNER_MANAGE)
    if user_id == target_user_id:
        raise HTTPException(403, 'cannot_delete_self')
    reason = ((body or {}).get('reason') or '')
    return await _admin_user_status_change(
        actor, target_user_id,
        status='deleted', reason=reason,
        action_name='user_soft_deleted',
        require_owner=True,
    )


# ---- metrics reconciliation -----------------------------------------------

@api.get('/admin/metrics/reconciliation')
async def admin_metrics_reconciliation(
    month: Optional[str] = None,
    user_id: str = Depends(get_current_active_user_id),
):
    """Compare Admin Overview numbers against fresh aggregations from
    raw collections. Read-only. Admin-only via admin.audit.view."""
    started = datetime.utcnow()
    await _require_admin_permission(user_id, _admin_roles.PERM_AUDIT_VIEW)
    if _rate_limited('admin_metrics_reconciliation', user_id,
                     limit=RATE_LIMIT_ADMIN_HEAVY_PER_MIN, window_seconds=60):
        logger.warning('rate_limit_hit bucket=admin_metrics_reconciliation user_id=%s', user_id)
        raise HTTPException(429, 'Too many reconciliation requests. Try again in a minute.')
    event_month = (month or _usage_month(datetime.utcnow())).strip()
    if not re.fullmatch(r'\d{4}-\d{2}', event_month):
        raise HTTPException(400, 'month must be YYYY-MM')
    month_start, month_end = _dashboard_month_bounds(event_month)

    async def _count_monthly_comments(predicate) -> int:
        count = 0
        cursor = db.comments.find({})
        async for comment in cursor:
            dt = _dashboard_comment_dt(comment)
            if not dt or not (month_start <= dt < month_end):
                continue
            if predicate(comment):
                count += 1
        return count

    async def _count_monthly_clicks() -> int:
        count = 0
        try:
            cursor = db.link_click_events.find({})
            async for click in cursor:
                dt = _dashboard_dt(click.get('clickedAt'), click.get('createdAt'), click.get('created'))
                if dt and month_start <= dt < month_end:
                    count += 1
        except Exception:
            return 0
        return count

    # ---- Recompute totals from raw sources ----
    # active automations
    dash_active_autos = await db.automations.count_documents({'status': 'active'})
    recomputed_active_autos = dash_active_autos  # same source today

    # connected IG accounts
    dash_ig = await db.instagram_accounts.count_documents({'connectionValid': True})
    recomputed_ig = dash_ig

    # public_replies_sent (this month) recomputed from comments with provider proof
    # vs sum of monthly_usage.public_replies_sent
    dashboard_public_replies = 0
    cursor = db.monthly_usage.find(_monthly_usage_user_scope_query(event_month))
    async for r in cursor:
        dashboard_public_replies += int(r.get('public_replies_sent') or 0)
    # Recompute by counting comments with provider proof flagged in this
    # month. We use the comment's `replied_at` if present; otherwise
    # `updated`. This is approximate — admins read it as a sanity check.
    recomputed_public_replies = await _count_monthly_comments(_dashboard_public_reply_confirmed)

    dashboard_dms = 0
    cursor = db.monthly_usage.find(_monthly_usage_user_scope_query(event_month))
    async for r in cursor:
        dashboard_dms += int(r.get('dms_sent') or 0)
    recomputed_dms = await _count_monthly_comments(_dashboard_dm_confirmed)

    dashboard_links = 0
    cursor = db.monthly_usage.find(_monthly_usage_user_scope_query(event_month))
    async for r in cursor:
        dashboard_links += int(r.get('links_clicked') or 0)
    recomputed_links = await _count_monthly_clicks()

    plan_limited = await db.comments.count_documents({'action_status': 'plan_limited'})
    retryable = await db.comments.count_documents({'action_status': 'failed_retryable'})
    permanent = await db.comments.count_documents({
        'action_status': {'$in': ['failed_permanent', 'failed_retry_exhausted']},
    })

    def _row(name, dash, recomputed, source, notes=''):
        diff = (dash - recomputed) if (isinstance(dash, int) and isinstance(recomputed, int)) else None
        status = 'ok' if diff == 0 else 'mismatch'
        return {
            'metric_name': name,
            'dashboard_value': dash,
            'recomputed_value': recomputed,
            'difference': diff,
            'status': status,
            'source': source,
            'notes': notes,
        }

    rows = [
        _row('active_automations', dash_active_autos, recomputed_active_autos,
             source='automations.status=active'),
        _row('connected_instagram_accounts', dash_ig, recomputed_ig,
             source='instagram_accounts.connectionValid=true'),
        _row('public_replies_sent_month',
             dashboard_public_replies, recomputed_public_replies,
             source='sum(monthly_usage.public_replies_sent) vs comments.reply_provider_response_ok=true',
             notes='provider-proof comments only; legacy false successes are excluded'),
        _row('dms_sent_month', dashboard_dms, recomputed_dms,
             source='sum(monthly_usage.dms_sent) vs comments.dm_status=success',
             notes='status-confirmed DMs only'),
        _row('links_clicked_month', dashboard_links, recomputed_links,
             source='sum(monthly_usage.links_clicked) vs link_click_events count',
             notes='tracked link events only'),
        _row('plan_limited_counts', plan_limited, plan_limited,
             source='comments.action_status=plan_limited (single source)'),
        _row('retryable_failure_counts', retryable, retryable,
             source='comments.action_status=failed_retryable (single source)'),
        _row('permanent_failure_counts', permanent, permanent,
             source='comments.action_status in failed_permanent/failed_retry_exhausted'),
    ]
    account_usage_reconciliation = await _usage_subject_reconciliation(event_month)
    duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
    logger.info(
        'admin_metrics_reconciliation_calculated user_id=%s resultCount=%s durationMs=%s',
        user_id, len(rows), duration_ms,
    )
    return {
        'event_month': event_month,
        'items': rows,
        'mismatch_count': sum(1 for r in rows if r['status'] == 'mismatch'),
        'metric_sources': DASHBOARD_METRIC_SOURCES,
        'account_usage_reconciliation': account_usage_reconciliation,
        'billing_enabled': False,
    }


@api.get('/admin/limits/usage-reservation-diagnostics')
async def admin_usage_reservation_diagnostics(
    month: Optional[str] = None,
    user_id: str = Depends(get_current_active_user_id),
):
    """Read-only reservation ledger diagnostics. Returns counts only."""
    await _require_admin_permission(user_id, _admin_roles.PERM_AUDIT_VIEW)
    event_month = (month or _usage_month(datetime.utcnow())).strip()
    if not re.fullmatch(r'\d{4}-\d{2}', event_month):
        raise HTTPException(400, 'month must be YYYY-MM')
    now = datetime.utcnow()
    statuses = {s: 0 for s in ('pending', 'reserved', 'confirmed', 'released', 'expired', 'failed')}
    stale_reserved = 0
    confirmed_by_metric: Dict[str, int] = {}
    try:
        cursor = db.usage_reservations.find({'month': event_month}).limit(5000)
        async for row in cursor:
            status = str(row.get('status') or 'unknown')
            statuses[status] = statuses.get(status, 0) + 1
            if status == 'reserved':
                expires_at = row.get('expires_at')
                if isinstance(expires_at, datetime) and expires_at < now:
                    stale_reserved += 1
            if status == 'confirmed':
                metric = str(row.get('metric') or 'unknown')
                confirmed_by_metric[metric] = confirmed_by_metric.get(metric, 0) + int(row.get('amount') or 1)
    except Exception:
        pass
    monthly_by_metric = {field: 0 for field in USAGE_COUNTER_FIELDS}
    legacy_user_scoped_rows = 0
    try:
        cursor = db.monthly_usage.find({
            'event_month': event_month,
            'limit_subject_type': 'instagram_account',
        }).limit(5000)
        async for row in cursor:
            for field in USAGE_COUNTER_FIELDS:
                monthly_by_metric[field] += int(row.get(field) or 0)
        legacy_user_scoped_rows = await db.monthly_usage.count_documents({
            'event_month': event_month,
            '$or': [
                {'limit_subject_type': {'$exists': False}},
                {'limit_subject_type': 'user'},
            ],
        })
    except Exception:
        pass
    mismatches = []
    for metric, confirmed_amount in sorted(confirmed_by_metric.items()):
        monthly_amount = int(monthly_by_metric.get(metric) or 0)
        if confirmed_amount != monthly_amount:
            mismatches.append({
                'metric': metric,
                'confirmed_reservations': confirmed_amount,
                'monthly_usage': monthly_amount,
                'status': 'mismatch',
            })
    return {
        'month': event_month,
        'statuses': statuses,
        'stale_reserved_count': stale_reserved,
        'confirmed_by_metric': confirmed_by_metric,
        'monthly_usage_by_metric': monthly_by_metric,
        'mismatches': mismatches,
        'mismatch_count': len(mismatches),
        'legacy_user_scoped_monthly_usage_rows': legacy_user_scoped_rows,
        'privacy': {
            'raw_text_returned': False,
            'tokens_returned': False,
        },
    }


@api.post('/admin/limits/backfill-instagram-usage-subjects')
async def admin_backfill_instagram_usage_subjects(
    body: Optional[dict] = Body(None),
    user_id: str = Depends(get_current_active_user_id),
):
    """Admin-only migration path for legacy user-scoped monthly usage.

    Dry-run by default. It never deletes legacy data and marks ambiguous rows
    instead of guessing.
    """
    actor, _role = await _require_admin_permission(user_id, _admin_roles.PERM_PLANS_ASSIGN)
    if _rate_limited('admin_backfill_instagram_usage_subjects', user_id,
                     limit=RATE_LIMIT_ADMIN_HEAVY_PER_MIN, window_seconds=60):
        logger.warning('rate_limit_hit bucket=admin_backfill_instagram_usage_subjects user_id=%s', user_id)
        raise HTTPException(429, 'Too many backfill requests. Try again in a minute.')
    body = body or {}
    month = (body.get('month') or '').strip() or None
    if month and not re.fullmatch(r'\d{4}-\d{2}', month):
        raise HTTPException(400, 'month must be YYYY-MM')
    dry_run = bool(body.get('dry_run', True))
    if not dry_run and body.get('confirm') is not True:
        raise HTTPException(400, 'confirm=true is required when dry_run=false')
    result = await _backfill_instagram_account_usage_subjects(
        month=month,
        dry_run=dry_run,
        limit=int(body.get('limit') or 5000),
    )
    await _record_admin_action(
        actor,
        action='instagram_account_usage_backfill_dry_run' if dry_run else 'instagram_account_usage_backfill_applied',
        target_user_id=None,
        metadata={
            'month': month,
            'dry_run': dry_run,
            'checked': result.get('checked'),
            'mapped': result.get('mapped'),
            'unmapped': result.get('unmapped'),
            'ambiguous': result.get('ambiguous'),
        },
    )
    return result


async def _safe_index_names(collection) -> List[str]:
    try:
        info = await collection.index_information()
        return sorted(list((info or {}).keys()))
    except Exception:
        return []


@api.get('/admin/limits/instagram-account-diagnostics')
async def admin_instagram_account_limit_diagnostics(
    user_id: str = Depends(get_current_active_user_id),
):
    """Read-only production diagnostic for account-level limit enforcement."""
    await _require_admin_permission(user_id, _admin_roles.PERM_AUDIT_VIEW)
    instagram_index_names = await _safe_index_names(db.instagram_accounts)
    usage_index_names = await _safe_index_names(db.usage_events)
    monthly_index_names = await _safe_index_names(db.monthly_usage)
    trial_index_names = await _safe_index_names(db.instagram_account_trial_claims)

    active_missing_canonical = await db.instagram_accounts.count_documents({
        'connectionValid': True,
        'isActive': {'$ne': False},
        '$or': [
            {'instagramAccountId': {'$exists': False}},
            {'instagramAccountId': None},
            {'instagramAccountId': ''},
        ],
    })
    duplicate_groups = 0
    seen: Dict[str, int] = {}
    cursor = db.instagram_accounts.find({'connectionValid': True, 'isActive': {'$ne': False}}).limit(10000)
    async for account in cursor:
        canonical = _canonical_instagram_account_id(
            account.get('instagramAccountId') or account.get('igUserId')
        )
        if not canonical:
            continue
        seen[canonical] = seen.get(canonical, 0) + 1
    duplicate_groups = sum(1 for count in seen.values() if count > 1)

    return {
        'canonical_instagram_account_id_field': 'instagram_accounts.instagramAccountId',
        'legacy_alias_fields': ['instagram_accounts.igUserId', 'users.ig_user_id'],
        'duplicate_active_ownership_policy': 'blocked_by_unique_partial_index_and_connect_guard',
        'duplicate_active_group_count_sampled': duplicate_groups,
        'active_accounts_missing_canonical_id': active_missing_canonical,
        'usage_subject_policy': {
            'monthly_comments_processed_limit': 'instagram_account',
            'monthly_public_replies_sent_limit': 'instagram_account',
            'monthly_dms_sent_limit': 'instagram_account',
            'other_limits': 'user',
        },
        'trial_claim_policy': 'one trial claim per Instagram account and trial identifier',
        'admin_overrides_policy': 'user_scoped; account-scoped overrides are not enabled',
        'required_indexes': {
            'uniq_active_instagram_account_owner': 'uniq_active_instagram_account_owner' in instagram_index_names,
            'usage_events_subject_month': 'usage_events_subject_month' in usage_index_names,
            'monthly_usage_subject_month': 'monthly_usage_subject_month' in monthly_index_names,
            'monthly_usage_subject_unique': 'monthly_usage_subject_unique' in monthly_index_names,
            'uniq_instagram_trial_claim': 'uniq_instagram_trial_claim' in trial_index_names,
        },
        'index_names': {
            'instagram_accounts': instagram_index_names,
            'usage_events': usage_index_names,
            'monthly_usage': monthly_index_names,
            'instagram_account_trial_claims': trial_index_names,
        },
        'read_only': True,
    }


# ---------------- Instagram OAuth (Business Login) ----------------
# Uses Instagram API with Business Login flow — required for the
# /{ig_user_id}/subscribed_apps endpoint to accept our access token.
# Facebook Login for Business (Pages) returns a Page token that the
# new IG Graph API rejects with "Application does not have the capability".
IG_SCOPES = (
    'instagram_business_basic,'
    'instagram_business_manage_messages,'
    'instagram_business_manage_comments'
)
VALID_IG_ACCOUNT_TYPES = {'BUSINESS', 'CREATOR', 'MEDIA_CREATOR'}
IG_OAUTH_STATE_TTL_SECONDS = 30 * 60


def _token_prefix(token: str) -> Optional[str]:
    return token[:6] if token else None


def _safe_return_to(return_to: Optional[str]) -> str:
    value = (return_to or '/app/settings?tab=instagram').strip()
    if not value.startswith('/app') or '://' in value or '\n' in value or '\r' in value:
        return '/app/settings?tab=instagram'
    return value


def _frontend_redirect_url(return_to: Optional[str], params: Optional[dict] = None) -> str:
    path = _safe_return_to(return_to)
    extra = urlencode(params or {})
    if extra:
        path = f"{path}{'&' if '?' in path else '?'}{extra}"
    return f"{FRONTEND_URL}{path}"


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode('ascii').rstrip('=')


def _b64url_decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + '=' * (-len(text) % 4))


def _sign_instagram_oauth_state(payload: Dict[str, Any]) -> str:
    state_payload = {
        **payload,
        'iat': int(datetime.utcnow().timestamp()),
    }
    body = _b64url_encode(json.dumps(
        state_payload,
        separators=(',', ':'),
        sort_keys=True,
    ).encode('utf-8'))
    sig = hmac.new(JWT_SECRET.encode('utf-8'), body.encode('ascii'), hashlib.sha256).digest()
    return f'{body}.{_b64url_encode(sig)}'


def _verify_instagram_oauth_state(state: Optional[str]) -> Optional[Dict[str, Any]]:
    if not state or '.' not in state or not JWT_SECRET:
        return None
    body, sig = state.rsplit('.', 1)
    expected = _b64url_encode(hmac.new(
        JWT_SECRET.encode('utf-8'),
        body.encode('ascii'),
        hashlib.sha256,
    ).digest())
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        payload = json.loads(_b64url_decode(body).decode('utf-8'))
    except Exception:
        return None
    user_id = payload.get('userId') or payload.get('user_id')
    issued_at = int(payload.get('iat') or 0)
    if not user_id or issued_at <= 0:
        return None
    if int(datetime.utcnow().timestamp()) - issued_at > IG_OAUTH_STATE_TTL_SECONDS:
        return None
    mode = payload.get('mode') or 'connect'
    if mode not in {'connect', 'add_account', 'reconnect'}:
        mode = 'connect'
    return {
        'userId': str(user_id),
        'mode': mode,
        'returnTo': _safe_return_to(payload.get('returnTo')),
        'iat': issued_at,
    }


def _safe_graph_error(body: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(body, dict):
        return None
    err = body.get('error')
    if not isinstance(err, dict):
        return None
    return {
        'message': err.get('message'),
        'type': err.get('type'),
        'code': err.get('code'),
        'error_subcode': err.get('error_subcode'),
        'fbtrace_id': err.get('fbtrace_id'),
    }


def _parse_graph_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if not value:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(value)
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
            return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
        except Exception:
            return None
    return None


def _iso_or_none(value: Any) -> Optional[str]:
    dt = _parse_graph_datetime(value)
    return dt.isoformat() if dt else None


def _instagram_account_doc_id(user_id: str, instagram_account_id: str) -> str:
    raw = f'{user_id}:{instagram_account_id}'
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _days_until(value: Any, now: Optional[datetime] = None) -> Optional[float]:
    dt = _parse_graph_datetime(value)
    if not dt:
        return None
    base = now or datetime.utcnow()
    return (dt - base).total_seconds() / 86400


def _token_refresh_public_row(account: dict, now: Optional[datetime] = None) -> dict:
    base = now or datetime.utcnow()
    expires_at = _parse_graph_datetime(account.get('tokenExpiresAt'))
    days = _days_until(expires_at, base) if expires_at else None
    refresh_status = account.get('refreshStatus') or 'unknown'
    expired = bool(expires_at and expires_at <= base)
    critical = bool(
        expired or
        (days is not None and days <= 3) or
        (days is not None and days <= 7 and refresh_status == 'failed') or
        int(account.get('refreshAttempts') or 0) >= 3
    )
    return {
        'accountId': account.get('id'),
        'instagramAccountId': account.get('instagramAccountId') or account.get('igUserId'),
        'username': account.get('username'),
        'tokenExpiresAt': expires_at.isoformat() if expires_at else None,
        'daysUntilExpiry': round(days, 2) if days is not None else None,
        'refreshStatus': refresh_status,
        'lastRefreshedAt': _iso_or_none(account.get('lastRefreshedAt')),
        'refreshAttempts': int(account.get('refreshAttempts') or 0),
        'critical': critical,
        'expired': expired,
    }


def _instagram_account_public_row(account: dict, active_account_id: str = '') -> dict:
    instagram_account_id = account.get('instagramAccountId') or account.get('igUserId')
    token_row = _token_refresh_public_row(account)
    return {
        'id': account.get('id'),
        'instagramAccountId': instagram_account_id,
        'igUserId': instagram_account_id,
        'username': account.get('username'),
        'profilePictureUrl': account.get('profilePictureUrl') or account.get('profile_picture_url'),
        'accountType': account.get('accountType'),
        'connectionValid': bool(account.get('connectionValid')),
        'isActive': bool(account.get('isActive')),
        'active': bool(active_account_id and account.get('id') == active_account_id),
        'isCurrent': bool(active_account_id and account.get('id') == active_account_id),
        'tokenSource': account.get('tokenSource'),
        'tokenStatus': {
            'refreshStatus': token_row.get('refreshStatus'),
            'tokenExpiresAt': token_row.get('tokenExpiresAt'),
            'daysUntilExpiry': token_row.get('daysUntilExpiry'),
            'critical': token_row.get('critical'),
            'expired': token_row.get('expired'),
        },
        'tokenExpiresAt': _iso_or_none(account.get('tokenExpiresAt')),
        'lastRefreshedAt': _iso_or_none(account.get('lastRefreshedAt')),
        'refreshStatus': account.get('refreshStatus') or 'unknown',
        'refreshAttempts': int(account.get('refreshAttempts') or 0),
        'createdAt': _iso_or_none(account.get('createdAt')),
        'updatedAt': _iso_or_none(account.get('updatedAt')),
    }


def _is_public_switchable_instagram_account(account: dict) -> bool:
    if not isinstance(account, dict):
        return False
    instagram_account_id = account.get('instagramAccountId') or account.get('igUserId')
    return bool(
        account.get('id')
        and instagram_account_id
        and account.get('accessToken')
        and account.get('connectionValid') is True
        and account.get('isActive') is not False
        and account.get('refreshStatus') not in {
            'disconnected',
            'auto_cleanup_users_disconnected',
            'auto_cleanup_single_account_plan',
            'force_disconnected_by_admin',
            'replaced_by_reconnect',
        }
    )


async def _cleanup_extra_instagram_accounts_for_single_account_plan(
    user_id: str,
    active_account_id: str,
) -> int:
    if not user_id or not active_account_id:
        return 0
    try:
        effective = await compute_effective_limits(user_id)
        max_accounts = effective.get('max_instagram_accounts')
        if max_accounts is None or int(max_accounts) > 1:
            return 0
        result = await db.instagram_accounts.update_many(
            {
                '$or': [{'userId': user_id}, {'user_id': user_id}],
                'id': {'$ne': active_account_id},
                'connectionValid': True,
            },
            {'$set': {
                'connectionValid': False,
                'isActive': False,
                'isCurrent': False,
                'refreshStatus': 'auto_cleanup_single_account_plan',
                'refreshLockedUntil': None,
                'updatedAt': datetime.utcnow(),
            }},
        )
        cleaned = int(getattr(result, 'modified_count', 0) or 0)
        if cleaned:
            logger.info(
                'instagram_accounts_single_plan_cleanup user_id=%s active_account_id=%s rows_cleaned=%s',
                user_id, active_account_id, cleaned,
            )
            await invalidate_dashboard_summary(user_id)
        return cleaned
    except Exception as exc:
        logger.warning(
            'instagram_accounts_single_plan_cleanup_failed user_id=%s exception=%s',
            user_id, type(exc).__name__,
        )
        return 0


def _instagram_context_from_account(account_doc: Optional[dict]) -> dict:
    account_doc = account_doc or {}
    instagram_account_id = str(account_doc.get('instagramAccountId') or account_doc.get('igUserId') or '')
    username = (account_doc.get('username') or '').replace('@', '')
    return {
        'instagramAccountDbId': account_doc.get('id') or '',
        'instagram_account_id': account_doc.get('id') or '',
        'instagramAccountId': instagram_account_id,
        'igUserId': instagram_account_id,
        'instagramUsername': username,
    }


def _current_instagram_context(user_doc: dict) -> dict:
    return {
        'instagramAccountDbId': user_doc.get('active_instagram_account_id') or '',
        'instagram_account_id': user_doc.get('active_instagram_account_id') or '',
        'instagramAccountId': str(user_doc.get('ig_user_id') or ''),
        'igUserId': str(user_doc.get('ig_user_id') or ''),
        'instagramUsername': (user_doc.get('instagramHandle') or '').replace('@', ''),
    }


def _account_scoped_query(user_id: str, account_or_ig_id: Any) -> dict:
    query = {'user_id': user_id}
    if isinstance(account_or_ig_id, dict):
        account_id = str(
            account_or_ig_id.get('id') or
            account_or_ig_id.get('instagramAccountDbId') or
            account_or_ig_id.get('instagram_account_id') or
            ''
        )
        instagram_account_id = str(account_or_ig_id.get('instagramAccountId') or account_or_ig_id.get('igUserId') or '')
    else:
        account_id = ''
        instagram_account_id = str(account_or_ig_id or '')
    clauses = []
    if account_id:
        clauses.extend([
            {'instagramAccountDbId': account_id},
            {'instagram_account_id': account_id},
            {'instagramAccountId': account_id},
            {'accountId': account_id},
        ])
    if instagram_account_id:
        clauses.extend([
            {'instagramAccountId': instagram_account_id},
            {'igUserId': instagram_account_id},
            {'ig_user_id': instagram_account_id},
            {'accountId': instagram_account_id},
        ])
    if clauses:
        query['$or'] = [
            clause for i, clause in enumerate(clauses)
            if clause not in clauses[:i]
        ]
    return query


async def _validate_selected_media_owned_by_account(account_doc: dict, media_id: Optional[str]) -> None:
    media_id = str(media_id or '').strip()
    if not media_id:
        return
    instagram_account_id = str(
        (account_doc or {}).get('instagramAccountId') or
        (account_doc or {}).get('igUserId') or
        ''
    ).strip()
    token = str((account_doc or {}).get('accessToken') or '').strip()
    # Unit tests and legacy local fixtures sometimes omit access tokens. In
    # production, an active/valid connection carries one; tokenless active
    # operations are rejected separately by connection validation.
    if not instagram_account_id or not token:
        return
    try:
        recent_media_ids = await _fetch_recent_media_ids(token, instagram_account_id, limit=100)
    except Exception as exc:
        logger.warning(
            'selected_media_validation_failed instagramAccountId=%s media_id=%s exception_type=%s',
            instagram_account_id,
            media_id,
            type(exc).__name__,
        )
        raise HTTPException(400, 'selected_media_not_found_or_not_owned')
    if media_id not in set(str(mid) for mid in recent_media_ids):
        raise HTTPException(400, 'selected_media_not_found_or_not_owned')


async def _validate_automation_integrity_for_account(
    user_id: str,
    account_doc: dict,
    automation_doc: dict,
    *,
    require_connected: bool = False,
) -> None:
    if not account_doc:
        if not require_connected:
            return
        raise HTTPException(400, 'No Instagram account connected')
    if require_connected and account_doc.get('connectionValid') is False:
        raise HTTPException(400, 'instagram_reconnect_required')
    expected_ig = str(account_doc.get('instagramAccountId') or account_doc.get('igUserId') or '').strip()
    actual_ig = str(
        automation_doc.get('instagramAccountId') or
        automation_doc.get('igUserId') or
        ''
    ).strip()
    if expected_ig and actual_ig and actual_ig != expected_ig:
        logger.warning(
            'automation_account_context_mismatch user_id=%s automation_id=%s expected_ig=%s actual_ig=%s',
            user_id,
            automation_doc.get('id') or '',
            expected_ig,
            actual_ig,
        )
        raise HTTPException(400, 'instagram_account_mismatch')
    media_id = _selected_specific_media_id(automation_doc) or automation_doc.get('media_id')
    latest = bool(automation_doc.get('latest')) or automation_doc.get('post_scope') in ('latest', 'next')
    if media_id and not latest:
        await _validate_selected_media_owned_by_account(account_doc, str(media_id))


def _with_instagram_account_context(user_doc: dict, account_doc: Optional[dict]) -> dict:
    if not account_doc:
        return user_doc
    instagram_account_id = account_doc.get('instagramAccountId') or account_doc.get('igUserId') or ''
    merged = {**user_doc}
    merged.update({
        'active_instagram_account_id': account_doc.get('id') or user_doc.get('active_instagram_account_id'),
        'ig_user_id': instagram_account_id,
        'meta_access_token': account_doc.get('accessToken') or user_doc.get('meta_access_token') or '',
        'instagramHandle': account_doc.get('username') or user_doc.get('instagramHandle') or '',
        'instagram_connection_valid': bool(account_doc.get('connectionValid')),
        'instagramConnectionValid': bool(account_doc.get('connectionValid')),
        'instagram_token_source': account_doc.get('tokenSource') or user_doc.get('instagram_token_source'),
        'instagramTokenSource': account_doc.get('tokenSource') or user_doc.get('instagramTokenSource'),
    })
    return merged


async def getActiveInstagramAccount(user_id: str) -> dict:
    """Return the server-side active Instagram account for this website user.

    The active account id is persisted on users.active_instagram_account_id.
    Legacy users.ig_user_id/meta_access_token are kept in sync for older paths,
    but new account-scoped code should use this helper's returned account doc.
    """
    user_doc = await db.users.find_one({'id': user_id})
    if not user_doc:
        raise HTTPException(404, 'User not found')
    await _sync_user_instagram_account_doc(user_doc)

    active_id = user_doc.get('active_instagram_account_id') or ''
    account = None
    if active_id:
        account = await db.instagram_accounts.find_one({
            'id': active_id,
            'userId': user_id,
            'isActive': {'$ne': False},
        })
    if not account:
        account = await db.instagram_accounts.find_one({
            'userId': user_id,
            'isCurrent': True,
            'isActive': {'$ne': False},
        })
    if not account and user_doc.get('ig_user_id'):
        account = await db.instagram_accounts.find_one({
            'userId': user_id,
            '$or': [
                {'instagramAccountId': str(user_doc.get('ig_user_id'))},
                {'igUserId': str(user_doc.get('ig_user_id'))},
            ],
            'isActive': {'$ne': False},
        })
    if not account:
        account = await db.instagram_accounts.find_one({
            'userId': user_id,
            'isActive': {'$ne': False},
        })
    if not account:
        raise HTTPException(400, 'No Instagram account connected')

    instagram_account_id = account.get('instagramAccountId') or account.get('igUserId') or ''
    token = account.get('accessToken') or ''
    now = datetime.utcnow()
    await db.instagram_accounts.update_many(
        {'userId': user_id},
        {'$set': {'isCurrent': False}},
    )
    await db.instagram_accounts.update_one(
        {'id': account['id'], 'userId': user_id},
        {'$set': {'isCurrent': True, 'updatedAt': now}},
    )
    await db.users.update_one(
        {'id': user_id},
        {'$set': {
            'active_instagram_account_id': account['id'],
            'instagramConnected': True,
            'instagram_connection_valid': bool(account.get('connectionValid')),
            'instagramConnectionValid': bool(account.get('connectionValid')),
            'instagram_connection_blocker': None if account.get('connectionValid') else 'selected_account_invalid',
            'instagramHandle': account.get('username') or '',
            'instagram_account_type': account.get('accountType'),
            'instagram_profile_picture_url': account.get('profilePictureUrl') or account.get('profile_picture_url'),
            'ig_user_id': instagram_account_id,
            'meta_access_token': token,
            'instagramTokenSource': account.get('tokenSource'),
            'instagram_token_source': account.get('tokenSource'),
            'tokenExpiresAt': _parse_graph_datetime(account.get('tokenExpiresAt')),
            'instagram_token_expires_at': _parse_graph_datetime(account.get('tokenExpiresAt')),
            'lastRefreshedAt': _parse_graph_datetime(account.get('lastRefreshedAt')),
            'refreshStatus': account.get('refreshStatus'),
            'refreshAttempts': int(account.get('refreshAttempts') or 0),
            'updated': now,
        }},
    )
    return account


async def _ensure_automation_account_scope_for_user(user_doc: dict) -> int:
    """Attach legacy unscoped automations/comments to the user's current IG account.

    This is intentionally conservative: it only fills missing account fields and
    never moves a rule that is already tied to another Instagram account.
    """
    user_id = user_doc.get('id')
    if not user_id:
        return 0
    accounts = await db.instagram_accounts.find({
        'userId': user_id,
        'isActive': {'$ne': False},
    }).to_list(2)
    if len(accounts) != 1:
        return 0
    ctx = _instagram_context_from_account(accounts[0])
    now = datetime.utcnow()
    missing_account = {'$or': [
        {'instagramAccountId': {'$exists': False}},
        {'instagramAccountId': None},
        {'instagramAccountId': ''},
    ]}
    auto_res = await db.automations.update_many(
        {'user_id': user_id, **missing_account},
        {'$set': {**ctx, 'updatedAt': now}},
    )
    await db.comments.update_many(
        {'user_id': user_id, **missing_account},
        {'$set': ctx},
    )
    await db.conversations.update_many(
        {'user_id': user_id, **missing_account},
        {'$set': ctx},
    )
    await db.dm_rules.update_many(
        {'user_id': user_id, **missing_account},
        {'$set': ctx},
    )
    await db.dm_logs.update_many(
        {'user_id': user_id, **missing_account},
        {'$set': ctx},
    )
    return getattr(auto_res, 'modified_count', 0) or 0


async def _find_user_doc_for_instagram_account_id(instagram_account_id: str) -> tuple:
    if not instagram_account_id:
        return None, None
    account_doc = await db.instagram_accounts.find_one({'$or': [
        {'instagramAccountId': instagram_account_id},
        {'igUserId': instagram_account_id},
    ], 'isActive': {'$ne': False}})
    if account_doc:
        owner = await db.users.find_one({'id': account_doc.get('userId') or account_doc.get('user_id')})
        if owner:
            return _with_instagram_account_context(owner, account_doc), 'instagram_accounts'

    user_doc = await db.users.find_one({'$or': [
        {'ig_user_id': instagram_account_id},
        {'fb_page_id': instagram_account_id},
    ]})
    if user_doc:
        return user_doc, 'users.ig_user_id'
    return None, None


async def _active_instagram_account_owner(instagram_account_id: str) -> Optional[dict]:
    canonical = _canonical_instagram_account_id(instagram_account_id)
    if not canonical:
        return None
    account = await db.instagram_accounts.find_one({
        'instagramAccountId': canonical,
        'isActive': {'$ne': False},
        'connectionValid': True,
    })
    if not account:
        return None
    owner_id = account.get('userId') or account.get('user_id')
    if not owner_id:
        return None
    owner = await db.users.find_one({'id': owner_id}) or {}
    if owner.get('status') in ('deleted', 'suspended'):
        return None
    return account


async def _ensure_instagram_account_connect_allowed(user_id: str, instagram_account_id: str) -> None:
    owner = await _active_instagram_account_owner(instagram_account_id)
    owner_user_id = (owner or {}).get('userId') or (owner or {}).get('user_id')
    if owner and owner_user_id and owner_user_id != user_id:
        logger.warning(
            'instagram_duplicate_active_owner_blocked instagramAccountIdPartial=%s requesting_user_id=%s',
            _safe_partial_identifier(instagram_account_id),
            user_id,
        )
        raise HTTPException(
            409,
            {
                'code': 'instagram_account_already_connected',
                'message': 'This Instagram account is already connected to another MyChat account.',
            },
        )


async def _record_instagram_trial_claim(user_id: str, instagram_account_id: str,
                                        plan_or_trial: str = 'instagram_account_connected') -> None:
    canonical = _canonical_instagram_account_id(instagram_account_id)
    if not canonical:
        return
    now = datetime.utcnow()
    try:
        await db.instagram_account_trial_claims.update_one(
            {'instagram_account_id': canonical, 'plan_trial_identifier': plan_or_trial},
            {'$setOnInsert': {
                '_id': secrets.token_urlsafe(12),
                'id': secrets.token_urlsafe(12),
                'instagram_account_id': canonical,
                'first_claimed_by_user_id': user_id,
                'claimed_at': now,
                'plan_trial_identifier': plan_or_trial,
                'status': 'claimed',
            }},
            upsert=True,
        )
    except Exception as e:
        logger.warning('instagram_trial_claim_record_failed instagramAccountIdPartial=%s reason=%s',
                       _safe_partial_identifier(canonical), str(e)[:100])


async def _connected_instagram_account_ids_for_user(user_id: str, *, active_only: bool = False,
                                                    limit: int = 25) -> List[str]:
    query = {'$or': [{'userId': user_id}, {'user_id': user_id}]}
    if active_only:
        query.update({'connectionValid': True, 'isActive': {'$ne': False}})
    ids: List[str] = []
    cursor = db.instagram_accounts.find(query).limit(limit)
    async for account in cursor:
        canonical = _canonical_instagram_account_id(
            account.get('instagramAccountId') or account.get('igUserId')
        )
        if canonical and canonical not in ids:
            ids.append(canonical)
    return ids


async def _ensure_instagram_trial_claim_available(user_id: str,
                                                  plan_or_trial: str = 'trial_grant') -> List[str]:
    """Prevent account-level trial grants from being claimed through a new user.

    This keeps admin/user-level grants independent of paid billing while making
    the Instagram professional account the durable anti-abuse subject for trials.
    """
    account_ids = await _connected_instagram_account_ids_for_user(user_id, active_only=True)
    for canonical in account_ids:
        claim = await db.instagram_account_trial_claims.find_one({
            'instagram_account_id': canonical,
            'plan_trial_identifier': plan_or_trial,
            'status': {'$ne': 'revoked'},
        })
        first_user = (claim or {}).get('first_claimed_by_user_id')
        if claim and first_user and first_user != user_id:
            logger.warning(
                'instagram_trial_duplicate_claim_blocked instagramAccountIdPartial=%s requesting_user_id=%s',
                _safe_partial_identifier(canonical),
                user_id,
            )
            raise HTTPException(409, {
                'code': 'instagram_account_trial_already_claimed',
                'message': 'This Instagram account has already used this trial grant.',
            })
    return account_ids


async def _map_user_usage_to_instagram_account(user_id: str) -> Tuple[str, Optional[str]]:
    account_ids = await _connected_instagram_account_ids_for_user(user_id, active_only=False, limit=10)
    if len(account_ids) == 1:
        return 'mapped', account_ids[0]
    if not account_ids:
        return 'unmapped_no_instagram_account', None
    return 'ambiguous_multiple_instagram_accounts', None


def _monthly_usage_row_identity(row: dict) -> dict:
    if row.get('_id') is not None:
        return {'_id': row.get('_id')}
    if row.get('id'):
        return {'id': row.get('id')}
    return {
        'user_id': row.get('user_id'),
        'event_month': row.get('event_month'),
        'limit_subject_type': row.get('limit_subject_type'),
        'limit_subject_id': row.get('limit_subject_id'),
    }


async def _backfill_instagram_account_usage_subjects(
    *,
    month: Optional[str] = None,
    dry_run: bool = True,
    limit: int = 5000,
) -> dict:
    """Map legacy user-scoped monthly_usage rows to Instagram-account subjects.

    No legacy rows are deleted. Ambiguous rows are marked/reportable so admins
    can resolve them manually before any enforcement decision depends on them.
    """
    query = {}
    if month:
        query['event_month'] = month
    cursor = db.monthly_usage.find(query).limit(max(1, min(int(limit or 5000), 10000)))
    now = datetime.utcnow()
    summary = {
        'dry_run': dry_run,
        'checked': 0,
        'mapped': 0,
        'unmapped': 0,
        'ambiguous': 0,
        'written': 0,
        'examples': [],
    }
    async for row in cursor:
        subject_type = row.get('limit_subject_type') or 'user'
        if subject_type == 'instagram_account':
            continue
        user_id = str(row.get('user_id') or row.get('limit_subject_id') or '')
        event_month = row.get('event_month')
        if not user_id or not event_month:
            continue
        summary['checked'] += 1
        status, canonical = await _map_user_usage_to_instagram_account(user_id)
        if status == 'mapped' and canonical:
            summary['mapped'] += 1
            if not dry_run:
                existing = await db.monthly_usage.find_one(
                    _monthly_usage_instagram_query(canonical, event_month)
                ) or {}
                counters = {
                    field: max(int(existing.get(field) or 0), int(row.get(field) or 0))
                    for field in USAGE_COUNTER_FIELDS
                }
                set_on_insert = {
                    '_id': secrets.token_urlsafe(12),
                    'id': secrets.token_urlsafe(12),
                    'user_id': user_id,
                    'event_month': event_month,
                    'limit_subject_type': 'instagram_account',
                    'limit_subject_id': canonical,
                    'instagram_account_id': canonical,
                    'created_at': now,
                }
                await db.monthly_usage.update_one(
                    _monthly_usage_instagram_query(canonical, event_month),
                    {
                        '$setOnInsert': set_on_insert,
                        '$set': {
                            **counters,
                            'updated_at': now,
                            'user_id': user_id,
                            'instagram_account_id': canonical,
                            'backfilled_from_user_monthly_usage': True,
                            'backfilled_at': now,
                        },
                    },
                    upsert=True,
                )
                await db.monthly_usage.update_one(
                    _monthly_usage_row_identity(row),
                    {'$set': {
                        'limit_subject_type': row.get('limit_subject_type') or 'user',
                        'limit_subject_id': row.get('limit_subject_id') or user_id,
                        'instagram_subject_mapping_status': 'mapped',
                        'mapped_instagram_account_id': canonical,
                        'mapped_at': now,
                    }},
                )
                summary['written'] += 1
        else:
            if status.startswith('ambiguous'):
                summary['ambiguous'] += 1
            else:
                summary['unmapped'] += 1
            if not dry_run:
                await db.monthly_usage.update_one(
                    _monthly_usage_row_identity(row),
                    {'$set': {
                        'limit_subject_type': row.get('limit_subject_type') or 'user',
                        'limit_subject_id': row.get('limit_subject_id') or user_id,
                        'instagram_subject_mapping_status': status,
                        'mapping_checked_at': now,
                    }},
                )
        if len(summary['examples']) < 10 and status != 'mapped':
            summary['examples'].append({
                'user_id': user_id,
                'event_month': event_month,
                'mapping_status': status,
            })
    return summary


async def _usage_subject_reconciliation(event_month: str) -> dict:
    monthly_user = {field: 0 for field in USAGE_COUNTER_FIELDS}
    monthly_account = {field: 0 for field in USAGE_COUNTER_FIELDS}
    rows = {'user_scoped': 0, 'instagram_account_scoped': 0, 'unmapped': 0, 'ambiguous': 0}
    cursor = db.monthly_usage.find({'event_month': event_month})
    async for row in cursor:
        subject_type = row.get('limit_subject_type') or 'user'
        mapping_status = row.get('instagram_subject_mapping_status') or ''
        if mapping_status.startswith('unmapped'):
            rows['unmapped'] += 1
        if mapping_status.startswith('ambiguous'):
            rows['ambiguous'] += 1
        if subject_type == 'instagram_account':
            rows['instagram_account_scoped'] += 1
            target = monthly_account
        else:
            rows['user_scoped'] += 1
            target = monthly_user
        for field in USAGE_COUNTER_FIELDS:
            target[field] += int(row.get(field) or 0)

    events_account = {field: 0 for field in USAGE_COUNTER_FIELDS}
    events_user = {field: 0 for field in USAGE_COUNTER_FIELDS}
    cursor = db.usage_events.find({'event_month': event_month})
    async for event in cursor:
        counter = USAGE_COUNTER_BY_EVENT.get(event.get('event_type'))
        if not counter:
            continue
        if event.get('limit_subject_type') == 'instagram_account':
            events_account[counter] += 1
        else:
            events_user[counter] += 1

    return {
        'event_month': event_month,
        'monthly_usage_rows': rows,
        'monthly_usage_user_scoped_counters': monthly_user,
        'monthly_usage_instagram_account_scoped_counters': monthly_account,
        'usage_events_user_scoped_counters': events_user,
        'usage_events_instagram_account_scoped_counters': events_account,
        'repair_applied': False,
    }


def _cron_secret_is_valid(provided: Optional[str]) -> bool:
    return bool(CRON_SECRET and provided and hmac.compare_digest(str(provided), CRON_SECRET))


def _cron_secret_from_request(request: Request) -> Optional[str]:
    auth = request.headers.get('authorization') or request.headers.get('Authorization') or ''
    if auth.lower().startswith('bearer '):
        return auth.split(' ', 1)[1].strip()
    return (
        request.headers.get('x-cron-secret') or
        request.headers.get('X-Cron-Secret')
    )


async def notifyTokenRefreshProblem(message: str, metadata: Optional[dict] = None):
    """Placeholder alert hook. Replace with email/Slack/etc. when available."""
    safe_metadata = _redact_secrets(metadata or {})
    logger.warning('instagram_token_refresh_problem: %s metadata=%s', message, safe_metadata)


async def _sync_user_instagram_account_doc(
    user_doc: dict,
    access_token: Optional[str] = None,
    token_expires_at: Optional[datetime] = None,
    token_source: Optional[str] = None,
    refresh_status: Optional[str] = None,
    last_refreshed_at: Optional[datetime] = None,
) -> Optional[dict]:
    """Mirror the current legacy users.* Instagram connection into instagram_accounts.

    The production code still reads users.meta_access_token, so this collection is
    additive/backwards-compatible and lets token refresh jobs work per account.
    """
    user_id = user_doc.get('id')
    instagram_account_id = str(user_doc.get('ig_user_id') or '')
    token = access_token if access_token is not None else (user_doc.get('meta_access_token') or '')
    if not (user_id and instagram_account_id and token):
        return None
    await _ensure_instagram_account_connect_allowed(user_id, instagram_account_id)
    now = datetime.utcnow()
    deterministic_account_id = _instagram_account_doc_id(user_id, instagram_account_id)
    existing = await db.instagram_accounts.find_one({'$or': [
        {'id': deterministic_account_id},
        {'userId': user_id, 'instagramAccountId': instagram_account_id},
        {'userId': user_id, 'igUserId': instagram_account_id},
    ]})
    account_id = (existing or {}).get('id') or deterministic_account_id
    created_at = (
        _parse_graph_datetime((existing or {}).get('createdAt')) or
        _parse_graph_datetime(user_doc.get('instagram_connected_at')) or
        _parse_graph_datetime(user_doc.get('createdAt')) or
        _parse_graph_datetime(user_doc.get('created')) or
        now
    )
    expires_at = (
        token_expires_at or
        _parse_graph_datetime(user_doc.get('tokenExpiresAt')) or
        _parse_graph_datetime(user_doc.get('instagram_token_expires_at')) or
        _parse_graph_datetime((existing or {}).get('tokenExpiresAt'))
    )
    doc = {
        'id': account_id,
        'userId': user_id,
        'user_id': user_id,
        'instagramAccountId': instagram_account_id,
        'igUserId': instagram_account_id,
        'username': (user_doc.get('instagramHandle') or '').replace('@', ''),
        'accountType': user_doc.get('instagram_account_type'),
        'accessToken': token,
        'tokenSource': token_source or user_doc.get('instagram_token_source') or user_doc.get('instagramTokenSource'),
        'authKind': user_doc.get('ig_auth_kind') or 'instagram_business_login',
        'connectionValid': bool(user_doc.get('instagram_connection_valid')),
        'isActive': bool(user_doc.get('instagramConnected')),
        'tokenExpiresAt': expires_at,
        'lastRefreshedAt': last_refreshed_at or _parse_graph_datetime(user_doc.get('lastRefreshedAt')),
        'refreshStatus': refresh_status or user_doc.get('refreshStatus') or (existing or {}).get('refreshStatus') or 'unknown',
        'refreshError': (existing or {}).get('refreshError'),
        'refreshAttempts': int((existing or {}).get('refreshAttempts') or user_doc.get('refreshAttempts') or 0),
        'refreshLockedUntil': (existing or {}).get('refreshLockedUntil'),
        'metadata': {'source': 'users_legacy_connection'},
        'createdAt': created_at,
        'updatedAt': now,
    }
    try:
        await db.instagram_accounts.update_one(
            {'id': account_id},
            {
                '$set': doc,
                '$setOnInsert': {'connectedAt': created_at},
            },
            upsert=True,
        )
    except DuplicateKeyError:
        logger.warning(
            'instagram_duplicate_active_owner_blocked_db instagramAccountIdPartial=%s user_id=%s',
            _safe_partial_identifier(instagram_account_id),
            user_id,
        )
        raise HTTPException(
            409,
            {
                'code': 'instagram_account_already_connected',
                'message': 'This Instagram account is already connected to another MyChat account.',
            },
        )
    await _record_instagram_trial_claim(user_id, instagram_account_id)
    await db.users.update_one(
        {'id': user_id, '$or': [
            {'active_instagram_account_id': {'$exists': False}},
            {'active_instagram_account_id': None},
            {'active_instagram_account_id': ''},
        ]},
        {'$set': {'active_instagram_account_id': account_id}},
    )
    stored = await db.instagram_accounts.find_one({'id': account_id})
    return stored


async def _ensure_instagram_account_docs_for_connected_users(limit: int = 1000) -> int:
    users = await db.users.find({
        'instagramConnected': True,
        'ig_user_id': {'$nin': [None, '']},
        'meta_access_token': {'$nin': [None, '']},
    }).limit(limit).to_list(limit)
    count = 0
    for user_doc in users:
        if await _sync_user_instagram_account_doc(user_doc):
            count += 1
    return count


async def _mark_instagram_account_expired(account: dict, reason: str = 'token_expired') -> dict:
    now = datetime.utcnow()
    update = {
        'refreshStatus': 'expired',
        'refreshError': {'reason': reason},
        'connectionValid': False,
        'refreshLockedUntil': None,
        'updatedAt': now,
    }
    await db.instagram_accounts.update_one({'id': account['id']}, {'$set': update})
    await db.users.update_one(
        {
            'id': account.get('userId') or account.get('user_id'),
            'ig_user_id': account.get('instagramAccountId') or account.get('igUserId'),
        },
        {'$set': {
            'instagram_connection_valid': False,
            'instagramConnectionValid': False,
            'instagram_connection_blocker': reason,
            'refreshStatus': 'expired',
            'updated': now,
        }},
    )
    await notifyTokenRefreshProblem('Instagram token expired; manual reconnect required', {
        'accountId': account.get('id'),
        'instagramAccountId': account.get('instagramAccountId') or account.get('igUserId'),
        'userId': account.get('userId') or account.get('user_id'),
    })
    return {'ok': False, 'status': 'expired', 'reason': reason}


async def refreshInstagramToken(accountId: str, force: bool = False) -> dict:
    """Refresh one long-lived Instagram token without blocking webhook/comment paths."""
    now = datetime.utcnow()
    account = await db.instagram_accounts.find_one({'id': accountId})
    if not account:
        return {'ok': False, 'status': 'not_found', 'accountId': accountId}

    token = account.get('accessToken') or ''
    if not token:
        await db.instagram_accounts.update_one(
            {'id': accountId},
            {'$set': {
                'refreshStatus': 'missing_token',
                'refreshError': {'reason': 'missing_access_token'},
                'updatedAt': now,
            }},
        )
        return {'ok': False, 'status': 'missing_token', 'accountId': accountId}

    expires_at = _parse_graph_datetime(account.get('tokenExpiresAt'))
    days = _days_until(expires_at, now) if expires_at else None
    if expires_at and expires_at <= now:
        return await _mark_instagram_account_expired(account)

    token_source = account.get('tokenSource') or ''
    if token_source and token_source != 'long_lived':
        await db.instagram_accounts.update_one(
            {'id': accountId},
            {'$set': {
                'refreshStatus': 'not_long_lived',
                'refreshError': {'reason': 'token_source_is_not_long_lived'},
                'refreshLockedUntil': None,
                'updatedAt': now,
            }},
        )
        return {'ok': True, 'status': 'skipped_not_long_lived', 'accountId': accountId}

    if not force and expires_at and expires_at > now + timedelta(days=TOKEN_REFRESH_LOOKAHEAD_DAYS):
        return {'ok': True, 'status': 'skipped_not_due', 'accountId': accountId}

    recent_cutoff = now - timedelta(hours=TOKEN_REFRESH_MIN_AGE_HOURS)
    last_touch = (
        _parse_graph_datetime(account.get('lastRefreshedAt')) or
        _parse_graph_datetime(account.get('createdAt'))
    )
    if not force and last_touch and last_touch > recent_cutoff:
        return {'ok': True, 'status': 'skipped_recently_refreshed', 'accountId': accountId}

    locked = await db.instagram_accounts.find_one_and_update(
        {
            'id': accountId,
            '$or': [
                {'refreshLockedUntil': {'$exists': False}},
                {'refreshLockedUntil': None},
                {'refreshLockedUntil': {'$lte': now}},
            ],
        },
        {'$set': {
            'refreshLockedUntil': now + timedelta(minutes=TOKEN_REFRESH_LOCK_MINUTES),
            'refreshStatus': 'refreshing',
            'updatedAt': now,
        }},
        return_document=ReturnDocument.AFTER,
    )
    if not locked:
        return {'ok': True, 'status': 'skipped_locked', 'accountId': accountId}

    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(
                'https://graph.instagram.com/refresh_access_token',
                params={'grant_type': 'ig_refresh_token', 'access_token': token},
            )
            try:
                body = r.json()
            except Exception:
                body = {'raw': r.text[:500]}

        if r.status_code == 200 and isinstance(body, dict):
            new_token = body.get('access_token') or ''
            expires_in = int(body.get('expires_in') or 0)
            if new_token and expires_in > 0:
                new_expires_at = now + timedelta(seconds=expires_in)
                update = {
                    'accessToken': new_token,
                    'tokenExpiresAt': new_expires_at,
                    'lastRefreshedAt': now,
                    'refreshStatus': 'ok',
                    'refreshError': None,
                    'refreshAttempts': 0,
                    'refreshLockedUntil': None,
                    'updatedAt': now,
                    'connectionValid': True,
                    'isActive': True,
                }
                await db.instagram_accounts.update_one({'id': accountId}, {'$set': update})
                await db.users.update_one(
                    {
                        'id': locked.get('userId') or locked.get('user_id'),
                        'ig_user_id': locked.get('instagramAccountId') or locked.get('igUserId'),
                    },
                    {'$set': {
                        'meta_access_token': new_token,
                        'tokenExpiresAt': new_expires_at,
                        'instagram_token_expires_at': new_expires_at,
                        'lastRefreshedAt': now,
                        'refreshStatus': 'ok',
                        'refreshError': None,
                        'refreshAttempts': 0,
                        'instagram_connection_valid': True,
                        'instagramConnectionValid': True,
                        'instagram_connection_blocker': None,
                        'updated': now,
                    }},
                )
                logger.info('instagram_token_refresh_ok account=%s expires_at=%s',
                            accountId, new_expires_at.isoformat())
                return {
                    'ok': True,
                    'status': 'refreshed',
                    'accountId': accountId,
                    'tokenExpiresAt': new_expires_at.isoformat(),
                }
            body = {'error': {'message': 'refresh response missing access_token or expires_in',
                              'status': r.status_code, 'keys': sorted(body.keys())}}

        safe_error = _redact_secrets(body)
        attempts = int(locked.get('refreshAttempts') or 0) + 1
        refresh_status = 'failed'
        if days is not None and days <= 3:
            refresh_status = 'critical'
        await db.instagram_accounts.update_one(
            {'id': accountId},
            {'$set': {
                'refreshStatus': refresh_status,
                'refreshError': safe_error,
                'refreshLockedUntil': None,
                'updatedAt': now,
            }, '$inc': {'refreshAttempts': 1}},
        )
        await db.users.update_one(
            {
                'id': locked.get('userId') or locked.get('user_id'),
                'ig_user_id': locked.get('instagramAccountId') or locked.get('igUserId'),
            },
            {'$set': {
                'refreshStatus': refresh_status,
                'refreshError': safe_error,
                'refreshAttempts': attempts,
                'updated': now,
            }},
        )
        if (days is not None and days < 7) or attempts >= 3:
            await notifyTokenRefreshProblem('Instagram token refresh failed', {
                'accountId': accountId,
                'instagramAccountId': locked.get('instagramAccountId') or locked.get('igUserId'),
                'daysUntilExpiry': round(days, 2) if days is not None else None,
                'attempts': attempts,
                'status': refresh_status,
                'error': safe_error,
            })
        return {
            'ok': False,
            'status': refresh_status,
            'accountId': accountId,
            'error': safe_error,
        }
    except Exception as e:
        safe_error = {'exception': str(e)[:500]}
        await db.instagram_accounts.update_one(
            {'id': accountId},
            {'$set': {
                'refreshStatus': 'failed',
                'refreshError': safe_error,
                'refreshLockedUntil': None,
                'updatedAt': now,
            }, '$inc': {'refreshAttempts': 1}},
        )
        await notifyTokenRefreshProblem('Instagram token refresh exception', {
            'accountId': accountId,
            'error': safe_error,
        })
        return {'ok': False, 'status': 'failed', 'accountId': accountId, 'error': safe_error}


async def runInstagramTokenRefreshCron() -> dict:
    await _ensure_instagram_account_docs_for_connected_users()
    now = datetime.utcnow()
    lookahead = now + timedelta(days=TOKEN_REFRESH_LOOKAHEAD_DAYS)
    accounts = await db.instagram_accounts.find({
        'isActive': {'$ne': False},
        'connectionValid': {'$ne': False},
        'accessToken': {'$nin': [None, '']},
        '$or': [
            {'tokenExpiresAt': {'$exists': False}},
            {'tokenExpiresAt': None},
            {'tokenExpiresAt': {'$lte': lookahead}},
        ],
    }).sort('tokenExpiresAt', 1).to_list(500)

    summary = {
        'totalChecked': len(accounts),
        'refreshed': 0,
        'skipped': 0,
        'failed': 0,
        'expiringSoon': 0,
        'critical': 0,
        'expired': 0,
        'results': [],
    }
    for account in accounts:
        row = _token_refresh_public_row(account, now)
        if row.get('daysUntilExpiry') is None or row['daysUntilExpiry'] <= TOKEN_REFRESH_LOOKAHEAD_DAYS:
            summary['expiringSoon'] += 1
        result = await refreshInstagramToken(account['id'])
        status = result.get('status')
        if status == 'refreshed':
            summary['refreshed'] += 1
        elif status in ('failed', 'critical'):
            summary['failed'] += 1
        elif status == 'expired':
            summary['expired'] += 1
            summary['failed'] += 1
        else:
            summary['skipped'] += 1
        refreshed_account = await db.instagram_accounts.find_one({'id': account['id']}) or account
        public_row = _token_refresh_public_row(refreshed_account)
        public_row['result'] = status
        summary['critical'] += 1 if public_row.get('critical') else 0
        summary['results'].append(public_row)
        if (
            public_row.get('expired') or
            (
                public_row.get('daysUntilExpiry') is not None and
                public_row['daysUntilExpiry'] < 3 and
                status != 'refreshed'
            )
        ):
            await notifyTokenRefreshProblem('Instagram token expiry critical', {
                'accountId': public_row.get('accountId'),
                'instagramAccountId': public_row.get('instagramAccountId'),
                'daysUntilExpiry': public_row.get('daysUntilExpiry'),
                'refreshStatus': public_row.get('refreshStatus'),
                'refreshAttempts': public_row.get('refreshAttempts'),
                'result': status,
            })
    logger.info('instagram_token_refresh_summary %s', {
        k: v for k, v in summary.items() if k != 'results'
    })
    return summary


def _comment_rule_trigger_value(rule: dict) -> str:
    trigger = rule.get('trigger') or ''
    if trigger:
        return str(trigger)
    for node in rule.get('nodes') or []:
        if node.get('type') == 'trigger':
            data = node.get('data') or {}
            node_trigger = data.get('trigger') or ''
            if node_trigger:
                return str(node_trigger)
    return ''


def _is_comment_automation_rule(rule: dict) -> bool:
    return _comment_rule_trigger_value(rule).lower().startswith('comment:')


def _selected_specific_media_id(rule: dict) -> Optional[str]:
    """Return the one selected media id for a single-post rule, else None."""
    media_id = str(rule.get('media_id') or rule.get('trigger_media_id') or '').strip()
    post_scope = str(rule.get('post_scope') or '').strip().lower()
    trigger = _comment_rule_trigger_value(rule).strip()
    trigger_l = trigger.lower()

    if post_scope in ('any', 'all', 'latest', 'next'):
        return None
    if trigger_l in ('comment:any', 'comment:all', 'comment:latest', 'comment:next'):
        return None

    trigger_media = ''
    if trigger_l.startswith('comment:'):
        trigger_media = trigger.split(':', 1)[1].strip()
        if trigger_media.lower() in ('any', 'all', 'latest', 'next'):
            return None

    selected = media_id or trigger_media
    if not selected:
        return None
    if media_id and trigger_media and media_id != trigger_media:
        return None
    if post_scope and post_scope != 'specific':
        return None
    return selected


def _normalize_historical_catchup_flag(rule: dict) -> bool:
    requested = bool(
        rule.get('process_existing_unreplied_comments')
        or rule.get('processExistingUnrepliedComments')
        or rule.get('processExistingComments')
    )
    return bool(requested and _selected_specific_media_id(rule))


def _historical_catchup_enabled_for_media(rule: dict, media_id: Optional[str]) -> bool:
    selected_media_id = _selected_specific_media_id(rule)
    return bool(
        rule.get('process_existing_unreplied_comments')
        and selected_media_id
        and media_id
        and selected_media_id == media_id
    )


def _comment_rule_scope(rule: dict, media_id: Optional[str] = None) -> str:
    selected_media_id = _selected_specific_media_id(rule)
    if selected_media_id:
        return 'specific_post_exact' if media_id and selected_media_id == media_id else 'specific_post_other'
    trigger = _comment_rule_trigger_value(rule).strip().lower()
    post_scope = str(rule.get('post_scope') or rule.get('postScope') or '').strip().lower()
    if trigger in ('comment:any', 'comment:all') or post_scope in ('any', 'all'):
        return 'broad'
    if post_scope in ('latest', 'next') or trigger in ('comment:latest', 'comment:next'):
        return 'scoped'
    return 'broad'


def _comment_rule_priority(rule: dict, media_id: Optional[str] = None) -> int:
    scope = _comment_rule_scope(rule, media_id)
    if scope == 'specific_post_exact':
        return 1
    if scope in ('specific_post_other', 'scoped'):
        return 2
    return 3


def _sort_comment_rules_by_priority(rules: list, media_id: Optional[str] = None) -> list:
    def key(rule: dict):
        return (
            _comment_rule_priority(rule, media_id),
            str(rule.get('createdAt') or rule.get('created') or ''),
            str(rule.get('id') or ''),
        )
    return sorted(list(rules or []), key=key)


async def _debug_token_with_ig_app(token: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        'debugTokenWorks': False,
        'tokenAppId': None,
        'matchesIgAppId': False,
        'scopes': [],
        'isValid': False,
        'expiresAt': None,
        'error': None,
    }
    if not token:
        out['error'] = 'token_missing'
        return out
    if not IG_APP_ID or not IG_APP_SECRET:
        out['error'] = 'ig_app_credentials_missing'
        return out
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                'https://graph.instagram.com/debug_token',
                params={
                    'input_token': token,
                    'access_token': f'{IG_APP_ID}|{IG_APP_SECRET}',
                },
            )
            body = r.json() if r.headers.get('content-type', '').startswith('application/json') else {}
            if r.status_code == 200:
                d = body.get('data') or {}
                token_app_id = str(d.get('app_id') or '') or None
                out.update({
                    'debugTokenWorks': True,
                    'tokenAppId': token_app_id,
                    'matchesIgAppId': bool(token_app_id and token_app_id == IG_APP_ID),
                    'scopes': d.get('scopes') or d.get('granular_scopes') or [],
                    'isValid': bool(d.get('is_valid')),
                    'expiresAt': d.get('expires_at'),
                })
            else:
                out['error'] = _safe_graph_error(body) or {'status': r.status_code}
    except Exception as e:
        out['error'] = str(e)[:200]
    return out


async def _verify_instagram_token(
    c: httpx.AsyncClient,
    token: str,
    oauth_user_id: str = '',
) -> Dict[str, Any]:
    fields = 'id,user_id,username,account_type'
    probes = [
        ('graph.instagram.com/me', 'https://graph.instagram.com/me'),
        ('graph.instagram.com/v21.0/me', 'https://graph.instagram.com/v21.0/me'),
    ]
    if oauth_user_id:
        probes.extend([
            ('graph.instagram.com/{oauth_user_id}', f'https://graph.instagram.com/{oauth_user_id}'),
            ('graph.instagram.com/v21.0/{oauth_user_id}', f'https://graph.instagram.com/v21.0/{oauth_user_id}'),
        ])

    probe_results: Dict[str, Any] = {}
    body: Dict[str, Any] = {}
    chosen_probe = None
    for label, url in probes:
        r = await c.get(url, params={'fields': fields, 'access_token': token})
        try:
            probe_body = r.json()
        except Exception:
            probe_body = {'raw': r.text[:300]}
        entry: Dict[str, Any] = {
            'status': r.status_code,
            'bodyKeys': sorted(probe_body.keys()) if isinstance(probe_body, dict) else [],
        }
        err = _safe_graph_error(probe_body)
        if err:
            entry['error'] = err
        probe_results[label] = entry
        if r.status_code == 200 and isinstance(probe_body, dict):
            body = probe_body
            chosen_probe = label
            break

    if not chosen_probe:
        first = next(iter(probe_results.values()), {})
        return {
            'ok': False,
            'status': first.get('status'),
            'probes': probe_results,
            'error': first.get('error') or {'message': 'profile_probe_failed'},
            'blocker': 'token_cannot_call_graph_me',
            'fix': 'Disconnect and reconnect Instagram, then verify a profile probe before saving the token.',
        }

    canonical_id = str(body.get('user_id') or body.get('id') or '')
    username = body.get('username') or ''
    account_type = body.get('account_type') or ''
    if not canonical_id:
        return {
            'ok': False,
            'status': 200,
            'probeUsed': chosen_probe,
            'probes': probe_results,
            'bodyKeys': sorted(body.keys()),
            'error': {'message': 'Graph /me did not return id or user_id'},
            'blocker': 'graph_me_missing_canonical_id',
        }
    if not username:
        return {
            'ok': False,
            'status': 200,
            'probeUsed': chosen_probe,
            'probes': probe_results,
            'bodyKeys': sorted(body.keys()),
            'error': {'message': 'Profile probe did not return username'},
            'blocker': 'token_cannot_read_profile',
        }
    if account_type not in VALID_IG_ACCOUNT_TYPES:
        return {
            'ok': False,
            'status': 200,
            'probeUsed': chosen_probe,
            'probes': probe_results,
            'bodyKeys': sorted(body.keys()),
            'error': {'message': f'Unsupported account_type: {account_type or "missing"}'},
            'blocker': 'instagram_account_type_not_supported',
        }
    return {
        'ok': True,
        'status': 200,
        'probeUsed': chosen_probe,
        'probes': probe_results,
        'bodyKeys': sorted(body.keys()),
        'canonicalIgId': canonical_id,
        'graphMeId': str(body.get('id') or ''),
        'graphMeUserId': str(body.get('user_id') or ''),
        'username': username,
        'accountType': account_type,
    }


async def _run_instagram_me_probes(c: httpx.AsyncClient, token: str) -> Dict[str, Any]:
    """Run the exact /me probe order used to validate OAuth tokens."""
    variants = [
        ('/me?fields=user_id,username', 'https://graph.instagram.com/me',
         {'fields': 'user_id,username'}),
        ('/me?fields=id,username', 'https://graph.instagram.com/me',
         {'fields': 'id,username'}),
        ('/me', 'https://graph.instagram.com/me', {}),
        ('/v25.0/me?fields=user_id,username', 'https://graph.instagram.com/v25.0/me',
         {'fields': 'user_id,username'}),
        ('/v25.0/me?fields=id,username', 'https://graph.instagram.com/v25.0/me',
         {'fields': 'id,username'}),
        ('/v25.0/me', 'https://graph.instagram.com/v25.0/me', {}),
    ]
    results = []
    for label, url, extra_params in variants:
        params = {**extra_params, 'access_token': token}
        try:
            r = await c.get(url, params=params)
            try:
                body = r.json()
            except Exception:
                body = {'raw': r.text[:300]}
            canonical_id = ''
            username = ''
            if r.status_code == 200 and isinstance(body, dict):
                canonical_id = str(body.get('user_id') or body.get('id') or '')
                username = body.get('username') or ''
            item = {
                'variant': label,
                'status': r.status_code,
                'bodyKeys': sorted(body.keys()) if isinstance(body, dict) else [],
                'canonicalIgUserIdExists': bool(canonical_id),
                'usernameExists': bool(username),
            }
            err = _safe_graph_error(body)
            if err:
                item['error'] = err
            results.append(item)
            if r.status_code == 200 and canonical_id:
                return {
                    'ok': True,
                    'results': results,
                    'whichMeVariantWorks': label,
                    'canonicalIgUserId': canonical_id,
                    'username': username,
                    'bodyKeys': item['bodyKeys'],
                }
        except Exception as e:
            results.append({'variant': label, 'status': 0, 'error': str(e)[:200]})
    return {
        'ok': False,
        'results': results,
        'whichMeVariantWorks': None,
        'canonicalIgUserId': None,
        'username': None,
    }


@api.get('/instagram/auth-url')
async def instagram_auth_url(
    mode: str = Query('connect'),
    returnTo: str = Query('/app/settings?tab=instagram'),
    user_id: str = Depends(get_current_active_user_id),
):
    if _rate_limited('instagram_connect', user_id,
                     limit=RATE_LIMIT_INSTAGRAM_CONNECT_PER_MIN, window_seconds=60):
        logger.warning('rate_limit_hit bucket=instagram_connect user_id=%s', user_id)
        raise HTTPException(429, 'Too many Instagram connection attempts. Try again in a minute.')
    if not IG_APP_ID or not IG_APP_SECRET:
        raise HTTPException(503, 'IG_APP_ID and IG_APP_SECRET are not configured. Set them in .env')
    redirect_uri = f"{BACKEND_PUBLIC_URL}/api/instagram/callback"
    oauth_mode = mode if mode in {'connect', 'add_account', 'reconnect'} else 'connect'
    # Phase 2.2 plan enforcement: block adding a NEW account if at cap.
    # 'reconnect' is always allowed (it replaces an existing connection,
    # not adding a new one).
    if oauth_mode == 'add_account':
        # Phase 2.18M: auto-heal inconsistent state before counting.
        # The user's screenshot showed Settings = "No account connected"
        # but the plan-cap check still saying "free allows 1, upgrade".
        # That happens when an old disconnect path left users.* at
        # disconnected but legacy instagram_accounts rows still had
        # connectionValid=True. We treat the users.* document as the
        # authoritative "is this user connected to ANY IG account"
        # signal — if it says no, we deactivate every still-valid row
        # owned by this user before we count.
        user_doc_for_cleanup = await db.users.find_one({'id': user_id}) or {}
        users_say_connected = bool(
            user_doc_for_cleanup.get('instagramConnected')
            or user_doc_for_cleanup.get('instagram_connection_valid')
        )
        if not users_say_connected:
            try:
                cleanup = await db.instagram_accounts.update_many(
                    {
                        '$or': [{'userId': user_id}, {'user_id': user_id}],
                        'connectionValid': True,
                    },
                    {'$set': {
                        'connectionValid': False,
                        'isActive': False,
                        'isCurrent': False,
                        'refreshStatus': 'auto_cleanup_users_disconnected',
                        'refreshLockedUntil': None,
                        'updatedAt': datetime.utcnow(),
                    }},
                )
                if getattr(cleanup, 'modified_count', 0) > 0:
                    logger.info(
                        'instagram_plan_check_auto_cleanup user_id=%s rows_cleaned=%s',
                        user_id, cleanup.modified_count,
                    )
                    await invalidate_dashboard_summary(user_id)
            except Exception:
                # Cleanup is best-effort — don't block the user.
                pass
        plan = await get_user_plan(user_id)
        effective = await compute_effective_limits(user_id)
        snapshots = await _usage_snapshots_for_user(user_id)
        connected = int(snapshots.get('instagram_accounts_connected_snapshot') or 0)
        max_accounts = effective.get('max_instagram_accounts')
        if max_accounts is not None and connected >= int(max_accounts):
            raise HTTPException(
                402,
                f'Plan {plan["plan_key"]} allows {max_accounts} Instagram '
                f'account(s); upgrade to connect more.',
            )
    return_to = _safe_return_to(returnTo)
    state = _sign_instagram_oauth_state({
        'userId': user_id,
        'mode': oauth_mode,
        'returnTo': return_to,
    })
    params = {
        'enable_fb_login': '0',
        'force_authentication': '1',
        'client_id': IG_APP_ID,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': IG_SCOPES,
        'state': state,
    }
    url = f"https://www.instagram.com/oauth/authorize?{urlencode(params)}"
    return {
        'url': url,
        'configured': True,
        'mode': oauth_mode,
        'returnTo': return_to,
        'redirect_uri': redirect_uri,
        'authorizeUrlDebug': {
            'host': 'www.instagram.com',
            'clientIdLast4': IG_APP_ID[-4:] if IG_APP_ID else None,
            'redirect_uri': redirect_uri,
            'scope': IG_SCOPES,
            'response_type': 'code',
            'force_authentication': '1',
        },
    }


@api.get('/instagram/callback')
async def instagram_callback(request: Request,
                             code: Optional[str] = Query(None),
                             state: Optional[str] = Query(None),
                              error: str = Query(None), error_description: str = Query(None)):
    from fastapi.responses import RedirectResponse
    audit: Dict[str, Any] = {
        'callbackPath': '/api/instagram/callback',
        'requestQueryParamsReceived': sorted(request.query_params.keys()),
        'codeExists': bool(code),
        'redirectUriUsed': f"{BACKEND_PUBLIC_URL}/api/instagram/callback",
        'clientIdSource': INSTAGRAM_APP_ID_SOURCE,
        'clientSecretSource': INSTAGRAM_APP_SECRET_SOURCE,
        'tokenExchangeEndpoint': 'https://api.instagram.com/oauth/access_token',
        'tokenExchangeResponseKeys': [],
        'shortLivedAccessTokenExists': False,
        'userIdReturnedFromTokenExchange': None,
        'permissionsReturned': [],
        'longLivedExchangeAttempted': False,
        'longLivedExchangeEndpoint': 'GET https://graph.instagram.com/access_token',
        'longLivedExchangeStatus': None,
        'longLivedExchangeResponseKeys': [],
        'longLivedExchangeError': None,
        'finalTokenStoredSource': None,
        'finalTokenLength': None,
        'finalTokenPrefix': None,
        'finalIgUserIdStoredSource': None,
        'verification': None,
        'debugToken': None,
        'stateValid': False,
        'mode': None,
        'returnTo': None,
        'createdAt': datetime.utcnow(),
    }

    state_payload = _verify_instagram_oauth_state(state)
    user_id = state_payload.get('userId') if state_payload else None
    oauth_mode = state_payload.get('mode') if state_payload else None
    return_to = state_payload.get('returnTo') if state_payload else '/app/settings?tab=instagram'
    audit['stateValid'] = bool(state_payload)
    audit['mode'] = oauth_mode
    audit['returnTo'] = return_to

    async def _store_oauth_failure(
        uid: Optional[str],
        blocker: str,
        detail: Any = None,
        clear_existing_connection: bool = True,
    ):
        if not uid:
            return
        update = {
            '$set': {
                'instagram_connection_blocker': blocker,
                'last_instagram_connect_error': blocker,
                'ig_oauth_last_audit': _redact_secrets({**audit, 'failureDetail': detail}),
                'updated': datetime.utcnow(),
            },
        }
        if clear_existing_connection:
            update['$set'].update({
                'instagramConnected': False,
                'instagram_connection_valid': False,
                'instagramConnectionValid': False,
            })
            update['$unset'] = {
                'meta_access_token': '',
                'ig_user_id': '',
                'instagramHandle': '',
                'instagram_account_type': '',
                'instagram_graph_me_id': '',
                'instagram_graph_me_user_id': '',
            }
        await db.users.update_one({'id': uid}, update)

    clear_existing_on_failure = oauth_mode != 'add_account'
    if error:
        logger.warning('IG OAuth denied: %s — %s', error, error_description)
        await _store_oauth_failure(
            user_id,
            'oauth_denied',
            {'error': error},
            clear_existing_connection=clear_existing_on_failure,
        )
        return RedirectResponse(_frontend_redirect_url(return_to, {'ig': 'error', 'reason': error}))
    if not state_payload:
        return RedirectResponse(_frontend_redirect_url(
            '/app/settings?tab=instagram',
            {'ig': 'error', 'reason': 'invalid_state'},
        ))
    if not code:
        await _store_oauth_failure(
            user_id,
            'oauth_code_missing',
            clear_existing_connection=clear_existing_on_failure,
        )
        return RedirectResponse(_frontend_redirect_url(return_to, {'ig': 'error', 'reason': 'missing_code'}))
    redirect_uri = audit['redirectUriUsed']
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            # 1) Exchange code for short-lived IG user token (form-encoded POST)
            r = await c.post(
                'https://api.instagram.com/oauth/access_token',
                data={
                    'client_id': IG_APP_ID,
                    'client_secret': IG_APP_SECRET,
                    'grant_type': 'authorization_code',
                    'redirect_uri': redirect_uri,
                    'code': code,
                },
            )
            data = r.json() if r.status_code == 200 else {'raw': r.text, 'status': r.status_code}
            audit['tokenExchangeStatus'] = r.status_code
            audit['tokenExchangeResponseKeys'] = sorted(data.keys()) if isinstance(data, dict) else []
            token = data.get('access_token')
            ig_user_id_from_oauth = str(data.get('user_id') or '')
            audit['shortLivedAccessTokenExists'] = bool(token)
            audit['userIdReturnedFromTokenExchange'] = ig_user_id_from_oauth or None
            audit['permissionsReturned'] = data.get('permissions') or data.get('scope') or []
            if not token:
                safe = _redact_secrets(data)
                logger.error('IG token exchange failed: %s', safe)
                await _store_oauth_failure(
                    user_id,
                    'token_exchange_failed',
                    safe,
                    clear_existing_connection=clear_existing_on_failure,
                )
                return RedirectResponse(_frontend_redirect_url(
                    return_to,
                    {'ig': 'error', 'reason': 'token_exchange_failed'},
                ))

            # OAuth validation path: verify the short token first, then try to
            # upgrade to long-lived without making that upgrade a blocker.
            audit['apiVersionsTested'] = ['unversioned', 'v25.0']
            short_me = await _run_instagram_me_probes(c, token)
            audit['shortTokenLength'] = len(token)
            audit['shortTokenMeResults'] = short_me['results']
            if not short_me.get('ok'):
                audit['failureStage'] = 'me_verification'
                audit['blocker'] = 'token_exchange_returns_unusable_instagram_token'
                audit['whichTokenWorks'] = 'none'
                audit['whichMeVariantWorks'] = None
                audit['finalTokenSource'] = None
                audit['finalTokenStoredSource'] = None
                audit['connectionSaved'] = False
                await _store_oauth_failure(
                    user_id,
                    'token_exchange_returns_unusable_instagram_token',
                    {'shortTokenMeResults': short_me['results']},
                    clear_existing_connection=clear_existing_on_failure,
                )
                return RedirectResponse(_frontend_redirect_url(
                    return_to,
                    {'ig': 'error', 'reason': 'token_cannot_call_graph_me'},
                ))

            audit['debugToken'] = await _debug_token_with_ig_app(token)
            audit['longLivedExchangeAttempted'] = True
            audit['longLivedExchangeMethodUsed'] = 'GET'
            audit['longLivedExchangeEndpoint'] = 'GET https://graph.instagram.com/access_token'
            ll = await c.get(
                'https://graph.instagram.com/access_token',
                params={
                    'grant_type': 'ig_exchange_token',
                    'client_secret': IG_APP_SECRET,
                    'access_token': token,
                },
            )
            try:
                ll_data = ll.json()
            except Exception:
                ll_data = {'raw': ll.text[:300]}
            audit['longLivedExchangeStatus'] = ll.status_code
            audit['longLivedExchangeResponseKeys'] = sorted(ll_data.keys()) if isinstance(ll_data, dict) else []
            if ll.status_code != 200:
                audit['longLivedExchangeError'] = _safe_graph_error(ll_data) or ll_data

            final_token = token
            final_token_source = 'short_lived'
            final_me = short_me
            long_token = ll_data.get('access_token') if ll.status_code == 200 and isinstance(ll_data, dict) else None
            audit['longTokenExists'] = bool(long_token)
            audit['longTokenMeResults'] = None
            if long_token:
                long_me = await _run_instagram_me_probes(c, long_token)
                audit['longTokenMeResults'] = long_me['results']
                if long_me.get('ok'):
                    final_token = long_token
                    final_token_source = 'long_lived'
                    final_me = long_me
                else:
                    audit['warning'] = 'long_token_me_failed'
            else:
                audit['warning'] = 'long_lived_exchange_failed'

            audit['whichTokenWorks'] = final_token_source
            audit['whichMeVariantWorks'] = final_me.get('whichMeVariantWorks')
            audit['finalTokenSource'] = final_token_source
            audit['finalTokenStoredSource'] = final_token_source
            audit['finalTokenLength'] = len(final_token)
            audit['finalTokenPrefix'] = _token_prefix(final_token)
            audit['finalIgUserIdStoredSource'] = 'me_user_id_or_id'
            final_token_expires_at = None
            try:
                final_expires_in = (
                    int(ll_data.get('expires_in') or 0)
                    if final_token_source == 'long_lived' and isinstance(ll_data, dict)
                    else int(data.get('expires_in') or 0)
                )
            except Exception:
                final_expires_in = 0
            if final_expires_in > 0:
                final_token_expires_at = datetime.utcnow() + timedelta(seconds=final_expires_in)
            audit['tokenExpiresAt'] = final_token_expires_at.isoformat() if final_token_expires_at else None
            audit['verification'] = {
                'ok': True,
                'canonicalIgId': final_me['canonicalIgUserId'],
                'username': final_me.get('username') or '',
                'probeUsed': final_me.get('whichMeVariantWorks'),
            }
            audit['connectionSaved'] = True
            try:
                await _ensure_instagram_account_connect_allowed(
                    user_id, final_me['canonicalIgUserId'],
                )
            except HTTPException as exc:
                detail = exc.detail if isinstance(exc.detail, dict) else {}
                if exc.status_code == 409 and detail.get('code') == 'instagram_account_already_connected':
                    await _store_oauth_failure(
                        user_id,
                        'instagram_account_already_connected',
                        clear_existing_connection=False,
                        audit={'canonicalIgUserIdExists': True},
                    )
                    return RedirectResponse(_frontend_redirect_url(return_to, {
                        'ig': 'error',
                        'reason': 'instagram_account_already_connected',
                    }))
                raise

            await db.users.update_one(
                {'id': user_id},
                {'$set': {
                    'instagramConnected': True,
                    'instagram_connection_valid': True,
                    'instagramConnectionValid': True,
                    'instagram_connection_blocker': None,
                    'instagramHandle': final_me.get('username') or '',
                    'ig_user_id': final_me['canonicalIgUserId'],
                    'meta_access_token': final_token,
                    'ig_auth_kind': 'instagram_business_login',
                    'instagramTokenSource': final_token_source,
                    'instagram_token_source': final_token_source,
                    'tokenExpiresAt': final_token_expires_at,
                    'instagram_token_expires_at': final_token_expires_at,
                    'lastRefreshedAt': datetime.utcnow() if final_token_source == 'long_lived' else None,
                    'refreshStatus': 'ok' if final_token_source == 'long_lived' else 'not_long_lived',
                    'refreshError': None,
                    'refreshAttempts': 0,
                    'ig_oauth_last_audit': _redact_secrets(audit),
                }},
            )
            account_doc = await _sync_user_instagram_account_doc({
                'id': user_id,
                'instagramConnected': True,
                'instagram_connection_valid': True,
                'instagramHandle': final_me.get('username') or '',
                'ig_user_id': final_me['canonicalIgUserId'],
                'meta_access_token': final_token,
                'ig_auth_kind': 'instagram_business_login',
                'instagram_token_source': final_token_source,
                'tokenExpiresAt': final_token_expires_at,
            }, access_token=final_token, token_expires_at=final_token_expires_at,
                token_source=final_token_source,
                refresh_status='ok' if final_token_source == 'long_lived' else 'not_long_lived',
                last_refreshed_at=datetime.utcnow() if final_token_source == 'long_lived' else None)
            connected_account_id = (account_doc or {}).get('id')
            if connected_account_id:
                audit['connectedInstagramAccountId'] = connected_account_id
                await db.users.update_one(
                    {'id': user_id},
                    {'$set': {'ig_oauth_last_audit': _redact_secrets(audit)}},
                )
                await instagram_account_activate(connected_account_id, user_id=user_id)
                if oauth_mode != 'add_account':
                    cleanup = await db.instagram_accounts.update_many(
                        {
                            '$or': [{'userId': user_id}, {'user_id': user_id}],
                            'id': {'$ne': connected_account_id},
                            'connectionValid': True,
                        },
                        {'$set': {
                            'connectionValid': False,
                            'isActive': False,
                            'isCurrent': False,
                            'refreshStatus': 'replaced_by_reconnect',
                            'refreshLockedUntil': None,
                            'updatedAt': datetime.utcnow(),
                        }},
                    )
                    if getattr(cleanup, 'modified_count', 0):
                        await invalidate_dashboard_summary(user_id)
                await _safe_record_usage_event(
                    user_id=user_id,
                    event_type='instagram_account_connected',
                    instagram_account_id=(account_doc or {}).get('instagramAccountId') or final_me['canonicalIgUserId'],
                    metadata={
                        'account_db_id': connected_account_id,
                        'token_source': final_token_source,
                        'mode': oauth_mode,
                    },
                )
            logger.info('IG connected (Business Login) for user %s via %s',
                        user_id, audit['whichMeVariantWorks'])
            return RedirectResponse(_frontend_redirect_url(
                return_to,
                {'ig': 'connected', 'accountId': connected_account_id or ''},
            ))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception('IG callback failed')
        from fastapi.responses import RedirectResponse
        await _store_oauth_failure(
            user_id,
            'server_error',
            str(e)[:200],
            clear_existing_connection=clear_existing_on_failure,
        )
        return RedirectResponse(_frontend_redirect_url(return_to, {'ig': 'error', 'reason': 'server_error'}))
    from fastapi.responses import RedirectResponse
    return RedirectResponse(_frontend_redirect_url(return_to, {'ig': 'connected'}))







@api.get('/instagram/oauth/last-attempt')
async def oauth_last_attempt(user_id: str = Depends(get_current_active_user_id)):
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    audit = u.get('ig_oauth_last_audit') or {}
    
    return {
        "callbackPath": audit.get("callbackPath", "/api/instagram/oauth/callback"),
        "codeReceived": audit.get("codeReceived", False),
        "codeLength": audit.get("codeLength", 0),
        "redirectUriUsedInAuthorize": audit.get("redirectUriUsedInAuthorize", ""),
        "redirectUriUsedInTokenExchange": audit.get("redirectUriUsedInTokenExchange", ""),
        "redirectUriExactMatch": audit.get("redirectUriExactMatch", False),
        "clientIdSource": audit.get("clientIdSource", "UNKNOWN"),
        "clientIdLast4": audit.get("clientIdLast4", ""),
        "clientSecretSource": audit.get("clientSecretSource", "UNKNOWN"),
        "tokenExchangeEndpoint": audit.get("tokenExchangeEndpoint", ""),
        "tokenExchangeStatus": audit.get("tokenExchangeStatus", 0),
        "tokenExchangeResponseKeys": audit.get("tokenExchangeResponseKeys", []),
        "shortTokenExists": audit.get("shortTokenExists", False),
        "shortTokenLength": audit.get("shortTokenLength", 0),
        "tokenExchangeUserId": audit.get("tokenExchangeUserId", ""),
        "longLivedExchangeAttempted": audit.get("longLivedExchangeAttempted", False),
        "longLivedExchangeEndpoint": audit.get("longLivedExchangeEndpoint", ""),
        "longLivedExchangeStatus": audit.get("longLivedExchangeStatus", 0),
        "longLivedResponseKeys": audit.get("longLivedResponseKeys", []),
        "longTokenExists": audit.get("longTokenExists", False),
        "longTokenLength": audit.get("longTokenLength", 0),
        "finalTokenSource": audit.get("finalTokenSource", "none"),
        "shortTokenMeStatus": audit.get("shortTokenMeStatus", 0),
        "shortTokenMeBody": audit.get("shortTokenMeBody", {}),
        "longTokenMeStatus": audit.get("longTokenMeStatus", 0),
        "longTokenMeBody": audit.get("longTokenMeBody", {}),
        "minimalMeStatus": audit.get("minimalMeStatus", 0),
        "minimalMeBody": audit.get("minimalMeBody", {}),
        "whichTokenWorks": audit.get("whichTokenWorks", "none"),
        "connectionSaved": audit.get("connectionSaved", False),
        "failureStage": audit.get("failureStage", audit.get("blocker", "unknown"))
    }

@api.delete('/admin/users/{email}')
async def admin_delete_user(email: str, user_id: str = Depends(get_current_active_user_id)):
    """Legacy hard-delete helper for disposable test accounts only.

    This endpoint keeps the historical route for compatibility, but it is
    admin-gated and cannot be used by ordinary authenticated users.
    """
    await _require_admin_permission(user_id, _admin_roles.PERM_USERS_MANAGE)
    requester = await db.users.find_one({'id': user_id})
    if not requester:
        raise HTTPException(404, 'requester not found')
    target = await db.users.find_one({'email': email.lower()})
    if not target:
        target = await db.users.find_one({'email': email})
    if not target:
        raise HTTPException(404, f'user {email} not found')
    target_email = (target.get('email') or '').lower()
    is_test = target_email.endswith('@test.com') or target_email.endswith('@example.com')
    if not is_test:
        raise HTTPException(403, 'Hard delete is limited to disposable test accounts')
    target_id = target['id']
    # Clean up associated data
    await db.dm_rules.delete_many({'user_id': target_id})
    await db.dm_logs.delete_many({'user_id': target_id})
    await db.automations.delete_many({'user_id': target_id})
    await db.conversations.delete_many({'user_id': target_id})
    await db.users.delete_one({'id': target_id})
    logger.info('admin_delete_user email=%s by=%s', email, user_id)
    return {'ok': True, 'deleted': email}


@api.get('/instagram/status')
async def instagram_status(user_id: str = Depends(get_current_active_user_id)):
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    connected = bool(u.get('instagramConnected') and u.get('instagram_connection_valid'))
    return {
        'connected': connected,
        'handle': u.get('instagramHandle'),
        'profilePictureUrl': u.get('instagram_profile_picture_url'),
        'followers': u.get('instagramFollowers', 0),
        'ig_user_id': u.get('ig_user_id'),
        'connectionValid': bool(u.get('instagram_connection_valid')),
        'connectionBlocker': u.get('instagram_connection_blocker'),
        'accountType': u.get('instagram_account_type'),
        'meta_configured': bool(META_APP_ID and META_APP_SECRET),
    }


@api.get('/instagram/profile')
async def instagram_profile(user_id: str = Depends(get_current_active_user_id)):
    """Return safe, user-scoped Instagram profile data for UI previews.

    Never returns the access token. If Graph does not expose a profile picture
    for the current token, we fall back to the stored account metadata.
    """
    account = await getActiveInstagramAccount(user_id)
    connected = bool(account.get('connectionValid') and account.get('accessToken'))
    out = {
        'connected': connected,
        'accountId': account.get('id'),
        'username': account.get('username') or None,
        'profilePictureUrl': account.get('profilePictureUrl') or account.get('profile_picture_url') or None,
        'igUserId': account.get('instagramAccountId') or account.get('igUserId') or None,
        'accountType': account.get('accountType') or None,
    }
    token = account.get('accessToken') or ''
    if not connected or not token:
        return out

    try:
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(
                'https://graph.instagram.com/me',
                params={
                    'access_token': token,
                    'fields': 'id,user_id,username,profile_picture_url,account_type',
                },
            )
            if r.status_code == 200:
                body = r.json() or {}
                username = body.get('username') or out['username']
                profile_picture_url = body.get('profile_picture_url') or out['profilePictureUrl']
                account_type = body.get('account_type') or out['accountType']
                canonical_id = str(body.get('user_id') or body.get('id') or out['igUserId'] or '')
                out.update({
                    'username': username,
                    'profilePictureUrl': profile_picture_url,
                    'igUserId': canonical_id or out['igUserId'],
                    'accountType': account_type,
                })
                await db.users.update_one(
                    {'id': user_id},
                    {'$set': {
                        'instagramHandle': username,
                        'instagram_profile_picture_url': profile_picture_url,
                        'instagram_account_type': account_type,
                    }},
                )
                await db.instagram_accounts.update_one(
                    {'id': account['id'], 'userId': user_id},
                    {'$set': {
                        'username': username,
                        'profilePictureUrl': profile_picture_url,
                        'accountType': account_type,
                        'updatedAt': datetime.utcnow(),
                    }},
                )
            else:
                out['profilePictureUnavailable'] = True
    except Exception as e:
        out['profilePictureUnavailable'] = True
        out['error'] = str(e)[:160]
    return out


@api.post('/instagram/subscribe-webhook')
async def instagram_subscribe_webhook(user_id: str = Depends(get_current_active_user_id)):
    """Force-subscribe the user's connected IG user to webhook fields via
    Instagram API (graph.instagram.com). Requires an IG user access token
    obtained through Instagram Business Login."""
    account = await getActiveInstagramAccount(user_id)
    token = account.get('accessToken') or ''
    ig_user_id = account.get('instagramAccountId') or account.get('igUserId') or ''
    if not ig_user_id:
        raise HTTPException(400, 'ig_user_id missing — reconnect Instagram')
    async with httpx.AsyncClient(timeout=20) as c:
        try:
            ig_sub = await c.post(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={
                    'access_token': token,
                    'subscribed_fields': (
                        'comments,messages,messaging_postbacks,'
                        'messaging_seen,message_reactions,live_comments'
                    ),
                },
            )
            verify = await c.get(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={'access_token': token},
            )
            return {
                'ok': ig_sub.status_code == 200,
                'ig_user_id': ig_user_id,
                'subscribe_status': ig_sub.status_code,
                'subscribe_body': ig_sub.text[:500],
                'verify_status': verify.status_code,
                'verify_body': verify.json() if verify.status_code == 200 else verify.text[:500],
            }
        except Exception as e:
            raise HTTPException(500, str(e))


@api.post('/instagram/subscribe-webhook-legacy')
async def instagram_subscribe_webhook_legacy(user_id: str = Depends(get_current_active_user_id)):
    """Legacy Page-based subscribe (kept for old Facebook-Login-flow users)."""
    u = await db.users.find_one({'id': user_id})
    if not _has_valid_instagram_connection(u):
        raise HTTPException(400, _instagram_connection_error(u))
    token = u.get('meta_access_token', '')
    # Look up page id fresh. The stored token is usually a PAGE access token
    # (set during OAuth callback) so /me/accounts may return nothing — fall
    # back to /me which, for a page token, returns the Page itself.
    async with httpx.AsyncClient(timeout=20) as c:
        page_id = None
        page_token = token
        accs = await c.get('https://graph.facebook.com/v21.0/me/accounts',
                           params={'access_token': token,
                                   'fields': 'id,name,access_token,instagram_business_account'})
        data = accs.json().get('data', []) if accs.status_code == 200 else []
        for acc in data:
            if acc.get('instagram_business_account'):
                page_id = acc.get('id')
                page_token = acc.get('access_token') or token
                break
        if not page_id:
            # Page-token fallback: /me returns the page itself
            me = await c.get('https://graph.facebook.com/v21.0/me',
                             params={'access_token': token,
                                     'fields': 'id,name,instagram_business_account'})
            if me.status_code == 200:
                mb = me.json()
                if mb.get('instagram_business_account') or mb.get('id'):
                    page_id = mb.get('id')
                    page_token = token
        if not page_id and u.get('fb_page_id'):
            page_id = u['fb_page_id']
            page_token = token
        if not page_id:
            raise HTTPException(404, f'No page found. /me/accounts={accs.text[:200]}')
        # Try each field independently so a missing permission on one
        # doesn't block the others.
        candidate_fields = [
            'feed',
            'messages', 'messaging_postbacks', 'messaging_optins',
            'message_deliveries', 'message_reads',
            'message_reactions', 'messaging_referrals',
        ]
        field_results = {}
        any_ok = False
        for f in candidate_fields:
            try:
                r = await c.post(
                    f"https://graph.facebook.com/v21.0/{page_id}/subscribed_apps",
                    params={'access_token': page_token, 'subscribed_fields': f},
                )
                field_results[f] = {'status': r.status_code, 'body': r.text[:300]}
                if r.status_code == 200:
                    any_ok = True
            except Exception as e:
                field_results[f] = {'status': 0, 'body': str(e)}
        ok = any_ok
        import json as _json
        body = _json.dumps(field_results)[:2000]
        # Keep `sub` defined for the legacy return keys
        class _S: pass
        sub = _S()
        sub.status_code = 200 if any_ok else 403
        sub.text = body
        # Verify the subscription
        verify = await c.get(f"https://graph.facebook.com/v21.0/{page_id}/subscribed_apps",
                             params={'access_token': page_token})
        # Also subscribe the Instagram user itself (required for comments/mentions
        # webhooks to route to our app under the new IG Graph API).
        ig_user_id = u.get('ig_user_id', '')
        ig_sub_status = None
        ig_sub_body = None
        if ig_user_id:
            try:
                ig_sub = await c.post(
                    f"https://graph.facebook.com/v21.0/{ig_user_id}/subscribed_apps",
                    params={'access_token': page_token,
                            'subscribed_fields': 'comments,messages,mentions,message_reactions,live_comments'},
                )
                ig_sub_status = ig_sub.status_code
                ig_sub_body = ig_sub.text
            except Exception as e:
                ig_sub_body = str(e)
        # Persist legacy Page credentials separately. Do not overwrite
        # meta_access_token, which must remain the verified IG user token.
        await db.users.update_one(
            {'id': user_id},
            {'$set': {'fb_page_id': page_id, 'fb_page_access_token': page_token}},
        )
        return {'ok': ok, 'status': sub.status_code, 'body': body,
                'page_id': page_id, 'ig_user_id': ig_user_id,
                'ig_subscribe_status': ig_sub_status, 'ig_subscribe_body': ig_sub_body,
                'subscribed_apps': verify.json()}


@api.get('/instagram/force-resubscribe')
async def instagram_force_resubscribe(email: str, key: str, fields: str = ''):
    """Admin tool: DELETE then POST the user's webhook subscription to force
    Meta to re-establish delivery. DISABLED in production — use the
    authenticated /api/instagram/dm/resubscribe endpoint instead."""
    raise HTTPException(403, 'Disabled in production. Use /api/instagram/dm/resubscribe with JWT auth.')
    u = await db.users.find_one({'email': email.lower()})
    if not u:
        u = await db.users.find_one({'email': email})
    if not u:
        raise HTTPException(404, 'user not found')
    token = u.get('meta_access_token', '')
    ig_user_id = u.get('ig_user_id', '')
    if not (token and ig_user_id):
        raise HTTPException(400, 'user missing token or ig_user_id')
    want_fields = fields or 'comments,messages,messaging_postbacks,messaging_seen,message_reactions,live_comments'
    out = {'email': email, 'ig_user_id': ig_user_id, 'requested_fields': want_fields}
    async with httpx.AsyncClient(timeout=30) as c:
        try:
            d = await c.delete(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={'access_token': token},
            )
            out['delete'] = {'status': d.status_code, 'body': d.text[:500]}
        except Exception as e:
            out['delete'] = {'error': str(e)}
        try:
            p = await c.post(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={'access_token': token, 'subscribed_fields': want_fields},
            )
            out['post'] = {'status': p.status_code, 'body': p.text[:500]}
        except Exception as e:
            out['post'] = {'error': str(e)}
        try:
            g = await c.get(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={'access_token': token},
            )
            out['verify'] = {'status': g.status_code,
                             'body': g.json() if g.status_code == 200 else g.text[:500]}
        except Exception as e:
            out['verify'] = {'error': str(e)}
    return out


@api.get('/instagram/debug-dump')
async def instagram_debug_dump(email: str = '', key: str = '', media_id: str = ''):
    """FULL diagnostic dump — DISABLED in production.
    Use the authenticated diagnostic endpoints instead:
      GET /api/instagram/credentials/diagnostics
      GET /api/instagram/dm/debug-latest
      GET /api/instagram/oauth/last-attempt
    """
    raise HTTPException(403, 'Disabled in production. Use authenticated diagnostic endpoints with JWT auth.')
    token = u.get('meta_access_token', '')
    ig_user_id = u.get('ig_user_id', '')
    page_id = u.get('fb_page_id', '')
    automations = await db.automations.find({'user_id': u.get('id')}).to_list(100)
    for a in automations:
        a.pop('_id', None)
    out = {
        'user': {
            'id': u.get('id'),
            'email': u.get('email'),
            'instagramConnected': u.get('instagramConnected'),
            'instagramHandle': u.get('instagramHandle'),
            'ig_user_id': ig_user_id,
            'fb_page_id': page_id,
            'has_meta_token': bool(token),
            'meta_token_prefix': (token or '')[:20],
        },
        'automations': automations,
    }
    async with httpx.AsyncClient(timeout=20) as c:
        try:
            me = await c.get('https://graph.instagram.com/me',
                             params={'access_token': token,
                                     'fields': 'user_id,username,name,account_type,followers_count'})
            out['graph_me'] = {'status': me.status_code,
                               'body': me.json() if me.status_code == 200 else me.text[:300]}
        except Exception as e:
            out['graph_me'] = {'error': str(e)}
        if ig_user_id:
            try:
                igs = await c.get(f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                                  params={'access_token': token})
                out['ig_subscribed_apps'] = {
                    'status': igs.status_code,
                    'body': igs.json() if igs.status_code == 200 else igs.text[:500]
                }
            except Exception as e:
                out['ig_subscribed_apps'] = {'error': str(e)}
            try:
                igp = await c.post(
                    f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                    params={'access_token': token,
                            'subscribed_fields': 'comments,messages,messaging_postbacks,messaging_seen,message_reactions,live_comments'},
                )
                out['ig_subscribe_attempt'] = {'status': igp.status_code, 'body': igp.text[:500]}
            except Exception as e:
                out['ig_subscribe_attempt'] = {'error': str(e)}
    # Include recent webhook deliveries for diagnosis
    try:
        hooks = await db.webhook_log.find().sort('received', -1).limit(30).to_list(30)
        for h in hooks:
            h.pop('_id', None)
            if isinstance(h.get('received'), datetime):
                h['received'] = h['received'].isoformat()
        out['recent_webhooks'] = hooks
        out['webhook_count'] = await db.webhook_log.count_documents({})
    except Exception as e:
        out['recent_webhooks'] = {'error': str(e)}
    # Fetch comments directly from IG Graph API to see whether the "HI" comment
    # even reached IG. Use media_id from query if provided, else fall back to
    # every automation's trigger_media_id.
    target_media_ids: list = []
    if media_id:
        target_media_ids.append(media_id)
    for a in automations:
        mid = a.get('trigger_media_id') or a.get('media_id')
        if mid and mid not in target_media_ids:
            target_media_ids.append(mid)
    if token and target_media_ids:
        media_checks = {}
        async with httpx.AsyncClient(timeout=20) as c:
            for mid in target_media_ids[:5]:
                try:
                    # Fetch comments on the media
                    cr = await c.get(
                        f'https://graph.instagram.com/{mid}/comments',
                        params={'access_token': token,
                                'fields': 'id,text,username,timestamp,from',
                                'limit': 25},
                    )
                    body = cr.json() if cr.status_code == 200 else cr.text[:500]
                    # Also fetch basic media info
                    mr = await c.get(
                        f'https://graph.instagram.com/{mid}',
                        params={'access_token': token,
                                'fields': 'id,caption,comments_count,like_count,timestamp,permalink,media_type'},
                    )
                    minfo = mr.json() if mr.status_code == 200 else mr.text[:300]
                    media_checks[mid] = {
                        'comments_status': cr.status_code,
                        'comments': body,
                        'media_info_status': mr.status_code,
                        'media_info': minfo,
                    }
                except Exception as e:
                    media_checks[mid] = {'error': str(e)}
        out['media_checks'] = media_checks
    return out


@api.get('/instagram/media')
async def instagram_media(user_id: str = Depends(get_current_active_user_id), limit: int = 25):
    """List the user's recent Instagram posts via Graph API.

    Always uses /me/media as the primary endpoint (Instagram Business Login).
    If /me/media fails, optionally tries /{ig_user_id}/media — but the
    response is always shaped consistently:

      ok=true   when /me/media returned 200 (count may still be 0)
      ok=false  when /me/media failed; error details + optional fallback info

    The wizard never sees the /{ig_user_id}/media error if /me/media succeeded.
    """
    account = await getActiveInstagramAccount(user_id)
    if not account.get('connectionValid'):
        raise HTTPException(400, 'Instagram account is not connected')
    token = account.get('accessToken', '')
    ig_id = str(account.get('instagramAccountId') or account.get('igUserId') or '')
    if not token:
        raise HTTPException(400, 'Missing access token')
    fields = 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,comments_count'
    lim = max(1, min(limit, 50))
    me_id_for_debug = None
    
    endpoints = []
    endpoints.append(('https://graph.instagram.com/me/media', '/me/media'))
    endpoints.append(('https://graph.instagram.com/v21.0/me/media', '/v21.0/me/media'))
    if ig_id:
        endpoints.append((f'https://graph.instagram.com/{ig_id}/media', f'/{ig_id}/media'))
        endpoints.append((f'https://graph.facebook.com/v21.0/{ig_id}/media', f'graph.facebook.com/v21.0/{ig_id}/media'))

    errors = {}

    # Phase 2.18E performance: the hot path for the wizard is /me/media
    # alone — typical latency to Instagram Graph is 300-800ms per call,
    # so we used to add a serial /me debug call (extra 300-800ms) before
    # the actual media fetch. We now fire the /me debug call in
    # parallel with the first media endpoint, so the wizard sees /me/media
    # data as soon as it returns instead of waiting on a metadata probe.
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            async def _fetch_me():
                try:
                    mer = await c.get(
                        'https://graph.instagram.com/me',
                        params={'access_token': token, 'fields': 'user_id,id,username'},
                    )
                    if mer.status_code == 200:
                        body = mer.json() or {}
                        return str(body.get('user_id') or body.get('id') or '') or None
                except Exception:
                    pass
                return None

            async def _fetch_first_endpoint():
                url, label = endpoints[0]
                try:
                    r = await c.get(url, params={'access_token': token, 'fields': fields, 'limit': lim})
                    return label, r
                except Exception as e:
                    return label, e

            me_id_for_debug, (first_label, first_result) = await asyncio.gather(
                _fetch_me(), _fetch_first_endpoint(),
            )

            if not isinstance(first_result, Exception) and first_result.status_code == 200:
                items = (first_result.json() or {}).get('data') or []
                return {
                    'ok': True,
                    'accountId': account.get('id'),
                    'endpointUsed': first_label,
                    'media': items,
                    'items': items,
                    'count': len(items),
                    'warning': None if items else f'No media returned from {first_label}',
                    'errors': errors,
                    'graphMeId': me_id_for_debug,
                    'dbIgUserId': ig_id or None,
                    'idMatch': (bool(me_id_for_debug) and me_id_for_debug == ig_id) if ig_id else None,
                }

            # First endpoint failed; record reason and fall through to the
            # legacy serial fallback over the remaining endpoints.
            if isinstance(first_result, Exception):
                errors[first_label] = {'exception': str(first_result)[:200]}
            else:
                errors[first_label] = {'status': first_result.status_code, 'body': first_result.text[:500]}

            for url, label in endpoints[1:]:
                try:
                    r = await c.get(url, params={'access_token': token, 'fields': fields, 'limit': lim})
                    if r.status_code == 200:
                        items = (r.json() or {}).get('data') or []
                        return {
                            'ok': True,
                            'accountId': account.get('id'),
                            'endpointUsed': label,
                            'media': items,
                            'items': items,
                            'count': len(items),
                            'warning': None if items else f'No media returned from {label}',
                            'errors': errors,
                            'graphMeId': me_id_for_debug,
                            'dbIgUserId': ig_id or None,
                            'idMatch': (bool(me_id_for_debug) and me_id_for_debug == ig_id) if ig_id else None,
                        }
                    else:
                        errors[label] = {'status': r.status_code, 'body': r.text[:500]}
                except Exception as e:
                    errors[label] = {'exception': str(e)[:200]}

        return {
            'ok': False,
            'accountId': account.get('id'),
            'endpointUsed': None,
            'media': [],
            'items': [],
            'count': 0,
            'error': {'body': 'All media endpoints failed', 'details': errors},
            'graphMeId': me_id_for_debug,
            'dbIgUserId': ig_id or None,
        }
    except Exception as e:
        logger.exception('IG media fetch failed')
        return {
            'ok': False,
            'media': [],
            'items': [],
            'count': 0,
            'error': {'body': str(e)},
        }


@api.get('/instagram/media/diagnostics')
async def instagram_media_diagnostics(user_id: str = Depends(get_current_active_user_id)):
    """Self-diagnose why /instagram/media may be returning empty.

    Hits /me, /me/media, and /{ig_user_id}/media against graph.instagram.com
    using the stored long-lived IG user token. Compares the Graph /me id
    with the value persisted in users.ig_user_id and returns a structured
    blocker classification. Never returns the raw token.
    """
    u = await db.users.find_one({'id': user_id})
    out: Dict[str, Any] = {
        'connected': False,
        'dbIgUserId': None,
        'graphMeId': None,
        'idMatch': None,
        'username': None,
        'accountType': None,
        'tokenExists': False,
        'tokenValid': None,
        'tokenLength': 0,
        'authKind': None,
        'mediaEndpointUsed': None,
        'meMediaCount': None,
        'igUserMediaCount': None,
        'mediaCount': 0,
        'firstMediaPreview': None,
        'errors': {},
        'blocker': None,
    }
    if not u:
        out['blocker'] = 'user_not_found'
        return out
    out['connected'] = _has_valid_instagram_connection(u)
    db_ig_id = str(u.get('ig_user_id') or '')
    token = u.get('meta_access_token', '') or ''
    out['dbIgUserId'] = db_ig_id or None
    out['tokenExists'] = bool(token)
    out['tokenLength'] = len(token)
    out['authKind'] = u.get('ig_auth_kind')
    if not out['connected']:
        out['blocker'] = u.get('instagram_connection_blocker') or 'token_cannot_call_graph_me'
        out['errors']['connection'] = _instagram_connection_error(u)
        return out
    if not token:
        out['blocker'] = 'token_missing'
        return out

    async with httpx.AsyncClient(timeout=20) as c:
        # 1) /me
        try:
            r = await c.get(
                'https://graph.instagram.com/me',
                params={
                    'access_token': token,
                    'fields': 'user_id,id,username,account_type',
                },
            )
            if r.status_code == 200:
                me = r.json() or {}
                out['graphMeId'] = str(me.get('user_id') or me.get('id') or '') or None
                out['username'] = me.get('username')
                out['accountType'] = me.get('account_type')
                out['tokenValid'] = True
            else:
                out['tokenValid'] = False
                out['errors']['me'] = {'status': r.status_code, 'body': r.text[:400]}
        except Exception as e:
            out['errors']['me'] = {'exception': str(e)}

        # 2) /me/media
        try:
            r = await c.get(
                'https://graph.instagram.com/me/media',
                params={
                    'access_token': token,
                    'fields': 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp',
                    'limit': 5,
                },
            )
            if r.status_code == 200:
                items = (r.json() or {}).get('data') or []
                out['meMediaCount'] = len(items)
                if items and not out['firstMediaPreview']:
                    p = items[0]
                    out['firstMediaPreview'] = {
                        'id': p.get('id'),
                        'media_type': p.get('media_type'),
                        'permalink': p.get('permalink'),
                        'timestamp': p.get('timestamp'),
                        'caption': (p.get('caption') or '')[:120],
                        'has_thumbnail': bool(p.get('thumbnail_url') or p.get('media_url')),
                    }
                    out['mediaEndpointUsed'] = '/me/media'
            else:
                out['errors']['me_media'] = {'status': r.status_code, 'body': r.text[:400]}
        except Exception as e:
            out['errors']['me_media'] = {'exception': str(e)}

        # 3) /{ig_user_id}/media using the DB id
        if db_ig_id:
            try:
                r = await c.get(
                    f'https://graph.instagram.com/{db_ig_id}/media',
                    params={
                        'access_token': token,
                        'fields': 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp',
                        'limit': 5,
                    },
                )
                if r.status_code == 200:
                    items = (r.json() or {}).get('data') or []
                    out['igUserMediaCount'] = len(items)
                    if items and not out['firstMediaPreview']:
                        p = items[0]
                        out['firstMediaPreview'] = {
                            'id': p.get('id'),
                            'media_type': p.get('media_type'),
                            'permalink': p.get('permalink'),
                            'timestamp': p.get('timestamp'),
                            'caption': (p.get('caption') or '')[:120],
                            'has_thumbnail': bool(p.get('thumbnail_url') or p.get('media_url')),
                        }
                        out['mediaEndpointUsed'] = f'/{{ig_user_id}}/media'
                else:
                    out['errors']['ig_user_media'] = {'status': r.status_code, 'body': r.text[:400]}
            except Exception as e:
                out['errors']['ig_user_media'] = {'exception': str(e)}

    if out['graphMeId'] and db_ig_id:
        out['idMatch'] = (out['graphMeId'] == db_ig_id)

    me_n = out['meMediaCount']
    ig_n = out['igUserMediaCount']
    out['mediaCount'] = max(me_n or 0, ig_n or 0)

    # Blocker classification
    if out['tokenValid'] is False:
        out['blocker'] = 'token_invalid_or_expired'
    elif out['idMatch'] is False:
        out['blocker'] = 'db_ig_user_id_mismatch_with_graph_me'
    elif (me_n is None) and (ig_n is None):
        out['blocker'] = 'graph_media_call_failed'
    elif (me_n == 0) and (ig_n in (0, None)):
        out['blocker'] = 'graph_returned_zero_media_check_account_type_or_posts_visibility'
    else:
        out['blocker'] = None

    return out


@api.get('/instagram/identity-matrix')
async def instagram_identity_matrix(user_id: str = Depends(get_current_active_user_id)):
    """Full token + identity + media endpoint matrix.

    Always returns 200 with a structured JSON envelope so the frontend
    can render the failure stage instead of a bare toast. Tokens are
    never echoed; only the env source names are reported.
    """
    logger.info('identity_matrix_started user=%s', user_id)
    partial: Dict[str, Any] = {
        'connected': False,
        'tokenExists': False,
        'dbIgUserId': None,
        'authKind': None,
        'igAppIdSource': INSTAGRAM_APP_ID_SOURCE,
    }

    def _fail(stage: str, exc: Optional[BaseException] = None,
              status: int = 500, message: Optional[str] = None) -> Dict[str, Any]:
        logger.exception('identity_matrix_failed stage=%s', stage)
        return {
            'ok': False,
            'stage': stage,
            'error': {
                'type': type(exc).__name__ if exc else 'Error',
                'message': message or (str(exc) if exc else 'unknown'),
                'safeDetail': (str(exc)[:300] if exc else None),
                'status': status,
            },
            'partial': partial,
        }

    try:
        u = await db.users.find_one({'id': user_id})
    except Exception as e:
        return _fail('db_lookup', e)
    if not u:
        return _fail('user_lookup', None, 404, 'user not found')
    logger.info('identity_matrix_user_loaded user=%s', user_id)

    token = u.get('meta_access_token', '') or ''
    db_ig_id = str(u.get('ig_user_id') or '')
    partial.update({
        'connected': bool(u.get('instagramConnected')),
        'tokenExists': bool(token),
        'dbIgUserId': db_ig_id or None,
        'authKind': u.get('ig_auth_kind'),
    })
    if not token:
        return _fail('token_missing', None, 400, 'No stored access token')
    logger.info('identity_matrix_token_present user=%s len=%d', user_id, len(token))

    fields_me = 'id,user_id,username,account_type'
    fields_media = (
        'id,caption,media_type,media_url,thumbnail_url,'
        'permalink,timestamp,comments_count'
    )

    async def _probe(c: httpx.AsyncClient, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            r = await c.get(url, params=params)
            entry: Dict[str, Any] = {'status': r.status_code}
            try:
                entry['body'] = r.json()
            except Exception:
                entry['body'] = r.text[:600]
            return entry
        except Exception as e:
            logger.warning('identity_matrix_me_probe_failed url=%s err=%s', url, e)
            return {'status': 0, 'body': {'exception': str(e)}}

    me_unver: Dict[str, Any] = {}
    me_ver: Dict[str, Any] = {}
    me_fb: Dict[str, Any] = {}
    matrix: List[Dict[str, Any]] = []
    ig_me_id: Optional[str] = None
    ig_me_user_id: Optional[str] = None
    username: Optional[str] = None
    account_type: Optional[str] = None

    try:
      async with httpx.AsyncClient(timeout=20) as c:
        # ---- /me probes ----
        logger.info('identity_matrix_me_probe_started user=%s', user_id)
        me_unver = await _probe(c, 'https://graph.instagram.com/me',
                                {'access_token': token, 'fields': fields_me})
        me_ver = await _probe(c, 'https://graph.instagram.com/v21.0/me',
                              {'access_token': token, 'fields': fields_me})
        me_fb = await _probe(c, 'https://graph.facebook.com/v21.0/me',
                             {'access_token': token, 'fields': 'id,name'})

        # Pick whichever IG /me worked
        for src in (me_unver, me_ver):
            if src.get('status') == 200 and isinstance(src.get('body'), dict):
                b = src['body']
                ig_me_id = ig_me_id or (str(b.get('id')) if b.get('id') is not None else None)
                ig_me_user_id = ig_me_user_id or (str(b.get('user_id')) if b.get('user_id') is not None else None)
                username = username or b.get('username')
                account_type = account_type or b.get('account_type')

        canonical = ig_me_user_id or ig_me_id or None
        id_match = bool(canonical and db_ig_id and canonical == db_ig_id)
        mismatch_reason = None
        if not id_match:
            if not canonical:
                mismatch_reason = 'graph_me_failed_or_did_not_return_id'
            elif not db_ig_id:
                mismatch_reason = 'db_ig_user_id_empty'
            else:
                mismatch_reason = f'canonical_{canonical}_!=_db_{db_ig_id}'

        # ---- /media probes ----
        async def add_media_probe(label: str, url: str):
            r = await _probe(c, url, {'access_token': token, 'fields': fields_media, 'limit': 5})
            count = 0
            err_code = None
            err_msg = None
            works = False
            body = r.get('body')
            if r.get('status') == 200 and isinstance(body, dict):
                count = len(body.get('data') or [])
                works = True
            elif isinstance(body, dict) and isinstance(body.get('error'), dict):
                err_code = body['error'].get('code')
                err_msg = body['error'].get('message')
            matrix.append({
                'endpoint': label,
                'status': r.get('status'),
                'count': count,
                'errorCode': err_code,
                'errorMessage': err_msg,
                'works': works,
            })

        if canonical:
            logger.info('identity_matrix_media_probe_started user=%s', user_id)
            await add_media_probe('GET graph.instagram.com/me/media',
                                  'https://graph.instagram.com/me/media')
            await add_media_probe('GET graph.instagram.com/v21.0/me/media',
                                  'https://graph.instagram.com/v21.0/me/media')
            if ig_me_id:
                await add_media_probe(f'GET graph.instagram.com/{{ig_me_id}}/media',
                                      f'https://graph.instagram.com/{ig_me_id}/media')
            if ig_me_user_id and ig_me_user_id != ig_me_id:
                await add_media_probe(f'GET graph.instagram.com/{{ig_me_user_id}}/media',
                                      f'https://graph.instagram.com/{ig_me_user_id}/media')
        else:
            await db.users.update_one(
                {'id': user_id},
                {'$set': {
                    'instagramConnected': False,
                    'instagram_connection_valid': False,
                    'instagram_connection_blocker': 'token_cannot_call_graph_me',
                    'updated': datetime.utcnow(),
                }},
            )
    except Exception as e:
        # Probe loop crashed mid-way. Return whatever we collected with a
        # structured failure envelope so the wizard can render it.
        partial.update({
            'meUnverStatus': me_unver.get('status') if me_unver else None,
            'matrixSoFar': matrix,
        })
        return _fail('me_probe' if not matrix else 'media_matrix', e)

    working = [m for m in matrix if m['works']]
    chosen = working[0]['endpoint'] if working else None

    # Surface OAuth credentials info (already redacted helpers exist but we
    # only echo source names + booleans, never values).
    cred_info = {
        'instagramAppIdSource': INSTAGRAM_APP_ID_SOURCE,
        'instagramAppSecretSource': INSTAGRAM_APP_SECRET_SOURCE,
        'instagramAppIdConfigured': bool(INSTAGRAM_APP_ID),
        'instagramAppSecretConfigured': bool(INSTAGRAM_APP_SECRET),
    }
    blocker = 'token_cannot_call_graph_me' if not canonical else None
    reconnect_recommended = bool(blocker or (canonical and db_ig_id and canonical != db_ig_id))
    debug_token = await _debug_token_with_ig_app(token)

    logger.info('identity_matrix_success user=%s chosen=%s', user_id, chosen)
    return {
        'ok': True,
        'blocker': blocker,
        'tokenLength': len(token),
        'authKind': u.get('ig_auth_kind'),
        'credentials': cred_info,
        'debugToken': debug_token,
        'oauthLastAudit': _redact_secrets(u.get('ig_oauth_last_audit')) if u.get('ig_oauth_last_audit') else None,
        'meProbes': {
            'graphInstagramMeUnversioned': me_unver,
            'graphInstagramMeVersioned': me_ver,
            'graphFacebookMeVersioned': me_fb,
        },
        'identity': {
            'dbIgUserId': db_ig_id or None,
            'instagramMeId': ig_me_id,
            'instagramMeUserId': ig_me_user_id,
            'bestCanonicalIgId': canonical,
            'username': username,
            'accountType': account_type,
            'idMatch': id_match,
            'mismatchReason': mismatch_reason,
            'sourceField': 'user_id' if ig_me_user_id else ('id' if ig_me_id else None),
        },
        'mediaMatrix': matrix,
        'chosenEndpoint': chosen,
        'reconnectRecommended': reconnect_recommended,
    }


async def _fetch_latest_media_id(access_token: str, ig_user_id: str) -> Optional[str]:
    if not access_token or not ig_user_id:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f'https://graph.instagram.com/{ig_user_id}/media',
                params={'access_token': access_token, 'fields': 'id,timestamp', 'limit': 1},
            )
            if r.status_code != 200:
                return None
            data = r.json().get('data') or []
            return data[0]['id'] if data else None
    except Exception:
        return None


async def _fetch_recent_media_ids(access_token: str, ig_user_id: str, limit: int = 10) -> list:
    if not access_token or not ig_user_id:
        return []
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f'https://graph.instagram.com/{ig_user_id}/media',
                params={
                    'access_token': access_token,
                    'fields': 'id,timestamp',
                    'limit': max(1, min(int(limit or 10), 25)),
                },
            )
            if r.status_code != 200:
                return []
            return [
                str(item.get('id'))
                for item in ((r.json() or {}).get('data') or [])
                if item.get('id')
            ]
    except Exception:
        return []


@api.post('/instagram/disconnect')
async def instagram_disconnect(user_id: str = Depends(get_current_active_user_id)):
    """Disconnect EVERY Instagram account row owned by this user.

    Phase 2.18K: the previous implementation only disconnected the
    single row matching users.ig_user_id, which left any other linked
    or orphan row for the same user with connectionValid=True. That
    row kept counting toward the plan's max_instagram_accounts cap and
    silently blocked the user from reconnecting ('Plan free allows
    1 Instagram account...'). Now we deactivate ALL of the user's
    instagram_accounts rows in one update_many so the slate is truly
    clean and the snapshot count drops to zero.
    """
    now = datetime.utcnow()
    u = await db.users.find_one({'id': user_id})
    await db.users.update_one(
        {'id': user_id},
        {
            '$set': {
                'instagramConnected': False,
                'instagram_connection_valid': False,
                'instagram_connection_blocker': 'disconnected_by_user',
                'instagramHandle': None,
                'active_instagram_account_id': None,
            },
            '$unset': {
                'meta_access_token': '',
                'ig_user_id': '',
                'instagram_account_type': '',
                'instagram_graph_me_id': '',
                'instagram_graph_me_user_id': '',
                'instagram_profile_picture_url': '',
            },
        }
    )
    # Nuke EVERY instagram_accounts row owned by this user (both
    # userId and user_id field variants) so no phantom row keeps
    # counting toward the plan cap.
    result = await db.instagram_accounts.update_many(
        {'$or': [{'userId': user_id}, {'user_id': user_id}]},
        {'$set': {
            'isActive': False,
            'isCurrent': False,
            'connectionValid': False,
            'refreshStatus': 'disconnected',
            'refreshLockedUntil': None,
            'updatedAt': now,
        }},
    )
    # Drop the dashboard snapshot so the plan-limit guard re-reads
    # the new (zero) connected count on the very next request.
    try:
        await invalidate_dashboard_summary(user_id)
    except Exception:
        pass
    logger.info(
        'instagram_disconnect_all user_id=%s rows_disconnected=%s',
        user_id, result.modified_count if hasattr(result, 'modified_count') else 0,
    )
    return {
        'ok': True,
        'rows_disconnected': getattr(result, 'modified_count', 0),
    }


@api.get('/instagram/webhook')
async def instagram_webhook_verify(request: Request):
    params = dict(request.query_params)
    mode = params.get('hub.mode')
    token = params.get('hub.verify_token')
    challenge = params.get('hub.challenge', '')
    if mode == 'subscribe' and token == META_VERIFY_TOKEN:
        return PlainTextResponse(challenge)
    raise HTTPException(403, 'Verification failed')


def _verify_webhook_signature(request_body: bytes, signature_header: str) -> dict:
    """Verify the X-Hub-Signature-256 header from Meta.
    Returns {valid, reason, signature_present, computed_prefix, received_prefix}.
    """
    out = {
        'valid': False,
        'reason': None,
        'signature_present': bool(signature_header),
        'secret_configured': bool(META_WEBHOOK_APP_SECRET),
        'computed_prefix': None,
        'received_prefix': None,
    }
    if not META_WEBHOOK_APP_SECRET:
        out['reason'] = 'no_secret_configured'
        return out
    if not signature_header:
        out['reason'] = 'no_signature_header'
        return out
    # Header format: "sha256=<hex>"
    if not signature_header.startswith('sha256='):
        out['reason'] = 'bad_signature_format'
        out['received_prefix'] = signature_header[:20]
        return out
    received_sig = signature_header[7:]  # strip "sha256="
    computed_sig = hmac.new(
        META_WEBHOOK_APP_SECRET.encode('utf-8'),
        request_body,
        hashlib.sha256,
    ).hexdigest()
    out['computed_prefix'] = computed_sig[:8]
    out['received_prefix'] = received_sig[:8]
    if hmac.compare_digest(computed_sig, received_sig):
        out['valid'] = True
        out['reason'] = 'signature_valid'
    else:
        out['reason'] = 'signature_mismatch'
    return out


async def _write_webhook_log_async(payload: dict, sig_result: dict):
    """Persist only safe webhook metadata off the ACK path. Bounded retention."""
    try:
        entries = payload.get('entry') or []
        entry_summaries = []
        if isinstance(entries, list):
            for entry in entries[:20]:
                if not isinstance(entry, dict):
                    continue
                messaging = entry.get('messaging') if isinstance(entry.get('messaging'), list) else []
                changes = entry.get('changes') if isinstance(entry.get('changes'), list) else []
                entry_summaries.append({
                    'entry_id_hash': _safe_text_hash(entry.get('id'))[:12] if entry.get('id') else None,
                    'time_exists': bool(entry.get('time')),
                    'messaging_count': len(messaging),
                    'changes_count': len(changes),
                })
        await db.webhook_log.insert_one({
            'received': datetime.utcnow(),
            'object': payload.get('object'),
            'entry_count': len(entries) if isinstance(entries, list) else 0,
            'entry_summaries': entry_summaries,
            'signature_valid': sig_result.get('valid'),
            'signature_reason': sig_result.get('reason'),
            'enforce_mode': bool(META_WEBHOOK_HMAC_ENFORCE),
        })
        count = await db.webhook_log.count_documents({})
        if count > 50:
            oldest = await db.webhook_log.find().sort('received', 1).limit(count - 50).to_list(count)
            for o in oldest:
                await db.webhook_log.delete_one({'_id': o['_id']})
    except Exception:
        logger.exception('webhook_log write failed')


@api.post('/instagram/webhook')
async def instagram_webhook(request: Request):
    import time as _time
    ack_start = _time.monotonic()
    raw_body = await request.body()
    sig_header = request.headers.get('x-hub-signature-256') or ''
    sig_result = _verify_webhook_signature(raw_body, sig_header)
    if not sig_result['valid']:
        logger.warning('webhook_signature_check reason=%s received_prefix=%s '
                       'computed_prefix=%s enforce=%s',
                       sig_result['reason'],
                       sig_result.get('received_prefix'),
                       sig_result.get('computed_prefix'),
                       META_WEBHOOK_HMAC_ENFORCE)
        if META_WEBHOOK_HMAC_ENFORCE:
            # Reject bad/missing signatures regardless of whether a secret
            # is configured. If HMAC enforcement is on, we never want to
            # accept an unsigned webhook — that would defeat the point.
            return JSONResponse(
                status_code=403,
                content={'error': 'invalid_signature',
                         'reason': sig_result['reason']},
            )
        # Warn-only mode: log a one-line marker so ops can grep for it.
        if not sig_result['secret_configured']:
            logger.warning('webhook_hmac_not_enforced reason=secret_not_configured')
        else:
            logger.warning('webhook_hmac_not_enforced reason=%s', sig_result['reason'])
    else:
        logger.info('webhook_signature_valid prefix=%s', sig_result['computed_prefix'])
    import json as _json
    try:
        payload = _json.loads(raw_body)
        if not isinstance(payload, dict):
            raise ValueError('payload_not_object')
    except (_json.JSONDecodeError, ValueError, TypeError):
        # Bad JSON: respond 400 immediately and DO NOT spawn any
        # background processing task — the body is untrusted/garbage.
        logger.warning(
            'webhook_invalid_json size=%s sig_valid=%s enforce=%s',
            len(raw_body or b''), sig_result.get('valid'), META_WEBHOOK_HMAC_ENFORCE,
        )
        return JSONResponse(
            status_code=400,
            content={'error': 'invalid_json'},
        )
    logger.info('ig_webhook_payload_received object=%s entries=%s',
                payload.get('object'), len(payload.get('entry') or []))
    # ACK fast — heavy work moves to background tasks. Fire-and-forget keeps
    # the webhook ACK well under the 5-second Meta retry threshold.
    global WEBHOOK_LAST_RECEIVED_AT
    WEBHOOK_LAST_RECEIVED_AT = datetime.utcnow()
    logger.info('webhook_received')
    create_tracked_task(_write_webhook_log_async(payload, sig_result), 'webhook_log')
    create_tracked_task(_supervised_process_webhook(payload), 'webhook_processor')
    ack_ms = int((_time.monotonic() - ack_start) * 1000)
    logger.info('webhook_ack_duration_ms=%s', ack_ms)
    return {'ok': True}


async def _supervised_process_webhook(payload: dict):
    """Wrap _process_webhook so a fire-and-forget task can never silently
    swallow exceptions. Stamps WEBHOOK_LAST_PROCESSED_AT for the health
    endpoint and logs webhook_processor_failed on error."""
    global WEBHOOK_LAST_PROCESSED_AT
    try:
        logger.info('webhook_processor_started')
        await _process_webhook(payload)
        WEBHOOK_LAST_PROCESSED_AT = datetime.utcnow()
    except Exception:
        logger.exception('webhook_processor_failed')


def _webhook_log_safe_summary(doc: dict) -> dict:
    """Return a redacted summary of a stored webhook log entry.

    Strips raw payload, full text fields, and any potential secrets.
    Only metadata that is safe to surface to support staff is returned.
    """
    legacy_payload = doc.get('payload') if isinstance(doc.get('payload'), dict) else {}
    legacy_entries = legacy_payload.get('entry') if isinstance(legacy_payload.get('entry'), list) else []
    return {
        'id': str(doc.get('_id') or ''),
        'received': doc.get('received').isoformat() if isinstance(doc.get('received'), datetime) else doc.get('received'),
        'object': doc.get('object') or legacy_payload.get('object'),
        'entry_count': int(doc.get('entry_count') if doc.get('entry_count') is not None else len(legacy_entries)),
        'signature_valid': doc.get('signature_valid'),
        'signature_reason': doc.get('signature_reason'),
        'enforce_mode': doc.get('enforce_mode'),
    }


@api.get('/instagram/webhook-log')
async def instagram_webhook_log(
    request: Request,
    limit: int = 20,
    user_id: str = Depends(get_current_active_user_id),
):
    """Return a SAFE summary of recent webhook deliveries for ops debugging.

    Admin-only. Requires JWT via Authorization header — the legacy ?token=
    query parameter is no longer accepted because it leaks credentials in
    server logs and browser history. Raw payloads, full comment/DM text,
    and any token-like fields are NEVER returned.
    """
    await _require_admin_permission(user_id, _admin_roles.PERM_FAILURES_VIEW)
    docs = await db.webhook_log.find().sort('received', -1).limit(max(1, min(limit, 50))).to_list(50)
    items = [_webhook_log_safe_summary(d) for d in docs]
    return {'items': items, 'count': await db.webhook_log.count_documents({})}


async def _handle_new_comment(user_doc: dict, comment_data: dict, source: str = 'webhook'):
    """Process one incoming Instagram comment (shared by webhook + polling).

    Returns a dict with keys:
      processed (bool) — True if a new comment doc was inserted
      matched (bool)   — True if any automation rule fired
      action_status    — 'pending'|'success'|'failed'|'skipped'
      rule_id          — id of the matched automation, when matched
      already_processed (bool) — True if dedup hit
    """
    if IS_SHUTTING_DOWN:
        return {'processed': False, 'matched': False, 'action_status': 'skipped',
                'reason': 'shutting_down'}
    import time as _time
    import uuid as _uuid
    processing_started = _time.monotonic()
    user_id = user_doc['id']
    ig_account_id = user_doc.get('ig_user_id') or ''
    ig_comment_id = comment_data.get('ig_comment_id')
    commenter_id = comment_data.get('commenter_id')
    media_id = comment_data.get('media_id')
    comment_text = _normalize_comment_text(comment_data.get('text'))
    force_queue = bool(comment_data.get('force_queue'))
    force_queue_reason = comment_data.get('queue_reason') or 'queued_rate_limit'

    # log: comment_seen (every comment we observe, before dedup)
    # Never log raw comment text — Railway log retention may keep it for
    # weeks. Log only length and a stable short fingerprint hash.
    logger.info('comment_seen ig_comment_id=%s media=%s source=%s text_length=%s text_hash=%s',
                ig_comment_id, media_id, source, len(comment_text or ''), _hash_text(comment_text or ''))
    if source == 'webhook':
        logger.info('webhook_comment_received comment_id=%s media_id=%s user_id=%s instagramAccountId=%s',
                    ig_comment_id, media_id, user_id, ig_account_id)
    elif source in ('polling', 'manual_catchup'):
        logger.info('poller_comment_seen comment_id=%s media_id=%s user_id=%s instagramAccountId=%s source=%s',
                    ig_comment_id, media_id, user_id, ig_account_id, source)
    logger.info('comment_text_classified comment_id=%s source=%s text_length=%s non_empty=%s',
                ig_comment_id, source, len(comment_text or ''), bool(comment_text))

    if not ig_comment_id or not commenter_id:
        return {'processed': False, 'matched': False, 'action_status': 'skipped',
                'reason': 'missing_id'}
    if commenter_id == ig_account_id:
        logger.info('comment_skipped_bot_own_reply ig_comment_id=%s user=%s source=%s',
                    ig_comment_id, user_doc.get('email'), source)
        return {'processed': False, 'matched': False, 'action_status': 'skipped',
                'reason': 'bot_own_reply'}

    ts_raw = comment_data.get('timestamp')
    comment_ts = _parse_graph_datetime(ts_raw)
    now = datetime.utcnow()

    # For webhook events: if the payload carries no comment timestamp, use
    # entry.time (the Meta dispatch time) as the effective timestamp so the
    # activation-cutoff check still works. Webhook events are by definition
    # real-time, so this is safe — we never risk processing a historical
    # pre-rule comment via this path.
    effective_ts = comment_ts
    if effective_ts is None and source == 'webhook':
        entry_time_iso = comment_data.get('entry_time')
        effective_ts = _parse_graph_datetime(entry_time_iso) if entry_time_iso else now
        logger.info(
            'webhook_comment_missing_timestamp_using_entry_time '
            'ig_comment_id=%s entry_time=%s effective_ts=%s',
            ig_comment_id, entry_time_iso, effective_ts.isoformat())
    if source == 'webhook':
        timestamp_source = 'payload' if comment_ts else ('entry_time' if comment_data.get('entry_time') else 'now')
        logger.info(
            'webhook_comment_effective_timestamp ig_comment_id=%s source=webhook timestamp_source=%s ts=%s',
            ig_comment_id,
            timestamp_source,
            effective_ts.isoformat() if effective_ts else None,
        )
    retry_existing = False
    retry_reason = None
    existing_doc_id = None
    existing_created = None

    # Dedupe
    existing = await db.comments.find_one({
        'user_id': user_id, 'ig_comment_id': ig_comment_id,
        '$or': [
            {'instagramAccountId': ig_account_id},
            {'igUserId': ig_account_id},
        ],
    })
    if existing:
        previous_skip = existing.get('skip_reason') or existing.get('skipReason')
        previous_status = str(existing.get('action_status') or existing.get('actionStatus') or '').lower()
        previous_reply_status = (
            existing.get('reply_status')
            or existing.get('replyStatus')
            or ('success' if existing.get('replied') else None)
        )
        previous_dm_status = existing.get('dm_status') or existing.get('dmStatus')
        already_replied = _reply_provider_proof_exists(existing)
        legacy_reply_success_without_proof = _reply_marked_success_without_provider_proof(existing)
        dm_succeeded_or_disabled = _status_is_success(previous_dm_status) or _status_is_disabled(previous_dm_status)
        previous_dm_failure_reason = existing.get('dm_failure_reason')
        dm_failed = str(previous_dm_status or '').lower() == 'failed'
        dm_retryable = dm_failed and _dm_failure_retryable_from_doc(existing)
        retryable_skip = previous_skip in (
            'missing_comment_timestamp',
            'no_rule_match',
            'skipped_empty_comment',
        )
        selected_post_catchup_retry = (
            source == 'manual_catchup'
            and previous_skip == 'historical_before_rule_activation'
        )
        # A failed action is retryable EXCEPT when the only failure was a
        # permanent DM error and the public reply already succeeded — in
        # that case the comment is in partial_success and should not
        # retry the reply (it would duplicate) nor retry the DM (it's
        # permanently undeliverable).
        partial_success_with_permanent_dm_failure = (
            previous_reply_status == 'success'
            and previous_dm_status == 'failed'
            and previous_dm_failure_reason in PERMANENT_GRAPH_FAILURE_REASONS
        )
        # Transient DM failure on an otherwise-successful reply is NOT
        # retryable from the public-reply path either — duplicating the
        # public reply is worse than failing to retry the DM. Only the
        # /dm-retry path (out of scope here) should re-send the DM.
        partial_success_with_transient_dm_failure = (
            previous_reply_status == 'success'
            and previous_dm_status == 'failed'
            and previous_dm_failure_reason in TRANSIENT_GRAPH_FAILURE_REASONS
        )
        retryable_status = (
            (previous_status in ('failed', 'failed_retryable') and not already_replied)
            or (previous_status == 'partial_success' and _has_retryable_step_failure(existing))
            or (previous_status == 'skipped' and retryable_skip)
            or selected_post_catchup_retry
            or (already_replied and dm_retryable)
            or legacy_reply_success_without_proof
        )
        if previous_status in ('pending', 'processing'):
            logger.info('comment_already_pending_queue comment_id=%s user=%s next_retry_at=%s',
                        ig_comment_id, user_doc.get('email'), existing.get('next_retry_at'))
            return {'processed': False, 'already_processed': True, 'matched': True,
                    'action_status': previous_status, 'queued': True,
                    'reason': 'comment_already_pending_queue'}
        # Treat fully-successful comments (reply + DM BOTH succeeded) as
        # already processed for dedup purposes. Provider proof is the
        # canonical signal, but legacy docs (no proof) where BOTH steps
        # succeeded are safely "done" — retrying would duplicate the
        # public reply. We deliberately do NOT include dm_status='disabled'
        # here — those docs go through the legacy provider-proof repair
        # path so they end up with a real reply_provider_response_ok flag.
        legacy_full_success = (
            str(previous_reply_status or '').lower() == 'success'
            and _status_is_success(previous_dm_status)
        )
        if (
            (already_replied and (previous_status in ('success', 'replied') or dm_succeeded_or_disabled))
            or (legacy_full_success and previous_status in ('success', 'replied'))
        ):
            logger.info(
                'comment_already_replied_success ig_comment_id=%s user=%s '
                'reply_status=%s action_status=%s replied_at=%s '
                'reply_provider_response_ok=%s reply_provider_comment_id_exists=%s '
                'reply_success_source=%s',
                ig_comment_id,
                user_doc.get('email'),
                previous_reply_status,
                previous_status,
                existing.get('replied_at') or existing.get('replySentAt'),
                bool(existing.get('reply_provider_response_ok') is True),
                _reply_provider_comment_id_exists(existing),
                existing.get('reply_success_source') or existing.get('source') or 'legacy_migration',
            )
            return {'processed': False, 'already_processed': True, 'matched': False,
                    'action_status': 'skipped',
                    'reason': 'already_replied_success',
                    'classified_reason': 'comment_already_replied_success'}
        # Partial-success guard: if the prior run recorded a successful
        # public reply AND the DM failed PERMANENTLY, we must NOT retry —
        # the public reply would duplicate and the DM would just fail again.
        # Transient DM failures fall through to the retry path: execute_flow
        # itself dedups the reply step via _reply_provider_proof_exists.
        if partial_success_with_permanent_dm_failure:
            logger.info(
                'comment_already_partial_success ig_comment_id=%s user=%s dm_reason=%s',
                ig_comment_id, user_doc.get('email'),
                existing.get('dm_failure_reason'),
            )
            return {'processed': False, 'already_processed': True, 'matched': False,
                    'action_status': 'partial_success',
                    'reason': 'comment_already_partial_success',
                    'classified_reason': 'comment_already_partial_success'}
        if legacy_reply_success_without_proof:
            retry_existing = True
            retry_reason = 'legacy_success_without_provider_confirmation'
            existing_doc_id = existing.get('id')
            existing_created = existing.get('created')
            logger.warning(
                'comment_retryable_failed_before ig_comment_id=%s user=%s source=%s reason=%s '
                'reply_status=%s action_status=%s replied_at=%s reply_provider_response_ok=%s',
                ig_comment_id, user_doc.get('email'), source, retry_reason,
                previous_reply_status, previous_status,
                existing.get('replied_at') or existing.get('replySentAt'),
                bool(existing.get('reply_provider_response_ok') is True),
            )
        if already_replied and dm_failed and not dm_retryable:
            logger.info('comment_already_partial_success ig_comment_id=%s user=%s dm_reason=%s',
                        ig_comment_id, user_doc.get('email'), existing.get('dm_failure_reason'))
            return {'processed': False, 'already_processed': True, 'matched': False,
                    'action_status': 'partial_success', 'reason': 'comment_already_partial_success'}
        if dm_failed and not dm_retryable:
            logger.info('comment_already_dm_failed ig_comment_id=%s user=%s dm_reason=%s',
                        ig_comment_id, user_doc.get('email'), existing.get('dm_failure_reason'))
            return {'processed': False, 'already_processed': True, 'matched': False,
                    'action_status': previous_status or 'failed', 'reason': 'comment_already_dm_failed'}
        if retry_existing:
            pass
        elif (
            (previous_skip == 'missing_comment_timestamp' and effective_ts)
            or retryable_status
            or retryable_skip
        ):
            retry_existing = True
            retry_reason = previous_skip or previous_status or 'unreplied_existing'
            existing_doc_id = existing.get('id')
            existing_created = existing.get('created')
            logger.info(
                'comment_retryable_failed_before ig_comment_id=%s user=%s source=%s reason=%s '
                'previous_status=%s reply_status=%s dm_status=%s',
                ig_comment_id, user_doc.get('email'), source, retry_reason,
                previous_status, previous_reply_status, previous_dm_status,
            )
        else:
            # Recovery for the production bug: a prior run set
            # dm_status=success but reply_status=disabled even though the
            # matched rule has public reply configured. The legacy
            # _compute_comment_action_status returned 'success' for that
            # combo, masking the missing public reply. Detect it here
            # and re-queue for public reply only.
            existing_rule_id = existing.get('rule_id') or existing.get('ruleId')
            existing_rule = None
            if existing_rule_id and (
                str(previous_reply_status or '').lower() in ('disabled', '', 'skipped')
                and _status_is_success(previous_dm_status)
                and not _reply_provider_proof_exists(existing)
            ):
                try:
                    existing_rule = await db.automations.find_one({'id': existing_rule_id})
                except Exception:
                    existing_rule = None
                if existing_rule and _automation_public_reply_required(existing_rule):
                    now_recover = datetime.utcnow()
                    await db.comments.update_one(
                        {'id': existing.get('id')},
                        {'$set': {
                            'reply_status': 'failed_retryable',
                            'replyStatus': 'failed_retryable',
                            'reply_failure_reason': 'public_reply_required_not_attempted',
                            'reply_failure_retryable': True,
                            'reply_skip_reason': 'public_reply_required_not_attempted',
                            'action_status': 'failed_retryable',
                            'actionStatus': 'failed_retryable',
                            'queued': True,
                            'next_retry_at': now_recover,
                            'updated': now_recover,
                        }},
                    )
                    logger.warning(
                        'public_reply_required_recovery ig_comment_id=%s user=%s rule_id=%s '
                        'previous_status=%s reply_status=%s dm_status=%s',
                        ig_comment_id, user_doc.get('email'), existing_rule_id,
                        previous_status, previous_reply_status, previous_dm_status,
                    )
                    return {'processed': True, 'recovered': True, 'matched': True,
                            'action_status': 'failed_retryable',
                            'queued': True,
                            'reason': 'public_reply_required_recovery',
                            'classified_reason': 'public_reply_required_not_attempted',
                            'rule_id': existing_rule_id,
                            'comment_doc_id': existing.get('id')}

            # Classify the exact reason this comment is being skipped now
            # — the legacy comment_already_processed line was vague and
            # masked partial_success / DM-failed states.
            exact_reason = 'comment_processed_unknown_state'
            return_reason = 'unknown_state'
            if previous_skip == 'historical_before_rule_activation':
                exact_reason = 'comment_skipped_historical'
                return_reason = 'historical'
            elif (
                partial_success_with_permanent_dm_failure
                or partial_success_with_transient_dm_failure
            ):
                exact_reason = 'comment_already_partial_success'
                return_reason = 'already_partial_success'
            elif (
                previous_reply_status == 'success'
                and previous_dm_status in ('success', 'disabled', '')
            ):
                exact_reason = 'comment_already_replied_success'
                return_reason = 'already_replied_success'
            elif (
                previous_reply_status in ('disabled', '')
                and previous_dm_status == 'failed'
            ):
                exact_reason = 'comment_already_dm_failed'
                return_reason = 'already_dm_failed'
            elif already_replied:
                # replied=True / action_status=success but no reply_status
                # field (legacy doc written before this refactor).
                exact_reason = 'comment_already_replied_success'
                return_reason = 'already_replied_success'
            logger.info(
                '%s ig_comment_id=%s user=%s source=%s previous_status=%s '
                'reply_status=%s dm_status=%s dm_failure_reason=%s',
                exact_reason, ig_comment_id, user_doc.get('email'), source,
                previous_status, previous_reply_status, previous_dm_status,
                previous_dm_failure_reason,
            )
            return {'processed': False, 'already_processed': True, 'matched': False,
                    'action_status': 'skipped', 'reason': return_reason,
                    'classified_reason': exact_reason}

    commenter_username = comment_data.get('commenter_username') or f'ig_{commenter_id[:8]}'

    # Match automations to determine rule_id BEFORE insert
    logger.info('comment_rule_matching_started comment_id=%s media_id=%s source=%s user_id=%s instagramAccountId=%s',
                ig_comment_id, media_id, source, user_id, ig_account_id)
    if source in ('polling', 'manual_catchup'):
        logger.info('poller_comment_rule_matching_started comment_id=%s media_id=%s source=%s',
                    ig_comment_id, media_id, source)
    automations = await db.automations.find(
        {**_account_scoped_query(user_id, ig_account_id), 'status': 'active'}
    ).to_list(100)
    automations = _sort_comment_rules_by_priority(automations, media_id)
    logger.info(
        'rule_priority_sorted comment_id=%s media_id=%s count=%s order=%s',
        ig_comment_id,
        media_id,
        len(automations),
        ','.join(
            f"{a.get('id')}:{_comment_rule_priority(a, media_id)}:{_comment_rule_scope(a, media_id)}"
            for a in automations[:20]
        ),
    )
    latest_media_id = None
    matched_rule = None
    matched_rule_priority = None
    matched_rule_scope = None
    broad_rules_skipped_due_specific_match = False
    cutoff_rule = None
    cutoff_skip_reason = None
    for auto in automations:
        rule_priority = _comment_rule_priority(auto, media_id)
        rule_scope = _comment_rule_scope(auto, media_id)
        logger.info(
            'rule_candidate_evaluated comment_id=%s media_id=%s rule_id=%s rule_scope=%s priority=%s',
            ig_comment_id, media_id, auto.get('id'), rule_scope, rule_priority
        )
        raw_trigger = auto.get('trigger') or ''
        trigger = raw_trigger.lower()
        fire = False

        def apply_activation_cutoff() -> Optional[str]:
            requested_historical = bool(
                auto.get('process_existing_unreplied_comments')
                or auto.get('processExistingComments')
                or auto.get('processExistingUnrepliedComments')
            )
            selected_historical_media_id = _selected_specific_media_id(auto)
            process_existing = (
                requested_historical
                and selected_historical_media_id
                and media_id
                and selected_historical_media_id == media_id
            )
            if requested_historical and not process_existing:
                logger.info(
                    'process_existing_unreplied_comments_ignored_reason=broad_scope '
                    'rule_id=%s comment=%s source=%s media_id=%s selected_media_id=%s',
                    auto.get('id'), ig_comment_id, source, media_id, selected_historical_media_id
                )
            activation = _parse_graph_datetime(
                auto.get('activationStartedAt') or auto.get('createdAt') or auto.get('created')
            ) or now
            logger.info('rule_activation_cutoff_applied rule_id=%s comment=%s activation=%s process_existing=%s',
                        auto.get('id'), ig_comment_id, activation, process_existing)
            # For polling: no timestamp means we cannot safely determine
            # whether the comment predates the rule — skip it. The poller
            # will retry once timestamp becomes available via Graph API.
            # For webhook: effective_ts is always set (entry.time or now),
            # so this branch is only reached for polling.
            if not effective_ts:
                logger.info('comment_skipped_missing_timestamp ig_comment_id=%s rule_id=%s source=%s',
                            ig_comment_id, auto.get('id'), source)
                return 'missing_comment_timestamp'
            # Instagram comment timestamps are second-precision, while our
            # activation time includes microseconds. Compare at second
            # precision so a fresh test comment in the same second as rule
            # creation is not treated as historical.
            activation_floor = activation.replace(microsecond=0) if activation else None
            comment_floor = effective_ts.replace(microsecond=0)
            if activation_floor and comment_floor < activation_floor:
                if process_existing:
                    logger.info(
                        'historical_selected_post_catchup_allowed ig_comment_id=%s rule_id=%s media_id=%s source=%s',
                        ig_comment_id, auto.get('id'), media_id, source
                    )
                    return None
                logger.info('comment_skipped_historical ig_comment_id=%s rule_id=%s comment_ts=%s activation=%s source=%s',
                            ig_comment_id, auto.get('id'), effective_ts, activation, source)
                logger.info('skipped_pre_rule_comment ig_comment_id=%s rule_id=%s source=%s',
                            ig_comment_id, auto.get('id'), source)
                return 'historical_before_rule_activation'
            logger.info('comment_processed_after_activation ig_comment_id=%s rule_id=%s comment_ts=%s activation=%s source=%s',
                        ig_comment_id, auto.get('id'), effective_ts, activation, source)
            if source in ('polling', 'manual_catchup'):
                logger.info('catchup_after_activation_only ig_comment_id=%s rule_id=%s source=%s',
                            ig_comment_id, auto.get('id'), source)
            return None

        if trigger.startswith('keyword:'):
            cutoff_skip_reason = apply_activation_cutoff()
            if cutoff_skip_reason:
                cutoff_rule = cutoff_rule or auto
                continue
            match_result = matchesAutomationRule(
                auto,
                comment_text,
                {'media_id': media_id, 'source': source},
            )
            fire = bool(match_result.get('matches'))
            if not fire:
                cutoff_skip_reason = match_result.get('reason') or 'no_rule_match'
        elif trigger.startswith('comment:'):
            target = raw_trigger.split(':', 1)[1].strip()
            media_hit = False
            if target.lower() == 'any':
                media_hit = bool(media_id)
            elif target.lower() == 'latest':
                if latest_media_id is None:
                    latest_media_id = await _fetch_latest_media_id(
                        user_doc.get('meta_access_token', ''),
                        user_doc.get('ig_user_id', ''),
                    ) or ''
                if media_id and latest_media_id and media_id == latest_media_id:
                    media_hit = True
            elif target and media_id and target == media_id:
                media_hit = True
            if media_hit:
                cutoff_skip_reason = apply_activation_cutoff()
                if cutoff_skip_reason:
                    cutoff_rule = cutoff_rule or auto
                    continue
                match_result = matchesAutomationRule(
                    auto,
                    comment_text,
                    {'media_id': media_id, 'source': source},
                )
                fire = bool(match_result.get('matches'))
                if not fire:
                    cutoff_skip_reason = match_result.get('reason') or 'no_rule_match'
        if fire:
            matched_rule = auto
            matched_rule_priority = rule_priority
            matched_rule_scope = rule_scope
            broad_rules_skipped_due_specific_match = bool(
                rule_priority < 3
                and any(_comment_rule_priority(candidate, media_id) == 3 for candidate in automations)
            )
            if rule_scope == 'specific_post_exact':
                logger.info(
                    'rule_specific_post_matched comment_id=%s media_id=%s selected_rule_id=%s rule_scope=%s priority=%s',
                    ig_comment_id, media_id, auto.get('id'), rule_scope, rule_priority
                )
            if broad_rules_skipped_due_specific_match:
                logger.info(
                    'rule_broad_skipped_due_specific_match comment_id=%s media_id=%s selected_rule_id=%s rule_scope=%s priority=%s',
                    ig_comment_id, media_id, auto.get('id'), rule_scope, rule_priority
                )
            logger.info(
                'rule_selected_for_comment comment_id=%s media_id=%s selected_rule_id=%s rule_scope=%s priority=%s',
                ig_comment_id, media_id, auto.get('id'), rule_scope, rule_priority
            )
            logger.info(
                'rule_evaluation_stopped_after_match comment_id=%s media_id=%s selected_rule_id=%s rule_scope=%s priority=%s',
                ig_comment_id, media_id, auto.get('id'), rule_scope, rule_priority
            )
            break

    rule_id = matched_rule.get('id') if matched_rule else (cutoff_rule.get('id') if cutoff_rule else None)
    matched = bool(matched_rule)
    if matched:
        logger.info('rule_matched source=%s ig_comment_id=%s rule_id=%s user=%s',
                    source, ig_comment_id, rule_id, user_doc.get('email'))
        logger.info('comment_rule_matched comment_id=%s media_id=%s source=%s rule_id=%s user_id=%s instagramAccountId=%s',
                    ig_comment_id, media_id, source, rule_id, user_id, ig_account_id)
        if source in ('polling', 'manual_catchup'):
            logger.info('poller_comment_rule_matched comment_id=%s media_id=%s rule_id=%s source=%s',
                        ig_comment_id, media_id, rule_id, source)
    elif cutoff_skip_reason:
        logger.info('rule_skipped_by_activation_cutoff ig_comment_id=%s rule_id=%s reason=%s user=%s',
                    ig_comment_id, rule_id, cutoff_skip_reason, user_doc.get('email'))
    else:
        cutoff_skip_reason = 'no_rule_match'
        logger.info('rule_not_matched ig_comment_id=%s user=%s',
                    ig_comment_id, user_doc.get('email'))

    rule_activation_started_at = (
        cutoff_rule.get('activationStartedAt') if cutoff_rule else
        (matched_rule.get('activationStartedAt') if matched_rule else None)
    )
    process_existing_comments = _historical_catchup_enabled_for_media(
        matched_rule or cutoff_rule or {},
        media_id,
    )
    action_status = 'pending' if matched else 'skipped'
    legacy_reply_repair = retry_reason == 'legacy_success_without_provider_confirmation'
    existing_reply_status = (
        None if legacy_reply_repair else (existing.get('reply_status') if retry_existing and existing else None)
    )
    existing_dm_status = existing.get('dm_status') if retry_existing and existing else None
    reply_status = existing_reply_status or (
        'pending' if matched and _automation_has_node_type(matched_rule, 'reply_comment') else 'disabled'
    )
    dm_status = existing_dm_status or (
        'pending' if matched and _automation_has_node_type(matched_rule, 'message') else 'disabled'
    )
    doc = {
        'id': existing_doc_id or str(_uuid.uuid4()),
        'user_id': user_id,
        'instagramAccountId': ig_account_id,
        'igUserId': ig_account_id,
        'instagramUsername': (user_doc.get('instagramHandle') or '').replace('@', ''),
        'ig_comment_id': ig_comment_id,
        'igCommentId': ig_comment_id,
        'media_id': media_id,
        'mediaId': media_id,
        'commenter_id': commenter_id,
        'commenter_username': commenter_username,
        'text': comment_text,
        'replied': False if legacy_reply_repair else (
            bool(existing.get('replied')) if retry_existing and existing else False
        ),
        'source': source,                # 'webhook' or 'polling'
        'rule_id': rule_id,
        'ruleId': rule_id,
        'matched_rule_id': rule_id,
        'matched_rule_priority': matched_rule_priority,
        'matched_rule_scope': matched_rule_scope,
        'broad_rules_skipped_due_specific_match': broad_rules_skipped_due_specific_match,
        'matched': matched,
        'action_status': action_status,
        'actionStatus': action_status,
        'reply_status': reply_status,
        'replyStatus': reply_status,
        'dm_status': dm_status,
        'dmStatus': dm_status,
        'reply_failure_reason': (
            'legacy_success_without_provider_confirmation'
            if legacy_reply_repair else (existing.get('reply_failure_reason') if retry_existing and existing else None)
        ),
        'dm_failure_reason': existing.get('dm_failure_reason') if retry_existing and existing else None,
        'reply_failure_retryable': True if legacy_reply_repair else (
            existing.get('reply_failure_retryable') if retry_existing and existing else False
        ),
        'dm_failure_retryable': existing.get('dm_failure_retryable') if retry_existing and existing else False,
        'replied_at': None if legacy_reply_repair else (
            existing.get('replied_at') if retry_existing and existing else None
        ),
        'dm_sent_at': existing.get('dm_sent_at') if retry_existing and existing else None,
        'last_attempt_at': now if matched else None,
        'skip_reason': cutoff_skip_reason,
        'skipReason': cutoff_skip_reason,
        'error': None,
        'timestamp': ts_raw,
        'commentTimestamp': effective_ts or comment_ts or ts_raw,
        'effective_timestamp': effective_ts,
        'timestamp_source': 'payload' if comment_ts else ('entry_time' if source == 'webhook' else 'none'),
        'ruleActivationStartedAt': rule_activation_started_at,
        'processExistingComments': process_existing_comments,
        'processed_at': now,
        'queued': bool(force_queue and matched),
        'next_retry_at': now if force_queue and matched else None,
        'attempts': int(existing.get('attempts') or 0) if retry_existing and existing else 0,
        'last_queue_attempt_at': existing.get('last_queue_attempt_at') if retry_existing and existing else None,
        'queue_lock_until': existing.get('queue_lock_until') if retry_existing and existing else None,
        'reprocessed_after_missing_timestamp': retry_reason == 'missing_comment_timestamp',
        'reprocessed_retryable': retry_existing,
        'reprocessed_reason': retry_reason,
        'updated': now,
        'created': existing_created or now,
    }
    if legacy_reply_repair:
        doc.update({
            'reply_provider_status': None,
            'reply_provider_response_ok': False,
            'reply_provider_comment_id': None,
            'reply_success_source': None,
            'skip_reason': 'legacy_success_without_provider_confirmation',
            'skipReason': 'legacy_success_without_provider_confirmation',
            'queued': True,
            'next_retry_at': now,
        })
    if retry_existing:
        await db.comments.update_one(
            {'id': doc['id'], 'user_id': user_id},
            {'$set': doc},
        )
    else:
        try:
            await db.comments.insert_one(doc)
        except Exception as e:
            # Race against unique index; another worker inserted first.
            logger.info('comment_insert_race ig_comment_id=%s err=%s', ig_comment_id, e)
            return {'processed': False, 'already_processed': True, 'matched': False,
                    'action_status': 'skipped', 'reason': 'race'}

    await ws_manager.send(user_id, {'type': 'comment', 'comment': _strip_mongo({**doc})})

    if matched:
        if force_queue:
            await db.comments.update_one(
                {'id': doc['id']},
                {'$set': {
                    'action_status': 'pending',
                    'actionStatus': 'pending',
                    'reply_status': reply_status,
                    'replyStatus': reply_status,
                    'dm_status': dm_status,
                    'dmStatus': dm_status,
                    'skip_reason': force_queue_reason,
                    'skipReason': force_queue_reason,
                    'queued': True,
                    'next_retry_at': now,
                    'updated': now,
                }},
            )
            logger.info('comment_processing_rate_limited_queued comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s reason=%s next_retry_at=%s',
                        ig_comment_id, media_id, user_id, ig_account_id, rule_id,
                        force_queue_reason, now)
            return {'processed': True, 'reprocessed': retry_existing,
                    'matched': matched, 'action_status': 'pending',
                    'rule_id': rule_id, 'comment_doc_id': doc['id'],
                    'queued': True, 'reason': force_queue_reason}
        try:
            logger.info('action_execution_started ig_comment_id=%s rule_id=%s',
                        ig_comment_id, rule_id)
            logger.info('comment_processing_immediate comment_id=%s media_id=%s source=%s rule_id=%s user_id=%s instagramAccountId=%s',
                        ig_comment_id, media_id, source, rule_id, user_id, ig_account_id)
            # Phase 2.2 plan enforcement: stop the action chain BEFORE
            # any Meta call when the monthly comment-processed limit is
            # exceeded. The comment doc stays so we don't lose it; the
            # queue does NOT auto-retry — the user must upgrade or wait
            # for next month's reset.
            comment_limit_check = await reserve_usage_limit(
                user_id, 'monthly_comments_processed_limit', increment=1,
                instagram_account_id=ig_account_id,
                source=source or 'runtime',
                automation_id=rule_id,
                ig_comment_id=ig_comment_id,
                action_id=doc['id'],
            )
            if not comment_limit_check.get('allowed') or (
                comment_limit_check.get('exceeded') and not comment_limit_check.get('fail_open')
            ):
                now_pl = datetime.utcnow()
                await db.comments.update_one(
                    {'id': doc['id']},
                    {'$set': {
                        'action_status': 'plan_limited',
                        'actionStatus': 'plan_limited',
                        'reply_status': 'plan_limited',
                        'replyStatus': 'plan_limited',
                        'dm_status': 'plan_limited',
                        'dmStatus': 'plan_limited',
                        'skip_reason': 'skipped_plan_limit',
                        'skipReason': 'skipped_plan_limit',
                        'reply_failure_reason': 'plan_limit_exceeded',
                        'reply_failure_retryable': False,
                        'next_retry_at': None,
                        'queued': False,
                        'plan_key_at_skip': comment_limit_check.get('plan_key'),
                        'updated': now_pl,
                    }},
                )
                logger.warning(
                    'comment_skipped_plan_limit ig_comment_id=%s user_id=%s rule_id=%s '
                    'plan_key=%s used=%s limit=%s',
                    ig_comment_id, user_id, rule_id,
                    comment_limit_check.get('plan_key'),
                    comment_limit_check.get('used'),
                    comment_limit_check.get('limit'),
                )
                return {'processed': True, 'matched': True,
                        'action_status': 'plan_limited',
                        'rule_id': rule_id, 'comment_doc_id': doc['id'],
                        'reason': 'skipped_plan_limit',
                        'plan_key': comment_limit_check.get('plan_key')}
            confirmed_comment_usage = await confirm_usage_reservation(
                comment_limit_check,
                user_id=user_id,
                event_type='comment_processed',
                instagram_account_id=ig_account_id,
                automation_id=rule_id,
                comment_id=doc['id'],
                metadata={
                    'source': source,
                    'media_id': media_id,
                    'matched_rule_priority': matched_rule_priority,
                    'matched_rule_scope': matched_rule_scope,
                },
            )
            if confirmed_comment_usage:
                await db.comments.update_one(
                    {'id': doc['id']},
                    {'$set': {
                        'usage_comment_processed_recorded': True,
                        'usage_comment_processed_recorded_at': datetime.utcnow(),
                    }},
                )
            ok = await _run_and_record_action(
                user_doc, matched_rule, commenter_id, comment_text,
                comment_doc_id=doc['id'], ig_comment_id=ig_comment_id,
                source=source, received_monotonic=processing_started,
            )
            if isinstance(ok, dict):
                action_status = ok.get('action_status') or ('success' if ok.get('ok') else 'failed')
            else:
                action_status = 'success' if ok else 'failed'
            log_name = 'comment_processing_success'
            if action_status == 'partial_success':
                log_name = 'comment_processing_partial_success'
            elif action_status == 'failed_retryable':
                log_name = 'comment_processing_temporary_error_queued'
            elif action_status in ('failed_permanent', 'failed_retry_exhausted'):
                log_name = 'comment_processing_failed_permanent'
            logger.info('%s comment_id=%s media_id=%s source=%s rule_id=%s action_status=%s',
                        log_name, ig_comment_id, media_id, source, rule_id, action_status)
        except Exception as e:
            logger.exception('action_execution_failed ig_comment_id=%s err=%s',
                             ig_comment_id, e)
            await db.comments.update_one(
                {'id': doc['id']},
                {'$set': {'action_status': 'failed', 'actionStatus': 'failed',
                          'error': str(e)[:500]}}
            )
            action_status = 'failed'

    return {'processed': True, 'reprocessed': retry_existing,
            'matched': matched, 'action_status': action_status,
            'rule_id': rule_id, 'comment_doc_id': doc['id']}


def _compute_action_status(flow_results: dict) -> str:
    """Combine per-step results into a single dashboard-facing action_status.

    Rules (in priority order):
      • reply ran and FAILED               -> 'failed'
      • reply succeeded, DM failed         -> 'partial_success'
      • reply succeeded (DM disabled / OK) -> 'success'
      • only DM ran and succeeded          -> 'success'
      • only DM ran and failed             -> 'failed'
      • neither ran                        -> 'skipped'
    """
    rs = flow_results.get('reply_status') or 'disabled'
    ds = flow_results.get('dm_status') or 'disabled'
    if rs == 'failed':
        return 'failed'
    if rs == 'success':
        if ds == 'failed':
            return 'partial_success'
        return 'success'
    # reply was disabled or skipped — outcome is determined by DM alone.
    if ds == 'success':
        return 'success'
    if ds == 'failed':
        return 'failed'
    return 'skipped'


async def _run_and_record_action(user_doc, automation, commenter_id, comment_text,
                                 comment_doc_id: str, ig_comment_id: str,
                                 source: str = 'webhook',
                                 received_monotonic: Optional[float] = None):
    """Wrap execute_flow so we record success/failure on the comment doc.

    Persists per-step outcomes (reply_status, dm_status, action_status,
    failure reasons, last_attempt_at) so dedup on a later poll can decide
    whether the comment is truly done or still retryable, and so the
    dashboard can show partial_success when only the DM step failed.
    """
    flow_results: dict = {}
    now = datetime.utcnow()
    try:
        ok = await execute_flow(
            user_doc, automation, commenter_id, comment_text,
            comment_context={
                'ig_comment_id': ig_comment_id,
                'comment_doc_id': comment_doc_id,
                'source': source,
                'received_monotonic': received_monotonic,
            },
            flow_results=flow_results,
        )
        saved = await db.comments.find_one({'id': comment_doc_id}) or {}
        reply_status = saved.get('reply_status') or saved.get('replyStatus') or 'disabled'
        dm_status = saved.get('dm_status') or saved.get('dmStatus') or 'disabled'
        # Hard invariant: if the matched rule is configured with a public
        # reply, an unattempted public reply is illegal. Convert
        # reply_status='disabled' -> 'failed_retryable' so the queue can
        # re-run the public reply step. We do NOT touch dm_status, so a
        # successful DM stays successful and is not resent.
        public_reply_required = bool(flow_results.get('public_reply_required')) or _automation_public_reply_required(automation)
        if public_reply_required and _status_is_disabled(reply_status):
            logger.warning(
                'public_reply_required_not_attempted ig_comment_id=%s rule_id=%s '
                'matched_rule_scope=%s dm_status=%s — converting reply_status disabled -> failed_retryable',
                ig_comment_id, automation.get('id'),
                saved.get('matched_rule_scope') or saved.get('matchedRuleScope'),
                dm_status,
            )
            reply_status = 'failed_retryable'
            now_invariant = datetime.utcnow()
            await db.comments.update_one(
                {'id': comment_doc_id},
                {'$set': {
                    'reply_status': 'failed_retryable',
                    'replyStatus': 'failed_retryable',
                    'reply_failure_reason': 'public_reply_required_not_attempted',
                    'reply_failure_retryable': True,
                    'reply_skip_reason': 'public_reply_required_not_attempted',
                    'next_retry_at': now_invariant,
                    'last_attempt_at': now_invariant,
                    'updated': now_invariant,
                }},
            )
            saved = await db.comments.find_one({'id': comment_doc_id}) or saved
            saved.update({
                'reply_status': 'failed_retryable',
                'reply_failure_retryable': True,
                'reply_skip_reason': 'public_reply_required_not_attempted',
            })
        action_status = _compute_comment_action_status(reply_status, dm_status)
        if action_status == 'failed' and ok and not public_reply_required:
            action_status = 'success'
        saved_for_status = {**saved, 'reply_status': reply_status, 'dm_status': dm_status}
        retryable_failure = _has_retryable_step_failure(saved_for_status)
        attempts = int(saved.get('attempts') or 0)
        permanent_failure = (
            action_status == 'failed'
            and not retryable_failure
        )
        retry_exhausted = retryable_failure and attempts >= AUTOMATION_QUEUE_MAX_ATTEMPTS
        if action_status == 'failed':
            action_status = 'failed_retry_exhausted' if retry_exhausted else (
                'failed_retryable' if retryable_failure else 'failed_permanent'
            )
        update = {
            'action_status': action_status,
            'actionStatus': action_status,
            'last_attempt_at': datetime.utcnow(),
            'updated': datetime.utcnow(),
        }
        if action_status in ('success', 'partial_success'):
            update['actionSentAt'] = datetime.utcnow()
            update['error'] = None
        else:
            update['error'] = 'automation_action_send_failed'
        if action_status == 'failed_retryable':
            update['next_retry_at'] = _next_retry_time(attempts)
            update['skip_reason'] = 'queued_retryable_failure'
            update['skipReason'] = 'queued_retryable_failure'
        elif action_status == 'partial_success' and retryable_failure:
            update['next_retry_at'] = _next_retry_time(attempts)
            update['skip_reason'] = 'queued_retryable_failure'
            update['skipReason'] = 'queued_retryable_failure'
            update['queued'] = True
        elif action_status in ('failed_permanent', 'failed_retry_exhausted'):
            update['next_retry_at'] = None
            update['skip_reason'] = _failure_category_from_doc(saved_for_status)
            update['skipReason'] = update['skip_reason']
        await db.comments.update_one(
            {'id': comment_doc_id},
            {'$set': update},
        )
        if action_status == 'failed_retryable':
            await _record_comment_usage_once(
                comment_doc_id,
                'usage_retryable_failure_recorded',
                user_id=user_doc.get('id', ''),
                event_type='retryable_failure',
                instagram_account_id=user_doc.get('ig_user_id') or '',
                automation_id=automation.get('id'),
                comment_id=comment_doc_id,
                metadata={
                    'source': source,
                    'failure_category': _failure_category_from_doc(saved_for_status),
                    'attempts': attempts,
                },
            )
        elif action_status in ('failed_permanent', 'failed_retry_exhausted'):
            await _record_comment_usage_once(
                comment_doc_id,
                'usage_permanent_failure_recorded',
                user_id=user_doc.get('id', ''),
                event_type='permanent_failure',
                instagram_account_id=user_doc.get('ig_user_id') or '',
                automation_id=automation.get('id'),
                comment_id=comment_doc_id,
                metadata={
                    'source': source,
                    'failure_category': _failure_category_from_doc(saved_for_status),
                    'attempts': attempts,
                    'status': action_status,
                },
            )
        if action_status in ('failed_retryable', 'failed_permanent', 'failed_retry_exhausted'):
            logger.warning('action_execution_send_failed ig_comment_id=%s rule_id=%s',
                           ig_comment_id, automation.get('id'))
            return {'ok': False, 'action_status': action_status}
        logger.info('action_execution_%s ig_comment_id=%s rule_id=%s reply_status=%s dm_status=%s',
                    action_status, ig_comment_id, automation.get('id'), reply_status, dm_status)
        return {'ok': action_status in ('success', 'partial_success'),
                'action_status': action_status}
    except Exception as e:
        next_retry = _next_retry_time(0)
        await db.comments.update_one(
            {'id': comment_doc_id},
            {'$set': {
                'action_status': 'failed_retryable',
                'actionStatus': 'failed_retryable',
                'error': str(e)[:300],
                'next_retry_at': next_retry,
                'skip_reason': 'temporary_graph_error',
                'skipReason': 'temporary_graph_error',
                'updated': datetime.utcnow(),
            }}
        )
        await _record_comment_usage_once(
            comment_doc_id,
            'usage_retryable_failure_recorded',
            user_id=user_doc.get('id', ''),
            event_type='retryable_failure',
            instagram_account_id=user_doc.get('ig_user_id') or '',
            automation_id=automation.get('id'),
            comment_id=comment_doc_id,
            metadata={'source': source, 'failure_category': 'action_execution_exception'},
        )
        logger.exception('action_execution_failed ig_comment_id=%s rule_id=%s err=%s',
                         ig_comment_id, automation.get('id'), e)
        return {'ok': False, 'action_status': 'failed_retryable'}

    action_status = _compute_action_status(flow_results)
    update_set = {
        'action_status': action_status,
        'actionStatus': action_status,
        'reply_status': flow_results.get('reply_status') or 'disabled',
        'dm_status': flow_results.get('dm_status') or 'disabled',
        'reply_failure_reason': flow_results.get('reply_failure_reason'),
        'dm_failure_reason': flow_results.get('dm_failure_reason'),
        'last_attempt_at': now,
        'updated': now,
    }
    if action_status in ('success', 'partial_success'):
        update_set['actionSentAt'] = now
    await db.comments.update_one({'id': comment_doc_id}, {'$set': update_set})

    if action_status == 'success':
        logger.info('action_execution_success ig_comment_id=%s rule_id=%s reply=%s dm=%s',
                    ig_comment_id, automation.get('id'),
                    flow_results.get('reply_status'), flow_results.get('dm_status'))
        return True
    if action_status == 'partial_success':
        logger.warning(
            'action_execution_partial_success ig_comment_id=%s rule_id=%s '
            'reply=success dm=failed dm_failure_reason=%s',
            ig_comment_id, automation.get('id'),
            flow_results.get('dm_failure_reason'),
        )
        return True
    logger.warning(
        'action_execution_send_failed ig_comment_id=%s rule_id=%s reply=%s dm=%s '
        'reply_failure_reason=%s dm_failure_reason=%s',
        ig_comment_id, automation.get('id'),
        flow_results.get('reply_status'), flow_results.get('dm_status'),
        flow_results.get('reply_failure_reason'), flow_results.get('dm_failure_reason'),
    )
    return False


# ---------------- DM Automation (independent from Comments) ----------------
def _dm_match(text: str, keyword: str, mode: str) -> bool:
    """Match incoming DM text against a rule keyword. Case-insensitive."""
    if not text or not keyword:
        return False
    t = text.strip().lower()
    k = keyword.strip().lower()
    if not t or not k:
        return False
    if mode == 'exact':
        return t == k
    if mode == 'starts_with':
        return t.startswith(k)
    # default: contains
    return k in t


def _classify_messaging_event(event: dict) -> dict:
    """Classify a raw Instagram messaging-item dict into our internal kind.
    Returns: {kind, sender_id, recipient_id, message_id, text, is_echo,
              has_message, has_read, has_delivery, has_postback, has_reaction,
              has_referral, has_attachments, message_keys, item_keys, timestamp}
    eventKind ∈ message_text | message_echo | message_attachment |
                read | delivery | postback | reaction | referral | unknown
    """
    item_keys = sorted(list(event.keys())) if isinstance(event, dict) else []
    sender = event.get('sender') if isinstance(event, dict) else None
    recipient = event.get('recipient') if isinstance(event, dict) else None
    message = event.get('message') if isinstance(event, dict) else None
    postback = event.get('postback') if isinstance(event, dict) else None
    sender_id = (sender or {}).get('id') if isinstance(sender, dict) else None
    recipient_id = (recipient or {}).get('id') if isinstance(recipient, dict) else None
    message_keys = sorted(list(message.keys())) if isinstance(message, dict) else []

    has_read = 'read' in event if isinstance(event, dict) else False
    has_delivery = 'delivery' in event if isinstance(event, dict) else False
    has_postback = 'postback' in event if isinstance(event, dict) else False
    has_reaction = 'reaction' in event if isinstance(event, dict) else False
    has_referral = 'referral' in event if isinstance(event, dict) else False
    has_message = isinstance(message, dict)
    is_echo = bool(message.get('is_echo')) if has_message else False
    text = (message.get('text') if has_message else None) or ''
    quick_reply = message.get('quick_reply') if has_message and isinstance(message.get('quick_reply'), dict) else None
    quick_reply_payload = (quick_reply or {}).get('payload') if quick_reply else None
    postback_payload = (postback or {}).get('payload') if isinstance(postback, dict) else None
    postback_title = (postback or {}).get('title') if isinstance(postback, dict) else None
    attachments = message.get('attachments') if has_message else None
    has_attachments = bool(attachments)
    message_id = (message.get('mid') or message.get('id')) if has_message else None
    timestamp = event.get('timestamp') if isinstance(event, dict) else None

    if has_read:
        kind = 'read'
    elif has_delivery:
        kind = 'delivery'
    elif has_reaction:
        kind = 'reaction'
    elif has_referral:
        kind = 'referral'
    elif has_postback:
        kind = 'postback'
    elif has_message:
        if is_echo:
            kind = 'message_echo'
        elif quick_reply_payload:
            kind = 'quick_reply'
        elif text:
            kind = 'message_text'
        elif has_attachments:
            kind = 'message_attachment'
        else:
            kind = 'unknown'
    else:
        kind = 'unknown'

    return {
        'kind': kind,
        'sender_id': sender_id,
        'recipient_id': recipient_id,
        'message_id': message_id,
        'text': text,
        'quick_reply_payload': quick_reply_payload,
        'postback_payload': postback_payload,
        'postback_title': postback_title,
        'is_echo': is_echo,
        'has_message': has_message,
        'has_read': has_read,
        'has_delivery': has_delivery,
        'has_postback': has_postback,
        'has_reaction': has_reaction,
        'has_referral': has_referral,
        'has_attachments': has_attachments,
        'message_keys': message_keys,
        'item_keys': item_keys,
        'timestamp': timestamp,
    }


@api.get('/instagram/webhook/diagnostics')
async def webhook_diagnostics(user_id: str = Depends(get_current_active_user_id)):
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    token = u.get('meta_access_token')
    ig_id = str(u.get('ig_user_id') or '')
    connected = _has_valid_instagram_connection(u)
    
    out = {
        "connected": connected,
        "connectionValid": connected,
        "canonicalIgUserId": ig_id or None,
        "dbIgUserId": ig_id or None,
        "idMatch": True,
        "callbackUrl": f"{BACKEND_PUBLIC_URL}/api/instagram/webhook",
        "verifyTokenConfigured": bool(META_VERIFY_TOKEN),
        "signatureValidationEnabled": META_WEBHOOK_HMAC_ENFORCE,
        "subscribedFields": [],
        "recentWebhookEntryIds": [],
        "recentWebhookRecipientIds": [],
        "webhookAccountMatch": False,
        "recentEventKinds": []
    }
    
    if connected and token and ig_id:
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(f"https://graph.facebook.com/v21.0/{ig_id}/subscribed_apps", params={'access_token': token})
                if r.status_code == 200:
                    data = r.json().get('data', [])
                    if data:
                        out['subscribedFields'] = data[0].get('subscribed_fields', [])
        except Exception:
            pass
            
    recent_logs = await db.dm_logs.find({'user_id': user_id}).sort('created', -1).limit(10).to_list(10)
    out['recentEventKinds'] = list(set([L.get('event_kind') for L in recent_logs]))
    out['recentWebhookEntryIds'] = list(set([L.get('ig_user_id') for L in recent_logs if L.get('ig_user_id')]))
    out['recentWebhookRecipientIds'] = list(set([L.get('recipient_id') for L in recent_logs if L.get('recipient_id')]))
    
    if ig_id and out['recentWebhookEntryIds']:
        out['webhookAccountMatch'] = ig_id in out['recentWebhookEntryIds']
    elif not out['recentWebhookEntryIds']:
        out['webhookAccountMatch'] = True # None yet
        
    return out

@api.post('/instagram/webhook/resubscribe')
async def webhook_resubscribe(user_id: str = Depends(get_current_active_user_id)):
    u = await db.users.find_one({'id': user_id})
    if not u or not _has_valid_instagram_connection(u):
        raise HTTPException(400, 'Instagram not connected')
    token = u.get('meta_access_token')
    ig_id = str(u.get('ig_user_id') or '')
    if not token or not ig_id:
        raise HTTPException(400, 'Missing IG identity')
        
    fields = 'messages,messaging_postbacks,messaging_seen,message_reactions,comments'
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"https://graph.facebook.com/v21.0/{ig_id}/subscribed_apps", params={'access_token': token, 'subscribed_fields': fields})
            if r.status_code != 200:
                raise HTTPException(400, f'Subscription failed: {r.text}')
                
            vr = await c.get(f"https://graph.facebook.com/v21.0/{ig_id}/subscribed_apps", params={'access_token': token})
            subs = []
            if vr.status_code == 200:
                data = vr.json().get('data', [])
                if data:
                    subs = data[0].get('subscribed_fields', [])
            
            return {
                "igUserIdUsed": ig_id,
                "subscribedFields": subs,
                "messagesSubscribed": 'messages' in subs,
                "commentsSubscribed": 'comments' in subs,
                "idMatch": True
            }
    except Exception as e:
        logger.error(f'Resubscribe error: {e}')
        raise HTTPException(500, str(e))

async def _handle_new_dm_message(user_doc: dict, event: dict, source: str = 'webhook'):
    """Process one incoming Instagram DM messaging-item.

    `event` is the RAW messaging-item dict from
      payload.entry[].messaging[]
    OR a legacy flattened dict with keys
      sender_id, message_id, text, timestamp, is_echo
    Returns dict: {processed, matched, status, rule_id, log_id, event_kind}
    Status values: received | matched | replied | failed | skipped
    skip_reason values: duplicate | echo | self_message | missing_sender |
                       missing_text | no_active_rules | no_rule_match | send_failed |
                       read_receipt | delivery_receipt | reaction |
                       postback_unsupported | attachment_unsupported |
                       non_message_event
    """
    import uuid as _uuid
    import hashlib as _hashlib
    user_id = user_doc['id']
    ig_account_id = user_doc.get('ig_user_id') or ''
    now = datetime.utcnow()

    # Accept both raw shape (sender/recipient/message) and legacy flattened.
    if isinstance(event, dict) and ('sender' in event or 'message' in event
                                    or 'read' in event or 'delivery' in event
                                    or 'postback' in event or 'reaction' in event
                                    or 'referral' in event):
        cls = _classify_messaging_event(event)
        sender_id = cls['sender_id']
        recipient_id = cls['recipient_id']
        message_id = cls['message_id']
        text = cls['text']
        quick_reply_payload = cls.get('quick_reply_payload')
        postback_payload = cls.get('postback_payload')
        postback_title = cls.get('postback_title')
        is_echo = cls['is_echo']
        ts = cls['timestamp']
        event_kind = cls['kind']
        message_keys = cls['message_keys']
        item_keys = cls['item_keys']
    else:
        # Legacy flattened input
        sender_id = event.get('sender_id')
        recipient_id = event.get('recipient_id')
        message_id = event.get('message_id')
        text = event.get('text') or ''
        quick_reply_payload = event.get('quick_reply_payload')
        postback_payload = event.get('postback_payload')
        postback_title = event.get('postback_title')
        is_echo = bool(event.get('is_echo'))
        ts = event.get('timestamp')
        event_kind = 'message_echo' if is_echo else ('message_text' if text else 'unknown')
        message_keys = []
        item_keys = sorted(list(event.keys())) if isinstance(event, dict) else []

    logger.info('dm_processor_invoked user_id=%s ig_account=%s source=%s',
                user_id, ig_account_id, source)
    logger.info('dm_event_kind_classified kind=%s sender=%s msg_id=%s echo=%s '
                'item_keys=%s message_keys=%s',
                event_kind, sender_id, message_id, is_echo, item_keys, message_keys)
    logger.info('dm_webhook_received ig_account=%s sender=%s msg_id=%s echo=%s kind=%s',
                ig_account_id, sender_id, message_id, is_echo, event_kind)

    # Compute a dedup key that NEVER collides on null. Prefer Meta's mid/id;
    # otherwise hash the available identifying surface for THIS event kind so
    # read/delivery/reaction events also dedup deterministically.
    if message_id:
        dedup_key = f'mid:{message_id}'
    else:
        watermark = ''
        try:
            if isinstance(event, dict):
                if isinstance(event.get('read'), dict):
                    watermark = str(event['read'].get('watermark') or '')
                elif isinstance(event.get('delivery'), dict):
                    watermark = str(event['delivery'].get('watermark') or '')
                elif isinstance(event.get('reaction'), dict):
                    watermark = str(event['reaction'].get('mid') or '') + ':' + \
                                str(event['reaction'].get('action') or '')
                elif isinstance(event.get('postback'), dict):
                    watermark = str(event['postback'].get('mid') or event['postback'].get('payload') or '')
        except Exception:
            watermark = ''
        h = _hashlib.sha256(
            f'{ig_account_id}|{sender_id or ""}|{ts or ""}|{text or ""}|{event_kind}|{watermark}'.encode('utf-8')
        ).hexdigest()
        dedup_key = f'sha:{h}'

    def _mk_log(status: str, skip_reason=None, matched_rule=None, error=None):
        return {
            'id': str(_uuid.uuid4()),
            'user_id': user_id,
            **_current_instagram_context(user_doc),
            'ig_user_id': ig_account_id,
            'sender_id': sender_id,
            'recipient_id': locals().get('recipient_id') if 'recipient_id' in locals() else None,
            'message_id': message_id,
            'dedup_key': dedup_key,
            'event_kind': event_kind,
            'message_keys': message_keys,
            'item_keys': item_keys,
            'incoming_text': text if text else None,
            'quick_reply_payload': quick_reply_payload,
            'postback_payload': postback_payload,
            'postback_title': postback_title,
            'matched_rule_id': matched_rule.get('id') if matched_rule else None,
            'matched_rule_name': matched_rule.get('name') if matched_rule else None,
            'reply_text': matched_rule.get('reply_text') if matched_rule else None,
            'status': status,
            'skip_reason': skip_reason,
            'error': error,
            'source': source,
            'is_echo': is_echo,
            'timestamp': ts,
            'created': now,
        }

    async def _persist(doc):
        logger.info('dm_log_insert_started dedup_key=%s kind=%s status=%s',
                    dedup_key, event_kind, doc.get('status'))
        try:
            await db.dm_logs.insert_one(doc)
            logger.info('dm_log_insert_success dedup_key=%s id=%s', dedup_key, doc.get('id'))
            return True
        except Exception as e:
            # DuplicateKeyError on unique (user_id, dedup_key) — that is fine,
            # someone else inserted first. Anything else is a real failure.
            logger.warning('dm_log_insert_failed dedup_key=%s err=%s', dedup_key, str(e)[:200])
            return False

    # Dedup FIRST so replays never double-reply.
    existing = await db.dm_logs.find_one({
        'user_id': user_id,
        'dedup_key': dedup_key,
        '$or': [
            {'instagramAccountId': ig_account_id},
            {'igUserId': ig_account_id},
            {'instagramAccountId': {'$exists': False}},
        ],
    })
    if existing:
        logger.info('dm_message_duplicate_skipped dedup_key=%s', dedup_key)
        return {'processed': False, 'status': 'skipped', 'reason': 'duplicate',
                'log_id': existing.get('id'), 'event_kind': event_kind}

    # Classify non-text messaging events explicitly. Always log them so the
    # debug panel can show why nothing was sent.
    if event_kind == 'read':
        await _persist(_mk_log('skipped', skip_reason='read_receipt'))
        return {'processed': False, 'status': 'skipped', 'reason': 'read_receipt',
                'event_kind': event_kind}
    if event_kind == 'delivery':
        await _persist(_mk_log('skipped', skip_reason='delivery_receipt'))
        return {'processed': False, 'status': 'skipped', 'reason': 'delivery_receipt',
                'event_kind': event_kind}
    if event_kind == 'reaction':
        await _persist(_mk_log('skipped', skip_reason='reaction'))
        return {'processed': False, 'status': 'skipped', 'reason': 'reaction',
                'event_kind': event_kind}
    if event_kind == 'referral':
        await _persist(_mk_log('skipped', skip_reason='referral'))
        return {'processed': False, 'status': 'skipped', 'reason': 'referral',
                'event_kind': event_kind}
    if event_kind == 'postback' and not text:
        session = await _find_pending_comment_dm_session(
            user_doc, sender_id, payload=postback_payload
        )
        if session:
            log_doc = _mk_log('matched')
            log_doc['comment_flow_session_id'] = session.get('id')
            if not await _persist(log_doc):
                return {'processed': False, 'status': 'skipped', 'reason': 'race',
                        'event_kind': event_kind}
            if _comment_dm_follow_confirmation_matches(
                session,
                text=postback_title or '',
                payload=postback_payload,
            ):
                await _mark_comment_dm_follow_confirmed(session)
                logger.info('follow_gate_confirmation_received session=%s via=postback',
                            session.get('id'))
            ok = await _send_comment_dm_flow_completion(user_doc, session)
            await db.dm_logs.update_one(
                {'id': log_doc['id']},
                {'$set': {'status': 'replied' if ok else 'failed',
                          'skip_reason': None if ok else 'comment_flow_send_failed'}}
            )
            return {'processed': True, 'matched': True,
                    'status': 'replied' if ok else 'failed',
                    'reason': 'comment_flow_response',
                    'log_id': log_doc['id'], 'event_kind': event_kind}
        await _persist(_mk_log('skipped', skip_reason='postback_unsupported'))
        return {'processed': False, 'status': 'skipped', 'reason': 'postback_unsupported',
                'event_kind': event_kind}
    if event_kind == 'message_attachment':
        await _persist(_mk_log('skipped', skip_reason='attachment_unsupported'))
        return {'processed': False, 'status': 'skipped', 'reason': 'attachment_unsupported',
                'event_kind': event_kind}
    if event_kind == 'unknown' and not text:
        await _persist(_mk_log('skipped', skip_reason='non_message_event'))
        return {'processed': False, 'status': 'skipped', 'reason': 'non_message_event',
                'event_kind': event_kind}

    # Real text messages from this point on.
    if is_echo or event_kind == 'message_echo':
        await _persist(_mk_log('skipped', skip_reason='echo'))
        return {'processed': False, 'status': 'skipped', 'reason': 'echo',
                'event_kind': event_kind}
    if sender_id and sender_id == ig_account_id:
        await _persist(_mk_log('skipped', skip_reason='self_message'))
        return {'processed': False, 'status': 'skipped', 'reason': 'self_message',
                'event_kind': event_kind}
    if not sender_id:
        await _persist(_mk_log('skipped', skip_reason='missing_sender'))
        return {'processed': False, 'status': 'skipped', 'reason': 'missing_sender',
                'event_kind': event_kind}
    if not text and not quick_reply_payload:
        await _persist(_mk_log('skipped', skip_reason='missing_text'))
        return {'processed': False, 'status': 'skipped', 'reason': 'missing_text',
                'event_kind': event_kind}

    logger.info('dm_sender_extracted sender=%s', sender_id)
    logger.info('dm_text_extracted len=%s text_hash=%s', len(text), _hash_text(text))

    session = await _find_pending_comment_dm_session(
        user_doc, sender_id, payload=quick_reply_payload or postback_payload
    )
    if session:
        log_doc = _mk_log('matched')
        log_doc['comment_flow_session_id'] = session.get('id')
        if not await _persist(log_doc):
            return {'processed': False, 'status': 'skipped', 'reason': 'race',
                    'event_kind': event_kind}
        if _comment_dm_follow_confirmation_matches(
            session,
            text=text or postback_title or '',
            payload=quick_reply_payload or postback_payload,
        ):
            await _mark_comment_dm_follow_confirmed(session)
            logger.info('follow_gate_confirmation_received session=%s via=message',
                        session.get('id'))
        ok = await _send_comment_dm_flow_completion(user_doc, session)
        await db.dm_logs.update_one(
            {'id': log_doc['id']},
            {'$set': {'status': 'replied' if ok else 'failed',
                      'skip_reason': None if ok else 'comment_flow_send_failed'}}
        )
        return {'processed': True, 'matched': True,
                'status': 'replied' if ok else 'failed',
                'reason': 'comment_flow_response',
                'log_id': log_doc['id'], 'event_kind': event_kind}

    # Load active DM rules for this user
    rules = await db.dm_rules.find(
        {**_account_scoped_query(user_id, _current_instagram_context(user_doc)), 'is_active': True}
    ).to_list(200)
    logger.info('dm_rule_loaded count=%s user=%s', len(rules), user_doc.get('email'))

    if not rules:
        log_doc = _mk_log('skipped', skip_reason='no_active_rules')
        await _persist(log_doc)
        return {'processed': True, 'matched': False, 'status': 'skipped',
                'reason': 'no_active_rules', 'log_id': log_doc['id']}

    matched_rule = None
    for r in rules:
        if _dm_match(text, r.get('keyword') or '', (r.get('match_mode') or 'contains').lower()):
            matched_rule = r
            break

    if not matched_rule:
        log_doc = _mk_log('skipped', skip_reason='no_rule_match')
        await _persist(log_doc)
        logger.info('dm_rule_not_matched dedup_key=%s', dedup_key)
        return {'processed': True, 'matched': False, 'status': 'skipped',
                'reason': 'no_rule_match', 'log_id': log_doc['id']}

    rule_id = matched_rule.get('id')
    logger.info('dm_rule_matched rule_id=%s dedup_key=%s', rule_id, dedup_key)
    log_doc = _mk_log('matched', matched_rule=matched_rule)
    if not await _persist(log_doc):
        return {'processed': False, 'status': 'skipped', 'reason': 'race'}

    reply_text = (matched_rule.get('reply_text') or '').strip()
    if not reply_text:
        await db.dm_logs.update_one(
            {'id': log_doc['id']},
            {'$set': {'status': 'failed', 'skip_reason': 'send_failed',
                      'error': 'rule_has_empty_reply_text'}}
        )
        return {'processed': True, 'matched': True, 'status': 'failed',
                'rule_id': rule_id, 'log_id': log_doc['id']}

    logger.info('dm_reply_started rule_id=%s dedup_key=%s', rule_id, dedup_key)
    dm_reservation = await reserve_usage_limit(
        user_id,
        'monthly_dms_sent_limit',
        increment=1,
        instagram_account_id=ig_account_id,
        source=source or 'dm_automation',
        automation_id=rule_id,
        ig_comment_id=message_id or dedup_key,
        action_id=f"{log_doc['id']}:dm_rule",
    )
    if not dm_reservation.get('allowed') or (
        dm_reservation.get('exceeded') and not dm_reservation.get('fail_open')
    ):
        await db.dm_logs.update_one(
            {'id': log_doc['id']},
            {'$set': {'status': 'plan_limited', 'skip_reason': 'plan_limit_exceeded'}}
        )
        logger.warning(
            'dm_reply_skipped_plan_limit rule_id=%s dedup_key=%s plan_key=%s used=%s limit=%s',
            rule_id, dedup_key, dm_reservation.get('plan_key'),
            dm_reservation.get('used'), dm_reservation.get('limit'),
        )
        return {'processed': True, 'matched': True, 'status': 'plan_limited',
                'reason': 'plan_limit_exceeded', 'rule_id': rule_id, 'log_id': log_doc['id']}
    ok = False
    err = None
    try:
        ok = await send_ig_dm(
            user_doc.get('meta_access_token', ''),
            ig_account_id,
            sender_id,
            reply_text,
        )
    except Exception as e:
        err = str(e)[:500]

    if ok:
        await db.dm_logs.update_one(
            {'id': log_doc['id']},
            {'$set': {'status': 'replied'}}
        )
        await confirm_usage_reservation(
            dm_reservation,
            user_id=user_id,
            event_type='dm_sent',
            instagram_account_id=ig_account_id,
            automation_id=rule_id,
            comment_id=log_doc['id'],
            metadata={'source': source or 'dm_automation', 'message_event_kind': event_kind},
        )
        logger.info('dm_reply_success rule_id=%s dedup_key=%s', rule_id, dedup_key)
        return {'processed': True, 'matched': True, 'status': 'replied',
                'rule_id': rule_id, 'log_id': log_doc['id']}
    else:
        await db.dm_logs.update_one(
            {'id': log_doc['id']},
            {'$set': {'status': 'failed', 'skip_reason': 'send_failed',
                      'error': err or 'meta_send_returned_false'}}
        )
        logger.warning('dm_reply_failed rule_id=%s dedup_key=%s err=%s',
                       rule_id, dedup_key, err)
        return {'processed': True, 'matched': True, 'status': 'failed',
                'rule_id': rule_id, 'log_id': log_doc['id'], 'error': err}


async def _process_webhook(payload: dict):
    """Process Instagram webhook events asynchronously."""
    try:
        for entry in payload.get('entry', []):
            ig_account_id = entry.get('id')
            logger.info('dm_user_mapping_started entry_id=%s', ig_account_id)
            # Find which user owns this IG account — entry.id can be either the
            # Instagram Business account id OR the Facebook Page id depending on
            # how the subscription was created, so try both.
            user_doc, mapping_via = await _find_user_doc_for_instagram_account_id(ig_account_id)
            if user_doc and mapping_via == 'users.ig_user_id':
                mapping_via = 'entry.id'
            # Fallback: if entry.id doesn't match any user, try the recipient.id
            # of any messaging-item in this entry. Read/delivery events from a
            # business account use sender=business, recipient=user — but for
            # incoming text DMs, recipient.id == business IG account.
            if not user_doc:
                for ev in entry.get('messaging', []) or []:
                    rid = (ev.get('recipient') or {}).get('id')
                    if rid:
                        user_doc, account_mapping_via = await _find_user_doc_for_instagram_account_id(rid)
                        if user_doc:
                            mapping_via = f'recipient.id:{account_mapping_via}'
                            ig_account_id = rid
                            break
            # Last-resort fallback for single-tenant deployments: if exactly
            # one user has an IG account connected, attribute the event to it.
            if not user_doc:
                connected_accounts = await db.instagram_accounts.find(
                    {'isActive': {'$ne': False}, 'connectionValid': {'$ne': False}}
                ).limit(2).to_list(2)
                if len(connected_accounts) == 1:
                    account_doc = connected_accounts[0]
                    owner = await db.users.find_one({'id': account_doc.get('userId') or account_doc.get('user_id')})
                    if owner:
                        user_doc = _with_instagram_account_context(owner, account_doc)
                        mapping_via = 'single_tenant_instagram_account_fallback'
                        ig_account_id = account_doc.get('instagramAccountId') or account_doc.get('igUserId') or ig_account_id
            if not user_doc:
                logger.warning('dm_user_mapping_failed entry_id=%s', entry.get('id'))
                continue
            # Normalize so downstream code uses the real IG account id
            ig_account_id = user_doc.get('ig_user_id') or ig_account_id
            user_id = user_doc['id']
            logger.info('dm_user_mapping_success entry_id=%s user_id=%s via=%s',
                        entry.get('id'), user_id, mapping_via)

            for event in entry.get('messaging', []):
                # ALWAYS feed the DM automation handler first, with the raw
                # messaging item, so every event (read/delivery/reaction/
                # postback/text/echo) produces an explicit dm_logs row.
                try:
                    await _handle_new_dm_message(user_doc, event, source='webhook')
                except Exception:
                    logger.exception('DM automation handler error')

                sender_id = event.get('sender', {}).get('id')
                if sender_id == ig_account_id:
                    continue  # skip own messages for legacy conv/flow path
                msg_obj = event.get('message', {})
                msg_text = msg_obj.get('text', '')
                if not msg_text:
                    continue  # legacy conv/flow path is text-only

                # Save incoming message to conversation
                import uuid as _uuid
                conv = await db.conversations.find_one({
                    'user_id': user_id,
                    'contact.ig_id': sender_id,
                    '$or': [
                        {'instagramAccountId': ig_account_id},
                        {'igUserId': ig_account_id},
                        {'instagramAccountId': {'$exists': False}},
                    ],
                })
                if not conv:
                    # Create new conversation for this contact
                    conv_id = str(_uuid.uuid4())
                    conv = {
                        'id': conv_id, 'user_id': user_id,
                        'instagramAccountId': ig_account_id,
                        'igUserId': ig_account_id,
                        'instagramUsername': (user_doc.get('instagramHandle') or '').replace('@', ''),
                        'contact': {'name': f'User {sender_id[:8]}', 'username': f'@ig_{sender_id[:8]}',
                                    'avatar': f'https://i.pravatar.cc/150?u={sender_id}',
                                    'ig_id': sender_id},
                        'messages': [], 'lastMessage': msg_text, 'time': 'now', 'unread': 0,
                        'created': datetime.utcnow(),
                    }
                    await db.conversations.insert_one(conv)
                else:
                    conv_id = conv['id']

                incoming = {'id': str(_uuid.uuid4()), 'from': 'contact', 'text': msg_text,
                            'time': datetime.utcnow().strftime('%I:%M %p')}
                await db.conversations.update_one(
                    {'id': conv_id},
                    {'$push': {'messages': incoming},
                     '$set': {'lastMessage': msg_text, 'time': 'now', 'unread': 1}}
                )
                # Push to live WS if user is connected
                await ws_manager.send(user_id, {'type': 'incoming', 'conv_id': conv_id, 'message': incoming})

                # Match automations by keyword trigger (legacy flow builder)
                automations = await db.automations.find(
                    {**_account_scoped_query(user_id, ig_account_id), 'status': 'active'}
                ).to_list(100)
                for auto in automations:
                    trigger = (auto.get('trigger') or '').lower()
                    if trigger.startswith('keyword:'):
                        keyword = trigger.split(':', 1)[1].strip()
                        if keyword and keyword.lower() in msg_text.lower():
                            create_tracked_task(execute_flow(user_doc, auto, sender_id, msg_text), 'execute_flow')
                    elif trigger == 'new follower' and event.get('follow'):
                        create_tracked_task(execute_flow(user_doc, auto, sender_id, msg_text), 'execute_flow')

                # DM Automation handler was already called at the top of the
                # loop with the raw messaging item.

            for change in entry.get('changes', []):
                field = change.get('field')
                value = change.get('value', {})
                # Normalize: IG sends field='comments'; FB Page feed sends field='feed' with item='comment'
                is_comment = field == 'comments' or (field == 'feed' and value.get('item') == 'comment')
                if is_comment:
                    commenter = value.get('from', {}) or {}
                    media_obj = value.get('media') or {}
                    # entry.time is a Unix timestamp (int) set by Meta when the
                    # entry was dispatched. It is a reliable proxy for "now"
                    # when the comment payload itself carries no timestamp.
                    entry_time_unix = entry.get('time')
                    entry_time_iso: Optional[str] = None
                    if entry_time_unix:
                        try:
                            entry_time_iso = datetime.utcfromtimestamp(
                                int(entry_time_unix)).strftime('%Y-%m-%dT%H:%M:%S+0000')
                        except (ValueError, OSError, OverflowError):
                            entry_time_iso = None
                    await _handle_new_comment(user_doc, {
                        'ig_comment_id': value.get('comment_id') or value.get('id'),
                        'media_id': media_obj.get('id') or value.get('post_id') or value.get('parent_id'),
                        'commenter_id': commenter.get('id'),
                        'commenter_username': commenter.get('username') or commenter.get('name'),
                        'text': value.get('text') or value.get('message', ''),
                        'timestamp': (
                            value.get('timestamp') or value.get('created_time') or
                            value.get('created_at') or value.get('time')
                        ),
                        # Fallback for activation-cutoff when payload has no timestamp.
                        # Only used when timestamp is absent and source='webhook'.
                        'entry_time': entry_time_iso,
                    }, source='webhook')
                elif field == 'story_insights' or (field == 'feed' and value.get('item') == 'story_insights'):
                    replier_id = value.get('from', {}).get('id')
                    if replier_id:
                        automations = await db.automations.find(
                            {'user_id': user_id, 'status': 'active', 'trigger': 'Story Reply'}
                        ).to_list(20)
                        for auto in automations:
                            create_tracked_task(execute_flow(user_doc, auto, replier_id, ''), 'execute_flow')
    except Exception:
        logger.exception('Webhook processing error')


# ---------------- Comment polling service ----------------
# Works around the Meta limitation that comment webhooks only fire when the app
# is in Live mode with Advanced Access for instagram_business_manage_comments.
# Until App Review completes, we poll GET /{media_id}/comments directly.
# Source: https://developers.facebook.com/docs/instagram-platform/webhooks/ —
# "Advanced Access is required to receive comments and live_comments webhook notifications."
IG_POLL_INTERVAL_SECONDS = int(os.environ.get('IG_POLL_INTERVAL_SECONDS', '60'))
IG_POLL_ENABLED = os.environ.get('IG_POLL_ENABLED', '1') == '1'
IG_POLL_COMMENT_BATCH_LIMIT = max(1, min(int(os.environ.get('IG_POLL_COMMENT_BATCH_LIMIT', '20')), 20))
IG_POLL_REPLY_CAP_PER_RUN = max(1, min(int(os.environ.get('IG_POLL_REPLY_CAP_PER_RUN', '10')), 10))
AUTOMATION_QUEUE_INTERVAL_SECONDS = _env_int_clamped('AUTOMATION_QUEUE_INTERVAL_SECONDS', 5, 2, 60)
COMMENT_REPLY_MIN_SPACING_SECONDS = _env_int_clamped('COMMENT_REPLY_MIN_SPACING_SECONDS', 2, 1, 30)
DM_SEND_MIN_SPACING_SECONDS = _env_int_clamped('DM_SEND_MIN_SPACING_SECONDS', 2, 1, 30)
AUTOMATION_QUEUE_BATCH_SIZE = _env_int_clamped('AUTOMATION_QUEUE_BATCH_SIZE', 10, 1, 50)
AUTOMATION_QUEUE_MAX_ATTEMPTS = _env_int_clamped('AUTOMATION_QUEUE_MAX_ATTEMPTS', 5, 1, 20)
AUTOMATION_TEMP_RETRY_BASE_SECONDS = _env_int_clamped('AUTOMATION_TEMP_RETRY_BASE_SECONDS', 30, 5, 3600)
# Background tasks are tracked in _BG_TASKS (see _register_bg_task below).
# These legacy globals stayed here only as fallbacks during the migration.
_poll_task: Optional[asyncio.Task] = None
_follow_verifier_task: Optional[asyncio.Task] = None


async def _collect_target_media_ids(user_doc: dict, automations: list) -> list:
    """Resolve the set of media IDs we need to poll for this user."""
    target: list = []
    needs_latest = False
    needs_any = False
    for a in automations:
        raw_trigger = a.get('trigger') or ''
        trigger = raw_trigger.lower()
        if trigger.startswith('comment:'):
            t = raw_trigger.split(':', 1)[1].strip()
            if t.lower() == 'latest':
                needs_latest = True
            elif t.lower() == 'any':
                needs_any = True
            elif t and t not in target:
                target.append(t)
        # Also honor explicit trigger_media_id on the automation doc
        mid = a.get('trigger_media_id') or a.get('media_id')
        if mid and mid not in target:
            target.append(mid)
    if needs_latest:
        latest = await _fetch_latest_media_id(
            user_doc.get('meta_access_token', ''),
            user_doc.get('ig_user_id', ''),
        )
        if latest and latest not in target:
            target.append(latest)
    if needs_any:
        for mid in await _fetch_recent_media_ids(
            user_doc.get('meta_access_token', ''),
            user_doc.get('ig_user_id', ''),
            limit=10,
        ):
            if mid and mid not in target:
                target.append(mid)
    return target


async def _poll_user_comments(user_doc: dict) -> dict:
    """Poll comments for one user's automations. Returns aggregated stats."""
    user_id = user_doc['id']
    token = user_doc.get('meta_access_token', '')
    ig_id = user_doc.get('ig_user_id', '')
    stats: dict = {
        'user_id': user_id,
        'mediaChecked': 0,
        'commentsSeen': 0,
        'newComments': 0,
        'matched': 0,
        'actionsSucceeded': 0,
        'actionsFailed': 0,
        'media': {},
        'errors': [],
        'replyCap': IG_POLL_REPLY_CAP_PER_RUN,
    }

    if not token or not ig_id:
        stats['errors'].append('missing_token_or_ig_id')
        return stats

    automations = await db.automations.find(
        {**_account_scoped_query(user_id, _current_instagram_context(user_doc)), 'status': 'active'}
    ).to_list(200)
    if not automations:
        return stats

    media_ids = await _collect_target_media_ids(user_doc, automations)
    if not media_ids:
        return stats

    reply_attempts = 0
    async with httpx.AsyncClient(timeout=20) as c:
        for mid in media_ids[:10]:  # cap per-user per-tick
            if reply_attempts >= IG_POLL_REPLY_CAP_PER_RUN:
                logger.info('catchup_reply_cap_reached user=%s cap=%s',
                            user_doc.get('email'), IG_POLL_REPLY_CAP_PER_RUN)
                stats['errors'].append('reply_cap_reached')
                return stats
            stats['mediaChecked'] += 1
            logger.info('media_comments_fetch_started user=%s media_id=%s',
                        user_doc.get('email'), mid)
            try:
                r = await c.get(
                    f'https://graph.instagram.com/{mid}/comments',
                    params={
                        'access_token': token,
                        'fields': 'id,text,username,timestamp,from',
                        'limit': IG_POLL_COMMENT_BATCH_LIMIT,
                    },
                )
                if r.status_code != 200:
                    logger.warning('media_comments_fetch_failed user=%s media_id=%s http=%s body=%s',
                                   user_doc.get('email'), mid, r.status_code, r.text[:200])
                    stats['media'][mid] = {'http': r.status_code, 'error': r.text[:200]}
                    stats['errors'].append({'media_id': mid, 'http': r.status_code, 'error': r.text[:200]})
                    continue
                data = (r.json() or {}).get('data') or []
                logger.info('media_comments_fetch_success user=%s media_id=%s count=%s',
                            user_doc.get('email'), mid, len(data))
                stats['commentsSeen'] += len(data)
                new_count = 0
                matched_count = 0
                succeeded = 0
                failed = 0
                for cm in data:
                    ig_comment_id = cm.get('id')
                    from_obj = cm.get('from') or {}
                    commenter_id = from_obj.get('id')
                    commenter_username = (
                        from_obj.get('username') or cm.get('username') or
                        (f'ig_{commenter_id[:8]}' if commenter_id else None)
                    )
                    if not commenter_id and commenter_username:
                        commenter_id = f'u:{commenter_username}'
                    res = await _handle_new_comment(user_doc, {
                        'ig_comment_id': ig_comment_id,
                        'media_id': mid,
                        'commenter_id': commenter_id,
                        'commenter_username': commenter_username,
                        'text': cm.get('text') or '',
                        'timestamp': cm.get('timestamp'),
                        'force_queue': reply_attempts >= IG_POLL_REPLY_CAP_PER_RUN,
                        'queue_reason': 'queued_rate_limit',
                    }, source='polling') or {}
                    if reply_attempts >= IG_POLL_REPLY_CAP_PER_RUN and res.get('queued'):
                        logger.info('poller_comment_queued comment_id=%s media_id=%s reason=reply_cap_reached',
                                    ig_comment_id, mid)
                    if res.get('processed'):
                        new_count += 1
                    if res.get('matched'):
                        matched_count += 1
                    st = res.get('action_status')
                    if st in ('success', 'partial_success'):
                        succeeded += 1
                        reply_attempts += 1
                        logger.info('poller_comment_processed comment_id=%s media_id=%s action_status=%s',
                                    ig_comment_id, mid, st)
                    elif st in ('failed_retryable', 'failed_permanent', 'failed_retry_exhausted', 'failed'):
                        failed += 1
                        reply_attempts += 1
                    elif st == 'pending' and res.get('queued'):
                        stats['errors'].append('reply_cap_reached')
                stats['newComments'] += new_count
                stats['matched'] += matched_count
                stats['actionsSucceeded'] += succeeded
                stats['actionsFailed'] += failed
                stats['media'][mid] = {'total': len(data), 'new': new_count,
                                       'matched': matched_count}
            except Exception as e:
                logger.exception('media_comments_fetch_failed user=%s media_id=%s exc=%s',
                                 user_doc.get('email'), mid, e)
                stats['media'][mid] = {'error': f'exc:{e}'}
                stats['errors'].append({'media_id': mid, 'error': f'exc:{e}'})
    return stats


# ---------------- Background task supervision ----------------
# Every long-lived loop registers itself here so a watchdog can detect
# silent deaths (uncaught exception, asyncio cancellation, container
# pause) and restart the task. The registry is also surfaced to admins
# via /api/instagram/automation-health — never with secrets.
_BG_TASKS: Dict[str, Dict[str, Any]] = {}
_BG_FACTORIES: Dict[str, Any] = {}
_WATCHDOG_INTERVAL_SECONDS = 60
WEBHOOK_LAST_RECEIVED_AT: Optional[datetime] = None
WEBHOOK_LAST_PROCESSED_AT: Optional[datetime] = None

# Graceful-shutdown plumbing. SHUTDOWN_EVENT is awaited by long-lived
# loops in place of plain asyncio.sleep so they exit promptly when the
# container receives SIGTERM. IS_SHUTTING_DOWN is checked at every
# entry point that does Mongo writes — once set, the entry point
# bails out instead of touching the database, since the Motor client
# is about to close.
SHUTDOWN_EVENT: asyncio.Event = asyncio.Event()
IS_SHUTTING_DOWN: bool = False
# Short-lived fire-and-forget tasks (webhook processors, send-DM
# tasks, broadcast jobs) register here so the shutdown hook can wait
# for them to finish before closing Mongo. The set is bounded by the
# rate of incoming webhooks and is self-cleaning via _release_inflight.
_INFLIGHT_TASKS: "set[asyncio.Task]" = set()
SHUTDOWN_BG_CANCEL_TIMEOUT_SECONDS = 5
SHUTDOWN_INFLIGHT_WAIT_SECONDS = 10


def _release_inflight(task: asyncio.Task):
    _INFLIGHT_TASKS.discard(task)
    try:
        exc = task.exception()
    except (asyncio.CancelledError, asyncio.InvalidStateError):
        exc = None
    if exc is not None:
        logger.exception('inflight_task_failed name=%s exc=%s',
                         getattr(task, '_mychat_name', '?'),
                         type(exc).__name__,
                         exc_info=exc)


def create_tracked_task(coro, name: str) -> Optional[asyncio.Task]:
    """asyncio.create_task wrapper that registers the task in
    _INFLIGHT_TASKS so the shutdown hook can wait for it.

    If shutdown has already begun the coroutine is closed immediately
    rather than scheduled — this prevents new Mongo writes after the
    Motor client starts closing. Returns the Task on success, or None
    if the work was refused due to shutdown.
    """
    if IS_SHUTTING_DOWN:
        try:
            coro.close()
        except Exception:
            pass
        logger.info('inflight_task_refused_shutting_down name=%s', name)
        return None
    task = asyncio.create_task(coro)
    setattr(task, '_mychat_name', name)
    _INFLIGHT_TASKS.add(task)
    task.add_done_callback(_release_inflight)
    return task


def _register_bg_task(name: str, factory):
    """Register a background loop coroutine factory and start it once.

    Idempotent: re-registering the same name with an already-running task
    is a no-op. Factories must be plain coroutine functions () -> coroutine.
    """
    existing = _BG_TASKS.get(name)
    if existing is not None:
        existing_task = existing.get('task')
        if existing_task is not None and not existing_task.done():
            return existing
    prior_restarts = (existing or {}).get('restarts', 0)
    _BG_FACTORIES[name] = factory
    task = asyncio.create_task(factory())
    _BG_TASKS[name] = {
        'task': task,
        'started_at': datetime.utcnow(),
        'last_tick_at': None,
        'last_success_at': None,
        'last_error_at': None,
        'last_error_type': None,
        'restarts': prior_restarts,
        'consecutive_failures': 0,
    }
    logger.info('background_task_registered name=%s running=%s', name, not task.done())
    return _BG_TASKS[name]


def _bg_tick(name: str, success: bool = True, error: Optional[BaseException] = None):
    """Update tick metadata for a registered background loop. Safe to call
    from inside the loop itself — the watchdog reads from this map."""
    info = _BG_TASKS.get(name)
    if info is None:
        return
    now = datetime.utcnow()
    info['last_tick_at'] = now
    if success:
        info['last_success_at'] = now
        info['consecutive_failures'] = 0
        info['last_error_type'] = None
    else:
        info['last_error_at'] = now
        info['last_error_type'] = type(error).__name__ if error else 'unknown'
        info['consecutive_failures'] = int(info.get('consecutive_failures') or 0) + 1


async def _watchdog_loop():
    """Restart any registered background task that has died.

    Logs background_task_crashed (with safe metadata) and
    background_task_restarted on recovery. Never raises into the runtime.
    """
    logger.info('background_task_watchdog_started interval=%ss',
                _WATCHDOG_INTERVAL_SECONDS)
    while not SHUTDOWN_EVENT.is_set():
        try:
            for name, info in list(_BG_TASKS.items()):
                if name == 'watchdog':
                    continue
                if IS_SHUTTING_DOWN:
                    break
                task = info.get('task')
                if task is None or task.done():
                    exc_type = None
                    if task is not None:
                        try:
                            exc = task.exception()
                            exc_type = type(exc).__name__ if exc else None
                        except Exception:
                            exc_type = 'unknown'
                    logger.error('background_task_crashed name=%s exc=%s',
                                 name, exc_type)
                    factory = _BG_FACTORIES.get(name)
                    if factory is not None and not IS_SHUTTING_DOWN:
                        new_task = asyncio.create_task(factory())
                        info['task'] = new_task
                        info['started_at'] = datetime.utcnow()
                        info['restarts'] = int(info.get('restarts') or 0) + 1
                        logger.info('background_task_restarted name=%s restarts=%s',
                                    name, info['restarts'])
        except Exception:
            logger.exception('watchdog_loop error')
        try:
            await asyncio.wait_for(SHUTDOWN_EVENT.wait(), timeout=_WATCHDOG_INTERVAL_SECONDS)
        except (asyncio.TimeoutError, RuntimeError):
            await asyncio.sleep(_WATCHDOG_INTERVAL_SECONDS)


def _classify_meta_error(exc: BaseException, status: Optional[int] = None) -> str:
    """Map a Graph error to a stable category for retry/back-off decisions."""
    if status == 429:
        return 'rate_limit'
    if status in (401, 403):
        return 'invalid_token_or_permission'
    if status and 500 <= status < 600:
        return 'temporary_5xx'
    name = type(exc).__name__
    if name in ('TimeoutException', 'ReadTimeout', 'ConnectTimeout',
                'ConnectError', 'NetworkError'):
        return 'network_timeout'
    return 'unknown'


def _automation_queue_due_query(now: datetime) -> dict:
    return {
        '$or': [
            {
                'matched': True,
                'action_status': {'$in': ['pending', 'failed_retryable']},
                'next_retry_at': {'$lte': now},
            },
            {
                'matched': True,
                'action_status': 'partial_success',
                '$or': [
                    {'dm_status': 'failed', 'dm_failure_retryable': True},
                    {'reply_status': 'failed', 'reply_failure_retryable': True},
                ],
                'next_retry_at': {'$lte': now},
            },
        ],
    }


async def _automation_queue_tick() -> dict:
    now = datetime.utcnow()
    summary = {'checked': 0, 'processed': 0, 'success': 0, 'partial_success': 0,
               'failed_retryable': 0, 'failed_permanent': 0}
    cursor = db.comments.find(_automation_queue_due_query(now)).sort('next_retry_at', 1).limit(
        AUTOMATION_QUEUE_BATCH_SIZE
    )
    due = await cursor.to_list(AUTOMATION_QUEUE_BATCH_SIZE)
    if not due:
        logger.info('automation_queue_no_due_items')
        return summary

    for item in due:
        if IS_SHUTTING_DOWN:
            break
        summary['checked'] += 1
        comment_id = item.get('ig_comment_id') or item.get('igCommentId') or item.get('id')
        media_id = item.get('media_id') or item.get('mediaId')
        user_id = item.get('user_id')
        instagram_account_id = item.get('instagramAccountId') or item.get('igUserId')
        rule_id = item.get('rule_id') or item.get('ruleId')
        attempts = int(item.get('attempts') or 0)
        lock_until = datetime.utcnow() + timedelta(minutes=5)
        claim_query = {
            'id': item.get('id'),
            '$and': [
                _automation_queue_due_query(datetime.utcnow()),
                {
                    '$or': [
                        {'queue_lock_until': {'$exists': False}},
                        {'queue_lock_until': None},
                        {'queue_lock_until': {'$lte': datetime.utcnow()}},
                    ],
                },
            ],
        }
        try:
            claimed = await db.comments.find_one_and_update(
                claim_query,
                {'$set': {
                    'action_status': 'processing',
                    'actionStatus': 'processing',
                    'queue_lock_until': lock_until,
                    'last_queue_attempt_at': datetime.utcnow(),
                    'updated': datetime.utcnow(),
                }, '$inc': {'attempts': 1}},
                return_document=ReturnDocument.AFTER,
            )
        except TypeError:
            claimed = await db.comments.find_one_and_update(
                claim_query,
                {'$set': {
                    'action_status': 'processing',
                    'actionStatus': 'processing',
                    'queue_lock_until': lock_until,
                    'last_queue_attempt_at': datetime.utcnow(),
                    'updated': datetime.utcnow(),
                }, '$inc': {'attempts': 1}},
            )
        if not claimed:
            continue
        item = claimed
        comment_id = item.get('ig_comment_id') or item.get('igCommentId') or item.get('id')
        media_id = item.get('media_id') or item.get('mediaId')
        user_id = item.get('user_id')
        instagram_account_id = item.get('instagramAccountId') or item.get('igUserId')
        rule_id = item.get('rule_id') or item.get('ruleId')
        attempts = max(int(item.get('attempts') or 1) - 1, attempts)
        claim_attempt = attempts + 1
        logger.info('automation_queue_item_claimed comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s action_status=%s reply_status=%s dm_status=%s attempt=%s next_retry_at=%s reason=%s',
                    comment_id, media_id, user_id, instagram_account_id, rule_id,
                    item.get('action_status'), item.get('reply_status'), item.get('dm_status'),
                    claim_attempt, item.get('next_retry_at'), item.get('skip_reason'))
        try:
            user_doc = await db.users.find_one({'id': user_id})
            automation = await db.automations.find_one({'id': rule_id, 'user_id': user_id})
            if not user_doc or not automation:
                await db.comments.update_one(
                    {'id': item.get('id')},
                    {'$set': {
                        'action_status': 'failed_permanent',
                        'actionStatus': 'failed_permanent',
                        'skip_reason': 'missing_user_or_rule',
                        'skipReason': 'missing_user_or_rule',
                        'queue_lock_until': None,
                        'updated': datetime.utcnow(),
                    }},
                )
                logger.info('automation_queue_item_failed_permanent comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s reason=missing_user_or_rule',
                            comment_id, media_id, user_id, instagram_account_id, rule_id)
                summary['failed_permanent'] += 1
                continue
            account_doc = await db.instagram_accounts.find_one({
                'userId': user_id,
                '$or': [
                    {'id': item.get('instagramAccountDbId') or item.get('instagram_account_id')},
                    {'instagramAccountId': instagram_account_id},
                    {'igUserId': instagram_account_id},
                ],
            })
            if account_doc:
                user_doc = _with_instagram_account_context(user_doc, account_doc)
            logger.info('automation_queue_item_processing_started comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s attempt=%s',
                        comment_id, media_id, user_id, instagram_account_id, rule_id, attempts + 1)
            result = await _run_and_record_action(
                user_doc,
                automation,
                item.get('commenter_id'),
                item.get('text') or '',
                comment_doc_id=item.get('id'),
                ig_comment_id=comment_id,
                source='automation_queue',
            )
            action_status = (result or {}).get('action_status') if isinstance(result, dict) else None
            saved = await db.comments.find_one({'id': item.get('id')}) or {}
            action_status = action_status or saved.get('action_status') or 'failed_retryable'
            update = {'queue_lock_until': None, 'updated': datetime.utcnow()}
            retryable_step_remaining = _has_retryable_step_failure(saved)
            if action_status == 'failed_retryable':
                if claim_attempt >= AUTOMATION_QUEUE_MAX_ATTEMPTS:
                    action_status = 'failed_retry_exhausted'
                    update.update({
                        'action_status': action_status,
                        'actionStatus': action_status,
                        'next_retry_at': None,
                        'skip_reason': 'max_attempts_reached',
                        'skipReason': 'max_attempts_reached',
                    })
                    logger.info('automation_queue_item_failed_permanent comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s action_status=%s attempt=%s reason=max_attempts_reached',
                                comment_id, media_id, user_id, instagram_account_id, rule_id,
                                action_status, claim_attempt)
                else:
                    next_retry = _next_retry_time(claim_attempt)
                    update.update({'next_retry_at': next_retry, 'queued': True})
                    logger.info('automation_queue_item_rescheduled comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s action_status=%s reply_status=%s dm_status=%s attempt=%s next_retry_at=%s reason=%s',
                                comment_id, media_id, user_id, instagram_account_id, rule_id,
                                action_status, saved.get('reply_status'), saved.get('dm_status'),
                                claim_attempt, next_retry, _failure_category_from_doc(saved))
            elif action_status == 'partial_success' and retryable_step_remaining:
                if claim_attempt >= AUTOMATION_QUEUE_MAX_ATTEMPTS:
                    update.update({
                        'queued': False,
                        'next_retry_at': None,
                        'skip_reason': 'max_attempts_reached',
                        'skipReason': 'max_attempts_reached',
                    })
                    logger.info('automation_queue_item_partial_retry_exhausted comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s attempt=%s reason=max_attempts_reached',
                                comment_id, media_id, user_id, instagram_account_id, rule_id,
                                claim_attempt)
                else:
                    next_retry = _next_retry_time(claim_attempt)
                    update.update({'queued': True, 'next_retry_at': next_retry})
                    logger.info('automation_queue_item_partial_rescheduled comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s attempt=%s next_retry_at=%s reason=%s',
                                comment_id, media_id, user_id, instagram_account_id, rule_id,
                                claim_attempt, next_retry, _failure_category_from_doc(saved))
            elif action_status in ('success', 'partial_success'):
                update.update({'queued': False, 'next_retry_at': None})
            elif action_status == 'failed_permanent':
                update.update({'queued': False, 'next_retry_at': None})
            await db.comments.update_one({'id': item.get('id')}, {'$set': update})
            logger.info('automation_queue_item_lock_released comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s action_status=%s reply_status=%s dm_status=%s attempt=%s next_retry_at=%s reason=%s',
                        comment_id, media_id, user_id, instagram_account_id, rule_id,
                        action_status, saved.get('reply_status'), saved.get('dm_status'),
                        claim_attempt, update.get('next_retry_at'), _failure_category_from_doc(saved))
            if action_status == 'success':
                summary['success'] += 1
                logger.info('automation_queue_item_processing_success comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s action_status=%s',
                            comment_id, media_id, user_id, instagram_account_id, rule_id, action_status)
            elif action_status == 'partial_success':
                summary['partial_success'] += 1
                logger.info('automation_queue_item_processing_partial_success comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s action_status=%s',
                            comment_id, media_id, user_id, instagram_account_id, rule_id, action_status)
            elif action_status == 'failed_retryable':
                summary['failed_retryable'] += 1
                logger.info('automation_queue_item_failed_retryable comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s reason=%s',
                            comment_id, media_id, user_id, instagram_account_id, rule_id,
                            _failure_category_from_doc(saved))
            else:
                summary['failed_permanent'] += 1
            summary['processed'] += 1
            await _safe_record_usage_event(
                user_id=user_id,
                event_type='queue_job_processed',
                instagram_account_id=instagram_account_id,
                automation_id=rule_id,
                comment_id=item.get('id'),
                queue_job_id=item.get('id'),
                metadata={
                    'action_status': action_status,
                    'reply_status': saved.get('reply_status'),
                    'dm_status': saved.get('dm_status'),
                    'attempt': claim_attempt,
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            claim_attempt = attempts + 1
            exhausted = claim_attempt >= AUTOMATION_QUEUE_MAX_ATTEMPTS
            next_retry = None if exhausted else _next_retry_time(claim_attempt)
            action_status = 'failed_retry_exhausted' if exhausted else 'failed_retryable'
            await db.comments.update_one(
                {'id': item.get('id')},
                {'$set': {
                    'action_status': action_status,
                    'actionStatus': action_status,
                    'next_retry_at': next_retry,
                    'queue_lock_until': None,
                    'skip_reason': 'max_attempts_reached' if exhausted else 'queue_processing_exception',
                    'skipReason': 'max_attempts_reached' if exhausted else 'queue_processing_exception',
                    'updated': datetime.utcnow(),
                }},
            )
            logger.error('automation_queue_item_failed comment_id=%s media_id=%s user_id=%s instagramAccountId=%s rule_id=%s action_status=%s next_retry_at=%s reason=%s exception_type=%s',
                         comment_id, media_id, user_id, instagram_account_id, rule_id,
                         action_status, next_retry,
                         'max_attempts_reached' if exhausted else 'queue_processing_exception',
                         type(exc).__name__)
            if exhausted:
                summary['failed_permanent'] += 1
            else:
                summary['failed_retryable'] += 1
    return summary


async def _repair_legacy_reply_success_without_provider_proof(limit: int = 500) -> dict:
    """Move legacy false public-reply successes back into the retry queue.

    Older records could have replied=True/reply_status=success without any
    Meta /replies confirmation. Only repair comments that still look eligible;
    proofed replies, bot-owned comments, and historical ineligible comments are
    left untouched.
    """
    now = datetime.utcnow()
    summary = {'checked': 0, 'repaired': 0, 'skipped': 0}
    query = {
        'reply_provider_response_ok': {'$ne': True},
        '$or': [
            {'replied': True},
            {'reply_status': {'$in': ['success', 'sent', 'replied']}},
            {'replyStatus': {'$in': ['success', 'sent', 'replied']}},
        ],
    }
    try:
        docs = await db.comments.find(query).limit(limit).to_list(limit)
    except Exception as exc:
        logger.warning('legacy_reply_success_repair_load_failed err=%s', type(exc).__name__)
        return {**summary, 'error': type(exc).__name__}
    for comment in docs:
        summary['checked'] += 1
        if _reply_provider_proof_exists(comment):
            summary['skipped'] += 1
            continue
        rule_id = comment.get('rule_id') or comment.get('ruleId')
        user_id = comment.get('user_id')
        media_id = comment.get('media_id') or comment.get('mediaId')
        commenter_id = comment.get('commenter_id') or comment.get('instagramUserId')
        ig_account_id = comment.get('instagramAccountId') or comment.get('igUserId')
        if not rule_id or not user_id or not ig_account_id:
            summary['skipped'] += 1
            continue
        if commenter_id and commenter_id == ig_account_id:
            summary['skipped'] += 1
            continue
        rule = await db.automations.find_one({'id': rule_id, 'user_id': user_id})
        if not rule:
            summary['skipped'] += 1
            continue
        comment_ts = _parse_graph_datetime(
            comment.get('effective_timestamp')
            or comment.get('commentTimestamp')
            or comment.get('timestamp')
        )
        activation_ts = _parse_graph_datetime(rule.get('activationStartedAt') or rule.get('createdAt'))
        historical_allowed = _historical_catchup_enabled_for_media(rule, media_id)
        if activation_ts and comment_ts and comment_ts <= activation_ts and not historical_allowed:
            summary['skipped'] += 1
            continue
        await db.comments.update_one(
            {'id': comment.get('id'), 'user_id': user_id},
            {'$set': {
                'replied': False,
                'reply_status': 'pending',
                'replyStatus': 'pending',
                'reply_failure_reason': 'legacy_success_without_provider_confirmation',
                'reply_failure_retryable': True,
                'reply_provider_response_ok': False,
                'reply_provider_comment_id': None,
                'reply_success_source': None,
                'action_status': 'failed_retryable',
                'actionStatus': 'failed_retryable',
                'skip_reason': 'legacy_success_without_provider_confirmation',
                'skipReason': 'legacy_success_without_provider_confirmation',
                'queued': True,
                'next_retry_at': now,
                'queue_lock_until': None,
                'updated': now,
            }}
        )
        summary['repaired'] += 1
    if summary['checked']:
        logger.info('legacy_reply_success_repair checked=%s repaired=%s skipped=%s',
                    summary['checked'], summary['repaired'], summary['skipped'])
    return summary


async def _automation_queue_loop():
    logger.info('automation_queue_started interval=%s batch_size=%s max_attempts=%s',
                AUTOMATION_QUEUE_INTERVAL_SECONDS, AUTOMATION_QUEUE_BATCH_SIZE,
                AUTOMATION_QUEUE_MAX_ATTEMPTS)
    while not SHUTDOWN_EVENT.is_set():
        if IS_SHUTTING_DOWN:
            break
        cycle_ok = True
        cycle_err: Optional[BaseException] = None
        try:
            logger.info('automation_queue_tick_started')
            summary = await _automation_queue_tick()
            logger.info('automation_queue_tick_finished checked=%s processed=%s success=%s partial_success=%s failed_retryable=%s failed_permanent=%s',
                        summary.get('checked'), summary.get('processed'),
                        summary.get('success'), summary.get('partial_success'),
                        summary.get('failed_retryable'), summary.get('failed_permanent'))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            cycle_ok = False
            cycle_err = exc
            logger.exception('automation_queue_tick_failed')
        _bg_tick('automation_queue', success=cycle_ok, error=cycle_err)
        try:
            await asyncio.wait_for(SHUTDOWN_EVENT.wait(), timeout=AUTOMATION_QUEUE_INTERVAL_SECONDS)
        except (asyncio.TimeoutError, RuntimeError):
            await asyncio.sleep(AUTOMATION_QUEUE_INTERVAL_SECONDS)


async def _comment_poller_loop():
    """Runs forever: every IG_POLL_INTERVAL_SECONDS, poll comments for all
    connected users that have active comment automations.

    Resilience guarantees:
      • Per-user errors stay scoped — one bad account never blocks others.
      • Outer try/except keeps the loop alive on Mongo errors.
      • Consecutive-failure backoff prevents tight loops on persistent error.
      • _bg_tick updates the supervisor so the watchdog and health endpoint
        can see liveness.
    """
    logger.info('comment_poller_started interval=%ss', IG_POLL_INTERVAL_SECONDS)
    while not SHUTDOWN_EVENT.is_set():
        if IS_SHUTTING_DOWN:
            break
        cycle_ok = True
        cycle_err: Optional[BaseException] = None
        try:
            cursor = db.users.find({'instagramConnected': True})
            users = await cursor.to_list(500)
            logger.info('comment_poller_tick accounts=%s', len(users))
            for u in users:
                if IS_SHUTTING_DOWN:
                    break
                try:
                    s = await _poll_user_comments(u)
                    if s.get('newComments'):
                        logger.info('polling_user_summary user_id=%s new=%s matched=%s ok=%s fail=%s',
                                    u.get('id'), s['newComments'], s['matched'],
                                    s['actionsSucceeded'], s['actionsFailed'])
                except Exception as per_user_exc:
                    logger.exception('comment_poller_per_user_error user_id=%s',
                                     u.get('id'))
                    cycle_err = per_user_exc
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            cycle_ok = False
            cycle_err = exc
            logger.exception('comment_poller_tick_failed')
        else:
            if not IS_SHUTTING_DOWN:
                logger.info('comment_poller_tick_success')
        _bg_tick('comment_poller', success=cycle_ok, error=cycle_err)
        if SHUTDOWN_EVENT.is_set():
            break
        # Back off when consecutive failures pile up (capped).
        info = _BG_TASKS.get('comment_poller') or {}
        consecutive = int(info.get('consecutive_failures') or 0)
        sleep_s = IG_POLL_INTERVAL_SECONDS
        if not cycle_ok and consecutive > 0:
            sleep_s = min(IG_POLL_INTERVAL_SECONDS * (2 ** min(consecutive, 5)),
                          IG_POLL_INTERVAL_SECONDS * 32)
        try:
            await asyncio.wait_for(SHUTDOWN_EVENT.wait(), timeout=sleep_s)
        except (asyncio.TimeoutError, RuntimeError):
            # RuntimeError when event is bound to a different loop (tests).
            await asyncio.sleep(sleep_s)


async def _follow_verifier_loop():
    """Background re-verifier for pending follow-gate sessions.

    Meta sometimes updates is_user_follow_business a few seconds after
    the user actually follows. This loop re-checks pending sessions so a
    user who followed but didn't re-tap still gets the link.

    Idempotency: _send_comment_dm_flow_completion checks finalDmSentAt
    before sending; only sessions inside the cutoff window are processed.
    """
    logger.info('follow_verifier_started interval=%ss',
                FOLLOW_BACKGROUND_VERIFIER_INTERVAL_SECONDS)
    while not SHUTDOWN_EVENT.is_set():
        if IS_SHUTTING_DOWN:
            break
        cycle_ok = True
        cycle_err: Optional[BaseException] = None
        try:
            cutoff = datetime.utcnow() - timedelta(
                minutes=FOLLOW_BACKGROUND_VERIFIER_MAX_AGE_MINUTES
            )
            cursor = db.comment_dm_sessions.find({
                'status': 'pending',
                'follow_request_enabled': True,
                'verify_actual_follow': {'$ne': False},
                'follow_verified': {'$ne': True},
                'finalDmSentAt': None,
                'follow_confirmed': True,
                'created': {'$gte': cutoff},
            })
            sessions = await cursor.to_list(200)
            if sessions:
                logger.info('follow_verifier_tick pending=%s', len(sessions))
            for sess in sessions:
                if IS_SHUTTING_DOWN:
                    break
                try:
                    user_doc = await db.users.find_one({'id': sess.get('user_id')})
                    if not user_doc:
                        continue
                    await _send_comment_dm_flow_completion(user_doc, sess)
                except Exception:
                    logger.exception('follow_verifier_per_session_error session=%s',
                                     sess.get('id'))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            cycle_ok = False
            cycle_err = exc
            logger.exception('follow_verifier_tick_failed')
        _bg_tick('follow_verifier', success=cycle_ok, error=cycle_err)
        try:
            await asyncio.wait_for(SHUTDOWN_EVENT.wait(),
                                   timeout=FOLLOW_BACKGROUND_VERIFIER_INTERVAL_SECONDS)
        except (asyncio.TimeoutError, RuntimeError):
            await asyncio.sleep(FOLLOW_BACKGROUND_VERIFIER_INTERVAL_SECONDS)


@api.get('/instagram/automation-health')
async def instagram_automation_health(user_id: str = Depends(get_current_active_user_id)):
    """Operational status for comment / DM / follow automation.

    Returns liveness data for every background loop, the time of the
    last webhook receive/process, and a per-account token-health summary.
    Never exposes tokens, secrets, or message content.
    """
    def _iso(dt: Optional[datetime]) -> Optional[str]:
        return dt.isoformat() if isinstance(dt, datetime) else None

    tasks_out = {}
    for name, info in _BG_TASKS.items():
        task = info.get('task')
        running = bool(task is not None and not task.done())
        tasks_out[name] = {
            'running': running,
            'started_at': _iso(info.get('started_at')),
            'last_tick_at': _iso(info.get('last_tick_at')),
            'last_success_at': _iso(info.get('last_success_at')),
            'last_error_at': _iso(info.get('last_error_at')),
            'last_error_type': info.get('last_error_type'),
            'restarts': info.get('restarts') or 0,
            'consecutive_failures': info.get('consecutive_failures') or 0,
        }

    # Per-user token health summary, scoped to the caller's user_id.
    accounts_out = []
    try:
        u = await db.users.find_one({'id': user_id})
        if u:
            accounts_out.append({
                'user_id': u.get('id'),
                'instagramAccountId': u.get('ig_user_id') or None,
                'instagramConnected': bool(u.get('instagramConnected')),
                'connectionValid': bool(u.get('instagramConnectionValid', True)),
                'auth_kind': u.get('ig_auth_kind'),
                # Token presence as a boolean; never the value.
                'tokenPresent': bool(u.get('meta_access_token')),
            })
    except Exception:
        logger.exception('automation_health user_lookup failed')

    pending_jobs = 0
    failed_jobs = 0
    try:
        pending_jobs = await db.comment_dm_sessions.count_documents(
            {'status': 'pending', 'user_id': user_id}
        )
        failed_jobs = await db.comment_dm_sessions.count_documents(
            {'status': {'$in': ['failed', 'verification_failed']},
             'user_id': user_id}
        )
    except Exception:
        logger.exception('automation_health session_counts failed')

    return {
        'tasks': tasks_out,
        'webhook': {
            'last_received_at': _iso(WEBHOOK_LAST_RECEIVED_AT),
            'last_processed_at': _iso(WEBHOOK_LAST_PROCESSED_AT),
        },
        'accounts': accounts_out,
        'jobs': {
            'pending_comment_dm_sessions': pending_jobs,
            'failed_comment_dm_sessions': failed_jobs,
        },
        'config': {
            'comment_poller_interval_seconds': IG_POLL_INTERVAL_SECONDS,
            'comment_poller_enabled': IG_POLL_ENABLED,
            'automation_queue_interval_seconds': AUTOMATION_QUEUE_INTERVAL_SECONDS,
            'automation_queue_batch_size': AUTOMATION_QUEUE_BATCH_SIZE,
            'automation_queue_max_attempts': AUTOMATION_QUEUE_MAX_ATTEMPTS,
            'follow_verifier_interval_seconds': FOLLOW_BACKGROUND_VERIFIER_INTERVAL_SECONDS,
            'watchdog_interval_seconds': _WATCHDOG_INTERVAL_SECONDS,
        },
    }


@api.post('/instagram/process-unreplied-comments')
async def instagram_process_unreplied_comments(user_id: str = Depends(get_current_active_user_id)):
    """Manual catch-up for selected-post historical replies.

    This intentionally fetches comments only for active rules that request
    process_existing_unreplied_comments and are scoped to exactly one selected
    media id. Broad rules are ignored, and _handle_new_comment still performs
    matching, activation-cutoff checks, rate caps, and duplicate protection.
    """
    if _rate_limited('process_unreplied', user_id,
                     limit=RATE_LIMIT_PROCESS_UNREPLIED_PER_MIN, window_seconds=60):
        logger.warning('rate_limit_hit bucket=process_unreplied user_id=%s', user_id)
        raise HTTPException(429, 'Too many catch-up requests. Try again in a minute.')
    user_doc = await db.users.find_one({'id': user_id})
    if not user_doc:
        raise HTTPException(404, 'user not found')
    try:
        account = await getActiveInstagramAccount(user_id)
    except HTTPException as e:
        if e.status_code == 400:
            account = None
        else:
            raise
    if not account:
        return {
            'checked': 0, 'matched': 0, 'replied': 0,
            'skipped_historical': 0, 'skipped_broad_scope': 0,
            'skipped_duplicate': 0, 'failed': 0,
            'reason': 'instagram_not_connected',
        }
    u = _with_instagram_account_context(user_doc, account)
    token = u.get('meta_access_token') or ''
    summary = {'checked': 0, 'matched': 0, 'replied': 0,
               'skipped_historical': 0, 'skipped_broad_scope': 0,
               'skipped_duplicate': 0, 'failed': 0,
               'reply_cap': IG_POLL_REPLY_CAP_PER_RUN,
               'batch_limit': IG_POLL_COMMENT_BATCH_LIMIT,
               'media_ids': []}

    rules = await db.automations.find(
        {**_account_scoped_query(user_id, account), 'status': 'active'}
    ).to_list(200)
    media_ids: list = []
    for rule in rules:
        requested = bool(
            rule.get('process_existing_unreplied_comments')
            or rule.get('processExistingComments')
            or rule.get('processExistingUnrepliedComments')
        )
        if not requested:
            continue
        selected_media_id = _selected_specific_media_id(rule)
        if not selected_media_id:
            summary['skipped_broad_scope'] += 1
            logger.info('process_existing_unreplied_comments_ignored_reason=broad_scope rule_id=%s user_id=%s',
                        rule.get('id'), user_id)
            continue
        if selected_media_id not in media_ids:
            media_ids.append(selected_media_id)
    summary['media_ids'] = media_ids
    if not media_ids:
        logger.info('process_unreplied_comments_summary user_id=%s %s', user_id, summary)
        return summary
    if not token:
        summary['failed'] += 1
        summary['reason'] = 'missing_instagram_token'
        return summary

    logger.info('selected_post_catchup_started user_id=%s media_ids=%s batch_limit=%s reply_cap=%s',
                user_id, media_ids, IG_POLL_COMMENT_BATCH_LIMIT, IG_POLL_REPLY_CAP_PER_RUN)
    async with httpx.AsyncClient(timeout=20) as c:
        for media_id in media_ids:
            if summary['checked'] >= IG_POLL_COMMENT_BATCH_LIMIT:
                summary['batch_limit_reached'] = True
                break
            cap_already_reached = summary['replied'] + summary['failed'] >= IG_POLL_REPLY_CAP_PER_RUN
            if cap_already_reached:
                logger.info('catchup_reply_cap_reached user_id=%s cap=%s; fetched comments will be queued only',
                            user_id, IG_POLL_REPLY_CAP_PER_RUN)
                summary['reply_cap_reached'] = True
            try:
                remaining = max(1, IG_POLL_COMMENT_BATCH_LIMIT - summary['checked'])
                r = await c.get(
                    f'https://graph.instagram.com/{media_id}/comments',
                    params={
                        'fields': 'id,text,username,timestamp,from',
                        'access_token': token,
                        'limit': min(remaining, IG_POLL_COMMENT_BATCH_LIMIT),
                    },
                )
                if r.status_code != 200:
                    summary['failed'] += 1
                    logger.warning('selected_post_catchup_fetch_failed user_id=%s media_id=%s http=%s body=%s',
                                   user_id, media_id, r.status_code, r.text[:200])
                    continue
                for cm in (r.json() or {}).get('data') or []:
                    if summary['checked'] >= IG_POLL_COMMENT_BATCH_LIMIT:
                        summary['batch_limit_reached'] = True
                        break
                    queue_only = summary['replied'] + summary['failed'] >= IG_POLL_REPLY_CAP_PER_RUN
                    if queue_only:
                        summary['reply_cap_reached'] = True
                    commenter = cm.get('from') or {}
                    commenter_id = (
                        commenter.get('id') or cm.get('user_id') or cm.get('owner_id') or
                        cm.get('username') or ''
                    )
                    if not commenter_id:
                        continue
                    summary['checked'] += 1
                    res = await _handle_new_comment(u, {
                        'ig_comment_id': cm.get('id'),
                        'media_id': media_id,
                        'commenter_id': str(commenter_id),
                        'commenter_username': cm.get('username') or commenter.get('username') or commenter.get('name'),
                        'text': cm.get('text') or '',
                        'timestamp': cm.get('timestamp') or cm.get('created_time'),
                        'force_queue': queue_only,
                        'queue_reason': 'queued_rate_limit',
                    }, source='manual_catchup') or {}
                    reason = res.get('reason') or res.get('skip_reason')
                    if res.get('already_processed'):
                        summary['skipped_duplicate'] += 1
                    elif reason == 'historical_before_rule_activation':
                        summary['skipped_historical'] += 1
                    elif res.get('matched'):
                        summary['matched'] += 1
                        if res.get('action_status') in ('success', 'partial_success'):
                            summary['replied'] += 1
                        elif res.get('action_status') in ('failed', 'failed_retryable', 'failed_permanent', 'failed_retry_exhausted'):
                            summary['failed'] += 1
                        elif res.get('queued'):
                            summary.setdefault('queued', 0)
                            summary['queued'] += 1
            except Exception:
                logger.exception('process_unreplied_comments per-media error media_id=%s', media_id)
                summary['failed'] += 1
    logger.info('process_unreplied_comments_summary user_id=%s %s', user_id, summary)
    return summary


@api.get('/instagram/poll-now')
async def instagram_poll_now(email: str = '', key: str = ''):
    """Manually trigger a single poll for one user — DISABLED in production.
    Use the authenticated POST /api/instagram/comments/poll-now instead."""
    raise HTTPException(403, 'Disabled in production. Use POST /api/instagram/comments/poll-now with JWT auth.')
    u = await db.users.find_one({'email': email})
    if not u:
        raise HTTPException(404, 'user not found')
    stats = await _poll_user_comments(u)
    return stats


@api.post('/instagram/comments/poll-now')
async def instagram_comments_poll_now(user_id: str = Depends(get_current_active_user_id)):
    """Authenticated trigger: poll comments for the calling user right now.
    Returns a summary in the documented shape."""
    if _rate_limited('poll_now', user_id,
                     limit=RATE_LIMIT_POLL_NOW_PER_MIN, window_seconds=60):
        logger.warning('rate_limit_hit bucket=poll_now user_id=%s', user_id)
        raise HTTPException(429, 'Too many poll-now requests. Try again in a minute.')
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'user not found')
    if not u.get('instagramConnected'):
        return {
            'ok': False, 'accountsChecked': 0, 'mediaChecked': 0,
            'commentsSeen': 0, 'newComments': 0, 'matched': 0,
            'actionsSucceeded': 0, 'actionsFailed': 0,
            'errors': [{'error': 'instagram_not_connected'}],
        }
    s = await _poll_user_comments(u)
    return {
        'ok': True,
        'accountsChecked': 1,
        'mediaChecked': s.get('mediaChecked', 0),
        'commentsSeen': s.get('commentsSeen', 0),
        'newComments': s.get('newComments', 0),
        'matched': s.get('matched', 0),
        'actionsSucceeded': s.get('actionsSucceeded', 0),
        'actionsFailed': s.get('actionsFailed', 0),
        'errors': s.get('errors', []),
    }


@api.get('/instagram/comments/processed')
async def instagram_comments_processed(
    limit: int = 50,
    user_id: str = Depends(get_current_active_user_id),
):
    """Diagnostic: list recently processed comments for the calling user."""
    limit = max(1, min(limit, 200))
    cur = db.comments.find({'user_id': user_id}).sort('created', -1).limit(limit)
    items = await cur.to_list(limit)
    out = []
    for d in items:
        d.pop('_id', None)
        for k in ('created', 'processed_at', 'commentTimestamp', 'ruleActivationStartedAt'):
            if isinstance(d.get(k), datetime):
                d[k] = d[k].isoformat()
        out.append({
            'id': d.get('id'),
            'igCommentId': d.get('ig_comment_id'),
            'mediaId': d.get('media_id'),
            'ruleId': d.get('rule_id'),
            'commentTimestamp': d.get('commentTimestamp') or d.get('timestamp'),
            'ruleActivationStartedAt': d.get('ruleActivationStartedAt'),
            'processExistingComments': bool(d.get('processExistingComments')),
            'commenterUsername': d.get('commenter_username'),
            'text': d.get('text'),
            'source': d.get('source'),
            'matched': bool(d.get('matched')),
            'actionStatus': d.get('action_status'),
            'skipReason': d.get('skipReason') or d.get('skip_reason'),
            'error': d.get('error'),
            'timestamp': d.get('timestamp'),
            'processedAt': d.get('processed_at'),
            'created': d.get('created'),
        })
    return {'count': len(out), 'items': out}


@api.get('/instagram/diagnostics/full')
async def instagram_diagnostics_full(user_id: str = Depends(get_current_active_user_id)):
    """Comprehensive end-to-end diagnostic.
    Hits Graph API directly with the stored token and reports every layer.
    Token values are never returned."""
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'user not found')

    token = u.get('meta_access_token') or ''
    ig_user_id = u.get('ig_user_id') or ''
    redirect_uri = f"{BACKEND_PUBLIC_URL}/api/instagram/callback"
    webhook_url = f"{BACKEND_PUBLIC_URL}/api/meta/webhook"

    runtime = {
        'appIdConfigured': bool(IG_APP_ID),
        'appSecretConfigured': bool(IG_APP_SECRET),
        'verifyTokenConfigured': bool(META_VERIFY_TOKEN),
        'graphApiVersion': 'v21.0',
        'graphHost': 'graph.instagram.com',
        'redirectUri': redirect_uri,
        'webhookUrl': webhook_url,
        'frontendUrl': FRONTEND_URL,
        'pollingEnabled': IG_POLL_ENABLED,
        'pollingIntervalSeconds': IG_POLL_INTERVAL_SECONDS,
    }

    account = {
        'connected': bool(u.get('instagramConnected')),
        'igUserId': ig_user_id,
        'username': u.get('instagramHandle'),
        'followers': u.get('instagramFollowers', 0),
        'tokenExists': bool(token),
        'tokenExpired': None,
        'tokenAppId': None,
        'scopes': [],
        'accountType': None,
        'authKind': u.get('ig_auth_kind'),
        'lastSubscribeStatus': u.get('ig_subscribe_status'),
    }

    # --- Active comment rules ---
    automations = await db.automations.find(
        {'user_id': user_id, 'status': 'active'}
    ).to_list(200)
    media_ids = await _collect_target_media_ids(u, automations) if (token and ig_user_id) else []
    rules = {
        'activeCount': len(automations),
        'activeCommentRules': sum(
            1 for a in automations
            if (a.get('trigger') or '').lower().startswith('comment:')
        ),
        'mediaIds': media_ids,
        'rulesPreview': [
            {
                'id': a.get('id'),
                'name': a.get('name'),
                'trigger': a.get('trigger'),
                'mediaId': a.get('trigger_media_id') or a.get('media_id'),
                'keyword': a.get('keyword'),
                'match': a.get('match'),
            } for a in automations[:10]
        ],
    }

    subscriptions = {'subscribedFields': [], 'raw': None, 'error': None}
    media_list = []
    comments_readability = []
    recent_errors = []

    if token and ig_user_id:
        async with httpx.AsyncClient(timeout=20) as c:
            # 1) debug_token — diagnostic-only, never affects blockerReason.
            #    IG Business Login tokens are issued by graph.instagram.com; the
            #    Facebook-graph debug_token endpoint frequently 400s for them.
            #    Try the IG host first, then fall back to FB host. Both errors
            #    are recorded but classified as 'diagnostic_only'.
            app_access_token = f'{IG_APP_ID}|{IG_APP_SECRET}'
            debug_attempts = []
            for host in ('graph.instagram.com', 'graph.facebook.com'):
                try:
                    r = await c.get(
                        f'https://{host}/debug_token',
                        params={'input_token': token,
                                'access_token': app_access_token},
                    )
                    if r.status_code == 200:
                        d = (r.json() or {}).get('data') or {}
                        account['tokenAppId'] = d.get('app_id')
                        scopes = d.get('scopes')
                        if not scopes and d.get('granular_scopes'):
                            scopes = [s.get('scope') for s in d.get('granular_scopes', []) if s.get('scope')]
                        account['scopes'] = scopes or []
                        expires_at = d.get('expires_at') or d.get('data_access_expires_at')
                        if expires_at:
                            account['tokenExpired'] = (
                                expires_at != 0 and expires_at < int(datetime.utcnow().timestamp())
                            )
                        account['tokenIsValid'] = bool(d.get('is_valid'))
                        account['debugTokenHost'] = host
                        debug_attempts = []  # success — drop earlier failures
                        break
                    else:
                        # Sanitize the error body before recording — Meta's
                        # error responses don't normally include the token,
                        # but redact defensively.
                        try:
                            body = _redact_secrets(r.json())
                        except Exception:
                            body = r.text[:300]
                        debug_attempts.append({'host': host, 'http': r.status_code, 'body': body})
                except Exception as e:
                    debug_attempts.append({'host': host, 'error': str(e)[:200]})
            for a in debug_attempts:
                # Mark diagnostic-only so UI/log readers don't treat it as a blocker.
                a['classification'] = 'diagnostic_only'
                recent_errors.append({'step': 'debug_token', **a})

            # 2) /me
            try:
                r = await c.get(
                    'https://graph.instagram.com/me',
                    params={'access_token': token,
                            'fields': 'user_id,username,account_type'},
                )
                if r.status_code == 200:
                    d = r.json() or {}
                    account['username'] = '@' + d.get('username') if d.get('username') else account['username']
                    account['accountType'] = d.get('account_type')
                else:
                    recent_errors.append({'step': 'me', 'http': r.status_code, 'body': r.text[:300]})
            except Exception as e:
                recent_errors.append({'step': 'me', 'error': str(e)[:200]})

            # 3) /{ig_user_id}/media
            try:
                r = await c.get(
                    f'https://graph.instagram.com/{ig_user_id}/media',
                    params={'access_token': token,
                            'fields': 'id,caption,comments_count,media_type,permalink,timestamp',
                            'limit': 25},
                )
                if r.status_code == 200:
                    media_list = (r.json() or {}).get('data') or []
                else:
                    recent_errors.append({'step': 'media', 'http': r.status_code, 'body': r.text[:300]})
            except Exception as e:
                recent_errors.append({'step': 'media', 'error': str(e)[:200]})

            # 4) /{media_id}/comments for each active rule's media (or fall back to first 5 media)
            check_media = list(media_ids) if media_ids else [m.get('id') for m in media_list[:5] if m.get('id')]
            count_lookup = {m.get('id'): m.get('comments_count', 0) for m in media_list}
            for mid in check_media[:10]:
                row = {'mediaId': mid, 'commentsCount': count_lookup.get(mid),
                       'commentsReturned': 0, 'readable': False,
                       'mismatch': False, 'likelyCause': None, 'http': None}
                try:
                    r = await c.get(
                        f'https://graph.instagram.com/{mid}/comments',
                        params={'access_token': token,
                                'fields': 'id,text,username,timestamp,from',
                                'limit': 25},
                    )
                    row['http'] = r.status_code
                    if r.status_code == 200:
                        items = (r.json() or {}).get('data') or []
                        row['commentsReturned'] = len(items)
                        row['readable'] = True
                        # If counter is missing, try fetching it now
                        if row['commentsCount'] is None:
                            try:
                                rm = await c.get(
                                    f'https://graph.instagram.com/{mid}',
                                    params={'access_token': token,
                                            'fields': 'comments_count'},
                                )
                                if rm.status_code == 200:
                                    row['commentsCount'] = (rm.json() or {}).get('comments_count')
                            except Exception:
                                pass
                        cc = row['commentsCount'] or 0
                        if cc > 0 and len(items) == 0:
                            row['mismatch'] = True
                            row['gated'] = True
                            row['likelyCause'] = (
                                'Meta can see comment count, but this app cannot read '
                                'comment contents. This indicates Meta access gate / '
                                'Advanced Access requirement for '
                                'instagram_business_manage_comments.'
                            )
                    else:
                        row['error'] = r.text[:300]
                        row['likelyCause'] = f'Graph API returned {r.status_code}.'
                except Exception as e:
                    row['error'] = str(e)[:200]
                comments_readability.append(row)

            # 5) /{ig_user_id}/subscribed_apps
            try:
                r = await c.get(
                    f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                    params={'access_token': token},
                )
                if r.status_code == 200:
                    body = r.json() or {}
                    fields = []
                    for app in body.get('data') or []:
                        for f in app.get('subscribed_fields') or []:
                            if f not in fields:
                                fields.append(f)
                    subscriptions['subscribedFields'] = fields
                    subscriptions['raw'] = body
                else:
                    subscriptions['error'] = f'http {r.status_code}: {r.text[:300]}'
            except Exception as e:
                subscriptions['error'] = str(e)[:200]

    # Recent processed comments
    recent_cur = db.comments.find({'user_id': user_id}).sort('created', -1).limit(20)
    recent_items = await recent_cur.to_list(20)
    recent_processed = []
    for d in recent_items:
        d.pop('_id', None)
        for k in ('created', 'processed_at'):
            if isinstance(d.get(k), datetime):
                d[k] = d[k].isoformat()
        recent_processed.append({
            'igCommentId': d.get('ig_comment_id'),
            'mediaId': d.get('media_id'),
            'commenterUsername': d.get('commenter_username'),
            'text': (d.get('text') or '')[:120],
            'source': d.get('source'),
            'matched': bool(d.get('matched')),
            'actionStatus': d.get('action_status'),
            'error': d.get('error'),
            'created': d.get('created'),
        })

    # Recent webhook events (counts by field)
    wh_cursor = db.webhook_log.find().sort('received', -1).limit(50)
    wh_recent = await wh_cursor.to_list(50)
    wh_field_counts: dict = {}
    for w in wh_recent:
        try:
            for entry in (w.get('payload') or {}).get('entry', []):
                for ch in entry.get('changes', []):
                    f = ch.get('field') or 'unknown'
                    wh_field_counts[f] = wh_field_counts.get(f, 0) + 1
                if entry.get('messaging'):
                    wh_field_counts['messaging'] = wh_field_counts.get('messaging', 0) + len(entry['messaging'])
        except Exception:
            pass

    # ---- Status logic ----
    instagram_connected = account['connected'] and account['tokenExists']
    comments_readable = any(r.get('readable') and not r.get('mismatch') and (r.get('commentsReturned', 0) > 0 or (r.get('commentsCount') or 0) == 0)
                            for r in comments_readability)
    any_mismatch = any(r.get('mismatch') for r in comments_readability)
    comments_webhook_subscribed = 'comments' in subscriptions['subscribedFields']
    has_active_comment_rule = rules['activeCommentRules'] > 0
    valid_media = bool(media_ids)

    blocker_reason = None
    if not instagram_connected:
        blocker_reason = 'instagram_not_connected'
    elif not has_active_comment_rule:
        blocker_reason = 'no_active_comment_rule'
    elif not valid_media:
        blocker_reason = 'no_media_id_resolved_for_rule'
    elif not comments_webhook_subscribed:
        blocker_reason = 'webhook_not_subscribed_to_comments_field'
    elif any_mismatch:
        blocker_reason = 'meta_access_gate_filtering_comments'
    elif not comments_readable and any(r.get('http') and r['http'] != 200 for r in comments_readability):
        blocker_reason = 'graph_api_comments_endpoint_error'

    comments_automation_ready = (
        instagram_connected and has_active_comment_rule and valid_media
        and comments_webhook_subscribed and (comments_readable or not any_mismatch)
    )

    # ---- Final classification panel (high-level, human-readable) ----
    def _verdict(ok, blocked=False):
        return 'BLOCKED' if blocked else ('OK' if ok else 'NOT_READY')
    classification = {
        'appConnection': _verdict(instagram_connected),
        'mediaMapping': _verdict(valid_media),
        'commentWebhookSubscription': _verdict(comments_webhook_subscribed),
        'graphCommentsReadability': _verdict(comments_readable, blocked=any_mismatch),
        'requiredNextStep': (
            'App Review / Advanced Access for instagram_business_manage_comments'
            if any_mismatch else (
                None if comments_automation_ready else
                ('Connect Instagram' if not instagram_connected else
                 'Add an active comment automation rule' if not has_active_comment_rule else
                 'Resolve a valid mediaId for the rule' if not valid_media else
                 'Subscribe webhook to comments field' if not comments_webhook_subscribed else
                 'Investigate Graph API errors')
            )
        ),
        'note': (
            'Token, code, media mapping, webhook subscription and polling are all OK. '
            'Comment contents are filtered at the Meta access-tier gate.'
            if any_mismatch and instagram_connected and has_active_comment_rule
               and valid_media and comments_webhook_subscribed
            else None
        ),
    }

    return {
        'runtime': runtime,
        'account': account,
        'rules': rules,
        'subscriptions': subscriptions,
        'classification': classification,
        'media': [
            {
                'id': m.get('id'),
                'mediaType': m.get('media_type'),
                'commentsCount': m.get('comments_count'),
                'permalink': m.get('permalink'),
                'timestamp': m.get('timestamp'),
                'caption': (m.get('caption') or '')[:80],
            } for m in media_list[:10]
        ],
        'commentsReadability': comments_readability,
        'polling': {
            'enabled': IG_POLL_ENABLED,
            'intervalSeconds': IG_POLL_INTERVAL_SECONDS,
        },
        'webhookFieldCountsRecent50': wh_field_counts,
        'recentProcessedComments': recent_processed,
        'recentErrors': recent_errors,
        'status': {
            'instagramConnected': instagram_connected,
            'commentsReadable': comments_readable,
            'commentsWebhookSubscribed': comments_webhook_subscribed,
            'hasActiveCommentRule': has_active_comment_rule,
            'validMediaId': valid_media,
            'commentsAutomationReady': comments_automation_ready,
            'blockerReason': blocker_reason,
        },
    }


# ---------------- DM Automation API ----------------
_DM_VALID_MODES = {'exact', 'contains', 'starts_with'}


def _dm_rule_out(d: dict) -> dict:
    if not d:
        return d
    return {
        'id': d.get('id'),
        'instagramAccountId': d.get('instagramAccountId') or d.get('igUserId'),
        'instagramAccountDbId': d.get('instagramAccountDbId') or d.get('instagram_account_id'),
        'instagramUsername': d.get('instagramUsername'),
        'name': d.get('name'),
        'keyword': d.get('keyword'),
        'matchMode': d.get('match_mode'),
        'replyText': d.get('reply_text'),
        'isActive': bool(d.get('is_active')),
        'createdAt': d.get('created_at').isoformat() if isinstance(d.get('created_at'), datetime) else d.get('created_at'),
        'updatedAt': d.get('updated_at').isoformat() if isinstance(d.get('updated_at'), datetime) else d.get('updated_at'),
    }


@api.get('/instagram/dm/rules')
async def list_dm_rules(user_id: str = Depends(get_current_active_user_id)):
    account = await getActiveInstagramAccount(user_id)
    rows = await db.dm_rules.find(_account_scoped_query(user_id, account)).sort('created_at', -1).to_list(500)
    return {'items': [_dm_rule_out(r) for r in rows], 'count': len(rows)}


@api.post('/instagram/dm/rules')
async def create_dm_rule(data: DmRuleIn, user_id: str = Depends(get_current_active_user_id)):
    import uuid as _uuid
    mode = (data.matchMode or 'contains').lower()
    if mode not in _DM_VALID_MODES:
        raise HTTPException(400, f'matchMode must be one of {sorted(_DM_VALID_MODES)}')
    if not data.name.strip() or not data.keyword.strip() or not data.replyText.strip():
        raise HTTPException(400, 'name, keyword and replyText are required')
    account = await getActiveInstagramAccount(user_id)
    ctx = _instagram_context_from_account(account)
    now = datetime.utcnow()
    doc = {
        'id': str(_uuid.uuid4()),
        'user_id': user_id,
        **ctx,
        'ig_user_id': ctx.get('igUserId') or None,
        'name': data.name.strip(),
        'keyword': data.keyword.strip(),
        'match_mode': mode,
        'reply_text': data.replyText,
        'is_active': bool(data.isActive),
        'created_at': now,
        'updated_at': now,
    }
    await db.dm_rules.insert_one(doc)
    return _dm_rule_out(doc)


@api.patch('/instagram/dm/rules/{rid}')
async def patch_dm_rule(rid: str, data: DmRulePatch, user_id: str = Depends(get_current_active_user_id)):
    account = await getActiveInstagramAccount(user_id)
    scoped = _account_scoped_query(user_id, account)
    update: dict = {'updated_at': datetime.utcnow()}
    if data.name is not None:
        update['name'] = data.name.strip()
    if data.keyword is not None:
        update['keyword'] = data.keyword.strip()
    if data.matchMode is not None:
        mode = data.matchMode.lower()
        if mode not in _DM_VALID_MODES:
            raise HTTPException(400, f'matchMode must be one of {sorted(_DM_VALID_MODES)}')
        update['match_mode'] = mode
    if data.replyText is not None:
        update['reply_text'] = data.replyText
    if data.isActive is not None:
        update['is_active'] = bool(data.isActive)
    res = await db.dm_rules.update_one({'id': rid, **scoped}, {'$set': update})
    if res.matched_count == 0:
        raise HTTPException(404, 'rule not found')
    doc = await db.dm_rules.find_one({'id': rid, **scoped})
    return _dm_rule_out(doc)


@api.delete('/instagram/dm/rules/{rid}')
async def delete_dm_rule(rid: str, user_id: str = Depends(get_current_active_user_id)):
    account = await getActiveInstagramAccount(user_id)
    res = await db.dm_rules.delete_one({'id': rid, **_account_scoped_query(user_id, account)})
    if res.deleted_count == 0:
        raise HTTPException(404, 'rule not found')
    return {'ok': True}


@api.post('/instagram/dm/test-rule')
async def test_dm_rule(data: DmTestIn, user_id: str = Depends(get_current_active_user_id)):
    """Match `text` against the user's active rules without sending anything."""
    account = await getActiveInstagramAccount(user_id)
    rules = await db.dm_rules.find(
        {**_account_scoped_query(user_id, account), 'is_active': True}
    ).to_list(200)
    matches = []
    for r in rules:
        if _dm_match(data.text, r.get('keyword') or '',
                     (r.get('match_mode') or 'contains').lower()):
            matches.append({
                'ruleId': r.get('id'),
                'name': r.get('name'),
                'keyword': r.get('keyword'),
                'matchMode': r.get('match_mode'),
                'replyText': r.get('reply_text'),
            })
    return {
        'inputText': data.text,
        'matchCount': len(matches),
        'firstMatch': matches[0] if matches else None,
        'allMatches': matches,
    }


@api.get('/instagram/dm/logs')
async def list_dm_logs(limit: int = 50, user_id: str = Depends(get_current_active_user_id)):
    limit = max(1, min(limit, 200))
    account = await getActiveInstagramAccount(user_id)
    rows = await db.dm_logs.find(_account_scoped_query(user_id, account)).sort('created', -1).limit(limit).to_list(limit)
    out = []
    for d in rows:
        d.pop('_id', None)
        created = d.get('created')
        out.append({
            'id': d.get('id'),
            'instagramAccountId': d.get('instagramAccountId') or d.get('igUserId'),
            'instagramAccountDbId': d.get('instagramAccountDbId') or d.get('instagram_account_id'),
            'instagramUsername': d.get('instagramUsername'),
            'senderId': d.get('sender_id'),
            'messageId': d.get('message_id'),
            'dedupKey': d.get('dedup_key'),
            'eventKind': d.get('event_kind'),
            'incomingText': d.get('incoming_text'),
            'matchedRuleId': d.get('matched_rule_id'),
            'matchedRuleName': d.get('matched_rule_name'),
            'replyText': d.get('reply_text'),
            'status': d.get('status'),
            'skipReason': d.get('skip_reason'),
            'error': d.get('error'),
            'source': d.get('source'),
            'isEcho': d.get('is_echo'),
            'created': created.isoformat() if isinstance(created, datetime) else created,
        })
    return {'items': out, 'count': len(out)}


@api.get('/instagram/dm/diagnostics')
async def dm_diagnostics(user_id: str = Depends(get_current_active_user_id)):
    account = await getActiveInstagramAccount(user_id)
    u = _with_instagram_account_context(await db.users.find_one({'id': user_id}) or {}, account)
    ig_user_id = account.get('instagramAccountId') or account.get('igUserId') or ''
    token = account.get('accessToken') or ''
    connected = bool(account.get('connectionValid') and token and ig_user_id)

    # messaging webhook subscription state — read live from Graph
    messaging_subscribed = False
    subscription_error = None
    if connected:
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.get(
                    f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                    params={'access_token': token},
                )
                if r.status_code == 200:
                    body = r.json() or {}
                    fields: list = []
                    for app in body.get('data') or []:
                        for f in app.get('subscribed_fields') or []:
                            fields.append(f)
                    messaging_subscribed = 'messages' in fields
                else:
                    subscription_error = f'http {r.status_code}'
        except Exception as e:
            subscription_error = str(e)[:200]

    active_rules = await db.dm_rules.count_documents({**_account_scoped_query(user_id, account), 'is_active': True})

    # Recent messaging events (count from last 50 webhook log rows)
    wh = await db.webhook_log.find().sort('received', -1).limit(50).to_list(50)
    msg_events = 0
    last_msg_at = None
    for w in wh:
        try:
            for entry in (w.get('payload') or {}).get('entry', []):
                ms = entry.get('messaging') or []
                if ms:
                    msg_events += len(ms)
                    rec = w.get('received')
                    if isinstance(rec, datetime) and (last_msg_at is None or rec > last_msg_at):
                        last_msg_at = rec
        except Exception:
            pass

    last_log = await db.dm_logs.find(_account_scoped_query(user_id, account)).sort('created', -1).limit(1).to_list(1)
    last_reply_status = last_log[0].get('status') if last_log else 'none'

    blocker_reason = None
    if not connected:
        blocker_reason = 'instagram_not_connected'
    elif not messaging_subscribed:
        blocker_reason = 'webhook_not_subscribed_to_messages_field'
    elif active_rules == 0:
        blocker_reason = 'no_active_dm_rule'

    return {
        'connected': connected,
        'igUserId': ig_user_id,
        'messagingWebhookSubscribed': messaging_subscribed,
        'subscriptionError': subscription_error,
        'activeDmRules': active_rules,
        'recentMessagingEvents': msg_events,
        'lastMessageAt': last_msg_at.isoformat() if last_msg_at else None,
        'lastReplyStatus': last_reply_status,
        'blockerReason': blocker_reason,
    }


def _redact_id(s):
    if not s or not isinstance(s, str):
        return s
    if len(s) <= 6:
        return s[:2] + '***'
    return s[:4] + '***' + s[-2:]


@api.get('/instagram/credentials/diagnostics')
async def instagram_credentials_diagnostics(user_id: str = Depends(get_current_active_user_id)):
    """Audit which credential set is wired into each integration step.
    Never returns the secret values themselves — only presence flags and the
    env-var name that supplied each one.
    """
    u = await db.users.find_one({'id': user_id})
    token = (u or {}).get('meta_access_token') or ''

    # Run debug_token using the Instagram App credential pair (which is the
    # pair that issues IG Business Login user tokens).
    token_app_id = None
    debug_token_works = False
    debug_token_error = None
    debug_token_host = None
    if token and INSTAGRAM_APP_ID and INSTAGRAM_APP_SECRET:
        app_access_token = f'{INSTAGRAM_APP_ID}|{INSTAGRAM_APP_SECRET}'
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                for host in ('graph.instagram.com', 'graph.facebook.com'):
                    try:
                        r = await c.get(
                            f'https://{host}/debug_token',
                            params={'input_token': token,
                                    'access_token': app_access_token},
                        )
                        if r.status_code == 200:
                            d = (r.json() or {}).get('data') or {}
                            token_app_id = str(d.get('app_id') or '') or None
                            debug_token_works = bool(d.get('is_valid'))
                            debug_token_host = host
                            break
                        else:
                            debug_token_error = f'{host} http {r.status_code}'
                    except Exception as e:
                        debug_token_error = f'{host} {str(e)[:120]}'
        except Exception as e:
            debug_token_error = str(e)[:200]

    warnings: list = []
    # OAuth integrity: warn loudly if the Instagram credential pair fell back
    # to the Meta App pair — that means INSTAGRAM_APP_ID/IG_APP_ID is unset and
    # we're driving Instagram OAuth with Facebook App credentials, which only
    # works if the user explicitly configured the same id+secret to be used
    # for both products on the Meta dashboard.
    if INSTAGRAM_APP_ID_SOURCE == 'META_APP_ID' or INSTAGRAM_APP_SECRET_SOURCE == 'META_APP_SECRET':
        warnings.append(
            'instagram_credentials_falling_back_to_meta_app: '
            f'INSTAGRAM_APP_ID resolved from {INSTAGRAM_APP_ID_SOURCE}, '
            f'INSTAGRAM_APP_SECRET resolved from {INSTAGRAM_APP_SECRET_SOURCE}. '
            'If the Instagram product on the Meta dashboard uses a different '
            'App ID/Secret pair than the Facebook product, set INSTAGRAM_APP_ID '
            'and INSTAGRAM_APP_SECRET (or IG_APP_ID/IG_APP_SECRET) explicitly.'
        )
    if not INSTAGRAM_APP_ID or not INSTAGRAM_APP_SECRET:
        warnings.append('instagram_app_credentials_missing')
    if not META_VERIFY_TOKEN:
        warnings.append('meta_webhook_verify_token_missing')
    if not META_WEBHOOK_APP_SECRET:
        warnings.append('meta_webhook_app_secret_missing')
    # debug_token cross-check
    matches_instagram = bool(token_app_id and INSTAGRAM_APP_ID
                             and token_app_id == INSTAGRAM_APP_ID)
    matches_meta = bool(token_app_id and META_APP_ID
                        and token_app_id == META_APP_ID)
    if token and token_app_id and not matches_instagram:
        if matches_meta:
            warnings.append(
                'token_was_issued_by_meta_app_id_not_instagram_app_id: '
                'the stored Instagram user token reports a Meta/Facebook App ID '
                'in debug_token, not the Instagram App ID. OAuth was likely '
                'driven by Facebook Login instead of Instagram Business Login.'
            )
        else:
            warnings.append(
                f'token_app_id_unknown_app: tokenAppId={token_app_id} matches '
                'neither INSTAGRAM_APP_ID nor META_APP_ID. Reconnect Instagram.'
            )

    return {
        'oauth': {
            'usesInstagramAppId': bool(INSTAGRAM_APP_ID),
            'instagramAppIdConfigured': bool(INSTAGRAM_APP_ID),
            'instagramAppSecretConfigured': bool(INSTAGRAM_APP_SECRET),
            'authorizeUrlClientIdSource': INSTAGRAM_APP_ID_SOURCE,
            'tokenExchangeSecretSource': INSTAGRAM_APP_SECRET_SOURCE,
            'authorizeHost': 'api.instagram.com',
            'tokenExchangeHost': 'api.instagram.com / graph.instagram.com',
        },
        'webhook': {
            'verifyTokenConfigured': bool(META_VERIFY_TOKEN),
            'verifyTokenSource': META_VERIFY_TOKEN_SOURCE,
            'signatureSecretSource': META_WEBHOOK_APP_SECRET_SOURCE,
            'signatureValidationEnabled': bool(META_WEBHOOK_APP_SECRET),
            'signatureEnforceMode': META_WEBHOOK_HMAC_ENFORCE,
            'metaAppIdConfigured': bool(META_APP_ID),
            'metaAppSecretConfigured': bool(META_APP_SECRET),
            'callbackUrl': f'{BACKEND_PUBLIC_URL}/api/instagram/webhook',
        },
        'graph': {
            'host': 'graph.instagram.com',
            'version': 'v21.0',
            'tokenSource': 'users.meta_access_token (per-user)',
        },
        'debugToken': {
            'appAccessTokenSource': f'{INSTAGRAM_APP_ID_SOURCE}|{INSTAGRAM_APP_SECRET_SOURCE}',
            'debugTokenWorks': debug_token_works,
            'debugTokenHost': debug_token_host,
            'debugTokenError': debug_token_error,
            'tokenAppId': token_app_id,
            'matchesInstagramAppId': matches_instagram,
            'matchesMetaAppId': matches_meta,
            'instagramAppIdSnapshot': INSTAGRAM_APP_ID[-4:] if INSTAGRAM_APP_ID else None,
            'metaAppIdSnapshot': META_APP_ID[-4:] if META_APP_ID else None,
        },
        'warnings': warnings,
    }


@api.get('/instagram/dm/debug-latest')
async def dm_debug_latest(user_id: str = Depends(get_current_active_user_id)):
    """Self-diagnostic: reads live DB collections + Graph subscription state.
    Never exposes tokens or full webhook payloads. Sender IDs are partially
    redacted. Used by the DM Automation page "Run DM debug" button.
    """
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'user not found')
    ig_user_id = u.get('ig_user_id') or ''
    token = u.get('meta_access_token') or ''
    connected = bool(u.get('instagramConnected') and token and ig_user_id)

    messaging_subscribed = False
    subscribed_fields_list: list = []
    sub_error = None
    if connected:
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.get(
                    f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                    params={'access_token': token},
                )
                if r.status_code == 200:
                    body = r.json() or {}
                    for app in body.get('data') or []:
                        for f in app.get('subscribed_fields') or []:
                            subscribed_fields_list.append(f)
                    messaging_subscribed = 'messages' in subscribed_fields_list
                else:
                    sub_error = f'http {r.status_code}'
        except Exception as e:
            sub_error = str(e)[:200]

    rules_rows = await db.dm_rules.find(
        {'user_id': user_id, 'is_active': True}
    ).sort('created_at', -1).to_list(50)
    active_rules = [{
        'id': r.get('id'),
        'name': r.get('name'),
        'keyword': r.get('keyword'),
        'matchMode': r.get('match_mode'),
        'isActive': r.get('is_active'),
        'createdAt': r.get('created_at').isoformat()
                     if isinstance(r.get('created_at'), datetime) else r.get('created_at'),
    } for r in rules_rows]

    # Recent webhook events: scan last 50 webhook_log rows, surface only the
    # messaging events that belong to this IG account, with safe summaries.
    wh_rows = await db.webhook_log.find().sort('received', -1).limit(50).to_list(50)
    recent_events = []
    for w in wh_rows:
        try:
            payload = w.get('payload') or {}
            obj_kind = payload.get('object')
            received = w.get('received')
            for entry in payload.get('entry', []):
                entry_id = entry.get('id')
                if entry_id and entry_id != ig_user_id and entry_id != u.get('fb_page_id'):
                    continue
                changes = entry.get('changes') or []
                fields = sorted({c.get('field') for c in changes if c.get('field')})
                ms = entry.get('messaging') or []
                if not ms and not fields:
                    continue
                if ms:
                    for ev in ms:
                        cls = _classify_messaging_event(ev)
                        msg_text = cls['text'] or ''
                        recent_events.append({
                            'createdAt': received.isoformat()
                                         if isinstance(received, datetime) else received,
                            'object': obj_kind,
                            'fields': fields,
                            'hasMessagingArray': True,
                            'messagingItemShape': cls['item_keys'],
                            'messagingItemKeys': cls['item_keys'],
                            'messageKeys': cls['message_keys'],
                            'senderPresent': bool(cls['sender_id']),
                            'senderIdPresent': bool(cls['sender_id']),
                            'senderIdRedacted': _redact_id(cls['sender_id']),
                            'recipientPresent': bool(cls['recipient_id']),
                            'recipientIdPresent': bool(cls['recipient_id']),
                            'hasMessage': cls['has_message'],
                            'messageIdPresent': bool(cls['message_id']),
                            'messageTextPresent': bool(msg_text),
                            'hasRead': cls['has_read'],
                            'hasDelivery': cls['has_delivery'],
                            'hasPostback': cls['has_postback'],
                            'hasReaction': cls['has_reaction'],
                            'hasReferral': cls['has_referral'],
                            'hasAttachments': cls['has_attachments'],
                            'isEcho': cls['is_echo'],
                            'eventKind': cls['kind'],
                            'textPreview': (msg_text[:40] if msg_text else ''),
                        })
                else:
                    recent_events.append({
                        'createdAt': received.isoformat()
                                     if isinstance(received, datetime) else received,
                        'object': obj_kind,
                        'fields': fields,
                        'hasMessagingArray': False,
                        'messagingItemShape': [],
                        'messagingItemKeys': [],
                        'messageKeys': [],
                        'senderPresent': False,
                        'senderIdPresent': False,
                        'recipientPresent': False,
                        'recipientIdPresent': False,
                        'hasMessage': False,
                        'messageIdPresent': False,
                        'messageTextPresent': False,
                        'hasRead': False,
                        'hasDelivery': False,
                        'hasPostback': False,
                        'hasReaction': False,
                        'hasReferral': False,
                        'hasAttachments': False,
                        'isEcho': False,
                        'eventKind': 'unknown',
                        'textPreview': '',
                    })
        except Exception:
            continue
    recent_events = recent_events[:20]

    log_rows = await db.dm_logs.find({'user_id': user_id}).sort('created', -1).limit(20).to_list(20)
    recent_logs = []
    for d in log_rows:
        created = d.get('created')
        recent_logs.append({
            'createdAt': created.isoformat() if isinstance(created, datetime) else created,
            'senderId': _redact_id(d.get('sender_id')),
            'messageId': d.get('message_id'),
            'dedupKey': d.get('dedup_key'),
            'eventKind': d.get('event_kind'),
            'incomingText': (d.get('incoming_text') or '')[:120] if d.get('incoming_text') else None,
            'matchedRuleId': d.get('matched_rule_id'),
            'matchedRuleName': d.get('matched_rule_name'),
            'status': d.get('status'),
            'skipReason': d.get('skip_reason'),
            'error': d.get('error'),
        })

    # Build lastDecision from the most-recent messaging webhook event + most-recent log
    last_msg_event = next((e for e in recent_events if e.get('hasMessagingArray')), None)
    last_text_event = next(
        (e for e in recent_events
         if e.get('hasMessagingArray') and e.get('eventKind') == 'message_text'),
        None,
    )
    last_log = recent_logs[0] if recent_logs else None
    webhook_received = bool(last_msg_event)
    message_parsed = bool(last_text_event and last_text_event.get('senderIdPresent')
                          and last_text_event.get('messageTextPresent'))
    rule_matched = bool(last_log and last_log.get('matchedRuleId'))
    send_attempted = bool(last_log and last_log.get('status') in ('replied', 'failed'))
    reply_sent = bool(last_log and last_log.get('status') == 'replied')

    # Classification
    blocker = None
    fix = None
    if not connected:
        blocker = 'instagram_not_connected'
        fix = 'Reconnect Instagram from Settings.'
    elif not messaging_subscribed:
        blocker = 'webhook_not_subscribed_to_messages_field'
        fix = 'POST /api/instagram/dm/resubscribe to subscribe the messages field on this IG account.'
    elif not active_rules:
        blocker = 'no_active_dm_rule'
        fix = 'Create or activate a rule on the DM Automation page.'
    elif not webhook_received:
        blocker = 'no_messaging_webhook_received'
        fix = 'Send a test DM from a different IG account. If still nothing arrives, the IG account is not subscribed for messages — call resubscribe.'
    elif not last_text_event:
        kinds = sorted({e.get('eventKind') for e in recent_events
                        if e.get('hasMessagingArray') and e.get('eventKind')})
        blocker = 'no_message_text_event'
        fix = ('Meta delivered messaging events but none were message_text. '
               f'Observed kinds: {kinds}. '
               'If you only see read/delivery/reaction, send a brand-new text DM '
               'from another IG account that has not previously messaged you. '
               'If you only see message_echo, the test DM was sent FROM the '
               'connected business account itself.')
    elif not message_parsed:
        blocker = 'webhook_payload_shape_mismatch'
        fix = 'A message_text event arrived but sender/text fields were missing. Inspect recentWebhookEvents.messagingItemKeys / messageKeys.'
    elif last_log and last_log.get('skipReason') == 'no_rule_match':
        blocker = 'rule_did_not_match_text'
        fix = f'Incoming text did not satisfy any active rule. Check keyword + matchMode against text="{(last_log.get("incomingText") or "")[:40]}".'
    elif last_log and last_log.get('skipReason') == 'duplicate':
        blocker = 'duplicate_event'
        fix = 'This was a webhook replay. Send a fresh DM.'
    elif last_log and last_log.get('skipReason') in ('echo', 'self_message'):
        blocker = 'echo_or_self_message'
        fix = 'The DM came from the connected business account itself or was an echo. Send from a different IG account.'
    elif last_log and last_log.get('skipReason') in ('missing_sender', 'missing_text'):
        blocker = f'webhook_{last_log.get("skipReason")}'
        fix = 'Meta delivered an event without required fields. See recentWebhookEvents.'
    elif last_log and last_log.get('status') == 'failed':
        blocker = 'graph_send_error'
        fix = f'Graph send failed: {last_log.get("error") or "unknown"}. Common causes: 24h messaging window closed, instagram_business_manage_messages permission missing, or invalid recipient.'
    elif rule_matched and reply_sent:
        blocker = None
        fix = 'Working — last DM was replied to.'

    # ---------------- Identity panel ----------------
    graph_me_id = None
    graph_username = None
    graph_account_type = None
    graph_me_error = None
    if connected:
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                me = await c.get(
                    'https://graph.instagram.com/me',
                    params={'fields': 'id,username,account_type',
                            'access_token': token},
                )
                if me.status_code == 200:
                    mb = me.json() or {}
                    graph_me_id = mb.get('id')
                    graph_username = mb.get('username')
                    graph_account_type = mb.get('account_type')
                else:
                    graph_me_error = f'http {me.status_code}'
        except Exception as e:
            graph_me_error = str(e)[:200]

    # Scan webhook_log GLOBALLY (not filtered to this user) to find what IG
    # account ids Meta is actually addressing in `entry.id` / `recipient.id`.
    wh_all = await db.webhook_log.find().sort('received', -1).limit(50).to_list(50)
    entry_ids: list = []
    recipient_ids: list = []
    sender_ids: list = []
    unmapped_count = 0
    messaging_event_count = 0
    for w in wh_all:
        try:
            for entry in (w.get('payload') or {}).get('entry', []):
                eid = entry.get('id')
                if eid:
                    entry_ids.append(eid)
                ms = entry.get('messaging') or []
                for ev in ms:
                    messaging_event_count += 1
                    rid = (ev.get('recipient') or {}).get('id')
                    sid = (ev.get('sender') or {}).get('id')
                    if rid:
                        recipient_ids.append(rid)
                    if sid:
                        sender_ids.append(sid)
                # Unmapped: entry.id doesn't match this user's ig_user_id/fb_page_id
                # and no recipient.id in messaging items matches either.
                matched = (eid == ig_user_id) or (eid == u.get('fb_page_id'))
                if not matched:
                    rids = {(ev.get('recipient') or {}).get('id') for ev in ms}
                    if not (ig_user_id in rids or u.get('fb_page_id') in rids):
                        unmapped_count += len(ms) or 1
        except Exception:
            continue

    entry_ids_unique = sorted(set(entry_ids))
    recipient_ids_unique = sorted(set(recipient_ids))
    sender_ids_unique = sorted(set(sender_ids))

    id_match = bool(graph_me_id and ig_user_id and graph_me_id == ig_user_id)
    mismatch_reason = None
    if not graph_me_id:
        mismatch_reason = graph_me_error or 'graph_me_unavailable'
    elif graph_me_id != ig_user_id:
        mismatch_reason = 'graph_me_id_does_not_match_db_ig_user_id'
    elif entry_ids_unique and ig_user_id not in entry_ids_unique \
            and ig_user_id not in recipient_ids_unique \
            and (not u.get('fb_page_id') or u['fb_page_id'] not in entry_ids_unique):
        mismatch_reason = 'webhook_entry_id_does_not_match_connected_ig_user_id'

    identity = {
        'dbIgUserId': ig_user_id,
        'dbIgUserIdRedacted': _redact_id(ig_user_id),
        'graphMeId': graph_me_id,
        'graphMeIdRedacted': _redact_id(graph_me_id),
        'graphUsername': graph_username,
        'graphAccountType': graph_account_type,
        'graphMeError': graph_me_error,
        'subscribedAppsCheckedForIgUserId': ig_user_id,
        'subscribedAppsCheckedForIgUserIdRedacted': _redact_id(ig_user_id),
        'latestWebhookEntryIds': [_redact_id(x) for x in entry_ids_unique[:10]],
        'latestWebhookRecipientIds': [_redact_id(x) for x in recipient_ids_unique[:10]],
        'latestWebhookSenderIds': [_redact_id(x) for x in sender_ids_unique[:10]],
        'idMatch': id_match,
        'mismatchReason': mismatch_reason,
    }

    # ---------------- Webhook config panel ----------------
    webhook_config = {
        'expectedWebhookPath': '/api/instagram/webhook',
        'verifyTokenConfigured': bool(META_VERIFY_TOKEN),
        'appIdConfigured': bool(IG_APP_ID),
        'appSecretConfigured': bool(IG_APP_SECRET),
        'signatureValidationEnabled': bool(META_WEBHOOK_APP_SECRET),
        'signatureEnforceMode': META_WEBHOOK_HMAC_ENFORCE,
        'graphApiVersion': 'v21.0',
        'graphHost': 'graph.instagram.com',
        'callbackUrlUsedByRuntime': f'{BACKEND_PUBLIC_URL}/api/instagram/webhook',
        'oauthRedirectUri': f'{BACKEND_PUBLIC_URL}/api/instagram/callback',
        'backendPublicUrl': BACKEND_PUBLIC_URL,
        'webhookEventsStored': len(wh_all),
    }

    # ---------------- Processor panel ----------------
    dm_logs_for_user = await db.dm_logs.count_documents({'user_id': user_id})
    dm_logs_global = await db.dm_logs.count_documents({})
    skip_reasons_recent = sorted({l.get('skipReason') for l in recent_logs if l.get('skipReason')})
    processor = {
        'webhookEventsCount': messaging_event_count,
        'dmLogsForCurrentUser': dm_logs_for_user,
        'dmLogsGlobalRecent': dm_logs_global,
        'unmappedMessagingEvents': unmapped_count,
        'recentSkipReasons': list(skip_reasons_recent),
    }

    # ---------------- lastDecision (smarter blocker order) ----------------
    blocker = None
    fix = None
    if not connected:
        blocker = 'instagram_not_connected'
        fix = 'Reconnect Instagram from Settings.'
    elif graph_me_error:
        blocker = 'instagram_token_invalid_or_expired'
        fix = ('Graph /me could not validate the stored Instagram token. '
               'Disconnect and reconnect Instagram from Settings so the app '
               'stores a fresh token, ig_user_id, and webhook subscription.')
    elif mismatch_reason == 'graph_me_id_does_not_match_db_ig_user_id':
        blocker = 'id_mismatch'
        fix = ('The access_token in DB returns a different IG user id from '
               'the stored ig_user_id. Disconnect and reconnect Instagram so '
               'the token, ig_user_id, and webhook subscription all reference '
               'the same account.')
    elif mismatch_reason == 'webhook_entry_id_does_not_match_connected_ig_user_id':
        blocker = 'webhook_account_mismatch'
        fix = ('Webhook events are arriving for a different IG account id than '
               'the one stored in users.ig_user_id. Confirm the same Meta App '
               'is used for OAuth and webhook subscription, and that the same '
               'IG account id was passed to subscribed_apps.')
    elif not messaging_subscribed:
        blocker = 'webhook_not_subscribed_to_messages_field'
        fix = 'POST /api/instagram/dm/resubscribe to subscribe the messages field on this IG account.'
    elif not active_rules:
        blocker = 'no_active_dm_rule'
        fix = 'Create or activate a rule on the DM Automation page.'
    elif not webhook_received:
        blocker = 'no_messaging_webhook_received'
        fix = 'Send a test DM from a different IG account. If still nothing arrives, the IG account is not subscribed for messages — call resubscribe.'
    elif messaging_event_count > 0 and dm_logs_for_user == 0:
        blocker = 'processor_not_logging_events'
        fix = ('Webhook messaging events exist but the processor wrote zero '
               'dm_logs rows for this user. Likely user mapping failure '
               '(entry.id and recipient.id never matched users.ig_user_id) '
               'or the events were stored before the logging change deployed. '
               f'unmappedMessagingEvents={unmapped_count}. Send a fresh DM '
               'and re-run debug.')
    elif not last_text_event:
        kinds = sorted({e.get('eventKind') for e in recent_events
                        if e.get('hasMessagingArray') and e.get('eventKind')})
        blocker = 'no_message_text_event'
        fix = ('Meta delivered messaging events but none were message_text. '
               f'Observed kinds: {kinds}. Verify ID mapping and webhook payload '
               'shape first. In Development mode, app roles may affect some tests, '
               'but do not assume this is the cause until ID mapping and '
               'payload handling are proven correct.')
    elif not message_parsed:
        blocker = 'webhook_payload_shape_mismatch'
        fix = 'A message_text event arrived but sender/text fields were missing. Inspect recentWebhookEvents.messagingItemKeys / messageKeys.'
    elif last_log and last_log.get('skipReason') == 'no_rule_match':
        blocker = 'message_text_received_but_no_rule_match'
        fix = f'Incoming text did not satisfy any active rule. Check keyword + matchMode against text="{(last_log.get("incomingText") or "")[:40]}".'
    elif last_log and last_log.get('skipReason') == 'duplicate':
        blocker = 'duplicate_event'
        fix = 'This was a webhook replay. Send a fresh DM.'
    elif last_log and last_log.get('skipReason') in ('echo', 'self_message'):
        blocker = 'echo_or_self_message'
        fix = 'The DM came from the connected business account itself or was an echo. Send from a different IG account.'
    elif last_log and last_log.get('skipReason') in ('missing_sender', 'missing_text'):
        blocker = f'webhook_{last_log.get("skipReason")}'
        fix = 'Meta delivered an event without required fields. See recentWebhookEvents.'
    elif last_log and last_log.get('status') == 'failed':
        blocker = 'send_api_failed'
        fix = f'Graph send failed: {last_log.get("error") or "unknown"}.'
    elif rule_matched and reply_sent:
        blocker = None
        fix = 'replied_successfully — last DM was replied to.'

    return {
        'connected': connected,
        'igUserId': ig_user_id,
        'messagingWebhookSubscribed': messaging_subscribed,
        'subscribedFields': subscribed_fields_list,
        'subscriptionError': sub_error,
        'identity': identity,
        'webhookConfig': webhook_config,
        'processor': processor,
        'activeRules': active_rules,
        'recentWebhookEvents': recent_events,
        'recentDmLogs': recent_logs,
        'lastDecision': {
            'webhookReceived': webhook_received,
            'messageParsed': message_parsed,
            'ruleMatched': rule_matched,
            'sendAttempted': send_attempted,
            'replySent': reply_sent,
            'blocker': blocker,
            'fix': fix,
        },
    }


@api.post('/instagram/dm/resubscribe')
async def dm_resubscribe(user_id: str = Depends(get_current_active_user_id)):
    """Re-subscribe the connected IG account to the messaging webhook fields.
    Calls POST /{ig_user_id}/subscribed_apps with the messaging field set,
    then GETs the current state and returns it.
    """
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'user not found')
    ig_user_id = u.get('ig_user_id') or ''
    token = u.get('meta_access_token') or ''
    if not (ig_user_id and token):
        raise HTTPException(400, 'instagram not connected')

    fields = 'messages,messaging_postbacks,messaging_seen,message_reactions'
    post_status = None
    post_body = None
    get_status = None
    subscribed_fields_list: list = []
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            pr = await c.post(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={'subscribed_fields': fields, 'access_token': token},
            )
            post_status = pr.status_code
            try:
                post_body = pr.json()
            except Exception:
                post_body = {'raw': pr.text[:300]}
            gr = await c.get(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={'access_token': token},
            )
            get_status = gr.status_code
            if gr.status_code == 200:
                body = gr.json() or {}
                for app in body.get('data') or []:
                    for f in app.get('subscribed_fields') or []:
                        subscribed_fields_list.append(f)
    except Exception as e:
        raise HTTPException(502, f'graph error: {str(e)[:200]}')

    # Cross-check: ask /me with the same token and confirm the IG account id
    # we just subscribed actually matches the token's IG identity.
    graph_me_id = None
    graph_me_error = None
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            mr = await c.get('https://graph.instagram.com/me',
                             params={'fields': 'id', 'access_token': token})
            if mr.status_code == 200:
                graph_me_id = (mr.json() or {}).get('id')
            else:
                graph_me_error = f'http {mr.status_code}'
    except Exception as e:
        graph_me_id = None
        graph_me_error = str(e)[:200]

    id_match = bool(graph_me_id and graph_me_id == ig_user_id)
    messages_subscribed = 'messages' in subscribed_fields_list
    blocker = None
    fix = None
    if graph_me_error:
        blocker = 'instagram_token_invalid_or_expired'
        fix = ('Graph /me could not validate the stored Instagram token. '
               'Disconnect and reconnect Instagram from Settings, then run '
               'resubscribe again.')
    elif graph_me_id and not id_match:
        blocker = 'id_mismatch'
        fix = ('The stored Instagram token resolves to a different IG user id. '
               'Disconnect and reconnect Instagram from Settings.')
    elif not messages_subscribed:
        blocker = 'webhook_not_subscribed_to_messages_field'
        fix = 'Graph did not confirm the messages field. Check postStatus/getStatus and retry after reconnecting if needed.'

    return {
        'igUserIdUsed': ig_user_id,
        'igUserId': ig_user_id,
        'graphMeId': graph_me_id,
        'graphMeError': graph_me_error,
        'idMatch': id_match,
        'requestedFields': fields.split(','),
        'postStatus': post_status,
        'postResponse': _redact_secrets(post_body) if isinstance(post_body, (dict, list)) else post_body,
        'getStatus': get_status,
        'subscribedFields': subscribed_fields_list,
        'messagesSubscribed': messages_subscribed,
        'ok': bool(id_match and messages_subscribed and not graph_me_error),
        'blocker': blocker,
        'fix': fix,
    }


async def _backfill_account_profile_picture(account: dict) -> dict:
    """Phase 2.18H: lazy-fetch missing profile_picture_url from Instagram
    Graph. Each connected Instagram account has its own access token —
    the secondary account may have been linked without a profile-photo
    refresh, leaving the avatar field null. We fetch on demand (one
    Graph round-trip per account, in parallel via gather upstream) and
    persist the result so subsequent calls hit the DB only."""
    if not isinstance(account, dict):
        return account
    if account.get('profilePictureUrl') or account.get('profile_picture_url'):
        return account
    token = account.get('accessToken') or ''
    ig_id = account.get('instagramAccountId') or account.get('igUserId') or ''
    if not (token and ig_id):
        return account
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(
                f'https://graph.instagram.com/{ig_id}',
                params={
                    'access_token': token,
                    'fields': 'profile_picture_url,username,account_type',
                },
            )
            if r.status_code != 200:
                return account
            body = r.json() or {}
            pic = body.get('profile_picture_url') or None
            if not pic:
                return account
            update_doc = {
                'profilePictureUrl': pic,
                'profile_picture_url': pic,
                'updatedAt': datetime.utcnow(),
            }
            if body.get('username') and not account.get('username'):
                update_doc['username'] = body['username']
            if body.get('account_type') and not account.get('accountType'):
                update_doc['accountType'] = body['account_type']
            await db.instagram_accounts.update_one(
                {'id': account.get('id')},
                {'$set': update_doc},
            )
            account = {**account, **update_doc}
    except Exception:
        # Stay silent — failing to backfill the avatar must never fail
        # the parent /instagram/accounts request.
        pass
    return account


@api.get('/instagram/accounts')
async def instagram_accounts(user_id: str = Depends(get_current_active_user_id)):
    user_doc = await db.users.find_one({'id': user_id}) or {}
    # Phase 2.18T: reconcile users.* legacy flags FIRST so the rest of
    # this endpoint operates on a consistent view of the world.
    user_doc = await _reconcile_user_instagram_flags(user_id, user_doc) or user_doc
    if user_doc:
        await _sync_user_instagram_account_doc(user_doc)
    try:
        active_account = await getActiveInstagramAccount(user_id)
        active_account_id = active_account.get('id') or ''
    except HTTPException as e:
        if e.status_code != 400:
            raise
        active_account_id = user_doc.get('active_instagram_account_id') or ''
    await _cleanup_extra_instagram_accounts_for_single_account_plan(user_id, active_account_id)
    rows = await db.instagram_accounts.find({
        '$or': [{'userId': user_id}, {'user_id': user_id}],
    }).sort('updatedAt', -1).to_list(100)
    rows = [row for row in rows if _is_public_switchable_instagram_account(row)]
    # Phase 2.18H: parallel-backfill missing profile pictures for every
    # account that lacks one. Connected accounts without a saved avatar
    # show a generic placeholder otherwise. Each fetch is independent
    # so gather() runs them concurrently.
    missing = [row for row in rows if not (row.get('profilePictureUrl') or row.get('profile_picture_url'))]
    if missing:
        refreshed = await asyncio.gather(*(_backfill_account_profile_picture(r) for r in missing))
        by_id = {r.get('id'): r for r in refreshed}
        rows = [by_id.get(r.get('id'), r) for r in rows]
    return {
        'accounts': [_instagram_account_public_row(row, active_account_id) for row in rows],
        'activeInstagramAccountId': active_account_id or None,
        'count': len(rows),
    }


@api.post('/instagram/accounts/{account_id}/activate')
async def instagram_account_activate(account_id: str, user_id: str = Depends(get_current_active_user_id)):
    user_doc = await db.users.find_one({'id': user_id})
    if user_doc:
        await _sync_user_instagram_account_doc(user_doc)
    account = await db.instagram_accounts.find_one({'id': account_id, 'userId': user_id})
    if not account:
        raise HTTPException(404, 'Instagram account not found')
    token = account.get('accessToken') or ''
    instagram_account_id = account.get('instagramAccountId') or account.get('igUserId') or ''
    if not (token and instagram_account_id):
        raise HTTPException(400, 'Instagram account is missing token or id')
    if account.get('refreshStatus') == 'expired':
        raise HTTPException(400, 'Instagram token expired. Reconnect this account.')
    now = datetime.utcnow()
    await db.instagram_accounts.update_many(
        {'userId': user_id},
        {'$set': {'isCurrent': False, 'updatedAt': now}},
    )
    await db.instagram_accounts.update_one(
        {'id': account_id, 'userId': user_id},
        {'$set': {'isCurrent': True, 'isActive': True, 'updatedAt': now}},
    )
    await db.users.update_one(
        {'id': user_id},
        {'$set': {
            'active_instagram_account_id': account_id,
            'instagramConnected': True,
            'instagram_connection_valid': bool(account.get('connectionValid')),
            'instagramConnectionValid': bool(account.get('connectionValid')),
            'instagram_connection_blocker': None if account.get('connectionValid') else 'selected_account_invalid',
            'instagramHandle': account.get('username') or '',
            'instagram_account_type': account.get('accountType'),
            'ig_user_id': instagram_account_id,
            'meta_access_token': token,
            'instagramTokenSource': account.get('tokenSource'),
            'instagram_token_source': account.get('tokenSource'),
            'tokenExpiresAt': _parse_graph_datetime(account.get('tokenExpiresAt')),
            'instagram_token_expires_at': _parse_graph_datetime(account.get('tokenExpiresAt')),
            'lastRefreshedAt': _parse_graph_datetime(account.get('lastRefreshedAt')),
            'refreshStatus': account.get('refreshStatus'),
            'refreshAttempts': int(account.get('refreshAttempts') or 0),
            'updated': now,
        }},
    )
    await invalidate_dashboard_summary(user_id)
    refreshed = await db.instagram_accounts.find_one({'id': account_id}) or account
    return {'ok': True, 'account': _instagram_account_public_row(refreshed, account_id)}


@api.post('/cron/refresh-instagram-tokens')
async def cron_refresh_instagram_tokens(request: Request):
    if not _cron_secret_is_valid(_cron_secret_from_request(request)):
        raise HTTPException(403, 'Invalid cron secret')
    return await runInstagramTokenRefreshCron()


@api.get('/instagram/token-refresh/status')
async def instagram_token_refresh_status(user_id: str = Depends(get_current_active_user_id)):
    user_doc = await db.users.find_one({'id': user_id})
    if user_doc:
        await _sync_user_instagram_account_doc(user_doc)
    rows = await db.instagram_accounts.find({'userId': user_id}).sort('updatedAt', -1).to_list(100)
    return {
        'accounts': [_token_refresh_public_row(row) for row in rows],
        'count': len(rows),
    }


# ---------------- root ----------------
@api.get('/')
async def root():
    return {'app': 'mychat', 'status': 'ok'}


@app.get('/r/{short_code}')
async def tracked_link_redirect(short_code: str, request: Request):
    link = await db.tracked_links.find_one({'shortCode': short_code})
    if not link:
        raise HTTPException(404, 'Tracked link not found')

    now = datetime.utcnow()
    expires_at = _parse_graph_datetime(link.get('expiresAt')) if link.get('expiresAt') else None
    if not link.get('isActive') or (expires_at and expires_at <= now):
        raise HTTPException(410, 'Tracked link is expired or inactive')

    original_url = (link.get('originalUrl') or '').strip()
    if not _is_valid_original_url(original_url):
        raise HTTPException(410, 'Tracked link destination is unavailable')

    client_ip = request.client.host if request.client else ''
    user_agent = request.headers.get('user-agent', '')
    referrer = request.headers.get('referer') or request.headers.get('referrer') or ''
    event_id = secrets.token_urlsafe(12)
    link_reservation = await reserve_usage_limit(
        str(link.get('user_id') or link.get('userId') or ''),
        'monthly_links_clicked_limit',
        increment=1,
        instagram_account_id=link.get('instagramAccountId') or link.get('igUserId'),
        source='link_redirect',
        automation_id=link.get('automation_id') or link.get('ruleId'),
        ig_comment_id=link.get('relatedCommentId'),
        action_id=f'{short_code}:{event_id}',
    )
    if not link_reservation.get('allowed') or (
        link_reservation.get('exceeded') and not link_reservation.get('fail_open')
    ):
        raise HTTPException(402, 'usage_limit_exceeded')

    event = {
        'id': event_id,
        'shortCode': short_code,
        'trackedLinkId': link.get('id'),
        'user_id': link.get('user_id') or link.get('userId'),
        'userId': link.get('userId') or link.get('user_id'),
        'instagramAccountId': link.get('instagramAccountId') or link.get('igUserId'),
        'instagramAccountDbId': link.get('instagramAccountDbId') or link.get('instagram_account_id'),
        'instagram_account_id': link.get('instagram_account_id') or link.get('instagramAccountDbId'),
        'igUserId': link.get('igUserId') or link.get('instagramAccountId'),
        'ruleId': link.get('ruleId') or link.get('automation_id'),
        'automation_id': link.get('automation_id') or link.get('ruleId'),
        'instagramUserId': link.get('instagramUserId') or link.get('recipient_id'),
        'recipient_id': link.get('recipient_id') or link.get('instagramUserId'),
        'relatedCommentId': link.get('relatedCommentId'),
        'ipHash': _hash_tracking_value(client_ip),
        'userAgentHash': _hash_tracking_value(user_agent),
        'referrerHash': _hash_tracking_value(referrer),
        'referrerPresent': bool(referrer),
        'clickedAt': now,
        'createdAt': now,
        'created': now,
    }
    await db.link_click_events.insert_one(event)
    update = {
        'lastClickedAt': now,
        'updatedAt': now,
        'updated': now,
    }
    if not link.get('firstClickedAt'):
        update['firstClickedAt'] = now
    await db.tracked_links.update_one(
        {'shortCode': short_code},
        {'$inc': {'clicksCount': 1}, '$set': update},
    )
    await confirm_usage_reservation(
        link_reservation,
        user_id=event.get('user_id'),
        event_type='link_clicked',
        instagram_account_id=event.get('instagramAccountId'),
        automation_id=event.get('automation_id') or event.get('ruleId'),
        comment_id=event.get('relatedCommentId'),
        metadata={
            'short_code': short_code,
            'tracked_link_id': event.get('trackedLinkId'),
        },
    )
    return RedirectResponse(original_url, status_code=302)


app.include_router(api)


# ---------------- WebSocket ----------------
@app.websocket('/ws/{user_id}')
async def websocket_endpoint(ws: WebSocket, user_id: str, token: str = Query(...)):
    try:
        uid = decode_token(token)
        if uid != user_id:
            await ws.close(code=4003)
            return
    except Exception:
        await ws.close(code=4001)
        return

    await ws_manager.connect(user_id, ws)
    try:
        while True:
            data = await ws.receive_json()
            msg_type = data.get('type')

            if msg_type == 'message':
                conv_id = data.get('conv_id')
                text = (data.get('text') or '').strip()
                if not conv_id or not text:
                    continue
                import uuid as _uuid
                conv = await db.conversations.find_one({'id': conv_id, 'user_id': user_id})
                if not conv:
                    continue
                # Try Graph API first so we can report delivery status
                user_doc = await db.users.find_one({'id': user_id})
                ig_recipient = conv.get('contact', {}).get('ig_id')
                delivered = False
                if user_doc and user_doc.get('instagramConnected') and ig_recipient:
                    delivered = await send_ig_dm(
                        user_doc.get('meta_access_token', ''),
                        user_doc.get('ig_user_id', ''),
                        ig_recipient, text
                    )
                msg = {'id': str(_uuid.uuid4()), 'from': 'me', 'text': text,
                       'time': datetime.utcnow().strftime('%I:%M %p'),
                       'delivered': delivered}
                await db.conversations.update_one(
                    {'id': conv_id},
                    {'$push': {'messages': msg},
                     '$set': {'lastMessage': text, 'time': 'now', 'unread': 0}}
                )
                await ws_manager.send(user_id, {'type': 'message', 'conv_id': conv_id, 'message': msg})

            elif msg_type == 'ping':
                await ws_manager.send(user_id, {'type': 'pong'})

    except WebSocketDisconnect:
        ws_manager.disconnect(user_id)
_RESOLVED_CORS_ORIGINS = _resolved_cors_origins()
logger.info('cors_origins_configured production=%s count=%s sample=%s',
            IS_PRODUCTION, len(_RESOLVED_CORS_ORIGINS),
            ','.join(_RESOLVED_CORS_ORIGINS[:3]))
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=_RESOLVED_CORS_ORIGINS,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.middleware('http')
async def response_timing_middleware(request, call_next):
    """Apply security headers and measure per-request timing.

    Phase 2.13C: Adds X-Response-Time header and logs slow requests
    (>3s) for production performance diagnosis.
    """
    started = datetime.utcnow()
    try:
        path = str(request.url.path)
    except Exception:
        path = getattr(request, 'url', 'unknown')
    response = await call_next(request)
    duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
    headers = response.headers
    headers['X-Response-Time'] = str(duration_ms)
    headers.setdefault('X-Content-Type-Options', 'nosniff')
    headers.setdefault('X-Frame-Options', 'DENY')
    headers.setdefault('Referrer-Policy', 'strict-origin-when-cross-origin')
    headers.setdefault(
        'Permissions-Policy',
        'camera=(), microphone=(), geolocation=(), payment=()',
    )
    csp_override = os.environ.get('CONTENT_SECURITY_POLICY')
    if csp_override:
        headers.setdefault('Content-Security-Policy', csp_override)
    elif IS_PRODUCTION:
        headers.setdefault(
            'Content-Security-Policy',
            "default-src 'none'; frame-ancestors 'none'; base-uri 'none'",
        )
    if IS_PRODUCTION:
        proto = (request.headers.get('x-forwarded-proto') or request.url.scheme).lower()
        if proto == 'https':
            headers.setdefault(
                'Strict-Transport-Security',
                'max-age=31536000; includeSubDomains',
            )
    # Phase 2.18G security: prevent caches (browser, intermediaries) from
    # storing authenticated or admin responses, which could otherwise be
    # served to a different user on a shared device. Public health,
    # /auth/google/config, and /plans deliberately stay cacheable.
    path_str = path if isinstance(path, str) else str(path or '')
    if (
        path_str.startswith('/api/auth/')
        or path_str.startswith('/api/admin/')
        or path_str.startswith('/api/dashboard/')
        or path_str.startswith('/api/automations')
        or path_str.startswith('/api/instagram/')
        or path_str.startswith('/api/usage/')
        or path_str.startswith('/api/plan/current')
    ) and not path_str == '/api/auth/google/config':
        headers['Cache-Control'] = 'no-store, private'
        headers.setdefault('Pragma', 'no-cache')
    if duration_ms > 3000:
        logger.warning(
            'slow_request path=%s durationMs=%s',
            path, duration_ms,
        )
    return response


@app.on_event('startup')
async def _startup():
    global _poll_task, IS_SHUTTING_DOWN
    IS_SHUTTING_DOWN = False
    # Phase 2.5: optional Sentry init. No-ops cleanly when SENTRY_DSN is
    # missing or the SDK isn't installed.
    try:
        import observability as _observability  # noqa: WPS433
        _observability.init_sentry()
    except Exception as _e:
        logger.info('observability_init_skipped reason=%s', str(_e)[:80])
    SHUTDOWN_EVENT.clear()
    # Ensure a unique index on (user_id, ig_comment_id) so dedup is fast and safe
    try:
        await db.comments.drop_index('uniq_user_ig_comment')
    except Exception:
        pass
    try:
        await db.comments.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('ig_comment_id', 1)],
            unique=True, sparse=True, name='uniq_user_account_ig_comment'
        )
    except Exception as e:
        logger.warning('comments index create: %s', e)
    # DM automation: dedup index on (user_id, message_id) so the same incoming
    # DM is never replied to twice even if the webhook is replayed.
    # Drop legacy non-unique-safe index on (user_id, message_id) which collided
    # on null mid and silently deduped real messages. Replaced with a unique
    # index on (user_id, dedup_key) where dedup_key is mid|id|content-hash.
    try:
        await db.dm_logs.drop_index('uniq_user_dm_message')
    except Exception:
        pass
    try:
        await db.dm_logs.drop_index('uniq_user_dm_dedup_key')
    except Exception:
        pass
    try:
        await db.dm_logs.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('dedup_key', 1)],
            unique=True, sparse=True, name='uniq_user_ig_dm_dedup_key',
        )
    except Exception as e:
        logger.warning('dm_logs dedup_key index create: %s', e)
    try:
        await db.dm_rules.create_index([('user_id', 1), ('is_active', 1)],
                                       name='dm_rules_user_active')
    except Exception as e:
        logger.warning('dm_rules index create: %s', e)
    try:
        await db.automations.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('status', 1)],
            name='automations_user_ig_status',
        )
        await db.comments.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('created', -1)],
            name='comments_user_ig_created',
        )
        await db.comments.create_index(
            [('action_status', 1), ('next_retry_at', 1), ('queue_lock_until', 1)],
            name='comments_automation_queue_due',
        )
        await db.conversations.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('created', -1)],
            name='conversations_user_ig_created',
        )
        await db.dm_rules.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('is_active', 1)],
            name='dm_rules_user_ig_active',
        )
        await db.dm_logs.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('created', -1)],
            name='dm_logs_user_ig_created',
        )
        await db.comment_dm_sessions.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('created', -1)],
            name='comment_dm_sessions_user_ig_created',
        )
        await db.data_deletion_requests.create_index(
            [('created_at', -1)],
            name='data_deletion_requests_created',
        )
        await db.contacts.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('created', -1)],
            name='contacts_user_ig_created',
        )
        await db.tracked_links.create_index(
            [('shortCode', 1)],
            unique=True,
            sparse=True,
            name='tracked_links_short_code_unique',
        )
        await db.tracked_links.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('ruleId', 1)],
            name='tracked_links_user_ig_rule',
        )
        await db.tracked_links.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('created', -1)],
            name='tracked_links_user_ig_created',
        )
        await db.tracked_links.create_index(
            [('expiresAt', 1)],
            name='tracked_links_expires_at',
        )
        await db.link_click_events.create_index(
            [('trackedLinkId', 1)],
            name='link_click_events_tracked_link',
        )
        await db.link_click_events.create_index(
            [('shortCode', 1)],
            name='link_click_events_short_code',
        )
        await db.link_click_events.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('clickedAt', -1)],
            name='link_click_events_user_ig_clicked',
        )
        await db.link_click_events.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('instagramUserId', 1)],
            name='link_click_events_user_ig_contact',
        )
        await db.link_click_events.create_index(
            [('userId', 1)],
            name='link_click_events_user_id',
        )
        await db.usage_events.create_index(
            [('user_id', 1), ('event_month', 1)],
            name='usage_events_user_month',
        )
        await db.usage_events.create_index(
            [('user_id', 1), ('event_type', 1), ('event_month', 1)],
            name='usage_events_user_type_month',
        )
        await db.usage_events.create_index(
            [('user_id', 1), ('event_type', 1), ('event_date', -1)],
            name='usage_events_user_type_date',
        )
        await db.usage_events.create_index(
            [('instagram_account_id', 1), ('event_month', 1)],
            name='usage_events_instagram_account_month',
        )
        await db.usage_events.create_index(
            [('event_month', 1), ('limit_subject_type', 1), ('limit_subject_id', 1)],
            name='usage_events_subject_month',
        )
        await db.usage_events.create_index(
            [('automation_id', 1), ('event_month', 1)],
            name='usage_events_automation_month',
        )
        await db.usage_events.create_index(
            [('created_at', -1)],
            name='usage_events_created_at',
        )
        await db.usage_reservations.create_index(
            [('idempotency_key', 1)],
            unique=True,
            name='usage_reservations_idempotency_unique',
        )
        await db.usage_reservations.create_index(
            [('limit_subject_type', 1), ('limit_subject_id', 1),
             ('month', 1), ('metric', 1), ('status', 1)],
            name='usage_reservations_subject_month_metric_status',
        )
        await db.usage_reservations.create_index(
            [('expires_at', 1), ('status', 1)],
            name='usage_reservations_expires_status',
        )
        await db.usage_reservation_buckets.create_index(
            [('limit_subject_type', 1), ('limit_subject_id', 1), ('month', 1), ('metric', 1)],
            unique=True,
            name='usage_reservation_buckets_subject_month_metric_unique',
        )
        await db.dashboard_summaries.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('month', 1)],
            unique=True,
            name='dashboard_summaries_user_account_month_unique',
        )
        await db.dashboard_summaries.create_index(
            [('user_id', 1), ('month', 1)],
            name='dashboard_summaries_user_month',
        )
        await db.dashboard_summaries.create_index(
            [('instagramAccountId', 1), ('month', 1)],
            name='dashboard_summaries_account_month',
        )
        await db.dashboard_summaries.create_index(
            [('expires_at', 1)],
            name='dashboard_summaries_expires_at',
        )
        try:
            await db.monthly_usage.drop_index('monthly_usage_user_month_unique')
        except Exception:
            pass
        await db.monthly_usage.create_index(
            [('user_id', 1), ('event_month', 1)],
            name='monthly_usage_user_month_lookup',
        )
        await db.monthly_usage.create_index(
            [('event_month', 1), ('limit_subject_type', 1), ('limit_subject_id', 1)],
            name='monthly_usage_subject_month',
        )
        await db.monthly_usage.create_index(
            [('event_month', 1), ('limit_subject_type', 1), ('limit_subject_id', 1)],
            unique=True,
            name='monthly_usage_subject_unique',
            partialFilterExpression={
                'limit_subject_type': {'$exists': True},
                'limit_subject_id': {'$exists': True},
            },
        )
        # Phase 2.2: user_plans — one row per user.
        await db.user_plans.create_index(
            [('user_id', 1)], unique=True, name='user_plans_user_unique',
        )
        # Phase 2.4: admin audit log indexes for the console activity feed.
        await db.admin_audit_logs.create_index(
            [('admin_user_id', 1), ('created_at', -1)],
            name='admin_audit_admin_created',
        )
        await db.admin_audit_logs.create_index(
            [('target_user_id', 1), ('created_at', -1)],
            name='admin_audit_target_user_created',
        )
        await db.admin_audit_logs.create_index(
            [('action', 1), ('created_at', -1)],
            name='admin_audit_action_created',
        )
        # Phase 2.7: Google sub unique (sparse — most users won't have one).
        await db.users.create_index(
            [('google_sub', 1)], unique=True, sparse=True,
            name='users_google_sub_unique',
        )
        # Phase 2.12G: normalized email lookup. This is intentionally
        # non-unique until legacy duplicate diagnostics can be reviewed.
        await db.users.create_index(
            [('normalized_email', 1)], sparse=True,
            name='users_normalized_email_lookup',
        )
        await db.users.create_index(
            [('email_verification_token_hash', 1)],
            sparse=True,
            name='users_email_verification_token_hash',
        )
        # Phase 2.8: user_limit_overrides indexes.
        await db.user_limit_overrides.create_index(
            [('user_id', 1), ('status', 1)],
            name='user_limit_overrides_user_status',
        )
        await db.user_limit_overrides.create_index(
            [('user_id', 1), ('starts_at', 1), ('ends_at', 1)],
            name='user_limit_overrides_user_window',
        )
        await db.user_limit_overrides.create_index(
            [('status', 1), ('ends_at', 1)],
            name='user_limit_overrides_status_ends',
        )
        await db.user_limit_overrides.create_index(
            [('created_by_user_id', 1), ('created_at', -1)],
            name='user_limit_overrides_creator_created',
        )
        # Phase 2.8: status field on users (sparse — legacy rows treated as active).
        await db.users.create_index(
            [('status', 1)], sparse=True,
            name='users_status',
        )
        # Phase 2.6: admin_members.
        await db.admin_members.create_index(
            [('user_id', 1)], unique=True, sparse=True,
            name='admin_members_user_unique',
        )
        await db.admin_members.create_index(
            [('email', 1)], unique=True, sparse=True,
            name='admin_members_email_unique',
        )
        await db.admin_members.create_index(
            [('role', 1), ('created_at', -1)],
            name='admin_members_role_created',
        )
        await db.admin_members.create_index(
            [('disabled_at', 1)],
            name='admin_members_disabled_at',
        )
    except Exception as e:
        logger.warning('account scoped index create: %s', e)
    try:
        await db.comment_dm_sessions.create_index(
            [('user_id', 1), ('recipient_id', 1), ('status', 1), ('created', -1)],
            name='comment_dm_sessions_pending_lookup',
        )
    except Exception as e:
        logger.warning('comment_dm_sessions index create: %s', e)
    try:
        await db.instagram_accounts.create_index([('id', 1)], unique=True,
                                                 name='instagram_accounts_id_unique')
        await db.instagram_accounts.create_index(
            [('userId', 1), ('instagramAccountId', 1)],
            unique=True,
            sparse=True,
            name='instagram_accounts_user_ig_unique',
        )
        await db.instagram_accounts.create_index(
            [('isActive', 1), ('connectionValid', 1), ('tokenExpiresAt', 1)],
            name='instagram_accounts_refresh_due',
        )
        await db.instagram_accounts.create_index(
            [('instagramAccountId', 1)],
            name='instagram_accounts_canonical_lookup',
        )
        await db.instagram_accounts.create_index(
            [('instagramAccountId', 1)],
            unique=True,
            name='uniq_active_instagram_account_owner',
            partialFilterExpression={
                'instagramAccountId': {'$exists': True},
                'isActive': True,
                'connectionValid': True,
            },
        )
        await db.instagram_accounts.create_index(
            [('userId', 1), ('isActive', 1)],
            name='instagram_accounts_user_active',
        )
        await db.instagram_account_trial_claims.create_index(
            [('instagram_account_id', 1), ('plan_trial_identifier', 1)],
            unique=True,
            name='uniq_instagram_trial_claim',
        )
        await db.instagram_account_trial_claims.create_index(
            [('first_claimed_by_user_id', 1), ('claimed_at', -1)],
            name='instagram_trial_claims_user_claimed',
        )
    except Exception as e:
        logger.warning('instagram_accounts index create: %s', e)
    try:
        migrated = await _ensure_instagram_account_docs_for_connected_users()
        if migrated:
            logger.info('instagram_accounts_migrated_from_users count=%s', migrated)
        scoped_users = await db.users.find({
            'instagramConnected': True,
            'ig_user_id': {'$nin': [None, '']},
        }).limit(1000).to_list(1000)
        scoped_rules = 0
        for user_doc in scoped_users:
            scoped_rules += await _ensure_automation_account_scope_for_user(user_doc)
        if scoped_rules:
            logger.info('instagram_automation_account_scope_migrated count=%s', scoped_rules)
    except Exception as e:
        logger.warning('instagram_accounts migration: %s', e)
    try:
        now = datetime.utcnow()
        comment_rules = {
            '$or': [
                {'trigger': {'$regex': '^comment:', '$options': 'i'}},
                {'nodes.data.trigger': {'$regex': '^comment:', '$options': 'i'}},
            ],
        }
        await db.automations.update_many(
            {**comment_rules, 'activationStartedAt': {'$exists': False}},
            {'$set': {'activationStartedAt': now}}
        )
        await db.automations.update_many(
            {**comment_rules, 'processExistingComments': {'$exists': False}},
            {'$set': {'processExistingComments': False}}
        )
        await db.automations.update_many(
            {**comment_rules, 'post_scope': {'$in': ['any', 'all', 'latest', 'next']}},
            {'$set': {'processExistingComments': False,
                      'process_existing_unreplied_comments': False}}
        )
        await db.automations.update_many(
            {**comment_rules, 'trigger': {'$in': ['comment:any', 'comment:all', 'comment:latest', 'comment:next']}},
            {'$set': {'processExistingComments': False,
                      'process_existing_unreplied_comments': False}}
        )
        await db.automations.update_many(
            {**comment_rules, 'createdAt': {'$exists': False}},
            {'$set': {'createdAt': now}}
        )
        await db.automations.update_many(
            {**comment_rules, 'updatedAt': {'$exists': False}},
            {'$set': {'updatedAt': now}}
        )
    except Exception as e:
        logger.warning('comment rule activation migration: %s', e)
    try:
        await _repair_legacy_reply_success_without_provider_proof()
    except Exception as e:
        logger.warning('legacy reply provider-proof repair: %s', e)
    if IG_POLL_ENABLED:
        _register_bg_task('comment_poller', _comment_poller_loop)
    else:
        logger.info('Comment poller disabled via IG_POLL_ENABLED=0')
    logger.info('automation_queue_registering interval=%s batch_size=%s',
                AUTOMATION_QUEUE_INTERVAL_SECONDS, AUTOMATION_QUEUE_BATCH_SIZE)
    _register_bg_task('automation_queue', _automation_queue_loop)
    _register_bg_task('follow_verifier', _follow_verifier_loop)
    # Watchdog last so it can supervise the others.
    _register_bg_task('watchdog', _watchdog_loop)


@app.on_event('shutdown')
async def shutdown_db_client():
    global IS_SHUTTING_DOWN
    # Step 1 — announce
    logger.info('shutdown_started')

    # Step 2 — signal all loops to stop; new write guards check this flag
    IS_SHUTTING_DOWN = True
    SHUTDOWN_EVENT.set()

    # Step 3 — watchdog: it checks IS_SHUTTING_DOWN before restarting tasks,
    # so setting the flag above is sufficient. Cancel it explicitly now.
    watchdog_info = _BG_TASKS.get('watchdog')
    watchdog_task = watchdog_info.get('task') if watchdog_info else None
    if watchdog_task is not None and not watchdog_task.done():
        watchdog_task.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(watchdog_task),
                                   timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
            pass

    # Step 4 — cancel registered long-running background loops (not watchdog)
    bg_cancel_tasks = []
    for name, info in list(_BG_TASKS.items()):
        if name == 'watchdog':
            continue
        task = info.get('task')
        if task is not None and not task.done():
            task.cancel()
            bg_cancel_tasks.append(task)

    # Step 5 — wait for background loops to exit
    if bg_cancel_tasks:
        done, pending = await asyncio.wait(
            bg_cancel_tasks, timeout=SHUTDOWN_BG_CANCEL_TIMEOUT_SECONDS)
        for t in pending:
            logger.warning('shutdown_bg_task_still_running name=%s',
                           getattr(t, '_mychat_name', '?'))

    # Step 6 — wait for in-flight short-lived tasks (webhook processors, broadcasts)
    if _INFLIGHT_TASKS:
        logger.info('shutdown_waiting_inflight count=%s', len(_INFLIGHT_TASKS))
        remaining = list(_INFLIGHT_TASKS)
        done, pending = await asyncio.wait(
            remaining, timeout=SHUTDOWN_INFLIGHT_WAIT_SECONDS)
        if pending:
            logger.warning('shutdown_cancelling_overdue_inflight count=%s', len(pending))
            for t in pending:
                t.cancel()

    # Step 7 — drain any tasks that were just cancelled
    await asyncio.sleep(0)

    # Step 8 — close WebSocket connections (best-effort)
    try:
        from websockets.exceptions import ConnectionClosed  # type: ignore
    except ImportError:
        pass

    # Step 9 — close Mongo client AFTER all writes are done
    try:
        client.close()
    except Exception:
        logger.exception('shutdown_mongo_close_error')

    # Step 10 — done
    logger.info('shutdown_complete')
