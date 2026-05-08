"""Phase 2.5: backend observability foundation.

Optional Sentry integration. ALL configuration is opt-in via env vars;
nothing breaks when SENTRY_DSN is unset (the default in CI / local).

This module:
  - Owns the scrubber (`redact_sentry_event`) used by Sentry's `before_send`.
    It is a pure function so tests cover it without sentry-sdk installed.
  - Owns `init_sentry()` which lazy-imports sentry_sdk and silently no-ops
    if the SDK isn't installed or the DSN is missing.
  - Owns `observability_status()` which returns a sanitized status dict
    for the admin / system-health UI (never exposes the DSN).

Privacy contract:
  The scrubber removes Authorization headers, access tokens (anywhere in
  the event), Cookies, raw comment / reply / DM text, raw Graph error
  bodies, and request bodies for webhook / comment / DM paths. The DSN
  itself is also never echoed to the API.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


# ---- env / config ----------------------------------------------------------

SENTRY_DSN = (os.environ.get('SENTRY_DSN') or '').strip()
SENTRY_ENVIRONMENT = (os.environ.get('SENTRY_ENVIRONMENT') or os.environ.get('APP_ENV') or 'unknown').strip()
SENTRY_RELEASE = (os.environ.get('SENTRY_RELEASE') or os.environ.get('RAILWAY_GIT_COMMIT_SHA') or '').strip() or None


# ---- pure scrubber ---------------------------------------------------------

# Header / property keys that must always be removed (case-insensitive).
_FORBIDDEN_HEADER_KEYS = frozenset(s.lower() for s in (
    'authorization',
    'cookie',
    'set-cookie',
    'access_token',
    'meta_access_token',
    'x-hub-signature',
    'x-hub-signature-256',
    'x-api-key',
    'jwt',
    'csrf-token',
    'x-csrf-token',
))

# Body / context keys that may carry raw user / Graph content.
_FORBIDDEN_BODY_KEYS = frozenset(s.lower() for s in (
    'access_token',
    'meta_access_token',
    'authorization',
    'token',
    'jwt',
    'code',
    'state',
    'raw',
    'body',
    'payload',
    'graph_error',
    'error_body',
    'comment_text',
    'comment',
    'comment_message',
    'reply_text',
    'reply_message',
    'dm_text',
    'dm_message',
    'message_text',
    'message',
    'private_message',
    'text',
    'caption',
))

# URL paths whose request bodies are NEVER sent to Sentry — they may carry
# raw IG webhook payloads, raw comment bodies, raw reply text, or DM bodies.
_REDACT_BODY_PATH_PREFIXES = (
    '/api/instagram/webhook',
    '/api/comments/',
    '/api/dm-rules/',
    '/api/dm-automation',
    '/api/automations',
    '/api/instagram/webhook-log',
)


def _scrub_mapping(obj: Any, forbidden: frozenset, depth: int = 0) -> Any:
    """Walk a dict/list and remove any key in `forbidden` (case-insensitive)."""
    if depth > 5 or obj is None:
        return obj
    if isinstance(obj, dict):
        cleaned = {}
        for key, value in obj.items():
            if str(key).lower() in forbidden:
                cleaned[key] = '[redacted]'
                continue
            cleaned[key] = _scrub_mapping(value, forbidden, depth + 1)
        return cleaned
    if isinstance(obj, list):
        return [_scrub_mapping(item, forbidden, depth + 1) for item in obj]
    return obj


def _scrub_request(request: Optional[dict]) -> Optional[dict]:
    """Sanitize a Sentry-format `request` dict in place-safe (returns a copy)."""
    if not isinstance(request, dict):
        return request
    cleaned = dict(request)
    headers = cleaned.get('headers') or {}
    if isinstance(headers, dict):
        cleaned['headers'] = {
            k: ('[redacted]' if str(k).lower() in _FORBIDDEN_HEADER_KEYS else v)
            for k, v in headers.items()
        }
    cookies = cleaned.get('cookies') or {}
    if cookies:
        cleaned['cookies'] = '[redacted]'
    # Drop URL query string entirely if it contains likely-sensitive params.
    qs = cleaned.get('query_string') or ''
    if isinstance(qs, str) and any(p in qs.lower() for p in ('token', 'code=', 'state=', 'access_token', 'jwt')):
        cleaned['query_string'] = '[redacted]'
    # Always drop the body for routes that may carry raw user / Graph text.
    url = str(cleaned.get('url') or '')
    if any(p in url for p in _REDACT_BODY_PATH_PREFIXES):
        cleaned['data'] = '[redacted]'
    elif 'data' in cleaned:
        cleaned['data'] = _scrub_mapping(cleaned['data'], _FORBIDDEN_BODY_KEYS)
    return cleaned


def redact_sentry_event(event: dict, hint: Optional[dict] = None) -> Optional[dict]:
    """Sentry `before_send` hook. Pure function: cover with tests.

    Returns a sanitized event with tokens, raw text, cookies, and webhook
    bodies removed. Returns None to drop the event entirely (we never use
    that here — we always send the scrubbed version so admins see errors).
    """
    if not isinstance(event, dict):
        return event
    sanitized = dict(event)
    # Top-level request
    if 'request' in sanitized:
        sanitized['request'] = _scrub_request(sanitized['request'])
    # Extra / contexts may contain user-supplied dicts
    if 'extra' in sanitized:
        sanitized['extra'] = _scrub_mapping(sanitized.get('extra'), _FORBIDDEN_BODY_KEYS)
    if 'contexts' in sanitized:
        sanitized['contexts'] = _scrub_mapping(sanitized.get('contexts'), _FORBIDDEN_BODY_KEYS)
    # Breadcrumbs may capture http / log entries with bodies.
    crumbs = sanitized.get('breadcrumbs')
    if isinstance(crumbs, dict) and isinstance(crumbs.get('values'), list):
        cleaned_values = []
        for crumb in crumbs['values']:
            if not isinstance(crumb, dict):
                cleaned_values.append(crumb)
                continue
            c = dict(crumb)
            data = c.get('data')
            if isinstance(data, dict):
                c['data'] = _scrub_mapping(data, _FORBIDDEN_BODY_KEYS)
            msg = c.get('message')
            if isinstance(msg, str) and any(k in msg.lower() for k in ('access_token=', 'authorization:')):
                c['message'] = '[redacted]'
            cleaned_values.append(c)
        sanitized['breadcrumbs'] = {**crumbs, 'values': cleaned_values}
    # User context: keep id, drop email/IP unless explicitly safe.
    user = sanitized.get('user')
    if isinstance(user, dict):
        safe_user = {}
        for safe_key in ('id', 'username'):
            if safe_key in user:
                safe_user[safe_key] = user[safe_key]
        sanitized['user'] = safe_user
    return sanitized


# ---- init / status ---------------------------------------------------------

_SENTRY_INITIALIZED = False
_SENTRY_INIT_ERROR: Optional[str] = None


def init_sentry() -> bool:
    """Lazy-init Sentry. Returns True if initialized, False otherwise.

    Safe to call multiple times — re-init is a no-op. Missing SDK or
    missing DSN both result in clean disable.
    """
    global _SENTRY_INITIALIZED, _SENTRY_INIT_ERROR
    if _SENTRY_INITIALIZED:
        return True
    if not SENTRY_DSN:
        return False
    try:
        import sentry_sdk  # type: ignore
        from sentry_sdk.integrations.logging import LoggingIntegration  # type: ignore
    except Exception as e:
        _SENTRY_INIT_ERROR = f'sentry_sdk not installed: {str(e)[:80]}'
        logger.info('observability_sentry_disabled reason=sdk_missing detail=%s', _SENTRY_INIT_ERROR)
        return False
    try:
        sentry_sdk.init(
            dsn=SENTRY_DSN,
            environment=SENTRY_ENVIRONMENT,
            release=SENTRY_RELEASE,
            send_default_pii=False,
            traces_sample_rate=0.0,  # opt out of perf tracing for now
            before_send=redact_sentry_event,
            integrations=[LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)],
        )
        sentry_sdk.set_tag('service', 'backend')
        if SENTRY_RELEASE:
            sentry_sdk.set_tag('build_sha', SENTRY_RELEASE[:12])
        _SENTRY_INITIALIZED = True
        logger.info('observability_sentry_initialized environment=%s release=%s',
                    SENTRY_ENVIRONMENT, (SENTRY_RELEASE or '')[:12] or 'none')
        return True
    except Exception as e:
        _SENTRY_INIT_ERROR = str(e)[:120]
        logger.warning('observability_sentry_init_failed reason=%s', _SENTRY_INIT_ERROR)
        return False


def observability_status() -> Dict[str, Any]:
    """Return sanitized observability config for the admin / health UI.

    The DSN is NEVER echoed — only whether one is configured.
    """
    return {
        'sentry_configured': bool(SENTRY_DSN),
        'sentry_initialized': bool(_SENTRY_INITIALIZED),
        'sentry_init_error': _SENTRY_INIT_ERROR,
        'environment': SENTRY_ENVIRONMENT,
        'build_sha': (SENTRY_RELEASE[:12] if SENTRY_RELEASE else None),
        'service': 'backend',
    }
