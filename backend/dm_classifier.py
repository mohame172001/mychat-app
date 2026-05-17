"""Phase 2.18Z — Instagram Graph send-error classifier.

Extracted from server.py to keep the 19k-line monolith maintainable.
Pure functions only — no Mongo, no FastAPI, no httpx. Safe to import
from anywhere in the backend without circular-import risk.

Public API:
    _redact_secrets(value)               - recursive secret scrubber
    _safe_provider_error_payload(error)  - normalise raw Graph error
    classify_instagram_send_error(error, status_code) - main classifier
    _detailed_send_result(ok, status, body, error)    - response shape
    _classify_graph_send_error(status, body)          - legacy string path
    PERMANENT_GRAPH_FAILURE_REASONS                   - frozenset
    TRANSIENT_GRAPH_FAILURE_REASONS                   - frozenset
"""
from __future__ import annotations

import json
from typing import Any, Optional


# Meta subcodes that mean "messaging not deliverable to this user" —
# Meta deliberately obscures the human reason in the public message
# field, so the subcode is the only stable identifier.
# Reference: https://developers.facebook.com/docs/messenger-platform/error-codes
#   1545033 = recipient not eligible / 24-hour window expired
#   1545041 = messaging policy violation
#   2018028 = no matching user / cannot message
#   2018278 = recipient cannot receive messages
_MESSAGING_WINDOW_SUBCODES = frozenset({1545033, 1545041, 2018028, 2018278})


PERMANENT_GRAPH_FAILURE_REASONS = frozenset({
    'recipient_unavailable',
    'messaging_not_allowed',
    'user_blocked_messages',
    'permission_error',
})

TRANSIENT_GRAPH_FAILURE_REASONS = frozenset({
    'rate_limited',
    'temporary_graph_error',
})


_SECRET_KEYS = frozenset({
    'access_token', 'accesstoken', 'meta_access_token',
    'client_secret', 'app_secret', 'refresh_token',
    'credential', 'google_credential', 'id_token',
    'google_id_token', 'token', 'authorization',
})


def _redact_secrets(value):
    """Recursively redact obvious credential keys from dicts/strings before
    they hit the log stream or HTTP response bodies."""
    if isinstance(value, dict):
        return {k: ('***REDACTED***' if k.lower() in _SECRET_KEYS else _redact_secrets(v))
                for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_secrets(v) for v in value]
    return value


def _safe_provider_error_payload(error: Any) -> dict:
    """Normalise any raw Graph error (dict / JSON string / arbitrary
    object) into a flat dict suitable for classification. Secrets are
    scrubbed before return."""
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
    """Classify Graph send failures without exposing raw provider payloads.

    Returns a stable dict shape:
      {
        'failure_reason': str,        # low-cardinality string
        'retryable': bool,            # whether retry will help
        'provider_code': int|None,    # Meta's numeric code
        'provider_subcode': int|None, # Meta's subcode
        'status_code': int|None,      # HTTP status
        'safe_label': str,            # same as failure_reason for now
      }
    """
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
    try:
        sub_int = int(subcode) if subcode is not None else None
    except (TypeError, ValueError):
        sub_int = None
    if sub_int in _MESSAGING_WINDOW_SUBCODES:
        reason = 'messaging_window_expired'
        retryable = False
    elif status_code in (429,) or code in (4, 17, 32, 613) or 'rate limit' in message or 'too many' in message:
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

    if reason in PERMANENT_GRAPH_FAILURE_REASONS:
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
    """Build a structured send-result dict used by every IG message-
    sender helper. On failure it embeds the classified reason so
    callers can decide retry vs. permanent."""
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


def _classify_graph_send_error(status_code, body_text):
    """Legacy string-only classifier used by older Graph send wrappers.

    Returns a tuple (failure_reason, retryable). The newer
    classify_instagram_send_error returns a richer dict and should be
    preferred for new code paths.
    """
    classified = classify_instagram_send_error(body_text, status_code)
    return classified['failure_reason'], classified['retryable']
