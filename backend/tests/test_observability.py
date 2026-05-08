"""Phase 2.5: backend observability scrubber + status tests.

These tests verify the pure scrubber and the safe status endpoint without
requiring sentry-sdk to be installed. They are the privacy contract for
what backend errors are allowed to leave the cluster.
"""
import os
import sys
from datetime import datetime
from pathlib import Path

import pytest

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://example.com')
os.environ.setdefault('FRONTEND_URL', 'https://example.com')
os.environ.setdefault('IG_APP_ID', '123')
os.environ.setdefault('IG_APP_SECRET', 'secret')
os.environ.setdefault('CRON_SECRET', 'cron-secret')

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server  # noqa: E402
import observability  # noqa: E402

from test_instagram_token_refresh import FakeDB, _account, _run, _user  # noqa: E402


# ── boots cleanly without DSN ───────────────────────────────────────────────

def test_init_sentry_returns_false_without_dsn(monkeypatch):
    monkeypatch.setattr(observability, 'SENTRY_DSN', '')
    monkeypatch.setattr(observability, '_SENTRY_INITIALIZED', False)
    assert observability.init_sentry() is False


def test_observability_status_safe_with_no_dsn(monkeypatch):
    monkeypatch.setattr(observability, 'SENTRY_DSN', '')
    status = observability.observability_status()
    assert status['sentry_configured'] is False
    assert status['service'] == 'backend'
    # DSN never echoed.
    assert 'dsn' not in str(status).lower()


def test_observability_status_with_dsn_does_not_echo_dsn(monkeypatch):
    secret_dsn = 'https://verysecretkey:verysecretsecret@o123.ingest.sentry.io/456'
    monkeypatch.setattr(observability, 'SENTRY_DSN', secret_dsn)
    monkeypatch.setattr(observability, 'SENTRY_RELEASE', 'abcdef1234567890')
    status = observability.observability_status()
    assert status['sentry_configured'] is True
    assert status['build_sha'] == 'abcdef123456'  # truncated to 12 — public commit sha is fine
    # DSN secrets must NOT appear anywhere in the status payload.
    assert secret_dsn not in str(status)
    assert 'verysecretkey' not in str(status)
    assert 'verysecretsecret' not in str(status)
    assert 'sentry.io' not in str(status)
    assert 'ingest' not in str(status)


# ── status endpoint ──────────────────────────────────────────────────────────

def test_observability_status_endpoint_returns_safe_shape(monkeypatch):
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA'),
        _user(id='u1', email='u1@example.com'),
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(observability, 'SENTRY_DSN', '')
    res = _run(server.observability_status_endpoint(user_id='u1'))
    assert 'backend' in res
    assert 'frontend' in res
    assert res['backend']['sentry_configured'] is False
    assert res['frontend']['sentry_dsn_env_var'] == 'REACT_APP_SENTRY_DSN'
    assert res['frontend']['posthog_key_env_var'] == 'REACT_APP_POSTHOG_KEY'
    # No DSN values, no API keys.
    serialized = repr(res)
    assert 'dsn=' not in serialized.lower() or 'sentry_dsn_env_var' in serialized


# ── pure scrubber: headers ──────────────────────────────────────────────────

def test_scrubber_redacts_authorization_header():
    event = {
        'request': {
            'url': 'https://api.example.com/health',
            'headers': {
                'Authorization': 'Bearer SECRET-JWT-VALUE',
                'X-Forwarded-For': '1.2.3.4',
            },
        },
    }
    out = observability.redact_sentry_event(event)
    assert out['request']['headers']['Authorization'] == '[redacted]'
    assert 'SECRET-JWT-VALUE' not in repr(out)
    assert out['request']['headers']['X-Forwarded-For'] == '1.2.3.4'


def test_scrubber_redacts_cookie_and_signature_headers():
    event = {
        'request': {
            'url': 'https://api.example.com/x',
            'headers': {
                'Cookie': 'session=abc',
                'X-Hub-Signature-256': 'sha256=secret',
                'X-Api-Key': 'k123',
                'Set-Cookie': 'session=abc; HttpOnly',
            },
        },
    }
    out = observability.redact_sentry_event(event)
    h = out['request']['headers']
    assert h['Cookie'] == '[redacted]'
    assert h['X-Hub-Signature-256'] == '[redacted]'
    assert h['X-Api-Key'] == '[redacted]'
    assert h['Set-Cookie'] == '[redacted]'


def test_scrubber_redacts_query_string_with_oauth_code():
    event = {
        'request': {
            'url': 'https://api.example.com/instagram/callback',
            'query_string': 'code=AQB-secret&state=xyz',
            'headers': {},
        },
    }
    out = observability.redact_sentry_event(event)
    assert out['request']['query_string'] == '[redacted]'
    assert 'AQB-secret' not in repr(out)


# ── pure scrubber: bodies ────────────────────────────────────────────────────

def test_scrubber_drops_webhook_body_entirely():
    event = {
        'request': {
            'url': 'https://api.example.com/api/instagram/webhook',
            'data': {
                'entry': [{'changes': [{'value': {'text': 'private comment text 12345'}}]}],
            },
            'headers': {},
        },
    }
    out = observability.redact_sentry_event(event)
    assert out['request']['data'] == '[redacted]'
    assert 'private comment text 12345' not in repr(out)


def test_scrubber_drops_comment_body_entirely():
    event = {
        'request': {
            'url': 'https://api.example.com/api/comments/abc/reply',
            'data': {'text': 'private operator reply 99999'},
            'headers': {},
        },
    }
    out = observability.redact_sentry_event(event)
    assert out['request']['data'] == '[redacted]'
    assert 'private operator reply 99999' not in repr(out)


def test_scrubber_redacts_forbidden_keys_in_extra():
    event = {
        'extra': {
            'access_token': 'EAA-secret-token',
            'meta_access_token': 'EAA-secret-token',
            'authorization': 'Bearer secret',
            'comment_text': 'private comment 1234',
            'dm_text': 'private dm 5678',
            'reply_text': 'private reply 9999',
            'state': 'oauth-state-secret',
            'code': 'oauth-code-secret',
            'safe_field': 'keep_me',
        },
    }
    out = observability.redact_sentry_event(event)
    extra = out['extra']
    for forbidden in ('access_token', 'meta_access_token', 'authorization',
                      'comment_text', 'dm_text', 'reply_text', 'state', 'code'):
        assert extra[forbidden] == '[redacted]'
    assert extra['safe_field'] == 'keep_me'
    serialized = repr(out)
    assert 'EAA-secret-token' not in serialized
    assert 'private comment 1234' not in serialized
    assert 'private dm 5678' not in serialized
    assert 'private reply 9999' not in serialized
    assert 'oauth-state-secret' not in serialized
    assert 'oauth-code-secret' not in serialized


def test_scrubber_redacts_nested_keys_in_contexts():
    event = {
        'contexts': {
            'request': {
                'graph_error': {'message': 'private graph body 7777'},
                'safe': 'ok',
            },
        },
    }
    out = observability.redact_sentry_event(event)
    assert out['contexts']['request']['graph_error'] == '[redacted]'
    assert out['contexts']['request']['safe'] == 'ok'
    assert 'private graph body 7777' not in repr(out)


def test_scrubber_strips_user_email_keeps_id():
    event = {
        'user': {
            'id': 'u_target',
            'email': 'private@example.com',
            'ip_address': '1.2.3.4',
        },
    }
    out = observability.redact_sentry_event(event)
    assert out['user'] == {'id': 'u_target'}
    assert 'private@example.com' not in repr(out)
    assert '1.2.3.4' not in repr(out)


def test_scrubber_safe_on_missing_or_string_event():
    assert observability.redact_sentry_event(None) is None
    assert observability.redact_sentry_event('a string') == 'a string'
    assert observability.redact_sentry_event({}) == {}


# ── breadcrumbs ──────────────────────────────────────────────────────────────

def test_scrubber_redacts_breadcrumb_messages_with_secrets():
    event = {
        'breadcrumbs': {
            'values': [
                {'message': 'GET /api/x?access_token=SECRET'},
                {'message': 'Authorization: Bearer SECRET'},
                {'message': 'normal log line'},
                {'data': {'access_token': 'SECRET', 'route': '/api/x'}},
            ],
        },
    }
    out = observability.redact_sentry_event(event)
    crumbs = out['breadcrumbs']['values']
    assert crumbs[0]['message'] == '[redacted]'
    assert crumbs[1]['message'] == '[redacted]'
    assert crumbs[2]['message'] == 'normal log line'
    assert crumbs[3]['data']['access_token'] == '[redacted]'
    assert crumbs[3]['data']['route'] == '/api/x'
