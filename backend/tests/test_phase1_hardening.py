"""Phase 1 production hardening tests.

Covers:
  • Webhook: malformed JSON returns 400, logs webhook_invalid_json,
    spawns no background task.
  • /api/instagram/webhook-log: admin-only, no ?token= query, redacted
    payload (no raw text, no tokens).
  • CORS: configured origins via CORS_ALLOWED_ORIGINS + FRONTEND_URL,
    wildcard never used in production.
  • Security headers middleware: nosniff, X-Frame-Options DENY,
    Referrer-Policy, Permissions-Policy on every response. HSTS only
    when production + https.
  • Docs disabled in production unless ENABLE_DOCS_IN_PRODUCTION.
  • Rate limits: login (10/min), signup (5/h), poll-now (5/min),
    process-unreplied (5/min).
  • HMAC enforce mode: bad/missing signature returns 403 even when
    secret is configured. Warn mode logs webhook_hmac_not_enforced.
  • No raw private text / Graph error body leaks into logs.
  • DM classification consistency between the simple message-node path
    and the comment-DM-flow-entry / quick-reply path.
"""
import asyncio
import importlib
import json
import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

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


def _run(coro):
    return asyncio.run(coro)


# ── Lightweight Request stub for endpoints that take Request ─────────────────

class _FakeRequest:
    def __init__(self, headers=None, client_host='1.2.3.4', body=b'',
                 url_scheme='https'):
        self.headers = headers or {}
        self.client = SimpleNamespace(host=client_host)
        self._body = body
        self.url = SimpleNamespace(scheme=url_scheme)

    async def body(self):
        return self._body


def _reset_rate_limits():
    server._RATE_LIMIT_HITS.clear()


# ── Webhook: malformed JSON ──────────────────────────────────────────────────

def test_webhook_invalid_json_returns_400_no_task(monkeypatch, caplog):
    spawned = []

    def fake_create_tracked_task(coro, name, *_a, **_kw):
        spawned.append(name)
        # Close the coroutine so no warning about un-awaited coroutine.
        try:
            coro.close()
        except Exception:
            pass
        return SimpleNamespace()

    monkeypatch.setattr(server, 'create_tracked_task', fake_create_tracked_task)
    monkeypatch.setattr(server, '_verify_webhook_signature',
                        lambda body, sig: {'valid': True, 'reason': 'ok',
                                           'computed_prefix': 'sha256=AB',
                                           'received_prefix': 'sha256=AB',
                                           'secret_configured': True})

    req = _FakeRequest(body=b'{not valid json')
    with caplog.at_level(logging.WARNING, logger='mychat'):
        response = _run(server.instagram_webhook(req))
    assert response.status_code == 400
    body = json.loads(bytes(response.body))
    assert body.get('error') == 'invalid_json'
    assert spawned == [], 'no background task may run for malformed JSON'
    assert 'webhook_invalid_json' in caplog.text


def test_webhook_non_object_json_also_400(monkeypatch):
    spawned = []

    def fake_task(coro, name, *_a, **_kw):
        spawned.append(name)
        try:
            coro.close()
        except Exception:
            pass

    monkeypatch.setattr(server, 'create_tracked_task', fake_task)
    monkeypatch.setattr(server, '_verify_webhook_signature',
                        lambda b, s: {'valid': True, 'reason': 'ok',
                                      'secret_configured': True,
                                      'computed_prefix': '', 'received_prefix': ''})
    req = _FakeRequest(body=b'"just a string"')
    response = _run(server.instagram_webhook(req))
    assert response.status_code == 400
    assert spawned == []


def test_webhook_valid_json_does_spawn_tasks(monkeypatch):
    spawned = []

    def fake_task(coro, name, *_a, **_kw):
        spawned.append(name)
        try:
            coro.close()
        except Exception:
            pass

    monkeypatch.setattr(server, 'create_tracked_task', fake_task)
    monkeypatch.setattr(server, '_verify_webhook_signature',
                        lambda b, s: {'valid': True, 'reason': 'ok',
                                      'secret_configured': True,
                                      'computed_prefix': '', 'received_prefix': ''})
    req = _FakeRequest(body=b'{"object":"instagram","entry":[]}')
    res = _run(server.instagram_webhook(req))
    assert res == {'ok': True}
    assert 'webhook_log' in spawned
    assert 'webhook_processor' in spawned


# ── Webhook signature: HMAC enforce/warn ─────────────────────────────────────

def test_webhook_hmac_enforce_rejects_bad_signature(monkeypatch):
    monkeypatch.setattr(server, 'META_WEBHOOK_HMAC_ENFORCE', True)
    monkeypatch.setattr(server, '_verify_webhook_signature',
                        lambda b, s: {'valid': False, 'reason': 'mismatch',
                                      'secret_configured': True,
                                      'computed_prefix': 'sha256=AB',
                                      'received_prefix': 'sha256=ZZ'})
    req = _FakeRequest(body=b'{"object":"instagram"}',
                       headers={'x-hub-signature-256': 'sha256=ZZ'})
    res = _run(server.instagram_webhook(req))
    assert res.status_code == 403


def test_webhook_hmac_enforce_rejects_missing_signature(monkeypatch):
    monkeypatch.setattr(server, 'META_WEBHOOK_HMAC_ENFORCE', True)
    # Even with secret_configured=False we still must reject when
    # enforcement is on — that's the whole point of "enforce".
    monkeypatch.setattr(server, '_verify_webhook_signature',
                        lambda b, s: {'valid': False, 'reason': 'no_signature',
                                      'secret_configured': False,
                                      'computed_prefix': '', 'received_prefix': ''})
    req = _FakeRequest(body=b'{}')
    res = _run(server.instagram_webhook(req))
    assert res.status_code == 403


def test_webhook_hmac_warn_mode_logs_marker(monkeypatch, caplog):
    monkeypatch.setattr(server, 'META_WEBHOOK_HMAC_ENFORCE', False)
    monkeypatch.setattr(server, '_verify_webhook_signature',
                        lambda b, s: {'valid': False, 'reason': 'mismatch',
                                      'secret_configured': True,
                                      'computed_prefix': '', 'received_prefix': ''})

    spawned = []

    def fake_task(coro, name, *_a, **_kw):
        spawned.append(name)
        try:
            coro.close()
        except Exception:
            pass

    monkeypatch.setattr(server, 'create_tracked_task', fake_task)
    req = _FakeRequest(body=b'{"object":"instagram"}')
    with caplog.at_level(logging.WARNING, logger='mychat'):
        res = _run(server.instagram_webhook(req))
    assert res == {'ok': True}, 'warn-mode must still ACK so Meta does not retry'
    assert 'webhook_hmac_not_enforced' in caplog.text


# ── Webhook-log: admin-only + redaction ──────────────────────────────────────

class _FakeWebhookLogColl:
    def __init__(self, docs):
        self.docs = list(docs)

    def find(self):
        outer = self

        class _Cursor:
            def sort(self, *_a, **_kw):
                return self

            def limit(self, n):
                self._n = n
                return self

            async def to_list(self, n):
                return outer.docs[:n]
        return _Cursor()

    async def count_documents(self, _q):
        return len(self.docs)


class _FakeUsers:
    def __init__(self, users):
        self.users = list(users)

    async def find_one(self, query):
        for u in self.users:
            if all(u.get(k) == v for k, v in query.items()):
                return u
        return None


def test_webhook_log_blocks_non_admin(monkeypatch):
    monkeypatch.setattr(server, 'ADMIN_EMAILS', {'admin@example.com'})
    db = SimpleNamespace(
        users=_FakeUsers([{'id': 'u1', 'email': 'user@example.com'}]),
        webhook_log=_FakeWebhookLogColl([]),
    )
    monkeypatch.setattr(server, 'db', db)
    req = _FakeRequest()
    with pytest.raises(Exception) as exc:
        _run(server.instagram_webhook_log(req, limit=20, user_id='u1'))
    assert '403' in str(exc.value) or 'Admin' in str(exc.value)


def test_webhook_log_allows_admin_returns_redacted(monkeypatch):
    monkeypatch.setattr(server, 'ADMIN_EMAILS', {'admin@example.com'})
    secret_token = 'IGAA_SUPER_SECRET_TOKEN_ABC'
    private_text = 'this is a private comment that must not appear'
    docs = [{
        '_id': 'abc',
        'received': server.datetime.utcnow(),
        'payload': {
            'object': 'instagram',
            'entry': [{'id': 'biz1', 'time': 123}],
            'access_token': secret_token,
            'comment': {'text': private_text},
        },
        'signature_valid': True,
        'signature_reason': 'ok',
        'enforce_mode': True,
    }]
    db = SimpleNamespace(
        users=_FakeUsers([{'id': 'u_admin', 'email': 'admin@example.com'}]),
        webhook_log=_FakeWebhookLogColl(docs),
    )
    monkeypatch.setattr(server, 'db', db)
    req = _FakeRequest()
    out = _run(server.instagram_webhook_log(req, limit=20, user_id='u_admin'))
    assert out['count'] == 1
    item = out['items'][0]
    serialized = json.dumps(item, default=str)
    assert secret_token not in serialized
    assert private_text not in serialized
    # Safe metadata IS exposed
    assert item['object'] == 'instagram'
    assert item['entry_count'] == 1
    assert item['signature_valid'] is True


def test_webhook_log_signature_does_not_accept_token_query():
    """The endpoint signature must NOT have a ?token= parameter — that's
    the regression we are protecting against (it leaked JWTs into logs)."""
    import inspect
    sig = inspect.signature(server.instagram_webhook_log)
    assert 'token' not in sig.parameters, (
        'webhook-log must not accept a ?token= query parameter; '
        'use the Authorization header instead'
    )


# ── CORS configuration ──────────────────────────────────────────────────────

def test_cors_origins_include_frontend_url():
    origins = server._resolved_cors_origins()
    assert any(server.FRONTEND_URL.rstrip('/') in o for o in origins)


def test_cors_origins_exclude_wildcard_in_production(monkeypatch):
    monkeypatch.setattr(server, 'IS_PRODUCTION', True)
    monkeypatch.setattr(server, 'FRONTEND_URL', 'https://app.example.com')
    monkeypatch.setattr(server, 'CORS_ALLOWED_ORIGINS_ENV',
                        ['https://app.example.com'])
    monkeypatch.setattr(server, 'RAILWAY_PUBLIC_DOMAINS', [])
    origins = server._resolved_cors_origins()
    assert '*' not in origins
    assert all(not o.startswith('http://localhost') for o in origins)


def test_cors_origins_include_localhost_in_dev(monkeypatch):
    monkeypatch.setattr(server, 'IS_PRODUCTION', False)
    monkeypatch.setattr(server, 'CORS_ALLOWED_ORIGINS_ENV', [])
    monkeypatch.setattr(server, 'RAILWAY_PUBLIC_DOMAINS', [])
    origins = server._resolved_cors_origins()
    assert any('localhost:3000' in o for o in origins)


# ── Security headers ─────────────────────────────────────────────────────────

def test_security_headers_applied_to_response(monkeypatch):
    """Run the middleware against a stub call_next and confirm headers."""
    captured_headers = {}

    class _Resp:
        def __init__(self):
            self.headers = {}

    async def fake_next(_request):
        return _Resp()

    monkeypatch.setattr(server, 'IS_PRODUCTION', False)
    res = _run(server.security_headers_middleware(_FakeRequest(), fake_next))
    h = res.headers
    assert h['X-Content-Type-Options'] == 'nosniff'
    assert h['X-Frame-Options'] == 'DENY'
    assert 'Referrer-Policy' in h
    assert 'Permissions-Policy' in h
    # HSTS NOT set in non-production
    assert 'Strict-Transport-Security' not in h


def test_hsts_only_in_production_https(monkeypatch):
    class _Resp:
        def __init__(self):
            self.headers = {}

    async def fake_next(_r):
        return _Resp()

    monkeypatch.setattr(server, 'IS_PRODUCTION', True)
    req_https = _FakeRequest(headers={'x-forwarded-proto': 'https'})
    res = _run(server.security_headers_middleware(req_https, fake_next))
    assert 'Strict-Transport-Security' in res.headers

    req_http = _FakeRequest(headers={'x-forwarded-proto': 'http'})
    res2 = _run(server.security_headers_middleware(req_http, fake_next))
    assert 'Strict-Transport-Security' not in res2.headers


# ── Docs disabled in production ──────────────────────────────────────────────

def test_docs_disabled_when_production_env_set():
    """Production environments must default to docs/openapi off."""
    # We can't import the full module again easily; instead assert on the
    # _FASTAPI_KW resolution branch by faking IS_PRODUCTION at import time.
    env = {
        **os.environ,
        'APP_ENV': 'production',
        'JWT_SECRET': 'x',
        'MONGO_URL': 'mongodb://x',
        'BACKEND_PUBLIC_URL': 'https://x',
        'FRONTEND_URL': 'https://x',
        'IG_APP_ID': '1', 'IG_APP_SECRET': 's',
        'META_WEBHOOK_VERIFY_TOKEN': 'verify',
        'CRON_SECRET': 'c',
        'ADMIN_EMAILS': 'admin@example.com',
    }
    with patch.dict(os.environ, env, clear=False):
        # Reload server module under production env.
        import server as fresh_server
        fresh_server = importlib.reload(fresh_server)
        assert fresh_server.IS_PRODUCTION is True
        assert fresh_server.app.docs_url is None
        assert fresh_server.app.redoc_url is None
        assert fresh_server.app.openapi_url is None
    # Reload back to dev for subsequent tests.
    os.environ.pop('APP_ENV', None)
    importlib.reload(server)


# ── Rate limits ──────────────────────────────────────────────────────────────

def test_rate_limit_signup_per_ip(monkeypatch):
    _reset_rate_limits()
    monkeypatch.setattr(server, 'RATE_LIMIT_SIGNUP_PER_HOUR', 2)

    class _Users:
        async def find_one(self, _q):
            return None

        async def insert_one(self, _d):
            return SimpleNamespace(inserted_id='x')

    db = SimpleNamespace(users=_Users())
    monkeypatch.setattr(server, 'db', db)

    async def _no_seed(_uid):
        return None

    monkeypatch.setattr(server, '_seed_user', _no_seed)

    from models import SignupIn
    req = _FakeRequest(headers={'x-forwarded-for': '5.5.5.5'})
    # 2 allowed
    _run(server.signup(SignupIn(username='a1', email='a1@e.com', password='passwd123'),
                       req))
    _run(server.signup(SignupIn(username='a2', email='a2@e.com', password='passwd123'),
                       req))
    # 3rd from same IP → 429
    with pytest.raises(Exception) as exc:
        _run(server.signup(SignupIn(username='a3', email='a3@e.com', password='passwd123'),
                           req))
    assert '429' in str(exc.value) or 'Too many' in str(exc.value)


def test_rate_limit_login_per_ip(monkeypatch):
    _reset_rate_limits()
    monkeypatch.setattr(server, 'RATE_LIMIT_LOGIN_PER_MIN', 2)

    class _Users:
        async def find_one(self, _q):
            return None

    monkeypatch.setattr(server, 'db', SimpleNamespace(users=_Users()))
    from models import LoginIn
    req = _FakeRequest(headers={'x-forwarded-for': '7.7.7.7'})
    # First two attempts: 401 (user not found) — both consume rate-limit slots.
    for i in range(2):
        with pytest.raises(Exception):
            _run(server.login(LoginIn(username=f'x{i}', password='p'), req))
    # Third attempt from same IP: 429
    with pytest.raises(Exception) as exc:
        _run(server.login(LoginIn(username='x9', password='p'), req))
    assert '429' in str(exc.value) or 'Too many' in str(exc.value)


def test_rate_limit_separate_buckets(monkeypatch):
    _reset_rate_limits()
    # Different (bucket, key) pairs must NOT share counters.
    assert not server._rate_limited('login', 'ip1', limit=1, window_seconds=60)
    assert server._rate_limited('login', 'ip1', limit=1, window_seconds=60)
    # Same key, different bucket
    assert not server._rate_limited('signup', 'ip1', limit=1, window_seconds=60)


def test_rate_limit_429_response_helper():
    res = server._rate_limit_response(retry_after_seconds=42)
    assert res.status_code == 429
    assert res.headers.get('Retry-After') == '42'


# ── Private-text safety in logs ──────────────────────────────────────────────

def test_send_ig_message_does_not_log_raw_body(monkeypatch, caplog):
    """send_ig_message must redact the Graph error body — only classified
    reason + short hash may be logged."""
    raw_body = '{"error":{"message":"User has blocked the page from sending messages","fbtrace_id":"FBTRACE_SECRET_X1Y2Z3"}}'

    class _R:
        status_code = 400
        text = raw_body

        def json(self):
            return json.loads(raw_body)

    class _Client:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def post(self, *_a, **_kw):
            return _R()

    monkeypatch.setattr(server.httpx, 'AsyncClient', _Client)
    with caplog.at_level(logging.ERROR, logger='mychat'):
        result = _run(server.send_ig_message('tok', 'biz', 'fan', {'text': 'hi'}))
    assert result['ok'] is False
    assert result['error'] == 'user_blocked_messages'
    # Raw fbtrace and raw error body must not appear in the log.
    assert 'FBTRACE_SECRET_X1Y2Z3' not in caplog.text
    assert 'User has blocked' not in caplog.text


def test_comment_seen_log_does_not_contain_text(monkeypatch, caplog):
    """The comment_seen log line must NOT include the raw comment text —
    only length and a short hash."""
    private_text = 'super secret comment text 12345'

    # Stub everything _handle_new_comment calls so we just hit the early
    # comment_seen log path.
    class _Empty:
        async def find_one(self, *_a, **_kw):
            return None
        def find(self, *_a, **_kw):
            class _C:
                def sort(self, *a, **kw): return self
                def limit(self, n): return self
                async def to_list(self, n): return []
            return _C()
        async def insert_one(self, doc):
            return SimpleNamespace(inserted_id='x')
        async def update_one(self, *a, **kw):
            return SimpleNamespace(matched_count=1, modified_count=1)

    db = SimpleNamespace(comments=_Empty(), automations=_Empty(), users=_Empty())
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, '_account_scoped_query',
                        lambda u, ig: {'user_id': u})
    monkeypatch.setattr(server, 'matchesAutomationRule',
                        lambda *a, **kw: {'matches': False})
    monkeypatch.setattr(server, '_fetch_latest_media_id',
                        lambda *a, **k: _async_none())
    monkeypatch.setattr(server, 'ws_manager',
                        SimpleNamespace(send=lambda *a, **k: _async_none()))

    user = {'id': 'u1', 'ig_user_id': 'biz1', 'meta_access_token': 't',
            'instagramConnected': True, 'email': 'u@e.com'}
    cdata = {
        'ig_comment_id': 'c1', 'commenter_id': 'fan', 'media_id': 'm1',
        'text': private_text,
        'timestamp': server.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+0000'),
    }
    with caplog.at_level(logging.INFO, logger='mychat'):
        _run(server._handle_new_comment(user, cdata, source='polling'))
    # The full text must never appear in any captured log line.
    assert private_text not in caplog.text


async def _async_none(*_a, **_kw):
    return None


# ── DM classification consistency: simple vs flow-entry path ─────────────────

def test_simple_dm_path_classifies_recipient_unavailable(monkeypatch):
    """Simple message-node DM path: failure reason is recipient_unavailable
    when send_ig_message returns the matching Graph error."""
    server._LAST_DM_FAILURE.set({})
    body = '{"error":{"message":"No message thread with this recipient"}}'

    class _R:
        status_code = 400
        text = body

        def json(self):
            return json.loads(body)

    class _Client:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def post(self, *_a, **_kw):
            return _R()

    monkeypatch.setattr(server.httpx, 'AsyncClient', _Client)
    res = _run(server.send_ig_dm_detailed('tok', 'biz', 'fan', 'hi'))
    assert res['ok'] is False
    assert res['failure_reason'] == 'recipient_unavailable'


def test_flow_entry_dm_path_classifies_same_reason(monkeypatch):
    """Comment-DM-flow-entry path: even when the Graph error is wrapped
    in send_ig_quick_reply (which goes through send_ig_message), the
    classification must reach _LAST_DM_FAILURE so the dashboard sees
    the same reason as the simple path. THIS is the audit point — we
    reproduced the bug where the flow-entry path lost classification."""
    server._LAST_DM_FAILURE.set({})
    body = '{"error":{"message":"User has blocked the page"}}'

    class _R:
        status_code = 400
        text = body

        def json(self):
            return json.loads(body)

    class _Client:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def post(self, *_a, **_kw):
            return _R()

    monkeypatch.setattr(server.httpx, 'AsyncClient', _Client)

    # ContextVar.set() inside asyncio.run is visible only within that
    # task. To verify the flow-entry path's classification we call the
    # send AND read the contextvar inside the same async run — exactly
    # what production execute_flow does.
    async def _send_and_read():
        # send_ig_quick_reply is what the flow-entry helper uses for the
        # opening message + button case.
        res = await server.send_ig_quick_reply(
            'tok', 'biz', 'fan', 'opening text', 'button', 'payload',
        )
        return res, server._LAST_DM_FAILURE.get() or {}

    res, last = _run(_send_and_read())
    assert res['ok'] is False
    assert last.get('failure_reason') == 'user_blocked_messages', \
        f'flow-entry path lost classification: {last!r}'


def test_dm_classification_is_consistent_across_paths(monkeypatch):
    """For the same Graph error body, simple path and flow-entry path
    must yield the SAME failure_reason."""
    server._LAST_DM_FAILURE.set({})
    body = '{"error":{"message":"Outside the messaging window"}}'

    class _R:
        status_code = 400
        text = body

        def json(self):
            return json.loads(body)

    class _Client:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def post(self, *_a, **_kw):
            return _R()

    monkeypatch.setattr(server.httpx, 'AsyncClient', _Client)

    async def _path(send_coro_factory):
        await send_coro_factory()
        return (server._LAST_DM_FAILURE.get() or {}).get('failure_reason')

    simple_reason = _run(_path(
        lambda: server.send_ig_dm('tok', 'biz', 'fan', 'hi'),
    ))
    flow_reason = _run(_path(
        lambda: server.send_ig_url_button(
            'tok', 'biz', 'fan', 'text', 'btn', 'https://e.com',
        ),
    ))

    assert simple_reason == flow_reason
    assert simple_reason in ('recipient_unavailable',), simple_reason
