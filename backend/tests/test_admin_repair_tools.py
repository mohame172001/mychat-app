"""Phase 1.4H: tests for admin repair endpoints (specific-rule public reply).

Covers:
A. diagnosis endpoint detects forbidden state
   (public_reply_required=true + reply_status=disabled + dm_status=success)
B. repair endpoint converts forbidden state to failed_retryable + skip_reason
   without touching DM
C. repair endpoint refuses if reply_provider_response_ok=true
D. endpoint refuses (404) if user does not own the comment AND is not admin
E. process-retry-now does not resend DM when dm_status=success
F. diagnosis never returns raw text (length/hash only)
"""
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

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

from test_instagram_token_refresh import FakeDB, _account, _run, _user  # noqa: E402


PRIVATE_REPLY = 'do not leak this reply text 12345'
PRIVATE_DM = 'do not leak this DM text 67890'


def _rule(public_reply=True):
    nodes = [{'id': 'n_trigger', 'type': 'trigger', 'data': {}}]
    edges = []
    if public_reply:
        nodes.append({'id': 'n_reply', 'type': 'reply_comment',
                      'data': {'text': PRIVATE_REPLY, 'replies': [PRIVATE_REPLY]}})
        edges.append({'source': 'n_trigger', 'target': 'n_reply'})
    nodes.append({'id': 'n_dm', 'type': 'message', 'data': {'text': PRIVATE_DM}})
    edges.append({'source': 'n_trigger', 'target': 'n_dm'})
    return {
        'id': 'auto_specific',
        'user_id': 'u1',
        'status': 'active',
        'trigger': 'comment:m1',
        'match': 'any',
        'post_scope': 'specific',
        'media_id': 'm1',
        'instagramAccountDbId': 'accA',
        'instagramAccountId': 'igA',
        'reply_under_post': public_reply,
        'comment_reply': PRIVATE_REPLY if public_reply else '',
        'nodes': nodes,
        'edges': edges,
    }


def _zombie_comment(user_id='u1', has_proof=False):
    return {
        'id': 'doc_zombie',
        'user_id': user_id,
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': '18004285310876247',
        'media_id': 'm1',
        'rule_id': 'auto_specific',
        'matched_rule_scope': 'specific_post',
        'matched': True,
        'replied': False,
        'reply_status': 'disabled',
        'dm_status': 'success',
        'action_status': 'success',
        'reply_provider_response_ok': has_proof,
        'text': 'commenter wrote this private text — must not leak',
    }


def _install(monkeypatch, *, comment, rule, user_email='u1@example.com'):
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA',
              email=user_email),
        automations=[rule] if rule else [],
        comments=[comment] if comment else [],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ws_manager',
                        SimpleNamespace(send=lambda *_a, **_kw: asyncio.sleep(0)))
    return fake_db


def _enable_flag(monkeypatch, value=True):
    monkeypatch.setattr(server, 'ENABLE_ADMIN_REPAIR_TOOLS', value)


# ── A. forbidden state detection ────────────────────────────────────────────

def test_a_diagnosis_detects_forbidden_state(monkeypatch):
    _install(monkeypatch, comment=_zombie_comment(), rule=_rule(public_reply=True))
    _enable_flag(monkeypatch)
    res = _run(server.admin_specific_reply_diagnosis(
        '18004285310876247', user_id='u1'
    ))
    assert res['public_reply_required'] is True
    assert res['reply_status'] == 'disabled'
    assert res['dm_status'] == 'success'
    assert res['forbidden_state_detected'] is True
    assert res['repairable'] is True
    assert res['reply_provider_response_ok'] is False


# ── B. repair flips to failed_retryable, DM untouched ───────────────────────

def test_b_repair_converts_forbidden_state(monkeypatch):
    db = _install(monkeypatch, comment=_zombie_comment(), rule=_rule(public_reply=True))
    _enable_flag(monkeypatch)
    res = _run(server.admin_repair_specific_public_reply(
        '18004285310876247', user_id='u1'
    ))
    assert res['ok'] is True
    assert res['repaired'] is True
    saved = db.comments.docs[0]
    assert saved['reply_status'] == 'failed_retryable'
    assert saved['reply_skip_reason'] == 'public_reply_required_not_attempted'
    assert saved['next_retry_at'] is not None
    # DM state must be unchanged.
    assert saved['dm_status'] == 'success'
    assert res['after']['dm_status'] == 'success'


# ── C. repair refuses if provider proof exists ──────────────────────────────

def test_c_repair_refuses_with_provider_proof(monkeypatch):
    db = _install(
        monkeypatch,
        comment=_zombie_comment(has_proof=True),
        rule=_rule(public_reply=True),
    )
    _enable_flag(monkeypatch)
    res = _run(server.admin_repair_specific_public_reply(
        '18004285310876247', user_id='u1'
    ))
    assert res['ok'] is False
    assert res['repaired'] is False
    assert 'reply_provider_response_ok_already_true' in res['reason']
    # Comment doc must be unchanged.
    saved = db.comments.docs[0]
    assert saved['reply_status'] == 'disabled'
    assert saved['reply_provider_response_ok'] is True


# ── D. ownership check (non-admin, non-owner) ───────────────────────────────

def test_d_endpoint_404s_for_non_owner_non_admin(monkeypatch):
    # Comment belongs to u_other.
    other_comment = _zombie_comment(user_id='u_other')
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA',
              email='u1@example.com'),
        automations=[_rule(public_reply=True)],
        comments=[other_comment],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ws_manager',
                        SimpleNamespace(send=lambda *_a, **_kw: asyncio.sleep(0)))
    _enable_flag(monkeypatch)
    # Caller is u1, comment is owned by u_other, u1 not in ADMIN_EMAILS.
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_specific_reply_diagnosis(
            '18004285310876247', user_id='u1'
        ))
    assert exc.value.status_code == 404


def test_d2_endpoint_404s_when_flag_off_and_not_admin(monkeypatch):
    """When ENABLE_ADMIN_REPAIR_TOOLS=false and user is not admin, even
    owners get 404 — flag-off is total."""
    _install(monkeypatch, comment=_zombie_comment(), rule=_rule(public_reply=True))
    monkeypatch.setattr(server, 'ENABLE_ADMIN_REPAIR_TOOLS', False)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_specific_reply_diagnosis(
            '18004285310876247', user_id='u1'
        ))
    assert exc.value.status_code == 404


def test_d3_admin_email_bypasses_flag_off(monkeypatch):
    """Admin email works even when ENABLE_ADMIN_REPAIR_TOOLS=false."""
    _install(monkeypatch, comment=_zombie_comment(), rule=_rule(public_reply=True),
             user_email='admin@mychat.app')
    monkeypatch.setattr(server, 'ENABLE_ADMIN_REPAIR_TOOLS', False)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', {'admin@mychat.app'})
    res = _run(server.admin_specific_reply_diagnosis(
        '18004285310876247', user_id='u1'
    ))
    assert res['forbidden_state_detected'] is True


# ── E. process-retry-now never resends DM ───────────────────────────────────

def test_e_process_retry_does_not_resend_dm(monkeypatch):
    db = _install(monkeypatch, comment=_zombie_comment(), rule=_rule(public_reply=True))
    _enable_flag(monkeypatch)

    dm_called = []

    async def dm_should_not_run(*_a, **_kw):
        dm_called.append('dm')
        return {'ok': True}

    async def reply_ok(*_a, **_kw):
        return {
            'ok': True, 'status_code': 200,
            'provider_response_ok': True,
            'provider_comment_id': 'replyProof',
            'body': {'id': 'replyProof'},
        }

    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_should_not_run)
    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)

    # The retry path expects the comment to be eligible. Set it up.
    comment_id = db.comments.docs[0]['id']
    db.comments.docs[0]['reply_status'] = 'failed_retryable'
    db.comments.docs[0]['reply_failure_reason'] = 'public_reply_required_not_attempted'

    res = _run(server.admin_process_retry_now(
        '18004285310876247', user_id='u1'
    ))

    # DM must NOT be called.
    assert dm_called == []
    # DM state must stay success.
    assert res['dm_status_after'] == 'success'
    assert res['dm_attempted'] is False


def test_e2_process_retry_short_circuits_when_provider_proof_exists(monkeypatch):
    _install(monkeypatch, comment=_zombie_comment(has_proof=True),
             rule=_rule(public_reply=True))
    _enable_flag(monkeypatch)

    reply_called = []

    async def reply_should_not_run(*_a, **_kw):
        reply_called.append('reply')
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_should_not_run)
    res = _run(server.admin_process_retry_now(
        '18004285310876247', user_id='u1'
    ))
    assert res['public_reply_attempted'] is False
    assert res['reply_provider_response_ok'] is True
    assert reply_called == []


# ── F. diagnosis never returns raw text ─────────────────────────────────────

def test_f_diagnosis_never_returns_raw_text(monkeypatch):
    _install(monkeypatch, comment=_zombie_comment(), rule=_rule(public_reply=True))
    _enable_flag(monkeypatch)
    res = _run(server.admin_specific_reply_diagnosis(
        '18004285310876247', user_id='u1'
    ))
    serialized = repr(res)
    # Reply text and DM text must not appear anywhere in the response.
    assert PRIVATE_REPLY not in serialized
    assert PRIVATE_DM not in serialized
    # Comment text must not appear either.
    assert 'commenter wrote this private text' not in serialized
    # Length and hash must be present and non-zero.
    assert res['public_reply_text_length'] > 0
    assert res['public_reply_text_hash']
    assert res['dm_text_length'] > 0
    assert res['dm_text_hash']


def test_g_admin_tools_enabled_reflects_flag_and_admin(monkeypatch):
    _install(monkeypatch, comment=None, rule=None)
    monkeypatch.setattr(server, 'ENABLE_ADMIN_REPAIR_TOOLS', True)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())
    res = _run(server.admin_tools_enabled(user_id='u1'))
    assert res['enabled'] is True
    assert res['flag'] is True
    assert res['is_admin'] is False

    monkeypatch.setattr(server, 'ENABLE_ADMIN_REPAIR_TOOLS', False)
    res = _run(server.admin_tools_enabled(user_id='u1'))
    assert res['enabled'] is False

    monkeypatch.setattr(server, 'ADMIN_EMAILS', {'u1@example.com'})
    res = _run(server.admin_tools_enabled(user_id='u1'))
    assert res['enabled'] is True
    assert res['is_admin'] is True
