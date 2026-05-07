"""Phase 1.4F regression tests: specific-rule public reply must never be
silently 'disabled' while DM succeeds.

Production case: a specific-post automation with public reply + DM
configured ended up with reply_status=disabled, dm_status=success,
action_status=success on first processing, then comment_processed_unknown_state
on the next poll. These tests lock in the invariant that this is not
allowed.
"""
import asyncio
import os
import sys
from datetime import datetime, timedelta
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


def _now():
    return datetime.utcnow()


def _user_doc():
    return {
        'id': 'u1',
        'email': 'u1@example.com',
        'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA',
        'meta_access_token': 'token-a',
        'instagramHandle': 'account_a',
    }


def _comment_payload(comment_id='c1', media_id='m_specific', text='link please'):
    return {
        'ig_comment_id': comment_id,
        'media_id': media_id,
        'commenter_id': 'contact1',
        'commenter_username': 'contact',
        'text': text,
        'timestamp': _now(),
    }


def _install_db(monkeypatch, automations, comments=None):
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA',
                 igUserId='igA', username='account_a', accessToken='token-a'),
        _user(active_instagram_account_id='accA', ig_user_id='igA'),
        automations=automations,
        comments=comments or [],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(
        server,
        'ws_manager',
        SimpleNamespace(send=lambda *_args, **_kwargs: asyncio.sleep(0)),
    )
    return fake_db


def _automation(nodes, edges, **overrides):
    activation = _now() - timedelta(minutes=5)
    doc = {
        'id': 'auto_specific',
        'user_id': 'u1',
        'status': 'active',
        'trigger': 'comment:m_specific',
        'match': 'any',
        'post_scope': 'specific',
        'media_id': 'm_specific',
        'instagramAccountDbId': 'accA',
        'instagramAccountId': 'igA',
        'activationStartedAt': activation,
        'reply_under_post': True,
        'nodes': nodes,
        'edges': edges,
    }
    doc.update(overrides)
    return doc


def _specific_rule_with_reply_and_dm():
    return _automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'Specific public reply',
                      'replies': ['Specific public reply']}},
            {'id': 'n_dm', 'type': 'message',
             'data': {'text': 'Specific DM body'}},
        ],
        [
            {'source': 'n_trigger', 'target': 'n_reply'},
            {'source': 'n_trigger', 'target': 'n_dm'},
        ],
        comment_reply='Specific public reply',
    )


def _specific_rule_top_level_reply_only():
    """Top-level comment_reply set, no graph reply node — _ensure_public_reply_node
    must synthesise one."""
    return _automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_dm', 'type': 'message',
             'data': {'text': 'Specific DM body'}},
        ],
        [
            {'source': 'n_trigger', 'target': 'n_dm'},
        ],
        comment_reply='Top-level only public reply',
    )


def _specific_rule_dm_only():
    """No public reply at all — reply_status='disabled' is valid here."""
    return _automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_dm', 'type': 'message',
             'data': {'text': 'DM only body'}},
        ],
        [
            {'source': 'n_trigger', 'target': 'n_dm'},
        ],
        reply_under_post=False,
        comment_reply='',
    )


def _specific_rule_graph_reply_only():
    """Graph reply_comment node only, no top-level fields."""
    return _automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'Graph reply'}},
            {'id': 'n_dm', 'type': 'message',
             'data': {'text': 'Specific DM body'}},
        ],
        [
            {'source': 'n_trigger', 'target': 'n_reply'},
            {'source': 'n_trigger', 'target': 'n_dm'},
        ],
        # Critical: no comment_reply top-level field at all.
    )


def _broad_rule():
    return _automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'Broad reply'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_reply'}],
        id='auto_broad',
        trigger='comment:any',
        post_scope='any',
        media_id=None,
        comment_reply='Broad reply',
    )


def _reply_provider_ok():
    return {
        'ok': True,
        'status_code': 200,
        'provider_response_ok': True,
        'provider_comment_id': 'replyProof',
        'body': {'id': 'replyProof'},
    }


# ── A. Specific rule with reply + DM cannot end with reply_status=disabled when DM succeeds ──

def test_a_specific_rule_with_reply_dm_cannot_end_disabled_when_dm_succeeds(monkeypatch):
    db = _install_db(monkeypatch, [_specific_rule_with_reply_and_dm()])
    reply_calls = []
    dm_calls = []

    async def reply_ok(*_a, **_kw):
        reply_calls.append('reply')
        return _reply_provider_ok()

    async def dm_ok(*_a, **_kw):
        dm_calls.append('dm')
        return {'ok': True, 'failure_reason': None, 'retryable': False}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    result = _run(server._handle_new_comment(
        _user_doc(), _comment_payload(comment_id='cA1'), source='webhook'
    ))
    saved = db.comments.docs[0]

    assert reply_calls == ['reply']
    assert dm_calls == ['dm']
    assert saved['reply_status'] == 'success'
    assert saved['reply_provider_response_ok'] is True
    assert saved['dm_status'] == 'success'
    # Critical: reply_status must NEVER be 'disabled' when rule has public reply.
    assert saved['reply_status'] != 'disabled'
    assert result['action_status'] == 'success'


# ── B. Top-level comment_reply only -> public_reply_required=True ──

def test_b_top_level_reply_only_marks_public_reply_required(monkeypatch):
    rule = _specific_rule_top_level_reply_only()
    assert server._automation_public_reply_required(rule) is True
    assert server._automation_public_reply_texts(rule) == ['Top-level only public reply']


# ── C. Graph reply_comment only -> public_reply_required=True ──

def test_c_graph_reply_only_marks_public_reply_required(monkeypatch):
    rule = _specific_rule_graph_reply_only()
    assert server._automation_public_reply_required(rule) is True
    assert 'Graph reply' in server._automation_public_reply_texts(rule)


# ── D. Existing comment with disabled+success+rule-has-reply → repaired to failed_retryable ──

def test_d_dedup_recovers_disabled_reply_success_dm(monkeypatch):
    """The exact production failure: existing doc has reply_status=disabled
    + dm_status=success + previous_status=success while the matched rule
    DOES require a public reply. Dedup must repair to failed_retryable
    instead of returning comment_processed_unknown_state."""
    rule = _specific_rule_with_reply_and_dm()
    existing = {
        'id': 'doc_zombie',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': 'c_zombie',
        'media_id': 'm_specific',
        'rule_id': 'auto_specific',
        'matched_rule_scope': 'specific_post',
        'replied': False,
        'reply_status': 'disabled',
        'dm_status': 'success',
        'action_status': 'success',
        'reply_provider_response_ok': False,
        'matched': True,
    }
    db = _install_db(monkeypatch, [rule], comments=[existing])

    res = _run(server._handle_new_comment(
        _user_doc(),
        _comment_payload(comment_id='c_zombie'),
        source='polling',
    ))

    # Recovery path triggered, not unknown_state.
    assert res.get('reason') == 'public_reply_required_recovery'
    assert res.get('action_status') == 'failed_retryable'
    saved = db.comments.docs[0]
    assert saved['reply_status'] == 'failed_retryable'
    assert saved['reply_skip_reason'] == 'public_reply_required_not_attempted'
    assert saved['queued'] is True
    assert saved['next_retry_at'] is not None
    # Critical: DM must NOT be touched. DM stayed at success.
    assert saved['dm_status'] == 'success'


# ── E. Queue retry path: retry-reply enqueues without resending DM ──

def test_e_retry_reply_endpoint_does_not_resend_dm(monkeypatch):
    rule = _specific_rule_with_reply_and_dm()
    existing = {
        'id': 'doc_retry',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': 'c_retry',
        'media_id': 'm_specific',
        'rule_id': 'auto_specific',
        'matched': True,
        'replied': False,
        'reply_status': 'failed_retryable',
        'reply_failure_reason': 'public_reply_required_not_attempted',
        'dm_status': 'success',
        'action_status': 'failed_retryable',
        'reply_provider_response_ok': False,
    }
    db = _install_db(monkeypatch, [rule], comments=[existing])

    dm_called = []

    async def dm_should_not_run(*_a, **_kw):
        dm_called.append('dm')
        return {'ok': True}

    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_should_not_run)

    result = _run(server.comment_retry_reply('c_retry', user_id='u1'))

    # Retry queue endpoint flips reply_status to pending, sets next_retry_at.
    assert result['ok'] is True
    saved = db.comments.docs[0]
    assert saved['reply_status'] == 'pending'
    assert saved['queued'] is True
    # DM must remain success and must NOT be touched/resent.
    assert saved['dm_status'] == 'success'
    assert dm_called == []


# ── F. Broad rule still works (regression guard) ──

def test_f_broad_rule_still_works(monkeypatch):
    db = _install_db(monkeypatch, [_broad_rule()])

    async def reply_ok(*_a, **_kw):
        return _reply_provider_ok()

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)

    result = _run(server._handle_new_comment(
        _user_doc(),
        _comment_payload(comment_id='c_broad', media_id='m_other'),
        source='webhook',
    ))
    saved = db.comments.docs[0]
    assert saved['reply_status'] == 'success'
    assert saved['reply_provider_response_ok'] is True
    assert result['action_status'] == 'success'


# ── G. Specific rule wins over broad — no fallback even if specific fails ──

def test_g_specific_rule_wins_over_broad_no_fallback(monkeypatch):
    """When both specific and broad rules exist on the same media, only the
    specific rule must fire. The fix here must NOT add a broad-fallback."""
    specific = _specific_rule_with_reply_and_dm()
    specific['id'] = 'auto_specific_priority'
    broad = _broad_rule()
    db = _install_db(monkeypatch, [specific, broad])

    matched_rule_ids = []

    async def reply_capture(*_a, **_kw):
        matched_rule_ids.append('reply')
        return _reply_provider_ok()

    async def dm_ok(*_a, **_kw):
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_capture)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    result = _run(server._handle_new_comment(
        _user_doc(),
        _comment_payload(comment_id='c_priority', media_id='m_specific'),
        source='webhook',
    ))
    saved = db.comments.docs[0]
    # Specific rule should be matched, not broad.
    assert saved.get('rule_id') == 'auto_specific_priority'
    assert result['action_status'] == 'success'


# ── H. Truly DM-only specific rule → reply_status=disabled remains valid ──

def test_h_dm_only_specific_rule_keeps_disabled_reply(monkeypatch):
    db = _install_db(monkeypatch, [_specific_rule_dm_only()])

    async def dm_ok(*_a, **_kw):
        return {'ok': True}

    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    result = _run(server._handle_new_comment(
        _user_doc(),
        _comment_payload(comment_id='c_dm_only'),
        source='webhook',
    ))
    saved = db.comments.docs[0]

    # Rule has no public reply - disabled is a valid terminal state.
    assert saved['reply_status'] == 'disabled'
    assert saved['dm_status'] == 'success'
    # action_status can be 'success' since no reply was required.
    assert result['action_status'] == 'success'


# ── public_reply_required helper unit tests ──

def test_public_reply_required_helper():
    """Direct unit coverage of the canonical helper."""
    assert server._automation_public_reply_required(None) is False
    assert server._automation_public_reply_required({}) is False
    # Top-level field set
    assert server._automation_public_reply_required({'comment_reply': 'hi'}) is True
    # reply_under_post=False overrides
    assert server._automation_public_reply_required(
        {'comment_reply': 'hi', 'reply_under_post': False}
    ) is False
    # Graph node only
    assert server._automation_public_reply_required({
        'nodes': [{'type': 'reply_comment', 'data': {'text': 'graph'}}]
    }) is True
    # Empty graph node + empty top level
    assert server._automation_public_reply_required({
        'nodes': [{'type': 'reply_comment', 'data': {}}],
        'comment_reply': '',
    }) is False


def test_compute_action_status_treats_failed_retryable_as_failure():
    """Stop the legacy bug where reply_status=failed_retryable + dm=success
    returned 'success' or just 'failed' instead of partial_success."""
    assert server._compute_comment_action_status('failed_retryable', 'success') == 'partial_success'
    assert server._compute_comment_action_status('failed_permanent', 'success') == 'partial_success'
    # disabled + success stays success only when public reply is not required
    # — this is the existing (correct) math.
    assert server._compute_comment_action_status('disabled', 'success') == 'success'
