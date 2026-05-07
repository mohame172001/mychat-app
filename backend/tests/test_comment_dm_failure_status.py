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


def _automation(nodes, edges, **overrides):
    now = datetime.utcnow()
    doc = {
        'id': 'auto1',
        'user_id': 'u1',
        'status': 'active',
        'trigger': 'comment:any',
        'match': 'any',
        'instagramAccountDbId': 'accA',
        'instagramAccountId': 'igA',
        'activationStartedAt': now - timedelta(minutes=5),
        'nodes': nodes,
        'edges': edges,
    }
    doc.update(overrides)
    return doc


def _comment_payload(comment_id='comment1', text='Price'):
    return {
        'ig_comment_id': comment_id,
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'commenter_username': 'contact',
        'text': text,
        'timestamp': datetime.utcnow(),
    }


def _user_doc():
    return {
        'id': 'u1',
        'email': 'u1@example.com',
        'active_instagram_account_id': 'accA',
        'ig_user_id': 'igA',
        'meta_access_token': 'token-a',
        'instagramHandle': 'account_a',
    }


def _reply_provider_ok(reply_id='reply1'):
    return {
        'ok': True,
        'status_code': 200,
        'provider_response_ok': True,
        'provider_comment_id': reply_id,
        'body': {'id': reply_id},
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


def _reply_and_dm_automation():
    return _automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Thanks'}},
            {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Here is the link'}},
        ],
        [
            {'source': 'n_trigger', 'target': 'n_reply'},
            {'source': 'n_trigger', 'target': 'n_dm'},
        ],
    )


def _specific_reply_automation(rule_id='specific1', media_id='media1', with_dm=False):
    nodes = [
        {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
        {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Specific reply'}},
    ]
    edges = [{'source': 'n_trigger', 'target': 'n_reply'}]
    if with_dm:
        nodes.append({'id': 'n_dm', 'type': 'message', 'data': {'text': 'Specific DM'}})
        edges.append({'source': 'n_trigger', 'target': 'n_dm'})
    return _automation(
        nodes,
        edges,
        id=rule_id,
        trigger=f'comment:{media_id}',
        post_scope='specific',
        media_id=media_id,
    )


def _broad_reply_automation(rule_id='broad1'):
    return _automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Broad reply'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_reply'}],
        id=rule_id,
        trigger='comment:any',
        post_scope='any',
    )


def test_reply_enabled_dm_success_records_success(monkeypatch):
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])
    calls = []

    async def reply_ok(*_args):
        calls.append('reply')
        return _reply_provider_ok()

    async def dm_ok(*_args):
        calls.append('dm')
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert calls == ['reply', 'dm']
    assert saved['reply_status'] == 'success'
    assert saved['reply_provider_response_ok'] is True
    assert saved['reply_provider_comment_id'] == 'reply1'
    assert saved['dm_status'] == 'success'
    assert saved['action_status'] == 'success'
    assert result['action_status'] == 'success'


def test_reply_ok_without_provider_proof_does_not_mark_success(monkeypatch):
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Thanks'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_reply'}],
    )])

    async def reply_without_proof(*_args):
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_without_proof)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert saved['reply_status'] == 'failed'
    assert saved['reply_provider_response_ok'] is False
    assert saved['reply_failure_reason'] == 'missing_provider_confirmation'
    assert saved['action_status'] == 'failed_retryable'
    assert result['action_status'] == 'failed_retryable'


def test_reply_api_failure_does_not_set_reply_success(monkeypatch):
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Thanks'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_reply'}],
    )])

    async def reply_failed(*_args):
        return {'ok': False, 'status_code': 400, 'failure_reason': 'permission_error', 'retryable': False}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_failed)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert saved.get('replied') is False
    assert saved['reply_status'] == 'failed'
    assert saved['reply_provider_response_ok'] is False
    assert saved['reply_failure_reason'] == 'permission_error'
    assert saved['action_status'] == 'failed_permanent'
    assert result['action_status'] == 'failed_permanent'


def test_dm_success_alone_does_not_set_reply_success(monkeypatch):
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Here is the link'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_dm'}],
    )])

    async def dm_ok(*_args):
        return {'ok': True}

    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert saved['reply_status'] == 'disabled'
    assert saved.get('reply_provider_response_ok') is not True
    assert saved['dm_status'] == 'success'
    assert saved['action_status'] == 'success'
    assert result['action_status'] == 'success'


def test_classifier_prefers_recipient_unavailable_over_generic_permission():
    result = server.classify_instagram_send_error({
        'error': {
            'message': 'This recipient cannot receive messages from this business.',
            'code': 10,
            'error_subcode': 2534022,
        }
    }, 400)

    assert result['failure_reason'] == 'recipient_unavailable'
    assert result['retryable'] is False
    assert result['provider_code'] == 10


def test_dm_recipient_unavailable_does_not_block_public_reply(monkeypatch):
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])
    calls = []

    async def reply_ok(*_args):
        calls.append('reply')
        return _reply_provider_ok()

    async def dm_unavailable(*_args):
        calls.append('dm')
        return {
            'ok': False,
            'failure_reason': 'recipient_unavailable',
            'retryable': False,
            'provider_code': 10,
            'provider_subcode': 2534022,
        }

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_unavailable)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert calls == ['reply', 'dm']
    assert saved['replied'] is True
    assert saved['reply_status'] == 'success'
    assert saved['reply_provider_response_ok'] is True
    assert saved['dm_status'] == 'failed'
    assert saved['dm_failure_reason'] == 'recipient_unavailable'
    assert saved['action_status'] == 'partial_success'
    assert result['action_status'] == 'partial_success'

    calls.clear()
    duplicate = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='polling'))
    assert duplicate['reason'] == 'comment_already_partial_success'
    assert calls == []


def test_reply_disabled_dm_unavailable_records_failed(monkeypatch):
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Here is the link'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_dm'}],
    )])

    async def dm_unavailable(*_args):
        return {'ok': False, 'failure_reason': 'messaging_not_allowed', 'retryable': False}

    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_unavailable)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert saved['reply_status'] == 'disabled'
    assert saved['dm_status'] == 'failed'
    assert saved['action_status'] == 'failed_permanent'
    assert result['action_status'] == 'failed_permanent'


def test_reply_enabled_dm_disabled_records_success(monkeypatch):
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Thanks'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_reply'}],
    )])

    async def reply_ok(*_args):
        return _reply_provider_ok()

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    saved = db.comments.docs[0]

    assert saved['reply_status'] == 'success'
    assert saved['dm_status'] == 'disabled'
    assert saved['action_status'] == 'success'
    assert result['action_status'] == 'success'


def test_temporary_dm_error_retries_dm_without_duplicate_reply(monkeypatch):
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])
    calls = []

    async def reply_ok(*_args):
        calls.append('reply')
        return _reply_provider_ok()

    dm_results = [
        {'ok': False, 'failure_reason': 'temporary_graph_error', 'retryable': True},
        {'ok': True},
    ]

    async def dm_flaky(*_args):
        calls.append('dm')
        return dm_results.pop(0)

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_flaky)

    first = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    assert first['action_status'] == 'partial_success'
    assert calls == ['reply', 'dm']

    second = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='polling'))
    saved = db.comments.docs[0]

    assert second['reprocessed'] is True
    assert calls == ['reply', 'dm', 'dm']
    assert saved['reply_status'] == 'success'
    assert saved['dm_status'] == 'success'
    assert saved['action_status'] == 'success'


def test_legacy_success_without_provider_proof_retries_public_reply(monkeypatch):
    now = datetime.utcnow()
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Thanks'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_reply'}],
    )], comments=[{
        'id': 'legacy1',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': 'legacyComment',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'text': 'PDF',
        'matched': True,
        'rule_id': 'auto1',
        'replied': True,
        'reply_status': 'success',
        'dm_status': 'disabled',
        'action_status': 'success',
        'timestamp': now,
    }])
    calls = []

    async def reply_ok(*_args):
        calls.append('reply')
        return _reply_provider_ok('reply_after_legacy')

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)

    result = _run(server._handle_new_comment(
        _user_doc(),
        _comment_payload(comment_id='legacyComment', text='PDF'),
        source='polling',
    ))
    saved = db.comments.docs[0]

    assert result['reprocessed'] is True
    assert calls == ['reply']
    assert saved['reply_status'] == 'success'
    assert saved['reply_provider_response_ok'] is True
    assert saved['reply_provider_comment_id'] == 'reply_after_legacy'
    assert saved['action_status'] == 'success'


def test_comment_diagnostics_is_scoped_and_redacted(monkeypatch):
    db = _install_db(monkeypatch, [], comments=[{
        'id': 'doc1',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'ig_comment_id': 'commentDiag',
        'media_id': 'media1',
        'text': 'private full comment text',
        'reply_status': 'success',
        'reply_provider_response_ok': True,
        'reply_provider_comment_id': 'replyDiag',
        'dm_status': 'failed',
        'action_status': 'partial_success',
        'dm_failure_reason': 'recipient_unavailable',
        'rule_id': 'auto1',
        'source': 'webhook',
    }])

    result = _run(server.comment_diagnostics('commentDiag', user_id='u1'))

    # After the diagnostics shape unification, commentId is the internal
    # stable doc id and igCommentId is the Instagram-side comment id.
    assert result['commentId'] == 'doc1'
    assert result['igCommentId'] == 'commentDiag'
    assert result['text_length'] == len('private full comment text')
    assert 'private full comment text' not in str(result)
    assert 'accessToken' not in str(result)
    assert result['dm_failure_reason'] == 'recipient_unavailable'
    assert result['reply_provider_confirmation_exists'] is True
    assert result['reply_provider_comment_id_exists'] is True


def test_why_not_replied_exposes_legacy_provider_proof_state(monkeypatch):
    _install_db(monkeypatch, [], comments=[{
        'id': 'legacyWhy',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'ig_comment_id': 'legacyWhyComment',
        'media_id': 'media1',
        'text': 'private body',
        'matched': True,
        'rule_id': 'auto1',
        'replied': True,
        'reply_status': 'success',
        'dm_status': 'disabled',
        'action_status': 'success',
    }])

    result = _run(server.comment_why_not_replied('legacyWhyComment', user_id='u1'))

    assert result['reply_provider_confirmation_exists'] is False
    assert result['reply_provider_comment_id_exists'] is False
    assert result['legacy_reply_success_without_provider_confirmation'] is True
    assert result['manual_retry_allowed'] is True
    assert result['thinks_replied_reason'] == 'legacy_success_without_provider_confirmation'


def test_retry_reply_endpoint_enqueues_legacy_false_success(monkeypatch):
    now = datetime.utcnow()
    db = _install_db(monkeypatch, [_reply_and_dm_automation()], comments=[{
        'id': 'legacyRetry',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': 'legacyRetryComment',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'text': 'PDF',
        'matched': True,
        'rule_id': 'auto1',
        'replied': True,
        'reply_status': 'success',
        'dm_status': 'disabled',
        'action_status': 'success',
        'timestamp': now,
    }])

    result = _run(server.comment_retry_reply('legacyRetryComment', user_id='u1'))
    saved = db.comments.docs[0]

    assert result['ok'] is True
    assert saved['reply_status'] == 'pending'
    assert saved['action_status'] == 'pending'
    assert saved['queued'] is True
    assert saved['next_retry_at'] is not None
    assert saved['reply_provider_response_ok'] is False


def test_retry_reply_endpoint_refuses_provider_confirmed_reply(monkeypatch):
    _install_db(monkeypatch, [_reply_and_dm_automation()], comments=[{
        'id': 'proofedRetry',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'ig_comment_id': 'proofedRetryComment',
        'media_id': 'media1',
        'text': 'PDF',
        'matched': True,
        'rule_id': 'auto1',
        'replied': True,
        'reply_status': 'success',
        'reply_provider_response_ok': True,
        'reply_provider_comment_id': 'replyProof',
        'dm_status': 'disabled',
        'action_status': 'success',
    }])

    with pytest.raises(server.HTTPException) as exc:
        _run(server.comment_retry_reply('proofedRetryComment', user_id='u1'))

    assert exc.value.status_code == 409


def test_legacy_repair_moves_false_success_to_retry_queue(monkeypatch):
    now = datetime.utcnow()
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Thanks'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_reply'}],
        activationStartedAt=now - timedelta(minutes=5),
    )], comments=[{
        'id': 'legacyRepair',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': 'legacyRepairComment',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'text': 'PDF',
        'matched': True,
        'rule_id': 'auto1',
        'replied': True,
        'reply_status': 'success',
        'dm_status': 'disabled',
        'action_status': 'success',
        'effective_timestamp': now,
    }, {
        'id': 'proofRepair',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'ig_comment_id': 'proofRepairComment',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'text': 'PDF',
        'matched': True,
        'rule_id': 'auto1',
        'replied': True,
        'reply_status': 'success',
        'reply_provider_response_ok': True,
        'reply_provider_comment_id': 'replyProof',
        'dm_status': 'disabled',
        'action_status': 'success',
        'effective_timestamp': now,
    }])

    summary = _run(server._repair_legacy_reply_success_without_provider_proof())
    legacy = next(doc for doc in db.comments.docs if doc['id'] == 'legacyRepair')
    proofed = next(doc for doc in db.comments.docs if doc['id'] == 'proofRepair')

    assert summary['repaired'] == 1
    assert legacy['reply_status'] == 'pending'
    assert legacy['action_status'] == 'failed_retryable'
    assert legacy['skip_reason'] == 'legacy_success_without_provider_confirmation'
    assert legacy['next_retry_at'] is not None
    assert proofed['reply_status'] == 'success'
    assert proofed['reply_provider_response_ok'] is True


def test_specific_post_rule_wins_over_broad_rule(monkeypatch):
    db = _install_db(monkeypatch, [
        _broad_reply_automation('broad_first'),
        _specific_reply_automation('specific_media1', 'media1'),
    ])
    calls = []

    async def reply_ok(_token, _comment_id, text):
        calls.append(text)
        return _reply_provider_ok('reply_specific')

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)

    result = _run(server._handle_new_comment(
        _user_doc(), _comment_payload(comment_id='priority1', text='hello'), source='webhook'
    ))
    saved = db.comments.docs[0]

    assert result['rule_id'] == 'specific_media1'
    assert saved['matched_rule_id'] == 'specific_media1'
    assert saved['matched_rule_priority'] == 1
    assert saved['matched_rule_scope'] == 'specific_post_exact'
    assert saved['broad_rules_skipped_due_specific_match'] is True
    assert calls == ['Specific reply']


def test_broad_rule_handles_comment_when_no_specific_media_matches(monkeypatch):
    db = _install_db(monkeypatch, [
        _specific_reply_automation('specific_other', 'otherMedia'),
        _broad_reply_automation('broad_any'),
    ])
    calls = []

    async def reply_ok(_token, _comment_id, text):
        calls.append(text)
        return _reply_provider_ok('reply_broad')

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)

    result = _run(server._handle_new_comment(
        _user_doc(), _comment_payload(comment_id='priority2', text='hello'), source='webhook'
    ))
    saved = db.comments.docs[0]

    assert result['rule_id'] == 'broad_any'
    assert saved['matched_rule_id'] == 'broad_any'
    assert saved['matched_rule_priority'] == 3
    assert saved['matched_rule_scope'] == 'broad'
    assert calls == ['Broad reply']


def test_queued_specific_rule_blocks_broad_rule(monkeypatch):
    db = _install_db(monkeypatch, [
        _broad_reply_automation('broad_queue'),
        _specific_reply_automation('specific_queue', 'media1'),
    ])

    result = _run(server._handle_new_comment(
        _user_doc(),
        {**_comment_payload(comment_id='priority3', text='hello'), 'force_queue': True},
        source='webhook',
    ))
    saved = db.comments.docs[0]

    assert result['queued'] is True
    assert result['rule_id'] == 'specific_queue'
    assert saved['matched_rule_id'] == 'specific_queue'
    assert saved['action_status'] == 'pending'
    assert saved['broad_rules_skipped_due_specific_match'] is True


def test_specific_partial_success_blocks_broad_rule(monkeypatch):
    db = _install_db(monkeypatch, [
        _broad_reply_automation('broad_partial'),
        _specific_reply_automation('specific_partial', 'media1', with_dm=True),
    ])
    calls = []

    async def reply_ok(_token, _comment_id, text):
        calls.append(('reply', text))
        return _reply_provider_ok('reply_partial_specific')

    async def dm_unavailable(*_args):
        calls.append(('dm', 'specific'))
        return {'ok': False, 'failure_reason': 'recipient_unavailable', 'retryable': False}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_unavailable)

    result = _run(server._handle_new_comment(
        _user_doc(), _comment_payload(comment_id='priority4', text='hello'), source='webhook'
    ))
    saved = db.comments.docs[0]

    assert result['rule_id'] == 'specific_partial'
    assert saved['action_status'] == 'partial_success'
    assert saved['matched_rule_id'] == 'specific_partial'
    assert calls == [('reply', 'Specific reply'), ('dm', 'specific')]


def test_specific_rule_top_level_reply_is_attempted_when_reply_node_missing(monkeypatch):
    db = _install_db(monkeypatch, [
        _broad_reply_automation('broad_missing_node'),
        _automation(
            [
                {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
                {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Specific DM'}},
            ],
            [{'source': 'n_trigger', 'target': 'n_dm'}],
            id='specific_missing_reply_node',
            trigger='comment:media1',
            post_scope='specific',
            media_id='media1',
            reply_under_post=True,
            comment_reply='Specific top-level reply',
        ),
    ])
    calls = []

    async def reply_ok(_token, _comment_id, text):
        calls.append(('reply', text))
        return _reply_provider_ok('reply_specific_top_level')

    async def dm_ok(*_args):
        calls.append(('dm', 'specific'))
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    result = _run(server._handle_new_comment(
        _user_doc(), _comment_payload(comment_id='specificMissingReplyNode', text='hello'), source='webhook'
    ))
    saved = db.comments.docs[0]

    assert result['rule_id'] == 'specific_missing_reply_node'
    assert saved['matched_rule_scope'] == 'specific_post_exact'
    assert saved['reply_status'] == 'success'
    assert saved['reply_provider_response_ok'] is True
    assert saved['dm_status'] == 'success'
    assert saved['action_status'] == 'success'
    assert calls == [('reply', 'Specific top-level reply'), ('dm', 'specific')]


def test_persistence_normalizes_graph_reply_node_to_top_level_fields():
    normalized = server._normalize_public_reply_for_persistence({
        'id': 'specific_graph',
        'trigger': 'comment:media1',
        'post_scope': 'specific',
        'media_id': 'media1',
        'reply_under_post': True,
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {
                'text': 'Graph reply',
                'replies': ['Graph reply', 'Graph reply 2'],
            }},
            {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Specific DM'}},
        ],
        'edges': [
            {'source': 'n_trigger', 'target': 'n_reply'},
            {'source': 'n_reply', 'target': 'n_dm'},
        ],
    })

    assert normalized['reply_under_post'] is True
    assert normalized['comment_reply'] == 'Graph reply'
    assert normalized['comment_reply_2'] == 'Graph reply 2'
    assert server._automation_public_reply_source(normalized) == 'graph_node'


def test_persistence_injects_reply_node_from_top_level_fields():
    normalized = server._normalize_public_reply_for_persistence({
        'id': 'specific_top',
        'trigger': 'comment:media1',
        'post_scope': 'specific',
        'media_id': 'media1',
        'reply_under_post': True,
        'comment_reply': 'Top level reply',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Specific DM'}},
        ],
        'edges': [{'source': 'n_trigger', 'target': 'n_dm'}],
    })

    assert normalized['reply_under_post'] is True
    assert normalized['comment_reply'] == 'Top level reply'
    assert any(node.get('type') == 'reply_comment' for node in normalized['nodes'])
    assert server._automation_public_reply_source(normalized) == 'graph_node'


def test_dm_edit_payload_preserves_existing_public_reply():
    existing = _automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Existing reply'}},
            {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Old DM'}},
        ],
        [
            {'source': 'n_trigger', 'target': 'n_reply'},
            {'source': 'n_reply', 'target': 'n_dm'},
        ],
        id='specific_edit',
        trigger='comment:media1',
        post_scope='specific',
        media_id='media1',
    )
    normalized = server._normalize_public_reply_for_persistence({
        'reply_under_post': False,
        'comment_reply': '',
        'comment_reply_2': '',
        'comment_reply_3': '',
        'dm_text': 'Edited DM',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Edited DM'}},
        ],
        'edges': [{'source': 'n_trigger', 'target': 'n_dm'}],
    }, existing)

    assert normalized['reply_under_post'] is True
    assert normalized['comment_reply'] == 'Existing reply'
    assert any(node.get('type') == 'reply_comment' for node in normalized['nodes'])


def test_specific_rule_top_level_reply_success_dm_permanent_failure_is_partial(monkeypatch):
    db = _install_db(monkeypatch, [
        _automation(
            [
                {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
                {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Specific DM'}},
            ],
            [{'source': 'n_trigger', 'target': 'n_dm'}],
            id='specific_top_reply_dm_fail',
            trigger='comment:media1',
            post_scope='specific',
            media_id='media1',
            reply_under_post=True,
            comment_reply='Specific public reply',
        ),
    ])
    calls = []

    async def reply_ok(_token, _comment_id, text):
        calls.append(('reply', text))
        return _reply_provider_ok('reply_specific_partial')

    async def dm_unavailable(*_args):
        calls.append(('dm', 'specific'))
        return {'ok': False, 'failure_reason': 'recipient_unavailable', 'retryable': False}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_unavailable)

    result = _run(server._handle_new_comment(
        _user_doc(), _comment_payload(comment_id='specificTopReplyDmFail', text='hello'), source='webhook'
    ))
    saved = db.comments.docs[0]

    assert result['action_status'] == 'partial_success'
    assert saved['reply_status'] == 'success'
    assert saved['reply_provider_response_ok'] is True
    assert saved['dm_status'] == 'failed'
    assert saved['dm_failure_reason'] == 'recipient_unavailable'
    assert calls == [('reply', 'Specific public reply'), ('dm', 'specific')]

    calls.clear()
    duplicate = _run(server._handle_new_comment(
        _user_doc(), _comment_payload(comment_id='specificTopReplyDmFail', text='hello'), source='polling'
    ))
    assert duplicate['reason'] == 'comment_already_partial_success'
    assert calls == []


def test_specific_rule_dm_success_reply_retryable_failure_retries_reply_only(monkeypatch):
    now = datetime.utcnow()
    db = _install_db(monkeypatch, [
        _automation(
            [
                {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
                {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Specific DM'}},
            ],
            [{'source': 'n_trigger', 'target': 'n_dm'}],
            id='specific_reply_retry_only',
            trigger='comment:media1',
            post_scope='specific',
            media_id='media1',
            reply_under_post=True,
            comment_reply='Specific retry reply',
        ),
    ])
    calls = []
    reply_results = [
        {'ok': False, 'status_code': 500, 'failure_reason': 'temporary_graph_error', 'retryable': True},
        _reply_provider_ok('reply_after_retry'),
    ]

    async def reply_flaky(_token, _comment_id, text):
        calls.append(('reply', text))
        return reply_results.pop(0)

    async def dm_ok(*_args):
        calls.append(('dm', 'specific'))
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_flaky)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    first = _run(server._handle_new_comment(
        _user_doc(), _comment_payload(comment_id='specificRetryReplyOnly', text='hello'), source='webhook'
    ))
    saved = db.comments.docs[0]

    assert first['action_status'] == 'partial_success'
    assert saved['dm_status'] == 'success'
    assert saved['reply_status'] == 'failed'
    assert saved['reply_failure_retryable'] is True
    assert saved['queued'] is True
    assert calls == [('reply', 'Specific retry reply'), ('dm', 'specific')]

    saved['next_retry_at'] = now - timedelta(seconds=1)
    summary = _run(server._automation_queue_tick())

    assert summary['success'] == 1
    assert calls == [
        ('reply', 'Specific retry reply'),
        ('dm', 'specific'),
        ('reply', 'Specific retry reply'),
    ]
    assert saved['reply_status'] == 'success'
    assert saved['dm_status'] == 'success'
    assert saved['action_status'] == 'success'


def test_specific_permanent_failure_does_not_fallback_to_broad_rule(monkeypatch):
    db = _install_db(monkeypatch, [
        _broad_reply_automation('broad_after_failure'),
        _specific_reply_automation('specific_failure', 'media1'),
    ])
    calls = []

    async def reply_failed(_token, _comment_id, text):
        calls.append(text)
        return {'ok': False, 'status_code': 400, 'failure_reason': 'permission_error', 'retryable': False}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_failed)

    result = _run(server._handle_new_comment(
        _user_doc(), _comment_payload(comment_id='priority5', text='hello'), source='webhook'
    ))
    saved = db.comments.docs[0]

    assert result['rule_id'] == 'specific_failure'
    assert saved['action_status'] == 'failed_permanent'
    assert saved['matched_rule_id'] == 'specific_failure'
    assert calls == ['Specific reply']


def test_queue_processes_original_specific_matched_rule(monkeypatch):
    now = datetime.utcnow()
    db = _install_db(monkeypatch, [
        _broad_reply_automation('broad_queue_tick'),
        _specific_reply_automation('specific_queue_tick', 'media1'),
    ], comments=[{
        'id': 'queuedPriority',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': 'queuedPriorityComment',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'text': 'hello',
        'matched': True,
        'rule_id': 'specific_queue_tick',
        'matched_rule_id': 'specific_queue_tick',
        'matched_rule_priority': 1,
        'matched_rule_scope': 'specific_post_exact',
        'reply_status': 'pending',
        'dm_status': 'disabled',
        'action_status': 'pending',
        'next_retry_at': now - timedelta(seconds=1),
        'attempts': 0,
    }])
    calls = []

    async def reply_ok(_token, _comment_id, text):
        calls.append(text)
        return _reply_provider_ok('reply_queue_priority')

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)

    summary = _run(server._automation_queue_tick())
    saved = db.comments.docs[0]

    assert summary['success'] == 1
    assert saved['rule_id'] == 'specific_queue_tick'
    assert saved['reply_status'] == 'success'
    assert calls == ['Specific reply']


def test_priority_diagnostics_show_specific_match(monkeypatch):
    db = _install_db(monkeypatch, [
        _broad_reply_automation('broad_diag'),
        _specific_reply_automation('specific_diag', 'media1'),
    ])

    async def reply_ok(*_args):
        return _reply_provider_ok('reply_diag_priority')

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)

    _run(server._handle_new_comment(
        _user_doc(), _comment_payload(comment_id='priorityDiag', text='hello'), source='webhook'
    ))
    result = _run(server.comment_why_not_replied('priorityDiag', user_id='u1'))

    assert result['matched_rule_id'] == 'specific_diag'
    assert result['matched_rule_priority'] == 1
    assert result['matched_rule_scope'] == 'specific_post_exact'
    assert result['broad_rules_skipped_due_specific_match'] is True


def test_pending_comment_is_processed_by_queue_tick(monkeypatch):
    now = datetime.utcnow()
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Thanks'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_reply'}],
    )], comments=[{
        'id': 'queued1',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': 'queuedComment',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'text': '🔥👏',
        'matched': True,
        'rule_id': 'auto1',
        'reply_status': 'pending',
        'dm_status': 'disabled',
        'action_status': 'pending',
        'next_retry_at': now - timedelta(seconds=1),
        'attempts': 0,
    }])

    async def reply_ok(*_args):
        return _reply_provider_ok()

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)

    summary = _run(server._automation_queue_tick())
    saved = db.comments.docs[0]

    assert summary['processed'] == 1
    assert saved['reply_status'] == 'success'
    assert saved['action_status'] == 'success'
    assert saved['attempts'] == 1
    assert saved['queue_lock_until'] is None


def test_retryable_queue_failure_is_rescheduled(monkeypatch):
    now = datetime.utcnow()
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_dm', 'type': 'message', 'data': {'text': 'Link'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_dm'}],
    )], comments=[{
        'id': 'queued2',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': 'queuedDm',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'text': 'PDF',
        'matched': True,
        'rule_id': 'auto1',
        'reply_status': 'disabled',
        'dm_status': 'pending',
        'action_status': 'pending',
        'next_retry_at': now - timedelta(seconds=1),
        'attempts': 0,
    }])

    async def dm_temp(*_args):
        return {'ok': False, 'failure_reason': 'temporary_graph_error', 'retryable': True}

    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_temp)

    summary = _run(server._automation_queue_tick())
    saved = db.comments.docs[0]

    assert summary['failed_retryable'] == 1
    assert saved['dm_status'] == 'failed'
    assert saved['action_status'] == 'failed_retryable'
    assert saved['next_retry_at'] is not None
    assert saved['attempts'] == 1


def test_queue_does_not_duplicate_successful_public_reply(monkeypatch):
    now = datetime.utcnow()
    db = _install_db(monkeypatch, [_reply_and_dm_automation()], comments=[{
        'id': 'queued3',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'igUserId': 'igA',
        'ig_comment_id': 'partialRetry',
        'media_id': 'media1',
        'commenter_id': 'contact1',
        'text': 'PDF',
        'matched': True,
        'rule_id': 'auto1',
        'replied': True,
        'reply_status': 'success',
        'reply_provider_response_ok': True,
        'reply_provider_comment_id': 'reply_partial',
        'reply_success_source': 'webhook',
        'replied_at': now - timedelta(seconds=10),
        'dm_status': 'failed',
        'dm_failure_retryable': True,
        'dm_failure_reason': 'temporary_graph_error',
        'action_status': 'partial_success',
        'next_retry_at': now - timedelta(seconds=1),
        'attempts': 0,
    }])
    calls = []

    async def reply_ok(*_args):
        calls.append('reply')
        return _reply_provider_ok()

    async def dm_ok(*_args):
        calls.append('dm')
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    summary = _run(server._automation_queue_tick())
    saved = db.comments.docs[0]

    assert summary['success'] == 1
    assert calls == ['dm']
    assert saved['reply_status'] == 'success'
    assert saved['dm_status'] == 'success'
    assert saved['action_status'] == 'success'


def test_why_not_replied_explains_queued_comment(monkeypatch):
    now = datetime.utcnow()
    _install_db(monkeypatch, [], comments=[{
        'id': 'queuedWhy',
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'ig_comment_id': 'whyComment',
        'media_id': 'media1',
        'text': 'private body',
        'matched': True,
        'rule_id': 'auto1',
        'reply_status': 'pending',
        'dm_status': 'disabled',
        'action_status': 'pending',
        'skip_reason': 'queued_rate_limit',
        'next_retry_at': now,
        'attempts': 2,
        'queued': True,
    }])

    result = _run(server.comment_why_not_replied('whyComment', user_id='u1'))

    assert result['eligible'] is True
    assert result['queued'] is True
    assert result['skip_reason'] == 'queued_rate_limit'
    assert result['attempts'] == 2
    assert 'private body' not in str(result)
