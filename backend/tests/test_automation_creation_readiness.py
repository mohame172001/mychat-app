"""Regression for a verified subscription rejected after a background heal."""
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from test_phase2m_certification_gate import server, _DB, _SubStub, _run


def _account():
    return {
        'id': 'account-1', 'userId': 'owner-1', 'username': 'test_creator',
        'instagramAccountId': 'ig-1', 'igUserId': 'ig-1',
        'accessToken': 'test-token', 'connectionValid': True,
        'grantedScopes': [], 'grantedScopesDebugTokenWorks': False,
        'grantedScopesMatchesIgAppId': False,
        'commentWebhookReady': False,
        'commentWebhookStatus': 'certification_scope_check_inconclusive',
    }


def _setup(monkeypatch):
    account = _account()
    db = _DB(accounts=[deepcopy(account)])
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, '_IG_REQUIRE_COMMENT_WEBHOOK_CERT', True)
    monkeypatch.setattr(server, '_subscribe_instagram_account_to_webhooks', _SubStub())
    return account, db


@pytest.mark.parametrize('reason', ['heal', 'sync', 'activate', 'admin_repair'])
def test_fresh_subscription_with_unproven_empty_scopes_does_not_flip_to_blocked(monkeypatch, reason):
    account, db = _setup(monkeypatch)
    result = _run(server.certify_instagram_account_for_comment_webhooks(account, reason=reason))
    assert result['comment_webhook_ready'] is True
    assert result['comment_webhook_status'] == 'subscription_verified_scope_proof_inconclusive'
    assert result['comment_permission_scope_check_proven'] is False
    assert result['comment_webhook_scope_warning']
    assert db.instagram_accounts.items[0]['commentWebhookReady'] is True


def test_create_quick_rule_after_heal_persists_on_the_same_account(monkeypatch):
    account, db = _setup(monkeypatch)
    monkeypatch.setattr(server, 'getActiveInstagramAccount', AsyncMock(side_effect=lambda _uid: db.instagram_accounts.items[0]))
    ownership_check = AsyncMock()
    monkeypatch.setattr(server, '_validate_selected_media_owned_by_account', ownership_check)
    monkeypatch.setattr(server, 'get_user_plan', AsyncMock(return_value={'plan_key': 'test'}))
    monkeypatch.setattr(server, 'compute_effective_limits', AsyncMock(return_value={'max_active_automations': None}))
    monkeypatch.setattr(server, '_safe_record_usage_event', AsyncMock())
    insert = AsyncMock()
    monkeypatch.setattr(db.automations, 'insert_one', insert, raising=False)
    payload = {'post_scope': 'specific', 'media_id': 'media-1', 'mode': 'reply_only',
               'match': 'any', 'comment_reply': 'Test public reply'}

    with pytest.raises(HTTPException) as rejected:
        _run(server.create_quick_comment_rule(payload, user_id='owner-1'))
    assert rejected.value.detail['code'] == 'comment_webhook_not_ready'
    insert.assert_not_awaited()

    _run(server.certify_instagram_account_for_comment_webhooks(account, reason='heal'))
    created = _run(server.create_quick_comment_rule(payload, user_id='owner-1'))
    assert created['user_id'] == 'owner-1'
    assert created['instagramAccountId'] == 'ig-1'
    assert created['media_id'] == 'media-1'
    assert created['status'] == 'active'
    insert.assert_awaited_once()
    ownership_check.assert_awaited_once()


def test_proven_missing_permission_is_still_blocked(monkeypatch):
    account, _db = _setup(monkeypatch)
    account.update(grantedScopesTokenPrefix=server._token_prefix(account['accessToken']),
                   grantedScopesDebugTokenWorks=True, grantedScopesMatchesIgAppId=True)
    result = _run(server.certify_instagram_account_for_comment_webhooks(account, reason='heal'))
    assert result['comment_webhook_ready'] is False
    assert result['comment_webhook_blocker'] == 'comment_permission_not_granted'


@pytest.mark.parametrize('status,fields', [(400, []), (200, ['messages'])])
def test_failed_or_incomplete_subscription_is_still_blocked(monkeypatch, status, fields):
    account, _db = _setup(monkeypatch)
    monkeypatch.setattr(server, '_subscribe_instagram_account_to_webhooks', _SubStub(fields, verify_status=status))
    result = _run(server.certify_instagram_account_for_comment_webhooks(account, reason='heal'))
    assert result['comment_webhook_ready'] is False


def test_non_comment_only_delivery_is_not_hidden_by_scope_warning(monkeypatch):
    account, _db = _setup(monkeypatch)
    monkeypatch.setattr(server, '_measure_account_webhook_delivery_signal', AsyncMock(return_value={
        'webhook_event_count': 3, 'comment_payload_seen': False,
    }))
    result = _run(server.certify_instagram_account_for_comment_webhooks(account, reason='heal'))
    assert result['comment_webhook_ready'] is False
    assert result['comment_webhook_status'] == 'meta_delivery_blocked'
