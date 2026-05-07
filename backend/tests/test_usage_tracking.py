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

from test_comment_dm_failure_status import (  # noqa: E402
    _automation,
    _comment_payload,
    _install_db,
    _reply_and_dm_automation,
    _reply_provider_ok,
    _user_doc,
)
from test_instagram_token_refresh import FakeDB, _account, _run, _user  # noqa: E402


def _event_types(db):
    return [event['event_type'] for event in db.usage_events.docs]


def _monthly(db):
    assert db.monthly_usage.docs
    return db.monthly_usage.docs[0]


def test_record_usage_event_writes_event_and_increments_monthly_counter(monkeypatch):
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        _user(id='u1'),
        automations=[{'id': 'auto1', 'user_id': 'u1', 'status': 'active'}],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    _run(server.record_usage_event(
        user_id='u1',
        event_type='public_reply_sent',
        instagram_account_id='igA',
        automation_id='auto1',
        comment_id='comment1',
        metadata={'source': 'test', 'access_token': 'secret-token', 'text': 'private text'},
        event_date=datetime(2026, 5, 5),
    ))

    assert fake_db.usage_events.docs[0]['event_type'] == 'public_reply_sent'
    assert fake_db.usage_events.docs[0]['event_month'] == '2026-05'
    assert fake_db.usage_events.docs[0]['metadata'] == {'source': 'test'}
    monthly = _monthly(fake_db)
    assert monthly['public_replies_sent'] == 1
    assert monthly['instagram_accounts_connected_snapshot'] == 1
    assert monthly['active_automations_snapshot'] == 1


def test_record_usage_event_rejects_invalid_event_type(monkeypatch):
    monkeypatch.setattr(server, 'db', FakeDB())

    with pytest.raises(ValueError):
        _run(server.record_usage_event(user_id='u1', event_type='raw_token_dump'))


def test_public_reply_and_dm_success_increment_usage_once(monkeypatch):
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])

    async def reply_ok(*_args):
        return _reply_provider_ok()

    async def dm_ok(*_args):
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_ok)

    _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))
    _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='polling'))

    assert _event_types(db).count('comment_processed') == 1
    assert _event_types(db).count('public_reply_sent') == 1
    assert _event_types(db).count('dm_sent') == 1
    monthly = _monthly(db)
    assert monthly['comments_processed'] == 1
    assert monthly['public_replies_sent'] == 1
    assert monthly['dms_sent'] == 1


def test_dm_failure_does_not_increment_dms_sent_and_partial_counts_reply(monkeypatch):
    db = _install_db(monkeypatch, [_reply_and_dm_automation()])

    async def reply_ok(*_args):
        return _reply_provider_ok()

    async def dm_failed(*_args):
        return {'ok': False, 'failure_reason': 'recipient_unavailable', 'retryable': False}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_ok)
    monkeypatch.setattr(server, 'send_ig_dm_detailed', dm_failed)

    result = _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))

    assert result['action_status'] == 'partial_success'
    assert 'dm_sent' not in _event_types(db)
    assert _event_types(db).count('public_reply_sent') == 1
    monthly = _monthly(db)
    assert monthly['public_replies_sent'] == 1
    assert monthly['dms_sent'] == 0


def test_public_reply_not_counted_without_provider_proof(monkeypatch):
    db = _install_db(monkeypatch, [_automation(
        [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment', 'data': {'text': 'Thanks'}},
        ],
        [{'source': 'n_trigger', 'target': 'n_reply'}],
    )])

    async def reply_without_provider_proof(*_args):
        return {'ok': True}

    monkeypatch.setattr(server, 'reply_to_ig_comment_detailed', reply_without_provider_proof)

    _run(server._handle_new_comment(_user_doc(), _comment_payload(), source='webhook'))

    assert 'public_reply_sent' not in _event_types(db)
    assert _monthly(db)['public_replies_sent'] == 0


def test_link_click_records_usage_event(monkeypatch):
    now = datetime.utcnow()
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        _user(id='u1'),
        tracked_links=[{
            'id': 'link1',
            'shortCode': 'abc123',
            'user_id': 'u1',
            'instagramAccountId': 'igA',
            'ruleId': 'auto1',
            'instagramUserId': 'contact1',
            'originalUrl': 'https://example.com/product',
            'isActive': True,
            'expiresAt': now + timedelta(days=1),
            'created': now,
        }],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    request = SimpleNamespace(
        client=SimpleNamespace(host='127.0.0.1'),
        headers={'user-agent': 'pytest', 'referer': 'https://instagram.com'},
    )

    response = _run(server.tracked_link_redirect('abc123', request))

    assert response.status_code == 302
    assert fake_db.link_click_events.docs
    assert _event_types(fake_db) == ['link_clicked']
    assert _monthly(fake_db)['links_clicked'] == 1


def test_current_usage_endpoint_returns_expected_structure(monkeypatch):
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        _user(id='u1'),
        automations=[{'id': 'auto1', 'user_id': 'u1', 'status': 'active'}],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    _run(server.record_usage_event(user_id='u1', event_type='dm_sent'))

    result = _run(server.current_usage(user_id='u1'))

    assert result['event_month'] == datetime.utcnow().strftime('%Y-%m')
    assert result['counters']['dms_sent'] == 1
    assert result['connectedInstagramAccountsCount'] == 1
    assert result['activeAutomationsCount'] == 1
    assert result['plan'] == 'free'
    assert result['billing_enabled'] is False
    assert 'accessToken' not in str(result)


def test_unsafe_metadata_keys_are_stripped(monkeypatch):
    """Tokens, raw text, headers, and Graph error bodies must not be
    persisted in usage_events.metadata, even if a caller passes them by
    accident."""
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        _user(id='u1'),
    )
    monkeypatch.setattr(server, 'db', fake_db)

    _run(server.record_usage_event(
        user_id='u1',
        event_type='dm_sent',
        metadata={
            'source': 'webhook',                       # safe
            'access_token': 'EAAB-secret',             # forbidden
            'meta_access_token': 'EAAB-secret',        # forbidden
            'authorization': 'Bearer xyz',             # forbidden
            'raw': '{"error": {"message": "..."}}',    # forbidden (raw Graph)
            'graph_error': 'something private',        # forbidden
            'comment_text': 'private text 12345',      # forbidden
            'dm_text': 'private DM text',              # forbidden
            'message_text': 'private message',         # forbidden
            'nested': {                                # nested forbidden keys
                'source': 'ok',
                'token': 'EAAB-secret',
                'access_token': 'EAAB-secret',
            },
        },
    ))

    saved = fake_db.usage_events.docs[0]['metadata']
    assert saved.get('source') == 'webhook'
    for forbidden in (
        'access_token', 'meta_access_token', 'authorization', 'raw',
        'graph_error', 'comment_text', 'dm_text', 'message_text',
    ):
        assert forbidden not in saved
    # Nested object: forbidden keys also stripped one level deep.
    assert 'token' not in saved.get('nested', {})
    assert 'access_token' not in saved.get('nested', {})
    assert saved['nested'].get('source') == 'ok'

    # Safety net: serialized event must not contain any of the forbidden values.
    serialized = repr(saved)
    assert 'EAAB-secret' not in serialized
    assert 'private text 12345' not in serialized
    assert 'private DM text' not in serialized


def test_usage_recording_failure_does_not_break_automation_flow(monkeypatch):
    """If usage_events.insert_one blows up, _safe_record_usage_event
    swallows the error and returns False so the calling automation flow
    still completes."""
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        _user(id='u1'),
    )
    monkeypatch.setattr(server, 'db', fake_db)

    async def insert_blows_up(*_args, **_kwargs):
        raise RuntimeError('simulated mongo failure')

    fake_db.usage_events.insert_one = insert_blows_up

    ok = _run(server._safe_record_usage_event(user_id='u1', event_type='dm_sent'))
    assert ok is False
    # Must not raise. Must not write a partial row. Caller flows continue.
    assert fake_db.usage_events.docs == []


def test_duplicate_retry_does_not_double_count_public_reply(monkeypatch):
    """A second call to _record_comment_usage_once with the same marker
    must not re-record the event."""
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        _user(id='u1'),
        comments=[{'id': 'doc_X', 'user_id': 'u1', 'ig_comment_id': 'c_X'}],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    for _ in range(3):
        _run(server._record_comment_usage_once(
            'doc_X',
            'usage_public_reply_recorded',
            user_id='u1',
            event_type='public_reply_sent',
            instagram_account_id='igA',
            comment_id='doc_X',
        ))

    types = _event_types(fake_db)
    assert types.count('public_reply_sent') == 1
    assert _monthly(fake_db)['public_replies_sent'] == 1


def test_admin_usage_endpoint_requires_admin(monkeypatch):
    """Non-admin caller gets 403 from /api/admin/usage/{user_id}."""
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        _user(id='u1', email='u1@example.com'),
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_user_usage('u1', month=None, user_id='u1'))
    assert exc.value.status_code == 403


def test_admin_usage_endpoint_returns_counters_for_target_user(monkeypatch):
    """Admin caller can read another user's monthly usage."""
    fake_db = FakeDB(
        _account(id='accA', userId='u_target', instagramAccountId='igT', connectionValid=True),
        _user(id='admin_u', email='admin@mychat.app'),
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', {'admin@mychat.app'})
    _run(server.record_usage_event(user_id='u_target', event_type='dm_sent'))

    result = _run(server.admin_user_usage('u_target', month=None, user_id='admin_u'))
    assert result['user_id'] == 'u_target'
    assert result['counters']['dms_sent'] == 1
    assert result['plan'] == 'free'
    assert result['billing_enabled'] is False


def test_admin_usage_endpoint_rejects_bad_month(monkeypatch):
    fake_db = FakeDB(
        _account(id='accA', userId='u1', instagramAccountId='igA'),
        _user(id='admin_u', email='admin@mychat.app'),
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', {'admin@mychat.app'})
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_user_usage('u1', month='2026/05', user_id='admin_u'))
    assert exc.value.status_code == 400
