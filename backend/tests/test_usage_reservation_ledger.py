import os
import sys
from datetime import datetime, timedelta
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
import plans as _plans  # noqa: E402
from test_instagram_token_refresh import FakeDB, _account, _run, _user  # noqa: E402


def _month():
    return datetime.utcnow().strftime('%Y-%m')


def _account_usage_row(user_id='u1', ig_id='igA', **counters):
    row = {
        'id': f'mu-account-{ig_id}',
        'user_id': user_id,
        'event_month': _month(),
        'limit_subject_type': 'instagram_account',
        'limit_subject_id': ig_id,
        'instagram_account_id': ig_id,
    }
    row.update(counters)
    return row


def _install(monkeypatch, *, usage=None, overrides=None, user=None, account=None, admin_members=None):
    db = FakeDB(
        account or _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        user or _user(id='u1', email='u1@example.com'),
        monthly_usage=usage or [],
        user_limit_overrides=overrides or [],
        admin_members=admin_members or [],
    )
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())
    return db


def _reserve_comment(user_id='u1', ig_id='igA', action_id='comment-action', source='webhook'):
    return server.reserve_usage_limit(
        user_id,
        'monthly_comments_processed_limit',
        instagram_account_id=ig_id,
        source=source,
        automation_id='auto1',
        ig_comment_id='comment1',
        action_id=action_id,
    )


def test_remaining_one_allows_one_active_reservation_and_denies_second(monkeypatch):
    db = _install(
        monkeypatch,
        usage=[_account_usage_row(comments_processed=249)],
    )

    first = _run(_reserve_comment(action_id='first-comment'))
    second = _run(_reserve_comment(action_id='second-comment'))

    assert first['allowed'] is True
    assert first['reserved'] is True
    assert second['allowed'] is False
    assert second['metric'] == 'comments_processed'

    confirmed = _run(server.confirm_usage_reservation(
        first,
        user_id='u1',
        event_type='comment_processed',
        instagram_account_id='igA',
        automation_id='auto1',
        comment_id='comment1',
        metadata={'raw_comment_text': 'must not persist', 'access_token': 'must not persist'},
    ))
    assert confirmed is True

    account_rows = [
        row for row in db.monthly_usage.docs
        if row.get('limit_subject_type') == 'instagram_account'
        and row.get('limit_subject_id') == 'igA'
    ]
    assert account_rows[0]['comments_processed'] == 250
    assert sum(1 for row in db.usage_reservations.docs if row['status'] == 'confirmed') == 1
    assert sum(1 for row in db.usage_reservations.docs if row['status'] == 'failed') == 1
    persisted = repr(db.usage_reservations.docs) + repr(db.usage_events.docs)
    assert 'must not persist' not in persisted
    assert 'access_token' not in persisted


def test_duplicate_idempotency_key_reuses_reservation_and_confirms_once(monkeypatch):
    db = _install(monkeypatch)

    first = _run(_reserve_comment(action_id='same-action'))
    duplicate = _run(_reserve_comment(action_id='same-action'))

    assert first['allowed'] is True
    assert duplicate['duplicate'] is True
    assert duplicate['reservation_id'] == first['reservation_id']
    assert len(db.usage_reservations.docs) == 1

    assert _run(server.confirm_usage_reservation(
        first,
        user_id='u1',
        event_type='comment_processed',
        instagram_account_id='igA',
        automation_id='auto1',
        comment_id='comment1',
    )) is True
    assert _run(server.confirm_usage_reservation(
        duplicate,
        user_id='u1',
        event_type='comment_processed',
        instagram_account_id='igA',
        automation_id='auto1',
        comment_id='comment1',
    )) is False

    account_rows = [
        row for row in db.monthly_usage.docs
        if row.get('limit_subject_type') == 'instagram_account'
        and row.get('limit_subject_id') == 'igA'
    ]
    assert account_rows[0]['comments_processed'] == 1
    assert sum(1 for event in db.usage_events.docs if event['event_type'] == 'comment_processed') == 1


def test_provider_failure_before_send_releases_reservation_without_usage(monkeypatch):
    db = _install(monkeypatch)

    reservation = _run(_reserve_comment(action_id='pre-send-failure'))
    assert reservation['reserved'] is True

    assert _run(server.release_usage_reservation(
        reservation,
        reason='provider_not_called',
    )) is True

    assert db.usage_reservations.docs[0]['status'] == 'released'
    assert db.usage_reservation_buckets.docs[0]['reserved_amount'] == 0
    assert db.usage_events.docs == []
    assert not any(row.get('comments_processed') for row in db.monthly_usage.docs)


def test_plan_limited_does_not_confirm_usage(monkeypatch):
    db = _install(
        monkeypatch,
        usage=[_account_usage_row(comments_processed=250)],
    )

    denied = _run(_reserve_comment(action_id='over-limit'))

    assert denied['allowed'] is False
    assert denied['reservation_required'] is True
    assert db.usage_reservations.docs[0]['status'] == 'failed'
    assert db.usage_events.docs == []
    assert db.monthly_usage.docs[0]['comments_processed'] == 250


def test_unlimited_plan_bypasses_blocking_but_still_records_usage(monkeypatch):
    db = _install(monkeypatch)

    async def unlimited_user_plan(_user_id):
        plan = dict(_plans.get_plan_limits('free'))
        plan['monthly_dms_sent_limit'] = None
        plan['_assignment'] = {'billing_enabled': False}
        return plan

    monkeypatch.setattr(server, 'get_user_plan', unlimited_user_plan)
    reservation = _run(server.reserve_usage_limit(
        'u1',
        'monthly_dms_sent_limit',
        instagram_account_id='igA',
        source='queue',
        automation_id='auto1',
        ig_comment_id='comment1',
        action_id='dm-action',
    ))

    assert reservation['allowed'] is True
    assert reservation['reservation_required'] is False
    assert reservation['unlimited'] is True
    assert _run(server.confirm_usage_reservation(
        reservation,
        user_id='u1',
        event_type='dm_sent',
        instagram_account_id='igA',
        automation_id='auto1',
        comment_id='comment1',
    )) is True
    account_rows = [
        row for row in db.monthly_usage.docs
        if row.get('limit_subject_type') == 'instagram_account'
        and row.get('limit_subject_id') == 'igA'
    ]
    assert account_rows[0]['dms_sent'] == 1


def test_custom_allowance_is_used_by_reservation_limit(monkeypatch):
    now = datetime.utcnow()
    db = _install(
        monkeypatch,
        usage=[_account_usage_row(comments_processed=250)],
        overrides=[{
            'id': 'ov1',
            'user_id': 'u1',
            'type': 'additive_allowance',
            'status': 'active',
            'metrics': {'comments_processed_extra': 1},
            'starts_at': now - timedelta(days=1),
            'ends_at': now + timedelta(days=1),
            'created_at': now,
        }],
    )

    reservation = _run(_reserve_comment(action_id='allowance-comment'))

    assert reservation['allowed'] is True
    assert reservation['reserved'] is True
    assert db.usage_reservation_buckets.docs[0]['reserved_amount'] == 1


def test_reservation_diagnostics_counts_stale_and_legacy_without_leaking(monkeypatch):
    past = datetime.utcnow() - timedelta(hours=1)
    month = _month()
    db = _install(
        monkeypatch,
        usage=[
            _account_usage_row(comments_processed=1),
            {
                'id': 'legacy-user-row',
                'user_id': 'u1',
                'event_month': month,
                'comments_processed': 1,
            },
        ],
        admin_members=[{
            'id': 'admin1',
            'user_id': 'admin_u',
            'email': 'owner@example.com',
            'normalized_email': 'owner@example.com',
            'role': 'owner',
            'status': 'active',
            'permissions': ['audit.view'],
        }],
        user=[
            _user(id='u1', email='u1@example.com'),
            _user(id='admin_u', email='owner@example.com'),
        ],
        account=[
            _account(id='accA', userId='u1', instagramAccountId='igA', connectionValid=True),
        ],
    )
    db.usage_reservations.docs.append({
        'reservation_id': 'r1',
        'idempotency_key': 'k1',
        'user_id': 'u1',
        'instagram_account_id': 'igA',
        'limit_subject_type': 'instagram_account',
        'limit_subject_id': 'igA',
        'metric': 'comments_processed',
        'event_type': 'comment_processed',
        'amount': 1,
        'month': month,
        'status': 'confirmed',
        'access_token': 'must-not-leak',
        'raw_comment_text': 'must-not-leak',
    })
    db.usage_reservations.docs.append({
        'reservation_id': 'r2',
        'idempotency_key': 'k2',
        'user_id': 'u1',
        'instagram_account_id': 'igA',
        'limit_subject_type': 'instagram_account',
        'limit_subject_id': 'igA',
        'metric': 'dms_sent',
        'event_type': 'dm_sent',
        'amount': 1,
        'month': month,
        'status': 'reserved',
        'expires_at': past,
    })

    result = _run(server.admin_usage_reservation_diagnostics(
        month=month,
        user_id='admin_u',
    ))

    assert result['statuses']['confirmed'] == 1
    assert result['statuses']['reserved'] == 1
    assert result['stale_reserved_count'] == 1
    assert result['legacy_user_scoped_monthly_usage_rows'] == 1
    assert result['privacy'] == {'raw_text_returned': False, 'tokens_returned': False}
    serialized = repr(result)
    assert 'must-not-leak' not in serialized
    assert 'raw_comment_text' not in serialized
    assert 'access_token' not in serialized


def test_reconnect_same_instagram_account_keeps_same_usage_subject(monkeypatch):
    db = _install(monkeypatch)

    first = _run(_reserve_comment(action_id='before-reconnect'))
    _run(server.release_usage_reservation(first, reason='test_release'))

    db.instagram_accounts.docs[0]['connectionValid'] = False
    db.instagram_accounts.docs[0]['connectionValid'] = True
    second = _run(_reserve_comment(action_id='after-reconnect'))

    assert second['allowed'] is True
    assert second['reservation']['limit_subject_type'] == 'instagram_account'
    assert second['reservation']['limit_subject_id'] == 'igA'
