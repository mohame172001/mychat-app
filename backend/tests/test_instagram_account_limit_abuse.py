import os
import sys
from datetime import datetime
from pathlib import Path

import pytest
from fastapi import HTTPException
from pymongo.errors import DuplicateKeyError

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://example.com')
os.environ.setdefault('FRONTEND_URL', 'https://example.com')
os.environ.setdefault('IG_APP_ID', '123')
os.environ.setdefault('IG_APP_SECRET', 'secret')
os.environ.setdefault('CRON_SECRET', 'cron-secret')

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server  # noqa: E402
from test_instagram_token_refresh import FakeCollection, FakeDB, _account, _run, _user  # noqa: E402


def _install_db(monkeypatch, **collections):
    db = FakeDB(**collections)
    monkeypatch.setattr(server, 'db', db)
    return db


def test_duplicate_active_instagram_account_blocked_without_owner_leak(monkeypatch):
    _install_db(
        monkeypatch,
        account=[
            _account(id='accA', userId='uA', user_id='uA', instagramAccountId='igX',
                     connectionValid=True, isActive=True),
        ],
        user=[
            _user(id='uA', email='owner@example.com', status='active'),
            _user(id='uB', email='new@example.com', status='active'),
        ],
    )

    with pytest.raises(HTTPException) as exc:
        _run(server._ensure_instagram_account_connect_allowed('uB', 'igX'))

    assert exc.value.status_code == 409
    assert exc.value.detail['code'] == 'instagram_account_already_connected'
    assert 'owner@example.com' not in str(exc.value.detail)
    assert 'new@example.com' not in str(exc.value.detail)
    _run(server._ensure_instagram_account_connect_allowed('uA', 'igX'))


def test_soft_deleted_owner_does_not_permanently_lock_instagram_account(monkeypatch):
    _install_db(
        monkeypatch,
        account=[
            _account(id='accA', userId='uA', user_id='uA', instagramAccountId='igX',
                     connectionValid=True, isActive=True),
        ],
        user=[
            _user(id='uA', email='deleted@example.com', status='deleted'),
            _user(id='uB', email='new@example.com', status='active'),
        ],
    )

    _run(server._ensure_instagram_account_connect_allowed('uB', 'igX'))


class DuplicateOnAccountWriteCollection(FakeCollection):
    async def update_one(self, query, update, upsert=False):
        if upsert:
            raise DuplicateKeyError('duplicate active owner')
        return await super().update_one(query, update, upsert=upsert)


def test_duplicate_key_from_unique_index_translates_to_safe_409(monkeypatch):
    db = _install_db(
        monkeypatch,
        account=[],
        user=[_user(id='uB', ig_user_id='igX', meta_access_token='token-b')],
    )
    db.instagram_accounts = DuplicateOnAccountWriteCollection([])

    with pytest.raises(HTTPException) as exc:
        _run(server._sync_user_instagram_account_doc(
            {'id': 'uB', 'ig_user_id': 'igX', 'meta_access_token': 'token-b',
             'instagramConnected': True, 'instagram_connection_valid': True}
        ))

    assert exc.value.status_code == 409
    assert exc.value.detail['code'] == 'instagram_account_already_connected'


def test_account_scoped_usage_blocks_limit_bypass_for_new_user(monkeypatch):
    month = datetime.utcnow().strftime('%Y-%m')
    _install_db(
        monkeypatch,
        user=[
            _user(id='uA', email='a@example.com', status='active'),
            _user(id='uB', email='b@example.com', status='active'),
        ],
        monthly_usage=[
            {
                'id': 'mu_ig',
                'user_id': 'uA',
                'event_month': month,
                'limit_subject_type': 'instagram_account',
                'limit_subject_id': 'igX',
                'instagram_account_id': 'igX',
                'comments_processed': 250,
            },
        ],
    )

    result = _run(server.check_plan_limit(
        'uB',
        'monthly_comments_processed_limit',
        instagram_account_id='igX',
    ))

    assert result['exceeded'] is True
    assert result['limit_subject_type'] == 'instagram_account'
    assert result['limit_subject_id'] == 'igX'


def test_disconnect_reconnect_same_user_preserves_account_usage_subject(monkeypatch):
    month = datetime.utcnow().strftime('%Y-%m')
    _install_db(
        monkeypatch,
        account=[
            _account(id='accA', userId='uA', user_id='uA', instagramAccountId='igX',
                     connectionValid=False, isActive=False),
        ],
        user=[_user(id='uA', email='a@example.com', status='active')],
        monthly_usage=[
            {
                'id': 'mu_ig',
                'user_id': 'uA',
                'event_month': month,
                'limit_subject_type': 'instagram_account',
                'limit_subject_id': 'igX',
                'instagram_account_id': 'igX',
                'dms_sent': 25,
            },
        ],
    )

    counters = _run(server._instagram_monthly_counters('igX', month))

    assert counters['dms_sent'] == 25


def test_legacy_user_scoped_usage_backfill_maps_single_instagram_account(monkeypatch):
    month = datetime.utcnow().strftime('%Y-%m')
    db = _install_db(
        monkeypatch,
        account=[
            _account(id='accA', userId='uA', user_id='uA', instagramAccountId='igX',
                     connectionValid=True, isActive=True),
        ],
        user=[_user(id='uA')],
        monthly_usage=[
            {'id': 'legacy', 'user_id': 'uA', 'event_month': month,
             'comments_processed': 12, 'public_replies_sent': 8},
        ],
    )

    result = _run(server._backfill_instagram_account_usage_subjects(
        month=month,
        dry_run=False,
    ))

    assert result['mapped'] == 1
    account_rows = [
        row for row in db.monthly_usage.docs
        if row.get('limit_subject_type') == 'instagram_account'
    ]
    assert account_rows[0]['limit_subject_id'] == 'igX'
    assert account_rows[0]['comments_processed'] == 12
    legacy = next(row for row in db.monthly_usage.docs if row.get('id') == 'legacy')
    assert legacy['instagram_subject_mapping_status'] == 'mapped'


def test_legacy_user_scoped_usage_backfill_marks_ambiguous(monkeypatch):
    month = datetime.utcnow().strftime('%Y-%m')
    db = _install_db(
        monkeypatch,
        account=[
            _account(id='accA', userId='uA', user_id='uA', instagramAccountId='igX'),
            _account(id='accB', userId='uA', user_id='uA', instagramAccountId='igY'),
        ],
        user=[_user(id='uA')],
        monthly_usage=[
            {'id': 'legacy', 'user_id': 'uA', 'event_month': month, 'comments_processed': 12},
        ],
    )

    result = _run(server._backfill_instagram_account_usage_subjects(
        month=month,
        dry_run=False,
    ))

    assert result['ambiguous'] == 1
    legacy = next(row for row in db.monthly_usage.docs if row.get('id') == 'legacy')
    assert legacy['instagram_subject_mapping_status'] == 'ambiguous_multiple_instagram_accounts'


def test_trial_grant_cannot_be_claimed_twice_for_same_instagram_account(monkeypatch):
    _install_db(
        monkeypatch,
        account=[
            _account(id='accB', userId='uB', user_id='uB', instagramAccountId='igX',
                     connectionValid=True, isActive=True),
        ],
        user=[_user(id='uB', email='b@example.com')],
        instagram_account_trial_claims=[
            {
                'id': 'claim1',
                'instagram_account_id': 'igX',
                'plan_trial_identifier': 'trial_grant',
                'first_claimed_by_user_id': 'uA',
                'status': 'claimed',
            },
        ],
    )

    with pytest.raises(HTTPException) as exc:
        _run(server._ensure_instagram_trial_claim_available('uB', 'trial_grant'))

    assert exc.value.status_code == 409
    assert exc.value.detail['code'] == 'instagram_account_trial_already_claimed'


def test_account_usage_reconciliation_reports_subject_sources(monkeypatch):
    month = datetime.utcnow().strftime('%Y-%m')
    _install_db(
        monkeypatch,
        monthly_usage=[
            {'id': 'user_row', 'user_id': 'uA', 'event_month': month,
             'limit_subject_type': 'user', 'limit_subject_id': 'uA',
             'comments_processed': 4},
            {'id': 'ig_row', 'user_id': 'uA', 'event_month': month,
             'limit_subject_type': 'instagram_account', 'limit_subject_id': 'igX',
             'comments_processed': 4},
        ],
        usage_events=[
            {'id': 'ev1', 'event_month': month, 'event_type': 'comment_processed',
             'limit_subject_type': 'instagram_account', 'limit_subject_id': 'igX'},
        ],
    )

    result = _run(server._usage_subject_reconciliation(month))

    assert result['monthly_usage_rows']['user_scoped'] == 1
    assert result['monthly_usage_rows']['instagram_account_scoped'] == 1
    assert result['monthly_usage_instagram_account_scoped_counters']['comments_processed'] == 4
    assert result['usage_events_instagram_account_scoped_counters']['comments_processed'] == 1
    assert 'repair_applied' in result and result['repair_applied'] is False
