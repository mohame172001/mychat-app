from datetime import datetime, timedelta
from pathlib import Path
import os
import sys

from fastapi import BackgroundTasks, Response

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://example.com')
os.environ.setdefault('FRONTEND_URL', 'https://example.com')
os.environ.setdefault('IG_APP_ID', '123')
os.environ.setdefault('IG_APP_SECRET', 'secret')
os.environ.setdefault('CRON_SECRET', 'cron-secret')

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server
from test_instagram_token_refresh import FakeDB, _account, _run, _user


def _usage_event(event_type, account_id='igA', user_id='u1', days_ago=0, **extra):
    dt = datetime.utcnow() - timedelta(days=days_ago)
    doc = {
        'id': f'{event_type}-{account_id}-{days_ago}-{len(extra)}',
        'user_id': user_id,
        'instagram_account_id': account_id,
        'event_type': event_type,
        'event_month': dt.strftime('%Y-%m'),
        'event_date': dt,
        'created_at': dt,
    }
    doc.update(extra)
    return doc


def _click(account_id='igA', instagram_user_id='viewer1', user_id='u1', days_ago=0):
    dt = datetime.utcnow() - timedelta(days=days_ago)
    return {
        'id': f'click-{account_id}-{instagram_user_id}-{days_ago}',
        'userId': user_id,
        'instagramAccountId': account_id,
        'instagramUserId': instagram_user_id,
        'clickedAt': dt,
        'createdAt': dt,
    }


def _comment(account_id='igA', commenter='viewer1', user_id='u1', days_ago=0, **extra):
    dt = datetime.utcnow() - timedelta(days=days_ago)
    doc = {
        'id': f'comment-{account_id}-{commenter}-{days_ago}-{len(extra)}',
        'user_id': user_id,
        'instagramAccountId': account_id,
        'commenter_id': commenter,
        'created': dt,
        'updated': dt,
    }
    doc.update(extra)
    return doc


def _summary_db():
    month = datetime.utcnow().strftime('%Y-%m')
    account_a = _account(id='accA', userId='u1', instagramAccountId='igA', username='acct_a')
    account_b = _account(id='accB', userId='u1', instagramAccountId='igB', username='acct_b')
    user = _user(id='u1', active_instagram_account_id='accA', ig_user_id='igA')
    return FakeDB(
        account=[account_a, account_b],
        user=user,
        automations=[
            {'id': 'autoA1', 'user_id': 'u1', 'instagramAccountId': 'igA', 'status': 'active', 'name': 'A active'},
            {'id': 'autoA2', 'user_id': 'u1', 'instagramAccountId': 'igA', 'status': 'draft', 'name': 'A draft'},
            {'id': 'autoB1', 'user_id': 'u1', 'instagramAccountId': 'igB', 'status': 'active', 'name': 'B active'},
        ],
        contacts=[
            {'id': 'c1', 'user_id': 'u1', 'instagramAccountId': 'igA', 'instagramUserId': 'viewer1'},
            {'id': 'c2', 'user_id': 'u1', 'instagramAccountId': 'igA', 'instagramUserId': 'viewer2'},
            {'id': 'c3', 'user_id': 'u1', 'instagramAccountId': 'igB', 'instagramUserId': 'viewer3'},
        ],
        usage_events=[
            _usage_event('dm_sent', 'igA'),
            _usage_event('public_reply_sent', 'igA'),
            _usage_event('dm_sent', 'igB'),
            _usage_event('public_reply_sent', 'igA', user_id='other-user'),
            _usage_event('link_clicked', 'igA'),
        ],
        link_click_events=[
            _click('igA', 'viewer1'),
            _click('igA', 'viewer1'),
            _click('igB', 'viewer3'),
        ],
        monthly_usage=[{
            'id': 'usage1',
            'user_id': 'u1',
            'event_month': month,
            'comments_processed': 9,
            'public_replies_sent': 4,
            'dms_sent': 5,
            'links_clicked': 2,
            'queue_jobs_processed': 3,
            'retryable_failures': 1,
            'permanent_failures': 0,
        }],
    )


def test_dashboard_summary_is_scoped_to_active_instagram_account(monkeypatch):
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_summary(user_id='u1'))

    assert result['instagram']['activeAccountId'] == 'accA'
    assert result['instagram']['instagramAccountId'] == 'igA'
    assert result['activeAutomations'] == 1
    assert result['messagesSent'] == 2
    assert result['totalContacts'] == 2
    assert result['convertedContacts'] == 1
    # Conversion rate is current-month converters / current-month contacts.
    # The only contact observed engaging this month in this fixture is the
    # one with a fresh link-click event (viewer1), so 1 / 1 = 100%.
    # totalContacts (all-time, all engagement sources) stays at 2.
    assert result['conversionRate'] == 100.0
    assert len(result['weeklyPerformance']) == 7
    assert sum(day['messages'] for day in result['weeklyPerformance']) == 2
    assert sum(day['conversions'] for day in result['weeklyPerformance']) == 1


def test_dashboard_summary_includes_plan_counters_from_monthly_usage(monkeypatch):
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_summary(user_id='u1'))

    assert result['plan']['counters']['comments_processed'] == 9
    assert result['plan']['counters']['dms_sent'] == 5
    # Dashboard cards stay active-account scoped; plan counters remain the
    # user-month source of truth for billing/limit display.
    assert result['commentsProcessed'] == 0
    assert result['dmsSent'] == 1
    assert result['publicRepliesSent'] == 1


def test_dashboard_summary_does_not_expose_tokens(monkeypatch):
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_summary(user_id='u1'))

    payload = str(result)
    assert 'old-token' not in payload
    assert 'accessToken' not in payload
    assert 'meta_access_token' not in payload


def test_dashboard_summary_without_instagram_account_returns_empty_state(monkeypatch):
    fake_db = FakeDB(
        account=[],
        user=_user(
            id='u1',
            ig_user_id=None,
            meta_access_token=None,
            active_instagram_account_id=None,
            instagramConnected=False,
            instagram_connection_valid=False,
        ),
        automations=[],
        contacts=[],
        usage_events=[],
        link_click_events=[],
        comments=[],
        monthly_usage=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    response = Response()

    result = _run(server.dashboard_summary(user_id='u1', response=response))

    assert result['instagram']['connected'] is False
    assert result['instagram']['activeAccountId'] is None
    assert result['instagram']['instagramAccountId'] is None
    assert result['totalContacts'] == 0
    assert result['activeAutomations'] == 0
    assert result['messagesSent'] == 0
    assert len(result['weeklyPerformance']) == 7
    assert response.headers['X-Dashboard-Summary-Source'] in {'rebuilt', 'read_model'}


def test_dashboard_summary_sets_safe_timing_headers(monkeypatch):
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)
    response = Response()

    result = _run(server.dashboard_summary(user_id='u1', response=response))

    assert result['instagram']['instagramAccountId'] == 'igA'
    assert response.headers['X-Dashboard-Summary-Source'] in {'rebuilt', 'read_model', 'stale_read_model'}
    assert response.headers['X-Dashboard-Summary-Time'].isdigit()
    assert response.headers['X-Dashboard-Summary-Slowest'] in {
        'active_account',
        'meta',
        'usage_limits',
        'usage_events',
        'automations',
        'contacts',
        'link_clicks',
        'comments',
        'parallel_reads',
        'read_model',
    }
    header_payload = str(dict(response.headers))
    assert 'old-token' not in header_payload
    assert 'accessToken' not in header_payload
    assert 'meta_access_token' not in header_payload


def test_dashboard_summary_missing_snapshot_rebuilds_and_stores(monkeypatch):
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)
    response = Response()

    result = _run(server.dashboard_summary(user_id='u1', response=response))

    assert response.headers['X-Dashboard-Summary-Source'] == 'rebuilt'
    assert result['activeAutomations'] == 1
    assert len(fake_db.dashboard_summaries.docs) == 1
    snapshot = fake_db.dashboard_summaries.docs[0]
    assert snapshot['user_id'] == 'u1'
    assert snapshot['instagramAccountId'] == 'igA'
    assert snapshot['summary']['activeAutomations'] == 1
    assert snapshot['expires_at'] > datetime.utcnow()
    assert snapshot['max_stale_at'] > snapshot['expires_at']


def test_dashboard_summary_fresh_snapshot_returns_read_model(monkeypatch):
    fake_db = _summary_db()
    month = datetime.utcnow().strftime('%Y-%m')
    now = datetime.utcnow()
    fake_db.dashboard_summaries.docs.append({
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'month': month,
        'range': server.DASHBOARD_RANGE_DEFAULT_KEY,
        'summary': {'activeAutomations': 123, 'instagram': {'instagramAccountId': 'igA'}},
        'expires_at': now + timedelta(seconds=30),
        'max_stale_at': now + timedelta(minutes=5),
    })
    monkeypatch.setattr(server, 'db', fake_db)
    response = Response()

    result = _run(server.dashboard_summary(user_id='u1', response=response))

    assert result['activeAutomations'] == 123
    assert response.headers['X-Dashboard-Summary-Source'] == 'read_model'
    assert response.headers['X-Dashboard-Summary-Slowest'] == 'read_model'


def test_dashboard_summary_stale_snapshot_returns_immediately_and_schedules_refresh(monkeypatch):
    fake_db = _summary_db()
    month = datetime.utcnow().strftime('%Y-%m')
    now = datetime.utcnow()
    fake_db.dashboard_summaries.docs.append({
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'month': month,
        'range': server.DASHBOARD_RANGE_DEFAULT_KEY,
        'summary': {'activeAutomations': 55, 'instagram': {'instagramAccountId': 'igA'}},
        'expires_at': now - timedelta(seconds=1),
        'max_stale_at': now + timedelta(minutes=5),
    })
    monkeypatch.setattr(server, 'db', fake_db)
    response = Response()
    background_tasks = BackgroundTasks()

    result = _run(server.dashboard_summary(
        user_id='u1',
        response=response,
        background_tasks=background_tasks,
    ))

    assert result['activeAutomations'] == 55
    assert response.headers['X-Dashboard-Summary-Source'] == 'stale_read_model'
    assert len(background_tasks.tasks) == 1


def test_dashboard_summary_expired_snapshot_rebuilds(monkeypatch):
    fake_db = _summary_db()
    month = datetime.utcnow().strftime('%Y-%m')
    now = datetime.utcnow()
    fake_db.dashboard_summaries.docs.append({
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'month': month,
        'range': server.DASHBOARD_RANGE_DEFAULT_KEY,
        'summary': {'activeAutomations': 55, 'instagram': {'instagramAccountId': 'igA'}},
        'expires_at': now - timedelta(minutes=10),
        'max_stale_at': now - timedelta(minutes=5),
    })
    monkeypatch.setattr(server, 'db', fake_db)
    response = Response()

    result = _run(server.dashboard_summary(user_id='u1', response=response))

    assert result['activeAutomations'] == 1
    assert response.headers['X-Dashboard-Summary-Source'] == 'rebuilt'
    assert fake_db.dashboard_summaries.docs[0]['summary']['activeAutomations'] == 1


def test_dashboard_summary_stale_fallback_on_rebuild_failure(monkeypatch):
    fake_db = _summary_db()
    month = datetime.utcnow().strftime('%Y-%m')
    now = datetime.utcnow()
    fake_db.dashboard_summaries.docs.append({
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'month': month,
        'range': server.DASHBOARD_RANGE_DEFAULT_KEY,
        'summary': {'activeAutomations': 44, 'instagram': {'instagramAccountId': 'igA'}},
        'expires_at': now - timedelta(seconds=1),
        'max_stale_at': now + timedelta(minutes=5),
    })
    monkeypatch.setattr(server, 'db', fake_db)

    async def boom(*_args, **_kwargs):
        raise RuntimeError('boom')

    monkeypatch.setattr(server, '_calculate_dashboard_summary_live', boom)
    result, meta = _run(server._get_dashboard_summary_readthrough('u1', _account(
        id='accA', userId='u1', instagramAccountId='igA'
    )))

    assert result['activeAutomations'] == 44
    assert meta['source'] == 'stale_read_model'


def test_invalidate_dashboard_summary_deletes_only_matching_scope(monkeypatch):
    fake_db = _summary_db()
    fake_db.dashboard_summaries.docs.extend([
        {'user_id': 'u1', 'instagramAccountId': 'igA', 'month': '2026-05', 'summary': {}},
        {'user_id': 'u1', 'instagramAccountId': 'igB', 'month': '2026-05', 'summary': {}},
        {'user_id': 'u2', 'instagramAccountId': 'igA', 'month': '2026-05', 'summary': {}},
    ])
    monkeypatch.setattr(server, 'db', fake_db)

    _run(server.invalidate_dashboard_summary('u1', instagram_account_id='igA', month='2026-05'))

    assert fake_db.dashboard_summaries.docs == [
        {'user_id': 'u1', 'instagramAccountId': 'igB', 'month': '2026-05', 'summary': {}},
        {'user_id': 'u2', 'instagramAccountId': 'igA', 'month': '2026-05', 'summary': {}},
    ]


def test_dashboard_summary_prefers_provider_proof_for_active_account(monkeypatch):
    month = datetime.utcnow().strftime('%Y-%m')
    account_a = _account(id='accA', userId='u1', instagramAccountId='igA', username='acct_a')
    account_b = _account(id='accB', userId='u1', instagramAccountId='igB', username='acct_b')
    fake_db = FakeDB(
        account=[account_a, account_b],
        user=_user(id='u1', active_instagram_account_id='accA', ig_user_id='igA'),
        contacts=[],
        comments=[
            _comment(
                'igA', 'viewer1',
                action_status='success',
                reply_status='success',
                reply_provider_response_ok=True,
                replied_at=datetime.utcnow(),
                dm_status='success',
                dm_sent_at=datetime.utcnow(),
            ),
            _comment(
                'igA', 'viewer2',
                action_status='partial_success',
                reply_status='success',
                reply_provider_response_ok=False,
                replied_at=datetime.utcnow(),
                dm_status='failed',
            ),
            _comment(
                'igB', 'viewer3',
                action_status='success',
                reply_status='success',
                reply_provider_response_ok=True,
                replied_at=datetime.utcnow(),
                dm_status='success',
                dm_sent_at=datetime.utcnow(),
            ),
        ],
        monthly_usage=[{
            'id': 'usage1',
            'user_id': 'u1',
            'event_month': month,
            'comments_processed': 99,
            'public_replies_sent': 99,
            'dms_sent': 99,
            'links_clicked': 0,
        }],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_summary(user_id='u1'))

    assert result['commentsProcessed'] == 2
    assert result['publicRepliesSent'] == 1
    assert result['dmsSent'] == 1
    assert result['messagesSent'] == 2
    assert result['totalContacts'] == 2
    assert sum(day['messages'] for day in result['weeklyPerformance']) == 2


def test_conversion_rate_uses_current_month_contacts_as_denominator(monkeypatch):
    """Pin the dashboard conversion-rate semantic contract:
    numerator (converted) and denominator (contacts) both come from the
    SAME current-month window. Before this fix the denominator was
    all-time, so any workspace with old contact history saw the rate
    artificially collapse on a fresh month."""
    account_a = _account(id='accA', userId='u1', instagramAccountId='igA', username='acct_a')
    fake_db = FakeDB(
        account=[account_a],
        user=_user(id='u1', active_instagram_account_id='accA', ig_user_id='igA'),
        contacts=[
            # Three historical contacts — none engaged this month.
            {'id': 'cA', 'user_id': 'u1', 'instagramAccountId': 'igA', 'instagramUserId': 'historical-1'},
            {'id': 'cB', 'user_id': 'u1', 'instagramAccountId': 'igA', 'instagramUserId': 'historical-2'},
            {'id': 'cC', 'user_id': 'u1', 'instagramAccountId': 'igA', 'instagramUserId': 'historical-3'},
        ],
        comments=[
            # Two external commenters observed THIS month — these
            # populate the conversion denominator.
            _comment('igA', 'fresh-fan-1'),
            _comment('igA', 'fresh-fan-2'),
        ],
        link_click_events=[
            # One of them clicked the tracked link.
            _click('igA', 'fresh-fan-1'),
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_summary(user_id='u1'))

    # totalContacts stays all-time and union-based:
    # historical-1/2/3 + fresh-fan-1/2 = 5 (fresh-fan-1 dedupes across
    # comments + clicks, fresh-fan-2 only from comments).
    assert result['totalContacts'] == 5
    # current-month contacts = {fresh-fan-1, fresh-fan-2} ⇒ denominator 2.
    # converted = {fresh-fan-1} ⇒ 1 / 2 = 50.0%.
    assert result['conversionRate'] == 50.0
    assert result['convertedContacts'] == 1


def test_conversion_rate_zero_when_no_current_month_contacts(monkeypatch):
    """If no external contact engaged this month, conversion rate is
    0%, not NaN, not last-month carryover."""
    account_a = _account(id='accA', userId='u1', instagramAccountId='igA', username='acct_a')
    fake_db = FakeDB(
        account=[account_a],
        user=_user(id='u1', active_instagram_account_id='accA', ig_user_id='igA'),
        contacts=[
            # Old contacts only.
            {'id': 'old1', 'user_id': 'u1', 'instagramAccountId': 'igA', 'instagramUserId': 'old-1'},
            {'id': 'old2', 'user_id': 'u1', 'instagramAccountId': 'igA', 'instagramUserId': 'old-2'},
        ],
        comments=[],
        link_click_events=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_summary(user_id='u1'))
    assert result['conversionRate'] == 0
    assert result['convertedContacts'] == 0
    # totalContacts is still the all-time value.
    assert result['totalContacts'] == 2


def test_conversion_rate_excludes_bot_own_replies_from_denominator(monkeypatch):
    """An automation's own-account bot reply must not appear as a
    contact in either the headline or the conversion denominator."""
    account_a = _account(id='accA', userId='u1', instagramAccountId='igA', username='acct_a')
    fake_db = FakeDB(
        account=[account_a],
        user=_user(id='u1', active_instagram_account_id='accA', ig_user_id='igA'),
        contacts=[],
        comments=[
            _comment('igA', commenter='igA'),  # bot-own reply (commenter == own ig id)
            _comment('igA', commenter='external-1'),
        ],
        link_click_events=[
            _click('igA', 'external-1'),
        ],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_summary(user_id='u1'))
    # Only the real external commenter counts.
    assert result['totalContacts'] == 1
    assert result['convertedContacts'] == 1
    assert result['conversionRate'] == 100.0


def test_top_automations_orders_by_sent_then_active_then_created(monkeypatch):
    """Top Automations row must sort by `sent` desc, then active first
    on ties, then created desc. Currently the dashboard summary fetches
    in created-desc order; the sort key must re-rank for the card."""
    now = datetime.utcnow()
    account_a = _account(id='accA', userId='u1', instagramAccountId='igA', username='acct_a')
    fake_db = FakeDB(
        account=[account_a],
        user=_user(id='u1', active_instagram_account_id='accA', ig_user_id='igA'),
        automations=[
            # Newest, draft, zero sends — pre-fix would be the #1 row.
            {'id': 'new-draft', 'user_id': 'u1', 'instagramAccountId': 'igA',
             'status': 'draft', 'name': 'New draft', 'sent': 0,
             'created': now - timedelta(minutes=1)},
            # Older active, lots of sends — must end up first after fix.
            {'id': 'top-volume-active', 'user_id': 'u1', 'instagramAccountId': 'igA',
             'status': 'active', 'name': 'Top volume active', 'sent': 999,
             'created': now - timedelta(days=30)},
            # Tied sent count, one active one paused.
            {'id': 'tied-active', 'user_id': 'u1', 'instagramAccountId': 'igA',
             'status': 'active', 'name': 'Tied active', 'sent': 50,
             'created': now - timedelta(days=2)},
            {'id': 'tied-paused', 'user_id': 'u1', 'instagramAccountId': 'igA',
             'status': 'paused', 'name': 'Tied paused', 'sent': 50,
             'created': now - timedelta(days=1)},
            # Same sent + same status — newer wins.
            {'id': 'tied-older', 'user_id': 'u1', 'instagramAccountId': 'igA',
             'status': 'active', 'name': 'Tied older', 'sent': 10,
             'created': now - timedelta(days=10)},
            {'id': 'tied-newer', 'user_id': 'u1', 'instagramAccountId': 'igA',
             'status': 'active', 'name': 'Tied newer', 'sent': 10,
             'created': now - timedelta(days=1)},
        ],
        comments=[],
        link_click_events=[],
        contacts=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_summary(user_id='u1'))
    ids = [row['id'] for row in result['topAutomations']]
    assert ids[0] == 'top-volume-active'
    # Tied-50: active must come before paused.
    assert ids[1] == 'tied-active'
    assert ids[2] == 'tied-paused'
    # Tied-10: newer created first.
    assert ids[3] == 'tied-newer'
    assert ids[4] == 'tied-older'
    # The brand-new zero-sent draft falls to the bottom (or out of top 6).
    assert ids[5] == 'new-draft'


def test_top_automations_does_not_leak_other_accounts(monkeypatch):
    """Multi-account: Top Automations must show only the active
    account's automations even though the sort key is global."""
    account_a = _account(id='accA', userId='u1', instagramAccountId='igA', username='acct_a')
    account_b = _account(id='accB', userId='u1', instagramAccountId='igB', username='acct_b')
    fake_db = FakeDB(
        account=[account_a, account_b],
        user=_user(id='u1', active_instagram_account_id='accA', ig_user_id='igA'),
        automations=[
            {'id': 'on-active', 'user_id': 'u1', 'instagramAccountId': 'igA',
             'status': 'active', 'name': 'On active', 'sent': 5},
            {'id': 'on-other', 'user_id': 'u1', 'instagramAccountId': 'igB',
             'status': 'active', 'name': 'On other', 'sent': 9999},
        ],
        comments=[],
        link_click_events=[],
        contacts=[],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    result = _run(server.dashboard_summary(user_id='u1'))
    ids = {row['id'] for row in result['topAutomations']}
    assert 'on-active' in ids
    assert 'on-other' not in ids


def test_dashboard_summary_range_24h_uses_hourly_buckets(monkeypatch):
    """When the client passes `range=24h`, the chart returns 24
    hourly buckets, not 7 daily ones. Counters are scoped to the
    last 24 hours."""
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)
    result = _run(server.dashboard_summary(user_id='u1', range='24h'))
    assert len(result['weeklyPerformance']) == 24
    # Every bucket key looks like an ISO hour 'YYYY-MM-DDTHH'.
    for bucket in result['weeklyPerformance']:
        assert isinstance(bucket['date'], str)
        assert bucket['day'].endswith('h')
    assert result['range'] == '24h'


def test_dashboard_summary_range_7d_uses_seven_daily_buckets(monkeypatch):
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)
    result = _run(server.dashboard_summary(user_id='u1', range='7d'))
    assert len(result['weeklyPerformance']) == 7
    assert result['range'] == '7d'


def test_dashboard_summary_range_30d_uses_thirty_daily_buckets(monkeypatch):
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)
    result = _run(server.dashboard_summary(user_id='u1', range='30d'))
    assert len(result['weeklyPerformance']) == 30
    assert result['range'] == '30d'


def test_dashboard_summary_range_all_uses_twelve_monthly_buckets(monkeypatch):
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)
    result = _run(server.dashboard_summary(user_id='u1', range='all'))
    assert len(result['weeklyPerformance']) == 12
    # Monthly bucket date keys are 'YYYY-MM'.
    for bucket in result['weeklyPerformance']:
        assert isinstance(bucket['date'], str)
        assert len(bucket['date']) == 7
        assert bucket['date'][4] == '-'
    assert result['range'] == 'all'


def test_dashboard_summary_invalid_range_falls_back_to_legacy_default(monkeypatch):
    """Unknown range values must not 400 the dashboard. They quietly
    fall back to the legacy default (last 7 daily buckets, current
    calendar month counters) so an old/buggy client never blanks the
    page."""
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)
    result = _run(server.dashboard_summary(user_id='u1', range='lifetime'))
    assert len(result['weeklyPerformance']) == 7
    assert result['range'] is None  # legacy default


def test_dashboard_summary_cache_key_includes_range(monkeypatch):
    """Two different range values must NOT collide in the dashboard
    snapshot cache."""
    fake_db = _summary_db()
    now = datetime.utcnow()
    month = now.strftime('%Y-%m')
    # Pre-seed a fresh '7d'-range snapshot.
    fake_db.dashboard_summaries.docs.append({
        'user_id': 'u1',
        'instagramAccountId': 'igA',
        'month': month,
        'range': '7d',
        'summary': {'activeAutomations': 777, 'instagram': {'instagramAccountId': 'igA'}},
        'expires_at': now + timedelta(seconds=30),
        'max_stale_at': now + timedelta(minutes=5),
    })
    monkeypatch.setattr(server, 'db', fake_db)
    # '7d' hits the seeded snapshot.
    r_7d = _run(server.dashboard_summary(user_id='u1', range='7d'))
    assert r_7d['activeAutomations'] == 777
    # '30d' must NOT see the same snapshot — different cache key.
    r_30d = _run(server.dashboard_summary(user_id='u1', range='30d'))
    assert r_30d['activeAutomations'] != 777


def test_dashboard_scoped_docs_no_broad_fallback_when_multiaccount(monkeypatch):
    """When a workspace has 2+ active linked Instagram accounts and
    the dashboard caller has no active account context (account=None),
    the previous broad `{user_id: user_id}` fallback would silently
    union contacts/comments/clicks from EVERY linked account into the
    same response — exactly the Total Contacts cross-account leak.
    With 2+ accounts and no active context, scoped_docs must return
    an empty result instead."""
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)
    # _summary_db builds two accounts (igA, igB). include_unscoped
    # should be False. With account=None, the broad fallback used to
    # fire — now it does not.
    docs = _run(server._dashboard_scoped_docs(
        'contacts', 'u1', None, include_unscoped=False, limit=100,
    ))
    assert docs == []


def test_dashboard_metric_sources_are_metadata_only(monkeypatch):
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_metric_sources(user_id='u1'))

    assert 'user_dashboard' in result['metrics']
    assert result['metrics']['user_dashboard']['public_replies_sent']['source']
    payload = str(result)
    assert 'accessToken' not in payload
    assert 'meta_access_token' not in payload
