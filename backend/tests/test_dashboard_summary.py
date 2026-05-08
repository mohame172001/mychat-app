from datetime import datetime, timedelta
from pathlib import Path
import os
import sys

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
    assert result['conversionRate'] == 50.0
    assert len(result['weeklyPerformance']) == 7
    assert sum(day['messages'] for day in result['weeklyPerformance']) == 2
    assert sum(day['conversions'] for day in result['weeklyPerformance']) == 1


def test_dashboard_summary_includes_plan_counters_from_monthly_usage(monkeypatch):
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_summary(user_id='u1'))

    assert result['plan']['counters']['comments_processed'] == 9
    assert result['plan']['counters']['dms_sent'] == 5
    assert result['commentsProcessed'] == 9
    assert result['dmsSent'] == 5
    assert result['publicRepliesSent'] == 4


def test_dashboard_summary_does_not_expose_tokens(monkeypatch):
    fake_db = _summary_db()
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.dashboard_summary(user_id='u1'))

    payload = str(result)
    assert 'old-token' not in payload
    assert 'accessToken' not in payload
    assert 'meta_access_token' not in payload
