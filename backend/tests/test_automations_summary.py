from datetime import datetime
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


def _db():
    account_a = _account(id='accA', userId='u1', instagramAccountId='igA', username='acct_a')
    account_b = _account(id='accB', userId='u1', instagramAccountId='igB', username='acct_b')
    user = _user(id='u1', active_instagram_account_id='accA', ig_user_id='igA')
    return FakeDB(
        account=[account_a, account_b],
        user=user,
        automations=[
            {
                'id': 'autoA',
                'user_id': 'u1',
                'instagramAccountId': 'igA',
                'status': 'active',
                'name': 'Specific A',
                'trigger': 'comment:mediaA',
                'post_scope': 'specific',
                'media_id': 'mediaA',
                'media_preview': {
                    'caption': 'Selected media caption',
                    'thumbnail_url': 'https://example.com/t.jpg',
                },
                'match': 'keyword',
                'keyword': 'price',
                'comment_reply': 'secret public reply text',
                'dm_text': 'secret dm text',
                'follow_request_enabled': True,
                'sent': 7,
                'createdAt': datetime.utcnow(),
                'updatedAt': datetime.utcnow(),
            },
            {
                'id': 'autoB',
                'user_id': 'u1',
                'instagramAccountId': 'igB',
                'status': 'active',
                'name': 'Other account',
                'comment_reply': 'other reply',
                'dm_text': 'other dm',
            },
            {
                'id': 'autoOtherUser',
                'user_id': 'other',
                'instagramAccountId': 'igA',
                'status': 'active',
                'name': 'Other user',
            },
        ],
    )


def test_automations_summary_is_compact_and_scoped(monkeypatch):
    fake_db = _db()
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.list_automations_summary(user_id='u1'))

    assert result['count'] == 1
    row = result['items'][0]
    assert row['id'] == 'autoA'
    assert row['status'] == 'active'
    assert row['post_scope'] == 'specific'
    assert row['selected_media_id'] == 'mediaA'
    assert row['has_public_reply'] is True
    assert row['has_dm'] is True
    assert row['has_follow_gate'] is True
    assert row['sent'] == 7


def test_automations_summary_does_not_leak_message_text_or_other_accounts(monkeypatch):
    fake_db = _db()
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.list_automations_summary(user_id='u1'))
    payload = str(result)

    assert 'secret public reply text' not in payload
    assert 'secret dm text' not in payload
    assert 'other reply' not in payload
    assert 'other dm' not in payload
    assert 'Other account' not in payload
    assert 'accessToken' not in payload
    assert 'old-token' not in payload


def test_full_automation_detail_still_returns_existing_shape(monkeypatch):
    fake_db = _db()
    monkeypatch.setattr(server, 'db', fake_db)

    result = _run(server.get_automation('autoA', user_id='u1'))

    assert result['id'] == 'autoA'
    assert result['comment_reply'] == 'secret public reply text'
    assert result['dm_text'] == 'secret dm text'
