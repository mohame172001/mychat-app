import asyncio
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault('MONGO_URL', 'mongodb://localhost:27017/test')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://example.com')
os.environ.setdefault('FRONTEND_URL', 'https://example.com')
os.environ.setdefault('IG_APP_ID', '123')
os.environ.setdefault('IG_APP_SECRET', 'secret')
os.environ.setdefault('CRON_SECRET', 'cron-secret')

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server  # noqa: E402


def _match(doc, query):
    for key, expected in query.items():
        value = doc.get(key)
        if isinstance(expected, dict):
            if '$exists' in expected and ((key in doc) != expected['$exists']):
                return False
            if '$ne' in expected and value == expected['$ne']:
                return False
        elif value != expected:
            return False
    return True


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def find_one(self, query, sort=None):  # noqa: ARG002
        return next((doc for doc in self.docs if _match(doc, query)), None)

    async def insert_one(self, doc):
        self.docs.append(dict(doc))
        return SimpleNamespace(inserted_id=doc.get('id'))

    async def update_one(self, query, update, upsert=False):  # noqa: ARG002
        doc = await self.find_one(query)
        if not doc:
            return SimpleNamespace(matched_count=0, modified_count=0)
        for k, v in update.get('$set', {}).items():
            doc[k] = v
        for k, v in update.get('$inc', {}).items():
            doc[k] = int(doc.get(k) or 0) + v
        return SimpleNamespace(matched_count=1, modified_count=1)


class FakeDB:
    def __init__(self, sessions=None):
        self.comment_dm_sessions = FakeCollection(sessions or [])
        self.tracked_links = FakeCollection([])


def _user(token='tok'):
    return {'id': 'u1', 'ig_user_id': 'biz1', 'meta_access_token': token}


def _session(**overrides):
    now = datetime.utcnow()
    base = {
        'id': 's1',
        'user_id': 'u1',
        'ig_user_id': 'biz1',
        'recipient_id': 'igsid-9001',
        'automation_id': 'a1',
        'link_dm_text': 'Here is the link',
        'link_button_text': 'Open',
        'link_url': 'https://example.com/landing',
        'conversionTrackingEnabled': False,
        'follow_request_enabled': True,
        'verify_actual_follow': True,
        'follow_request_message': 'follow first',
        'follow_request_button_text': 'I followed',
        'follow_confirmation_keywords': ['i followed'],
        'follow_not_detected_message': 'still not detected',
        'follow_verification_failed_message': 'cannot verify',
        'follow_retry_button_text': 'I followed',
        'max_follow_verification_attempts': 3,
        'follow_confirmed': True,  # user has tapped
        'follow_verified': False,
        'follow_verification_attempts': 0,
        'followLastCheckedAt': None,
        'followReminderCount': 0,
        'finalDmSentAt': None,
        'stage': 'awaiting_follow_confirmation',
        'status': 'pending',
        'created': now,
        'updated': now,
        'expiresAt': now + timedelta(minutes=60),
    }
    base.update(overrides)
    return base


def _run(coro):
    return asyncio.run(coro)


# --- helpers for fake API + send tracking ---------------------------------

def install_fakes(monkeypatch, follows_returns, send_calls):
    """Patch the verification helper and all DM senders.

    follows_returns: list of dicts in the order verify is invoked.
    send_calls: dict accumulator with lists of calls per channel.
    """
    queue = list(follows_returns)

    async def fake_verify(token, igsid):  # noqa: ARG001
        send_calls['verify'].append({'igsid': igsid, 'token_present': bool(token)})
        if not queue:
            return {'ok': False, 'reason': 'temporary_api_error', 'raw_status': None}
        return queue.pop(0)

    async def fake_quick_reply(token, ig_user, recipient, text, button, payload):  # noqa: ARG001
        send_calls['quick_reply'].append(
            {'recipient': recipient, 'text': text, 'button': button, 'payload': payload}
        )
        return {'ok': True, 'body': {'message_id': 'm-qr'}}

    async def fake_url_button(token, ig_user, recipient, text, button, url):  # noqa: ARG001
        send_calls['url_button'].append(
            {'recipient': recipient, 'text': text, 'button': button, 'url': url}
        )
        return {'ok': True, 'body': {'message_id': 'm-link'}}

    async def fake_dm(token, ig_user, recipient, text):  # noqa: ARG001
        send_calls['text_dm'].append({'recipient': recipient, 'text': text})
        return True

    monkeypatch.setattr(server, 'verify_instagram_user_follows_business', fake_verify)
    monkeypatch.setattr(server, 'send_ig_quick_reply', fake_quick_reply)
    monkeypatch.setattr(server, 'send_ig_url_button', fake_url_button)
    monkeypatch.setattr(server, 'send_ig_dm', fake_dm)


# --- tests ---------------------------------------------------------------

def test_follows_true_sends_link(monkeypatch):
    sess = _session()
    db = FakeDB(sessions=[sess])
    monkeypatch.setattr(server, 'db', db)
    calls = {'verify': [], 'quick_reply': [], 'url_button': [], 'text_dm': []}
    install_fakes(monkeypatch, [{'ok': True, 'follows': True, 'raw_status': 200,
                                 'profile_excerpt': {'username': 'u'}}], calls)

    ok = _run(server._send_comment_dm_flow_completion(_user(), sess))

    assert ok is True
    assert len(calls['verify']) == 1
    # Final link button must have been delivered.
    assert any(c['url'] == 'https://example.com/landing' for c in calls['url_button'])
    final = db.comment_dm_sessions.docs[0]
    assert final['follow_verified'] is True
    assert final['stage'] == 'final_sent'
    assert final['status'] == 'completed'


def test_follows_false_sends_reminder_and_no_link(monkeypatch):
    sess = _session()
    db = FakeDB(sessions=[sess])
    monkeypatch.setattr(server, 'db', db)
    calls = {'verify': [], 'quick_reply': [], 'url_button': [], 'text_dm': []}
    install_fakes(monkeypatch, [{'ok': True, 'follows': False, 'raw_status': 200,
                                 'profile_excerpt': {'username': 'u'}}], calls)

    _run(server._send_comment_dm_flow_completion(_user(), sess))

    # Reminder went out via quick reply.
    assert any('still not detected' in (c['text'] or '') for c in calls['quick_reply']), \
        f'reminder not sent; calls={calls}'
    # Link was NOT sent.
    assert calls['url_button'] == []
    final = db.comment_dm_sessions.docs[0]
    assert final['follow_verified'] is False
    assert final['stage'] == 'awaiting_actual_follow'
    # follow_confirmed reset so the user must click again.
    assert final['follow_confirmed'] is False


def test_second_confirmation_after_follow_sends_link(monkeypatch):
    sess = _session()
    db = FakeDB(sessions=[sess])
    monkeypatch.setattr(server, 'db', db)
    calls = {'verify': [], 'quick_reply': [], 'url_button': [], 'text_dm': []}
    install_fakes(
        monkeypatch,
        [
            {'ok': True, 'follows': False, 'raw_status': 200, 'profile_excerpt': {}},
            {'ok': True, 'follows': True, 'raw_status': 200, 'profile_excerpt': {}},
        ],
        calls,
    )

    # First attempt — not following.
    _run(server._send_comment_dm_flow_completion(_user(), sess))
    persisted = db.comment_dm_sessions.docs[0]
    assert persisted['follow_verified'] is False
    assert calls['url_button'] == []

    # User taps again — bypass cooldown so the test isn't time-dependent.
    persisted['follow_confirmed'] = True
    persisted['followLastCheckedAt'] = datetime.utcnow() - timedelta(minutes=2)

    _run(server._send_comment_dm_flow_completion(_user(), persisted))
    final = db.comment_dm_sessions.docs[0]
    assert final['follow_verified'] is True
    assert any(c['url'] == 'https://example.com/landing' for c in calls['url_button'])


def test_temporary_api_error_does_not_send_link(monkeypatch):
    sess = _session()
    db = FakeDB(sessions=[sess])
    monkeypatch.setattr(server, 'db', db)
    calls = {'verify': [], 'quick_reply': [], 'url_button': [], 'text_dm': []}
    install_fakes(
        monkeypatch,
        [{'ok': False, 'reason': 'temporary_api_error', 'raw_status': 500}],
        calls,
    )

    _run(server._send_comment_dm_flow_completion(_user(), sess))

    assert calls['url_button'] == []  # link withheld
    final = db.comment_dm_sessions.docs[0]
    assert final['follow_verified'] is False
    assert final['lastFollowVerificationError'] == 'temporary_api_error'
    # Status remains pending, not verification_failed.
    assert final['status'] == 'pending'


def test_permission_error_marks_verification_failed(monkeypatch):
    sess = _session()
    db = FakeDB(sessions=[sess])
    monkeypatch.setattr(server, 'db', db)
    calls = {'verify': [], 'quick_reply': [], 'url_button': [], 'text_dm': []}
    install_fakes(
        monkeypatch,
        [{'ok': False, 'reason': 'permission_or_consent_required', 'raw_status': 400}],
        calls,
    )

    _run(server._send_comment_dm_flow_completion(_user(), sess))

    assert calls['url_button'] == []
    final = db.comment_dm_sessions.docs[0]
    assert final['stage'] == 'verification_failed'
    assert final['status'] == 'verification_failed'


def test_duplicate_completion_does_not_resend_link(monkeypatch):
    sess = _session(follow_verified=True, finalDmSentAt=datetime.utcnow(),
                    stage='final_sent')
    db = FakeDB(sessions=[sess])
    monkeypatch.setattr(server, 'db', db)
    calls = {'verify': [], 'quick_reply': [], 'url_button': [], 'text_dm': []}
    install_fakes(monkeypatch, [], calls)

    ok = _run(server._send_comment_dm_flow_completion(_user(), sess))

    assert ok is True
    assert calls['url_button'] == []
    assert calls['verify'] == []  # no API call when already final


def test_max_attempts_exceeded_marks_verification_failed(monkeypatch):
    sess = _session(follow_verification_attempts=3,
                    max_follow_verification_attempts=3)
    db = FakeDB(sessions=[sess])
    monkeypatch.setattr(server, 'db', db)
    calls = {'verify': [], 'quick_reply': [], 'url_button': [], 'text_dm': []}
    install_fakes(monkeypatch, [], calls)

    _run(server._send_comment_dm_flow_completion(_user(), sess))

    assert calls['verify'] == []
    final = db.comment_dm_sessions.docs[0]
    assert final['stage'] == 'verification_failed'
    assert calls['url_button'] == []


def test_follow_gate_disabled_sends_link_directly(monkeypatch):
    sess = _session(follow_request_enabled=False, follow_confirmed=False)
    db = FakeDB(sessions=[sess])
    monkeypatch.setattr(server, 'db', db)
    calls = {'verify': [], 'quick_reply': [], 'url_button': [], 'text_dm': []}
    install_fakes(monkeypatch, [], calls)

    ok = _run(server._send_comment_dm_flow_completion(_user(), sess))

    assert ok is True
    assert calls['verify'] == []  # never called
    assert any(c['url'] == 'https://example.com/landing' for c in calls['url_button'])


def test_cooldown_sends_notice_and_does_not_call_meta(monkeypatch):
    """Second tap inside cooldown must not call Meta but must answer."""
    sess = _session(
        follow_verification_attempts=1,
        followLastCheckedAt=datetime.utcnow() - timedelta(seconds=5),
        follow_cooldown_message='wait a few seconds',
    )
    db = FakeDB(sessions=[sess])
    monkeypatch.setattr(server, 'db', db)
    calls = {'verify': [], 'quick_reply': [], 'url_button': [], 'text_dm': []}
    install_fakes(monkeypatch, [], calls)

    _run(server._send_comment_dm_flow_completion(_user(), sess))

    assert calls['verify'] == []  # Meta not called during cooldown
    assert calls['url_button'] == []  # link withheld
    assert any('wait a few seconds' in (c['text'] or '')
               for c in calls['quick_reply']), \
        f'cooldown notice not sent; calls={calls}'
    persisted = db.comment_dm_sessions.docs[0]
    assert persisted.get('lastCooldownNoticeAt') is not None


def test_cooldown_notice_rate_limited_to_once_per_window(monkeypatch):
    """Two rapid taps inside cooldown produce only one cooldown notice."""
    now = datetime.utcnow()
    sess = _session(
        follow_verification_attempts=1,
        followLastCheckedAt=now - timedelta(seconds=5),
        lastCooldownNoticeAt=now - timedelta(seconds=2),
        follow_cooldown_message='wait',
    )
    db = FakeDB(sessions=[sess])
    monkeypatch.setattr(server, 'db', db)
    calls = {'verify': [], 'quick_reply': [], 'url_button': [], 'text_dm': []}
    install_fakes(monkeypatch, [], calls)

    _run(server._send_comment_dm_flow_completion(_user(), sess))

    # Already notified within the window — stay silent.
    assert calls['quick_reply'] == []
    assert calls['text_dm'] == []
    assert calls['url_button'] == []
    assert calls['verify'] == []


def test_legacy_click_only_when_verify_actual_follow_disabled(monkeypatch):
    sess = _session(verify_actual_follow=False, follow_confirmed=True)
    db = FakeDB(sessions=[sess])
    monkeypatch.setattr(server, 'db', db)
    calls = {'verify': [], 'quick_reply': [], 'url_button': [], 'text_dm': []}
    install_fakes(monkeypatch, [], calls)

    ok = _run(server._send_comment_dm_flow_completion(_user(), sess))

    assert ok is True
    assert calls['verify'] == []  # legacy gate, no API call
    assert any(c['url'] == 'https://example.com/landing' for c in calls['url_button'])
