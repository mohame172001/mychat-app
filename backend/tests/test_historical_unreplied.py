"""Tests for safe processing of unreplied historical comments.

Covers the bug where comments created before a rule's activation timestamp
were skipped as historical_before_rule_activation even after the admin
enabled process_existing_unreplied_comments. Also covers the dedup
guarantee that an already-successfully-replied comment is never replied
again, and the model-level acceptance of the new flag on PATCH.
"""
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
from models import AutomationPatch  # noqa: E402


# ----- Lightweight fakes (mirrors patterns in other test files) -----

def _match(doc, query):
    for key, expected in query.items():
        if key in ('$or',):
            if not any(_match(doc, item) for item in expected):
                return False
            continue
        value = doc.get(key)
        if isinstance(expected, dict):
            if '$ne' in expected and value == expected['$ne']:
                return False
            if '$in' in expected and value not in expected['$in']:
                return False
            if '$exists' in expected and ((key in doc) != expected['$exists']):
                return False
        elif value != expected:
            return False
    return True


class _Cursor:
    def __init__(self, docs):
        self.docs = docs
    def sort(self, *_a, **_kw):
        return self
    def limit(self, n):
        self.docs = self.docs[:n]
        return self
    async def to_list(self, n):
        return list(self.docs[:n])


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def find_one(self, query, sort=None):  # noqa: ARG002
        return next((d for d in self.docs if _match(d, query)), None)

    def find(self, query=None):
        if query is None:
            return _Cursor(list(self.docs))
        return _Cursor([d for d in self.docs if _match(d, query)])

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

    async def count_documents(self, query):  # noqa: ARG002
        return len([d for d in self.docs if _match(d, query)])


class FakeDB:
    def __init__(self, automations=None, comments=None, users=None):
        self.automations = FakeCollection(automations or [])
        self.comments = FakeCollection(comments or [])
        self.users = FakeCollection(users or [])
        self.tracked_links = FakeCollection([])
        self.comment_dm_sessions = FakeCollection([])


def _run(coro):
    return asyncio.run(coro)


def _user():
    return {'id': 'u1', 'ig_user_id': 'biz1', 'meta_access_token': 'tok',
            'instagramConnected': True, 'instagramHandle': '@biz'}


def _rule(*, process_existing_unreplied=False, process_existing=False):
    """A simple keyword-match rule activated yesterday."""
    return {
        'id': 'r1',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'status': 'active',
        'trigger': 'keyword:any',
        'match': 'any',
        'mode': 'reply_only',
        'activationStartedAt': datetime.utcnow() - timedelta(hours=1),
        'createdAt': datetime.utcnow() - timedelta(hours=1),
        'processExistingComments': process_existing,
        'process_existing_unreplied_comments': process_existing_unreplied,
        'comment_reply': 'thanks!',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'thanks!', 'replies': ['thanks!']}},
        ],
        'edges': [{'id': 'e1', 'source': 'n_trigger', 'target': 'n_reply'}],
    }


def _comment(ts: datetime, **overrides):
    base = {
        'ig_comment_id': 'c1',
        'commenter_id': 'commenter-1',
        'commenter_username': 'fan',
        'media_id': 'm1',
        'text': 'price?',
        'timestamp': ts.strftime('%Y-%m-%dT%H:%M:%S+0000'),
    }
    base.update(overrides)
    return base


def _stub_helpers(monkeypatch):
    """Common stubs to keep _handle_new_comment focused on the cutoff path."""
    async def _no(*_a, **_kw):
        return None
    monkeypatch.setattr(server, '_account_scoped_query',
                        lambda _u, _ig: {'user_id': 'u1'})
    # matchesAutomationRule fires for keyword:any
    monkeypatch.setattr(server, 'matchesAutomationRule',
                        lambda auto, text, ctx: {'matches': True})
    monkeypatch.setattr(server, '_fetch_latest_media_id',
                        lambda *a, **k: _no())
    # Bypass the actual flow execution; just record that it ran.
    ran = {'count': 0, 'rules': []}
    async def fake_run(user_doc, automation, *a, **kw):  # noqa: ARG001
        ran['count'] += 1
        ran['rules'].append(automation.get('id'))
        # Record success on the comment doc (mimicking _run_and_record_action).
    monkeypatch.setattr(server, '_run_and_record_action', fake_run)
    return ran


# -------------------- Tests --------------------

def test_default_skips_historical_comment(monkeypatch):
    """A historical comment with no flags enabled stays skipped."""
    rule = _rule(process_existing_unreplied=False, process_existing=False)
    db = FakeDB(automations=[rule])
    monkeypatch.setattr(server, 'db', db)
    _stub_helpers(monkeypatch)
    historical_ts = datetime.utcnow() - timedelta(days=2)

    res = _run(server._handle_new_comment(_user(),
                                          _comment(historical_ts), source='polling'))

    assert res.get('matched') is False, \
        f'historical comment should not match without flag; got {res}'
    saved = db.comments.docs[0]
    assert saved['skip_reason'] == 'historical_before_rule_activation'


def test_process_existing_unreplied_flag_allows_historical(monkeypatch):
    """When the flag is on, an unreplied historical comment is processed."""
    rule = _rule(process_existing_unreplied=True)
    db = FakeDB(automations=[rule])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_helpers(monkeypatch)
    historical_ts = datetime.utcnow() - timedelta(days=2)

    res = _run(server._handle_new_comment(_user(),
                                          _comment(historical_ts), source='polling'))

    assert res.get('matched') is True, f'flag did not allow historical match; got {res}'
    assert ran['count'] == 1, 'rule action should have fired'
    saved = db.comments.docs[0]
    assert saved['skip_reason'] != 'historical_before_rule_activation'


def test_already_replied_historical_is_not_replied_again(monkeypatch):
    """Even with the flag on, a previously successfully-replied comment
    must be detected by dedup and skipped (no second reply)."""
    rule = _rule(process_existing_unreplied=True)
    historical_ts = datetime.utcnow() - timedelta(days=2)
    existing = {
        'id': 'cdoc1',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'ig_comment_id': 'c1',
        'replied': True,
        'action_status': 'success',
        'created': historical_ts,
    }
    db = FakeDB(automations=[rule], comments=[existing])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_helpers(monkeypatch)

    res = _run(server._handle_new_comment(_user(),
                                          _comment(historical_ts), source='polling'))

    assert res.get('already_processed') is True, \
        f'duplicate must short-circuit; got {res}'
    assert ran['count'] == 0, 'no second reply'


def test_previously_skipped_historical_is_retried_when_flag_enabled(monkeypatch):
    """Comment was skipped historically with the flag OFF. Admin enables
    the flag. On the next poll, the dedup logic must allow re-evaluation
    (historical_before_rule_activation is now in retryable_skip)."""
    rule = _rule(process_existing_unreplied=True)
    historical_ts = datetime.utcnow() - timedelta(days=2)
    existing = {
        'id': 'cdoc1',
        'user_id': 'u1',
        'instagramAccountId': 'biz1',
        'igUserId': 'biz1',
        'ig_comment_id': 'c1',
        'replied': False,
        'action_status': 'skipped',
        'skip_reason': 'historical_before_rule_activation',
        'created': historical_ts,
    }
    db = FakeDB(automations=[rule], comments=[existing])
    monkeypatch.setattr(server, 'db', db)
    ran = _stub_helpers(monkeypatch)

    res = _run(server._handle_new_comment(_user(),
                                          _comment(historical_ts), source='polling'))

    assert res.get('reprocessed') is True, \
        f'previously historical comment must be re-evaluated; got {res}'
    assert ran['count'] == 1, 'reply should fire on the re-evaluation'


def test_account_isolation_under_unreplied_flag(monkeypatch):
    """A rule on account B does not match a comment from account A."""
    rule_b = _rule(process_existing_unreplied=True)
    rule_b['instagramAccountId'] = 'biz2'
    rule_b['igUserId'] = 'biz2'
    rule_b['user_id'] = 'u2'
    db = FakeDB(automations=[rule_b])
    monkeypatch.setattr(server, 'db', db)
    _stub_helpers(monkeypatch)
    monkeypatch.setattr(
        server, '_account_scoped_query',
        lambda u, ig: {'user_id': u, 'instagramAccountId': ig}
    )

    res = _run(server._handle_new_comment(_user(),
                                          _comment(datetime.utcnow() - timedelta(days=1)),
                                          source='polling'))

    assert res.get('matched') is False
    saved = db.comments.docs[0]
    assert saved['skip_reason'] in ('no_rule_match', 'historical_before_rule_activation')


def test_patch_model_accepts_process_existing_unreplied_flag():
    """Pydantic must accept the new field on PATCH so it isn't silently
    dropped — this is the same class of bug as the previous comment_reply_2 fix."""
    p = AutomationPatch(process_existing_unreplied_comments=True)
    dumped = p.model_dump(exclude_none=True)
    assert dumped['process_existing_unreplied_comments'] is True

    p2 = AutomationPatch(processExistingUnrepliedComments=True)
    dumped2 = p2.model_dump(exclude_none=True)
    assert dumped2['processExistingUnrepliedComments'] is True
