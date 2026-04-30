import asyncio
import os
import sys
from datetime import datetime
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
        return SimpleNamespace(matched_count=1, modified_count=1)


class FakeDB:
    def __init__(self, automations=None, instagram_accounts=None):
        self.automations = FakeCollection(automations or [])
        self.instagram_accounts = FakeCollection(instagram_accounts or [])
        self.users = FakeCollection([])
        self.comments = FakeCollection([])
        self.comment_dm_sessions = FakeCollection([])


def _run(coro):
    return asyncio.run(coro)


def test_automation_patch_model_accepts_comment_reply_variations():
    """The Pydantic model must NOT silently drop variation 2 and 3.
    This is the root cause: AutomationPatch was missing these fields."""
    patch = AutomationPatch(
        comment_reply='one',
        comment_reply_2='two',
        comment_reply_3='three',
    )
    dumped = patch.model_dump(exclude_none=True)
    assert dumped['comment_reply'] == 'one'
    assert dumped['comment_reply_2'] == 'two', \
        'comment_reply_2 was being silently dropped before the fix'
    assert dumped['comment_reply_3'] == 'three', \
        'comment_reply_3 was being silently dropped before the fix'


def test_patch_persists_all_three_variations_and_rebuilds_n_reply(monkeypatch):
    """End-to-end: PATCH /automations/{id} with three reply variations
    must persist all three AND rebuild the n_reply node so the runtime
    randomizer sees the new list."""
    aid = 'a1'
    existing = {
        'id': aid,
        'user_id': 'u1',
        'instagramAccountId': 'ig1',
        'igUserId': 'ig1',
        'comment_reply': 'old reply',
        'comment_reply_2': '',
        'comment_reply_3': '',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'old reply', 'replies': ['old reply']}},
        ],
        'edges': [{'id': 'e1', 'source': 'n_trigger', 'target': 'n_reply'}],
        'status': 'active',
    }
    db = FakeDB(automations=[existing])
    monkeypatch.setattr(server, 'db', db)

    # Bypass account scoping helpers.
    async def _no_account(_user_id):
        return None
    monkeypatch.setattr(server, 'getActiveInstagramAccount', _no_account)
    monkeypatch.setattr(server, '_account_scoped_query',
                        lambda _u, _a: {'user_id': 'u1'})
    monkeypatch.setattr(server, '_is_comment_automation_rule', lambda _d: True)

    body = AutomationPatch(
        comment_reply='تعليقك ✅ 😍',
        comment_reply_2='بعتلك التفاصيل 👌',
        comment_reply_3='شوف الرسايل 🚀',
    )
    _run(server.patch_automation(aid, body, user_id='u1'))

    saved = db.automations.docs[0]
    assert saved['comment_reply'] == 'تعليقك ✅ 😍'
    assert saved['comment_reply_2'] == 'بعتلك التفاصيل 👌'
    assert saved['comment_reply_3'] == 'شوف الرسايل 🚀'

    # The n_reply node must carry the full list so execute_flow can
    # random.choice() across all three.
    n_reply = next(n for n in saved['nodes'] if n['id'] == 'n_reply')
    assert n_reply['data']['replies'] == [
        'تعليقك ✅ 😍',
        'بعتلك التفاصيل 👌',
        'شوف الرسايل 🚀',
    ]
    # The legacy single-text field should still be set to a usable value.
    assert n_reply['data']['text'] in n_reply['data']['replies']


def test_patch_with_only_two_variations_drops_empty_third(monkeypatch):
    """Empty variation 3 must be filtered out of the rebuilt node so
    runtime random.choice never picks an empty string."""
    aid = 'a2'
    existing = {
        'id': aid, 'user_id': 'u1',
        'comment_reply': 'one', 'comment_reply_2': 'two', 'comment_reply_3': 'three',
        'nodes': [{'id': 'n_reply', 'type': 'reply_comment',
                   'data': {'text': 'one', 'replies': ['one', 'two', 'three']}}],
        'edges': [],
    }
    db = FakeDB(automations=[existing])
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'getActiveInstagramAccount',
                        lambda _u: asyncio.sleep(0))
    monkeypatch.setattr(server, '_account_scoped_query',
                        lambda _u, _a: {'user_id': 'u1'})
    monkeypatch.setattr(server, '_is_comment_automation_rule', lambda _d: True)

    body = AutomationPatch(comment_reply_3='')
    _run(server.patch_automation(aid, body, user_id='u1'))

    saved = db.automations.docs[0]
    n_reply = next(n for n in saved['nodes'] if n['id'] == 'n_reply')
    assert n_reply['data']['replies'] == ['one', 'two'], \
        f"empty variation should be filtered; got {n_reply['data']['replies']}"


def test_runtime_random_choice_picks_only_non_empty_variations(monkeypatch):
    """execute_flow's reply_comment branch must use random.choice over the
    saved replies list — never the empty string."""
    user = {'id': 'u1', 'meta_access_token': 't', 'ig_user_id': 'ig1'}
    automation = {
        'id': 'a1',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'one', 'replies': ['one', 'two', 'three']}},
        ],
        'edges': [{'id': 'e1', 'source': 'n_trigger', 'target': 'n_reply'}],
    }

    sent_texts = []

    async def fake_reply(token, comment_id, text):  # noqa: ARG001
        sent_texts.append(text)
        return True

    monkeypatch.setattr(server, 'reply_to_ig_comment', fake_reply)
    fake_db = FakeDB(automations=[dict(automation)])
    monkeypatch.setattr(server, 'db', fake_db)

    # Run many times to confirm randomness ranges over the list and
    # never picks an empty string.
    for _ in range(50):
        _run(server.execute_flow(
            user, dict(automation), 'commenter1', '',
            comment_context={'ig_comment_id': 'c1', 'comment_doc_id': 'd1'},
        ))

    assert sent_texts, 'no replies were sent'
    assert all(t in {'one', 'two', 'three'} for t in sent_texts), \
        f'unexpected reply texts sent: {set(sent_texts) - {"one","two","three"}}'
    # In 50 iterations across 3 options, we expect at least 2 distinct
    # values (probability of all-same is essentially zero).
    assert len(set(sent_texts)) >= 2, \
        f'random.choice did not vary replies: only saw {set(sent_texts)}'


def test_runtime_falls_back_to_legacy_single_reply(monkeypatch):
    """An old automation that only has node.data.text (no replies list)
    must still send the legacy single reply."""
    user = {'id': 'u1', 'meta_access_token': 't', 'ig_user_id': 'ig1'}
    automation = {
        'id': 'a1',
        'nodes': [
            {'id': 'n_trigger', 'type': 'trigger', 'data': {}},
            {'id': 'n_reply', 'type': 'reply_comment',
             'data': {'text': 'legacy reply'}},  # no replies list
        ],
        'edges': [{'id': 'e1', 'source': 'n_trigger', 'target': 'n_reply'}],
    }
    sent_texts = []

    async def fake_reply(token, comment_id, text):  # noqa: ARG001
        sent_texts.append(text)
        return True
    monkeypatch.setattr(server, 'reply_to_ig_comment', fake_reply)
    monkeypatch.setattr(server, 'db', FakeDB(automations=[dict(automation)]))

    _run(server.execute_flow(
        user, dict(automation), 'commenter1', '',
        comment_context={'ig_comment_id': 'c1', 'comment_doc_id': 'd1'},
    ))
    assert sent_texts == ['legacy reply']
