"""Phase 2.12C: IDOR/RBAC/mass-assignment regression coverage."""
import asyncio
import os
import sys
from datetime import datetime
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
import admin_roles as _ar  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


def _match(doc, query):
    for key, expected in (query or {}).items():
        if key == '$or':
            if not any(_match(doc, item) for item in expected):
                return False
            continue
        value = doc.get(key)
        if isinstance(expected, dict):
            if '$exists' in expected and ((key in doc) != expected['$exists']):
                return False
            if '$in' in expected and value not in expected['$in']:
                return False
            if '$ne' in expected and value == expected['$ne']:
                return False
        elif value != expected:
            return False
    return True


class FakeCursor:
    def __init__(self, docs):
        self.docs = list(docs)
        self._limit = None

    def sort(self, *_args, **_kwargs):
        return self

    def skip(self, n):
        self.docs = self.docs[n:]
        return self

    def limit(self, n):
        self._limit = n
        return self

    async def to_list(self, n):
        cap = self._limit if self._limit is not None else n
        return list(self.docs[:cap])

    def __aiter__(self):
        docs = list(self.docs[: self._limit] if self._limit is not None else self.docs)

        async def _gen():
            for doc in docs:
                yield doc

        return _gen()


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def find_one(self, query, *_, **__):
        return next((doc for doc in self.docs if _match(doc, query)), None)

    def find(self, query=None):
        return FakeCursor([doc for doc in self.docs if _match(doc, query or {})])

    async def insert_one(self, doc):
        self.docs.append(dict(doc))
        return SimpleNamespace(inserted_id=doc.get('id'))

    async def update_one(self, query, update, upsert=False):
        doc = await self.find_one(query)
        if not doc and upsert:
            doc = dict(query)
            self.docs.append(doc)
        if not doc:
            return SimpleNamespace(matched_count=0, modified_count=0)
        if '$setOnInsert' in update and upsert:
            for key, value in update['$setOnInsert'].items():
                doc.setdefault(key, value)
        for key, value in update.get('$set', {}).items():
            doc[key] = value
        for key, value in update.get('$inc', {}).items():
            doc[key] = int(doc.get(key) or 0) + int(value)
        return SimpleNamespace(matched_count=1, modified_count=1)

    async def update_many(self, query, update):
        count = 0
        for doc in self.docs:
            if not _match(doc, query):
                continue
            for key, value in update.get('$set', {}).items():
                doc[key] = value
            count += 1
        return SimpleNamespace(matched_count=count, modified_count=count)

    async def delete_one(self, query):
        for idx, doc in enumerate(self.docs):
            if _match(doc, query):
                self.docs.pop(idx)
                return SimpleNamespace(deleted_count=1)
        return SimpleNamespace(deleted_count=0)

    async def count_documents(self, query):
        return len([doc for doc in self.docs if _match(doc, query)])


class FakeDB:
    def __init__(self, **collections):
        self.users = FakeCollection(collections.get('users', []))
        self.instagram_accounts = FakeCollection(collections.get('instagram_accounts', []))
        self.automations = FakeCollection(collections.get('automations', []))
        self.comments = FakeCollection(collections.get('comments', []))
        self.contacts = FakeCollection(collections.get('contacts', []))
        self.broadcasts = FakeCollection(collections.get('broadcasts', []))
        self.admin_members = FakeCollection(collections.get('admin_members', []))
        self.admin_audit_logs = FakeCollection(collections.get('admin_audit_logs', []))
        self.user_limit_overrides = FakeCollection(collections.get('user_limit_overrides', []))
        self.user_plans = FakeCollection(collections.get('user_plans', []))
        self.monthly_usage = FakeCollection(collections.get('monthly_usage', []))
        self.instagram_account_trial_claims = FakeCollection(
            collections.get('instagram_account_trial_claims', [])
        )


def _user(user_id, email, **overrides):
    doc = {
        'id': user_id,
        'email': email,
        'username': user_id,
        'name': user_id,
        'status': 'active',
        'ig_user_id': 'biz1',
        'instagramConnected': True,
        'instagram_connection_valid': True,
        'active_instagram_account_id': 'acc1',
        'meta_access_token': 'token',
    }
    doc.update(overrides)
    return doc


def _account(user_id='u1', ig='biz1'):
    return {
        'id': f'acc-{ig}',
        'userId': user_id,
        'user_id': user_id,
        'instagramAccountId': ig,
        'igUserId': ig,
        'username': f'ig_{ig}',
        'accessToken': 'token',
        'connectionValid': True,
        'isActive': True,
        'isCurrent': True,
    }


def _automation(auto_id='a1', user_id='u1', ig='biz1', **overrides):
    doc = {
        'id': auto_id,
        'user_id': user_id,
        'name': 'Auto',
        'status': 'draft',
        'trigger': 'Manual',
        'nodes': [],
        'edges': [],
        'instagramAccountId': ig,
        'igUserId': ig,
        'instagramUsername': f'ig_{ig}',
    }
    doc.update(overrides)
    return doc


def _setup_active_account(monkeypatch, ig='biz1'):
    async def fake_active(_user_id):
        return _account(_user_id, ig)

    monkeypatch.setattr(server, 'getActiveInstagramAccount', fake_active)


def test_automation_patch_cannot_mass_assign_instagram_account_context(monkeypatch):
    db = FakeDB(
        users=[_user('u1', 'u1@example.com')],
        automations=[_automation()],
    )
    monkeypatch.setattr(server, 'db', db)
    _setup_active_account(monkeypatch, 'biz1')

    patch = server.AutomationPatch(
        name='Renamed',
        instagramAccountId='biz_other',
        igUserId='biz_other',
        instagramUsername='other_owner',
    )

    result = _run(server.patch_automation('a1', patch, user_id='u1'))

    saved = db.automations.docs[0]
    assert result['name'] == 'Renamed'
    assert saved['instagramAccountId'] == 'biz1'
    assert saved['igUserId'] == 'biz1'
    assert saved['instagramUsername'] == 'ig_biz1'


def test_create_automation_ignores_client_supplied_instagram_account_context(monkeypatch):
    db = FakeDB(users=[_user('u1', 'u1@example.com')], automations=[])
    monkeypatch.setattr(server, 'db', db)
    _setup_active_account(monkeypatch, 'biz1')

    payload = server.AutomationIn(
        name='New auto',
        instagramAccountId='biz_other',
        igUserId='biz_other',
        instagramUsername='other_owner',
    )

    result = _run(server.create_automation(payload, user_id='u1'))

    assert result['user_id'] == 'u1'
    assert result['instagramAccountId'] == 'biz1'
    assert result['igUserId'] == 'biz1'
    assert result['instagramUsername'] == 'ig_biz1'
    assert db.automations.docs[0]['instagramAccountId'] == 'biz1'


def test_create_automation_rejects_selected_media_not_owned_by_active_account(monkeypatch):
    db = FakeDB(users=[_user('u1', 'u1@example.com')], automations=[])
    monkeypatch.setattr(server, 'db', db)
    _setup_active_account(monkeypatch, 'biz1')

    async def fake_recent_media(_token, _ig_user_id, limit=10):
        return ['media-owned']

    monkeypatch.setattr(server, '_fetch_recent_media_ids', fake_recent_media)
    payload = server.AutomationIn(
        name='Bad selected post',
        status='active',
        trigger='comment:media-other',
        media_id='media-other',
    )

    with pytest.raises(server.HTTPException) as exc:
        _run(server.create_automation(payload, user_id='u1'))

    assert exc.value.status_code == 400
    assert exc.value.detail == 'selected_media_not_found_or_not_owned'
    assert db.automations.docs == []


def test_patch_automation_rejects_selected_media_from_another_account(monkeypatch):
    db = FakeDB(
        users=[_user('u1', 'u1@example.com')],
        automations=[_automation(status='active', trigger='comment:media-owned', media_id='media-owned')],
    )
    monkeypatch.setattr(server, 'db', db)
    _setup_active_account(monkeypatch, 'biz1')

    async def fake_recent_media(_token, _ig_user_id, limit=10):
        return ['media-owned']

    monkeypatch.setattr(server, '_fetch_recent_media_ids', fake_recent_media)

    with pytest.raises(server.HTTPException) as exc:
        _run(server.patch_automation(
            'a1',
            server.AutomationPatch(media_id='media-other', trigger='comment:media-other'),
            user_id='u1',
        ))

    assert exc.value.status_code == 400
    assert exc.value.detail == 'selected_media_not_found_or_not_owned'
    assert db.automations.docs[0]['media_id'] == 'media-owned'


def test_activation_rejects_disconnected_instagram_account(monkeypatch):
    db = FakeDB(
        users=[_user('u1', 'u1@example.com')],
        automations=[_automation(status='paused', trigger='comment:any')],
    )
    monkeypatch.setattr(server, 'db', db)

    async def disconnected_account(_user_id):
        account = _account(_user_id, 'biz1')
        account['connectionValid'] = False
        return account

    monkeypatch.setattr(server, 'getActiveInstagramAccount', disconnected_account)

    with pytest.raises(server.HTTPException) as exc:
        _run(server.patch_automation('a1', server.AutomationPatch(status='active'), user_id='u1'))

    assert exc.value.status_code == 400
    assert exc.value.detail == 'instagram_reconnect_required'
    assert db.automations.docs[0]['status'] == 'paused'


def test_user_cannot_access_or_mutate_other_users_automation(monkeypatch):
    db = FakeDB(
        users=[_user('u1', 'u1@example.com')],
        automations=[_automation('other_auto', user_id='u2', ig='biz2')],
    )
    monkeypatch.setattr(server, 'db', db)
    _setup_active_account(monkeypatch, 'biz1')

    with pytest.raises(server.HTTPException) as exc:
        _run(server.get_automation('other_auto', user_id='u1'))
    assert exc.value.status_code == 404

    with pytest.raises(server.HTTPException) as exc:
        _run(server.patch_automation(
            'other_auto', server.AutomationPatch(name='stolen'), user_id='u1',
        ))
    assert exc.value.status_code == 404

    with pytest.raises(server.HTTPException) as exc:
        _run(server.delete_automation('other_auto', user_id='u1'))
    assert exc.value.status_code == 404
    assert db.automations.docs[0]['name'] == 'Auto'


def test_scoped_updates_do_not_touch_duplicate_contact_or_broadcast_ids(monkeypatch):
    db = FakeDB(
        users=[_user('u1', 'u1@example.com')],
        contacts=[
            {'id': 'same', 'user_id': 'u2', 'name': 'Other', 'username': 'other'},
            {'id': 'same', 'user_id': 'u1', 'name': 'Mine', 'username': 'mine'},
        ],
        broadcasts=[
            {'id': 'b_same', 'user_id': 'u2', 'name': 'Other', 'message': 'x'},
            {'id': 'b_same', 'user_id': 'u1', 'name': 'Mine', 'message': 'x'},
        ],
    )
    monkeypatch.setattr(server, 'db', db)

    contact = _run(server.patch_contact(
        'same', server.ContactPatch(name='Mine updated'), user_id='u1',
    ))
    broadcast = _run(server.patch_broadcast(
        'b_same', server.BroadcastPatch(name='Broadcast updated'), user_id='u1',
    ))

    assert contact['user_id'] == 'u1'
    assert contact['name'] == 'Mine updated'
    assert db.contacts.docs[0]['name'] == 'Other'
    assert broadcast['user_id'] == 'u1'
    assert broadcast['name'] == 'Broadcast updated'
    assert db.broadcasts.docs[0]['name'] == 'Other'


def test_support_cannot_suspend_or_soft_delete_users(monkeypatch):
    db = FakeDB(
        users=[
            _user('support_u', 'support@example.com'),
            _user('target_u', 'target@example.com'),
        ],
        admin_members=[
            {'id': 'm1', 'user_id': 'support_u', 'email': 'support@example.com',
             'role': _ar.ROLE_SUPPORT, 'disabled_at': None},
        ],
    )
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())

    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_suspend_user(
            'target_u', body={'reason': 'not allowed'}, user_id='support_u',
        ))
    assert exc.value.status_code == 403

    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_soft_delete_user(
            'target_u', body={'reason': 'not allowed'}, user_id='support_u',
        ))
    assert exc.value.status_code == 403
    assert db.users.docs[1]['status'] == 'active'


def test_admin_member_create_ignores_forbidden_body_fields_and_sanitizes_audit(monkeypatch):
    db = FakeDB(
        users=[
            _user('owner_u', 'owner@example.com'),
            _user('target_u', 'target@example.com'),
        ],
        admin_members=[
            {'id': 'm0', 'user_id': 'owner_u', 'email': 'owner@example.com',
             'role': _ar.ROLE_OWNER, 'disabled_at': None},
        ],
    )
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())

    result = _run(server.admin_members_add(
        body={
            'email': 'target@example.com',
            'role': _ar.ROLE_VIEWER,
            'reason': 'contains private ticket text',
            'disabled_at': datetime.utcnow(),
            'added_by_email': 'attacker@example.com',
            'permissions': [_ar.PERM_OWNER_MANAGE],
        },
        user_id='owner_u',
    ))

    saved = next(row for row in db.admin_members.docs if row.get('user_id') == 'target_u')
    assert result['member']['role'] == _ar.ROLE_VIEWER
    assert saved['disabled_at'] is None
    assert saved['added_by_email'] == 'owner@example.com'
    assert 'permissions' not in saved
    audit = next(row for row in db.admin_audit_logs.docs if row['action'] == 'admin_member_added')
    assert 'contains private ticket text' not in repr(audit)
    assert audit['metadata']['reason_length'] == len('contains private ticket text')


def test_limit_override_ignores_forbidden_subject_and_status_fields(monkeypatch):
    db = FakeDB(
        users=[
            _user('admin_u', 'admin@example.com'),
            _user('target_u', 'target@example.com'),
        ],
        admin_members=[
            {'id': 'm1', 'user_id': 'admin_u', 'email': 'admin@example.com',
             'role': _ar.ROLE_ADMIN, 'disabled_at': None},
        ],
    )
    monkeypatch.setattr(server, 'db', db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', set())

    result = _run(server.admin_create_user_override(
        'target_u',
        body={
            'type': 'additive_allowance',
            'metrics': {
                'comments_processed_extra': 100,
                'is_admin': 1,
                'usage_counters': 999999,
            },
            'status': 'revoked',
            'user_id': 'other_user',
            'limit_subject_type': 'instagram_account',
            'limit_subject_id': 'biz_other',
            'created_by_email': 'attacker@example.com',
        },
        user_id='admin_u',
    ))

    saved = db.user_limit_overrides.docs[0]
    assert result['override']['metrics'] == {'comments_processed_extra': 100}
    assert saved['user_id'] == 'target_u'
    assert saved['status'] == 'active'
    assert saved['created_by_email'] == 'admin@example.com'
    assert 'limit_subject_id' not in saved
    assert 'limit_subject_type' not in saved
