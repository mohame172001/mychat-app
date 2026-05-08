"""Phase 2.6: admin role + permission gating + members CRUD tests."""
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
from test_instagram_token_refresh import FakeDB, _account, _run, _user  # noqa: E402


def _setup(monkeypatch, *, users=None, members=None, automations=None,
           admin_emails=None):
    fake_db = FakeDB(
        _account(id='accA', userId=(users or [{}])[0].get('id', 'u1'),
                 instagramAccountId='igA'),
        users or [_user(id='u1', email='u1@example.com')],
        automations=automations or [],
        admin_members=members or [],
    )
    monkeypatch.setattr(server, 'db', fake_db)
    monkeypatch.setattr(server, 'ADMIN_EMAILS', admin_emails or set())
    return fake_db


# ── admin_roles module unit tests ───────────────────────────────────────────

def test_role_permissions_matrix():
    assert _ar.has_permission(_ar.ROLE_OWNER, _ar.PERM_OWNER_MANAGE)
    assert _ar.has_permission(_ar.ROLE_ADMIN, _ar.PERM_PLANS_ASSIGN)
    assert not _ar.has_permission(_ar.ROLE_ADMIN, _ar.PERM_OWNER_MANAGE)
    assert not _ar.has_permission(_ar.ROLE_ADMIN, _ar.PERM_MEMBERS_MANAGE)
    assert _ar.has_permission(_ar.ROLE_SUPPORT, _ar.PERM_USERS_VIEW)
    assert not _ar.has_permission(_ar.ROLE_SUPPORT, _ar.PERM_PLANS_ASSIGN)
    assert not _ar.has_permission(_ar.ROLE_SUPPORT, _ar.PERM_AUTOMATIONS_DISABLE)
    assert _ar.has_permission(_ar.ROLE_VIEWER, _ar.PERM_OVERVIEW_VIEW)
    assert not _ar.has_permission(_ar.ROLE_VIEWER, _ar.PERM_USERS_VIEW)
    assert not _ar.has_permission(_ar.ROLE_USER, _ar.PERM_OVERVIEW_VIEW)
    assert not _ar.has_permission(None, _ar.PERM_OVERVIEW_VIEW)


def test_can_manage_role_rules():
    # owner can manage anyone, including making owners.
    assert _ar.can_manage_role(_ar.ROLE_OWNER, _ar.ROLE_ADMIN, _ar.ROLE_OWNER)
    assert _ar.can_manage_role(_ar.ROLE_OWNER, _ar.ROLE_OWNER, _ar.ROLE_ADMIN)
    # admin cannot create/modify owners.
    assert not _ar.can_manage_role(_ar.ROLE_ADMIN, _ar.ROLE_OWNER)
    assert not _ar.can_manage_role(_ar.ROLE_ADMIN, _ar.ROLE_VIEWER, _ar.ROLE_OWNER)
    # admin cannot modify another admin (same rank).
    assert not _ar.can_manage_role(_ar.ROLE_ADMIN, _ar.ROLE_ADMIN, _ar.ROLE_VIEWER)
    # admin can manage support / viewer (lower rank).
    assert _ar.can_manage_role(_ar.ROLE_ADMIN, _ar.ROLE_SUPPORT, _ar.ROLE_VIEWER)
    assert _ar.can_manage_role(_ar.ROLE_ADMIN, _ar.ROLE_VIEWER, _ar.ROLE_SUPPORT)
    # NOTE: can_manage_role is the rank-comparison helper; the actual
    # endpoint is gated by PERM_MEMBERS_MANAGE which only owner has,
    # so non-owners can never reach this code path. We still test the
    # rank logic for defense-in-depth.
    # viewer/user have no admin authority below them.
    assert not _ar.can_manage_role(_ar.ROLE_VIEWER, _ar.ROLE_VIEWER)
    assert not _ar.can_manage_role(_ar.ROLE_USER, _ar.ROLE_VIEWER)
    # admin cannot escalate themselves to owner.
    assert not _ar.can_manage_role(_ar.ROLE_ADMIN, _ar.ROLE_VIEWER, _ar.ROLE_OWNER)


# ── /admin/me ──────────────────────────────────────────────────────────────

def test_admin_me_for_user_returns_no_role(monkeypatch):
    _setup(monkeypatch, users=[_user(id='u1', email='u1@example.com')])
    res = _run(server.admin_me(user_id='u1'))
    assert res['is_admin'] is False
    assert res['role'] is None
    assert res['permissions'] == []
    assert res['bootstrap_owner'] is False


def test_admin_me_bootstrap_owner_via_admin_emails_lazy_creates_member(monkeypatch):
    db = _setup(monkeypatch,
                users=[_user(id='owner_u', email='owner@mychat.app')],
                admin_emails={'owner@mychat.app'})
    res = _run(server.admin_me(user_id='owner_u'))
    assert res['is_admin'] is True
    assert res['role'] == 'owner'
    assert res['bootstrap_owner'] is True
    assert _ar.PERM_OWNER_MANAGE in res['permissions']
    # admin_members row was lazy-created.
    assert len(db.admin_members.docs) == 1
    assert db.admin_members.docs[0]['role'] == 'owner'


def test_admin_me_for_admin_member_row(monkeypatch):
    _setup(monkeypatch,
           users=[_user(id='ad_u', email='ad@example.com')],
           members=[{'id': 'm1', 'user_id': 'ad_u', 'email': 'ad@example.com',
                     'role': 'admin', 'disabled_at': None}])
    res = _run(server.admin_me(user_id='ad_u'))
    assert res['is_admin'] is True
    assert res['role'] == 'admin'
    assert res['bootstrap_owner'] is False
    assert _ar.PERM_PLANS_ASSIGN in res['permissions']
    assert _ar.PERM_OWNER_MANAGE not in res['permissions']


def test_admin_me_for_disabled_member_returns_user(monkeypatch):
    _setup(monkeypatch,
           users=[_user(id='ad_u', email='ad@example.com')],
           members=[{'id': 'm1', 'user_id': 'ad_u', 'email': 'ad@example.com',
                     'role': 'admin',
                     'disabled_at': datetime.utcnow()}])
    res = _run(server.admin_me(user_id='ad_u'))
    assert res['is_admin'] is False


# ── permission gates on existing endpoints ─────────────────────────────────

def test_admin_overview_blocked_for_user(monkeypatch):
    _setup(monkeypatch, users=[_user(id='u1', email='u1@example.com')])
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_overview(user_id='u1'))
    assert exc.value.status_code == 403


def test_admin_overview_allowed_for_viewer(monkeypatch):
    _setup(monkeypatch,
           users=[_user(id='v_u', email='v@example.com')],
           members=[{'id': 'm1', 'user_id': 'v_u', 'email': 'v@example.com',
                     'role': 'viewer', 'disabled_at': None}])
    res = _run(server.admin_overview(user_id='v_u'))
    assert 'total_users' in res


def test_users_list_blocked_for_viewer(monkeypatch):
    _setup(monkeypatch,
           users=[_user(id='v_u', email='v@example.com')],
           members=[{'id': 'm1', 'user_id': 'v_u', 'email': 'v@example.com',
                     'role': 'viewer', 'disabled_at': None}])
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_users_list(
            page=1, page_size=10, search=None, plan_key=None,
            sort=None, user_id='v_u',
        ))
    assert exc.value.status_code == 403


def test_users_list_allowed_for_support(monkeypatch):
    _setup(monkeypatch,
           users=[_user(id='s_u', email='s@example.com')],
           members=[{'id': 'm1', 'user_id': 's_u', 'email': 's@example.com',
                     'role': 'support', 'disabled_at': None}])
    res = _run(server.admin_users_list(
        page=1, page_size=10, search=None, plan_key=None,
        sort=None, user_id='s_u',
    ))
    assert 'items' in res


def test_plan_assign_blocked_for_support(monkeypatch):
    _setup(monkeypatch,
           users=[_user(id='s_u', email='s@example.com'),
                  _user(id='target', email='t@example.com')],
           members=[{'id': 'm1', 'user_id': 's_u', 'email': 's@example.com',
                     'role': 'support', 'disabled_at': None}])
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_assign_user_plan(
            'target', body={'plan_key': 'pro'}, user_id='s_u',
        ))
    assert exc.value.status_code == 403


def test_plan_assign_allowed_for_admin(monkeypatch):
    db = _setup(monkeypatch,
                users=[_user(id='ad_u', email='ad@example.com'),
                       _user(id='target', email='t@example.com')],
                members=[{'id': 'm1', 'user_id': 'ad_u', 'email': 'ad@example.com',
                          'role': 'admin', 'disabled_at': None}])
    res = _run(server.admin_assign_user_plan(
        'target', body={'plan_key': 'pro', 'reason': 'test'}, user_id='ad_u',
    ))
    assert res['ok'] is True
    # plan_assign audit row written.
    assert any(a['action'] == 'plan_assign' for a in db.admin_audit_logs.docs)


def test_automation_disable_blocked_for_support(monkeypatch):
    _setup(monkeypatch,
           users=[_user(id='s_u', email='s@example.com')],
           members=[{'id': 'm1', 'user_id': 's_u', 'email': 's@example.com',
                     'role': 'support', 'disabled_at': None}],
           automations=[{'id': 'a1', 'user_id': 'x', 'status': 'active'}])
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_disable_automation(
            'a1', body={'reason': 'spam'}, user_id='s_u',
        ))
    assert exc.value.status_code == 403


# ── members CRUD ────────────────────────────────────────────────────────────

def _owner_setup(monkeypatch, *, extra_members=None, extra_users=None):
    return _setup(
        monkeypatch,
        users=[
            _user(id='owner_u', email='owner@mychat.app'),
            *(extra_users or []),
        ],
        members=[
            {'id': 'm0', 'user_id': 'owner_u', 'email': 'owner@mychat.app',
             'role': 'owner', 'disabled_at': None},
            *(extra_members or []),
        ],
    )


def test_owner_can_list_members(monkeypatch):
    _owner_setup(monkeypatch)
    res = _run(server.admin_members_list(user_id='owner_u'))
    assert res['count'] == 1
    assert res['items'][0]['role'] == 'owner'


def test_admin_can_list_members_but_cannot_manage(monkeypatch):
    _setup(monkeypatch,
           users=[_user(id='ad_u', email='ad@example.com')],
           members=[{'id': 'm1', 'user_id': 'ad_u', 'email': 'ad@example.com',
                     'role': 'admin', 'disabled_at': None}])
    res = _run(server.admin_members_list(user_id='ad_u'))
    assert res['count'] == 1
    # admin cannot add a member.
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_members_add(
            body={'email': 'new@example.com', 'role': 'admin'},
            user_id='ad_u',
        ))
    assert exc.value.status_code == 403


def test_owner_can_add_admin(monkeypatch):
    db = _owner_setup(
        monkeypatch,
        extra_users=[_user(id='target_u', email='target@example.com')],
    )
    res = _run(server.admin_members_add(
        body={'email': 'target@example.com', 'role': 'admin', 'reason': 'team'},
        user_id='owner_u',
    ))
    assert res['ok'] is True
    assert res['member']['role'] == 'admin'
    # audit log row.
    assert any(a['action'] == 'admin_member_added' for a in db.admin_audit_logs.docs)
    # Email is hashed in audit metadata, never raw.
    audit = next(a for a in db.admin_audit_logs.docs if a['action'] == 'admin_member_added')
    assert 'target@example.com' not in repr(audit)
    assert audit['metadata']['target_email_hash']
    assert audit['metadata']['new_role'] == 'admin'


def test_owner_can_add_support_and_viewer(monkeypatch):
    _owner_setup(
        monkeypatch,
        extra_users=[
            _user(id='s_u', email='s@example.com'),
            _user(id='v_u', email='v@example.com'),
        ],
    )
    r1 = _run(server.admin_members_add(
        body={'email': 's@example.com', 'role': 'support'}, user_id='owner_u'))
    r2 = _run(server.admin_members_add(
        body={'email': 'v@example.com', 'role': 'viewer'}, user_id='owner_u'))
    assert r1['member']['role'] == 'support'
    assert r2['member']['role'] == 'viewer'


def test_add_member_unknown_email_404(monkeypatch):
    _owner_setup(monkeypatch)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_members_add(
            body={'email': 'ghost@nowhere.io', 'role': 'admin'}, user_id='owner_u',
        ))
    assert exc.value.status_code == 404


def test_add_member_invalid_role_400(monkeypatch):
    _owner_setup(monkeypatch,
                 extra_users=[_user(id='t', email='t@example.com')])
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_members_add(
            body={'email': 't@example.com', 'role': 'sysop'}, user_id='owner_u',
        ))
    assert exc.value.status_code == 400


def test_admin_cannot_create_owner(monkeypatch):
    db = _setup(monkeypatch,
                users=[_user(id='ad_u', email='ad@example.com'),
                       _user(id='target_u', email='t@example.com')],
                members=[{'id': 'm1', 'user_id': 'ad_u', 'email': 'ad@example.com',
                          'role': 'admin', 'disabled_at': None}])
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_members_add(
            body={'email': 't@example.com', 'role': 'owner'}, user_id='ad_u',
        ))
    assert exc.value.status_code == 403


def test_owner_can_change_role(monkeypatch):
    db = _owner_setup(monkeypatch, extra_members=[
        {'id': 'm1', 'user_id': 'mem_u', 'email': 'mem@example.com',
         'role': 'admin', 'disabled_at': None},
    ])
    res = _run(server.admin_members_update(
        'mem_u', body={'role': 'support', 'reason': 'demote'}, user_id='owner_u',
    ))
    assert res['member']['role'] == 'support'
    assert any(a['action'] == 'admin_member_role_changed'
               for a in db.admin_audit_logs.docs)


def test_owner_can_remove_admin(monkeypatch):
    db = _owner_setup(monkeypatch, extra_members=[
        {'id': 'm1', 'user_id': 'mem_u', 'email': 'mem@example.com',
         'role': 'admin', 'disabled_at': None},
    ])
    res = _run(server.admin_members_remove('mem_u', user_id='owner_u'))
    assert res['ok'] is True
    saved = next(d for d in db.admin_members.docs if d['user_id'] == 'mem_u')
    assert saved['disabled_at'] is not None
    assert any(a['action'] == 'admin_member_removed'
               for a in db.admin_audit_logs.docs)


def test_last_owner_cannot_be_demoted(monkeypatch):
    _owner_setup(monkeypatch)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_members_update(
            'owner_u', body={'role': 'admin'}, user_id='owner_u',
        ))
    assert exc.value.status_code == 409


def test_last_owner_cannot_be_removed(monkeypatch):
    _owner_setup(monkeypatch)
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_members_remove('owner_u', user_id='owner_u'))
    assert exc.value.status_code == 409


def test_owner_can_demote_when_another_owner_exists(monkeypatch):
    _owner_setup(monkeypatch, extra_members=[
        {'id': 'm1', 'user_id': 'second_owner', 'email': 's@x.com',
         'role': 'owner', 'disabled_at': None},
    ])
    res = _run(server.admin_members_update(
        'owner_u', body={'role': 'admin'}, user_id='owner_u',
    ))
    assert res['member']['role'] == 'admin'


def test_admin_cannot_remove_admin_member(monkeypatch):
    _setup(monkeypatch,
           users=[_user(id='a1', email='a1@example.com'),
                  _user(id='a2', email='a2@example.com')],
           members=[
               {'id': 'm1', 'user_id': 'a1', 'email': 'a1@example.com',
                'role': 'admin', 'disabled_at': None},
               {'id': 'm2', 'user_id': 'a2', 'email': 'a2@example.com',
                'role': 'admin', 'disabled_at': None},
           ])
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_members_remove('a2', user_id='a1'))
    assert exc.value.status_code == 403


def test_disabled_member_loses_admin_access(monkeypatch):
    _setup(monkeypatch,
           users=[_user(id='ad_u', email='ad@example.com')],
           members=[{'id': 'm1', 'user_id': 'ad_u', 'email': 'ad@example.com',
                     'role': 'admin',
                     'disabled_at': datetime.utcnow()}])
    with pytest.raises(server.HTTPException) as exc:
        _run(server.admin_overview(user_id='ad_u'))
    assert exc.value.status_code == 403
