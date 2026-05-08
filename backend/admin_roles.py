"""Phase 2.6: admin role + permission catalogue.

Centralizes the role hierarchy and the permission matrix so both the
permission gates and the frontend mirror have ONE source of truth.

Roles (most → least privileged):
    owner    — full control, can manage other owners
    admin    — manage users / plans / automations / view usage
    support  — read-only for users / failures / diagnostics
    viewer   — read-only for overview / analytics
    user     — no admin access (default for everyone else)

ADMIN_EMAILS is the bootstrap owner mechanism. A user whose email is in
ADMIN_EMAILS is treated as owner even without an admin_members row, and
on first /api/admin/me call we auto-create the row so the env list can
be removed once the team is in place.
"""
from __future__ import annotations

from typing import Any, Dict, FrozenSet, Optional, Set


ROLE_OWNER = 'owner'
ROLE_ADMIN = 'admin'
ROLE_SUPPORT = 'support'
ROLE_VIEWER = 'viewer'
ROLE_USER = 'user'

# Order from most to least privileged. Used for can_manage_role rank checks.
ROLE_ORDER = (ROLE_OWNER, ROLE_ADMIN, ROLE_SUPPORT, ROLE_VIEWER, ROLE_USER)
ADMIN_ROLE_KEYS = (ROLE_OWNER, ROLE_ADMIN, ROLE_SUPPORT, ROLE_VIEWER)
ASSIGNABLE_ROLE_KEYS = ADMIN_ROLE_KEYS  # API may set member rows to any of these


# ---- Permissions ----------------------------------------------------------

PERM_OVERVIEW_VIEW = 'admin.overview.view'
PERM_USERS_VIEW = 'admin.users.view'
PERM_USERS_MANAGE = 'admin.users.manage'
PERM_PLANS_ASSIGN = 'admin.plans.assign'
PERM_AUTOMATIONS_DISABLE = 'admin.automations.disable'
PERM_FAILURES_VIEW = 'admin.failures.view'
PERM_AUDIT_VIEW = 'admin.audit.view'
PERM_MEMBERS_VIEW = 'admin.members.view'
PERM_MEMBERS_MANAGE = 'admin.members.manage'
PERM_OWNER_MANAGE = 'admin.owner.manage'

ALL_PERMISSIONS: FrozenSet[str] = frozenset({
    PERM_OVERVIEW_VIEW,
    PERM_USERS_VIEW,
    PERM_USERS_MANAGE,
    PERM_PLANS_ASSIGN,
    PERM_AUTOMATIONS_DISABLE,
    PERM_FAILURES_VIEW,
    PERM_AUDIT_VIEW,
    PERM_MEMBERS_VIEW,
    PERM_MEMBERS_MANAGE,
    PERM_OWNER_MANAGE,
})


_ROLE_PERMISSIONS: Dict[str, FrozenSet[str]] = {
    ROLE_OWNER: ALL_PERMISSIONS,
    ROLE_ADMIN: frozenset({
        PERM_OVERVIEW_VIEW,
        PERM_USERS_VIEW,
        PERM_USERS_MANAGE,
        PERM_PLANS_ASSIGN,
        PERM_AUTOMATIONS_DISABLE,
        PERM_FAILURES_VIEW,
        PERM_AUDIT_VIEW,
        PERM_MEMBERS_VIEW,
    }),
    ROLE_SUPPORT: frozenset({
        PERM_OVERVIEW_VIEW,
        PERM_USERS_VIEW,
        PERM_FAILURES_VIEW,
        PERM_AUDIT_VIEW,
    }),
    ROLE_VIEWER: frozenset({
        PERM_OVERVIEW_VIEW,
        PERM_AUDIT_VIEW,
    }),
    ROLE_USER: frozenset(),
}


def is_admin_role(role: Optional[str]) -> bool:
    return role in ADMIN_ROLE_KEYS


def get_role_permissions(role: Optional[str]) -> Set[str]:
    if not role:
        return set()
    return set(_ROLE_PERMISSIONS.get(role, frozenset()))


def has_permission(role: Optional[str], permission: str) -> bool:
    return permission in get_role_permissions(role)


def role_rank(role: Optional[str]) -> int:
    """Lower number = higher rank. Returns max+1 for unknown / non-admin."""
    try:
        return ROLE_ORDER.index(role)
    except ValueError:
        return len(ROLE_ORDER)


def can_manage_role(actor_role: Optional[str], target_role: Optional[str],
                    new_role: Optional[str] = None) -> bool:
    """Decide whether actor_role can act on target_role / set them to new_role.

    Rules:
      - owner can do anything (subject to 'last owner' invariant which is
        enforced separately).
      - non-owner cannot create or modify an owner.
      - non-owner can only modify roles strictly below their own rank, and
        only assign roles strictly below their own rank.
    """
    if not is_admin_role(actor_role):
        return False
    if actor_role == ROLE_OWNER:
        return True
    # non-owner constraints:
    if target_role == ROLE_OWNER:
        return False
    if new_role == ROLE_OWNER:
        return False
    actor_rank = role_rank(actor_role)
    if target_role and role_rank(target_role) <= actor_rank:
        return False
    if new_role and role_rank(new_role) <= actor_rank:
        return False
    return True


def is_valid_assignable_role(role: Any) -> bool:
    return isinstance(role, str) and role in ASSIGNABLE_ROLE_KEYS
