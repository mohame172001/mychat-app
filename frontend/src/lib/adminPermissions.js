/**
 * Phase 2.6 — frontend mirror of the backend role/permission catalogue.
 *
 * Source of truth lives in `backend/admin_roles.py`. The values here
 * MUST stay in sync; tests lock the role names and key permissions so
 * a drift breaks CI.
 *
 * Admin nav and per-button gating use these helpers to decide whether
 * to render a control. The backend re-checks permissions on every
 * request, so the UI gate is a UX hint, not a security boundary.
 */

export const ROLE_OWNER = 'owner';
export const ROLE_ADMIN = 'admin';
export const ROLE_SUPPORT = 'support';
export const ROLE_VIEWER = 'viewer';
export const ROLE_USER = 'user';

export const ADMIN_ROLE_KEYS = [ROLE_OWNER, ROLE_ADMIN, ROLE_SUPPORT, ROLE_VIEWER];
export const ASSIGNABLE_ROLE_KEYS = ADMIN_ROLE_KEYS;

export const ROLE_DISPLAY = Object.freeze({
  [ROLE_OWNER]: 'Owner',
  [ROLE_ADMIN]: 'Admin',
  [ROLE_SUPPORT]: 'Support',
  [ROLE_VIEWER]: 'Viewer',
  [ROLE_USER]: 'User',
});

export const PERM_OVERVIEW_VIEW = 'admin.overview.view';
export const PERM_USERS_VIEW = 'admin.users.view';
export const PERM_USERS_MANAGE = 'admin.users.manage';
export const PERM_PLANS_ASSIGN = 'admin.plans.assign';
export const PERM_AUTOMATIONS_DISABLE = 'admin.automations.disable';
export const PERM_FAILURES_VIEW = 'admin.failures.view';
export const PERM_AUDIT_VIEW = 'admin.audit.view';
export const PERM_MEMBERS_VIEW = 'admin.members.view';
export const PERM_MEMBERS_MANAGE = 'admin.members.manage';
export const PERM_OWNER_MANAGE = 'admin.owner.manage';

export function isAdminRole(role) {
  return ADMIN_ROLE_KEYS.includes(role);
}

/**
 * Decide whether a permission is in the caller's permission set.
 * `me` is the payload from /api/admin/me — { is_admin, role, permissions, ... }.
 */
export function hasPermission(me, permission) {
  if (!me || !permission) return false;
  if (!me.is_admin) return false;
  const perms = Array.isArray(me.permissions) ? me.permissions : [];
  return perms.includes(permission);
}

/**
 * Mirror of can_manage_role from backend/admin_roles.py.
 * Used to decide if a 'Change role' or 'Remove' control should render.
 */
export function canManageRole(actorRole, targetRole, newRole = null) {
  if (!isAdminRole(actorRole)) return false;
  if (actorRole === ROLE_OWNER) return true;
  if (targetRole === ROLE_OWNER) return false;
  if (newRole === ROLE_OWNER) return false;
  const order = [ROLE_OWNER, ROLE_ADMIN, ROLE_SUPPORT, ROLE_VIEWER, ROLE_USER];
  const rank = (r) => {
    const i = order.indexOf(r);
    return i < 0 ? order.length : i;
  };
  const actorRank = rank(actorRole);
  if (targetRole && rank(targetRole) <= actorRank) return false;
  if (newRole && rank(newRole) <= actorRank) return false;
  return true;
}

export function roleOptionsAssignableBy(actorRole) {
  return ASSIGNABLE_ROLE_KEYS
    .filter((r) => canManageRole(actorRole, null, r))
    .map((r) => ({ value: r, label: ROLE_DISPLAY[r] }));
}
