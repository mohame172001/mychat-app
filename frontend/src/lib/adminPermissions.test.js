import {
  ROLE_OWNER, ROLE_ADMIN, ROLE_SUPPORT, ROLE_VIEWER, ROLE_USER,
  ADMIN_ROLE_KEYS, ASSIGNABLE_ROLE_KEYS, ROLE_DISPLAY,
  PERM_OVERVIEW_VIEW, PERM_USERS_VIEW, PERM_PLANS_ASSIGN,
  PERM_AUTOMATIONS_DISABLE, PERM_AUDIT_VIEW, PERM_MEMBERS_VIEW,
  PERM_MEMBERS_MANAGE, PERM_OWNER_MANAGE,
  isAdminRole, hasPermission, canManageRole, roleOptionsAssignableBy,
} from './adminPermissions';

describe('role + permission constants are stable', () => {
  test('role names match backend', () => {
    expect(ROLE_OWNER).toBe('owner');
    expect(ROLE_ADMIN).toBe('admin');
    expect(ROLE_SUPPORT).toBe('support');
    expect(ROLE_VIEWER).toBe('viewer');
    expect(ROLE_USER).toBe('user');
  });
  test('admin role list', () => {
    expect(ADMIN_ROLE_KEYS).toEqual(['owner', 'admin', 'support', 'viewer']);
    expect(ASSIGNABLE_ROLE_KEYS).toEqual(['owner', 'admin', 'support', 'viewer']);
  });
  test('display labels exist for each role', () => {
    for (const k of [ROLE_OWNER, ROLE_ADMIN, ROLE_SUPPORT, ROLE_VIEWER, ROLE_USER]) {
      expect(ROLE_DISPLAY[k]).toBeTruthy();
    }
  });
  test('permission keys match backend', () => {
    expect(PERM_OVERVIEW_VIEW).toBe('admin.overview.view');
    expect(PERM_USERS_VIEW).toBe('admin.users.view');
    expect(PERM_PLANS_ASSIGN).toBe('admin.plans.assign');
    expect(PERM_AUTOMATIONS_DISABLE).toBe('admin.automations.disable');
    expect(PERM_AUDIT_VIEW).toBe('admin.audit.view');
    expect(PERM_MEMBERS_VIEW).toBe('admin.members.view');
    expect(PERM_MEMBERS_MANAGE).toBe('admin.members.manage');
    expect(PERM_OWNER_MANAGE).toBe('admin.owner.manage');
  });
});

describe('isAdminRole', () => {
  test('admin roles', () => {
    expect(isAdminRole('owner')).toBe(true);
    expect(isAdminRole('admin')).toBe(true);
    expect(isAdminRole('support')).toBe(true);
    expect(isAdminRole('viewer')).toBe(true);
  });
  test('non-admin', () => {
    expect(isAdminRole('user')).toBe(false);
    expect(isAdminRole(null)).toBe(false);
    expect(isAdminRole(undefined)).toBe(false);
  });
});

describe('hasPermission', () => {
  test('returns false when not admin', () => {
    expect(hasPermission({ is_admin: false, permissions: [PERM_USERS_VIEW] }, PERM_USERS_VIEW))
      .toBe(false);
    expect(hasPermission(null, PERM_USERS_VIEW)).toBe(false);
    expect(hasPermission({}, PERM_USERS_VIEW)).toBe(false);
  });
  test('returns true when permission present', () => {
    expect(hasPermission({
      is_admin: true,
      role: 'admin',
      permissions: [PERM_OVERVIEW_VIEW, PERM_USERS_VIEW, PERM_PLANS_ASSIGN],
    }, PERM_PLANS_ASSIGN)).toBe(true);
  });
  test('returns false when permission missing', () => {
    expect(hasPermission({
      is_admin: true,
      role: 'support',
      permissions: [PERM_OVERVIEW_VIEW, PERM_USERS_VIEW, PERM_AUDIT_VIEW],
    }, PERM_PLANS_ASSIGN)).toBe(false);
  });
});

describe('canManageRole', () => {
  test('owner can manage anyone, including new owner', () => {
    expect(canManageRole(ROLE_OWNER, ROLE_ADMIN, ROLE_OWNER)).toBe(true);
    expect(canManageRole(ROLE_OWNER, ROLE_OWNER, ROLE_ADMIN)).toBe(true);
    expect(canManageRole(ROLE_OWNER, ROLE_VIEWER, ROLE_ADMIN)).toBe(true);
  });
  test('non-owner cannot create or modify owner', () => {
    expect(canManageRole(ROLE_ADMIN, ROLE_OWNER)).toBe(false);
    expect(canManageRole(ROLE_ADMIN, ROLE_VIEWER, ROLE_OWNER)).toBe(false);
  });
  test('admin cannot modify another admin (same rank)', () => {
    expect(canManageRole(ROLE_ADMIN, ROLE_ADMIN, ROLE_VIEWER)).toBe(false);
  });
  test('admin can modify lower ranks', () => {
    expect(canManageRole(ROLE_ADMIN, ROLE_SUPPORT, ROLE_VIEWER)).toBe(true);
    expect(canManageRole(ROLE_ADMIN, ROLE_VIEWER, ROLE_SUPPORT)).toBe(true);
  });
  test('non-admin cannot manage', () => {
    expect(canManageRole(ROLE_USER, ROLE_VIEWER)).toBe(false);
    expect(canManageRole(null, ROLE_VIEWER)).toBe(false);
  });
});

describe('roleOptionsAssignableBy', () => {
  test('owner sees all four roles', () => {
    const opts = roleOptionsAssignableBy(ROLE_OWNER);
    expect(opts.map(o => o.value)).toEqual(['owner', 'admin', 'support', 'viewer']);
  });
  test('admin sees support + viewer only', () => {
    const opts = roleOptionsAssignableBy(ROLE_ADMIN);
    expect(opts.map(o => o.value)).toEqual(['support', 'viewer']);
  });
  test('non-admin sees nothing', () => {
    expect(roleOptionsAssignableBy(ROLE_USER)).toEqual([]);
    expect(roleOptionsAssignableBy(null)).toEqual([]);
  });
});
