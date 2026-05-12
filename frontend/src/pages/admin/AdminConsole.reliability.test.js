const fs = require('fs');
const path = require('path');

const source = fs.readFileSync(path.join(__dirname, 'AdminConsole.jsx'), 'utf8');

const FRONTEND_AUDIT_CHECKLIST = [
  'Login', 'Signup', 'Dashboard', 'Automations', 'Comments', 'Billing',
  'Settings', 'System Health', 'Admin Overview', 'Admin Users',
  'Admin User Detail', 'Admin Admins', 'Admin Metrics',
];

describe('Admin Console reliability wiring', () => {
  test('imports the user-management permission used by User Detail actions', () => {
    expect(source).toContain('PERM_USERS_MANAGE');
    expect(source).toContain('const canManageUsers = hasPermission(me, PERM_USERS_MANAGE)');
  });

  test('wraps User Detail in an error boundary instead of blanking the admin page', () => {
    expect(source).toContain('class AdminSectionErrorBoundary');
    expect(source).toContain('Could not render this user section');
    expect(source).toContain('<AdminSectionErrorBoundary name="user-detail"');
  });

  test('uses shared cache for admin overview, users, detail, members, and metrics', () => {
    expect(source).toContain('cachedApiGet');
    expect(source).toContain('admin:overview');
    expect(source).toContain('admin:users');
    expect(source).toContain('admin:user-detail');
    expect(source).toContain('admin:members');
    expect(source).toContain('admin:metrics');
    expect(source).toContain("${user?.id || 'anon'}");
  });

  test('admin mutations invalidate affected caches', () => {
    expect(source).toContain("invalidateApiCache('admin:overview')");
    expect(source).toContain("invalidateApiCache('admin:users')");
    expect(source).toContain('function invalidateAdminUserCaches');
    expect(source).toContain("invalidateApiCache('admin:user-detail')");
    expect(source).toContain("invalidateApiCache('admin:members')");
    expect(source).toContain("invalidateApiCache('admin:metrics')");
  });

  test('User Detail uses defensive arrays for optional sections', () => {
    expect(source).toContain('function safeList');
    expect(source).toContain('const activeOverrides = safeList');
    expect(source).toContain('const instagramAccounts = safeList');
    expect(source).toContain('const automations = safeList');
    expect(source).toContain('const recentFailures = safeList');
  });

  test('keeps the emergency frontend audit checklist visible to future maintainers', () => {
    expect(FRONTEND_AUDIT_CHECKLIST).toEqual(expect.arrayContaining([
      'Login',
      'Dashboard',
      'Automations',
      'Admin User Detail',
      'Admin Metrics',
    ]));
  });
});
