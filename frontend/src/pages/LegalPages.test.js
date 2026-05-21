const fs = require('fs');
const path = require('path');

const src = (...parts) => fs.readFileSync(path.join(__dirname, '..', ...parts), 'utf8');

describe('Meta review public legal pages', () => {
  test('App exposes privacy, terms, and data deletion public routes', () => {
    const app = src('App.js');
    const routes = src('constants', 'routes.js');

    expect(routes).toContain("PRIVACY: '/privacy'");
    expect(routes).toContain("TERMS: '/terms'");
    expect(routes).toContain("DATA_DELETION: '/data-deletion'");
    expect(app).toContain('path={ROUTES.PRIVACY}');
    expect(app).toContain('path={ROUTES.TERMS}');
    expect(app).toContain('path={ROUTES.DATA_DELETION}');
    expect(app).toContain("import('./pages/DataDeletion')");
  });

  test('Login and Signup link to data deletion instructions', () => {
    expect(src('pages', 'Login.jsx')).toContain('/data-deletion');
    expect(src('pages', 'Signup.jsx')).toContain('/data-deletion');
  });

  test('Data deletion page documents callback without showing secrets', () => {
    const page = src('pages', 'DataDeletion.jsx');

    expect(page).toContain('/meta/data-deletion');
    expect(page).toContain('Data Deletion Request');
    expect(page).toContain('confirmation_code');
    expect(page).not.toContain('access_token');
    expect(page).not.toContain('client_secret');
    expect(page).not.toContain('Authorization');
  });

  test('Privacy policy avoids unsupported token encryption claims', () => {
    const privacy = src('pages', 'PrivacyPolicy.jsx');

    expect(privacy).toContain('Instagram data');
    expect(privacy).toContain('Data Deletion page');
    expect(privacy).toContain('stored only on the backend');
    expect(privacy).not.toContain('encrypted at rest');
  });
});
