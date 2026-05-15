const fs = require('fs');
const path = require('path');

const src = (...parts) => fs.readFileSync(path.join(__dirname, '..', ...parts), 'utf8');

describe('Google Sign-In production visibility wiring', () => {
  test('Login and Signup include GoogleSignInButton', () => {
    expect(src('pages', 'Login.jsx')).toContain('GoogleSignInButton');
    expect(src('pages', 'Login.jsx')).toContain('<GoogleSignInButton');
    expect(src('pages', 'Signup.jsx')).toContain('GoogleSignInButton');
    expect(src('pages', 'Signup.jsx')).toContain('<GoogleSignInButton');
  });

  test('GoogleSignInButton has visible configured and not-configured states', () => {
    const source = src('components', 'auth', 'GoogleSignInButton.jsx');

    expect(source).toContain('Continue with Google');
    // Phase 2.18R: end-user copy now reads cleanly. We test that the
    // CTA is still present and that the diagnostic data-attribute is
    // kept in the DOM for operator debugging — but the raw
    // google_config_* string is no longer rendered visibly to users.
    expect(source).toMatch(/Google sign-in is unavailable|Google sign-in is not configured/);
    expect(source).toContain('google-config-diagnostics');
    expect(source).toContain('google_config_request_attempted');
    expect(source).toContain('data-google-configured="false"');
    expect(source).toContain('data-google-configured="true"');
  });

  test('System Health renders the Google configured boolean label', () => {
    const source = src('pages', 'SystemHealth.jsx');

    expect(source).toContain('Google Sign-In configured');
    expect(source).toContain('Google config request');
    expect(source).toContain('google-signin-configured');
    expect(source).toContain('google-config-request-status');
    expect(source).not.toContain('REACT_APP_GOOGLE_CLIENT_ID');
  });
});
