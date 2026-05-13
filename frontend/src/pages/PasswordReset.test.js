const fs = require('fs');
const path = require('path');

const forgotSrc = fs.readFileSync(path.join(__dirname, 'ForgotPassword.jsx'), 'utf8');
const resetSrc = fs.readFileSync(path.join(__dirname, 'ResetPassword.jsx'), 'utf8');
const loginSrc = fs.readFileSync(path.join(__dirname, 'Login.jsx'), 'utf8');
const appSrc = fs.readFileSync(path.join(__dirname, '..', 'App.js'), 'utf8');

describe('Phase 2.14 account recovery — wiring', () => {
  test('App.js registers /forgot-password and /reset-password public routes', () => {
    expect(appSrc).toContain('/forgot-password');
    expect(appSrc).toContain('/reset-password');
    expect(appSrc).toContain('forgotPasswordFn');
    expect(appSrc).toContain('resetPasswordFn');
  });

  test('Login.jsx has a Forgot password? link pointing to /forgot-password', () => {
    expect(loginSrc).toContain('to="/forgot-password"');
    expect(loginSrc).toMatch(/Forgot password\?/);
    // Existing autocomplete contract preserved.
    expect(loginSrc).toContain('autoComplete="username"');
    expect(loginSrc).toContain('autoComplete="current-password"');
    expect(loginSrc).toContain('PasswordInput');
  });
});

describe('ForgotPassword page', () => {
  test('renders email field with autoComplete="email"', () => {
    expect(forgotSrc).toContain('autoComplete="email"');
    expect(forgotSrc).toContain('id="email"');
  });

  test('calls POST /auth/forgot-password and shows generic success copy', () => {
    expect(forgotSrc).toContain("api.post('/auth/forgot-password'");
    expect(forgotSrc).toMatch(/If an account exists.*reset link/i);
  });

  test('always shows generic success even when API returns a normal error', () => {
    // Any 200 leads to submitted=true; non-429 errors also resolve to
    // submitted=true so the page never leaks user enumeration.
    expect(forgotSrc).toContain('setSubmitted(true)');
    expect(forgotSrc).toContain('status === 429');
  });

  test('does not log token / password / response body', () => {
    expect(forgotSrc).not.toMatch(/console\.log\(.*password/);
    expect(forgotSrc).not.toMatch(/console\.log\(.*token/);
  });
});

describe('ResetPassword page', () => {
  test('renders two password fields with autoComplete="new-password"', () => {
    expect(resetSrc.match(/autoComplete="new-password"/g) || []).toHaveLength(2);
    expect(resetSrc).toContain('id="new-password"');
    expect(resetSrc).toContain('id="confirm-password"');
    expect(resetSrc).toContain('PasswordInput');
  });

  test('reads token from query string and immediately scrubs it from URL', () => {
    expect(resetSrc).toContain("params.get('token')");
    // Token is stripped from the visible URL via setParams + replace.
    expect(resetSrc).toContain('next.delete');
    expect(resetSrc).toMatch(/setParams\(next,\s*\{\s*replace:\s*true\s*\}\)/);
  });

  test('validates password length (>=6) and confirms match', () => {
    expect(resetSrc).toContain('length < 6');
    expect(resetSrc).toContain('newPassword !== confirmPassword');
  });

  test('posts to /auth/reset-password with {token, new_password}', () => {
    expect(resetSrc).toContain("api.post('/auth/reset-password'");
    expect(resetSrc).toContain('new_password: newPassword');
  });

  test('redirects to /login on success and clears in-memory token', () => {
    expect(resetSrc).toContain("navigate('/login'");
    expect(resetSrc).toContain("setToken('')");
  });

  test('does not store token in localStorage / sessionStorage', () => {
    expect(resetSrc).not.toMatch(/localStorage\.setItem\([^)]*token/i);
    expect(resetSrc).not.toMatch(/sessionStorage\.setItem\([^)]*token/i);
  });

  test('does not log raw token or new password', () => {
    expect(resetSrc).not.toMatch(/console\.log\(.*token/i);
    expect(resetSrc).not.toMatch(/console\.log\(.*password/i);
  });

  test('renders friendly states for expired / used / invalid tokens', () => {
    expect(resetSrc).toContain('password_reset_token_expired');
    expect(resetSrc).toContain('password_reset_token_used');
    expect(resetSrc).toContain('invalid_password_reset_token');
    expect(resetSrc).toContain('password_too_short');
  });
});
