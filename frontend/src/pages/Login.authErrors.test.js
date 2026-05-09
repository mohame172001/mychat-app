const fs = require('fs');
const path = require('path');

describe('Login auth error handling wiring', () => {
  const source = fs.readFileSync(path.join(__dirname, 'Login.jsx'), 'utf8');

  test('uses the shared auth error mapper instead of a generic login failure', () => {
    expect(source).toContain('authErrorMessageFromApiError');
    expect(source).not.toContain("toast.error(err?.response?.data?.detail || 'Login failed')");
  });
});

