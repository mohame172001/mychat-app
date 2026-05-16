const fs = require('fs');
const path = require('path');

const settings = fs.readFileSync(path.join(__dirname, 'Settings.jsx'), 'utf8');

describe('Phase 2.15 — Settings page must not show fake-success toasts', () => {
  test('Profile tab toast.success is gated by a real backend call', () => {
    // Phase 2.18U: Profile editing is now a real feature backed by
    // PATCH /auth/me. The 'Profile updated' toast is allowed, but only
    // if it sits AFTER an awaited api.patch('/auth/me', ...) call so
    // we never show a fake-success again.
    const lines = settings.split('\n');
    const toastLines = lines
      .map((line, idx) => ({ line, idx }))
      .filter(({ line }) => /toast\.success\(['"]Profile updated['"]\)/.test(line));
    if (toastLines.length === 0) {
      // No toast = trivially safe (the old read-only state).
      expect(true).toBe(true);
      return;
    }
    toastLines.forEach(({ line, idx }) => {
      const window = lines.slice(Math.max(0, idx - 20), idx + 1).join('\n');
      expect(window).toMatch(/await\s+api\.(patch|put|post)\(['"]\/auth\/me['"]/);
    });
  });

  test('every toast.success in Settings is reachable only after an awaited API call or an external refresh callback', () => {
    // We enforce: each `toast.success(` line must have an `await api.`
    // OR `refreshUser()` somewhere in the preceding 30 lines of the
    // same callback. This catches the bad pattern without forbidding
    // legitimate post-OAuth success messages.
    const lines = settings.split('\n');
    const failures = [];
    lines.forEach((line, i) => {
      if (!line.includes('toast.success(')) return;
      // Look at preceding 30 lines AND the current line itself — some
      // handlers put `await api.post(...); toast.success(...)` on one
      // statement, which is still a real backend-confirmed success.
      const window = lines.slice(Math.max(0, i - 30), i + 1).join('\n');
      const hasAwait = /await\s+api\./.test(window);
      const hasRefresh = /refreshUser\s*\(/.test(window);
      if (!hasAwait && !hasRefresh) {
        failures.push(`Settings.jsx:${i + 1}: ${line.trim().slice(0, 90)}`);
      }
    });
    expect(failures).toEqual([]);
  });
});
