const fs = require('fs');
const path = require('path');

const settings = fs.readFileSync(path.join(__dirname, 'Settings.jsx'), 'utf8');

describe('Phase 2.15 — Settings page must not show fake-success toasts', () => {
  test('Profile tab must NOT show a "Profile updated" toast without a backend call', () => {
    // The original bug: a Save button that called toast.success with no
    // API call attached. Phase 2.15 cleanup removed that button entirely
    // and made the Profile fields read-only until an edit endpoint exists.
    expect(settings).not.toMatch(/toast\.success\(['"]Profile updated['"]\)/);
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
