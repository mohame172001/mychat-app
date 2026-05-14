import { instagramErrorMessage } from '../lib/instagramErrors';
const fs = require('fs');
const path = require('path');

test('Instagram duplicate account error is explicit and points to a recovery path', () => {
  const message = instagramErrorMessage('instagram_account_already_connected');
  expect(message).toContain('already linked');
  expect(message).toMatch(/different MyChat account/i);
  expect(message).toMatch(/contact support/i);
});

test('Instagram duplicate error does not leak the owning user identity', () => {
  const message = instagramErrorMessage('instagram_account_already_connected');
  expect(message).not.toMatch(/@\w/);
  expect(message).not.toMatch(/\bemail\s*:/i);
});

test('All known OAuth callback reasons map to user-friendly text, not raw codes', () => {
  const reasons = [
    'oauth_denied',
    'access_denied',
    'invalid_state',
    'missing_code',
    'token_exchange_failed',
    'token_cannot_call_graph_me',
    'server_error',
  ];
  for (const reason of reasons) {
    const msg = instagramErrorMessage(reason);
    expect(msg.length).toBeGreaterThan(20);
    // No bare reason token should be the entire message.
    expect(msg).not.toBe(reason);
  }
});

test('Unknown reason still produces a usable message instead of "Server Error"', () => {
  const message = instagramErrorMessage('some_new_reason_we_did_not_anticipate');
  expect(message).toMatch(/instagram connection failed/i);
  expect(message).toMatch(/retry|try again|connect again/i);
});

test('Settings reconnects existing invalid Instagram identity instead of adding another account', () => {
  const source = fs.readFileSync(path.join(__dirname, 'Settings.jsx'), 'utf8');

  expect(source).toContain("mode: 'reconnect'");
  expect(source).toContain('instagramReconnectMode');
  expect(source).toContain('Reconnect Instagram');
  expect(source).not.toContain("mode: 'add_account'");
});
