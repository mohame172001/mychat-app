import { instagramErrorMessage } from '../lib/instagramErrors';

test('Instagram duplicate account error is explicit and safe', () => {
  expect(instagramErrorMessage('instagram_account_already_connected'))
    .toBe('This Instagram account is already connected to another MyChat account. Contact support if you own this account.');
});

test('Instagram error fallback does not expose owner identity', () => {
  const message = instagramErrorMessage('instagram_account_already_connected');
  expect(message).not.toMatch(/@/);
  expect(message).not.toMatch(/email/i);
});
