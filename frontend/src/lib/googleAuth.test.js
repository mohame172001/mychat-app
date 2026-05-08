import {
  googleClientId,
  isGoogleAuthEnabled,
  googleStatus,
  googleErrorMessage,
} from './googleAuth';

describe('googleClientId / isGoogleAuthEnabled — disabled by default', () => {
  beforeEach(() => {
    delete process.env.REACT_APP_GOOGLE_CLIENT_ID;
  });

  test('returns empty when not set', () => {
    expect(googleClientId()).toBe('');
    expect(isGoogleAuthEnabled()).toBe(false);
  });

  test('googleStatus reflects disabled', () => {
    const s = googleStatus();
    expect(s.enabled).toBe(false);
    expect(s.client_id_present).toBe(false);
    // Status payload must NOT echo the client id value.
    expect(JSON.stringify(s)).not.toContain('REACT_APP_GOOGLE_CLIENT_ID');
  });
});

describe('isGoogleAuthEnabled — enabled when client id set', () => {
  beforeEach(() => {
    process.env.REACT_APP_GOOGLE_CLIENT_ID = 'test-id-123.apps.googleusercontent.com';
  });
  afterEach(() => {
    delete process.env.REACT_APP_GOOGLE_CLIENT_ID;
  });

  test('returns true', () => {
    expect(isGoogleAuthEnabled()).toBe(true);
    expect(googleClientId()).toBe('test-id-123.apps.googleusercontent.com');
  });
});

describe('googleErrorMessage — backend detail mapping', () => {
  test('not configured', () => {
    expect(googleErrorMessage('google_auth_not_configured'))
      .toBe('Google sign-in is not configured');
  });
  test('sdk missing', () => {
    expect(googleErrorMessage('google_auth_sdk_not_installed'))
      .toBe('Google sign-in is temporarily unavailable');
  });
  test('email not verified', () => {
    expect(googleErrorMessage('google_email_not_verified'))
      .toBe('Email not verified by Google');
  });
  test('account already linked', () => {
    expect(googleErrorMessage('google_account_already_linked'))
      .toBe('This Google account is already linked to another user');
  });
  test('cross-account conflict', () => {
    expect(googleErrorMessage('google_account_conflict'))
      .toContain('conflicting');
  });
  test('expired token', () => {
    expect(googleErrorMessage('google_credential_invalid:token_expired'))
      .toBe('Google sign-in expired, try again');
  });
  test('generic invalid', () => {
    expect(googleErrorMessage('google_credential_invalid:wrong_audience'))
      .toBe('Google sign-in failed');
  });
  test('null/empty fallback', () => {
    expect(googleErrorMessage(null)).toBe('Google sign-in failed');
    expect(googleErrorMessage('')).toBe('Google sign-in failed');
    expect(googleErrorMessage(undefined)).toBe('Google sign-in failed');
  });
});
