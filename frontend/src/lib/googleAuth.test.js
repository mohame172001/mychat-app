import api from './api';
import {
  googleClientId,
  isGoogleAuthEnabled,
  googleStatus,
  loadGoogleRuntimeConfig,
  resetGoogleRuntimeConfigForTests,
  googleErrorMessage,
} from './googleAuth';

jest.mock('./api', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
  },
}));

describe('googleClientId / isGoogleAuthEnabled disabled by default', () => {
  beforeEach(() => {
    delete process.env.REACT_APP_GOOGLE_CLIENT_ID;
    delete process.env.REACT_APP_BACKEND_URL;
    api.get.mockReset();
    resetGoogleRuntimeConfigForTests();
  });

  test('returns empty when not set', () => {
    expect(googleClientId()).toBe('');
    expect(isGoogleAuthEnabled()).toBe(false);
  });

  test('googleStatus reflects disabled', () => {
    const s = googleStatus();
    expect(s.enabled).toBe(false);
    expect(s.client_id_present).toBe(false);
    expect(JSON.stringify(s)).not.toContain('REACT_APP_GOOGLE_CLIENT_ID');
  });
});

describe('isGoogleAuthEnabled enabled when client id set', () => {
  beforeEach(() => {
    process.env.REACT_APP_GOOGLE_CLIENT_ID = 'test-id-123.apps.googleusercontent.com';
    api.get.mockReset();
    resetGoogleRuntimeConfigForTests();
  });

  afterEach(() => {
    delete process.env.REACT_APP_GOOGLE_CLIENT_ID;
    resetGoogleRuntimeConfigForTests();
  });

  test('returns true', () => {
    expect(isGoogleAuthEnabled()).toBe(true);
    expect(googleClientId()).toBe('test-id-123.apps.googleusercontent.com');
  });
});

describe('loadGoogleRuntimeConfig backend fallback', () => {
  beforeEach(() => {
    delete process.env.REACT_APP_GOOGLE_CLIENT_ID;
    delete process.env.REACT_APP_BACKEND_URL;
    api.get.mockReset();
    resetGoogleRuntimeConfigForTests();
  });

  afterEach(() => {
    delete process.env.REACT_APP_BACKEND_URL;
    api.get.mockReset();
    resetGoogleRuntimeConfigForTests();
  });

  test('uses canonical API client and loads public client id from backend', async () => {
    api.get.mockResolvedValue({
      headers: { 'content-type': 'application/json' },
      data: JSON.stringify({
        enabled: true,
        client_id: 'runtime-client-id.apps.googleusercontent.com',
      }),
    });

    const cfg = await loadGoogleRuntimeConfig();

    expect(cfg.enabled).toBe(true);
    expect(cfg.source).toBe('backend');
    expect(googleClientId()).toBe('runtime-client-id.apps.googleusercontent.com');
    expect(isGoogleAuthEnabled()).toBe(true);
    expect(cfg.google_config_request_attempted).toBe(true);
    expect(cfg.google_config_request_ok).toBe(true);
    expect(cfg.google_config_response_enabled).toBe(true);
    expect(cfg.google_config_response_was_json).toBe(true);
    expect(api.get).toHaveBeenCalledWith(
      '/auth/google/config',
      expect.objectContaining({ headers: { Accept: 'application/json' } })
    );
  });

  test('stays disabled when backend config is missing', async () => {
    api.get.mockResolvedValue({
      headers: { 'content-type': 'application/json' },
      data: { enabled: false, client_id: '' },
    });

    const cfg = await loadGoogleRuntimeConfig();

    expect(cfg.enabled).toBe(false);
    expect(cfg.google_config_request_ok).toBe(true);
    expect(cfg.google_config_response_enabled).toBe(false);
    expect(googleClientId()).toBe('');
  });

  test('treats HTML response as invalid config instead of not configured', async () => {
    api.get.mockResolvedValue({
      headers: { 'content-type': 'text/html; charset=utf-8' },
      data: '<!doctype html><html><body>frontend shell</body></html>',
    });

    const cfg = await loadGoogleRuntimeConfig();

    expect(cfg.enabled).toBe(false);
    expect(cfg.google_config_request_attempted).toBe(true);
    expect(cfg.google_config_request_ok).toBe(false);
    expect(cfg.google_config_response_was_json).toBe(false);
    expect(cfg.google_config_error_code).toBe('config_fetch_invalid_response');
  });

  test('failed config fetch returns safe error diagnostics', async () => {
    api.get.mockRejectedValue(new Error('network unavailable'));

    const cfg = await loadGoogleRuntimeConfig();

    expect(cfg.enabled).toBe(false);
    expect(cfg.google_config_request_attempted).toBe(true);
    expect(cfg.google_config_request_ok).toBe(false);
    expect(cfg.google_config_error_code).toBe('config_fetch_failed');
  });
});

describe('googleErrorMessage backend detail mapping', () => {
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

  test('suspended/deleted linked accounts use auth status messages', () => {
    expect(googleErrorMessage('account_suspended'))
      .toBe('Your account is suspended. Contact support.');
    expect(googleErrorMessage('account_deleted'))
      .toBe('Your account has been deleted or disabled. Contact support.');
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
