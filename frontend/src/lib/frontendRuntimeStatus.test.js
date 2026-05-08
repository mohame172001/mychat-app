import { frontendRuntimeStatus, loadFrontendRuntimeStatus } from './frontendRuntimeStatus';
import { resetGoogleRuntimeConfigForTests } from './googleAuth';
import api from './api';

jest.mock('./api', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
  },
}));

describe('frontendRuntimeStatus', () => {
  afterEach(() => {
    delete process.env.REACT_APP_GOOGLE_CLIENT_ID;
    delete process.env.REACT_APP_BACKEND_URL;
    delete process.env.REACT_APP_SENTRY_DSN;
    delete process.env.REACT_APP_POSTHOG_KEY;
    api.get.mockReset();
    resetGoogleRuntimeConfigForTests();
  });

  test('reports Google Sign-In configured as a boolean without exposing client id', () => {
    process.env.REACT_APP_GOOGLE_CLIENT_ID = 'test-client-id.apps.googleusercontent.com';

    const status = frontendRuntimeStatus();

    expect(status.google_sign_in_configured).toBe(true);
    expect(JSON.stringify(status)).not.toContain('test-client-id.apps.googleusercontent.com');
  });

  test('reports Google Sign-In disabled when client id is missing', () => {
    delete process.env.REACT_APP_GOOGLE_CLIENT_ID;

    expect(frontendRuntimeStatus().google_sign_in_configured).toBe(false);
  });

  test('loads backend Google config before reporting runtime status', async () => {
    delete process.env.REACT_APP_GOOGLE_CLIENT_ID;
    api.get.mockResolvedValue({
      headers: { 'content-type': 'application/json' },
      data: JSON.stringify({
        enabled: true,
        client_id: 'runtime-client-id.apps.googleusercontent.com',
      }),
    });

    const status = await loadFrontendRuntimeStatus();

    expect(status.google_sign_in_configured).toBe(true);
    expect(status.google_config_request_attempted).toBe(true);
    expect(status.google_config_request_ok).toBe(true);
    expect(status.google_config_response_enabled).toBe(true);
    expect(status.google_config_response_was_json).toBe(true);
    expect(JSON.stringify(status)).not.toContain('runtime-client-id.apps.googleusercontent.com');
  });
});
