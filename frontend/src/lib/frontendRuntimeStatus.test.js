import { frontendRuntimeStatus } from './frontendRuntimeStatus';

describe('frontendRuntimeStatus', () => {
  afterEach(() => {
    delete process.env.REACT_APP_GOOGLE_CLIENT_ID;
    delete process.env.REACT_APP_SENTRY_DSN;
    delete process.env.REACT_APP_POSTHOG_KEY;
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
});
