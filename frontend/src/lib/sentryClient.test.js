import {
  redactSentryEvent,
  safeRouteFromUrl,
  sentryStatus,
  initSentry,
} from './sentryClient';

describe('redactSentryEvent — headers', () => {
  test('redacts Authorization / Cookie / signature / API key', () => {
    const event = {
      request: {
        url: 'https://api.example.com/x',
        headers: {
          Authorization: 'Bearer SECRET',
          Cookie: 'session=abc',
          'X-Hub-Signature-256': 'sha256=secret',
          'X-Api-Key': 'k123',
          'Set-Cookie': 'session=abc; HttpOnly',
          'X-Forwarded-For': '1.2.3.4',
        },
      },
    };
    const out = redactSentryEvent(event);
    expect(out.request.headers.Authorization).toBe('[redacted]');
    expect(out.request.headers.Cookie).toBe('[redacted]');
    expect(out.request.headers['X-Hub-Signature-256']).toBe('[redacted]');
    expect(out.request.headers['X-Api-Key']).toBe('[redacted]');
    expect(out.request.headers['Set-Cookie']).toBe('[redacted]');
    expect(out.request.headers['X-Forwarded-For']).toBe('1.2.3.4');
    expect(JSON.stringify(out)).not.toContain('SECRET');
  });

  test('redacts query string with OAuth code', () => {
    const out = redactSentryEvent({
      request: {
        url: 'https://api.example.com/instagram/callback',
        query_string: 'code=AQB-secret&state=xyz',
        headers: {},
      },
    });
    expect(out.request.query_string).toBe('[redacted]');
    expect(JSON.stringify(out)).not.toContain('AQB-secret');
  });
});

describe('redactSentryEvent — bodies', () => {
  test('drops webhook body entirely', () => {
    const out = redactSentryEvent({
      request: {
        url: 'https://api.example.com/api/instagram/webhook',
        data: { entry: [{ value: { text: 'private comment 12345' } }] },
        headers: {},
      },
    });
    expect(out.request.data).toBe('[redacted]');
    expect(JSON.stringify(out)).not.toContain('private comment 12345');
  });

  test('drops comment reply body entirely', () => {
    const out = redactSentryEvent({
      request: {
        url: 'https://api.example.com/api/comments/abc/reply',
        data: { text: 'private operator reply' },
        headers: {},
      },
    });
    expect(out.request.data).toBe('[redacted]');
    expect(JSON.stringify(out)).not.toContain('private operator reply');
  });

  test('redacts forbidden keys in extra/contexts', () => {
    const out = redactSentryEvent({
      extra: {
        access_token: 'EAA-secret',
        meta_access_token: 'EAA-secret',
        Authorization: 'Bearer X',
        comment_text: 'private',
        dm_text: 'private',
        reply_text: 'private',
        message_text: 'private',
        graph_error: '{"raw":"private"}',
        state: 'oauth-state',
        code: 'oauth-code',
        safe: 'keep_me',
      },
      contexts: {
        request: { graph_error: { message: 'private graph body' }, safe: 'ok' },
      },
    });
    expect(out.extra.access_token).toBe('[redacted]');
    expect(out.extra.meta_access_token).toBe('[redacted]');
    expect(out.extra.Authorization).toBe('[redacted]');
    expect(out.extra.comment_text).toBe('[redacted]');
    expect(out.extra.dm_text).toBe('[redacted]');
    expect(out.extra.reply_text).toBe('[redacted]');
    expect(out.extra.message_text).toBe('[redacted]');
    expect(out.extra.graph_error).toBe('[redacted]');
    expect(out.extra.state).toBe('[redacted]');
    expect(out.extra.code).toBe('[redacted]');
    expect(out.extra.safe).toBe('keep_me');
    expect(out.contexts.request.graph_error).toBe('[redacted]');
    expect(out.contexts.request.safe).toBe('ok');
    const serialized = JSON.stringify(out);
    expect(serialized).not.toContain('EAA-secret');
    expect(serialized).not.toContain('private');
    expect(serialized).not.toContain('oauth-state');
    expect(serialized).not.toContain('oauth-code');
  });
});

describe('redactSentryEvent — user / breadcrumbs', () => {
  test('strips email/IP from user, keeps id', () => {
    const out = redactSentryEvent({
      user: { id: 'u1', email: 'p@example.com', ip_address: '1.2.3.4' },
    });
    expect(out.user).toEqual({ id: 'u1' });
  });

  test('redacts breadcrumb messages and data with secrets', () => {
    const out = redactSentryEvent({
      breadcrumbs: {
        values: [
          { message: 'GET /api/x?access_token=SECRET' },
          { message: 'Authorization: Bearer SECRET' },
          { message: 'normal log' },
          { data: { access_token: 'SECRET', route: '/api/x' } },
        ],
      },
    });
    expect(out.breadcrumbs.values[0].message).toBe('[redacted]');
    expect(out.breadcrumbs.values[1].message).toBe('[redacted]');
    expect(out.breadcrumbs.values[2].message).toBe('normal log');
    expect(out.breadcrumbs.values[3].data.access_token).toBe('[redacted]');
    expect(out.breadcrumbs.values[3].data.route).toBe('/api/x');
  });

  test('safe on missing event', () => {
    expect(redactSentryEvent(null)).toBeNull();
    expect(redactSentryEvent({})).toEqual({});
  });
});

describe('safeRouteFromUrl', () => {
  test('strips OAuth params', () => {
    expect(safeRouteFromUrl('/app/settings?code=AQB-secret&tab=ig'))
      .toBe('/app/settings');
  });
  test('handles relative paths', () => {
    expect(safeRouteFromUrl('/app/billing')).toBe('/app/billing');
  });
});

describe('sentryStatus + initSentry — disabled by default', () => {
  beforeEach(() => {
    delete process.env.REACT_APP_SENTRY_DSN;
  });

  test('status reports not configured', () => {
    const s = sentryStatus();
    expect(s.sentry_configured).toBe(false);
    expect(s.sentry_initialized).toBe(false);
    expect(s.service).toBe('frontend');
    expect(JSON.stringify(s)).not.toContain('sentry.io');
  });

  test('initSentry() returns false without DSN', async () => {
    const ok = await initSentry();
    expect(ok).toBe(false);
  });
});
