import assert from 'node:assert/strict';
import test from 'node:test';
import { buildCronHeaders, buildCronUrl, runCron } from './instagram-token-refresh-cron.mjs';

test('builds Authorization header from CRON_SECRET', () => {
  const headers = buildCronHeaders({ CRON_SECRET: 'expected-secret' });
  assert.equal(headers.Authorization, 'Bearer expected-secret');
});

test('uses backend url env and cron endpoint path', () => {
  const url = buildCronUrl({
    BACKEND_URL: 'https://backend.example.com/',
    CRON_SECRET: 'expected-secret',
  });
  assert.equal(url, 'https://backend.example.com/api/cron/refresh-instagram-tokens');
});

test('runCron logs only secret existence, length, and status', async () => {
  const lines = [];
  const seen = {};
  const status = await runCron({
    env: {
      BACKEND_URL: 'https://backend.example.com',
      CRON_SECRET: 'super-secret-value',
    },
    logger: { log: line => lines.push(line) },
    fetchImpl: async (url, options) => {
      seen.url = url;
      seen.authorization = options.headers.Authorization;
      return { status: 200 };
    },
  });

  assert.equal(status, 200);
  assert.equal(seen.url, 'https://backend.example.com/api/cron/refresh-instagram-tokens');
  assert.equal(seen.authorization, 'Bearer super-secret-value');
  assert.deepEqual(lines, [
    'CRON_SECRET exists: true',
    'CRON_SECRET length: 18',
    'Status: 200',
  ]);
  assert.equal(lines.join('\n').includes('super-secret-value'), false);
});
