import { fileURLToPath } from 'node:url';

const DEFAULT_BACKEND_URL = 'https://backend-production-a1a3.up.railway.app';

export function buildCronHeaders(env = process.env) {
  const secret = env.CRON_SECRET || '';
  return {
    Authorization: `Bearer ${secret}`,
  };
}

export function buildCronUrl(env = process.env) {
  const baseUrl = (
    env.BACKEND_URL ||
    env.BACKEND_PUBLIC_URL ||
    DEFAULT_BACKEND_URL
  ).replace(/\/+$/, '');
  return `${baseUrl}/api/cron/refresh-instagram-tokens`;
}

export async function runCron({ env = process.env, fetchImpl = globalThis.fetch, logger = console } = {}) {
  const secret = env.CRON_SECRET || '';
  logger.log(`CRON_SECRET exists: ${Boolean(secret)}`);
  logger.log(`CRON_SECRET length: ${secret.length}`);

  if (!fetchImpl) {
    throw new Error('fetch is not available');
  }

  const response = await fetchImpl(buildCronUrl(env), {
    method: 'POST',
    headers: buildCronHeaders(env),
  });
  logger.log(`Status: ${response.status}`);
  return response.status;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  runCron()
    .then(status => {
      process.exit(status >= 200 && status < 300 ? 0 : 1);
    })
    .catch(error => {
      console.error(`Cron request failed: ${error?.message || 'unknown error'}`);
      process.exit(1);
    });
}
