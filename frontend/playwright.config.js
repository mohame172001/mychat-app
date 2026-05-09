const { defineConfig, devices } = require('@playwright/test');

const PORT = Number(process.env.E2E_PORT || 3100);
const LOCAL_BASE_URL = `http://127.0.0.1:${PORT}`;
const BASE_URL = process.env.E2E_BASE_URL || LOCAL_BASE_URL;
const useExternalApp = Boolean(process.env.E2E_BASE_URL);

module.exports = defineConfig({
  testDir: './e2e',
  timeout: 30_000,
  expect: { timeout: 8_000 },
  fullyParallel: false,
  retries: process.env.CI ? 1 : 0,
  reporter: [['list']],
  use: {
    baseURL: BASE_URL,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'off',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: useExternalApp ? undefined : {
    command: 'yarn start',
    url: LOCAL_BASE_URL,
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
    env: {
      ...process.env,
      BROWSER: 'none',
      HOST: '127.0.0.1',
      PORT: String(PORT),
      REACT_APP_BACKEND_URL: process.env.REACT_APP_BACKEND_URL || 'http://127.0.0.1:4010',
    },
  },
});

