const { test, expect } = require('@playwright/test');

test.describe('operator authenticated smoke', () => {
  test('logs in with real operator credentials when env vars are supplied @operator', async ({ page }) => {
    const username = process.env.E2E_USERNAME || process.env.E2E_EMAIL;
    const password = process.env.E2E_PASSWORD;
    test.skip(!username || !password || !process.env.E2E_BASE_URL,
      'Set E2E_BASE_URL plus E2E_USERNAME or E2E_EMAIL and E2E_PASSWORD to run this production smoke.');

    await page.goto('/login');
    await page.getByLabel(/username/i).fill(username);
    await page.getByLabel(/password/i).fill(password);
    await page.getByRole('button', { name: /sign in/i }).click();

    await expect(page.getByTestId('dashboard-page')).toBeVisible();
    await page.getByRole('link', { name: /automations/i }).click();
    await expect(page.getByRole('heading', { name: /^automations$/i })).toBeVisible();
    await page.getByRole('link', { name: /comments/i }).click();
    await expect(page.getByRole('heading', { name: /^comments$/i })).toBeVisible();
    await page.getByRole('link', { name: /billing/i }).click();
    await expect(page.getByTestId('billing-page')).toBeVisible();
  });
});

