const { test, expect } = require('@playwright/test');
const { setupMockApi, installBrowserGuards } = require('./fixtures/mockApi');

test.describe('unauthenticated auth smoke', () => {
  test.beforeEach(async ({ page }) => {
    await installBrowserGuards(page);
    await setupMockApi(page, { googleEnabled: false });
  });

  test('login page renders core form and safe invalid-login error', async ({ page }) => {
    await page.goto('/login');

    await expect(page.getByRole('heading', { name: /welcome back/i })).toBeVisible();
    await expect(page.getByRole('button', { name: /continue with google/i })).toBeVisible();
    await expect(page.getByText(/google sign-in is not configured/i)).toBeVisible();
    await expect(page.getByLabel(/username/i)).toBeVisible();
    await expect(page.getByLabel(/password/i)).toBeVisible();

    await page.getByLabel(/username/i).fill('missing-user');
    await page.getByLabel(/password/i).fill('wrong-password');
    await page.getByRole('button', { name: /sign in/i }).click();

    await expect(page.getByText(/invalid email or password/i)).toBeVisible();
    await expect(page.locator('body')).not.toContainText('account_suspended');
    await expect(page.locator('body')).not.toContainText('account_deleted');
  });

  test('signup page renders without hiding email/password signup', async ({ page }) => {
    await page.goto('/signup');

    await expect(page.getByRole('heading', { name: /create your account/i })).toBeVisible();
    await expect(page.getByRole('button', { name: /continue with google/i })).toBeVisible();
    await expect(page.getByLabel(/username/i)).toBeVisible();
    await expect(page.getByLabel(/email/i)).toBeVisible();
    await expect(page.getByLabel(/password/i)).toBeVisible();
    await expect(page.getByRole('button', { name: /create account/i })).toBeVisible();
  });
});

