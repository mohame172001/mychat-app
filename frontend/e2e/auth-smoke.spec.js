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
    await expect(page.getByText(/google sign-in is unavailable right now/i)).toBeVisible();
    await expect(page.getByRole('link', { name: /data deletion/i })).toBeVisible();
    await expect(page.getByLabel(/username/i)).toBeVisible();
    await expect(page.locator('#password')).toBeVisible();
    await expect(page.getByRole('link', { name: /forgot password/i })).toBeVisible();

    const passwordInput = page.locator('#password');
    await expect(passwordInput).toHaveAttribute('type', 'password');
    await expect(passwordInput).toHaveAttribute('autocomplete', 'current-password');
    await page.getByRole('button', { name: 'Show password' }).click();
    await expect(passwordInput).toHaveAttribute('type', 'text');
    await page.getByRole('button', { name: 'Hide password' }).click();
    await expect(passwordInput).toHaveAttribute('type', 'password');

    await page.getByLabel(/username/i).fill('missing-user');
    await page.locator('#password').fill('wrong-password');
    await page.getByRole('button', { name: /log in|sign in/i }).click();

    await expect(page.getByText(/invalid email or password/i)).toBeVisible();
    await expect(page.locator('body')).not.toContainText('account_suspended');
    await expect(page.locator('body')).not.toContainText('account_deleted');
  });

  test('signup page renders without hiding email/password signup', async ({ page }) => {
    await page.goto('/signup');

    await expect(page.getByRole('heading', { name: /create your mychat account/i })).toBeVisible();
    await expect(page.getByRole('button', { name: /continue with google/i })).toBeVisible();
    await expect(page.getByLabel(/username/i)).toBeVisible();
    await expect(page.getByLabel(/email/i)).toBeVisible();
    await expect(page.locator('#password')).toBeVisible();
    const passwordInput = page.locator('#password');
    await expect(passwordInput).toHaveAttribute('type', 'password');
    await expect(passwordInput).toHaveAttribute('autocomplete', 'new-password');
    await page.getByRole('button', { name: 'Show password' }).click();
    await expect(passwordInput).toHaveAttribute('type', 'text');
    await page.getByRole('button', { name: 'Hide password' }).click();
    await expect(passwordInput).toHaveAttribute('type', 'password');
    await expect(page.getByRole('button', { name: /create account/i })).toBeVisible();
  });

  test('forgot and reset password pages render usable recovery forms', async ({ page }) => {
    await page.goto('/forgot-password');

    await expect(page.getByRole('heading', { name: /reset your password/i })).toBeVisible();
    await expect(page.getByLabel('Email')).toBeVisible();
    await expect(page.getByRole('button', { name: /send reset link/i })).toBeVisible();

    await page.goto('/reset-password?token=test-token');
    await expect(page.getByRole('heading', { name: /set a new password/i })).toBeVisible();
    const newPassword = page.locator('#new-password');
    const confirmPassword = page.locator('#confirm-password');
    await expect(newPassword).toHaveAttribute('type', 'password');
    await expect(confirmPassword).toHaveAttribute('type', 'password');
    await expect(newPassword).toHaveAttribute('autocomplete', 'new-password');
    await expect(confirmPassword).toHaveAttribute('autocomplete', 'new-password');
    await expect(page.getByRole('button', { name: 'Show password' })).toHaveCount(2);
    await newPassword.fill('new-secret');
    await page.getByRole('button', { name: 'Show password' }).first().click();
    await expect(newPassword).toHaveAttribute('type', 'text');
    await page.getByRole('button', { name: 'Hide password' }).click();
    await expect(newPassword).toHaveAttribute('type', 'password');
    await expect(page.getByRole('button', { name: /reset password/i })).toBeVisible();
  });

  test('public data deletion page renders and submits safe request', async ({ page }) => {
    await page.goto('/data-deletion');

    await expect(page.getByRole('heading', { name: 'Data Deletion', exact: true })).toBeVisible();
    await expect(page.getByText(/Data Deletion Request/i)).toBeVisible();
    await expect(page.getByText(/\/api\/meta\/data-deletion/i)).toBeVisible();
    await page.getByLabel(/account email/i).fill('reviewer@example.com');
    await page.getByRole('button', { name: /submit request/i }).click();
    await expect(page.getByText(/mychat-del-e2e/i)).toBeVisible();
    await expect(page.locator('body')).not.toContainText('access_token');
    await expect(page.locator('body')).not.toContainText('client_secret');
  });
});
