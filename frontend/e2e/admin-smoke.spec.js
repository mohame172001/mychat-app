const { test, expect } = require('@playwright/test');
const {
  setupMockApi,
  signInWithMockUser,
  installBrowserGuards,
} = require('./fixtures/mockApi');

test.describe('admin smoke with mocked API', () => {
  test.beforeEach(async ({ page }) => {
    await installBrowserGuards(page);
    await signInWithMockUser(page);
    await setupMockApi(page);
  });

  test('overview, users, user detail, admins, and metrics render without blank pages', async ({ page }) => {
    await page.goto('/app/admin');

    await expect(page.getByTestId('admin-console')).toBeVisible();
    await expect(page.getByTestId('admin-overview')).toBeVisible();
    await expect(page.getByText('Total users')).toBeVisible();

    await page.getByTestId('admin-tab-users').click();
    await expect(page.getByTestId('admin-users')).toBeVisible();
    await expect(page.getByText('demo@example.com')).toBeVisible();
    await page.getByRole('button', { name: /^view$/i }).click();

    await expect(page.getByTestId('admin-user-detail')).toBeVisible();
    await expect(page.getByText('demo@example.com')).toBeVisible();
    await expect(page.getByTestId('admin-detail-plan-controls')).toBeVisible();
    await expect(page.getByText('Custom allowances')).toBeVisible();
    await expect(page.getByRole('button', { name: /revoke/i })).toBeVisible();
    await expect(page.getByTestId('admin-suspend-btn')).toBeVisible();
    await expect(page.getByTestId('admin-soft-delete-btn')).toBeVisible();
    await expect(page.locator('body')).not.toContainText('Could not render this user section');

    await page.getByPlaceholder('Extra comments processed').fill('500');
    const grantButton = page.getByTestId('admin-grant-allowance-btn');
    await grantButton.click();
    await expect(grantButton).toBeDisabled();
    await expect(page.getByText(/granting/i)).toBeVisible();
    await expect(grantButton).toBeEnabled();

    await page.getByTestId('admin-tab-admins').click();
    await expect(page.getByText('owner@example.com')).toBeVisible();

    await page.getByTestId('admin-tab-metrics').click();
    await expect(page.getByText('Metrics reconciliation')).toBeVisible();
    await expect(page.getByText('public_replies_sent_month')).toBeVisible();
  });
});

