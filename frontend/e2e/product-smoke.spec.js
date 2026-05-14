const { test, expect } = require('@playwright/test');
const {
  setupMockApi,
  signInWithMockUser,
  installBrowserGuards,
} = require('./fixtures/mockApi');

async function expectAppIsUsable(page) {
  await expect(page.locator('body')).not.toHaveText(/^\s*$/);
  await expect(page.getByTestId('build-marker')).toBeVisible();
  await expect(page.locator('body')).not.toContainText('Could not render this user section');
}

test.describe('authenticated product smoke with mocked API', () => {
  test.beforeEach(async ({ page }) => {
    await installBrowserGuards(page);
    await signInWithMockUser(page);
    await setupMockApi(page);
  });

  test('dashboard renders and route navigation does not full reload', async ({ page }) => {
    await page.goto('/app');
    await expect(page.getByTestId('dashboard-page')).toBeVisible();
    await expect(page.getByText('Total Contacts')).toBeVisible();
    await expect(page.getByText('Messages Sent', { exact: true })).toBeVisible();
    await page.getByTestId('dashboard-refresh').click();
    await expectAppIsUsable(page);

    const navRoutes = [
      { name: 'Automations', heading: /automations/i },
      { name: 'Billing', heading: /billing/i },
      { name: 'Settings', heading: /settings/i },
      { name: 'Dashboard', heading: /good morning/i },
    ];

    for (const route of navRoutes) {
      await page.getByRole('link', { name: new RegExp(route.name, 'i') }).click();
      await expect(page.getByRole('heading', { name: route.heading })).toBeVisible();
      await expectAppIsUsable(page);
    }

    const navigationEntryCount = await page.evaluate(() => performance.getEntriesByType('navigation').length);
    const locationAssignCalls = await page.evaluate(() => window.__locationAssignCalls || []);
    expect(navigationEntryCount).toBe(1);
    expect(locationAssignCalls).toEqual([]);
  });

  test('automations list renders controls and lazy edit builder', async ({ page }) => {
    await page.goto('/app/automations');

    await expect(page.getByRole('heading', { name: /^automations$/i })).toBeVisible();
    await expect(page.getByTestId('automations-refresh')).toBeVisible();
    await expect(page.getByRole('button', { name: /create automation/i })).toBeVisible();
    await expect(page.getByText('Specific post rule')).toBeVisible();

    await page.getByRole('button', { name: /edit/i }).first().click();
    await expect(page.getByRole('heading', { name: /edit automation/i })).toBeVisible();
    await expect(page.getByRole('button', { name: /save changes/i })).toBeVisible();
    await expectAppIsUsable(page);
  });

  test('billing remains placeholder-only without checkout providers', async ({ page }) => {
    await page.goto('/app/billing');

    await expect(page.getByTestId('billing-page')).toBeVisible();
    await expect(page.getByTestId('billing-disabled-banner')).toBeVisible();
    await expect(page.getByTestId('plan-card-free')).toBeVisible();
    await expect(page.getByTestId('plan-card-pro')).toBeVisible();
    await expect(page.getByTestId('upgrade-btn-pro')).toBeDisabled();
    await expect(page.locator('body')).not.toContainText(/stripe|paddle|paymob/i);
  });

  test('settings password fields have accessible show-hide controls', async ({ page }) => {
    await page.goto('/app/settings?tab=security');

    await expect(page.getByRole('heading', { name: /settings/i })).toBeVisible();
    await expect(page.locator('#current-password')).toHaveAttribute('autocomplete', 'current-password');
    await expect(page.locator('#new-password')).toHaveAttribute('autocomplete', 'new-password');
    await expect(page.locator('#confirm-password')).toHaveAttribute('autocomplete', 'new-password');
    await expect(page.getByRole('button', { name: 'Show password' })).toHaveCount(3);
    await page.locator('#current-password').fill('old-secret');
    await page.getByRole('button', { name: 'Show password' }).first().click();
    await expect(page.locator('#current-password')).toHaveAttribute('type', 'text');
    await page.getByRole('button', { name: 'Hide password' }).click();
    await expect(page.locator('#current-password')).toHaveAttribute('type', 'password');
  });
});
