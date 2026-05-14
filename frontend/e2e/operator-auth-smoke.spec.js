const { test, expect } = require('@playwright/test');

function measureRouteTimings(page, route, routes) {
  const timings = [];
  for (const routeName of routes) {
    const startTime = Date.now();
    const reqCountStart = page.requestCount ? page.requestCount() : 0;
    
    page.on('response', (response) => {
      timings.push({
        route: routeName,
        url: response.url(),
        status: response.status(),
        duration: Date.now() - startTime,
      });
    });
  }
  return timings;
}

test.describe('operator authenticated smoke', () => {
  test('logs in with real operator credentials when env vars are supplied @operator', async ({ page }) => {
    const username = process.env.E2E_USERNAME || process.env.E2E_EMAIL;
    const password = process.env.E2E_PASSWORD;
    const baseUrl = process.env.E2E_BASE_URL;
    
    test.skip(!username || !password || !baseUrl,
      'Set E2E_BASE_URL plus E2E_USERNAME or E2E_EMAIL and E2E_PASSWORD to run this production smoke.');

    const routeTimings = [];
    const apiCalls = [];

    page.on('response', async (response) => {
      const url = response.url();
      if (url.includes('/api/')) {
        const startTime = parseInt(response.headers()['x-request-start'] || Date.now());
        apiCalls.push({
          url: url.split('/api/')[1] || url,
          status: response.status(),
          duration: Date.now() - startTime,
        });
      }
    });

    await page.goto('/login');
    const loginStart = Date.now();
    await page.getByLabel(/username/i).fill(username);
    await page.getByLabel(/password/i).fill(password);
    await page.getByRole('button', { name: /sign in/i }).click();
    await expect(page.getByTestId('dashboard-page')).toBeVisible();
    const loginDuration = Date.now() - loginStart;
    console.log(`Login duration: ${loginDuration}ms`);

    const routes = [
      { name: 'dashboard', path: '/app', check: () => expect(page.getByTestId('dashboard-page')).toBeVisible() },
      { name: 'automations', path: '/app/automations', check: () => expect(page.getByRole('heading', { name: /^automations$/i })).toBeVisible() },
      { name: 'settings', path: '/app/settings', check: () => expect(page.getByRole('heading', { name: /settings/i })).toBeVisible() },
      { name: 'billing', path: '/app/billing', check: () => expect(page.getByTestId('billing-page')).toBeVisible() },
    ];

    for (const route of routes) {
      const start = Date.now();
      await page.goto(route.path);
      await route.check();
      const duration = Date.now() - start;
      routeTimings.push({ name: route.name, duration });
      console.log(`${route.name}: ${duration}ms`);
    }

    if (process.env.ADMIN_EMAIL || process.env.ADMIN_USERNAME) {
      const adminUser = process.env.ADMIN_EMAIL || process.env.ADMIN_USERNAME;
      const adminPass = process.env.ADMIN_PASSWORD;
      if (adminUser && adminPass) {
        await page.goto('/logout');
        await page.goto('/login');
        await page.getByLabel(/username/i).fill(adminUser);
        await page.getByLabel(/password/i).fill(adminPass);
        await page.getByRole('button', { name: /sign in/i }).click();
        await page.waitForTimeout(1000);

        const adminRoutes = [
          { name: 'admin-overview', path: '/app/admin', check: () => expect(page.getByRole('heading', { name: /admin/i })).toBeVisible() },
        ];

        for (const route of adminRoutes) {
          const start = Date.now();
          await page.goto(route.path);
          await route.check();
          const duration = Date.now() - start;
          routeTimings.push({ name: route.name, duration });
          console.log(`${route.name}: ${duration}ms`);
        }
      }
    }

    const slowRoutes = routeTimings.filter(t => t.duration > 5000);
    if (slowRoutes.length > 0) {
      console.log('SLOW ROUTES (>5s):', slowRoutes);
    }

    console.log('All route timings:', routeTimings);
    console.log('API calls captured:', apiCalls.length);
  });
});