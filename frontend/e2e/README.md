# MyChat E2E Smoke Tests

These Playwright tests exercise the critical product shell with mocked API
responses, so they do not require real Instagram, Google, admin, or billing
credentials.

Run locally:

```bash
yarn test:e2e
```

Run headed:

```bash
yarn test:e2e:headed
```

Optional operator production smoke:

```bash
E2E_BASE_URL=https://frontend-production-6eb2.up.railway.app \
E2E_USERNAME=<operator-login> \
E2E_PASSWORD=<operator-password> \
yarn test:e2e --grep @operator
```

The production smoke is skipped unless all env vars are present. Never commit
operator credentials.

