# Frontend Bundle Analysis

Phase 2.13 reviewed the production React bundle shape after `yarn build`.

## Current Build Shape

Observed build output after the Phase 2.13 local build:

| Asset | Approx raw size | Notes |
|---|---:|---|
| `main.*.js` | 462 KB | App shell, shared libraries, route loader, common UI |
| `754.*.chunk.js` | 50 KB | Lazy route chunk |
| `644.*.chunk.js` | 43 KB | Lazy route chunk |
| `614.*.chunk.js` | 24 KB | Lazy route chunk |
| `724.*.chunk.js` | 19 KB | Lazy route chunk |
| Remaining route chunks | 6-17 KB each | Lazy-loaded page chunks |
| CSS | ~14 KB gzip in build report | Shared styling |

The CRA build report still shows the main bundle around 145 KB gzip, with route chunks split out. Source maps are generated locally by the build, but production source-map exposure remains governed by the deployment/source-map policy in `docs/production-security-checklist.md`.

## Findings

- Route-level lazy loading is already enabled in `frontend/src/App.js` for public, app, admin, and legal pages.
- Admin console is lazy-loaded and no longer blocks the public/auth shell.
- Optional Sentry/PostHog modules are configured to fail safely when unavailable; build warnings for those optional packages are expected in the current environment.
- Main-bundle risk is shared app shell and UI dependencies, not admin-only page code.
- The Suspense fallback was changed from a viewport-centered full-page loader to a lightweight inline route fallback so navigation does not feel like a full app freeze.

## Follow-Up Candidates

- If authenticated route timings remain high in production, split Admin User Detail into section-level backend endpoints and lazy-load heavier diagnostics sections.
- If charts or rich tables are added later, import them only inside their route/section.
- Continue verifying live deployments through `asset-manifest.json` and all chunks, not only `main.js`.

