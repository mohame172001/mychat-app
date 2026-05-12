# Deployment Verification

Use this after every backend or frontend Railway deploy. Prefer direct HTTP requests with cache-busting over browser refreshes, because stale frontend bundles have happened before.

## Backend

1. Health:
   ```bash
   curl -i https://backend-production-a1a3.up.railway.app/api/
   ```
   Expected: `200` and `{"app":"mychat","status":"ok"}`.

2. Protected route checks:
   ```bash
   curl -i https://backend-production-a1a3.up.railway.app/api/admin/users
   curl -i https://backend-production-a1a3.up.railway.app/api/dashboard/summary
   ```
   Expected unauthenticated result: `403`.

3. Webhook verify-token negative check:
   ```bash
   curl -i "https://backend-production-a1a3.up.railway.app/api/instagram/webhook?hub.mode=subscribe&hub.verify_token=bad&hub.challenge=test"
   ```
   Expected: `403`.

## Frontend Bundle

1. Fetch the live manifest with a cache buster:
   ```bash
   curl "https://frontend-production-6eb2.up.railway.app/asset-manifest.json?cachebust=$(date +%s)"
   ```

2. Search every JavaScript file listed in `asset-manifest.json`, not only `main.js`.

3. Expected marker examples by phase:
   - Google: `auth/google/config`, `Continue with Google`
   - Dashboard: `dashboard/summary`
   - Automations: `automations/summary`
   - Security: `instagram_account_already_connected`, `account_suspended`, `retry-reply`

4. If local build contains a marker but live chunks do not, treat it as a deployment/serving mismatch.

## Railway Service Settings

- Frontend service:
  - Branch: `master`
  - Root directory: `frontend`
  - Build command: `yarn build`
  - Start command: `./node_modules/.bin/serve -s build -l $PORT`
- Backend service:
  - Branch: `master`
  - Root directory: backend/project root as configured for `backend/server.py`
  - Health route: `/api/`

## Wrong Service or Stale Domain Symptoms

- `asset-manifest.json` continues to reference old JS hashes after a successful deploy.
- Live JS lacks strings present in local `frontend/build/static/js`.
- The public URL points to a different Railway service than the one being deployed.
- Redeploying an old deployment instead of deploying the latest commit.

## Rollback

1. Roll back only the affected service.
2. Confirm backend health or frontend manifest immediately after rollback.
3. Confirm protected routes still return `403`.
4. Record the deployed commit SHA and public URL in the incident notes.
