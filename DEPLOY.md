# Deploy mychat to Railway from scratch

This guide assumes you have a Railway account and the repo at
`github.com/mohame172001/mychat-app` on `master`. Following it end to
end takes ~10 minutes.

> If you're recovering from a deleted project, you don't need to
> change any code — every config file (`railway.json`,
> `nixpacks.toml`, `Procfile`, `Dockerfile`) is already in the repo
> and tuned for Railway. You only need to wire up env vars.

## 1) Create the project

1. Open [railway.com](https://railway.com) → **New Project**
2. **Deploy from GitHub repo** → pick `mohame172001/mychat-app`
3. Railway will detect two services automatically. If it only creates
   one, add the second manually:
   - **backend**:  root directory = `/backend`, Nixpacks builder
   - **frontend**: root directory = `/frontend`, Nixpacks builder

## 2) Add MongoDB

In the same project: **+ New** → **Database** → **Add MongoDB**.

Railway creates the service with a `MONGO_URL` reference variable
you can plug into the backend service.

## 3) Generate secrets

Run these locally (or use any random-secret generator):

```bash
openssl rand -hex 32   # JWT_SECRET
openssl rand -hex 32   # CRON_SECRET
openssl rand -hex 32   # META_VERIFY_TOKEN (any random string also fine)
```

## 4) Backend service variables

In the backend service → **Variables** → add:

| Variable | Source / value |
|---|---|
| `MONGO_URL` | Reference → MongoDB service → `MONGO_URL` |
| `DB_NAME` | `mychat_db` |
| `JWT_SECRET` | the openssl 32-byte hex you just generated |
| `CRON_SECRET` | another openssl 32-byte hex |
| `META_APP_ID` | from [Meta App Dashboard](https://developers.facebook.com/apps) |
| `META_APP_SECRET` | from Meta App Dashboard → App Settings → Basic |
| `META_VERIFY_TOKEN` | random string you chose (also pasted into Meta webhook config) |
| `META_WEBHOOK_APP_SECRET` | same as `META_APP_SECRET` unless you're using Meta's "App Webhook" feature |
| `META_WEBHOOK_HMAC_ENFORCE` | `1` |
| `FRONTEND_URL` | will be set in step 6 (e.g. `https://frontend-production-xxxx.up.railway.app`) |
| `BACKEND_PUBLIC_URL` | will be set in step 6 (this service's public URL) |
| `GOOGLE_CLIENT_ID` | optional — leave blank to disable Google Sign-In |
| `APP_ENV` | `production` |
| `RAILWAY_GIT_COMMIT_SHA` | reference → leave Railway to auto-inject |
| `EMAIL_VERIFICATION_WEBHOOK_URL` | optional — webhook to your transactional-email provider |
| `EMAIL_VERIFICATION_WEBHOOK_TOKEN` | optional |
| `ADMIN_EMAILS` | comma-separated emails granted admin access |
| `CORS_ALLOWED_ORIGINS` | the frontend URL from step 6 |
| `SENTRY_DSN` | optional — paste your Sentry project DSN |

## 5) Frontend service variables

In the frontend service → **Variables** → add:

| Variable | Source / value |
|---|---|
| `REACT_APP_BACKEND_URL` | the backend service's public URL (no trailing slash). Get it from step 6. |
| `REACT_APP_GOOGLE_CLIENT_ID` | same as backend `GOOGLE_CLIENT_ID` (or blank) |

## 6) Provision public domains

On both services → **Settings** → **Networking** → **Generate Domain**.

Once you have both URLs:

1. Paste backend URL into `REACT_APP_BACKEND_URL` (frontend service)
2. Paste backend URL into `BACKEND_PUBLIC_URL` (backend service)
3. Paste frontend URL into `FRONTEND_URL` (backend service)
4. Paste frontend URL into `CORS_ALLOWED_ORIGINS` (backend service)
5. Redeploy both services so they pick up the new vars

## 7) Update Meta App webhook

In [Meta App Dashboard](https://developers.facebook.com/apps) →
your app → **Webhooks** → **Instagram**:

- Callback URL: `https://<backend-domain>/api/instagram-webhook`
- Verify Token: same value as `META_VERIFY_TOKEN`
- Subscribe to: `comments`, `messages`, `messaging_postbacks`

## 8) Verify

Healthcheck (no auth needed):

```bash
curl https://<backend-domain>/api/health
# {"ok":true,"service":"mychat-backend"}

curl https://<backend-domain>/api/version
# {"ok":true,"service":"mychat-backend","environment":"production","build_sha":"..."}
```

Open the frontend URL in a browser — you should see the landing
page in either English or Arabic depending on browser language.

## 9) (Optional) Cron service

If you want hourly Instagram-token refresh, add a third service:

- Source: `cron/` directory
- Schedule: `0 * * * *`
- Env: `CRON_SECRET`, `BACKEND_PUBLIC_URL`

## Common gotchas

| Symptom | Fix |
|---|---|
| Backend 503 immediately after deploy | Look at logs — almost always a missing env var (most common: `JWT_SECRET`, `META_APP_SECRET`). |
| Backend boots but `/api/health` healthcheck fails | Check `healthcheckPath` in `backend/railway.json` is `/api/health` (it is, in current master). |
| Frontend 404 on every route | The frontend uses React Router. `frontend/Procfile` runs `serve -s build` (with `-s` for single-page fallback) — don't change to `-l` alone. |
| "instagram_account_already_connected" 500 | Fixed in commit `2025c10`. Make sure you're on master. |
| CORS errors in browser console | `CORS_ALLOWED_ORIGINS` env var on backend must contain the **frontend** URL exactly (no trailing slash). |
