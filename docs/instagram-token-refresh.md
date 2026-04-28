# Instagram Token Refresh

This app stores Instagram access tokens in MongoDB. Do not store refreshed
tokens in Railway variables or `.env` files.

## Required Environment Variable

Set this on the Railway backend service:

```text
CRON_SECRET=<long random secret>
```

Keep the value private. The cron endpoint accepts it via either:

```text
Authorization: Bearer <CRON_SECRET>
```

or:

```text
X-Cron-Secret: <CRON_SECRET>
```

## Endpoint

Run token refresh with:

```bash
curl -X POST \
  https://backend-production-a1a3.up.railway.app/api/cron/refresh-instagram-tokens \
  -H "Authorization: Bearer $CRON_SECRET"
```

The response is a JSON summary with counts for checked, refreshed, skipped,
failed, critical, and expired accounts. It never returns access tokens.

## Railway Setup

Use two Railway services:

1. Backend service
   - Keep it as the normal always-on FastAPI web service.
   - Set a stable secret:

```text
CRON_SECRET=<stable long random secret>
```

2. `instagram-token-refresh-cron` service
   - Create it as a separate Railway service from the repo `cron/` directory.
   - Start command: `npm start`
   - Set variables:

```text
CRON_SECRET=${{ backend.CRON_SECRET }}
BACKEND_URL=https://backend-production-a1a3.up.railway.app
```

The cron service must not have its own unrelated `CRON_SECRET` value. It should
reference the backend service variable so both services use the same secret.

Configure the cron schedule to call/run the cron service once per day. Daily is
enough because long-lived Instagram tokens are refreshed only when they are
within the refresh window.

Suggested schedule:

```text
0 3 * * *
```

The cron script logs only:

```text
CRON_SECRET exists: true/false
CRON_SECRET length: <number>
Status: <number>
```

It never prints the secret value.

## Verifying Railway Logs

Backend logs should show:

```text
POST /api/cron/refresh-instagram-tokens 200
```

Cron service logs should show:

```text
CRON_SECRET exists: true
CRON_SECRET length: <non-zero number>
Status: 200
```

Meaning of common statuses:

- `403`: the endpoint path is correct, but the cron service did not send the
  same secret as the backend `CRON_SECRET`. Check that the cron service has
  `CRON_SECRET=${{ backend.CRON_SECRET }}` and redeploy it.
- `404`: the cron service is calling the wrong backend URL or path.
- `200`: the backend accepted the cron request and ran the refresh job. The
  JSON summary contains checked/refreshed/skipped/failed counts and never
  returns access tokens.

## How Refresh Works

For each connected Instagram account stored in `instagram_accounts`, the app:

1. Skips tokens that expire more than 15 days from now.
2. Skips tokens created/refreshed less than 24 hours ago.
3. Acquires a 5 minute database lock with `refreshLockedUntil`.
4. Calls Instagram:

```text
GET https://graph.instagram.com/refresh_access_token
grant_type=ig_refresh_token
access_token=<LONG_LIVED_ACCESS_TOKEN>
```

5. On success, saves the new token and expiry in MongoDB.
6. On failure, keeps the old token active and records a sanitized error.

The cron job is separate from webhook/comment processing. Refresh must not run
inside webhook requests because Graph API latency or failure must not block
comment replies.

## Status Endpoint

Authenticated users can inspect safe refresh status:

```bash
curl \
  https://backend-production-a1a3.up.railway.app/api/instagram/token-refresh/status \
  -H "Authorization: Bearer <JWT>"
```

Fields include account id, Instagram account id, expiry, refresh status,
attempt count, and critical/expired flags. It does not return `accessToken`.

## If A Token Expires

If a token is already expired, the app marks it as expired and requires manual
Instagram reconnect/OAuth from Settings. Refresh cannot guarantee recovery for
tokens that Meta no longer accepts.

## Local Test

```bash
cd backend
set MONGO_URL=mongodb://localhost:27017/test
set JWT_SECRET=test
set BACKEND_PUBLIC_URL=https://example.com
set FRONTEND_URL=https://example.com
set IG_APP_ID=123
set IG_APP_SECRET=secret
set CRON_SECRET=cron-test
python -m pytest tests/test_instagram_token_refresh.py -q
```

PowerShell from repo root:

```powershell
$env:MONGO_URL='mongodb://localhost:27017/test'
$env:JWT_SECRET='test'
$env:BACKEND_PUBLIC_URL='https://example.com'
$env:FRONTEND_URL='https://example.com'
$env:IG_APP_ID='123'
$env:IG_APP_SECRET='secret'
$env:CRON_SECRET='cron-test'
python -m pytest backend\tests\test_instagram_token_refresh.py -q
```

Cron script tests:

```powershell
cd cron
node --test
```

## Remaining Failure Cases

The code cannot prevent all external failures:

- Meta may revoke a token.
- The Instagram account may remove app permissions.
- App review/access level may change.
- The account may change from professional to unsupported account type.
- Railway cron may not run if the service or cron configuration is disabled.
