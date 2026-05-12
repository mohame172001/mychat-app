# Production Domain Inventory

Canonical production URLs:

- Frontend: `https://frontend-production-6eb2.up.railway.app`
- Backend API: `https://backend-production-a1a3.up.railway.app`
- Privacy: `https://frontend-production-6eb2.up.railway.app/privacy`
- Terms: `https://frontend-production-6eb2.up.railway.app/terms`
- Data deletion instructions: `https://frontend-production-6eb2.up.railway.app/data-deletion`
- Meta data deletion callback: `https://backend-production-a1a3.up.railway.app/api/meta/data-deletion`
- Instagram webhook callback: `https://backend-production-a1a3.up.railway.app/api/instagram/webhook`

## Railway Services

- Frontend service root: `frontend`
- Frontend build command: `yarn build`
- Frontend start command: `./node_modules/.bin/serve -s build -l $PORT`
- Backend service root: `backend`
- Backend health route: `/api/`

## CORS

Production CORS must allow only the canonical frontend domain and any explicitly approved custom domain. It must not use `*` with credentials.

Expected allowed origins:

- `https://frontend-production-6eb2.up.railway.app`
- future custom app domain after launch, if configured

Domains to review/remove later:

- old Railway preview domains
- stale frontend services used during previous deploy mismatch incidents
- localhost origins from production env

## Meta Dashboard

Use the canonical frontend/backend URLs above:

- App settings -> Basic -> App Domains: frontend domain and custom domain when added.
- Facebook Login for Business / Instagram Login -> Valid OAuth Redirect URIs: backend OAuth callback URI documented in the Meta dashboard values doc.
- Webhooks -> Callback URL: backend webhook callback URL above.
- Webhooks -> Verify Token: field name only; never paste into docs.

Expected webhook fields:

- `comments`
- `messages`

Keep unused fields disabled unless product support is explicitly implemented:

- `live_comments`
- `mentions`
- `message_edit`
- `message_reactions`
- `messaging_handover`
- `messaging_postbacks`
- `messaging_referral`
- `messaging_seen`
- `standby`

## Google OAuth

Allowed JavaScript origin should include the canonical frontend domain and future custom domain only. Do not add backend secrets or client secrets to frontend env.

## Deployment Verification

Use `docs/deployment-verification.md` after every deploy. Always fetch `asset-manifest.json` and search all JS chunks, not only `main.js`.
