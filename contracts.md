# mychat — API Contracts & Integration Plan

## Auth
- `POST /api/auth/signup` { username, email, password } → { token, user }
- `POST /api/auth/login`  { username, password } → { token, user }
- `GET  /api/auth/me` (Bearer) → { user }

JWT token stored client-side in localStorage under `mychat_token`. User in `mychat_user`.

## Automations  (`Bearer`)
- `GET    /api/automations`
- `POST   /api/automations` { name, trigger, status? }
- `GET    /api/automations/:id`
- `PATCH  /api/automations/:id` { name?, status?, trigger?, nodes?, edges? }
- `DELETE /api/automations/:id`
- `POST   /api/automations/:id/duplicate`

## Contacts (`Bearer`)
- `GET    /api/contacts?search=&tag=`
- `POST   /api/contacts` { name, username, tags?, subscribed? }
- `PATCH  /api/contacts/:id`
- `DELETE /api/contacts/:id`

## Broadcasts (`Bearer`)
- `GET  /api/broadcasts`
- `POST /api/broadcasts` { name, message, audience_size? }
- `PATCH /api/broadcasts/:id` { status? }

## Conversations + Messages (`Bearer`)
- `GET  /api/conversations`
- `GET  /api/conversations/:id`
- `POST /api/conversations/:id/messages` { text } → appends message, simulates contact reply async

## Dashboard (`Bearer`)
- `GET /api/dashboard/stats` → { total_contacts, active_automations, messages_sent, conversion_rate, weekly_chart: [...] }

## Instagram OAuth (Bearer)
- `GET  /api/instagram/auth-url` → { url }  (redirects to Facebook login)
- `GET  /api/instagram/callback?code=` → exchanges code for long-lived token, saves to user
- `POST /api/instagram/disconnect`
- `GET  /api/instagram/webhook` → verification
- `POST /api/instagram/webhook` → receives comment/dm events (mocked processor for MVP)

## Data Mocked in mock.js to replace
- automations, contacts, conversations, broadcasts, dashboardStats, chartData → all replaced with API responses.
- features/testimonials/pricing/stats on landing → stay as static marketing content.

## Frontend Integration
- Add `src/lib/api.js` with axios instance using `REACT_APP_BACKEND_URL/api` and auto-attach JWT.
- Update `AuthContext` to call real /auth/* endpoints.
- Each page replaces mock imports with hooks calling the respective endpoint (useEffect + state).
- Persist user's data per-account (scoped by user_id on server).

## Seed
On first signup, server seeds that user with: 4 contacts, 3 automations, 2 broadcasts, 2 conversations — so dashboard feels alive.
