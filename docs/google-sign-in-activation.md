# Google sign-in on MyChaat

## Production configuration

- Google Cloud project: `MyChaat` (`cedar-hawk-509719-t9`).
- OAuth web client: `MyChaat production web`.
- Authorized JavaScript origin: `https://mychaat.net`.
- Audience: external, in production.
- Application homepage: `https://mychaat.net`.
- Privacy policy: `https://mychaat.net/privacy`.
- Terms: `https://mychaat.net/terms`.
- Google Cloud billing and the free trial were not enabled.

Set `GOOGLE_CLIENT_ID` in Replit **production app secrets** and republish.
Use the client ID from this project's Clients page, not the legacy Railway
client. The client ID is public configuration, not a secret. Do not store a
Google client secret: the current Google Identity Services ID-token flow does
not use it.

Leave `REACT_APP_GOOGLE_CLIENT_ID` unset in production. The frontend reads
`GET /api/auth/google/config`, so its client ID matches the backend verifier.
Development preview origins are not authorized by the production client.

## Authentication path

1. Login and signup load the official Google Identity Services button.
2. Google returns an ID token through a JavaScript popup callback, not a server
   redirect. No Google redirect URI is needed for this implementation.
3. The frontend posts the token to `POST /api/auth/google`.
4. The backend checks Google's signature, audience, issuer, expiration, and
   verified email before issuing the application's own session token.
5. Existing users are resolved by Google subject or verified email; conflicting
   accounts are rejected. A new identity follows the normal signup path.

This flow uses basic sign-in identity only, not Gmail, Drive, or other Google
API access. Never log or commit ID tokens, application session tokens, or client
secrets.

## Account migration is separate

Enabling Google login does not migrate Railway users, owner roles, Instagram
connections, or automation rules into Replit. Signing in with an email absent
from Replit creates a new account through the normal signup path. Restore an
existing owner's identity separately with explicit authorization and conflict
checks; do not grant owner privileges as part of generic Google signup.

## Verification

- `GET https://mychaat.net/api/auth/google/config` should return `enabled: true`
  and the production Google client ID after redeploy.
- Open `https://mychaat.net/login` and confirm the official Google button loads.
- The popup must identify MyChaat's authorized domain without an origin error.
- Complete a real sign-in to verify database persistence and session creation;
  displaying a popup alone is not proof of a completed login.

Focused automated checks:

```text
python -m pytest backend/tests/test_google_auth.py -q
npm test -- --watchAll=false --runInBand --runTestsByPath src/lib/googleAuth.test.js
```

Run the npm command from `frontend`. The existing 19 backend tests and 16
frontend tests passed during configuration. Their Google token responses are
test doubles, not evidence of live user authentication.
