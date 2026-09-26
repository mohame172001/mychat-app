# Security review checkpoint - 2026-09-26

## Confirmed fixes

- WebSocket authentication previously validated JWT signature and subject only,
  bypassing the HTTP guard for revoked sessions, suspended/deleted users and
  required email verification. It now applies the existing active-user guard
  before connecting and before processing each incoming message, including a
  fresh JWT expiry check. Revoked idle connections are rejected when they next
  send a message; this is not a server-push revocation mechanism.
- WebSocket conversation writes now repeat the user ownership constraint.
- HTTP request size enforcement previously trusted Content-Length. A bounded
  ASGI middleware now checks actual received bytes, including chunked requests,
  before endpoint parsing. It preserves exact webhook bytes for HMAC checks.

## Validation

- Backend: 1054 passed, 55 skipped, 12 subtests passed. Includes 14 new regression
  tests for WebSocket sessions and request size enforcement.
- Frontend: 40 suites, 306 tests passed.
- git diff --check passed.
- No destructive tests or load tests performed against production.

## Open work and limits

- yarn audit reported 128 dependency findings (21 low, 30 moderate, 77 high).
  These are dependency-path findings, not 128 confirmed exploitable application
  bugs. Examples include ws under webpack-dev-server and svgo under the build
  pipeline. Runtime reachability and compatible upgrades require follow-up.
- npm audit cannot inspect this Yarn-lock project without an npm lockfile;
  Yarn audit was used instead. No second lockfile was generated.
- A full penetration test, deployed-secret review, DNS/cloud access audit,
  billing-provider verification and production load testing are not completed.
- Secrets pasted into previous conversations must be rotated at their providers
  with a coordinated rollout. Do not rotate the encryption key without planning
  migration of existing encrypted Instagram tokens.
- These local fixes must be deployed before they protect the production site.

This checkpoint is not a claim that the site is free of vulnerabilities.
