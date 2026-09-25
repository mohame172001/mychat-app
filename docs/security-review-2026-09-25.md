# Focused security review - 2026-09-25

## Confirmed issue and fix

Admin UI state was cached across logout, signup, password login and Google
login. Auth transitions now invalidate the cache, notify mounted consumers,
and discard late responses from the previous session. Backend permission
checks remain authoritative; this finding does not establish server-side
privilege escalation.

## Evidence

- Railway database: 2 users, 1 owner membership, 0 legacy is_admin=true users.
- Admin email allowlist contains 1 entry. Repair tools and single-tenant
  fallback are not enabled in the inspected production environment.
- Unauthenticated live GET /api/admin/me and /api/admin/overview returned 403.
- 94 backend tests passed covering security, tenant access, roles, webhook
  verification and token encryption.
- 3 frontend regression tests passed: account switch, late response, fail closed.
- Yarn audit reported 0 critical, 77 high, 30 moderate and 21 low occurrences.
  These are dependency-path counts, not 128 proven remotely exploitable bugs.
  Reported paths include react-scripts, development server and build tooling.
  Dependency upgrades and production reachability analysis remain outstanding.

## Remaining work

- Deploy the frontend fix and verify an actual owner-to-normal-user switch.
- Review and update vulnerable build dependencies with build/regression tests;
  do not force incompatible major-version resolutions.
- Previously disclosed credentials must be rotated in their providers and
  corresponding server secrets. Do not rotate the token encryption key blindly:
  existing encrypted records require a coordinated key migration.
- This is not a complete penetration test or a guarantee of no vulnerabilities.
  Replit production availability and migration remain separate unfinished work.
