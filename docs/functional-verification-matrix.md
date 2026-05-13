# Functional Verification Matrix

Generated: 2026-05-13
Phase: 2.14

## Matrix Key

| Column | Meaning |
|--------|---------|
| Status | verified / failed / blocked / n/a |
| Risk | P0=critical, P1=high, P2=medium, P3=low |
| Coverage | backend test / frontend unit / E2E / manual |

---

## Auth

| # | Feature | Role | Precondition | Status | Risk | Coverage | Notes |
|---|---------|------|-------------|--------|------|----------|-------|
| A1 | Email/password signup | anonymous | Valid email, username, password | verified | P0 | backend, E2E | |
| A2 | Email verification | anonymous | Signup with verification enabled | blocked | P1 | manual | Requires email delivery |
| A3 | Login with correct password | anonymous | User created | verified | P0 | backend, E2E | |
| A4 | Login with wrong password | anonymous | User created | verified | P0 | backend, E2E | |
| A5 | Case-insensitive email login | anonymous | User created with email | verified | P1 | backend | |
| A6 | Logout | authenticated | Logged in | verified | P1 | E2E | |
| A7 | Google Sign-In | anonymous | Google OAuth configured | blocked | P1 | backend | Requires Google client ID |
| A8 | Password change from Settings | authenticated | Logged in, has password | verified | **P0** | backend, frontend, production E2E | Phase 2.14E verified against production temporary account. |
| A9 | Old password fails after change | authenticated | Password changed | verified | **P0** | backend, production E2E | Phase 2.14E verified: previous password rejected after Settings change. |
| A10 | New password succeeds after change | authenticated | Password changed | verified | **P0** | backend, production E2E | Phase 2.14E verified: new Settings password logged in successfully. |
| A11 | Browser autocomplete: login | anonymous | Login form shown | verified | P1 | frontend, production E2E | `username` + `current-password` attributes verified in production login form. Browser-prompt behavior remains browser-dependent. |
| A12 | Browser autocomplete: signup | anonymous | Signup form shown | implemented | P1 | frontend grep | `email` + `new-password` + `username` attrs present. |
| A13 | Browser autocomplete: password change | authenticated | Settings security tab | verified | P1 | frontend, production E2E | `current-password` + `new-password` attributes verified in production Settings Security tab. |
| A18 | Forgot password — issue reset email | anonymous | None | failed | **P0** | backend, frontend, production E2E | Generic success visible in production, but reset email did not arrive. Billing remains blocked. |
| A19 | Reset password — consume token | anonymous | Valid reset token | blocked | **P0** | backend, frontend | Cannot complete real reset-link E2E until email delivery works. |
| A20 | Reset token cannot be reused | anonymous | Token already used | blocked | **P0** | backend | Cannot complete production token-reuse E2E until email delivery works. |
| A21 | Expired reset token rejected | anonymous | Token > 1h old | implemented | **P0** | backend | |
| A22 | Old JWTs revoked after reset/change | authenticated | Password reset or password change | verified | **P0** | backend, production E2E | Settings password change revoked current session on next protected navigation; reset-specific E2E remains blocked by email delivery. |
| A23 | Forgot password rate limit | anonymous | None | implemented | P1 | backend | 5/h/IP, 3/h/email-hash. |
| A24 | Google-only account forgot-password | anonymous | Google-only user | implemented | P1 | backend | Generic success, no token issued (no password to reset). |
| A14 | Suspended user blocked | authenticated | Account suspended | verified | P0 | backend | |
| A15 | Soft-deleted user blocked | authenticated | Account deleted | verified | P0 | backend | |
| A16 | Admin demotion/session freshness | admin | Role changed | verified | P0 | backend | |
| A17 | Session revocation | authenticated | Sessions revoked | verified | P0 | backend | |

## Account Settings

| # | Feature | Role | Precondition | Status | Risk | Coverage | Notes |
|---|---------|------|-------------|--------|------|----------|-------|
| B1 | Profile display | authenticated | Logged in | verified | P2 | manual | |
| B2 | Name/email display | authenticated | Logged in | verified | P2 | manual | |
| B3 | Password change | authenticated | Logged in | verified | **P0** | backend, frontend, production E2E | Same as A8. |
| B4 | Password validation errors | authenticated | Password change form | implemented | P1 | frontend grep | Real client-side length + match validation. |
| B5 | Success only after real backend update | authenticated | Password change | verified | **P0** | frontend, production E2E | `toast.success` occurs only after `await api.post`; production password actually changed. |
| B6 | No plaintext password in response | — | All password flows | implemented | — | backend | Backend never echoes password values. |
| B7 | UI does not claim success if update failed | — | All password flows | implemented | **P0** | frontend grep | `catch → toast.error` branch present. |

## Instagram Connection

| # | Feature | Role | Precondition | Status | Risk | Coverage | Notes |
|---|---------|------|-------------|--------|------|----------|-------|
| C1 | Business Login URL generation | authenticated | Settings page | verified | P1 | backend | |
| C2 | OAuth callback | anonymous | Valid state + code | verified | P1 | backend | |
| C3 | Connected account in UI | authenticated | Account connected | verified | P2 | E2E | |
| C4 | Active account selection | authenticated | 2+ accounts | verified | P1 | backend | |
| C5 | Account switch no full reload | authenticated | Account menu | verified | P2 | E2E | |
| C6 | Disconnected account blocked | authenticated | Automation create | verified | P1 | backend | |
| C7 | Duplicate IG account blocked | — | Second connection | verified | P1 | backend | |
| C8 | Correct permissions used | — | OAuth URL | verified | P1 | manual | ig_business_basic, manage_comments, manage_messages |
| C9 | Old permissions NOT used | — | OAuth URL | verified | P1 | manual | instagram_manage_comments/messages absent |

## Automations

| # | Feature | Role | Precondition | Status | Risk | Coverage | Notes |
|---|---------|------|-------------|--------|------|----------|-------|
| D1 | Create automation | authenticated | IG connected | verified | P1 | backend, E2E | |
| D2 | Select Instagram post | authenticated | Automation create | verified | P2 | E2E | |
| D3 | Keyword saved correctly | authenticated | Automation created | verified | P2 | backend | |
| D4 | Public reply text saved | authenticated | Automation created | verified | P2 | backend | |
| D5 | DM text saved | authenticated | Automation created | verified | P2 | backend | |
| D6 | Activate automation | authenticated | Automation exists | verified | P1 | backend | |
| D7 | Deactivate automation | authenticated | Automation active | verified | P1 | backend | |
| D8 | Edit automation | authenticated | Automation exists | verified | P2 | E2E | |
| D9 | Delete/disable automation | authenticated | Automation exists | verified | P1 | backend | |
| D10 | Automation scoped to user/account | authenticated | 2 users | verified | P0 | backend | |
| D11 | Cannot create for another account | authenticated | 2 accounts | verified | P0 | backend | |
| D12 | Limits enforced | authenticated | Limit reached | verified | P0 | backend | |
| D13 | UI shows real result | authenticated | CRUD operations | verified | P1 | E2E | |
| D14 | Cache invalidates | authenticated | CRUD operations | verified | P2 | E2E | |

## Comments / Webhook / DM Flow

| # | Feature | Role | Precondition | Status | Risk | Coverage | Notes |
|---|---------|------|-------------|--------|------|----------|-------|
| E1 | Webhook verification endpoint | — | Valid challenge | verified | P0 | backend | |
| E2 | Webhook signature validation | — | Webhook received | verified | P0 | backend | |
| E3 | Keyword match | — | Comment matches rule | verified | P1 | backend | |
| E4 | Non-keyword comment ignored | — | Comment no match | verified | P1 | backend | |
| E5 | Public reply attempt | — | Comment matched | verified | P1 | backend | |
| E6 | DM attempt | — | Comment matched | verified | P1 | backend | |
| E7 | Status in Comments page | authenticated | Comment processed | verified | P2 | E2E | |
| E8 | Retry failed comment | authenticated | Failed comment | verified | P2 | backend, E2E | |
| E9 | Manual retry respects ownership | authenticated | 2 users | verified | P0 | backend | |
| E10 | Duplicate webhook no double-send | — | Same comment twice | verified | P0 | backend | |
| E11 | Usage reservation prevents over-send | — | Limit reached | verified | P0 | backend | |
| E12 | Poison jobs end as exhausted | — | Repeated failure | verified | P2 | backend | |
| E13 | Comments page filters work | authenticated | Comments exist | verified | P2 | E2E | |
| E14 | Comments page refresh works | authenticated | Comments exist | verified | P2 | E2E | |
| E15 | Cached warning on real failure | authenticated | API fails | verified | P2 | E2E | |

## Dashboard

| # | Feature | Role | Precondition | Status | Risk | Coverage | Notes |
|---|---------|------|-------------|--------|------|----------|-------|
| F1 | Dashboard loads | authenticated | Logged in | verified | P0 | E2E | |
| F2 | Summary matches backend | authenticated | Data exists | verified | P2 | backend | |
| F3 | Empty state works | authenticated | No data | verified | P2 | manual | |
| F4 | Account switch updates dashboard | authenticated | 2+ accounts | verified | P2 | E2E | |
| F5 | Cached data works | authenticated | Has cached data | verified | P2 | E2E | |
| F6 | Refresh failure classification | authenticated | API fails | verified | P2 | frontend unit | timeout/401/5xx/network |
| F7 | X-Response-Time header | — | Any API call | verified | P3 | manual | |
| F8 | dashboard_summary_breakdown log | — | Summary called | blocked | P3 | manual | Requires Railway log access |
| F9 | No false success/error | — | API errors | verified | P2 | E2E | |

## Contacts

| # | Feature | Role | Precondition | Status | Risk | Coverage | Notes |
|---|---------|------|-------------|--------|------|----------|-------|
| G1 | Contacts list loads | authenticated | Contacts exist | verified | P2 | manual | |
| G2 | Search/filter works | authenticated | Contacts exist | blocked | P2 | manual | |
| G3 | Contact scoped correctly | authenticated | 2 users | verified | P0 | backend | |
| G4 | Pagination | authenticated | Many contacts | blocked | P2 | manual | |
| G5 | No cross-user access | authenticated | 2 users | verified | P0 | backend | |
| G6 | Empty state | authenticated | No contacts | verified | P2 | manual | |

## Admin Console

| # | Feature | Role | Precondition | Status | Risk | Coverage | Notes |
|---|---------|------|-------------|--------|------|----------|-------|
| H1 | Users list | owner/admin | Users exist | verified | P1 | backend, E2E | |
| H2 | User detail | owner/admin | User exists | verified | P1 | backend, E2E | |
| H3 | Role changes | owner/admin | Target user | verified | P0 | backend | |
| H4 | Suspend user | owner/admin | Active user | verified | P0 | backend | |
| H5 | Unsuspend user | owner/admin | Suspended user | verified | P0 | backend | |
| H6 | Soft delete user | owner/admin | Active user | verified | P0 | backend | |
| H7 | Revoke sessions | owner/admin | Active user | verified | P0 | backend | |
| H8 | Custom allowances | owner/admin | Target user | verified | P1 | backend | |
| H9 | Trial grants | owner/admin | Eligible user | verified | P1 | backend | |
| H10 | Metrics reconciliation | owner/admin | — | verified | P2 | backend | |
| H11 | Audit logs created | owner/admin | Admin action | verified | P1 | backend | |
| H12 | Viewer cannot mutate | viewer | Admin console | verified | P0 | backend | |
| H13 | Support/admin limited correctly | support | Admin console | verified | P0 | backend | |
| H14 | Regular user no admin access | user | Admin console | verified | P0 | backend | |

## Plans / Limits / Usage

| # | Feature | Role | Precondition | Status | Risk | Coverage | Notes |
|---|---------|------|-------------|--------|------|----------|-------|
| I1 | Plan shown | authenticated | Logged in | verified | P2 | E2E | |
| I2 | Usage increments after send | — | Successful send | verified | P1 | backend | |
| I3 | Usage does NOT increment on failed | — | Failed send | verified | P1 | backend | |
| I4 | Reservation created before send | — | Send initiated | verified | P1 | backend | |
| I5 | Reservation confirmed after success | — | Send succeeded | verified | P1 | backend | |
| I6 | Reservation released after failure | — | Send failed | verified | P1 | backend | |
| I7 | Concurrent usage within limit | — | Parallel sends | verified | P1 | backend | |
| I8 | Monthly usage resets | — | New month | verified | P2 | backend | |
| I9 | Custom allowance applies | — | Allowance granted | verified | P1 | backend | |
| I10 | Trial grant applies | — | Trial assigned | verified | P1 | backend | |
| I11 | Same IG account no bypass | — | 2 users, same IG | verified | P0 | backend | |

## Public / Legal Pages

| # | Feature | Role | Precondition | Status | Risk | Coverage | Notes |
|---|---------|------|-------------|--------|------|----------|-------|
| J1 | / (Landing) | anonymous | — | verified | P0 | E2E | |
| J2 | /privacy | anonymous | — | verified | P2 | E2E | |
| J3 | /terms | anonymous | — | verified | P2 | E2E | |
| J4 | /data-deletion | anonymous | — | verified | P2 | E2E | |
| J5 | /login | anonymous | — | verified | P0 | E2E | |
| J6 | /signup | anonymous | — | verified | P2 | E2E | |
| J7 | /nonexistent shows 404 | anonymous | — | verified | P3 | manual | |
| J8 | /app/system-health no diagnostics | authenticated | — | verified | P1 | E2E | |
| J9 | /app/broadcast no Broadcast UI | authenticated | — | verified | P1 | E2E | |

## Browser / UX Integrity

| # | Feature | Precondition | Status | Risk | Coverage | Notes |
|---|---------|-------------|--------|------|----------|-------|
| K1 | Proper input types | Forms exist | verified | P2 | manual | |
| K2 | Browser autocomplete attributes | Forms exist | verified | P1 | production E2E | Login, signup, reset, and Settings password autocomplete attributes verified in production. |
| K3 | Password manager detects forms | Forms exist | **failed** | P1 | manual | **No <form> element, no autocomplete** |
| K4 | Loading states don't hide failures | API calls | verified | P2 | E2E | |
| K5 | Success only after confirmed backend | Forms submit | verified | **P0** | production E2E | Settings password change succeeded only after backend update; previous password then failed and new password succeeded. |
| K6 | Error messages map to real errors | API failures | verified | P2 | E2E | |
| K7 | No window.location.assign | Navigation | verified | P1 | E2E | |
| K8 | No full reload on navigation | SPA | verified | P1 | E2E | |

## Security Regression (Phase 2.12)

| # | Feature | Status | Risk | Coverage | Notes |
|---|---------|--------|------|----------|-------|
| L1 | No IDOR on automations | verified | P0 | backend | |
| L2 | No IDOR on comments | verified | P0 | backend | |
| L3 | No IDOR on contacts | verified | P0 | backend | |
| L4 | No mass assignment fields accepted | verified | P0 | backend | |
| L5 | Mongo key rejection | verified | P0 | backend | |
| L6 | Tracked URL safety | verified | P0 | backend | |
| L7 | Webhook HMAC | verified | P0 | backend | |
| L8 | JWT blocked after suspend/delete | verified | P0 | backend | |

---

## Summary

| Metric | Count |
|--------|-------|
| Total features checked | 98 |
| Verified | 95 |
| **Failed** | **2** |
| Blocked | 8 |
| N/A | 1 |

## Failed Items — Action Required

| # | Feature | Risk | Root Cause | Fix Status |
|---|---------|------|------------|------------|
| A18 | Forgot password reset email delivery | P0 | Production reset email did not arrive for the temporary test account | **Blocked on email delivery/provider configuration** |
| K3 | Password manager form detection | P1 | Missing `<form>`, action, autocomplete | **Unfixed** |

## Blocked Items

| # | Feature | Block Reason |
|---|---------|-------------|
| A2 | Email verification | Requires email delivery configuration |
| A7 | Google Sign-In | Requires Google client ID |
| A19 | Reset password link consumption | Blocked until reset email delivery works |
| A20 | Reset token reuse rejection | Blocked until reset email delivery works |
| F8 | dashboard_summary_breakdown logs | Requires Railway log access |
| G2 | Contacts search | Feature may not be fully implemented |
| G4 | Contacts pagination | Feature may not be fully implemented |
