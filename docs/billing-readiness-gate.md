# Billing Readiness Gate

Generated: 2026-05-13

This gate intentionally does not add Paddle, Paymob, Stripe, checkout, subscriptions, or payment webhooks. It answers whether the security base is ready for a future billing implementation.

| Question | Expected | Result | Evidence | Notes |
|---|---:|---:|---|---|
| Can frontend change plan directly? | no | no | backend/tests/test_idor_rbac_mass_assignment_phase212c.py; frontend/e2e/product-smoke.spec.js | Plan changes are admin/server-side only. |
| Is plan enforcement server-side? | yes | yes | backend/server.py check_plan_limit; backend/tests/test_usage_reservation_ledger.py | Frontend values are display only. |
| Are limits atomic enough? | yes | yes | usage reservation ledger tests | Reservation/idempotency key protects remaining=1 races. |
| Can same Instagram account reset limits through new user? | no | no | backend/tests/test_instagram_account_limit_abuse.py | Duplicate active ownership is blocked and usage subject follows IG account. |
| Are admin overrides audited? | yes | yes | backend/tests/test_admin_advanced.py | Plan/allowance/member mutations write sanitized audit logs. |
| Are raw payment payloads prohibited by policy? | yes | yes | this document; security matrix BILL checks | No provider integration exists yet; future billing must preserve scrubber policy. |
| Is webhook idempotency pattern established? | yes | yes | backend/tests/test_webhook_idempotency.py; queue/state tests | Pattern can be reused for payment webhooks later. |
| Are secrets/env scanning rules documented? | yes | yes | docs/production-security-checklist.md; docs/deployment-verification.md | No real secrets found in static scan. |
| Are checkout buttons disabled until provider integration? | yes | yes | frontend/e2e/product-smoke.spec.js | Billing remains placeholder-only; no Stripe/Paddle/Paymob checkout. |
| Is there any P0/P1/P2 blocker before Billing? | no | no | docs/security-verification-matrix.md | Only P3 operational follow-ups remain. |

Conclusion: Billing can start from a security-readiness standpoint. Future billing work must add provider-specific webhook signature verification, idempotency keys, raw payload redaction, server-side plan mutation only, and reconciliation tests before accepting real payments.

### Phase 2.14 product-functional gate (added)

Even with the security gate green, billing remains **BLOCKED** on product-functional verification:

- Password change E2E: **verified on production** in Phase 2.14E. Settings Security rendered for an authenticated temporary production test account, show/hide toggles worked, autocomplete attributes were correct, password change succeeded only after the backend call, the current session was revoked on the next protected navigation, the previous password failed, and the new password succeeded.
- Password reset E2E: code shipped in Phase 2.14, public UI and backend generic response are live, but real email-delivery E2E is **blocked/failed** because the reset email did not arrive for the temporary production test account. Phase 2.14F added safe delivery diagnostics and broadened reset-link payload keys for provider-template compatibility, but billing remains blocked until a real reset email is received and consumed. See `docs/auth-recovery-notes.md`.

Billing must not start until password reset email delivery, reset-link consumption, old-password rejection after reset, new-password login after reset, and token-reuse rejection all pass on the live host with the production email webhook.

### Phase 2.16 performance gate (added)

Billing also remains **BLOCKED** on final production performance proof:

- Backend health measured warm at 180-183 ms after an initial 477 ms request from the workstation.
- `GET /api/dashboard/summary` now emits safe timing headers and safe frontend perf logs so authenticated dashboard timing can be measured without exposing sensitive data.
- Railway CLI access was unavailable locally (`invalid_grant`), so App Sleeping / always-on status must be verified in the Railway dashboard by the operator.
- Billing must not start until either:
  - authenticated dashboard warm p50 <= 500 ms and p95 <= 1200 ms with route usable render <= 2s, or
  - any remaining cold-start delay is explicitly accepted only after the backend is moved to an always-on service/plan.

See `docs/production-infrastructure-performance.md`.

## Red-Team Billing Abuse Delta

| Check | Result | Evidence |
|---|---:|---|
| Frontend cannot set plan | yes | Admin/server-only plan mutation tests and placeholder-only billing smoke. |
| Admin manual override precedence documented | yes | docs/billing-readiness-gate.md and effective limits tests. |
| Custom allowance precedence documented | yes | backend/limit_overrides.py and admin allowance tests. |
| Usage ledger/reservations authoritative | yes | usage reservation ledger tests. |
| Plan downgrade behavior planned before provider integration | yes | Future billing note in this gate. |
| Canceled/past_due/refund behavior planned before provider integration | yes | Future billing note in this gate. |
| Webhook event idempotency pattern documented | yes | Webhook and reservation idempotency tests. |
| Provider raw payload storage prohibited | yes | Scrubber policy and matrix LOG/BILL checks. |
| Checkout success page will not activate plan directly | yes | Billing gate requires server-side provider webhook confirmation in future phase. |
| Billing status endpoint must be auth-protected | yes | API inventory and billing gate requirement; no public billing mutation exists. |

## 2026 Security Standards Billing Delta

- OWASP API6 business-flow abuse: future billing status endpoints must be auth-protected and rate-limited.
- OWASP API10 third-party consumption: payment provider webhooks must verify signatures, use event idempotency, and never let checkout success pages activate plans directly.
- CISA Secure by Design: checkout remains disabled until provider integration is implemented and tested server-side.
- NIST SSDF: dependency and vulnerability response process is documented before adding payment code.

## Cross-Domain Payment Readiness Delta

- PCI DSS future-payment boundary: MyChat must not collect card numbers, CVV, or payment method secrets.
- Paddle/Paymob can start only after a Billing abstraction defines server-side plan mutation, provider webhook signature verification, provider event idempotency, replay handling, and raw payload redaction.
- Checkout success pages must never activate plans directly.
- No PCI compliance claim may be made unless formally assessed.
- Refund, chargeback, canceled, and past_due behavior must be defined before accepting real payments.
