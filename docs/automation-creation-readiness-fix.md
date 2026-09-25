# Automation creation readiness regression

## Observed failure

Production returned HTTP 400 for `POST /api/automations/quick-comment-rule`.
The connected account had a successful subscription readback, but background
certification (`reason=heal`) stored `certification_scope_check_inconclusive`
and `commentWebhookReady=false`. Its empty cached scope list had no valid
active-token proof. The equivalent admin repair already accepted this state
with an explicit warning; background certification undid that verdict.

## Fix

Treat unproven cached scope denials consistently across certification callers.
Only a fresh successful subscribe and readback, with all required fields,
qualifies for `subscription_verified_scope_proof_inconclusive`. This allows
rule creation without falsely claiming that permission proof is available.

Proven missing permissions, identity/token mismatches, failed or incomplete
subscriptions, and contrary webhook-delivery evidence remain blockers.
No authentication, tenant ownership, plan limits, or action code changed.
Existing paused rules are not automatically activated.

## Validation

Regression tests cover background heal, sync, activation, admin repair, the
quick-rule handler's rejection-before/creation-after flow, proven permission
denials, failed/missing subscriptions, and non-comment-only delivery evidence.
They use isolated database/Graph fixtures, not live Instagram actions.

After deployment, verify the real account's newly persisted certification
after the startup heal. A warning does not prove Meta comment delivery or
reply execution: those still require a real comment on the selected post.
