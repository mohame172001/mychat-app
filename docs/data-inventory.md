# MyChat Data Inventory

This inventory supports Meta App Review, data deletion handling, and incident response. It is intentionally operational rather than legal advice.

## users
- Stores: login identity, normalized email/username, password hash or linked provider metadata, account status, current Instagram account pointers, plan references.
- Meta data: current Instagram account pointer and handle metadata.
- Raw text: no comment/reply/DM body by design.
- Deletion: user account is soft-deleted first so audit and abuse history remain intact.
- Retention reason: abuse prevention, account recovery, auditability.
- Backup caveat: backups may retain historical copies until normal backup expiry.

## instagram_accounts
- Stores: canonical Instagram professional account id (`instagramAccountId`), owner user id, username/profile metadata, connection validity, token lifecycle metadata, encrypted/secret token fields where configured.
- Meta data: yes, account identifiers and profile metadata.
- Raw text: no.
- Deletion: disconnect marks inactive/invalid; usage/trial history is preserved to prevent limit/trial reset abuse.
- Retention reason: duplicate ownership prevention, trial/usage abuse prevention, auditability.
- Backup caveat: deleted/disconnected account records may remain in backups until expiry.

## automations
- Stores: rule names, triggers, selected media ids, sanitized graph/action config, reply/DM templates needed to execute user-requested automations.
- Meta data: selected media ids and Instagram account id.
- Raw text: user-entered reply/DM templates may be stored because they are the configured automation content.
- Deletion: disabled/paused during soft delete; reviewed during deletion requests.
- Retention reason: product operation and user configuration.

## comments / actions / queue jobs
- Stores: comment/action ids, statuses, provider proof booleans, retry state, safe hashes/lengths where available.
- Meta data: comment ids, media ids, commenter ids/usernames where required for automation state.
- Raw text: raw comment/reply/DM bodies should not be exposed in diagnostics/admin responses; operational storage should be minimized.
- Deletion: removed or anonymized according to verified deletion workflow; provider-proof/audit state may be retained in minimized form where required.
- Retention reason: dedupe, provider-proof, abuse prevention, queue recovery.

## usage_events and monthly_usage
- Stores: event counters, event month, limit subject type/id, automation/comment/account ids.
- Meta data: Instagram account id as limit subject.
- Raw text: no.
- Deletion: non-destructive reconciliation may retain aggregate counters; account-level usage remains for abuse prevention.
- Retention reason: billing readiness, limits, reconciliation, abuse prevention.

## user_plans and user_limit_overrides
- Stores: plan keys, effective limits, override amounts, dates, sanitized reason length/hash.
- Meta data: no direct Meta payloads.
- Raw text: raw admin reason text should not be stored.
- Deletion: retained as audit/business records in minimized form.

## instagram_account_trial_claims
- Stores: Instagram account id, first claiming user id, trial key/status, timestamps.
- Meta data: canonical Instagram account id.
- Raw text: no.
- Deletion: retained in minimized form to prevent repeated trial abuse.

## admin_audit_logs
- Stores: admin actor id, target id, action, timestamps, sanitized metadata.
- Meta data: may include safe account/user identifiers.
- Raw text: no raw reason text; use length/hash.
- Deletion: retained for security/audit integrity.

## tracked_links and link_click_events
- Stores: server-side link destination, short code, click counters, hashed IP/user-agent/referrer metadata.
- Meta data: automation/comment/account relation ids.
- Raw text: destination URL may be user-configured; click referrer is hashed, not stored raw.
- Deletion: removed/anonymized with automation/account deletion where applicable.

## data_deletion_requests
- Stores: confirmation code, request source, signed-request presence/validity, signed request hash, email hash, IP hash, status.
- Meta data: hashed signed_request only.
- Raw text: no raw signed_request or email.
- Deletion: retained as deletion request audit trail.

## observability logs
- Stores: route names, safe statuses, durations, exception types, sanitized fields.
- Meta data: safe ids/hashes may appear.
- Raw text: tokens, raw comments, raw replies, raw DMs, raw Graph/webhook bodies must be scrubbed.
- Deletion: subject to provider retention and manual incident handling if a leak occurs.

## backups
- Stores: database snapshots according to the configured Mongo provider.
- Deletion: deletion requests are applied to live data; backups expire according to provider policy. Emergency restore must re-apply deletion/anonymization records.
- Audit caveat: audit logs should remain available in minimized form after restore.
