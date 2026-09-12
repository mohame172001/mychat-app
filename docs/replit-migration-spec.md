# Replit PostgreSQL Migration Specification

Status: Phase 1 specification only  
Target: Replit-managed PostgreSQL  
Source system: the MongoDB database used by the current FastAPI application  
Scope verified against: `backend/server.py`, `backend/runtime_scaling.py`,
`backend/app/`, `backend/billing/`, `backend/scripts/`, `backend/models.py`,
`backend/tests/`, and `backend/.env.example`

## 1. Purpose and constraints

This document specifies how to replace the application's MongoDB persistence with
Replit-managed PostgreSQL without changing the public API contract or the
application's externally observable behavior.

This phase is documentation only. It does not authorize:

- runtime changes;
- creation or modification of PostgreSQL objects;
- access to secrets;
- production data export or import;
- changes to Railway configuration or production;
- deployment or cutover.

The repository has no authoritative MongoDB schema. Collection names, operations,
indexes, aggregation pipelines, and concurrency behavior below are verified from
source. Full field populations, type drift, cardinality, orphan rates, and indexes
created outside application bootstrap require a read-only production profile before
an executable migration can be approved.

## 2. Source architecture

- The backend creates a Motor client and selects a database in
  `backend/server.py:70,311`.
- Most application database calls are in `backend/server.py`.
- The durable webhook inbox wraps `db.webhook_inbox` in
  `backend/runtime_scaling.py:24-84`.
- `backend/billing/service.py` contains a provider-neutral MongoDB service for
  `subscriptions` and `invoices`. No import or construction of
  `SubscriptionService` is present in the current backend entry point, so these
  calls are dormant/unreachable from the current FastAPI runtime. They remain in
  migration scope because a database populated by an earlier or separately wired
  runtime could contain durable billing records.
- `backend/scripts/automation_stop_point_cli.py:64-75` selects a Mongo database
  dynamically by environment variable, but does not dynamically select collection
  names.
- `_index_bootstrap` creates and changes indexes and performs best-effort data
  repairs during application startup (`backend/server.py:31072-31724`). Startup
  schedules it asynchronously and logs rather than failing the process on an error.
  PostgreSQL schema management must not preserve this non-atomic, fail-open model.
- Application identifiers are predominantly application-generated strings. Source
  code does not construct MongoDB `ObjectId` values. The webhook inbox is the
  exception to normal IDs: its Mongo `_id` is a deterministic SHA-256 digest.

## 3. Verified MongoDB operation inventory

### 3.1 Supported methods

Static source analysis found these production Mongo methods:

- reads: `find`, `find_one`, `count_documents`, and `aggregate`;
- creates: `insert_one`;
- updates: `update_one`, `update_many`, and `find_one_and_update`;
- deletes: `delete_one` and `delete_many`;
- schema/index operations: `create_index`, `drop_index`, and `command` for a
  `collMod` TTL adjustment.

No production call was found for `insert_many`, `replace_one`, `bulk_write`,
`find_one_and_delete`, `distinct`, change streams, sessions, or MongoDB
transactions.

The table below records every fixed collection reference and each directly invoked
method found in production source. Counts are static call-site counts, not runtime
invocation counts. Index methods are included because they define source behavior.

| MongoDB collection | Verified methods and static call-site counts |
|---|---|
| `admin_audit_logs` | `find` (1), `insert_one` (1), `create_index` (3) |
| `admin_members` | `find` (2), `find_one` (9), `update_one` (4), `create_index` (4) |
| `automations` | `aggregate` (1), `count_documents` (10), `find` (14), `find_one` (15), `insert_one` (3), `update_one` (7), `update_many` (13), `delete_one` (1), `delete_many` (1), `create_index` (1) |
| `broadcasts` | `find` (1), `find_one` (2), `insert_one` (1), `update_one` (3) |
| `comment_dm_sessions` | `count_documents` (2), `find` (8), `find_one` (6), `insert_one` (1), `update_one` (14), `update_many` (1), `delete_many` (1), `create_index` (4) |
| `comments` | `aggregate` (1), `count_documents` (12), `find` (16), `find_one` (19), `find_one_and_update` (2), `insert_one` (1), `update_one` (37), `update_many` (2), `create_index` (4), `drop_index` (1) |
| `contacts` | `count_documents` (1), `find` (2), `find_one` (1), `insert_one` (1), `update_one` (1), `delete_one` (1), `create_index` (1) |
| `conversations` | `find` (1), `find_one` (4), `insert_one` (1), `update_one` (3), `update_many` (1), `delete_many` (1), `create_index` (1) |
| `dashboard_summaries` | `find_one` (1), `update_one` (1), `delete_many` (1), `create_index` (4) |
| `data_deletion_requests` | `insert_one` (1), `create_index` (1) |
| `dm_logs` | `count_documents` (2), `find` (7), `find_one` (1), `insert_one` (1), `update_one` (9), `update_many` (1), `delete_many` (1), `create_index` (2), `drop_index` (2) |
| `dm_rules` | `count_documents` (1), `find` (4), `find_one` (1), `insert_one` (1), `update_one` (1), `update_many` (1), `delete_one` (1), `delete_many` (1), `create_index` (2) |
| `instagram_account_trial_claims` | `find` (1), `find_one` (1), `update_one` (1), `create_index` (2) |
| `instagram_accounts` | `aggregate` (1), `count_documents` (10), `find` (29), `find_one` (33), `find_one_and_update` (1), `update_one` (27), `update_many` (12), `create_index` (7) |
| `instagram_automation_events` | `find` (5), `find_one` (2), `insert_one` (1), `create_index` (4), plus database `collMod` for TTL |
| `instagram_media_catalog` | `count_documents` (1), `find` (1), `find_one` (1), `update_one` (1), `create_index` (2) |
| `invoices` | `find` (1), `insert_one` (1), through dormant `SubscriptionService` |
| `link_click_events` | `find` (2), `insert_one` (1), `create_index` (6) |
| `monthly_usage` | `count_documents` (1), `find` (10), `find_one` (5), `update_one` (7), `create_index` (3), `drop_index` (1) |
| `oauth_code_consumed` | `find_one` (1), `find_one_and_update` (1), `update_one` (1), `create_index` (2) |
| `subscriptions` | `find` (1), `find_one` (3), `insert_one` (2), `update_one` (2), through dormant `SubscriptionService` |
| `tracked_links` | `find_one` (1), `insert_one` (1), `update_one` (3), `create_index` (4) |
| `usage_events` | `find` (3), `insert_one` (1), `create_index` (7) |
| `usage_reservation_buckets` | `find_one_and_update` (1), `update_one` (3), `create_index` (1) |
| `usage_reservations` | `find` (1), `find_one` (4), `find_one_and_update` (2), `insert_one` (1), `update_one` (2), `create_index` (3) |
| `user_limit_overrides` | `find` (2), `find_one` (2), `insert_one` (1), `update_one` (1), `create_index` (4) |
| `user_notification_preferences` | `find_one` (3), `update_one` (1), `create_index` (1) |
| `user_plans` | `find` (2), `find_one` (2), `update_one` (1), `create_index` (1) |
| `users` | `count_documents` (15), `find` (8), `find_one` (84), `insert_one` (2), `update_one` (28), `delete_one` (1), `create_index` (7) |
| `webhook_inbox` | `find_one_and_update` (1), `update_one` (4), `create_index` (2); enqueue also uses `update_one(..., upsert=True)` |
| `webhook_log` | `count_documents` (3), `find` (8), `insert_one` (1), `delete_one` (1) |
| `webhook_processing_failures` | `count_documents` (4), `find` (2), `find_one` (1), `insert_one` (1), `update_one` (4), `create_index` (3) |

The billing calls are source-verified at `backend/billing/service.py:84-115,
119-188,192-277`. The service documents the complete subscription shape at
`backend/billing/service.py:1-27` and creates invoice documents at
`backend/billing/service.py:219-236`. It stores opaque provider IDs and receipt
URLs, not card numbers or provider tokens. Because the service is not imported by
the current FastAPI entry point, production profiling must determine whether either
collection exists or contains historical data. Absence from the current runtime is
not permission to omit or delete those records.

### 3.2 Aggregation pipelines

There are exactly three verified production calls to `aggregate()`:

1. `instagram_accounts` at `backend/server.py:11755-11770`
   - `$match` accepts legacy `userId` or canonical `user_id` in a requested user
     list.
   - It requires `connectionValid: true`, treats `isActive != false` as active,
     requires a present, non-null, non-empty `accessToken`, and excludes known
     disconnected/cleanup/replacement `refreshStatus` values.
   - `$group` uses `$ifNull` to choose `userId` then `user_id`, and `$sum: 1`.
   - PostgreSQL translation: a grouped count over canonical `user_id` with explicit
     connection, activity, token-presence, and refresh-status predicates.

2. `automations` at `backend/server.py:11779-11782`
   - `$match`: `user_id IN (...)` and `status = 'active'`.
   - `$group`: count by `user_id` using `$sum: 1`.
   - PostgreSQL translation: `GROUP BY user_id`.

3. `comments` at `backend/server.py:24098-24108`
   - `$match`: terminal/partial action statuses and either missing or `unknown`
     `media_ownership_check`.
   - `$group`: count by `user_id`.
   - `$limit`: 50 grouped users.
   - PostgreSQL translation: grouped count with a null-or-unknown predicate and an
     explicit deterministic order before `LIMIT 50`. MongoDB does not define which
     50 groups are returned here; PostgreSQL behavior must be made deterministic
     and accepted as a clarified contract.

No `$lookup`, `$unwind`, `$project`, or aggregation `$sort` stage was found.

### 3.3 Query and update operators

Verified query operators include:

- logical: `$and`, `$or`;
- membership: `$in`, `$nin`;
- existence and inequality: `$exists`, `$ne`;
- ranges: `$lt`, `$lte`, `$gt`, `$gte`;
- text matching: `$regex` with `$options`;
- nested arrays: `$elemMatch`;
- dot-path access, including automation graph paths such as `nodes.data.trigger`.

Verified update operators include:

- `$set`;
- `$setOnInsert`;
- `$inc`;
- `$unset`;
- `$push`;
- `$addToSet`;
- `$pull`;
- `$max`;
- `$min`.

PostgreSQL translations:

- equality, membership, ranges, and nullability become typed SQL predicates;
- case-insensitive searches become escaped `ILIKE` predicates or indexed normalized
  search columns, not untrusted regular expressions;
- `$elemMatch` and graph dot paths remain JSONB predicates only during transition,
  unless profiling proves they are frequent enough to justify typed child tables;
- `$inc`, `$min`, and `$max` become single-statement conditional updates;
- `$addToSet` requires either a child table with a unique constraint or a
  transactionally updated JSONB array;
- `$push` and `$pull` must not be translated into read-modify-write application
  code without row locking;
- `$unset` becomes `NULL`, column removal from JSONB, or deletion of a child row,
  based on the mapped field.

Missing and explicit `null` are distinct in MongoDB. During import each field must
have a documented normalization rule. Unknown source fields are retained in
`source_extra jsonb` until production profiling and reconciliation prove they can
be discarded.

## 4. Identifiers and aliases

Verified identity and deduplication fields include:

- users: application `id`; Mongo `_id` is not used as the public identifier;
- ownership aliases: `user_id` and legacy `userId`;
- Instagram account aliases: `instagramAccountId`, `instagram_account_id`,
  `igUserId`, and `ig_user_id`;
- Instagram comment identity: `ig_comment_id`;
- DM deduplication: `dedup_key`;
- media uniqueness: `instagramAccountDbId` plus `media_id`;
- tracked links: `shortCode`;
- OAuth replay protection: `provider` plus `code_hash`;
- usage reservations: `idempotency_key`;
- trial claims: `instagram_account_id` plus `plan_trial_identifier`;
- webhook inbox: deterministic SHA-256 Mongo `_id`.

Target ID policy:

1. Preserve every source application ID byte-for-byte in a text-compatible
   canonical column. Do not cast a string to PostgreSQL `uuid` until production
   profiling proves all values are valid UUIDs.
2. PostgreSQL may use an internal generated key where useful, but all imported
   external IDs and deduplication keys remain unique and queryable.
3. Normalize aliases into one typed canonical column and retain the original alias
   fields temporarily in `source_extra`.
4. Do not use Mongo `_id` as a relationship key unless profiling finds records that
   lack an application ID.
5. Quarantine collisions instead of selecting a winner silently.

The repository's own staged alias work is documented in
`docs/canonical-field-migration-plan.md`; fallback removal must wait for production
data proof.

## 5. Sensitive, hashed, and encrypted fields

Verified sensitive persistence includes:

- `users.password_hash`;
- `users.email_verification_token_hash`;
- `users.password_reset_token_hash`;
- legacy/user-level `meta_access_token`;
- account-level `accessToken`, with legacy spellings read in some compatibility
  paths;
- `webhook_inbox.payload_encrypted`.

Billing documents store provider subscription and checkout-session IDs plus receipt
URLs. They are not payment credentials, but they are sensitive customer and
provider-correlating data and must be excluded from general logs and public
reconciliation output (`backend/billing/service.py:1-27,141-155,219-236`).

`backend/runtime_scaling.py:27,38,66,82` verifies that webhook inbox payloads use
Fernet with `WEBHOOK_INBOX_ENCRYPTION_KEY`, are decrypted only for processing, and
are removed on terminal completion. The inbox ciphertext must be copied
byte-for-byte. The same encryption key is required while any imported pending job
exists.

Source code verifies that Instagram access tokens are stored and read, but it does
not prove that those token values are encrypted at rest. `docs/data-inventory.md`
describes encrypted/secret token fields "where configured." Therefore:

- never label all historical access-token values as encrypted without profiling;
- preserve token bytes exactly during migration;
- classify each source representation as ciphertext, plaintext, null, or unknown;
- do not decrypt and re-encrypt as part of bulk import;
- validate decryptability only in an isolated authorized environment;
- never emit credential values in migration logs, rejects, hashes, or reports.

Password and one-time-token hashes remain hashes and are copied exactly. Secret
columns must be excluded from `source_extra`, general audit JSON, reconciliation
digests, and application logs.

## 6. Source indexes and TTL behavior

The following is the complete index set declared by application bootstrap.
Production profiling must additionally run MongoDB `listIndexes` because operators
may have created indexes outside this source.

### `comments`

- Drop legacy `uniq_user_ig_comment`.
- Unique sparse `(user_id, instagramAccountId, ig_comment_id)`.
- `(user_id, instagramAccountId, created DESC)`.
- Sparse `(user_id, instagramAccountId, opening_dedupe_key, created DESC)`.
- `(action_status, next_retry_at, queue_lock_until)`.
- Source: `backend/server.py:31084-31095,31126-31138`.

### `dm_logs`

- Drop legacy `uniq_user_dm_message` and `uniq_user_dm_dedup_key`.
- Unique sparse `(user_id, instagramAccountId, dedup_key)`.
- `(user_id, instagramAccountId, created DESC)`.
- Source: `backend/server.py:31096-31115,31147-31149`.

### Automation, conversation, contact, and session indexes

- `dm_rules`: `(user_id, is_active)` and
  `(user_id, instagramAccountId, is_active)`.
- `automations`: `(user_id, instagramAccountId, status)`.
- `conversations`: `(user_id, instagramAccountId, created DESC)`.
- `contacts`: `(user_id, instagramAccountId, created DESC)`.
- `comment_dm_sessions`: `(user_id, instagramAccountId, created DESC)`;
  sparse `(user_id, instagramAccountId, opening_dedupe_key, created DESC)`;
  `(user_id, recipient_id, status, created DESC)`.
- Source: `backend/server.py:31117-31167,31591-31596`.

### Media and link indexes

- `instagram_media_catalog`: unique `(instagramAccountDbId, media_id)` and
  `(instagramAccountDbId, media_timestamp DESC)`.
- `tracked_links`: unique sparse `shortCode`;
  `(user_id, instagramAccountId, ruleId)`;
  `(user_id, instagramAccountId, created DESC)`; `expiresAt`.
- `link_click_events`: `trackedLinkId`; `shortCode`;
  `(user_id, instagramAccountId, clickedAt DESC)`;
  `(user_id, instagramAccountId, instagramUserId)`; legacy `userId`.
- Source: `backend/server.py:31168-31218`.

### Usage indexes

- `usage_events`: `(user_id, event_month)`;
  `(user_id, event_type, event_month)`;
  `(user_id, event_type, event_date DESC)`;
  `(instagram_account_id, event_month)`;
  `(event_month, limit_subject_type, limit_subject_id)`;
  `(automation_id, event_month)`; `created_at DESC`.
- `usage_reservations`: unique `idempotency_key`;
  `(limit_subject_type, limit_subject_id, month, metric, status)`;
  `(expires_at, status)`.
- `usage_reservation_buckets`: unique
  `(limit_subject_type, limit_subject_id, month, metric)`.
- `monthly_usage`: drop `monthly_usage_user_month_unique`;
  `(user_id, event_month)`;
  `(event_month, limit_subject_type, limit_subject_id)`;
  partial unique over the latter subject tuple when both subject fields exist.
- Source: `backend/server.py:31254-31300,31464-31484`.

### Operations, identity, and administration indexes

- `webhook_processing_failures`: `(status, next_attempt_at)` and
  `first_failed_at DESC`.
- `dashboard_summaries`: unique
  `(user_id, instagramAccountId, month)`; `(user_id, month)`;
  `(instagramAccountId, month)`; `expires_at`.
- `instagram_automation_events`: `(username_key, created_at DESC)`;
  `(stage, created_at DESC)`; `created_at DESC`.
- `user_plans`: unique `user_id`.
- `admin_audit_logs`: `(admin_user_id, created_at DESC)`;
  `(target_user_id, created_at DESC)`; `(action, created_at DESC)`.
- `data_deletion_requests`: `created_at DESC`.
- `user_notification_preferences`: unique `user_id`.
- `user_limit_overrides`: `(user_id, status)`;
  `(user_id, starts_at, ends_at)`; `(status, ends_at)`;
  `(created_by_user_id, created_at DESC)`.
- `admin_members`: unique sparse `user_id`; unique sparse `email`;
  `(role, created_at DESC)`; `disabled_at`.
- Source: `backend/server.py:31301-31596`.

### User and Instagram account indexes

- `users`: unique sparse `google_sub`; sparse, non-unique
  `normalized_email`; sparse `email_verification_token_hash`; unique `id`;
  unique sparse `username`; sparse `password_reset_token_hash`; sparse `status`.
- `instagram_accounts`: unique `id`; unique sparse
  `(userId, instagramAccountId)`; `(isActive, connectionValid, tokenExpiresAt)`;
  `instagramAccountId`; partial unique active/valid `instagramAccountId` when the
  field exists and both booleans are true; `(userId, isActive)`; sparse `igUserId`.
- `instagram_account_trial_claims`: unique
  `(instagram_account_id, plan_trial_identifier)`;
  `(first_claimed_by_user_id, claimed_at DESC)`.
- `oauth_code_consumed`: unique `(provider, code_hash)`.
- Source: `backend/server.py:31502-31648,31708-31723`.

### TTL indexes

| Collection and field | Source behavior |
|---|---|
| `link_click_events.clickedAt` | Default 90 days; `LINK_CLICK_EVENTS_TTL_SECONDS`; clamped to 1-365 days (`backend/server.py:31219-31237`) |
| `webhook_processing_failures.terminal_at` | Default 30 days; `WEBHOOK_DLQ_TERMINAL_TTL_SECONDS`; clamped to 1-365 days; only terminal rows have the field (`backend/server.py:31301-31344`) |
| `comment_dm_sessions.expiresAt` | Default 30 days; clamped to 1-365 days (`backend/server.py:31345-31378`) |
| `instagram_automation_events.created_at` | Default 7 days; `IG_AUTOMATION_EVENTS_TTL_SECONDS`; clamped to 1-90 days; bootstrap uses `collMod` fallback (`backend/server.py:31396-31463`) |
| `oauth_code_consumed.created_at` | 600 seconds (`backend/server.py:31708-31723`) |
| `webhook_inbox.expires_at` | Absolute expiration with `expireAfterSeconds=0` (`backend/runtime_scaling.py:29-31`) |

`tracked_links.expiresAt`, `dashboard_summaries.expires_at`, and reservation
`expires_at` are indexed application expirations, not verified Mongo TTL indexes.
Their PostgreSQL queries must enforce expiration even before cleanup.

PostgreSQL replacement:

1. Every expiring row has typed `expires_at` or retention-basis `timestamptz`.
2. Reads exclude expired rows as part of correctness.
3. A separately scheduled, observable retention job deletes in bounded batches.
4. The job uses an advisory lock to prevent overlap and records metrics without
   recording sensitive payloads.
5. Audit, billing/usage, deletion-request, and deduplication evidence is never
   deleted merely because a nearby operational record has a TTL.

## 7. Target schema strategy

### 7.1 General rules

- Use typed columns for identity, ownership, external IDs, statuses, timestamps,
  retries, locks, idempotency keys, counters, limits, and all indexed predicates.
- Use `jsonb` for variable automation graphs, provider payloads, safe metadata, and
  source-only fields not yet classified.
- Use `timestamptz` in UTC for all instants and `date` or a constrained
  `YYYY-MM` representation for usage periods.
- Use `bigint` counters and explicit non-negative checks.
- Use foreign keys after orphan profiling and quarantine.
- Use check constraints for stable status vocabularies only after profiling proves
  the complete set.
- Do not hide a frequently queried value in JSONB.
- Do not normalize opaque provider payloads into dozens of nullable columns.

### 7.2 Proposed tables

| Target table | Source | Typed critical data | JSONB policy |
|---|---|---|---|
| `app_users` | `users` | source ID, normalized email, username, status, auth/provider identity, verification/reset expiry, created/updated | profile and unclassified legacy fields only; never secrets |
| `instagram_accounts` | `instagram_accounts` | source ID, owner FK, canonical/legacy external IDs, username, active/valid state, refresh status, token expiry, timestamps, token columns | non-secret provider profile metadata |
| `instagram_account_trial_claims` | same | account external ID, plan trial ID, claiming user, claimed time | source-only metadata |
| `automations` | same | source ID, owner/account FKs, name, status, trigger/type, timestamps, counters | `nodes`, `edges`, variable action/trigger configuration |
| `dm_rules` | same | source ID, owner/account FKs, active state, match type, timestamps | keyword/reply configuration where shape varies |
| `comments` | same | owner/account/automation IDs, provider comment/media/author IDs, text where contract requires it, action/reply/DM statuses, retry and lease fields, dedupe keys, timestamps | sanitized provider and decision diagnostics |
| `comment_dm_sessions` | same | owner/account/recipient/rule/media IDs, status, attempts, dedupe key, expiry, timestamps | variable gate/provider state |
| `dm_logs` | same | owner/account/conversation/contact IDs, direction/status, provider IDs, dedupe key, retry fields, timestamps | safe provider response metadata |
| `contacts` | same | source ID, owner/account IDs, provider user ID, username, subscribed/activity timestamps | tags/profile metadata initially |
| `conversations` | same | source ID, owner/account/contact IDs, unread count, last-message fields, timestamps | conversation-level metadata |
| `conversation_messages` | embedded in `conversations` where present | conversation FK, stable ordinal/source ID, direction, text, sent time, status | provider metadata |
| `broadcasts` | same | source ID, owner/account IDs, name/status/schedule, counters, timestamps | audience and message configuration |
| `instagram_media` | `instagram_media_catalog` | account FK/source ID, provider media ID, media type, timestamp | caption/permalink/provider metadata |
| `subscriptions` | same, dormant service | source ID, user FK, plan, status, provider, provider subscription/checkout IDs, period bounds, cancellation fields, grant fields, timestamps | safe provider metadata only |
| `invoices` | same, dormant service | source ID, user/subscription FKs, plan, integer minor-unit amount, ISO currency, status, provider, paid/created times, receipt URL | safe provider metadata only |
| `tracked_links` | same | source ID, owner/account/rule IDs, short code, target URL, expiry, counters, timestamps | source-only metadata |
| `link_click_events` | same | link FK/source ID, owner/account/provider user IDs, clicked time | privacy-reviewed request/referrer metadata |
| `usage_events` | same | source ID, user/account/automation IDs, subject type/ID, event type/date/month, amount, created time | non-secret attribution metadata |
| `usage_reservations` | same | source ID, idempotency key, subject, month, metric, amount, status, expiry, timestamps | safe request metadata |
| `usage_reservation_buckets` | same | subject, month, metric, reserved amount, updated time | none expected |
| `monthly_usage` | same | user/subject, month, metric counters, timestamps | additional counters pending profiling |
| `dashboard_summaries` | same | owner/account/month, expiry, generated time | calculated summary payload |
| `webhook_logs` | `webhook_log` | source/event ID, provider, status, received/processed timestamps | minimized and encrypted/redacted payload only if retention is approved |
| `webhook_inbox_jobs` | `webhook_inbox` | deterministic digest ID, status, attempts, availability, owner/fencing token, lease, expiry, errors, timestamps | encrypted payload remains a dedicated ciphertext column, not JSONB |
| `webhook_processing_failures` | same | source/event ID, status, attempts, next attempt, first/last/terminal times | encrypted or minimized payload/error metadata |
| `instagram_automation_events` | same | source ID, username key, stage, created time | redacted diagnostics |
| `user_plans` | same | user FK, plan, status, effective timestamps | provider-safe plan metadata |
| `user_limit_overrides` | same | user/creator FKs, status, starts/ends, limits, reason, timestamps | variable limit details only if not stable |
| `user_notification_preferences` | same | user FK and typed preference booleans | additional future preferences |
| `admin_members` | same | user FK/email, role, created/disabled times | none expected |
| `admin_audit_logs` | same | actor/target IDs, action, created time | redacted change metadata |
| `data_deletion_requests` | same | source ID, user/provider identity, status, requested/completed times | redacted execution details |
| `oauth_codes_consumed` | `oauth_code_consumed` | provider, code hash, created time | none |

All tables also receive `source_extra jsonb` during migration rehearsal. It is a
temporary loss-prevention mechanism, not a permanent substitute for schema design.
Its removal requires a field-frequency report and explicit approval.

### 7.3 Key constraints and indexes

- Case-folded lookup index on `app_users.normalized_email`. Source currently makes
  normalized email non-unique, so PostgreSQL must not add uniqueness until duplicate
  profiling and identity policy approval.
- Partial unique indexes for nullable dedupe keys:
  `(user_id, instagram_account_id, external_comment_id)`,
  `(user_id, instagram_account_id, dedup_key)`, and `short_code`.
- Unique `(instagram_account_id, media_id)`.
- Partial unique canonical Instagram account ID for active, valid rows.
- Unique `(provider, code_hash)`.
- Unique usage reservation `idempotency_key`.
- Unique usage bucket `(subject_type, subject_id, month, metric)`.
- Unique trial claim `(instagram_account_id, plan_trial_identifier)`.
- Unique subscription source ID; indexes for `(user_id, status, created_at DESC)`,
  provider subscription ID, and provider checkout-session ID. Provider IDs may need
  partial uniqueness after profiling.
- Unique invoice source ID; indexes for `(user_id, created_at DESC)` and
  `subscription_id`.
- Queue indexes must match eligibility predicates and deterministic ordering.
- Every foreign key receives a supporting index.

Foreign-key deletion behavior is conservative:

- audit, usage, deletion, and abuse-prevention records retain immutable subject IDs
  even when a user is anonymized;
- operational records are soft-disabled or explicitly deleted by the application's
  deletion workflow;
- broad `ON DELETE CASCADE` is not used until legal and product retention behavior
  is verified.

## 8. Atomicity and concurrency

### 8.1 Webhook inbox

Verified behavior in `backend/runtime_scaling.py:33-83`:

- canonical payload bytes are SHA-256 hashed into `_id`;
- enqueue uses `$setOnInsert` with `upsert=True`, making duplicate delivery
  idempotent;
- a worker atomically claims the earliest eligible pending or lease-expired row;
- claim sets a random owner, a 180-second lease, processing state, and increments
  attempts;
- completion/failure updates require both `_id` and owner;
- retries stop after five attempts;
- successful jobs retain seven days and terminal failures retain thirty days.

PostgreSQL contract:

1. Enqueue uses `INSERT ... ON CONFLICT (event_digest) DO NOTHING`.
2. Claim runs in a short transaction using:
   `SELECT ... FOR UPDATE SKIP LOCKED`, an explicit eligibility predicate, and
   deterministic `ORDER BY available_at, event_digest`.
3. Claim stores a new unguessable fencing token, lease expiry, state, and incremented
   attempt count before commit.
4. External processing occurs outside the claim transaction.
5. Finalization compares both job ID and fencing token.
6. Expired leases are reclaimable; stale workers cannot finalize after fencing.
7. Retention scheduling preserves current seven-day success and thirty-day terminal
   behavior unless separately approved.

### 8.2 Comment action queue

`comments` has queue fields and two atomic `find_one_and_update` call sites, with an
index over `action_status`, `next_retry_at`, and `queue_lock_until`
(`backend/server.py:31126-31138`). PostgreSQL must use the same short-transaction
claim/fence/finalize pattern. The acceptance suite must cover a crash after an
external Instagram send because a database transaction cannot roll back an external
side effect.

### 8.3 Usage reservations

Reservations and reservation buckets rely on unique idempotency and atomic
conditional updates. In PostgreSQL, reservation creation plus bucket increment is
one transaction. Commit or release plus corresponding bucket/monthly-usage changes
is also one transaction, with a state precondition preventing double application.
The source behavior is concentrated in `backend/server.py:1330-1704`; indexes are
declared at `backend/server.py:31282-31300`.

### 8.4 Other race-sensitive writes

OAuth replay markers, comment and DM dedupe, media upserts, trial claims, account
canonicalization, and tracked short codes must use PostgreSQL unique constraints
plus `INSERT ... ON CONFLICT` or conditional updates. Existing check-then-write
application sequences are not sufficient on their own.

The dormant billing webhook flow reads a subscription by either provider
subscription ID, checkout-session ID, or pending user record, then updates the
subscription and may insert an invoice (`backend/billing/service.py:192-248`).
Activation/update and invoice insertion must be one PostgreSQL transaction, with a
provider event or invoice uniqueness key added before the service is made reachable.
The current source claims webhook idempotency but does not persist a provider event
ID and can insert duplicate invoices if the same `payment.succeeded` event is
replayed. This is a source blocker, not behavior to reproduce.

## 9. Startup migrations discovered in source

The Mongo bootstrap does more than create indexes:

- drops and replaces legacy comment and DM dedupe indexes;
- changes monthly usage uniqueness;
- migrates Instagram account aliases/scopes;
- applies automation defaults and timestamp repairs;
- repairs legacy reply-success proof;
- changes the automation-events TTL with `collMod`.

Relevant source is `backend/server.py:31072-31724`.

These transformations become explicit, versioned, repeatable data-migration steps.
They must not run as best-effort startup work in PostgreSQL. Each step needs:

- a precondition query;
- an idempotent transformation;
- affected-row and rejected-row reporting;
- postcondition assertions;
- a documented rollback or proof that the step is additive.

## 10. Import and normalization contract

This section defines a later import; it does not authorize one.

1. Obtain a consistent, owner-authorized snapshot while Railway remains untouched.
2. Capture collection counts, source `listIndexes`, min/max timestamps, field/type
   frequencies, duplicate candidates, and orphan candidates.
3. Preserve original IDs, timestamps, ciphertext, status values, and unknown fields.
4. Normalize all timestamps to UTC while preserving the original value in rejected
   or ambiguous cases.
5. Normalize `userId`/`user_id` and Instagram account aliases using documented
   precedence rules. Conflicting non-empty aliases are quarantined.
6. Preserve missing-versus-null evidence in `source_extra` until approved.
7. Load parents before dependents:
   users and admin/plan/preferences; accounts and trial claims; automations/rules/
   media; contacts/conversations/messages; comments/sessions/DM logs;
   subscriptions then invoices; links/clicks; usage tables;
   webhooks/events/summaries; audit/deletion/OAuth markers.
8. Load into unconstrained staging tables first.
9. Quarantine malformed, duplicate, conflicting, and orphan rows; never silently
   coerce or discard them.
10. Reconcile staging, then load canonical tables and enable constraints/indexes.

No secret value may appear in rejects or migration reports. Reconciliation hashes
must omit or HMAC sensitive fields with separately controlled migration-only key
material.

## 11. Acceptance tests and approval gates

### 11.1 Static specification checks

- Every production `db.<collection>` reference maps to exactly one target table or
  a documented embedded-to-child-table transformation.
- Every fixed `self.db.<collection>` reference in service modules receives the same
  treatment, even when the service is currently unreachable from the entry point.
- Every `aggregate()` call has an equivalent SQL query and fixtures.
- Every Mongo query/update operator has a documented SQL or JSONB translation.
- Every source bootstrap index is mapped or explicitly rejected with rationale.
- Every TTL has an expiration-aware read rule and retention-job rule.
- Every `find_one_and_update` flow is classified as claim, idempotency, counter, or
  compare-and-set behavior and receives a transaction design.

### 11.2 Data profiling gate

Before schema implementation is finalized, an authorized read-only profile must
report for every collection:

- document count and storage size;
- every observed field and BSON type frequency;
- null, missing, empty-string, and default-value frequencies;
- maximum string, array, and document sizes;
- duplicate candidates for every proposed unique constraint;
- orphan relationships and conflicting alias values;
- status and enum vocabularies;
- earliest/latest timestamps and invalid dates;
- actual indexes and options from `listIndexes`;
- token representation classification without exposing values.

No `NOT NULL`, enum/check, foreign key, or new unique constraint is final until this
gate passes.

### 11.3 Rehearsal reconciliation

For an authorized non-production rehearsal:

- source and target counts match per collection/table transformation;
- dormant `subscriptions` and `invoices` are explicitly proven absent or reconciled;
- embedded conversation message counts match generated child rows;
- keyed hashes of canonicalized non-secret fields match;
- status, null, month, ownership, and account distributions match;
- all foreign keys resolve or every quarantined row has an approved disposition;
- usage totals reconcile by user, subject, metric, and month;
- subscription/invoice totals reconcile by user, provider, status, currency, and
  period, with receipt/provider identifiers compared through non-public keyed
  digests;
- dashboard source totals and SQL recomputation match;
- ciphertext bytes match and authorized decryptability checks pass;
- no secret appears in logs, reports, or rejects.

Required threshold: zero unexplained loss, zero unexplained duplicate collapse, and
zero unexplained reconciliation mismatch.

### 11.4 Behavioral parity

Run existing backend tests after replacing Mongo fakes with PostgreSQL-aware test
fixtures, then add:

- fixtures for all three aggregation translations;
- null-versus-missing and legacy-alias fixtures;
- comment/DM/media/trial/OAuth/idempotency duplicate races;
- at least two concurrent webhook workers proving one claim per job;
- stale worker fencing and expired lease recovery;
- crash-after-external-send retry behavior;
- usage reservation create/commit/release races and month boundaries;
- duplicate billing webhook delivery proving at most one invoice per provider
  payment event after the required idempotency design is implemented;
- retention tests for every TTL and application expiration;
- account isolation and IDOR checks across all target relations;
- user deletion/anonymization with audit and usage retention;
- API response-contract comparisons for dashboard, admin summaries, comments,
  automations, contacts, conversations, broadcasts, account settings, and usage.

### 11.5 Operational readiness gate

Before any future production cutover:

- PostgreSQL backups, point-in-time recovery, and restore are tested;
- connection pool and worker concurrency limits are load-tested;
- queue depth, lease expiry, retry, terminal failure, and retention metrics exist;
- the cron and webhook switch prevents Railway/Replit split-brain processing;
- a final snapshot/delta, maintenance window, rollback window, and ownership are
  approved;
- external Instagram sends are recognized as irreversible side effects.

## 12. Open blockers

1. Actual production field/type/cardinality profiles are unavailable.
2. Actual production indexes may differ from source bootstrap.
3. Historical token encryption/plaintext representation is unverified.
4. Duplicate and orphan rates are unknown.
5. Conversation embedded-message shape and maximum size need profiling.
6. Queue throughput, lease-expiry frequency, and concurrent worker count are
   unknown.
7. Required retention for raw webhook, DM, comment, audit, and click data requires
   product/legal confirmation.
8. The correct deterministic ordering for the currently unordered limited comments
   aggregation requires product approval.
9. The intended uniqueness policy for normalized email is unresolved; source
   deliberately indexes it non-uniquely.
10. PostgreSQL-only write rollback cannot be considered safe once external actions
    have occurred without an explicit idempotent replay/reconciliation design.
11. `SubscriptionService` is currently unreachable from the FastAPI entry point,
    but the existence, size, and retention requirements of historical
    `subscriptions` and `invoices` collections are unknown.
12. Billing webhook events have no verified persisted provider-event idempotency
    key; enabling the dormant service without one can create duplicate invoices.

Resolution of these blockers belongs to a reviewed profiling and implementation
phase. This specification alone must not trigger database creation, data access,
runtime changes, deployment, or Railway changes.