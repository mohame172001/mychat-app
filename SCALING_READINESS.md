# Scaling rollout and remaining release gates

## Implemented, opt-in

- `MYCHAT_PROCESS_ROLE=combined` preserves existing behavior by default.
- `api` accepts HTTP without background automation loops.
- `worker` runs the existing automation queue plus the durable webhook inbox.
- `scheduler` runs periodic maintenance and index bootstrap. Run exactly one
  scheduler replica; these legacy loops do not have distributed leadership.
- Docker starts one Uvicorn process. Do not multiply combined/scheduler processes.
- `WEBHOOK_INBOX_ENABLED=1` persists encrypted payloads before returning success.
  Database failures return 503 so the sender can retry. API/worker roles require it.
- Workers atomically claim leases, retry failures up to five attempts, and retain
  terminal failures for 30 days. Completed payloads are removed; deduplication
  markers expire after seven days. Delivery is at least once, not exactly once.
- `REDIS_URL` enables atomic shared fixed-window limits. Redis outage fails closed
  with 503 on limited routes. Without Redis the legacy local limiter remains.

## Safe activation order

1. Back up MongoDB and perform a restore test before migrating existing data.
   Provision authenticated managed MongoDB with sufficient storage and monitoring.
2. Deploy the code with the default combined role and inbox disabled; verify health.
3. Provision private authenticated Redis and configure the same REDIS_URL on all
   API instances. Verify successful login and rate-limit enforcement.
4. Generate one Fernet key and set WEBHOOK_INBOX_ENCRYPTION_KEY on all roles. Enable
   the inbox on the combined instance first. Verify a real signed comment is stored,
   processed and completed. Never rotate the key with pending encrypted payloads.
5. Start worker instances with inbox enabled and identical database/configuration.
   Atomic leases permit overlap during rollout.
6. Switch the backend to api, then start exactly one scheduler. Keep scheduler and
   worker endpoints private. Do not run scheduler and combined simultaneously.
7. Verify queued work after restarting a worker, action duplicate protection, token
   refresh, scheduler operation and database indexes before increasing replicas.

## Still required before a capacity claim

- Real MongoDB concurrency/restart integration tests, not only mocked unit tests.
- Backups with verified recovery, authenticated database, storage alerts.
- Representative tenant-isolated load tests with posts, comments and rules, not
  just anonymous health requests. No real Instagram sends in synthetic load tests.
- Measure API p95 latency, error rate, inbox oldest pending age, failed inbox count,
  Mongo connections/CPU/storage, Redis availability and Meta throttling.
- Account for third-party API quotas and per-account message policy limits.
- Billing provider, verified payment webhooks and plan enforcement remain separate
  unfinished business launch work; this change does not enable payments.

Neither 1,000 nor 10,000 users is certified by these code changes alone.
