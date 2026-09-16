# Replit pause checkpoint - 2026-09-16

Paused at the owner's request. Do not publish or incur charges without approval.

- Local source: `D:\mychat-app`, branch `master`.
- Latest pushed commit: `8689e11`; merged into the Replit workspace.
- Replit project: https://replit.com/@mmmohame172001/mychat-app
- Development preview: https://171e7d66-42c0-48a4-a948-e0f7902ab54d-00-2a0zzy348ae14.reed.replit.dev/
- PostgreSQL runtime is active through `backend/replit_start.py` and the
  `mychat_runtime` schema. No Mongo service is needed for this runtime.
- Current provider credentials were copied from Railway backend into Replit
  Secrets: IG_APP_ID, IG_APP_SECRET, META_APP_ID, META_APP_SECRET,
  META_VERIFY_TOKEN. Existing SESSION_SECRET was preserved. Never print values.
- Railway CLI login was refreshed successfully. No Railway data was migrated
  or deleted. The owner chose a fresh Replit database.
- Latest real Replit smoke passed: startup, health, signup, login, session,
  empty automations, auth gate, frontend route delivery, webhook challenge
  success/failure, configured Instagram OAuth URL/state/callback and no secret
  disclosure. No Instagram provider request was made by that test.
- Smoke log: `/tmp/mychat-runtime-smoke.log` in Replit.
- Preview was started from Shell with `python backend/replit_start.py`, with
  output at `/tmp/mychat-preview.log`, then resumed as background job 1.
  It may sleep/stop with the development workspace; verify before restarting.
- Earlier build and 7 PostgreSQL adapter / 49 regression / 3 startup tests passed.
- Public deployment is still UNPUBLISHED. Smallest displayed Reserved VM is
  0.5 vCPU / 2 GiB, $15/month ($0.0208/hour), excluding other metered resources.
  A cost approval question was sent but not answered before pause. No Publish
  action or paid add-on was activated. Existing in-process workers need an
  always-running deployment, not an unverified autoscale configuration.
- Next: obtain cost approval, verify production database/domain configuration,
  publish, update Meta callback/webhook URLs for the final public domain, then
  test real Instagram connection and one comment-to-action trace.
- Privacy hosting copy is now corrected locally in Arabic and English:
  Replit/PostgreSQL for the new deployment, with legacy Railway retention
  disclosed rather than claiming old records have already been removed.
  This UI change is not yet pushed or built on Replit. Batch its push with
  the next approved deployment step to avoid unnecessary Railway auto-builds.
- Validation on 2026-09-15: local `frontend/node_modules/.bin/craco.cmd build`
  compiled successfully; `git diff --check` passed. No Replit Agent, public
  publishing, provider calls or infrastructure changes were performed this turn.
- Leave the preexisting generated change in `frontend/src/buildInfo.generated.js`
  untouched. Preserve Replit's bundle and old database backup schemas.

The 2026-09-15 checkpoint and privacy update are saved in local commit `34a91ab`.
On 2026-09-16, the Replit launcher was corrected to force APP_ENV=production
when REPLIT_DEPLOYMENT=1, even if development settings were inherited.
Six startup tests passed, including published-domain selection, rejection of
preview-domain fallback and preservation of explicitly configured origins.
`git diff --check` passed. This fix is also local only, not pushed or deployed.
No browser, Replit Agent, paid publishing or infrastructure changes were used.
Resume from this file, check local git status/log, then push when needed.
