# Rollback Playbook

## Identify Current State

Run:

```powershell
git status --short
git rev-parse HEAD
git rev-parse origin/master
git log --oneline -10
Invoke-WebRequest -Uri "https://backend-production-a1a3.up.railway.app/api/version" -UseBasicParsing
Invoke-WebRequest -Uri "https://backend-production-a1a3.up.railway.app/api/" -UseBasicParsing
```

Record:

- local HEAD
- origin/master
- production build_sha
- backend health
- working tree status

## When Rollback Is Appropriate

Rollback may be appropriate when:

- A new commit clearly introduced a production outage.
- The bad commit is isolated and understood.
- Reverting it does not remove security fixes required by production.
- Forward fixing is riskier or slower than reverting.

## When Rollback Is Not Appropriate

Do not rollback when:

- The target commit removes HMAC hardening, dedupe, rate limits, or token safety.
- The root cause is an external Meta token/permission issue.
- The problem can be fixed with a smaller forward patch.
- The rollback would reintroduce `/app/admin/instagram-diagnostics`.

## Safe Git Revert Process

Prefer revert over reset:

```powershell
git status --short
git revert <bad_commit_sha>
git diff --check
python -m py_compile backend/server.py
python -m pytest backend/tests -x --tb=short -q
cd frontend
npm.cmd test -- --watchAll=false
npm.cmd run build
```

If frontend was not touched, frontend tests/build may be documented as not required, but route checks still apply after deploy.

## Push And Verify

```powershell
git push origin master
```

After Railway deploy:

- `/api/version` build_sha is the new revert commit.
- `/api/` returns ok.
- Protected admin support endpoints return 403 unauthenticated.
- Frontend key routes return 200.
- Diagnostics UI is not exposed.
- Billing remains untouched.

## Report After Rollback

Report:

- reverted commit
- new commit SHA
- tests run
- production build_sha
- health result
- routes checked
- remaining risk
- Billing status
