# Production Checklist

## Before Changes

Run and record:

```powershell
git status --short
git rev-parse HEAD
git rev-parse origin/master
Invoke-WebRequest -Uri "https://backend-production-a1a3.up.railway.app/api/version" -UseBasicParsing
Invoke-WebRequest -Uri "https://backend-production-a1a3.up.railway.app/api/" -UseBasicParsing
```

Record:

- local HEAD
- origin/master
- production build_sha
- backend health
- working tree status
- whether Billing is untouched

## Tests Before Push

Backend:

```powershell
python -m py_compile backend/server.py
python -m pytest backend/tests -x --tb=short -q
```

Frontend, if frontend changed:

```powershell
cd frontend
npm.cmd test -- --watchAll=false
npm.cmd run build
```

Docs-only:

```powershell
git diff --check
```

## After Deploy

Verify:

- production `/api/version` build_sha matches commit
- `/api/` returns ok
- protected admin endpoints return 403 unauthenticated
- frontend `/login` returns 200
- frontend `/signup` returns 200
- frontend `/app` returns 200
- frontend `/app/automations` returns 200
- frontend `/app/dm-automation` returns 200
- frontend `/app/settings` returns 200
- frontend `/app/billing` returns 200
- `/app/admin/instagram-diagnostics` does not expose diagnostics UI
- Billing untouched
- working tree clean

## Report Format

Report:

- commit SHA
- origin/master
- production build_sha
- backend health
- tests run
- frontend bundle hash if frontend changed
- routes checked
- remaining blockers
- Billing status
