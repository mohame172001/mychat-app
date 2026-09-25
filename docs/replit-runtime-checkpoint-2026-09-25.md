# Replit runtime checkpoint

- Owner requests Replit-only hosting and PostgreSQL, keeping Railway data safe
  until migration is verified. Do not enlarge or delete MongoDB.
- Replit workspace HEAD was `4cb0124`; clean before this change.
- PostgreSQL direct `select 1` succeeded in Replit; local `/api/health` returned 200.
- Real PostgreSQL document tests: 7 passed after explicitly enabling DB tests.
- Added bundled dependency path resolution before importing dotenv.
- GitHub fix: `f172b50` on `codex/replit-runtime-readiness`.
- Cherry-picked onto Replit as `4b56c42`; 8 startup tests passed there.
- Public domain `https://mychat-app.replit.app` returned 502 before publishing.
- Requested Republish using existing 0.5 vCPU / 2 GiB settings. No plan change,
  new add-ons, auto-reload, or development-data overwrite was selected.
- Publishing is still being verified. Do not report migration complete.
- Railway user data, encryption keys, Meta callbacks and live services were not
  modified. Data migration and real Instagram testing on Replit remain pending.
- Local earlier scaling changes remain uncommitted in `D:/mychat-app`; this
  runtime fix lives separately in `D:/mychat-replit-readiness`.
