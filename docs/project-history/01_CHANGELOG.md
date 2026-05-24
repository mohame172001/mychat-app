# Project Changelog

Important commits only. Keep this table updated after every meaningful commit.

| Date | Commit | Area | Summary | Behavior changed? | Tests | Deploy status |
|---|---|---|---|---|---|---|
| 2026-05-24 | `54fb527` | Instagram rule matching | Legacy general any-post comment rules now match when top-level `trigger` is missing/`Manual` but `post_scope=any` or `nodes[].data.trigger=comment:any` identifies the rule as comment-capable. Account scoping, dedupe, HMAC, rate limits, and post-specific strictness are unchanged. | Yes | `py_compile backend/server.py`; `test_multi_account_automation_routing.py` 96 passed; backend 627 passed | Deployed via `a488bbb` (docs commit); production build_sha `a488bbb3179c`; not known-good until live retest |
| 2026-05-24 | `e109f6e` | Instagram support diagnostics | Stop-point summary now prefers latest real external/commenter event over bot-owned public reply events, so `bot_own_reply` no longer hides the actual automation result. | No automation execution change | `py_compile backend/server.py`; `test_multi_account_automation_routing.py` 92 passed; backend 623 passed | Local, deploy required if accepted |
| 2026-05-24 | `d30f04e` | Instagram OAuth, frontend cache | Preserve existing IG connection on reused OAuth code; stop persisting failed media payloads; hide internal cache error from media picker. | Yes | Backend 622, frontend 184, build passed | Deployed, build_sha `d30f04e15582` |
| 2026-05-24 | `8ebe8c6` | OAuth, startup | Added OAuth code idempotency guard and moved heavy index/bootstrap work off startup healthcheck path. | Yes | Backend 621 reported in commit | Deployed before `d30f04e` |
| 2026-05-23 | `d547203` | Instagram token flow, media cache | Restored event-scoped token context for automation flow and refused to persist `ok=false` media snapshots. | Yes | Backend 614, frontend cache 17, build passed | Deployed before `8ebe8c6` |
| 2026-05-23 | `3500dcd` | Instagram token sends | Preferred stored account token before Graph sends. Later considered risky for Account 1 because stored account token can be stale. | Yes | Backend 614 | Deployed, superseded |
| 2026-05-23 | `5f5a34a` | Instagram token refresh | Added refresh retry after Graph code 190 for DM, public replies, and media listing. | Yes | Backend 612 | Deployed, superseded |
| 2026-05-23 | `582f644` | Comment-DM dedupe | Added completed-flow reopen TTL so old completed sessions do not block fresh live tests forever. | Yes | Backend 610 | Deployed |
| 2026-05-23 | `23e9f68` | Webhook security | Hardened webhook secret source resolution. | Yes | Focused webhook/security tests | Deployed |
| 2026-05-23 | `2141376` | Webhook security | Accepted Facebook app secret aliases for webhook verification. | Yes | Focused webhook tests | Deployed |
| 2026-05-23 | `129f420` | Quick reply fallback | Added automatic next-step DM delivery when click webhook is missing. | Yes | Backend focused tests | Deployed before later fixes |
| 2026-05-23 | `a770f7e` | Diagnostics | Classified post-opening-DM stop reasons deterministically. | Yes | Backend focused tests | Deployed before later fixes |
| 2026-05-23 | `868dc33` | Admin support | Added automation stop-point summary to protected admin console. | Yes | Backend/frontend focused tests | Deployed before diagnostics UI removal |
| 2026-05-23 | `deb5f21` | Support tooling | Added automation stop-point CLI for incident triage. | No user behavior | Backend focused tests | Local/support |
| 2026-05-23 | `c67dc96` | Support endpoint | Recorded automation stop points for comment flows. | Yes | Backend focused tests | Deployed |
| 2026-05-23 | `3dbef2c` | Quick replies | Continued after first quick reply for external users. | Yes | Backend focused tests | Deployed, later live issues remained |
| 2026-05-23 | `9e2363d` | External users | Continued comment-DM flows for external users. | Yes | Backend focused tests | Deployed, superseded |
| 2026-05-23 | `e2be355` | Rule matching | Ensured account-scoped and post-scoped automations share execution pipeline. | Yes | Backend focused tests | Deployed, superseded |
| 2026-05-23 | `0dfaa72` | Multi-account automation | Made comment automations fully multi-account instant. | Yes | Backend focused tests | Deployed, superseded |
| 2026-05-23 | `6e11993` | Frontend refactor | Centralized frontend routes and API clients. | No intended behavior change | Frontend tests/build | Deployed |
| 2026-05-23 | `d7b448b` | Backend refactor | Extracted Instagram account resolver. | No intended behavior change | Backend tests | Deployed |
| 2026-05-23 | `8fdc917` | Backend refactor | Extracted Instagram comment-DM flow service. | No intended behavior change | Backend tests | Deployed |
| 2026-05-23 | `422697b` | Backend refactor | Extracted Instagram rule normalizer. | No intended behavior change | Backend tests | Deployed |
| 2026-05-23 | `d29ec7d` | Frontend admin UI | Removed/hid diagnostics UI and added refactor plan. | Yes | Frontend tests/build | Deployed |
| 2026-05-23 | `7436984` | Multi-account routing | Isolated multi-account automation routing. | Yes | Backend focused tests | Deployed, superseded |
| 2026-05-23 | `f65cff7` | Idempotency | Enforced idempotent multi-account flow state. | Yes | Backend focused tests | Deployed, superseded |
