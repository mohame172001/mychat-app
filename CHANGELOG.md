# Changelog

All notable changes to mychat land here. Each entry is one production
deploy; commit shas link to the exact code in `master`.

## Phase 2.19 — i18n, RTL, hardening (May 2026)

### Highlights

- **Full Arabic UI** with professional MSA copy across every page,
  toast, error code, placeholder, aria-label, and library wrapper.
  Locale toggle persists to `localStorage` and flips `<html dir>` so
  Tailwind logical utilities (`ms-`, `me-`, `ps-`, `pe-`, `start-`,
  `end-`, `text-start`, `text-end`) mirror the entire layout
  automatically.
- **Production hardening** — JWT secret length gate, 2 MB request
  body cap, per-request X-Request-Id with sanitized client values,
  Pydantic `Field(min_length, max_length, ge, le)` on every input
  model, expanded `FORBIDDEN_PERSIST_KEYS` in the frontend cache,
  Meta-only OAuth-redirect allowlist.
- **Accessibility** — skip-link, aria-labels on icon-only buttons,
  aria-label on every `<nav>`, `prefers-reduced-motion` media query,
  every `<label htmlFor>` matched to an input id.
- **SEO + share-ability** — OpenGraph + Twitter cards, canonical URL,
  robots.txt, sitemap.xml, inline SVG favicon, bilingual `<noscript>`.
- **Resilience** — top-level `RootErrorBoundary`, `OfflineBanner`
  watching `navigator.onLine`, dev-log gating behind `?debug=1`.

### Backend changes

| sha | summary |
|---|---|
| `9e41e6d` | JWT_SECRET ≥ 32 chars in prod, 2 MB body cap, Field caps on signup/login/forgot/reset |
| `030a535` | Tighten Contact/Broadcast/Admin* field caps |
| `b8a0715` | Cap every text field on AutomationIn (names, replies, button text, follow-gate copy) |
| `44df37d` | X-Request-Id middleware + `/api/version` endpoint |
| `899738e` | Tests for X-Request-Id whitelist, `/api/version`, field caps |

### Frontend changes (i18n)

| sha | summary |
|---|---|
| `92c6777` | Arabic copy for AdminConsole headings + 27 toasts; bilingual `dmFailureReasons` |
| `ff4e336` | Profile save toasts + 4-code username validation error map |
| `a98691b` | Translate ResetPassword, GoogleSignIn, AdminConsole, full UI primitives RTL sweep |
| `b4c039e` | Finish AdminConsole tables, badges, Billing PlanCard, DM diagnostics |
| `f242b6e` | Automation wizard + list translated to professional MSA |
| `9d978db` | Contacts, FlowBuilder, StatusPage, SystemHealth, legal pages |

### Frontend changes (security/perf/ux)

| sha | summary |
|---|---|
| `c054a57` | Meta-host allowlist before OAuth redirect (defense in depth) |
| `a351a5c` | Expand `FORBIDDEN_PERSIST_KEYS` in apiCache |
| `b0bcce1`, `45b8dc2`, `5d45ed3` | `loading=lazy` + `referrerPolicy=no-referrer` on every `<img>` |
| `787e8ed` | Client-side form validation matching backend Pydantic caps |
| `9b1eb80` | Devtools chatter gated behind `?debug=1`; OfflineBanner |
| `78b54ae` | Skip-link, aria-labels, prefers-reduced-motion |
| `b9338ac` | OG/Twitter cards, robots.txt, sitemap.xml, RootErrorBoundary |
| `54ba1a9` | Password eye icon position fixed, Switch enlarged + RTL flip |
| `2dfcaf6` | Richer empty state on Contacts page |

### Docs

- New `SECURITY.md` with vulnerability reporting flow + inventory of
  hardening controls
- Refreshed `backend/.env.example` documenting every variable

## Earlier phases

See git history for Phase 2.18Z (status page, accessibility polish),
Phase 2.18Y (DM failure reasons, cold-start fixes), and earlier
admin / billing / auth work.
