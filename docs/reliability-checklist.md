# Reliability Checklist

- Bounded summaries: dashboard, automations, comments, admin users, diagnostics, and reconciliation must stay paginated or compact.
- Queue safety: poison jobs must reach final state; stale locks and stale reservations require operator monitoring.
- Deploy safety: verify backend health, protected 403s, public pages, and frontend asset-manifest all chunks.
- Performance smoke is required before billing UI launch.
- Railway scaling and backup status require operator confirmation in production dashboards.
