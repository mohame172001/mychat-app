"""Phase 2.8: custom allowances / limit overrides.

Pure logic for applying user_limit_overrides on top of base plan limits.
Database access lives in server.py; this module provides:

  - canonical override-type constants
  - filter_active_overrides(rows, now) — keep only currently-effective rows
  - compute_effective(base_limits, overrides) — merge into effective limits
  - explain_effective(base_limits, overrides) — per-metric explanation

Design choices:
  - additive_allowance: extras add on top of base limit (or override if any).
  - limit_override: replaces the base limit while active. If multiple
    active limit_overrides target the same metric, the HIGHEST (most
    permissive) wins so admins can layer grants without surprise downgrades.
  - trial_grant: same shape as limit_override (replace) plus optional
    extras, scoped to a starts_at/ends_at window.
  - max_instagram_accounts and max_active_automations are quota counts,
    not monthly counters. Overrides apply via limit_override only
    (additive doesn't make sense for hard caps).
"""
from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List, Optional


OVERRIDE_TYPE_ADDITIVE = 'additive_allowance'
OVERRIDE_TYPE_LIMIT = 'limit_override'
OVERRIDE_TYPE_TRIAL = 'trial_grant'
VALID_OVERRIDE_TYPES = frozenset((
    OVERRIDE_TYPE_ADDITIVE,
    OVERRIDE_TYPE_LIMIT,
    OVERRIDE_TYPE_TRIAL,
))

STATUS_ACTIVE = 'active'
STATUS_REVOKED = 'revoked'
STATUS_EXPIRED = 'expired'

# Map of additive metric keys to the corresponding plan limit key. Used
# by compute_effective() to decide which base limit to add the extra to.
ADDITIVE_KEYS = {
    'comments_processed_extra': 'monthly_comments_processed_limit',
    'public_replies_sent_extra': 'monthly_public_replies_sent_limit',
    'dms_sent_extra': 'monthly_dms_sent_limit',
    'links_clicked_extra': 'monthly_links_clicked_limit',
}

# Override metric keys that REPLACE a base plan key.
LIMIT_OVERRIDE_KEYS = {
    'monthly_comments_processed_limit_override': 'monthly_comments_processed_limit',
    'monthly_public_replies_sent_limit_override': 'monthly_public_replies_sent_limit',
    'monthly_dms_sent_limit_override': 'monthly_dms_sent_limit',
    'monthly_links_clicked_limit_override': 'monthly_links_clicked_limit',
    'max_instagram_accounts_override': 'max_instagram_accounts',
    'max_active_automations_override': 'max_active_automations',
}

ALL_METRIC_KEYS = (
    'monthly_comments_processed_limit',
    'monthly_public_replies_sent_limit',
    'monthly_dms_sent_limit',
    'monthly_links_clicked_limit',
    'max_instagram_accounts',
    'max_active_automations',
)


def is_valid_override_type(value) -> bool:
    return isinstance(value, str) and value in VALID_OVERRIDE_TYPES


def _to_dt(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            from datetime import datetime as _dt
            cleaned = value.replace('Z', '+00:00') if value.endswith('Z') else value
            parsed = _dt.fromisoformat(cleaned)
            if parsed.tzinfo is not None:
                parsed = parsed.replace(tzinfo=None)
            return parsed
        except Exception:
            return None
    return None


def is_override_active(row: dict, now: Optional[datetime] = None) -> bool:
    """A row is active when status='active' AND now is inside [starts_at, ends_at)
    (open ends_at means 'no end')."""
    if not isinstance(row, dict):
        return False
    if row.get('status') != STATUS_ACTIVE:
        return False
    now_dt = now or datetime.utcnow()
    starts = _to_dt(row.get('starts_at'))
    ends = _to_dt(row.get('ends_at'))
    if starts and now_dt < starts:
        return False
    if ends and now_dt >= ends:
        return False
    return True


def filter_active_overrides(rows: Iterable[dict],
                            now: Optional[datetime] = None) -> List[dict]:
    return [r for r in (rows or []) if is_override_active(r, now)]


def compute_effective(base_limits: Dict, overrides: Iterable[dict]) -> Dict:
    """Return effective_limits = base merged with active additive + override.

    Rules:
      - limit_override / trial_grant set_limit values: take the MAX across
        active rows for the same metric (most permissive wins).
      - additive_allowance values: SUM extras across active rows for the
        same metric, then add to whichever the override pipeline picked
        as the base for that metric (override-or-base).
      - None on the base means 'unlimited'; an override that sets a
        finite cap on top of unlimited collapses to that cap.
        An additive on top of unlimited stays unlimited (cannot extend
        what is already infinite).
    """
    base_limits = dict(base_limits or {})
    rows = list(overrides or [])
    eff = {key: base_limits.get(key) for key in ALL_METRIC_KEYS}

    # Pass 1: limit_override / trial_grant replace, MAX wins.
    for row in rows:
        if row.get('type') not in (OVERRIDE_TYPE_LIMIT, OVERRIDE_TYPE_TRIAL):
            continue
        metrics = (row.get('metrics') or {}) if isinstance(row.get('metrics'), dict) else {}
        for ov_key, base_key in LIMIT_OVERRIDE_KEYS.items():
            if ov_key not in metrics:
                continue
            new_val = metrics.get(ov_key)
            if new_val is None:
                continue
            current = eff.get(base_key)
            if current is None:
                # base is unlimited; a finite override would be a
                # *downgrade*. Refuse to make limits stricter than base
                # to keep the 'most permissive wins' rule consistent.
                continue
            try:
                new_int = int(new_val)
            except (TypeError, ValueError):
                continue
            eff[base_key] = max(int(current), new_int)

    # Pass 2: additive_allowance / trial_grant extras add on top of eff.
    additive_sums: Dict[str, int] = {k: 0 for k in ADDITIVE_KEYS}
    for row in rows:
        if row.get('type') not in (OVERRIDE_TYPE_ADDITIVE, OVERRIDE_TYPE_TRIAL):
            continue
        metrics = (row.get('metrics') or {}) if isinstance(row.get('metrics'), dict) else {}
        for k in ADDITIVE_KEYS:
            v = metrics.get(k)
            if v is None:
                continue
            try:
                additive_sums[k] += max(0, int(v))
            except (TypeError, ValueError):
                pass

    for add_key, base_key in ADDITIVE_KEYS.items():
        extra = additive_sums.get(add_key, 0)
        if extra <= 0:
            continue
        current = eff.get(base_key)
        if current is None:
            # already unlimited; additive is a no-op.
            continue
        eff[base_key] = int(current) + int(extra)

    return eff


def explain_effective(base_limits: Dict, overrides: Iterable[dict]) -> Dict:
    """Per-metric breakdown showing base, additive, override, effective.

    Returns:
        {metric_key: {base, additive_extra, override_value, effective}}
    """
    rows = list(overrides or [])
    base = dict(base_limits or {})
    eff = compute_effective(base, rows)

    additive_sums: Dict[str, int] = {k: 0 for k in ADDITIVE_KEYS}
    for row in rows:
        if row.get('type') not in (OVERRIDE_TYPE_ADDITIVE, OVERRIDE_TYPE_TRIAL):
            continue
        metrics = (row.get('metrics') or {}) if isinstance(row.get('metrics'), dict) else {}
        for k in ADDITIVE_KEYS:
            v = metrics.get(k)
            if v is None:
                continue
            try:
                additive_sums[k] += max(0, int(v))
            except (TypeError, ValueError):
                pass

    explanation: Dict[str, Dict] = {}
    for base_key in ALL_METRIC_KEYS:
        # Find best limit_override / trial limit for this metric.
        best_override = None
        for row in rows:
            if row.get('type') not in (OVERRIDE_TYPE_LIMIT, OVERRIDE_TYPE_TRIAL):
                continue
            metrics = (row.get('metrics') or {}) if isinstance(row.get('metrics'), dict) else {}
            for ov_key, mapped in LIMIT_OVERRIDE_KEYS.items():
                if mapped != base_key or ov_key not in metrics:
                    continue
                val = metrics.get(ov_key)
                if val is None:
                    continue
                try:
                    val_int = int(val)
                except (TypeError, ValueError):
                    continue
                if best_override is None or val_int > best_override:
                    best_override = val_int
        # Map base_key back to its additive key (only the monthly counters
        # have additive extras — quota caps don't).
        additive_extra = 0
        for add_key, mapped in ADDITIVE_KEYS.items():
            if mapped == base_key:
                additive_extra = additive_sums.get(add_key, 0)
                break
        explanation[base_key] = {
            'base': base.get(base_key),
            'additive_extra': additive_extra,
            'override_value': best_override,
            'effective': eff.get(base_key),
        }
    return explanation


def safe_override_summary(row: dict) -> dict:
    """Sanitized view of an override row for admin UI.

    Returns ids, type, status, dates, named metrics (numbers only), the
    grant_name, the reason text length and a short reason snippet, and
    timestamps. Never echoes anything else from the raw row.
    """
    if not isinstance(row, dict):
        return {}

    def _iso(value):
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    metrics = row.get('metrics') if isinstance(row.get('metrics'), dict) else {}
    safe_metrics = {}
    allowed_metric_keys = set(ADDITIVE_KEYS.keys()) | set(LIMIT_OVERRIDE_KEYS.keys())
    for k, v in metrics.items():
        if k in allowed_metric_keys and isinstance(v, (int, float)):
            safe_metrics[k] = int(v)
    reason = str(row.get('reason') or '')
    return {
        'id': row.get('id'),
        'user_id': row.get('user_id'),
        'type': row.get('type'),
        'status': row.get('status'),
        'grant_name': row.get('grant_name'),
        'reason': reason[:160],
        'reason_length': len(reason),
        'metrics': safe_metrics,
        'starts_at': _iso(row.get('starts_at')),
        'ends_at': _iso(row.get('ends_at')),
        'created_by_email': row.get('created_by_email'),
        'created_at': _iso(row.get('created_at')),
        'revoked_by_email': row.get('revoked_by_email'),
        'revoked_at': _iso(row.get('revoked_at')),
        'updated_at': _iso(row.get('updated_at')),
    }
