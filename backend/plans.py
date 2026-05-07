"""Phase 2.2: plan definitions and limits.

Centralizes all plan placeholder limits in one place so a future Phase 3
Stripe integration can swap or extend keys without hunting through
server.py. This module is BILLING-AGNOSTIC by design:
- no Stripe price/product ids
- no checkout sessions
- billing_enabled is always False
- all assignments are manual via the admin endpoint

Public callers should go through the helper functions:
    get_plan_limits(plan_key)
    plan_public_summary(plan_key)
    is_valid_plan_key(plan_key)
    PLAN_KEYS, DEFAULT_PLAN_KEY
"""
from __future__ import annotations

from typing import Dict, List, Optional


DEFAULT_PLAN_KEY = 'free'

# Counter keys must match server.USAGE_COUNTER_FIELDS so plan limits and
# monthly_usage counters share a vocabulary.
LIMIT_COUNTER_KEYS = (
    'monthly_comments_processed_limit',
    'monthly_public_replies_sent_limit',
    'monthly_dms_sent_limit',
    'monthly_links_clicked_limit',
    'queue_jobs_processed_limit',
)

# Mapping from limit key on the plan to the matching counter field on
# monthly_usage. Used by check_plan_limit to compare current usage to cap.
LIMIT_TO_COUNTER_FIELD = {
    'monthly_comments_processed_limit': 'comments_processed',
    'monthly_public_replies_sent_limit': 'public_replies_sent',
    'monthly_dms_sent_limit': 'dms_sent',
    'monthly_links_clicked_limit': 'links_clicked',
    'queue_jobs_processed_limit': 'queue_jobs_processed',
}


_PLAN_DEFINITIONS: Dict[str, Dict] = {
    'free': {
        'plan_key': 'free',
        'display_name': 'Free',
        'monthly_price_placeholder': 0,
        'max_instagram_accounts': 1,
        'max_active_automations': 2,
        'monthly_comments_processed_limit': 250,
        'monthly_public_replies_sent_limit': 100,
        'monthly_dms_sent_limit': 100,
        'monthly_links_clicked_limit': 100,
        'queue_jobs_processed_limit': None,
        'features': [
            '1 Instagram account',
            '2 active automations',
            'Up to 250 comments processed / month',
        ],
    },
    'starter': {
        'plan_key': 'starter',
        'display_name': 'Starter',
        'monthly_price_placeholder': 19,
        'max_instagram_accounts': 2,
        'max_active_automations': 10,
        'monthly_comments_processed_limit': 2000,
        'monthly_public_replies_sent_limit': 1000,
        'monthly_dms_sent_limit': 1000,
        'monthly_links_clicked_limit': 1000,
        'queue_jobs_processed_limit': None,
        'features': [
            '2 Instagram accounts',
            '10 active automations',
            '2,000 comments processed / month',
            'Email support',
        ],
    },
    'pro': {
        'plan_key': 'pro',
        'display_name': 'Pro',
        'monthly_price_placeholder': 79,
        'max_instagram_accounts': 5,
        'max_active_automations': 50,
        'monthly_comments_processed_limit': 20000,
        'monthly_public_replies_sent_limit': 10000,
        'monthly_dms_sent_limit': 10000,
        'monthly_links_clicked_limit': 10000,
        'queue_jobs_processed_limit': None,
        'features': [
            '5 Instagram accounts',
            '50 active automations',
            '20,000 comments processed / month',
            'Priority support',
        ],
    },
    'business': {
        'plan_key': 'business',
        'display_name': 'Business',
        'monthly_price_placeholder': 249,
        'max_instagram_accounts': 20,
        'max_active_automations': 200,
        'monthly_comments_processed_limit': 100000,
        'monthly_public_replies_sent_limit': 50000,
        'monthly_dms_sent_limit': 50000,
        'monthly_links_clicked_limit': 50000,
        'queue_jobs_processed_limit': None,
        'features': [
            '20 Instagram accounts',
            '200 active automations',
            '100,000 comments processed / month',
            'Dedicated success manager',
        ],
    },
}


PLAN_KEYS: tuple = tuple(_PLAN_DEFINITIONS.keys())


def is_valid_plan_key(plan_key) -> bool:
    return isinstance(plan_key, str) and plan_key in _PLAN_DEFINITIONS


def get_plan_limits(plan_key: Optional[str]) -> Dict:
    """Return a defensive copy of the plan definition; falls back to free."""
    key = plan_key if is_valid_plan_key(plan_key) else DEFAULT_PLAN_KEY
    return dict(_PLAN_DEFINITIONS[key])


def plan_public_summary(plan_key: Optional[str]) -> Dict:
    """Public-safe plan view (no internal-only fields). Currently every
    field is public, but this indirection lets us hide things later."""
    plan = get_plan_limits(plan_key)
    return {
        'plan_key': plan['plan_key'],
        'display_name': plan['display_name'],
        'monthly_price_placeholder': plan['monthly_price_placeholder'],
        'max_instagram_accounts': plan['max_instagram_accounts'],
        'max_active_automations': plan['max_active_automations'],
        'monthly_comments_processed_limit': plan['monthly_comments_processed_limit'],
        'monthly_public_replies_sent_limit': plan['monthly_public_replies_sent_limit'],
        'monthly_dms_sent_limit': plan['monthly_dms_sent_limit'],
        'monthly_links_clicked_limit': plan['monthly_links_clicked_limit'],
        'queue_jobs_processed_limit': plan['queue_jobs_processed_limit'],
        'features': list(plan['features']),
        'billing_enabled': False,
    }


def all_plan_summaries() -> List[Dict]:
    return [plan_public_summary(key) for key in PLAN_KEYS]


def remaining(limit_value: Optional[int], used: int) -> Optional[int]:
    """Compute remaining = max(limit - used, 0). None for unlimited."""
    if limit_value is None:
        return None
    return max(int(limit_value) - int(used or 0), 0)


def is_exceeded(limit_value: Optional[int], used: int, increment: int = 1) -> bool:
    """True if applying `increment` would exceed `limit_value`. None = unlimited."""
    if limit_value is None:
        return False
    return int(used or 0) + max(0, int(increment)) > int(limit_value)
