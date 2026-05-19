"""High-level subscription operations on top of a Mongo `subscriptions`
collection. Provider-agnostic: the service hands off to whichever
`PaymentProvider` the caller picked.

Collection shape
----------------

    subscriptions = {
        'id': '<uuid>',
        'user_id': '<user uuid>',
        'plan_key': 'starter' | 'pro' | 'business',
        'status': 'pending' | 'active' | 'cancelled' | 'past_due' | 'failed',
        'provider': 'stripe' | 'paymob' | 'kashier' | 'manual',
        'provider_subscription_id': '<provider-side id>',
        'provider_checkout_session_id': '<short-lived checkout id>',
        'current_period_start': datetime,
        'current_period_end': datetime,
        'cancelled_at': datetime | None,
        'cancellation_reason': str | None,
        'created_at': datetime,
        'updated_at': datetime,
    }

We don't store card numbers or any provider tokens — only the
opaque ids the provider returns so a future webhook can match the
event back to a user.
"""
from __future__ import annotations

import logging
import secrets
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .providers import CheckoutSession, PaymentProvider, WebhookEvent


logger = logging.getLogger('mychat.billing')

ACTIVE_STATUSES = {'active', 'pending'}
FINAL_STATUSES = {'cancelled', 'failed'}


@dataclass
class SubscriptionRow:
    """Lightweight read-only view of a subscription document."""
    id: str
    user_id: str
    plan_key: str
    status: str
    provider: str
    current_period_end: Optional[datetime]
    cancelled_at: Optional[datetime]
    created_at: datetime

    def to_public(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'plan_key': self.plan_key,
            'status': self.status,
            'provider': self.provider,
            'current_period_end': self.current_period_end.isoformat() if self.current_period_end else None,
            'cancelled_at': self.cancelled_at.isoformat() if self.cancelled_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class SubscriptionService:
    """Stateless wrapper around the mongo collection + provider calls.
    Pass the motor `db` handle when constructing so the service stays
    test-friendly (tests can swap in a stub db)."""

    def __init__(self, db):
        self.db = db

    # ---- read --------------------------------------------------------

    async def get_active_subscription(self, user_id: str) -> Optional[SubscriptionRow]:
        doc = await self.db.subscriptions.find_one(
            {'user_id': user_id, 'status': {'$in': list(ACTIVE_STATUSES)}},
            sort=[('created_at', -1)],
        )
        return _doc_to_row(doc) if doc else None

    async def list_user_subscriptions(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        cur = self.db.subscriptions.find({'user_id': user_id}).sort('created_at', -1).limit(limit)
        items: List[Dict[str, Any]] = []
        async for doc in cur:
            row = _doc_to_row(doc)
            if row:
                items.append(row.to_public())
        return items

    async def list_invoices(self, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        cur = self.db.invoices.find({'user_id': user_id}).sort('created_at', -1).limit(limit)
        items: List[Dict[str, Any]] = []
        async for doc in cur:
            items.append({
                'id': doc.get('id'),
                'amount': doc.get('amount'),
                'currency': doc.get('currency'),
                'status': doc.get('status'),
                'provider': doc.get('provider'),
                'description': doc.get('description'),
                'paid_at': doc.get('paid_at').isoformat() if doc.get('paid_at') else None,
                'created_at': doc.get('created_at').isoformat() if doc.get('created_at') else None,
                'receipt_url': doc.get('receipt_url'),
            })
        return items

    # ---- checkout / cancel ------------------------------------------

    async def start_checkout(
        self,
        *,
        user_id: str,
        user_email: str,
        plan_key: str,
        provider: PaymentProvider,
        success_url: str,
        cancel_url: str,
    ) -> CheckoutSession:
        """Open a checkout session and record a `pending` subscription
        row. The webhook handler later flips it to `active` once payment
        clears (or `failed` on a declined card)."""
        session = await provider.create_checkout_session(
            user_id=user_id,
            user_email=user_email,
            plan_key=plan_key,
            success_url=success_url,
            cancel_url=cancel_url,
        )
        now = _now()
        sub_id = str(uuid.uuid4())
        await self.db.subscriptions.insert_one({
            'id': sub_id,
            'user_id': user_id,
            'plan_key': plan_key,
            'status': 'pending',
            'provider': provider.key,
            'provider_subscription_id': None,
            'provider_checkout_session_id': session.session_id,
            'current_period_start': None,
            'current_period_end': None,
            'cancelled_at': None,
            'cancellation_reason': None,
            'created_at': now,
            'updated_at': now,
        })
        logger.info(
            'billing_checkout_started user_id=%s plan=%s provider=%s session_id=%s',
            user_id, plan_key, provider.key, session.session_id,
        )
        return session

    async def cancel(self, *, user_id: str, reason: str = 'user_requested') -> bool:
        sub = await self.get_active_subscription(user_id)
        if not sub:
            return False
        # Best-effort: ask the provider to cancel the recurring charge.
        # If the provider refuses or doesn't support it, we still flip
        # our local record so future webhooks know the user opted out.
        from .providers import get_provider
        prov = get_provider(sub.provider)
        provider_ok = True
        doc = await self.db.subscriptions.find_one({'id': sub.id})
        if prov and doc and doc.get('provider_subscription_id'):
            provider_ok = await prov.cancel_subscription(
                provider_subscription_id=doc['provider_subscription_id']
            )
        await self.db.subscriptions.update_one(
            {'id': sub.id},
            {'$set': {
                'status': 'cancelled',
                'cancelled_at': _now(),
                'cancellation_reason': reason,
                'updated_at': _now(),
            }}
        )
        logger.info('billing_cancelled user_id=%s sub_id=%s provider_ok=%s reason=%s',
                    user_id, sub.id, provider_ok, reason)
        return True

    # ---- webhook -----------------------------------------------------

    async def apply_webhook_event(self, event: WebhookEvent) -> Dict[str, Any]:
        """Apply a verified provider webhook to our subscription state.
        Idempotent: replaying the same event leaves the row unchanged."""
        if not event.subscription_id and not event.user_id:
            return {'applied': False, 'reason': 'no_correlation_id'}
        # Try to find the pending checkout row this event closes.
        query: Dict[str, Any] = {}
        if event.subscription_id:
            query = {'$or': [
                {'provider_subscription_id': event.subscription_id},
                {'provider_checkout_session_id': event.subscription_id},
            ]}
        elif event.user_id:
            query = {'user_id': event.user_id, 'status': 'pending'}
        doc = await self.db.subscriptions.find_one(query, sort=[('created_at', -1)])
        if not doc:
            return {'applied': False, 'reason': 'no_subscription_row'}
        now = _now()
        update: Dict[str, Any] = {'updated_at': now}
        if event.event_type == 'subscription.activated':
            update['status'] = 'active'
            update['current_period_start'] = now
            update['current_period_end'] = now + timedelta(days=30)
            update['provider_subscription_id'] = event.subscription_id or doc.get('provider_subscription_id')
        elif event.event_type == 'subscription.cancelled':
            update['status'] = 'cancelled'
            update['cancelled_at'] = now
        elif event.event_type == 'payment.succeeded':
            update['status'] = 'active'
            update['current_period_end'] = now + timedelta(days=30)
            # Drop an invoice row so the user sees a receipt in the UI.
            await self.db.invoices.insert_one({
                'id': str(uuid.uuid4()),
                'user_id': doc['user_id'],
                'subscription_id': doc['id'],
                'plan_key': doc['plan_key'],
                'amount': _coerce_amount(event.raw),
                'currency': _coerce_currency(event.raw),
                'status': 'paid',
                'provider': doc['provider'],
                'description': f'{doc["plan_key"]} plan',
                'paid_at': now,
                'created_at': now,
                'receipt_url': _coerce_receipt_url(event.raw),
            })
        elif event.event_type == 'payment.failed':
            update['status'] = 'past_due'
        else:
            # Unknown event type — log but don't fail.
            logger.info('billing_webhook_unhandled event_type=%s', event.event_type)
            return {'applied': False, 'reason': 'unhandled_event_type', 'event_type': event.event_type}
        await self.db.subscriptions.update_one({'id': doc['id']}, {'$set': update})
        logger.info(
            'billing_webhook_applied user_id=%s event=%s sub_id=%s new_status=%s',
            doc['user_id'], event.event_type, doc['id'], update.get('status', doc['status']),
        )
        return {'applied': True, 'subscription_id': doc['id'], 'status': update.get('status', doc['status'])}

    # ---- admin grants -----------------------------------------------

    async def admin_grant(self, *, user_id: str, plan_key: str, granted_by: str, reason: str = '') -> str:
        """Operator grants a subscription without going through a
        provider (comp accounts, paid via wire transfer, beta access).
        Returns the new subscription id."""
        now = _now()
        sub_id = str(uuid.uuid4())
        await self.db.subscriptions.insert_one({
            'id': sub_id,
            'user_id': user_id,
            'plan_key': plan_key,
            'status': 'active',
            'provider': 'manual',
            'provider_subscription_id': f'manual-grant-{secrets.token_urlsafe(8)}',
            'provider_checkout_session_id': None,
            'current_period_start': now,
            'current_period_end': now + timedelta(days=30),
            'cancelled_at': None,
            'cancellation_reason': None,
            'granted_by': granted_by,
            'grant_reason': reason,
            'created_at': now,
            'updated_at': now,
        })
        logger.info('billing_admin_grant user_id=%s plan=%s by=%s reason=%s',
                    user_id, plan_key, granted_by, reason)
        return sub_id


def _doc_to_row(doc: Optional[Dict[str, Any]]) -> Optional[SubscriptionRow]:
    if not doc:
        return None
    return SubscriptionRow(
        id=doc.get('id', ''),
        user_id=doc.get('user_id', ''),
        plan_key=doc.get('plan_key', ''),
        status=doc.get('status', 'pending'),
        provider=doc.get('provider', ''),
        current_period_end=doc.get('current_period_end'),
        cancelled_at=doc.get('cancelled_at'),
        created_at=doc.get('created_at') or _now(),
    )


def _coerce_amount(raw: Dict[str, Any]) -> Optional[int]:
    # Stripe: data.object.amount_paid (in cents)
    obj = (raw.get('data') or {}).get('object') or {}
    for k in ('amount_paid', 'amount_due', 'amount_total', 'amount'):
        if isinstance(obj.get(k), int):
            return obj[k]
    # Paymob: obj.amount_cents
    if isinstance((raw.get('obj') or {}).get('amount_cents'), int):
        return raw['obj']['amount_cents']
    return None


def _coerce_currency(raw: Dict[str, Any]) -> str:
    obj = (raw.get('data') or {}).get('object') or {}
    if obj.get('currency'):
        return str(obj['currency']).upper()
    paymob_currency = (raw.get('obj') or {}).get('currency')
    if paymob_currency:
        return str(paymob_currency).upper()
    return 'USD'


def _coerce_receipt_url(raw: Dict[str, Any]) -> Optional[str]:
    obj = (raw.get('data') or {}).get('object') or {}
    url = obj.get('hosted_invoice_url') or obj.get('receipt_url')
    return str(url) if url else None
