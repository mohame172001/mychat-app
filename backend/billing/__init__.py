"""mychat billing package.

Provider-agnostic subscription handling. Each payment provider (Stripe,
Paymob, Kashier, Fawry, or the no-op Manual provider used during
development) implements the same `PaymentProvider` interface, and the
service layer + API endpoints stay identical regardless of which one
the operator wires up.

Public exports:
    - registry / available_providers / get_provider — provider lookup
    - SubscriptionService — high-level operations on a user's subscription
    - PROVIDER_KEYS — recognised provider keys for env / DB validation

This module is intentionally side-effect-free at import time. The actual
provider client objects (stripe.Client, httpx clients for Paymob, etc.)
are only instantiated when a request that needs them lands.
"""
from .providers import (
    PaymentProvider,
    CheckoutSession,
    WebhookEvent,
    PROVIDER_KEYS,
    available_providers,
    get_provider,
    is_any_provider_configured,
)
from .service import SubscriptionService

__all__ = [
    'PaymentProvider',
    'CheckoutSession',
    'WebhookEvent',
    'PROVIDER_KEYS',
    'available_providers',
    'get_provider',
    'is_any_provider_configured',
    'SubscriptionService',
]
