"""Payment-provider abstraction.

Every concrete provider implements the same `PaymentProvider`
interface so the rest of the codebase doesn't care whether the user
paid with a Visa via Stripe, Vodafone Cash via Paymob, or a Fawry
voucher. The four bundled implementations are:

    - StripeProvider     — global cards / wallets, also supports EGP
    - PaymobProvider     — Egyptian gateway, cards + mobile wallets
                           (Vodafone Cash, Orange Cash, etsalat Cash) +
                           Fawry vouchers
    - KashierProvider    — alternative Egyptian gateway
    - ManualProvider     — no-op fallback for development; the admin
                           console grants subscriptions directly

Each provider only "activates" when its env vars are set. If no
provider is configured, the API still returns 200 on /billing/providers
with an empty list, and the frontend renders the "billing not enabled
yet" banner exactly the way it does today.

Provider keys MUST match the path segment used in
    POST /api/billing/webhook/{provider_key}
so adding a new provider here is the only file you touch to expose its
webhook to Meta-style verification.
"""
from __future__ import annotations

import hmac
import hashlib
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional


logger = logging.getLogger('mychat.billing')

PROVIDER_KEYS = ('stripe', 'paymob', 'kashier', 'manual')


@dataclass
class CheckoutSession:
    """What `PaymentProvider.create_checkout_session` returns.

    Always carries:
        - redirect_url: where the user's browser should go next
        - session_id:   provider-side handle, stored on our Subscription
                        row so a webhook can match this checkout
                        back to the user
        - provider:     which provider produced this session
        - metadata:     anything the provider returned that ops might
                        want to inspect later. Never tokens or secrets.
    """
    redirect_url: str
    session_id: str
    provider: str
    metadata: Dict = field(default_factory=dict)


@dataclass
class WebhookEvent:
    """Normalised view of a provider webhook event."""
    event_type: str             # e.g. 'subscription.activated', 'payment.failed'
    subscription_id: Optional[str]
    user_id: Optional[str]
    plan_key: Optional[str]
    raw: Dict
    valid_signature: bool


class PaymentProvider(ABC):
    """Abstract base. All four bundled providers implement this."""

    key: str = 'abstract'
    display_name: str = 'Abstract'
    supports_subscriptions: bool = True
    supports_one_off: bool = False
    # Currencies supported by the provider, ISO 4217.
    currencies: tuple = ()

    @abstractmethod
    def is_configured(self) -> bool:
        """True iff the env vars / DB rows this provider needs are set."""

    @abstractmethod
    async def create_checkout_session(
        self,
        *,
        user_id: str,
        user_email: str,
        plan_key: str,
        success_url: str,
        cancel_url: str,
    ) -> CheckoutSession:
        """Open a checkout session and return the redirect URL."""

    @abstractmethod
    async def cancel_subscription(self, *, provider_subscription_id: str) -> bool:
        """Cancel the recurring charge with the provider. Returns True on
        success, False if the provider rejected the request."""

    @abstractmethod
    async def verify_webhook(self, *, raw_body: bytes, signature_header: Optional[str]) -> bool:
        """Verify the signature on a webhook payload."""

    @abstractmethod
    async def parse_webhook(self, *, raw_body: bytes) -> WebhookEvent:
        """Parse a verified webhook into our normalised WebhookEvent."""

    # ---- helpers shared by every concrete subclass --------------------

    def public_summary(self) -> Dict:
        """Safe-to-render info for the frontend: key + label + currency
        list. Never includes API keys or webhook secrets."""
        return {
            'key': self.key,
            'display_name': self.display_name,
            'supports_subscriptions': self.supports_subscriptions,
            'supports_one_off': self.supports_one_off,
            'currencies': list(self.currencies),
        }


# ----- Concrete providers ----------------------------------------------

class StripeProvider(PaymentProvider):
    """Stripe Checkout / Subscriptions stub. Activates when
    STRIPE_SECRET_KEY + STRIPE_WEBHOOK_SECRET are set. The real `stripe`
    Python SDK is imported lazily so the dependency stays optional."""

    key = 'stripe'
    display_name = 'Card (Stripe)'
    supports_subscriptions = True
    supports_one_off = True
    currencies = ('USD', 'EUR', 'EGP', 'SAR', 'AED', 'GBP')

    def __init__(self):
        self._secret = os.environ.get('STRIPE_SECRET_KEY', '').strip()
        self._webhook_secret = os.environ.get('STRIPE_WEBHOOK_SECRET', '').strip()
        # Map of plan_key → Stripe price id. The operator fills these in
        # via env when they're ready to charge real money.
        self._price_ids = {
            'starter': os.environ.get('STRIPE_PRICE_STARTER', '').strip(),
            'pro': os.environ.get('STRIPE_PRICE_PRO', '').strip(),
            'business': os.environ.get('STRIPE_PRICE_BUSINESS', '').strip(),
        }

    def is_configured(self) -> bool:
        return bool(self._secret and self._webhook_secret)

    async def create_checkout_session(self, *, user_id, user_email, plan_key, success_url, cancel_url):
        price_id = self._price_ids.get(plan_key)
        if not price_id:
            raise RuntimeError(f'stripe_price_missing_for_plan:{plan_key}')
        # Lazy import so a deploy without the stripe library still boots.
        try:
            import stripe as _stripe  # type: ignore
        except ImportError as exc:
            raise RuntimeError('stripe_sdk_not_installed') from exc
        _stripe.api_key = self._secret
        session = _stripe.checkout.Session.create(
            mode='subscription',
            payment_method_types=['card'],
            line_items=[{'price': price_id, 'quantity': 1}],
            customer_email=user_email,
            client_reference_id=user_id,
            metadata={'user_id': user_id, 'plan_key': plan_key},
            success_url=success_url,
            cancel_url=cancel_url,
        )
        return CheckoutSession(
            redirect_url=session.url,
            session_id=session.id,
            provider=self.key,
            metadata={'customer': getattr(session, 'customer', None)},
        )

    async def cancel_subscription(self, *, provider_subscription_id):
        try:
            import stripe as _stripe  # type: ignore
        except ImportError:
            return False
        _stripe.api_key = self._secret
        try:
            _stripe.Subscription.delete(provider_subscription_id)
            return True
        except Exception as exc:
            logger.warning('stripe_cancel_failed sub_id=%s err=%s', provider_subscription_id, type(exc).__name__)
            return False

    async def verify_webhook(self, *, raw_body, signature_header):
        if not signature_header or not self._webhook_secret:
            return False
        try:
            import stripe as _stripe  # type: ignore
        except ImportError:
            return False
        try:
            _stripe.Webhook.construct_event(raw_body, signature_header, self._webhook_secret)
            return True
        except Exception:
            return False

    async def parse_webhook(self, *, raw_body):
        import json as _json
        payload = _json.loads(raw_body)
        event_type = payload.get('type', '')
        obj = (payload.get('data') or {}).get('object') or {}
        metadata = obj.get('metadata') or {}
        return WebhookEvent(
            event_type=_stripe_event_normalize(event_type),
            subscription_id=obj.get('subscription') or obj.get('id'),
            user_id=metadata.get('user_id') or obj.get('client_reference_id'),
            plan_key=metadata.get('plan_key'),
            raw=payload,
            valid_signature=True,
        )


def _stripe_event_normalize(event_type: str) -> str:
    """Map Stripe event names to our shorter internal vocabulary."""
    if event_type == 'checkout.session.completed':
        return 'subscription.activated'
    if event_type in ('customer.subscription.deleted',):
        return 'subscription.cancelled'
    if event_type in ('invoice.payment_failed',):
        return 'payment.failed'
    if event_type in ('invoice.payment_succeeded', 'invoice.paid'):
        return 'payment.succeeded'
    if event_type.startswith('customer.subscription.'):
        return 'subscription.updated'
    return event_type


class PaymobProvider(PaymentProvider):
    """Paymob (Egyptian gateway). Supports Visa/MC, mobile wallets
    (Vodafone Cash, Orange Cash, Etisalat Cash), and Fawry vouchers.

    Activates when PAYMOB_API_KEY + PAYMOB_HMAC + PAYMOB_INTEGRATION_*
    are set. We don't ship a Paymob SDK; the checkout flow uses Paymob
    iframe URLs the same way Stripe Checkout works.

    Docs: https://docs.paymob.com/docs/accept-overview
    """

    key = 'paymob'
    display_name = 'البطاقة + المحفظة (Paymob)'
    supports_subscriptions = False  # Paymob's subscription product is
                                    # opt-in per merchant; we charge per
                                    # billing cycle as a one-off until
                                    # ops enable it.
    supports_one_off = True
    currencies = ('EGP',)

    def __init__(self):
        self._api_key = os.environ.get('PAYMOB_API_KEY', '').strip()
        self._hmac = os.environ.get('PAYMOB_HMAC', '').strip()
        self._integration_card = os.environ.get('PAYMOB_INTEGRATION_CARD', '').strip()
        self._integration_wallet = os.environ.get('PAYMOB_INTEGRATION_WALLET', '').strip()
        self._integration_fawry = os.environ.get('PAYMOB_INTEGRATION_FAWRY', '').strip()
        self._iframe_id = os.environ.get('PAYMOB_IFRAME_ID', '').strip()

    def is_configured(self) -> bool:
        return bool(self._api_key and self._hmac and self._integration_card)

    async def create_checkout_session(self, *, user_id, user_email, plan_key, success_url, cancel_url):
        # Pricing in piastres (Paymob expects integer cents). Operator
        # overrides per plan via env.
        amount_piastres_map = {
            'starter': int(os.environ.get('PAYMOB_AMOUNT_STARTER', '59900')),    # 599 EGP
            'pro':     int(os.environ.get('PAYMOB_AMOUNT_PRO',     '249900')),   # 2499 EGP
            'business':int(os.environ.get('PAYMOB_AMOUNT_BUSINESS','799900')),   # 7999 EGP
        }
        amount = amount_piastres_map.get(plan_key)
        if amount is None:
            raise RuntimeError(f'paymob_price_missing_for_plan:{plan_key}')
        # Paymob's 3-step flow (auth → register order → payment key) is
        # implemented via httpx. We keep the actual call commented out
        # here because real-money provider configuration is operator-
        # supplied, but the orchestration sketch lives in the doc-string
        # of the surrounding service.
        import httpx
        async with httpx.AsyncClient(timeout=15) as client:
            # 1) auth
            auth = await client.post(
                'https://accept.paymob.com/api/auth/tokens',
                json={'api_key': self._api_key},
            )
            auth.raise_for_status()
            auth_token = auth.json().get('token')
            # 2) register order
            order = await client.post(
                'https://accept.paymob.com/api/ecommerce/orders',
                json={
                    'auth_token': auth_token,
                    'delivery_needed': False,
                    'amount_cents': amount,
                    'currency': 'EGP',
                    'merchant_order_id': f'mychat:{user_id}:{plan_key}',
                    'items': [],
                },
            )
            order.raise_for_status()
            order_id = order.json().get('id')
            # 3) payment key
            payment = await client.post(
                'https://accept.paymob.com/api/acceptance/payment_keys',
                json={
                    'auth_token': auth_token,
                    'amount_cents': amount,
                    'expiration': 3600,
                    'order_id': order_id,
                    'billing_data': {
                        'apartment': 'NA', 'email': user_email, 'floor': 'NA',
                        'first_name': 'mychat', 'street': 'NA', 'building': 'NA',
                        'phone_number': 'NA', 'shipping_method': 'NA',
                        'postal_code': 'NA', 'city': 'NA', 'country': 'EG',
                        'last_name': 'user', 'state': 'NA',
                    },
                    'currency': 'EGP',
                    'integration_id': int(self._integration_card),
                },
            )
            payment.raise_for_status()
            payment_token = payment.json().get('token')
        iframe_url = f'https://accept.paymob.com/api/acceptance/iframes/{self._iframe_id}?payment_token={payment_token}'
        return CheckoutSession(
            redirect_url=iframe_url,
            session_id=str(order_id),
            provider=self.key,
            metadata={'plan_key': plan_key, 'amount_piastres': amount},
        )

    async def cancel_subscription(self, *, provider_subscription_id):
        # Paymob doesn't support cancel-via-API for one-off orders; the
        # next billing cycle simply doesn't get charged. Surface True so
        # the caller flips the subscription state to "cancelled" locally.
        return True

    async def verify_webhook(self, *, raw_body, signature_header):
        if not self._hmac:
            return False
        import json as _json
        try:
            payload = _json.loads(raw_body)
            obj = payload.get('obj') or {}
        except Exception:
            return False
        # Paymob signs a deterministic concatenation of fields with HMAC-SHA512.
        # https://docs.paymob.com/docs/hmac-calculation
        fields = [
            'amount_cents', 'created_at', 'currency', 'error_occured', 'has_parent_transaction',
            'id', 'integration_id', 'is_3d_secure', 'is_auth', 'is_capture', 'is_refunded',
            'is_standalone_payment', 'is_voided', 'order.id', 'owner', 'pending', 'source_data.pan',
            'source_data.sub_type', 'source_data.type', 'success',
        ]
        def _resolve(path):
            cur = obj
            for part in path.split('.'):
                if not isinstance(cur, dict):
                    return ''
                cur = cur.get(part)
            return str(cur if cur is not None else '')
        concatenated = ''.join(_resolve(f) for f in fields)
        computed = hmac.new(self._hmac.encode('utf-8'), concatenated.encode('utf-8'), hashlib.sha512).hexdigest()
        provided = (signature_header or payload.get('hmac') or '').lower()
        return hmac.compare_digest(computed.lower(), provided)

    async def parse_webhook(self, *, raw_body):
        import json as _json
        payload = _json.loads(raw_body)
        obj = payload.get('obj') or {}
        merchant_order = obj.get('order', {}).get('merchant_order_id') or ''
        # We embed user_id + plan_key in merchant_order_id so a webhook
        # can correlate even when the user dropped the tab.
        parts = merchant_order.split(':')
        user_id = parts[1] if len(parts) >= 3 else None
        plan_key = parts[2] if len(parts) >= 3 else None
        success = bool(obj.get('success'))
        event_type = 'payment.succeeded' if success else 'payment.failed'
        if success:
            event_type = 'subscription.activated'
        return WebhookEvent(
            event_type=event_type,
            subscription_id=str(obj.get('id') or ''),
            user_id=user_id,
            plan_key=plan_key,
            raw=payload,
            valid_signature=True,
        )


class KashierProvider(PaymentProvider):
    """Kashier (Egyptian gateway). Activates when KASHIER_API_KEY +
    KASHIER_MERCHANT_ID + KASHIER_WEBHOOK_TOKEN are set. The redirect
    URL is signed with HMAC-SHA256.

    Docs: https://developers.kashier.io/payment/integrationflow.html
    """

    key = 'kashier'
    display_name = 'الدفع الإلكتروني (Kashier)'
    supports_subscriptions = False
    supports_one_off = True
    currencies = ('EGP', 'USD')

    def __init__(self):
        self._merchant = os.environ.get('KASHIER_MERCHANT_ID', '').strip()
        self._secret = os.environ.get('KASHIER_API_KEY', '').strip()
        self._webhook_token = os.environ.get('KASHIER_WEBHOOK_TOKEN', '').strip()
        self._mode = os.environ.get('KASHIER_MODE', 'test').strip()

    def is_configured(self) -> bool:
        return bool(self._merchant and self._secret)

    async def create_checkout_session(self, *, user_id, user_email, plan_key, success_url, cancel_url):
        amount_map = {
            'starter': int(os.environ.get('KASHIER_AMOUNT_STARTER', '599')),
            'pro':     int(os.environ.get('KASHIER_AMOUNT_PRO', '2499')),
            'business':int(os.environ.get('KASHIER_AMOUNT_BUSINESS', '7999')),
        }
        amount = amount_map.get(plan_key)
        if amount is None:
            raise RuntimeError(f'kashier_price_missing_for_plan:{plan_key}')
        order_id = f'mychat-{user_id}-{plan_key}-{int(__import__("time").time())}'
        payload = f'/?payment={self._merchant}.{order_id}.{amount}.EGP'
        signature = hmac.new(self._secret.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()
        redirect = (
            f'https://checkout.kashier.io/?merchantId={self._merchant}'
            f'&orderId={order_id}&amount={amount}&currency=EGP'
            f'&hash={signature}&mode={self._mode}&display=ar'
            f'&merchantRedirect={success_url}&failureRedirect={cancel_url}'
            f'&metaData={{"user_id":"{user_id}","plan_key":"{plan_key}"}}'
        )
        return CheckoutSession(
            redirect_url=redirect,
            session_id=order_id,
            provider=self.key,
            metadata={'amount': amount},
        )

    async def cancel_subscription(self, *, provider_subscription_id):
        return True

    async def verify_webhook(self, *, raw_body, signature_header):
        if not self._webhook_token:
            return False
        provided = (signature_header or '').strip()
        return bool(provided) and hmac.compare_digest(provided, self._webhook_token)

    async def parse_webhook(self, *, raw_body):
        import json as _json
        payload = _json.loads(raw_body)
        data = payload.get('data') or payload
        order = data.get('orderReference', '') or data.get('merchantOrderId', '')
        parts = order.split('-')
        user_id = parts[1] if len(parts) >= 3 else None
        plan_key = parts[2] if len(parts) >= 4 else None
        status = (data.get('status') or '').lower()
        event_type = 'subscription.activated' if status == 'success' else 'payment.failed'
        return WebhookEvent(
            event_type=event_type,
            subscription_id=order,
            user_id=user_id,
            plan_key=plan_key,
            raw=payload,
            valid_signature=True,
        )


class ManualProvider(PaymentProvider):
    """No-op provider used during development and the public beta.

    `create_checkout_session` returns a same-tab redirect to a stub
    success page that the admin endpoint immediately flips to
    'subscription.activated'. Lets us exercise the full subscription
    state machine end-to-end without any real money moving."""

    key = 'manual'
    display_name = 'Manual (testing)'
    supports_subscriptions = True
    supports_one_off = True
    currencies = ('USD', 'EGP')

    def is_configured(self) -> bool:
        # Only active when the operator explicitly opts in. Default off
        # in production so a stray request can't grant a paid plan.
        return os.environ.get('BILLING_MANUAL_ENABLED', '').lower() in ('1', 'true', 'yes')

    async def create_checkout_session(self, *, user_id, user_email, plan_key, success_url, cancel_url):
        return CheckoutSession(
            redirect_url=f'{success_url}?manual=pending&plan={plan_key}',
            session_id=f'manual:{user_id}:{plan_key}',
            provider=self.key,
            metadata={'note': 'manual provider — admin must confirm'},
        )

    async def cancel_subscription(self, *, provider_subscription_id):
        return True

    async def verify_webhook(self, *, raw_body, signature_header):
        # The manual provider has no webhook flow.
        return False

    async def parse_webhook(self, *, raw_body):
        return WebhookEvent(event_type='', subscription_id=None, user_id=None,
                            plan_key=None, raw={}, valid_signature=False)


# ----- Registry --------------------------------------------------------

_registry: Dict[str, PaymentProvider] = {}


def _get_registry() -> Dict[str, PaymentProvider]:
    # Build lazily so importing this module doesn't read every env var.
    if _registry:
        return _registry
    _registry['stripe'] = StripeProvider()
    _registry['paymob'] = PaymobProvider()
    _registry['kashier'] = KashierProvider()
    _registry['manual'] = ManualProvider()
    return _registry


def available_providers() -> List[PaymentProvider]:
    """Return only the providers that have all their env vars set."""
    return [p for p in _get_registry().values() if p.is_configured()]


def get_provider(key: str) -> Optional[PaymentProvider]:
    """Look up a provider by key. Returns None when the key isn't
    recognised OR when the provider exists but isn't configured."""
    if key not in PROVIDER_KEYS:
        return None
    p = _get_registry().get(key)
    if p is None or not p.is_configured():
        return None
    return p


def is_any_provider_configured() -> bool:
    return any(p.is_configured() for p in _get_registry().values())
