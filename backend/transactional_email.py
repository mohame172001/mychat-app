"""Server-only auth email delivery. Never log recipients or one-time links."""

import logging
import os
from html import escape
from urllib.parse import urlsplit

import httpx


logger = logging.getLogger('mychat')
RESEND_ENDPOINT = 'https://api.resend.com/emails'


def resend_configured() -> bool:
    return bool(os.environ.get('RESEND_API_KEY', '').strip()
                and os.environ.get('AUTH_EMAIL_FROM', '').strip())


async def send_auth_email(*, recipient: str, link: str, kind: str,
                          expires_in_minutes: int) -> bool:
    """True means Resend accepted the message, not confirmed inbox delivery."""
    if not resend_configured():
        return False
    labels = {
        'password_reset': ('Reset your MyChaat password', 'Reset password',
                           'Use the secure link below to choose a new password.'),
        'email_verification': ('Verify your MyChaat email', 'Verify email',
                               'Confirm your email address to activate your MyChaat account.'),
    }
    parsed = urlsplit(link)
    if kind not in labels or parsed.scheme != 'https' or not parsed.netloc or parsed.username:
        logger.warning('auth_email_failed provider=resend reason=invalid_template_or_link')
        return False
    subject, action, description = labels[kind]
    safe_link = escape(link, quote=True)
    expiry = max(1, int(expires_in_minutes))
    notice = f'This link expires in {expiry} minutes and can only be used once.'
    ignore = 'If you did not request this email, you can safely ignore it.'
    payload = {
        'from': os.environ['AUTH_EMAIL_FROM'].strip(),
        'to': [recipient],
        'subject': subject,
        'text': f'{subject}\n\n{description}\n\n{link}\n\n{notice}\n{ignore}',
        'html': (
            '<!doctype html><html lang="en"><body style="font-family:Arial,sans-serif;'
            'color:#172033;line-height:1.6;max-width:560px;margin:32px auto;padding:24px">'
            f'<p><strong>MyChaat</strong></p><h1>{subject}</h1><p>{description}</p>'
            f'<p><a href="{safe_link}" style="display:inline-block;padding:12px 22px;'
            f'background:#172033;color:#fff;border-radius:8px">{action}</a></p>'
            f'<p>{notice}</p><p>{ignore}</p>'
            f'<p>If the button does not work, open this link:</p><p>{safe_link}</p>'
            '</body></html>'
        ),
    }
    reply_to = os.environ.get('AUTH_EMAIL_REPLY_TO', '').strip()
    if reply_to:
        payload['reply_to'] = reply_to
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(RESEND_ENDPOINT, json=payload, headers={
                'Authorization': f"Bearer {os.environ['RESEND_API_KEY'].strip()}",
                'Content-Type': 'application/json',
            })
        if not 200 <= response.status_code < 300:
            logger.warning('auth_email_failed provider=resend kind=%s status_code=%s',
                           kind, response.status_code)
            return False
        body = response.json()
        if not isinstance(body, dict) or not body.get('id'):
            logger.warning('auth_email_failed provider=resend kind=%s reason=missing_message_id', kind)
            return False
        logger.info('auth_email_accepted provider=resend kind=%s', kind)
        return True
    except Exception as exc:
        # Provider bodies and exception messages can contain tokens or addresses.
        logger.warning('auth_email_failed provider=resend kind=%s reason=%s', kind, type(exc).__name__)
        return False
