"""Public support intake; recipients and sender are never client controlled."""
import logging
import os
from typing import Literal

import httpx
from pydantic import BaseModel, EmailStr, Field

from transactional_email import RESEND_ENDPOINT, resend_configured

logger = logging.getLogger('mychat')
SUPPORT_ADDRESS = 'support@mychaat.net'


class SupportMessage(BaseModel):
    email: EmailStr = Field(max_length=254)
    category: Literal['account', 'instagram', 'automation', 'billing', 'privacy', 'other']
    message: str = Field(min_length=20, max_length=5000)
    website: str = Field(default='', max_length=200)


async def send_support_message(data: SupportMessage) -> bool:
    if not resend_configured():
        return False
    payload = {
        'from': os.environ['AUTH_EMAIL_FROM'].strip(),
        'to': [SUPPORT_ADDRESS],
        'reply_to': str(data.email),
        'subject': f'MyChaat support: {data.category}',
        'text': f'Category: {data.category}\n\n{data.message.strip()}',
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(RESEND_ENDPOINT, json=payload, headers={
                'Authorization': f"Bearer {os.environ['RESEND_API_KEY'].strip()}",
            })
        if 200 <= response.status_code < 300 and isinstance(response.json(), dict) and response.json().get('id'):
            logger.info('support_email_accepted provider=resend')
            return True
        logger.warning('support_email_failed status_code=%s', response.status_code)
    except Exception as exc:
        logger.warning('support_email_failed reason=%s', type(exc).__name__)
    return False
