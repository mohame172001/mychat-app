import os
import asyncio
import base64
import hashlib
import hmac
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from urllib.parse import urlencode

from fastapi import FastAPI, APIRouter, HTTPException, Depends, Query, Request, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import JSONResponse, PlainTextResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument
import httpx

from models import (
    SignupIn, LoginIn, AuthOut, UserPublic,
    AutomationIn, AutomationPatch, Automation,
    ContactIn, ContactPatch, Contact,
    BroadcastIn, BroadcastPatch, Broadcast,
    MessageIn, Conversation,
    DmRuleIn, DmRulePatch, DmTestIn,
)
from auth_utils import hash_password, verify_password, create_token, get_current_user_id, decode_token, JWT_SECRET

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

MONGO_URL = os.environ['MONGO_URL']
DB_NAME = os.environ.get('DB_NAME', 'mychat')
META_APP_ID = os.environ.get('META_APP_ID', '')
META_APP_SECRET = os.environ.get('META_APP_SECRET', '')
# Instagram API with Business Login uses a SEPARATE App ID/Secret from the
# Facebook App. Resolution priority for the Instagram credential pair:
#   INSTAGRAM_APP_ID > IG_APP_ID > META_APP_ID
#   INSTAGRAM_APP_SECRET > IG_APP_SECRET > META_APP_SECRET
# We track which env name actually supplied each value so the credentials
# diagnostic endpoint can show what was used (without exposing the value).
def _resolve_env(*names):
    for n in names:
        v = os.environ.get(n, '')
        if v:
            return v, n
    return '', None


INSTAGRAM_APP_ID, INSTAGRAM_APP_ID_SOURCE = _resolve_env(
    'INSTAGRAM_APP_ID', 'IG_APP_ID', 'META_APP_ID')
INSTAGRAM_APP_SECRET, INSTAGRAM_APP_SECRET_SOURCE = _resolve_env(
    'INSTAGRAM_APP_SECRET', 'IG_APP_SECRET', 'META_APP_SECRET')

# Backward-compat aliases used by the rest of the codebase.
IG_APP_ID = INSTAGRAM_APP_ID
IG_APP_SECRET = INSTAGRAM_APP_SECRET

# Webhook GET verification token.
META_VERIFY_TOKEN, META_VERIFY_TOKEN_SOURCE = _resolve_env(
    'META_WEBHOOK_VERIFY_TOKEN', 'META_VERIFY_TOKEN')
if not META_VERIFY_TOKEN:
    META_VERIFY_TOKEN = 'mychat_verify_123'
    META_VERIFY_TOKEN_SOURCE = 'default'

# Webhook X-Hub-Signature-256 secret. If META_WEBHOOK_APP_SECRET is set we use
# it; otherwise we fall back to META_APP_SECRET.
META_WEBHOOK_APP_SECRET, META_WEBHOOK_APP_SECRET_SOURCE = _resolve_env(
    'META_WEBHOOK_APP_SECRET', 'META_APP_SECRET')
# When enforce=True (META_WEBHOOK_HMAC_ENFORCE=1 env), webhooks with bad or
# missing signatures are rejected with 403. Default is warn-only mode so
# existing setups aren't disrupted until the operator confirms the correct
# secret is configured.
META_WEBHOOK_HMAC_ENFORCE = os.environ.get('META_WEBHOOK_HMAC_ENFORCE', '0') == '1'
FRONTEND_URL = os.environ.get('FRONTEND_URL', 'http://localhost:3000')
BACKEND_PUBLIC_URL = os.environ.get('BACKEND_PUBLIC_URL', 'http://localhost:8001')
CRON_SECRET = os.environ.get('CRON_SECRET', '')
TOKEN_REFRESH_LOOKAHEAD_DAYS = int(os.environ.get('IG_TOKEN_REFRESH_LOOKAHEAD_DAYS', '15'))
TOKEN_REFRESH_MIN_AGE_HOURS = int(os.environ.get('IG_TOKEN_REFRESH_MIN_AGE_HOURS', '24'))
TOKEN_REFRESH_LOCK_MINUTES = int(os.environ.get('IG_TOKEN_REFRESH_LOCK_MINUTES', '5'))

client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]

app = FastAPI(title='mychat API')
api = APIRouter(prefix='/api')

logger = logging.getLogger('mychat')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Silence libraries that log full request URLs at INFO. httpx/httpcore otherwise
# emit lines like "HTTP Request: GET .../comments?access_token=IGAA..." which
# leaks the user's IG long-lived token into Railway log retention.
for _noisy in ('httpx', 'httpcore', 'httpcore.http11', 'httpcore.connection'):
    logging.getLogger(_noisy).setLevel(logging.WARNING)


def _redact_secrets(value):
    """Recursively redact obvious credential keys from dicts/strings before
    they hit the log stream or HTTP response bodies."""
    SECRET_KEYS = {'access_token', 'accesstoken', 'meta_access_token',
                   'client_secret', 'app_secret', 'refresh_token',
                   'token', 'authorization'}
    if isinstance(value, dict):
        return {k: ('***REDACTED***' if k.lower() in SECRET_KEYS else _redact_secrets(v))
                for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_secrets(v) for v in value]
    return value


# ---------------- WebSocket manager ----------------
class ConnectionManager:
    def __init__(self):
        self.active: Dict[str, WebSocket] = {}

    async def connect(self, user_id: str, ws: WebSocket):
        await ws.accept()
        self.active[user_id] = ws
        logger.info('WS connected: %s', user_id)

    def disconnect(self, user_id: str):
        self.active.pop(user_id, None)
        logger.info('WS disconnected: %s', user_id)

    async def send(self, user_id: str, data: dict):
        ws = self.active.get(user_id)
        if ws:
            try:
                await ws.send_json(data)
            except Exception:
                self.disconnect(user_id)


ws_manager = ConnectionManager()


# ---------------- Meta messaging helper ----------------
async def send_ig_message(access_token: str, ig_user_id: str, recipient_ig_id: str,
                          message: dict) -> dict:
    """Send a raw Instagram message object. Tokens are never returned."""
    if not access_token or not ig_user_id:
        logger.warning('send_ig_message: missing access_token or ig_user_id')
        return {'ok': False, 'status_code': None, 'error': 'missing_access_token_or_ig_user_id'}
    url = f'https://graph.instagram.com/{ig_user_id}/messages'
    payload = {
        'recipient': {'id': recipient_ig_id},
        'message': message,
    }
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(url, json=payload, params={'access_token': access_token})
            if r.status_code == 200:
                try:
                    body = r.json()
                except Exception:
                    body = {}
                return {'ok': True, 'status_code': r.status_code, 'body': _redact_secrets(body)}
            logger.error('send_ig_message error %s: %s', r.status_code, _redact_secrets(r.text))
            return {'ok': False, 'status_code': r.status_code, 'error': _redact_secrets(r.text[:500])}
    except Exception as e:
        logger.exception('send_ig_message exception: %s', e)
        return {'ok': False, 'status_code': None, 'error': str(e)[:500]}


async def send_ig_dm(access_token: str, ig_user_id: str, recipient_ig_id: str, text: str) -> bool:
    """Send a text DM via Instagram Graph API. Returns True on success."""
    result = await send_ig_message(access_token, ig_user_id, recipient_ig_id, {'text': text})
    return bool(result.get('ok'))


def _quick_reply_title(title: str, fallback: str = 'Send me the link') -> str:
    """Instagram quick reply labels are short; keep custom labels usable."""
    value = (title or fallback or '').strip() or fallback
    return value[:20]


async def send_ig_quick_reply(access_token: str, ig_user_id: str, recipient_ig_id: str,
                              text: str, title: str, payload: str) -> dict:
    return await send_ig_message(
        access_token,
        ig_user_id,
        recipient_ig_id,
        {
            'text': text,
            'quick_replies': [{
                'content_type': 'text',
                'title': _quick_reply_title(title),
                'payload': payload[:1000],
            }],
        },
    )


async def send_ig_url_button(access_token: str, ig_user_id: str, recipient_ig_id: str,
                             text: str, button_title: str, url: str) -> dict:
    return await send_ig_message(
        access_token,
        ig_user_id,
        recipient_ig_id,
        {
            'attachment': {
                'type': 'template',
                'payload': {
                    'template_type': 'button',
                    'text': text,
                    'buttons': [{
                        'type': 'web_url',
                        'url': url,
                        'title': (button_title or 'Open link').strip()[:20],
                    }],
                },
            },
        },
    )


async def get_instagram_messaging_user_profile(access_token: str, ig_scoped_id: str) -> dict:
    """Fetch the messaging user's profile, including follow relationship."""
    if not access_token or not ig_scoped_id:
        return {'ok': False, 'status_code': None, 'error': 'missing_access_token_or_igsid'}
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f'https://graph.instagram.com/{ig_scoped_id}',
                params={
                    'fields': (
                        'name,username,profile_pic,follower_count,'
                        'is_user_follow_business,is_business_follow_user'
                    ),
                    'access_token': access_token,
                },
            )
            try:
                body = r.json()
            except Exception:
                body = {'raw': r.text[:500]}
            if r.status_code == 200:
                return {'ok': True, 'status_code': r.status_code, 'profile': _redact_secrets(body)}
            return {'ok': False, 'status_code': r.status_code, 'error': _redact_secrets(body)}
    except Exception as e:
        logger.exception('instagram_user_profile_fetch_exception: %s', e)
        return {'ok': False, 'status_code': None, 'error': str(e)[:500]}


def _comment_dm_flow_enabled(automation: dict) -> bool:
    if (automation.get('mode') or '') != 'reply_and_dm':
        return False
    return any([
        automation.get('opening_dm_text'),
        automation.get('opening_dm_button_text'),
        automation.get('link_dm_text'),
        automation.get('link_url'),
        automation.get('follow_request_enabled'),
        automation.get('email_request_enabled'),
        automation.get('follow_up_enabled') and automation.get('follow_up_text'),
    ])


async def _create_comment_dm_session(user_doc: dict, automation: dict, recipient_ig_id: str,
                                     comment_context: Optional[dict], payload: str) -> dict:
    import uuid as _uuid
    now = datetime.utcnow()
    session = {
        'id': payload.split(':')[1] if ':' in payload else str(_uuid.uuid4()),
        'user_id': user_doc['id'],
        **_current_instagram_context(user_doc),
        'ig_user_id': user_doc.get('ig_user_id') or '',
        'recipient_id': recipient_ig_id,
        'automation_id': automation.get('id'),
        'automation_name': automation.get('name'),
        'comment_doc_id': (comment_context or {}).get('comment_doc_id'),
        'ig_comment_id': (comment_context or {}).get('ig_comment_id'),
        'payload': payload,
        'status': 'pending',
        'stage': 'awaiting_user_action',
        'link_dm_text': (automation.get('link_dm_text') or '').strip(),
        'link_button_text': (automation.get('link_button_text') or '').strip(),
        'link_url': (automation.get('link_url') or '').strip(),
        'follow_request_enabled': bool(automation.get('follow_request_enabled')),
        'follow_verified': False,
        'follow_verification_attempts': 0,
        'email_request_enabled': bool(automation.get('email_request_enabled')),
        'follow_up_enabled': bool(automation.get('follow_up_enabled')),
        'follow_up_text': (automation.get('follow_up_text') or '').strip(),
        'created': now,
        'updated': now,
    }
    await db.comment_dm_sessions.insert_one(session)
    return session


async def _send_comment_dm_flow_entry(user_doc: dict, automation: dict, recipient_ig_id: str,
                                      comment_context: Optional[dict] = None) -> bool:
    """Send the first DM only. The link step waits for the recipient response."""
    import uuid as _uuid
    access_token = user_doc.get('meta_access_token', '')
    ig_user_id = user_doc.get('ig_user_id', '')
    opening_text = (automation.get('opening_dm_text') or automation.get('dm_text') or '').strip()
    button_text = (automation.get('opening_dm_button_text') or 'Send me the link').strip()
    has_deferred_step = any([
        automation.get('link_dm_text'),
        automation.get('link_url'),
        automation.get('follow_request_enabled'),
        automation.get('email_request_enabled'),
        automation.get('follow_up_enabled') and automation.get('follow_up_text'),
    ])

    if opening_text and has_deferred_step:
        payload = f'comment_flow:{str(_uuid.uuid4())}:continue'
        await _create_comment_dm_session(user_doc, automation, recipient_ig_id, comment_context, payload)
        result = await send_ig_quick_reply(
            access_token, ig_user_id, recipient_ig_id,
            opening_text, button_text, payload,
        )
        if result.get('ok'):
            logger.info('comment_dm_opening_quick_reply_sent rule_id=%s recipient=%s',
                        automation.get('id'), recipient_ig_id)
            return True
        logger.warning('comment_dm_quick_reply_failed rule_id=%s err=%s; falling back to text',
                       automation.get('id'), result.get('error'))
        # Keep the pending session active. If quick replies are not accepted by
        # Meta for this account, the user can still type any response to continue.
        return await send_ig_dm(access_token, ig_user_id, recipient_ig_id, opening_text)

    if opening_text:
        return await send_ig_dm(access_token, ig_user_id, recipient_ig_id, opening_text)

    if has_deferred_step:
        payload = f'comment_flow:{str(_uuid.uuid4())}:continue'
        session = await _create_comment_dm_session(
            user_doc, automation, recipient_ig_id, comment_context, payload
        )
        return await _send_comment_dm_flow_completion(user_doc, session)

    return await _send_comment_dm_flow_completion(
        user_doc,
        {
            'user_id': user_doc['id'],
            'ig_user_id': ig_user_id,
            'recipient_id': recipient_ig_id,
            'automation_id': automation.get('id'),
            'link_dm_text': (automation.get('link_dm_text') or '').strip(),
            'link_button_text': (automation.get('link_button_text') or '').strip(),
            'link_url': (automation.get('link_url') or '').strip(),
            'follow_request_enabled': bool(automation.get('follow_request_enabled')),
            'follow_verified': False,
            'email_request_enabled': bool(automation.get('email_request_enabled')),
            'follow_up_enabled': bool(automation.get('follow_up_enabled')),
            'follow_up_text': (automation.get('follow_up_text') or '').strip(),
        },
    )


async def _verify_comment_dm_follow_gate(user_doc: dict, session: dict) -> dict:
    """Require a real follow before continuing a gated comment DM flow."""
    if not session.get('follow_request_enabled'):
        return {'allowed': True, 'checked': False}
    if session.get('follow_verified') is True:
        return {'allowed': True, 'checked': True, 'cached': True}

    access_token = user_doc.get('meta_access_token', '')
    ig_user_id = user_doc.get('ig_user_id', '')
    recipient_id = session.get('recipient_id')
    now = datetime.utcnow()
    profile_result = await get_instagram_messaging_user_profile(access_token, recipient_id)
    profile = profile_result.get('profile') or {}
    is_following = bool(profile.get('is_user_follow_business')) if profile_result.get('ok') else False

    update = {
        'stage': 'follow_verified' if is_following else 'awaiting_follow',
        'follow_verified': is_following,
        'lastFollowCheckAt': now,
        'lastFollowCheckOk': bool(profile_result.get('ok')),
        'lastFollowCheckStatus': profile_result.get('status_code'),
        'lastFollowCheckError': profile_result.get('error'),
        'updated': now,
    }
    if profile:
        update['lastFollowerProfile'] = {
            'username': profile.get('username'),
            'name': profile.get('name'),
            'is_user_follow_business': profile.get('is_user_follow_business'),
            'is_business_follow_user': profile.get('is_business_follow_user'),
        }
    await db.comment_dm_sessions.update_one(
        {'id': session.get('id')},
        {'$set': update, '$inc': {'follow_verification_attempts': 1}},
    )

    if is_following:
        logger.info('comment_dm_follow_gate_verified session=%s recipient=%s',
                    session.get('id'), recipient_id)
        return {'allowed': True, 'checked': True, 'profile': profile}

    prompt = (
        'Please follow this account first, then tap "I followed" and I will send the link.'
        if profile_result.get('ok')
        else 'I could not confirm the follow yet. Please follow this account, then tap "I followed".'
    )
    payload = session.get('payload') or f'comment_flow:{session.get("id")}:followed'
    result = await send_ig_quick_reply(
        access_token,
        ig_user_id,
        recipient_id,
        prompt,
        'I followed',
        payload,
    )
    prompt_sent = bool(result.get('ok'))
    if not prompt_sent:
        prompt_sent = await send_ig_dm(access_token, ig_user_id, recipient_id, prompt)
    logger.info('comment_dm_follow_gate_blocked session=%s recipient=%s prompt_sent=%s',
                session.get('id'), recipient_id, prompt_sent)
    return {
        'allowed': False,
        'checked': True,
        'prompt_sent': prompt_sent,
        'profile': profile,
        'error': profile_result.get('error'),
    }


async def _send_comment_dm_flow_completion(user_doc: dict, session: dict) -> bool:
    access_token = user_doc.get('meta_access_token', '')
    ig_user_id = user_doc.get('ig_user_id', '')
    recipient_id = session.get('recipient_id')
    if not recipient_id:
        return False

    follow_gate = await _verify_comment_dm_follow_gate(user_doc, session)
    if not follow_gate.get('allowed'):
        return bool(follow_gate.get('prompt_sent'))

    ok_all = True
    sent_steps = ['follow_verified'] if session.get('follow_request_enabled') else []
    link_text = (session.get('link_dm_text') or '').strip()
    link_url = (session.get('link_url') or '').strip()
    link_button = (session.get('link_button_text') or 'Open link').strip()

    if link_url:
        text_for_button = link_text or 'Here is the link'
        result = await send_ig_url_button(
            access_token, ig_user_id, recipient_id,
            text_for_button, link_button, link_url,
        )
        if result.get('ok'):
            sent_steps.append('link_button')
        else:
            fallback_text = f'{text_for_button}\n\n{link_url}'.strip()
            ok = await send_ig_dm(access_token, ig_user_id, recipient_id, fallback_text)
            ok_all = ok_all and ok
            sent_steps.append('link_text_fallback')
            if not ok:
                logger.warning('comment_dm_link_fallback_failed session=%s err=%s',
                               session.get('id'), result.get('error'))
    elif link_text:
        ok = await send_ig_dm(access_token, ig_user_id, recipient_id, link_text)
        ok_all = ok_all and ok
        sent_steps.append('link_text')

    extra_messages = []
    if session.get('email_request_enabled'):
        extra_messages.append('Reply with your email and we will send the details.')
    if session.get('follow_up_enabled') and session.get('follow_up_text'):
        extra_messages.append((session.get('follow_up_text') or '').strip())

    for text in [m for m in extra_messages if m]:
        ok = await send_ig_dm(access_token, ig_user_id, recipient_id, text)
        ok_all = ok_all and ok
        sent_steps.append('extra_message')

    if session.get('follow_request_enabled') and sent_steps == ['follow_verified']:
        ok = await send_ig_dm(access_token, ig_user_id, recipient_id, 'Thanks for following. You are all set.')
        ok_all = ok_all and ok
        sent_steps.append('follow_confirmation')

    if session.get('id'):
        await db.comment_dm_sessions.update_one(
            {'id': session['id']},
            {'$set': {
                'status': 'completed' if ok_all else 'failed',
                'completedAt': datetime.utcnow(),
                'updated': datetime.utcnow(),
                'sentSteps': sent_steps,
            }},
        )
    logger.info('comment_dm_flow_completed session=%s ok=%s steps=%s',
                session.get('id'), ok_all, sent_steps)
    return ok_all


async def _find_pending_comment_dm_session(user_doc: dict, sender_id: str,
                                           payload: Optional[str] = None) -> Optional[dict]:
    if not sender_id:
        return None
    q = {
        'user_id': user_doc['id'],
        'recipient_id': sender_id,
        'status': 'pending',
    }
    if payload and str(payload).startswith('comment_flow:'):
        parts = str(payload).split(':')
        if len(parts) >= 2 and parts[1]:
            q['id'] = parts[1]
    return await db.comment_dm_sessions.find_one(q, sort=[('created', -1)])


# ---------------- Comment reply helper ----------------
async def reply_to_ig_comment(access_token: str, ig_comment_id: str, text: str) -> bool:
    """Reply to an Instagram comment via Graph API."""
    if not access_token or not ig_comment_id:
        return False
    url = f'https://graph.instagram.com/{ig_comment_id}/replies'
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(url, data={'message': text, 'access_token': access_token})
            if r.status_code == 200:
                return True
            logger.error('reply_to_ig_comment error %s: %s', r.status_code, r.text)
            return False
    except Exception as e:
        logger.exception('reply_to_ig_comment exception: %s', e)
        return False


# ---------------- Automation engine ----------------
async def execute_flow(user: dict, automation: dict, sender_ig_id: str,
                       trigger_text: str = '', comment_context: Optional[dict] = None):
    """Walk the flow graph and execute each node in order.
    comment_context: when the trigger was an Instagram comment, holds
        {'ig_comment_id': ..., 'comment_doc_id': ...} so reply_comment nodes work."""
    nodes = automation.get('nodes', [])
    edges = automation.get('edges', [])
    if not nodes:
        return False

    node_map = {n['id']: n for n in nodes}
    edge_map: Dict[str, list] = {}
    for e in edges:
        edge_map.setdefault(e['source'], []).append(e['target'])

    start = next((n for n in nodes if n.get('type') == 'trigger'), None)
    if not start:
        return False

    access_token = user.get('meta_access_token', '')
    ig_user_id = user.get('ig_user_id', '')
    current_ids = [start['id']]
    visited: set = set()
    action_attempted = False
    ok_all = True

    while current_ids:
        nid = current_ids.pop(0)
        if nid in visited:
            continue
        visited.add(nid)
        node = node_map.get(nid)
        if not node:
            continue
        ntype = node.get('type', '')
        data = node.get('data', {})

        if ntype == 'message':
            msg_text = data.get('text') or data.get('message', '')
            if msg_text and sender_ig_id:
                action_attempted = True
                if _comment_dm_flow_enabled(automation):
                    ok = await _send_comment_dm_flow_entry(
                        user, automation, sender_ig_id, comment_context
                    )
                    logger.info('Flow comment DM entry to %s rule=%s ok=%s',
                                sender_ig_id, automation.get('id'), ok)
                else:
                    ok = await send_ig_dm(access_token, ig_user_id, sender_ig_id, msg_text)
                    logger.info('Flow message to %s: %s (ok=%s)', sender_ig_id, msg_text[:40], ok)
                ok_all = ok_all and bool(ok)
        elif ntype == 'reply_comment':
            replies = data.get('replies')
            if replies and isinstance(replies, list) and len(replies) > 0:
                import random
                msg_text = random.choice(replies)
            else:
                msg_text = data.get('text') or data.get('message', '')
            if msg_text and comment_context and comment_context.get('ig_comment_id'):
                action_attempted = True
                ok = await reply_to_ig_comment(access_token, comment_context['ig_comment_id'], msg_text)
                logger.info('Flow comment reply on %s: %s (ok=%s)',
                            comment_context['ig_comment_id'], msg_text[:40], ok)
                ok_all = ok_all and bool(ok)
                if ok and comment_context.get('comment_doc_id'):
                    await db.comments.update_one(
                        {'id': comment_context['comment_doc_id']},
                        {'$set': {'replied': True, 'reply_text': msg_text}}
                    )
        elif ntype == 'delay':
            secs = int(data.get('seconds', 0) or data.get('delay', 0))
            if secs > 0:
                await asyncio.sleep(min(secs, 30))
        elif ntype == 'condition':
            keyword = (data.get('value') or '').lower()
            match = keyword in trigger_text.lower() if keyword else True
            # Take 'yes' edge if match, 'no' edge otherwise
            for edge in edges:
                if edge['source'] == nid:
                    label = (edge.get('label') or '').lower()
                    if match and label in ('yes', 'true', ''):
                        current_ids.append(edge['target'])
                    elif not match and label in ('no', 'false'):
                        current_ids.append(edge['target'])
            continue

        for next_id in edge_map.get(nid, []):
            current_ids.append(next_id)

    if action_attempted and ok_all:
        await db.automations.update_one(
            {'id': automation['id']},
            {'$inc': {'sent': 1}, '$set': {'updated': datetime.utcnow()}}
        )
    else:
        await db.automations.update_one(
            {'id': automation['id']},
            {'$set': {'updated': datetime.utcnow()}}
        )
    return bool(action_attempted and ok_all)


# ---------------- helpers ----------------
def _strip_mongo(doc):
    if doc and '_id' in doc:
        doc.pop('_id', None)
    return doc


def _public_user(u: dict) -> UserPublic:
    instagram_valid = _has_valid_instagram_connection(u)
    return UserPublic(
        id=u['id'], username=u['username'], name=u['name'], email=u['email'],
        avatar=u.get('avatar') or f"https://i.pravatar.cc/150?u={u['username']}",
        instagramConnected=instagram_valid,
        instagramHandle=u.get('instagramHandle'),
        instagramProfilePictureUrl=u.get('instagram_profile_picture_url'),
        instagramConnectionValid=instagram_valid,
        instagramAccountType=u.get('instagram_account_type'),
        activeInstagramAccountId=u.get('active_instagram_account_id'),
        activeInstagramIgUserId=u.get('ig_user_id'),
    )


def _has_valid_instagram_connection(u: Optional[dict]) -> bool:
    return bool(
        u
        and u.get('instagramConnected')
        and u.get('instagram_connection_valid')
        and u.get('meta_access_token')
        and u.get('ig_user_id')
    )


def _instagram_connection_error(u: Optional[dict]) -> str:
    if not u:
        return 'Instagram not connected'
    blocker = u.get('instagram_connection_blocker') or 'token_cannot_call_graph_me'
    if u.get('instagramConnected') and not u.get('instagram_connection_valid'):
        return f'Instagram reconnect required: {blocker}'
    return 'Instagram not connected'


async def _seed_user(user_id: str):
    """No fake data. New users start with a clean slate — contacts,
    conversations, automations and comments will be populated by real
    Instagram webhook events once the account is connected."""
    return


# ---------------- auth ----------------
@api.post('/auth/signup', response_model=AuthOut)
async def signup(data: SignupIn):
    if await db.users.find_one({'username': data.username}):
        raise HTTPException(400, 'Username already taken')
    if await db.users.find_one({'email': data.email}):
        raise HTTPException(400, 'Email already registered')
    import uuid
    user_id = str(uuid.uuid4())
    user_doc = {
        'id': user_id, 'username': data.username, 'email': data.email,
        'name': data.username.capitalize(),
        'password_hash': hash_password(data.password),
        'avatar': f'https://i.pravatar.cc/150?u={data.username}',
        'instagramConnected': False, 'instagramHandle': None,
        'created': datetime.utcnow(),
    }
    await db.users.insert_one(user_doc)
    await _seed_user(user_id)
    return AuthOut(token=create_token(user_id), user=_public_user(user_doc))


@api.post('/auth/login', response_model=AuthOut)
async def login(data: LoginIn):
    u = await db.users.find_one({'username': data.username})
    if not u or not verify_password(data.password, u['password_hash']):
        raise HTTPException(401, 'Invalid username or password')
    return AuthOut(token=create_token(u['id']), user=_public_user(u))


@api.get('/auth/me', response_model=UserPublic)
async def me(user_id: str = Depends(get_current_user_id)):
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    return _public_user(u)


# ---------------- automations ----------------
@api.get('/automations')
async def list_automations(user_id: str = Depends(get_current_user_id)):
    try:
        account = await getActiveInstagramAccount(user_id)
    except HTTPException as e:
        if e.status_code == 400:
            return []
        raise
    cursor = db.automations.find(_account_scoped_query(user_id, account)).sort('updated', -1)
    return [_strip_mongo(d) for d in await cursor.to_list(1000)]


@api.post('/automations')
async def create_automation(data: AutomationIn, user_id: str = Depends(get_current_user_id)):
    automation_data = data.model_dump()
    account = await getActiveInstagramAccount(user_id)
    ctx = _instagram_context_from_account(account)
    if ctx['instagramAccountId']:
        automation_data.update(ctx)
    now = datetime.utcnow()
    # Handle None values for nodes and edges
    if automation_data.get('nodes') is None:
        automation_data['nodes'] = []
    if automation_data.get('edges') is None:
        automation_data['edges'] = []
    automation_data['createdAt'] = now
    automation_data['updatedAt'] = now
    automation_data['processExistingComments'] = bool(automation_data.get('processExistingComments', False))
    if _is_comment_automation_rule(automation_data):
        automation_data['activationStartedAt'] = now
    a = Automation(user_id=user_id, **automation_data)
    await db.automations.insert_one(a.model_dump())
    return a.model_dump()


@api.get('/automations/{aid}')
async def get_automation(aid: str, user_id: str = Depends(get_current_user_id)):
    account = await getActiveInstagramAccount(user_id)
    d = await db.automations.find_one({'id': aid, **_account_scoped_query(user_id, account)})
    if not d:
        raise HTTPException(404, 'Not found')
    return _strip_mongo(d)


@api.patch('/automations/{aid}')
async def patch_automation(aid: str, data: AutomationPatch, user_id: str = Depends(get_current_user_id)):
    update = {k: v for k, v in data.model_dump().items() if v is not None}
    account = await getActiveInstagramAccount(user_id)
    scoped = _account_scoped_query(user_id, account)
    existing = await db.automations.find_one({'id': aid, **scoped})
    if not existing:
        raise HTTPException(404, 'Not found')
    now = datetime.utcnow()
    update['updated'] = now
    update['updatedAt'] = now
    prospective = {**existing, **update}
    reset_fields = {
        'trigger', 'nodes', 'edges', 'match', 'keyword', 'media_id', 'latest',
        'mode', 'comment_reply', 'dm_text', 'media_preview', 'keywords',
        'post_scope', 'reply_under_post', 'opening_dm_enabled',
        'opening_dm_text', 'opening_dm_button_text', 'link_dm_text',
        'link_button_text', 'link_url', 'follow_request_enabled',
        'email_request_enabled', 'follow_up_enabled', 'follow_up_text',
        'processExistingComments',
    }
    status_reenabled = (
        update.get('status') == 'active' and existing.get('status') != 'active'
    )
    rule_shape_changed = any(field in update for field in reset_fields)
    if _is_comment_automation_rule(prospective) and (status_reenabled or rule_shape_changed):
        update['activationStartedAt'] = now
        logger.info('comment_rule_activation_reset rule_id=%s user_id=%s reason=%s',
                    aid, user_id, 'status_reenabled' if status_reenabled else 'rule_changed')
    res = await db.automations.update_one({'id': aid, **scoped}, {'$set': update})
    if res.matched_count == 0:
        raise HTTPException(404, 'Not found')
    d = await db.automations.find_one({'id': aid})
    return _strip_mongo(d)


@api.delete('/automations/{aid}')
async def delete_automation(aid: str, user_id: str = Depends(get_current_user_id)):
    account = await getActiveInstagramAccount(user_id)
    res = await db.automations.delete_one({'id': aid, **_account_scoped_query(user_id, account)})
    if res.deleted_count == 0:
        raise HTTPException(404, 'Not found')
    return {'ok': True}


@api.post('/automations/{aid}/duplicate')
async def duplicate_automation(aid: str, user_id: str = Depends(get_current_user_id)):
    account = await getActiveInstagramAccount(user_id)
    d = await db.automations.find_one({'id': aid, **_account_scoped_query(user_id, account)})
    if not d:
        raise HTTPException(404, 'Not found')
    import uuid
    copy = _strip_mongo({**d})
    copy['id'] = str(uuid.uuid4())
    copy['name'] = d['name'] + ' (Copy)'
    copy['status'] = 'draft'
    copy['sent'] = 0
    copy['clicks'] = 0
    now = datetime.utcnow()
    copy['created'] = now
    copy['updated'] = now
    copy['createdAt'] = now
    copy['updatedAt'] = now
    copy['processExistingComments'] = False
    if _is_comment_automation_rule(copy):
        copy['activationStartedAt'] = now
    await db.automations.insert_one(copy)
    return _strip_mongo(copy)


@api.post('/automations/quick-comment-rule')
async def create_quick_comment_rule(
    data: dict = Body(...),
    user_id: str = Depends(get_current_user_id),
):
    """Create an automation that watches comments on an IG media (specific or latest).

    Body:
      media_id?: str
      latest?: bool                         - watch whatever is currently newest
      media_preview?: {caption, thumbnail_url, media_type}  - cached for display
      mode?: 'reply_and_dm' | 'reply_only'  - default 'reply_and_dm'
      match?: 'any' | 'keyword'             - default 'any'
      keyword?: str                         - required if match=='keyword'
      comment_reply: str                    - public reply text
      dm_text?: str                         - DM text (ignored when mode=='reply_only')
      name?: str
    """
    import uuid

    def _split_keywords(value) -> list:
        raw = value
        if isinstance(raw, list):
            parts = raw
        else:
            parts = str(raw or '').split(',')
        seen = set()
        out = []
        for item in parts:
            kw = str(item or '').strip()
            key = kw.lower()
            if kw and key not in seen:
                seen.add(key)
                out.append(kw)
        return out

    media_id = (data.get('media_id') or '').strip() or None
    post_scope = (data.get('post_scope') or '').strip().lower()
    if not post_scope:
        post_scope = 'latest' if bool(data.get('latest')) else 'specific'
    if post_scope not in ('specific', 'any', 'latest', 'next'):
        raise HTTPException(400, "post_scope must be 'specific', 'any', 'latest', or 'next'")
    latest = post_scope in ('latest', 'next') or bool(data.get('latest'))
    if post_scope == 'specific' and not media_id:
        raise HTTPException(400, 'Provide media_id for a specific post')

    mode = (data.get('mode') or 'reply_and_dm').strip()
    if mode not in ('reply_and_dm', 'reply_only'):
        raise HTTPException(400, "mode must be 'reply_and_dm' or 'reply_only'")

    match = (data.get('match') or 'any').strip()
    keywords = _split_keywords(data.get('keywords') if 'keywords' in data else data.get('keyword'))
    keyword = ', '.join(keywords)
    if match == 'keyword' and not keywords:
        raise HTTPException(400, 'keyword is required when match=keyword')
    if match not in ('any', 'keyword'):
        raise HTTPException(400, "match must be 'any' or 'keyword'")

    reply_under_post = bool(data.get('reply_under_post', True))
    comment_reply = (data.get('comment_reply') or '').strip()
    comment_reply_2 = (data.get('comment_reply_2') or '').strip()
    comment_reply_3 = (data.get('comment_reply_3') or '').strip()
    opening_dm_enabled = bool(data.get('opening_dm_enabled', mode == 'reply_and_dm'))
    opening_dm_text = (data.get('opening_dm_text') or data.get('dm_text') or '').strip()
    opening_dm_button_text = (data.get('opening_dm_button_text') or '').strip()
    link_dm_text = (data.get('link_dm_text') or '').strip()
    link_button_text = (data.get('link_button_text') or '').strip()
    link_url = (data.get('link_url') or '').strip()
    follow_request_enabled = bool(data.get('follow_request_enabled', False))
    email_request_enabled = bool(data.get('email_request_enabled', False))
    follow_up_enabled = bool(data.get('follow_up_enabled', False))
    follow_up_text = (data.get('follow_up_text') or '').strip()
    has_dm_action = any([
        opening_dm_enabled and opening_dm_text,
        opening_dm_button_text,
        link_dm_text,
        link_button_text,
        link_url,
        follow_request_enabled,
        email_request_enabled,
        follow_up_enabled and follow_up_text,
    ])
    dm_text = ''
    if mode == 'reply_and_dm':
        dm_text = (
            opening_dm_text if opening_dm_enabled and opening_dm_text
            else (link_dm_text or link_url or '')
        ).strip()
    if mode == 'reply_and_dm' and has_dm_action and not dm_text:
        dm_text = 'Thanks for your comment.'
    process_existing_comments = bool(data.get('processExistingComments', False))
    if reply_under_post and not (comment_reply or comment_reply_2 or comment_reply_3):
        raise HTTPException(400, 'At least one comment reply is required')
    if mode == 'reply_and_dm' and not dm_text:
        dm_text = 'Thanks for your comment.'

    if mode == 'reply_and_dm' and not has_dm_action:
        dm_text = 'Thanks for your comment.'
    if not reply_under_post and not dm_text:
        raise HTTPException(400, 'Enable a public reply or a DM message')

    account = await getActiveInstagramAccount(user_id)
    ctx = _instagram_context_from_account(account)

    if post_scope == 'any':
        trigger = 'comment:any'
    elif latest:
        trigger = 'comment:latest'
    else:
        trigger = f'comment:{media_id}'
    preview = data.get('media_preview') or {}
    if post_scope == 'any':
        default_name = 'Any post - ' + (f'keywords "{keyword}"' if match == 'keyword' else 'any comment')
    elif latest:
        default_name = 'Latest post — ' + (f'keyword "{keyword}"' if match == 'keyword' else 'any comment')
    else:
        label = (preview.get('caption') or '')[:30] or (media_id[:10] if media_id else '')
        default_name = f'{label} — ' + (f'keyword "{keyword}"' if match == 'keyword' else 'any comment')
    name = (data.get('name') or default_name).strip()

    nodes = [{'id': 'n_trigger', 'type': 'trigger',
              'data': {'label': 'Comment trigger', 'trigger': trigger,
                       'match': match, 'keyword': keyword, 'keywords': keywords}}]
    edges = []
    prev = 'n_trigger'
    if reply_under_post and (comment_reply or comment_reply_2 or comment_reply_3):
        replies = [r for r in [comment_reply, comment_reply_2, comment_reply_3] if r]
        nodes.append({'id': 'n_reply', 'type': 'reply_comment',
                      'data': {'text': replies[0] if replies else '', 'replies': replies}})
        edges.append({'id': 'e1', 'source': prev, 'target': 'n_reply'})
        prev = 'n_reply'
    if dm_text:
        nodes.append({'id': 'n_dm', 'type': 'message', 'data': {
            'text': dm_text,
            'opening_dm_text': opening_dm_text,
            'opening_dm_button_text': opening_dm_button_text,
            'link_dm_text': link_dm_text,
            'link_button_text': link_button_text,
            'link_url': link_url,
            'follow_request_enabled': follow_request_enabled,
            'email_request_enabled': email_request_enabled,
            'follow_up_enabled': follow_up_enabled,
            'follow_up_text': follow_up_text,
        }})
        edges.append({'id': f'e{len(edges)+1}', 'source': prev, 'target': 'n_dm'})

    now = datetime.utcnow()
    doc = {
        'id': str(uuid.uuid4()),
        'user_id': user_id,
        **({} if not ctx['instagramAccountId'] else ctx),
        'name': name,
        'status': 'active',
        'trigger': trigger,
        'match': match,
        'keyword': keyword,
        'keywords': keywords,
        'mode': mode,
        'post_scope': post_scope,
        'reply_under_post': reply_under_post,
        'comment_reply': comment_reply,
        'comment_reply_2': comment_reply_2,
        'comment_reply_3': comment_reply_3,
        'dm_text': dm_text,
        'opening_dm_enabled': opening_dm_enabled,
        'opening_dm_text': opening_dm_text,
        'opening_dm_button_text': opening_dm_button_text,
        'link_dm_text': link_dm_text,
        'link_button_text': link_button_text,
        'link_url': link_url,
        'follow_request_enabled': follow_request_enabled,
        'email_request_enabled': email_request_enabled,
        'follow_up_enabled': follow_up_enabled,
        'follow_up_text': follow_up_text,
        'media_id': media_id,
        'latest': latest,
        'media_preview': preview,
        'nodes': nodes,
        'edges': edges,
        'sent': 0,
        'clicks': 0,
        'processExistingComments': process_existing_comments,
        'activationStartedAt': now,
        'createdAt': now,
        'updatedAt': now,
        'created': now,
        'updated': now,
    }
    await db.automations.insert_one(doc)
    return _strip_mongo({**doc})


# ---------------- contacts ----------------
@api.get('/contacts')
async def list_contacts(search: Optional[str] = None, tag: Optional[str] = None,
                        user_id: str = Depends(get_current_user_id)):
    q = {'user_id': user_id}
    if search:
        q['$or'] = [
            {'name': {'$regex': search, '$options': 'i'}},
            {'username': {'$regex': search, '$options': 'i'}},
        ]
    if tag:
        q['tags'] = tag
    docs = await db.contacts.find(q).sort('created', -1).to_list(2000)
    return [_strip_mongo(d) for d in docs]


@api.post('/contacts')
async def create_contact(data: ContactIn, user_id: str = Depends(get_current_user_id)):
    import uuid
    doc = {
        'id': str(uuid.uuid4()), 'user_id': user_id,
        'name': data.name, 'username': data.username,
        'avatar': data.avatar or f'https://i.pravatar.cc/150?u={data.username}',
        'tags': data.tags, 'subscribed': data.subscribed,
        'lastActive': datetime.utcnow(), 'created': datetime.utcnow(),
    }
    await db.contacts.insert_one(doc)
    return _strip_mongo(doc)


@api.patch('/contacts/{cid}')
async def patch_contact(cid: str, data: ContactPatch, user_id: str = Depends(get_current_user_id)):
    update = {k: v for k, v in data.model_dump().items() if v is not None}
    res = await db.contacts.update_one({'id': cid, 'user_id': user_id}, {'$set': update})
    if res.matched_count == 0:
        raise HTTPException(404, 'Not found')
    d = await db.contacts.find_one({'id': cid})
    return _strip_mongo(d)


@api.delete('/contacts/{cid}')
async def delete_contact(cid: str, user_id: str = Depends(get_current_user_id)):
    res = await db.contacts.delete_one({'id': cid, 'user_id': user_id})
    if res.deleted_count == 0:
        raise HTTPException(404, 'Not found')
    return {'ok': True}


# ---------------- broadcasts ----------------
@api.get('/broadcasts')
async def list_broadcasts(user_id: str = Depends(get_current_user_id)):
    docs = await db.broadcasts.find({'user_id': user_id}).sort('created', -1).to_list(500)
    return [_strip_mongo(d) for d in docs]


@api.post('/broadcasts')
async def create_broadcast(data: BroadcastIn, user_id: str = Depends(get_current_user_id)):
    import uuid
    total_audience = await db.contacts.count_documents({'user_id': user_id, 'subscribed': True})
    doc = {
        'id': str(uuid.uuid4()), 'user_id': user_id,
        'name': data.name, 'message': data.message,
        'status': 'draft',
        'audience': data.audience_size or total_audience or 0,
        'openRate': '-', 'clickRate': '-', 'date': '-',
        'created': datetime.utcnow(),
    }
    await db.broadcasts.insert_one(doc)
    return _strip_mongo(doc)


@api.patch('/broadcasts/{bid}')
async def patch_broadcast(bid: str, data: BroadcastPatch, user_id: str = Depends(get_current_user_id)):
    update = {k: v for k, v in data.model_dump().items() if v is not None}
    res = await db.broadcasts.update_one({'id': bid, 'user_id': user_id}, {'$set': update})
    if res.matched_count == 0:
        raise HTTPException(404, 'Not found')
    d = await db.broadcasts.find_one({'id': bid})
    return _strip_mongo(d)


@api.post('/broadcasts/{bid}/send')
async def send_broadcast(bid: str, user_id: str = Depends(get_current_user_id)):
    """Send a broadcast to all subscribed contacts via Meta API (or mock if IG not connected)."""
    broadcast = await db.broadcasts.find_one({'id': bid, 'user_id': user_id})
    if not broadcast:
        raise HTTPException(404, 'Not found')
    if broadcast.get('status') == 'sent':
        raise HTTPException(400, 'Already sent')

    user_doc = await db.users.find_one({'id': user_id})
    contacts = await db.contacts.find({'user_id': user_id, 'subscribed': True}).to_list(5000)

    await db.broadcasts.update_one({'id': bid}, {'$set': {'status': 'sending'}})
    asyncio.create_task(_send_broadcast_task(bid, broadcast, user_doc, contacts))
    return {'ok': True, 'status': 'sending', 'recipients': len(contacts)}


async def _send_broadcast_task(bid: str, broadcast: dict, user_doc: dict, contacts: list):
    """Background task: send DMs to all contacts."""
    sent = 0
    failed = 0
    ig_connected = user_doc.get('instagramConnected') and user_doc.get('meta_access_token')
    access_token = user_doc.get('meta_access_token', '')
    ig_user_id = user_doc.get('ig_user_id', '')
    msg_text = broadcast.get('message', '')

    for contact in contacts:
        ig_id = contact.get('ig_id')
        if ig_connected and ig_id:
            ok = await send_ig_dm(access_token, ig_user_id, ig_id, msg_text)
            if ok:
                sent += 1
            else:
                failed += 1
            await asyncio.sleep(0.5)  # respect rate limits
        else:
            # Not connected to Instagram, or contact has no IG id — cannot deliver
            failed += 1

    total = sent + failed
    # Open/click rates require Meta Insights — not wired yet, so leave blank
    open_rate = '-'
    click_rate = '-'
    await db.broadcasts.update_one(
        {'id': bid},
        {'$set': {
            'status': 'sent',
            'audience': total,
            'openRate': open_rate,
            'clickRate': click_rate,
            'date': datetime.utcnow().strftime('%b %d, %Y'),
        }}
    )
    logger.info('Broadcast %s done: %s sent, %s failed', bid, sent, failed)


# ---------------- conversations ----------------
@api.get('/conversations')
async def list_conversations(user_id: str = Depends(get_current_user_id)):
    try:
        account = await getActiveInstagramAccount(user_id)
        query = _account_scoped_query(user_id, account)
    except HTTPException as e:
        if e.status_code != 400:
            raise
        query = {'user_id': user_id, 'instagramAccountId': {'$exists': False}}
    docs = await db.conversations.find(query).sort('created', -1).to_list(500)
    return [_strip_mongo(d) for d in docs]


@api.get('/conversations/{cid}')
async def get_conversation(cid: str, user_id: str = Depends(get_current_user_id)):
    account = await getActiveInstagramAccount(user_id)
    d = await db.conversations.find_one({'id': cid, **_account_scoped_query(user_id, account)})
    if not d:
        raise HTTPException(404, 'Not found')
    return _strip_mongo(d)


@api.post('/conversations/{cid}/messages')
async def send_message(cid: str, data: MessageIn, user_id: str = Depends(get_current_user_id)):
    """Send a message. If the conversation is tied to a real IG contact and the
    user is connected to Instagram, send via Graph API. No fake auto-reply."""
    import uuid
    account = await getActiveInstagramAccount(user_id)
    user_doc = _with_instagram_account_context(
        await db.users.find_one({'id': user_id}) or {},
        account,
    )
    conv = await db.conversations.find_one({'id': cid, **_account_scoped_query(user_id, account)})
    if not conv:
        raise HTTPException(404, 'Not found')
    text = (data.text or '').strip()
    if not text:
        raise HTTPException(400, 'Empty message')

    msg_me = {'id': str(uuid.uuid4()), 'from': 'me', 'text': text,
              'time': datetime.utcnow().strftime('%I:%M %p')}
    new_messages = conv['messages'] + [msg_me]

    # Try to deliver to Instagram if we have a real recipient
    delivered = False
    delivery_error = None
    ig_recipient = (conv.get('contact') or {}).get('ig_id')
    if user_doc and user_doc.get('instagramConnected') and ig_recipient:
        try:
            delivered = await send_ig_dm(
                user_doc.get('meta_access_token', ''),
                user_doc.get('ig_user_id', ''),
                ig_recipient, text,
            )
            if not delivered:
                delivery_error = 'Graph API rejected the message'
        except Exception as e:
            delivery_error = str(e)
            logger.exception('send_message graph call failed')

    msg_me['delivered'] = delivered
    if delivery_error:
        msg_me['error'] = delivery_error
    await db.conversations.update_one(
        {'id': cid, **_account_scoped_query(user_id, account)},
        {'$set': {'messages': new_messages, 'lastMessage': text,
                  'time': 'now', 'unread': 0}}
    )
    # Push to WS so other tabs stay in sync
    await ws_manager.send(user_id, {'type': 'message', 'conv_id': cid, 'message': msg_me})
    return {'messages': new_messages, 'delivered': delivered, 'error': delivery_error}


# ---------------- comments ----------------
@api.get('/comments')
async def list_comments(
    user_id: str = Depends(get_current_user_id),
    limit: int = Query(50, le=100),
    page: int = Query(1, ge=1),
    unreplied: bool = Query(False)
):
    try:
        account = await getActiveInstagramAccount(user_id)
        query = _account_scoped_query(user_id, account)
    except HTTPException as e:
        if e.status_code != 400:
            raise
        query = {'user_id': user_id, 'instagramAccountId': {'$exists': False}}
        
    if unreplied:
        query['replied'] = {'$ne': True}

    skip = (page - 1) * limit
    total = await db.comments.count_documents(query)
    docs = await db.comments.find(query).sort('created', -1).skip(skip).limit(limit).to_list(limit)
    
    return {
        'comments': [_strip_mongo(d) for d in docs],
        'total': total,
        'page': page,
        'limit': limit,
        'has_more': (skip + limit) < total
    }


@api.post('/comments/{cid}/reply')
async def reply_to_comment(cid: str, data: MessageIn, user_id: str = Depends(get_current_user_id)):
    """Reply to an Instagram comment via Graph API.
    POST /{comment-id}/replies with message=..."""
    account = await getActiveInstagramAccount(user_id)
    comment = await db.comments.find_one({'id': cid, **_account_scoped_query(user_id, account)})
    if not comment:
        raise HTTPException(404, 'Comment not found')
    if not account.get('connectionValid'):
        raise HTTPException(400, 'Instagram not connected')
    ig_comment_id = comment.get('ig_comment_id')
    if not ig_comment_id:
        raise HTTPException(400, 'Comment has no Instagram ID (seed data cannot be replied to)')
    access_token = account.get('accessToken', '')
    text = (data.text or '').strip()
    if not text:
        raise HTTPException(400, 'Empty reply')
    url = f'https://graph.instagram.com/{ig_comment_id}/replies'
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(url, data={'message': text, 'access_token': access_token})
            if r.status_code != 200:
                logger.error('Comment reply error %s: %s', r.status_code, r.text)
                raise HTTPException(r.status_code, f'Graph API error: {r.text}')
            body = r.json()
    except HTTPException:
        raise
    except Exception as e:
        logger.exception('Comment reply failed')
        raise HTTPException(500, str(e))
    await db.comments.update_one(
        {'id': cid},
        {'$set': {'replied': True, 'reply_text': text, 'reply_id': body.get('id')}}
    )
    return {'ok': True, 'graph_reply_id': body.get('id')}


# ---------------- dashboard ----------------
def _dashboard_dt(*values) -> Optional[datetime]:
    for value in values:
        parsed = _parse_graph_datetime(value)
        if parsed:
            return parsed
    return None


def _dashboard_key(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.replace('@', '').lower()


def _dashboard_is_unscoped(doc: dict) -> bool:
    return not any(doc.get(k) for k in (
        'instagramAccountId', 'igUserId', 'instagramAccountDbId', 'instagram_account_id'
    ))


async def _dashboard_include_unscoped(user_id: str) -> bool:
    try:
        count = await db.instagram_accounts.count_documents({
            'userId': user_id,
            'isActive': {'$ne': False},
        })
        return count <= 1
    except Exception:
        return False


async def _dashboard_scoped_docs(collection_name: str, user_id: str, account: Optional[dict],
                                 include_unscoped: bool, limit: int = 5000) -> list:
    collection = getattr(db, collection_name)
    docs: list = []
    seen: set = set()

    async def add_many(query: dict):
        try:
            rows = await collection.find(query).sort('created', -1).to_list(limit)
        except Exception:
            rows = await collection.find(query).to_list(limit)
        for row in rows:
            key = row.get('id') or str(row.get('_id') or id(row))
            if key not in seen:
                seen.add(key)
                docs.append(row)

    if account:
        await add_many(_account_scoped_query(user_id, account))
    if include_unscoped:
        await add_many({
            'user_id': user_id,
            'instagramAccountId': {'$exists': False},
            'igUserId': {'$exists': False},
            'instagramAccountDbId': {'$exists': False},
            'instagram_account_id': {'$exists': False},
        })
    if not account and not include_unscoped:
        await add_many({'user_id': user_id})
    return docs


def _automation_active(auto: dict) -> bool:
    status = str(auto.get('status') or '').lower()
    if status:
        return status == 'active'
    if 'enabled' in auto:
        return bool(auto.get('enabled'))
    if 'isActive' in auto:
        return bool(auto.get('isActive'))
    return False


def _sent_day(doc: dict) -> Optional[str]:
    ts = _dashboard_dt(
        doc.get('sentAt'), doc.get('sent_at'), doc.get('completedAt'),
        doc.get('processed_at'), doc.get('updated'), doc.get('updatedAt'),
        doc.get('created'), doc.get('createdAt'),
    )
    return ts.date().isoformat() if ts else None


@api.get('/dashboard/stats')
async def dashboard_stats(user_id: str = Depends(get_current_user_id)):
    try:
        account = await getActiveInstagramAccount(user_id)
    except HTTPException as e:
        if e.status_code != 400:
            raise
        account = None

    include_unscoped = await _dashboard_include_unscoped(user_id)
    ig_owner_id = _dashboard_key(
        (account or {}).get('instagramAccountId') or (account or {}).get('igUserId')
    )

    autos = await _dashboard_scoped_docs('automations', user_id, account, include_unscoped, 5000)
    comments = await _dashboard_scoped_docs('comments', user_id, account, include_unscoped, 5000)
    conversations = await _dashboard_scoped_docs('conversations', user_id, account, include_unscoped, 5000)
    dm_logs = await _dashboard_scoped_docs('dm_logs', user_id, account, include_unscoped, 5000)
    sessions = await _dashboard_scoped_docs('comment_dm_sessions', user_id, account, include_unscoped, 5000)
    contacts = await _dashboard_scoped_docs('contacts', user_id, account, include_unscoped, 5000)

    active_automations = sum(1 for auto in autos if _automation_active(auto))
    automation_sent = sum(int(auto.get('sent') or 0) for auto in autos)

    contacts_seen: set = set()

    def add_contact(*values):
        for value in values:
            key = _dashboard_key(value)
            if key and key != ig_owner_id:
                contacts_seen.add(key)
                return

    for c in comments:
        add_contact(c.get('commenter_id'), c.get('commenterId'), c.get('commenter_username'))
    for log in dm_logs:
        if not log.get('is_echo'):
            add_contact(log.get('sender_id'), log.get('senderId'))
    for session in sessions:
        add_contact(session.get('recipient_id'), session.get('instagramUserId'))
    for conv in conversations:
        contact = conv.get('contact') or {}
        add_contact(contact.get('ig_id'), contact.get('instagramUserId'), contact.get('username'))
    for contact in contacts:
        if account or include_unscoped or not _dashboard_is_unscoped(contact):
            add_contact(contact.get('ig_id'), contact.get('instagramUserId'), contact.get('username'))

    from collections import OrderedDict
    today = datetime.utcnow().date()
    buckets = OrderedDict()
    for i in range(6, -1, -1):
        d = today - timedelta(days=i)
        buckets[d.isoformat()] = {
            'day': d.strftime('%a'),
            'date': d.isoformat(),
            'messages': 0,
            'conversions': 0,
        }

    def add_message(day: Optional[str], count: int = 1):
        if day in buckets and count > 0:
            buckets[day]['messages'] += count

    event_messages = 0
    for c in comments:
        sent_count = 0
        if c.get('replied') is True:
            sent_count += 1
        if (c.get('action_status') or c.get('actionStatus')) == 'success' and not c.get('replied'):
            sent_count += 1
        if sent_count:
            event_messages += sent_count
            add_message(_sent_day(c), sent_count)

    for log in dm_logs:
        if str(log.get('status') or '').lower() == 'replied':
            event_messages += 1
            add_message(_sent_day(log), 1)

    for session in sessions:
        if str(session.get('status') or '').lower() in {'completed', 'sent'}:
            event_messages += 1
            add_message(_sent_day(session), 1)

    for conv in conversations:
        for m in conv.get('messages', []) or []:
            is_outgoing = (m.get('from') == 'me' or m.get('from_') == 'me')
            failed = m.get('delivered') is False or m.get('status') in {'failed', 'skipped'}
            if is_outgoing and not failed:
                event_messages += 1
                add_message(_sent_day(m), 1)

    if automation_sent > event_messages:
        remaining = automation_sent - event_messages
        for auto in autos:
            if remaining <= 0:
                break
            count = min(int(auto.get('sent') or 0), remaining)
            add_message(_sent_day(auto), count)
            remaining -= count

    messages_sent = max(event_messages, automation_sent)
    conversions = 0
    conversion_rate = 0 if not contacts_seen else round((conversions / len(contacts_seen)) * 100, 1)
    weekly_performance = list(buckets.values())
    instagram_account_id = (account or {}).get('instagramAccountId') or (account or {}).get('igUserId')

    response = {
        'totalContacts': len(contacts_seen),
        'activeAutomations': active_automations,
        'messagesSent': messages_sent,
        'conversionRate': conversion_rate,
        'weeklyPerformance': weekly_performance,
        'instagram': {
            'connected': bool(account and account.get('connectionValid')),
            'username': (account or {}).get('username') or None,
            'activeAccountId': (account or {}).get('id') or None,
            'instagramAccountId': instagram_account_id or None,
        },
        'conversionTrackingImplemented': False,
        'commentsLogged': len(comments),
        # Backward-compatible keys used by older frontend bundles.
        'total_contacts': len(contacts_seen),
        'active_automations': active_automations,
        'messages_sent': messages_sent,
        'conversion_rate': conversion_rate,
        'weekly_chart': weekly_performance,
        'activeInstagramAccountId': (account or {}).get('id') or None,
        'current_instagram_account_id': instagram_account_id or None,
        'comments_logged': len(comments),
    }
    return response


# ---------------- Instagram OAuth (Business Login) ----------------
# Uses Instagram API with Business Login flow — required for the
# /{ig_user_id}/subscribed_apps endpoint to accept our access token.
# Facebook Login for Business (Pages) returns a Page token that the
# new IG Graph API rejects with "Application does not have the capability".
IG_SCOPES = (
    'instagram_business_basic,'
    'instagram_business_manage_messages,'
    'instagram_business_manage_comments'
)
VALID_IG_ACCOUNT_TYPES = {'BUSINESS', 'CREATOR', 'MEDIA_CREATOR'}
IG_OAUTH_STATE_TTL_SECONDS = 30 * 60


def _token_prefix(token: str) -> Optional[str]:
    return token[:6] if token else None


def _safe_return_to(return_to: Optional[str]) -> str:
    value = (return_to or '/app/settings?tab=instagram').strip()
    if not value.startswith('/app') or '://' in value or '\n' in value or '\r' in value:
        return '/app/settings?tab=instagram'
    return value


def _frontend_redirect_url(return_to: Optional[str], params: Optional[dict] = None) -> str:
    path = _safe_return_to(return_to)
    extra = urlencode(params or {})
    if extra:
        path = f"{path}{'&' if '?' in path else '?'}{extra}"
    return f"{FRONTEND_URL}{path}"


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode('ascii').rstrip('=')


def _b64url_decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + '=' * (-len(text) % 4))


def _sign_instagram_oauth_state(payload: Dict[str, Any]) -> str:
    state_payload = {
        **payload,
        'iat': int(datetime.utcnow().timestamp()),
    }
    body = _b64url_encode(json.dumps(
        state_payload,
        separators=(',', ':'),
        sort_keys=True,
    ).encode('utf-8'))
    sig = hmac.new(JWT_SECRET.encode('utf-8'), body.encode('ascii'), hashlib.sha256).digest()
    return f'{body}.{_b64url_encode(sig)}'


def _verify_instagram_oauth_state(state: Optional[str]) -> Optional[Dict[str, Any]]:
    if not state or '.' not in state or not JWT_SECRET:
        return None
    body, sig = state.rsplit('.', 1)
    expected = _b64url_encode(hmac.new(
        JWT_SECRET.encode('utf-8'),
        body.encode('ascii'),
        hashlib.sha256,
    ).digest())
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        payload = json.loads(_b64url_decode(body).decode('utf-8'))
    except Exception:
        return None
    user_id = payload.get('userId') or payload.get('user_id')
    issued_at = int(payload.get('iat') or 0)
    if not user_id or issued_at <= 0:
        return None
    if int(datetime.utcnow().timestamp()) - issued_at > IG_OAUTH_STATE_TTL_SECONDS:
        return None
    mode = payload.get('mode') or 'connect'
    if mode not in {'connect', 'add_account', 'reconnect'}:
        mode = 'connect'
    return {
        'userId': str(user_id),
        'mode': mode,
        'returnTo': _safe_return_to(payload.get('returnTo')),
        'iat': issued_at,
    }


def _safe_graph_error(body: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(body, dict):
        return None
    err = body.get('error')
    if not isinstance(err, dict):
        return None
    return {
        'message': err.get('message'),
        'type': err.get('type'),
        'code': err.get('code'),
        'error_subcode': err.get('error_subcode'),
        'fbtrace_id': err.get('fbtrace_id'),
    }


def _parse_graph_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if not value:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(value)
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
            return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
        except Exception:
            return None
    return None


def _iso_or_none(value: Any) -> Optional[str]:
    dt = _parse_graph_datetime(value)
    return dt.isoformat() if dt else None


def _instagram_account_doc_id(user_id: str, instagram_account_id: str) -> str:
    raw = f'{user_id}:{instagram_account_id}'
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _days_until(value: Any, now: Optional[datetime] = None) -> Optional[float]:
    dt = _parse_graph_datetime(value)
    if not dt:
        return None
    base = now or datetime.utcnow()
    return (dt - base).total_seconds() / 86400


def _token_refresh_public_row(account: dict, now: Optional[datetime] = None) -> dict:
    base = now or datetime.utcnow()
    expires_at = _parse_graph_datetime(account.get('tokenExpiresAt'))
    days = _days_until(expires_at, base) if expires_at else None
    refresh_status = account.get('refreshStatus') or 'unknown'
    expired = bool(expires_at and expires_at <= base)
    critical = bool(
        expired or
        (days is not None and days <= 3) or
        (days is not None and days <= 7 and refresh_status == 'failed') or
        int(account.get('refreshAttempts') or 0) >= 3
    )
    return {
        'accountId': account.get('id'),
        'instagramAccountId': account.get('instagramAccountId') or account.get('igUserId'),
        'username': account.get('username'),
        'tokenExpiresAt': expires_at.isoformat() if expires_at else None,
        'daysUntilExpiry': round(days, 2) if days is not None else None,
        'refreshStatus': refresh_status,
        'lastRefreshedAt': _iso_or_none(account.get('lastRefreshedAt')),
        'refreshAttempts': int(account.get('refreshAttempts') or 0),
        'critical': critical,
        'expired': expired,
    }


def _instagram_account_public_row(account: dict, active_account_id: str = '') -> dict:
    instagram_account_id = account.get('instagramAccountId') or account.get('igUserId')
    token_row = _token_refresh_public_row(account)
    return {
        'id': account.get('id'),
        'instagramAccountId': instagram_account_id,
        'igUserId': instagram_account_id,
        'username': account.get('username'),
        'profilePictureUrl': account.get('profilePictureUrl') or account.get('profile_picture_url'),
        'accountType': account.get('accountType'),
        'connectionValid': bool(account.get('connectionValid')),
        'isActive': bool(account.get('isActive')),
        'active': bool(active_account_id and account.get('id') == active_account_id),
        'isCurrent': bool(active_account_id and account.get('id') == active_account_id),
        'tokenSource': account.get('tokenSource'),
        'tokenStatus': {
            'refreshStatus': token_row.get('refreshStatus'),
            'tokenExpiresAt': token_row.get('tokenExpiresAt'),
            'daysUntilExpiry': token_row.get('daysUntilExpiry'),
            'critical': token_row.get('critical'),
            'expired': token_row.get('expired'),
        },
        'tokenExpiresAt': _iso_or_none(account.get('tokenExpiresAt')),
        'lastRefreshedAt': _iso_or_none(account.get('lastRefreshedAt')),
        'refreshStatus': account.get('refreshStatus') or 'unknown',
        'refreshAttempts': int(account.get('refreshAttempts') or 0),
        'createdAt': _iso_or_none(account.get('createdAt')),
        'updatedAt': _iso_or_none(account.get('updatedAt')),
    }


def _instagram_context_from_account(account_doc: Optional[dict]) -> dict:
    account_doc = account_doc or {}
    instagram_account_id = str(account_doc.get('instagramAccountId') or account_doc.get('igUserId') or '')
    username = (account_doc.get('username') or '').replace('@', '')
    return {
        'instagramAccountDbId': account_doc.get('id') or '',
        'instagram_account_id': account_doc.get('id') or '',
        'instagramAccountId': instagram_account_id,
        'igUserId': instagram_account_id,
        'instagramUsername': username,
    }


def _current_instagram_context(user_doc: dict) -> dict:
    return {
        'instagramAccountDbId': user_doc.get('active_instagram_account_id') or '',
        'instagram_account_id': user_doc.get('active_instagram_account_id') or '',
        'instagramAccountId': str(user_doc.get('ig_user_id') or ''),
        'igUserId': str(user_doc.get('ig_user_id') or ''),
        'instagramUsername': (user_doc.get('instagramHandle') or '').replace('@', ''),
    }


def _account_scoped_query(user_id: str, account_or_ig_id: Any) -> dict:
    query = {'user_id': user_id}
    if isinstance(account_or_ig_id, dict):
        account_id = str(
            account_or_ig_id.get('id') or
            account_or_ig_id.get('instagramAccountDbId') or
            account_or_ig_id.get('instagram_account_id') or
            ''
        )
        instagram_account_id = str(account_or_ig_id.get('instagramAccountId') or account_or_ig_id.get('igUserId') or '')
    else:
        account_id = ''
        instagram_account_id = str(account_or_ig_id or '')
    clauses = []
    if account_id:
        clauses.extend([
            {'instagramAccountDbId': account_id},
            {'instagram_account_id': account_id},
            {'instagramAccountId': account_id},
        ])
    if instagram_account_id:
        clauses.extend([
            {'instagramAccountId': instagram_account_id},
            {'igUserId': instagram_account_id},
        ])
    if clauses:
        query['$or'] = [
            clause for i, clause in enumerate(clauses)
            if clause not in clauses[:i]
        ]
    return query


def _with_instagram_account_context(user_doc: dict, account_doc: Optional[dict]) -> dict:
    if not account_doc:
        return user_doc
    instagram_account_id = account_doc.get('instagramAccountId') or account_doc.get('igUserId') or ''
    merged = {**user_doc}
    merged.update({
        'active_instagram_account_id': account_doc.get('id') or user_doc.get('active_instagram_account_id'),
        'ig_user_id': instagram_account_id,
        'meta_access_token': account_doc.get('accessToken') or user_doc.get('meta_access_token') or '',
        'instagramHandle': account_doc.get('username') or user_doc.get('instagramHandle') or '',
        'instagram_connection_valid': bool(account_doc.get('connectionValid')),
        'instagramConnectionValid': bool(account_doc.get('connectionValid')),
        'instagram_token_source': account_doc.get('tokenSource') or user_doc.get('instagram_token_source'),
        'instagramTokenSource': account_doc.get('tokenSource') or user_doc.get('instagramTokenSource'),
    })
    return merged


async def getActiveInstagramAccount(user_id: str) -> dict:
    """Return the server-side active Instagram account for this website user.

    The active account id is persisted on users.active_instagram_account_id.
    Legacy users.ig_user_id/meta_access_token are kept in sync for older paths,
    but new account-scoped code should use this helper's returned account doc.
    """
    user_doc = await db.users.find_one({'id': user_id})
    if not user_doc:
        raise HTTPException(404, 'User not found')
    await _sync_user_instagram_account_doc(user_doc)

    active_id = user_doc.get('active_instagram_account_id') or ''
    account = None
    if active_id:
        account = await db.instagram_accounts.find_one({
            'id': active_id,
            'userId': user_id,
            'isActive': {'$ne': False},
        })
    if not account:
        account = await db.instagram_accounts.find_one({
            'userId': user_id,
            'isCurrent': True,
            'isActive': {'$ne': False},
        })
    if not account and user_doc.get('ig_user_id'):
        account = await db.instagram_accounts.find_one({
            'userId': user_id,
            '$or': [
                {'instagramAccountId': str(user_doc.get('ig_user_id'))},
                {'igUserId': str(user_doc.get('ig_user_id'))},
            ],
            'isActive': {'$ne': False},
        })
    if not account:
        account = await db.instagram_accounts.find_one({
            'userId': user_id,
            'isActive': {'$ne': False},
        })
    if not account:
        raise HTTPException(400, 'No Instagram account connected')

    instagram_account_id = account.get('instagramAccountId') or account.get('igUserId') or ''
    token = account.get('accessToken') or ''
    now = datetime.utcnow()
    await db.instagram_accounts.update_many(
        {'userId': user_id},
        {'$set': {'isCurrent': False}},
    )
    await db.instagram_accounts.update_one(
        {'id': account['id'], 'userId': user_id},
        {'$set': {'isCurrent': True, 'updatedAt': now}},
    )
    await db.users.update_one(
        {'id': user_id},
        {'$set': {
            'active_instagram_account_id': account['id'],
            'instagramConnected': True,
            'instagram_connection_valid': bool(account.get('connectionValid')),
            'instagramConnectionValid': bool(account.get('connectionValid')),
            'instagram_connection_blocker': None if account.get('connectionValid') else 'selected_account_invalid',
            'instagramHandle': account.get('username') or '',
            'instagram_account_type': account.get('accountType'),
            'instagram_profile_picture_url': account.get('profilePictureUrl') or account.get('profile_picture_url'),
            'ig_user_id': instagram_account_id,
            'meta_access_token': token,
            'instagramTokenSource': account.get('tokenSource'),
            'instagram_token_source': account.get('tokenSource'),
            'tokenExpiresAt': _parse_graph_datetime(account.get('tokenExpiresAt')),
            'instagram_token_expires_at': _parse_graph_datetime(account.get('tokenExpiresAt')),
            'lastRefreshedAt': _parse_graph_datetime(account.get('lastRefreshedAt')),
            'refreshStatus': account.get('refreshStatus'),
            'refreshAttempts': int(account.get('refreshAttempts') or 0),
            'updated': now,
        }},
    )
    return account


async def _ensure_automation_account_scope_for_user(user_doc: dict) -> int:
    """Attach legacy unscoped automations/comments to the user's current IG account.

    This is intentionally conservative: it only fills missing account fields and
    never moves a rule that is already tied to another Instagram account.
    """
    user_id = user_doc.get('id')
    if not user_id:
        return 0
    accounts = await db.instagram_accounts.find({
        'userId': user_id,
        'isActive': {'$ne': False},
    }).to_list(2)
    if len(accounts) != 1:
        return 0
    ctx = _instagram_context_from_account(accounts[0])
    now = datetime.utcnow()
    missing_account = {'$or': [
        {'instagramAccountId': {'$exists': False}},
        {'instagramAccountId': None},
        {'instagramAccountId': ''},
    ]}
    auto_res = await db.automations.update_many(
        {'user_id': user_id, **missing_account},
        {'$set': {**ctx, 'updatedAt': now}},
    )
    await db.comments.update_many(
        {'user_id': user_id, **missing_account},
        {'$set': ctx},
    )
    await db.conversations.update_many(
        {'user_id': user_id, **missing_account},
        {'$set': ctx},
    )
    await db.dm_rules.update_many(
        {'user_id': user_id, **missing_account},
        {'$set': ctx},
    )
    await db.dm_logs.update_many(
        {'user_id': user_id, **missing_account},
        {'$set': ctx},
    )
    return getattr(auto_res, 'modified_count', 0) or 0


async def _find_user_doc_for_instagram_account_id(instagram_account_id: str) -> tuple:
    if not instagram_account_id:
        return None, None
    account_doc = await db.instagram_accounts.find_one({'$or': [
        {'instagramAccountId': instagram_account_id},
        {'igUserId': instagram_account_id},
    ], 'isActive': {'$ne': False}})
    if account_doc:
        owner = await db.users.find_one({'id': account_doc.get('userId') or account_doc.get('user_id')})
        if owner:
            return _with_instagram_account_context(owner, account_doc), 'instagram_accounts'

    user_doc = await db.users.find_one({'$or': [
        {'ig_user_id': instagram_account_id},
        {'fb_page_id': instagram_account_id},
    ]})
    if user_doc:
        return user_doc, 'users.ig_user_id'
    return None, None


def _cron_secret_is_valid(provided: Optional[str]) -> bool:
    return bool(CRON_SECRET and provided and hmac.compare_digest(str(provided), CRON_SECRET))


def _cron_secret_from_request(request: Request) -> Optional[str]:
    auth = request.headers.get('authorization') or request.headers.get('Authorization') or ''
    if auth.lower().startswith('bearer '):
        return auth.split(' ', 1)[1].strip()
    return (
        request.headers.get('x-cron-secret') or
        request.headers.get('X-Cron-Secret')
    )


async def notifyTokenRefreshProblem(message: str, metadata: Optional[dict] = None):
    """Placeholder alert hook. Replace with email/Slack/etc. when available."""
    safe_metadata = _redact_secrets(metadata or {})
    logger.warning('instagram_token_refresh_problem: %s metadata=%s', message, safe_metadata)


async def _sync_user_instagram_account_doc(
    user_doc: dict,
    access_token: Optional[str] = None,
    token_expires_at: Optional[datetime] = None,
    token_source: Optional[str] = None,
    refresh_status: Optional[str] = None,
    last_refreshed_at: Optional[datetime] = None,
) -> Optional[dict]:
    """Mirror the current legacy users.* Instagram connection into instagram_accounts.

    The production code still reads users.meta_access_token, so this collection is
    additive/backwards-compatible and lets token refresh jobs work per account.
    """
    user_id = user_doc.get('id')
    instagram_account_id = str(user_doc.get('ig_user_id') or '')
    token = access_token if access_token is not None else (user_doc.get('meta_access_token') or '')
    if not (user_id and instagram_account_id and token):
        return None
    now = datetime.utcnow()
    deterministic_account_id = _instagram_account_doc_id(user_id, instagram_account_id)
    existing = await db.instagram_accounts.find_one({'$or': [
        {'id': deterministic_account_id},
        {'userId': user_id, 'instagramAccountId': instagram_account_id},
        {'userId': user_id, 'igUserId': instagram_account_id},
    ]})
    account_id = (existing or {}).get('id') or deterministic_account_id
    created_at = (
        _parse_graph_datetime((existing or {}).get('createdAt')) or
        _parse_graph_datetime(user_doc.get('instagram_connected_at')) or
        _parse_graph_datetime(user_doc.get('createdAt')) or
        _parse_graph_datetime(user_doc.get('created')) or
        now
    )
    expires_at = (
        token_expires_at or
        _parse_graph_datetime(user_doc.get('tokenExpiresAt')) or
        _parse_graph_datetime(user_doc.get('instagram_token_expires_at')) or
        _parse_graph_datetime((existing or {}).get('tokenExpiresAt'))
    )
    doc = {
        'id': account_id,
        'userId': user_id,
        'user_id': user_id,
        'instagramAccountId': instagram_account_id,
        'igUserId': instagram_account_id,
        'username': (user_doc.get('instagramHandle') or '').replace('@', ''),
        'accountType': user_doc.get('instagram_account_type'),
        'accessToken': token,
        'tokenSource': token_source or user_doc.get('instagram_token_source') or user_doc.get('instagramTokenSource'),
        'authKind': user_doc.get('ig_auth_kind') or 'instagram_business_login',
        'connectionValid': bool(user_doc.get('instagram_connection_valid')),
        'isActive': bool(user_doc.get('instagramConnected')),
        'tokenExpiresAt': expires_at,
        'lastRefreshedAt': last_refreshed_at or _parse_graph_datetime(user_doc.get('lastRefreshedAt')),
        'refreshStatus': refresh_status or user_doc.get('refreshStatus') or (existing or {}).get('refreshStatus') or 'unknown',
        'refreshError': (existing or {}).get('refreshError'),
        'refreshAttempts': int((existing or {}).get('refreshAttempts') or user_doc.get('refreshAttempts') or 0),
        'refreshLockedUntil': (existing or {}).get('refreshLockedUntil'),
        'metadata': {'source': 'users_legacy_connection'},
        'createdAt': created_at,
        'updatedAt': now,
    }
    await db.instagram_accounts.update_one(
        {'id': account_id},
        {
            '$set': doc,
            '$setOnInsert': {'connectedAt': created_at},
        },
        upsert=True,
    )
    await db.users.update_one(
        {'id': user_id, '$or': [
            {'active_instagram_account_id': {'$exists': False}},
            {'active_instagram_account_id': None},
            {'active_instagram_account_id': ''},
        ]},
        {'$set': {'active_instagram_account_id': account_id}},
    )
    stored = await db.instagram_accounts.find_one({'id': account_id})
    return stored


async def _ensure_instagram_account_docs_for_connected_users(limit: int = 1000) -> int:
    users = await db.users.find({
        'instagramConnected': True,
        'ig_user_id': {'$nin': [None, '']},
        'meta_access_token': {'$nin': [None, '']},
    }).limit(limit).to_list(limit)
    count = 0
    for user_doc in users:
        if await _sync_user_instagram_account_doc(user_doc):
            count += 1
    return count


async def _mark_instagram_account_expired(account: dict, reason: str = 'token_expired') -> dict:
    now = datetime.utcnow()
    update = {
        'refreshStatus': 'expired',
        'refreshError': {'reason': reason},
        'connectionValid': False,
        'refreshLockedUntil': None,
        'updatedAt': now,
    }
    await db.instagram_accounts.update_one({'id': account['id']}, {'$set': update})
    await db.users.update_one(
        {
            'id': account.get('userId') or account.get('user_id'),
            'ig_user_id': account.get('instagramAccountId') or account.get('igUserId'),
        },
        {'$set': {
            'instagram_connection_valid': False,
            'instagramConnectionValid': False,
            'instagram_connection_blocker': reason,
            'refreshStatus': 'expired',
            'updated': now,
        }},
    )
    await notifyTokenRefreshProblem('Instagram token expired; manual reconnect required', {
        'accountId': account.get('id'),
        'instagramAccountId': account.get('instagramAccountId') or account.get('igUserId'),
        'userId': account.get('userId') or account.get('user_id'),
    })
    return {'ok': False, 'status': 'expired', 'reason': reason}


async def refreshInstagramToken(accountId: str, force: bool = False) -> dict:
    """Refresh one long-lived Instagram token without blocking webhook/comment paths."""
    now = datetime.utcnow()
    account = await db.instagram_accounts.find_one({'id': accountId})
    if not account:
        return {'ok': False, 'status': 'not_found', 'accountId': accountId}

    token = account.get('accessToken') or ''
    if not token:
        await db.instagram_accounts.update_one(
            {'id': accountId},
            {'$set': {
                'refreshStatus': 'missing_token',
                'refreshError': {'reason': 'missing_access_token'},
                'updatedAt': now,
            }},
        )
        return {'ok': False, 'status': 'missing_token', 'accountId': accountId}

    expires_at = _parse_graph_datetime(account.get('tokenExpiresAt'))
    days = _days_until(expires_at, now) if expires_at else None
    if expires_at and expires_at <= now:
        return await _mark_instagram_account_expired(account)

    token_source = account.get('tokenSource') or ''
    if token_source and token_source != 'long_lived':
        await db.instagram_accounts.update_one(
            {'id': accountId},
            {'$set': {
                'refreshStatus': 'not_long_lived',
                'refreshError': {'reason': 'token_source_is_not_long_lived'},
                'refreshLockedUntil': None,
                'updatedAt': now,
            }},
        )
        return {'ok': True, 'status': 'skipped_not_long_lived', 'accountId': accountId}

    if not force and expires_at and expires_at > now + timedelta(days=TOKEN_REFRESH_LOOKAHEAD_DAYS):
        return {'ok': True, 'status': 'skipped_not_due', 'accountId': accountId}

    recent_cutoff = now - timedelta(hours=TOKEN_REFRESH_MIN_AGE_HOURS)
    last_touch = (
        _parse_graph_datetime(account.get('lastRefreshedAt')) or
        _parse_graph_datetime(account.get('createdAt'))
    )
    if not force and last_touch and last_touch > recent_cutoff:
        return {'ok': True, 'status': 'skipped_recently_refreshed', 'accountId': accountId}

    locked = await db.instagram_accounts.find_one_and_update(
        {
            'id': accountId,
            '$or': [
                {'refreshLockedUntil': {'$exists': False}},
                {'refreshLockedUntil': None},
                {'refreshLockedUntil': {'$lte': now}},
            ],
        },
        {'$set': {
            'refreshLockedUntil': now + timedelta(minutes=TOKEN_REFRESH_LOCK_MINUTES),
            'refreshStatus': 'refreshing',
            'updatedAt': now,
        }},
        return_document=ReturnDocument.AFTER,
    )
    if not locked:
        return {'ok': True, 'status': 'skipped_locked', 'accountId': accountId}

    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(
                'https://graph.instagram.com/refresh_access_token',
                params={'grant_type': 'ig_refresh_token', 'access_token': token},
            )
            try:
                body = r.json()
            except Exception:
                body = {'raw': r.text[:500]}

        if r.status_code == 200 and isinstance(body, dict):
            new_token = body.get('access_token') or ''
            expires_in = int(body.get('expires_in') or 0)
            if new_token and expires_in > 0:
                new_expires_at = now + timedelta(seconds=expires_in)
                update = {
                    'accessToken': new_token,
                    'tokenExpiresAt': new_expires_at,
                    'lastRefreshedAt': now,
                    'refreshStatus': 'ok',
                    'refreshError': None,
                    'refreshAttempts': 0,
                    'refreshLockedUntil': None,
                    'updatedAt': now,
                    'connectionValid': True,
                    'isActive': True,
                }
                await db.instagram_accounts.update_one({'id': accountId}, {'$set': update})
                await db.users.update_one(
                    {
                        'id': locked.get('userId') or locked.get('user_id'),
                        'ig_user_id': locked.get('instagramAccountId') or locked.get('igUserId'),
                    },
                    {'$set': {
                        'meta_access_token': new_token,
                        'tokenExpiresAt': new_expires_at,
                        'instagram_token_expires_at': new_expires_at,
                        'lastRefreshedAt': now,
                        'refreshStatus': 'ok',
                        'refreshError': None,
                        'refreshAttempts': 0,
                        'instagram_connection_valid': True,
                        'instagramConnectionValid': True,
                        'instagram_connection_blocker': None,
                        'updated': now,
                    }},
                )
                logger.info('instagram_token_refresh_ok account=%s expires_at=%s',
                            accountId, new_expires_at.isoformat())
                return {
                    'ok': True,
                    'status': 'refreshed',
                    'accountId': accountId,
                    'tokenExpiresAt': new_expires_at.isoformat(),
                }
            body = {'error': {'message': 'refresh response missing access_token or expires_in',
                              'status': r.status_code, 'keys': sorted(body.keys())}}

        safe_error = _redact_secrets(body)
        attempts = int(locked.get('refreshAttempts') or 0) + 1
        refresh_status = 'failed'
        if days is not None and days <= 3:
            refresh_status = 'critical'
        await db.instagram_accounts.update_one(
            {'id': accountId},
            {'$set': {
                'refreshStatus': refresh_status,
                'refreshError': safe_error,
                'refreshLockedUntil': None,
                'updatedAt': now,
            }, '$inc': {'refreshAttempts': 1}},
        )
        await db.users.update_one(
            {
                'id': locked.get('userId') or locked.get('user_id'),
                'ig_user_id': locked.get('instagramAccountId') or locked.get('igUserId'),
            },
            {'$set': {
                'refreshStatus': refresh_status,
                'refreshError': safe_error,
                'refreshAttempts': attempts,
                'updated': now,
            }},
        )
        if (days is not None and days < 7) or attempts >= 3:
            await notifyTokenRefreshProblem('Instagram token refresh failed', {
                'accountId': accountId,
                'instagramAccountId': locked.get('instagramAccountId') or locked.get('igUserId'),
                'daysUntilExpiry': round(days, 2) if days is not None else None,
                'attempts': attempts,
                'status': refresh_status,
                'error': safe_error,
            })
        return {
            'ok': False,
            'status': refresh_status,
            'accountId': accountId,
            'error': safe_error,
        }
    except Exception as e:
        safe_error = {'exception': str(e)[:500]}
        await db.instagram_accounts.update_one(
            {'id': accountId},
            {'$set': {
                'refreshStatus': 'failed',
                'refreshError': safe_error,
                'refreshLockedUntil': None,
                'updatedAt': now,
            }, '$inc': {'refreshAttempts': 1}},
        )
        await notifyTokenRefreshProblem('Instagram token refresh exception', {
            'accountId': accountId,
            'error': safe_error,
        })
        return {'ok': False, 'status': 'failed', 'accountId': accountId, 'error': safe_error}


async def runInstagramTokenRefreshCron() -> dict:
    await _ensure_instagram_account_docs_for_connected_users()
    now = datetime.utcnow()
    lookahead = now + timedelta(days=TOKEN_REFRESH_LOOKAHEAD_DAYS)
    accounts = await db.instagram_accounts.find({
        'isActive': {'$ne': False},
        'connectionValid': {'$ne': False},
        'accessToken': {'$nin': [None, '']},
        '$or': [
            {'tokenExpiresAt': {'$exists': False}},
            {'tokenExpiresAt': None},
            {'tokenExpiresAt': {'$lte': lookahead}},
        ],
    }).sort('tokenExpiresAt', 1).to_list(500)

    summary = {
        'totalChecked': len(accounts),
        'refreshed': 0,
        'skipped': 0,
        'failed': 0,
        'expiringSoon': 0,
        'critical': 0,
        'expired': 0,
        'results': [],
    }
    for account in accounts:
        row = _token_refresh_public_row(account, now)
        if row.get('daysUntilExpiry') is None or row['daysUntilExpiry'] <= TOKEN_REFRESH_LOOKAHEAD_DAYS:
            summary['expiringSoon'] += 1
        result = await refreshInstagramToken(account['id'])
        status = result.get('status')
        if status == 'refreshed':
            summary['refreshed'] += 1
        elif status in ('failed', 'critical'):
            summary['failed'] += 1
        elif status == 'expired':
            summary['expired'] += 1
            summary['failed'] += 1
        else:
            summary['skipped'] += 1
        refreshed_account = await db.instagram_accounts.find_one({'id': account['id']}) or account
        public_row = _token_refresh_public_row(refreshed_account)
        public_row['result'] = status
        summary['critical'] += 1 if public_row.get('critical') else 0
        summary['results'].append(public_row)
        if (
            public_row.get('expired') or
            (
                public_row.get('daysUntilExpiry') is not None and
                public_row['daysUntilExpiry'] < 3 and
                status != 'refreshed'
            )
        ):
            await notifyTokenRefreshProblem('Instagram token expiry critical', {
                'accountId': public_row.get('accountId'),
                'instagramAccountId': public_row.get('instagramAccountId'),
                'daysUntilExpiry': public_row.get('daysUntilExpiry'),
                'refreshStatus': public_row.get('refreshStatus'),
                'refreshAttempts': public_row.get('refreshAttempts'),
                'result': status,
            })
    logger.info('instagram_token_refresh_summary %s', {
        k: v for k, v in summary.items() if k != 'results'
    })
    return summary


def _comment_rule_trigger_value(rule: dict) -> str:
    trigger = rule.get('trigger') or ''
    if trigger:
        return str(trigger)
    for node in rule.get('nodes') or []:
        if node.get('type') == 'trigger':
            data = node.get('data') or {}
            node_trigger = data.get('trigger') or ''
            if node_trigger:
                return str(node_trigger)
    return ''


def _is_comment_automation_rule(rule: dict) -> bool:
    return _comment_rule_trigger_value(rule).lower().startswith('comment:')


async def _debug_token_with_ig_app(token: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        'debugTokenWorks': False,
        'tokenAppId': None,
        'matchesIgAppId': False,
        'scopes': [],
        'isValid': False,
        'expiresAt': None,
        'error': None,
    }
    if not token:
        out['error'] = 'token_missing'
        return out
    if not IG_APP_ID or not IG_APP_SECRET:
        out['error'] = 'ig_app_credentials_missing'
        return out
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                'https://graph.instagram.com/debug_token',
                params={
                    'input_token': token,
                    'access_token': f'{IG_APP_ID}|{IG_APP_SECRET}',
                },
            )
            body = r.json() if r.headers.get('content-type', '').startswith('application/json') else {}
            if r.status_code == 200:
                d = body.get('data') or {}
                token_app_id = str(d.get('app_id') or '') or None
                out.update({
                    'debugTokenWorks': True,
                    'tokenAppId': token_app_id,
                    'matchesIgAppId': bool(token_app_id and token_app_id == IG_APP_ID),
                    'scopes': d.get('scopes') or d.get('granular_scopes') or [],
                    'isValid': bool(d.get('is_valid')),
                    'expiresAt': d.get('expires_at'),
                })
            else:
                out['error'] = _safe_graph_error(body) or {'status': r.status_code}
    except Exception as e:
        out['error'] = str(e)[:200]
    return out


async def _verify_instagram_token(
    c: httpx.AsyncClient,
    token: str,
    oauth_user_id: str = '',
) -> Dict[str, Any]:
    fields = 'id,user_id,username,account_type'
    probes = [
        ('graph.instagram.com/me', 'https://graph.instagram.com/me'),
        ('graph.instagram.com/v21.0/me', 'https://graph.instagram.com/v21.0/me'),
    ]
    if oauth_user_id:
        probes.extend([
            ('graph.instagram.com/{oauth_user_id}', f'https://graph.instagram.com/{oauth_user_id}'),
            ('graph.instagram.com/v21.0/{oauth_user_id}', f'https://graph.instagram.com/v21.0/{oauth_user_id}'),
        ])

    probe_results: Dict[str, Any] = {}
    body: Dict[str, Any] = {}
    chosen_probe = None
    for label, url in probes:
        r = await c.get(url, params={'fields': fields, 'access_token': token})
        try:
            probe_body = r.json()
        except Exception:
            probe_body = {'raw': r.text[:300]}
        entry: Dict[str, Any] = {
            'status': r.status_code,
            'bodyKeys': sorted(probe_body.keys()) if isinstance(probe_body, dict) else [],
        }
        err = _safe_graph_error(probe_body)
        if err:
            entry['error'] = err
        probe_results[label] = entry
        if r.status_code == 200 and isinstance(probe_body, dict):
            body = probe_body
            chosen_probe = label
            break

    if not chosen_probe:
        first = next(iter(probe_results.values()), {})
        return {
            'ok': False,
            'status': first.get('status'),
            'probes': probe_results,
            'error': first.get('error') or {'message': 'profile_probe_failed'},
            'blocker': 'token_cannot_call_graph_me',
            'fix': 'Disconnect and reconnect Instagram, then verify a profile probe before saving the token.',
        }

    canonical_id = str(body.get('user_id') or body.get('id') or '')
    username = body.get('username') or ''
    account_type = body.get('account_type') or ''
    if not canonical_id:
        return {
            'ok': False,
            'status': 200,
            'probeUsed': chosen_probe,
            'probes': probe_results,
            'bodyKeys': sorted(body.keys()),
            'error': {'message': 'Graph /me did not return id or user_id'},
            'blocker': 'graph_me_missing_canonical_id',
        }
    if not username:
        return {
            'ok': False,
            'status': 200,
            'probeUsed': chosen_probe,
            'probes': probe_results,
            'bodyKeys': sorted(body.keys()),
            'error': {'message': 'Profile probe did not return username'},
            'blocker': 'token_cannot_read_profile',
        }
    if account_type not in VALID_IG_ACCOUNT_TYPES:
        return {
            'ok': False,
            'status': 200,
            'probeUsed': chosen_probe,
            'probes': probe_results,
            'bodyKeys': sorted(body.keys()),
            'error': {'message': f'Unsupported account_type: {account_type or "missing"}'},
            'blocker': 'instagram_account_type_not_supported',
        }
    return {
        'ok': True,
        'status': 200,
        'probeUsed': chosen_probe,
        'probes': probe_results,
        'bodyKeys': sorted(body.keys()),
        'canonicalIgId': canonical_id,
        'graphMeId': str(body.get('id') or ''),
        'graphMeUserId': str(body.get('user_id') or ''),
        'username': username,
        'accountType': account_type,
    }


async def _run_instagram_me_probes(c: httpx.AsyncClient, token: str) -> Dict[str, Any]:
    """Run the exact /me probe order used to validate OAuth tokens."""
    variants = [
        ('/me?fields=user_id,username', 'https://graph.instagram.com/me',
         {'fields': 'user_id,username'}),
        ('/me?fields=id,username', 'https://graph.instagram.com/me',
         {'fields': 'id,username'}),
        ('/me', 'https://graph.instagram.com/me', {}),
        ('/v25.0/me?fields=user_id,username', 'https://graph.instagram.com/v25.0/me',
         {'fields': 'user_id,username'}),
        ('/v25.0/me?fields=id,username', 'https://graph.instagram.com/v25.0/me',
         {'fields': 'id,username'}),
        ('/v25.0/me', 'https://graph.instagram.com/v25.0/me', {}),
    ]
    results = []
    for label, url, extra_params in variants:
        params = {**extra_params, 'access_token': token}
        try:
            r = await c.get(url, params=params)
            try:
                body = r.json()
            except Exception:
                body = {'raw': r.text[:300]}
            canonical_id = ''
            username = ''
            if r.status_code == 200 and isinstance(body, dict):
                canonical_id = str(body.get('user_id') or body.get('id') or '')
                username = body.get('username') or ''
            item = {
                'variant': label,
                'status': r.status_code,
                'bodyKeys': sorted(body.keys()) if isinstance(body, dict) else [],
                'canonicalIgUserIdExists': bool(canonical_id),
                'usernameExists': bool(username),
            }
            err = _safe_graph_error(body)
            if err:
                item['error'] = err
            results.append(item)
            if r.status_code == 200 and canonical_id:
                return {
                    'ok': True,
                    'results': results,
                    'whichMeVariantWorks': label,
                    'canonicalIgUserId': canonical_id,
                    'username': username,
                    'bodyKeys': item['bodyKeys'],
                }
        except Exception as e:
            results.append({'variant': label, 'status': 0, 'error': str(e)[:200]})
    return {
        'ok': False,
        'results': results,
        'whichMeVariantWorks': None,
        'canonicalIgUserId': None,
        'username': None,
    }


@api.get('/instagram/auth-url')
async def instagram_auth_url(
    mode: str = Query('connect'),
    returnTo: str = Query('/app/settings?tab=instagram'),
    user_id: str = Depends(get_current_user_id),
):
    if not IG_APP_ID or not IG_APP_SECRET:
        raise HTTPException(503, 'IG_APP_ID and IG_APP_SECRET are not configured. Set them in .env')
    redirect_uri = f"{BACKEND_PUBLIC_URL}/api/instagram/callback"
    oauth_mode = mode if mode in {'connect', 'add_account', 'reconnect'} else 'connect'
    return_to = _safe_return_to(returnTo)
    state = _sign_instagram_oauth_state({
        'userId': user_id,
        'mode': oauth_mode,
        'returnTo': return_to,
    })
    params = {
        'enable_fb_login': '0',
        'force_authentication': '1',
        'client_id': IG_APP_ID,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': IG_SCOPES,
        'state': state,
    }
    url = f"https://www.instagram.com/oauth/authorize?{urlencode(params)}"
    return {
        'url': url,
        'configured': True,
        'mode': oauth_mode,
        'returnTo': return_to,
        'redirect_uri': redirect_uri,
        'authorizeUrlDebug': {
            'host': 'www.instagram.com',
            'clientIdLast4': IG_APP_ID[-4:] if IG_APP_ID else None,
            'redirect_uri': redirect_uri,
            'scope': IG_SCOPES,
            'response_type': 'code',
            'force_authentication': '1',
        },
    }


@api.get('/instagram/callback')
async def instagram_callback(request: Request,
                             code: Optional[str] = Query(None),
                             state: Optional[str] = Query(None),
                              error: str = Query(None), error_description: str = Query(None)):
    from fastapi.responses import RedirectResponse
    audit: Dict[str, Any] = {
        'callbackPath': '/api/instagram/callback',
        'requestQueryParamsReceived': sorted(request.query_params.keys()),
        'codeExists': bool(code),
        'redirectUriUsed': f"{BACKEND_PUBLIC_URL}/api/instagram/callback",
        'clientIdSource': INSTAGRAM_APP_ID_SOURCE,
        'clientSecretSource': INSTAGRAM_APP_SECRET_SOURCE,
        'tokenExchangeEndpoint': 'https://api.instagram.com/oauth/access_token',
        'tokenExchangeResponseKeys': [],
        'shortLivedAccessTokenExists': False,
        'userIdReturnedFromTokenExchange': None,
        'permissionsReturned': [],
        'longLivedExchangeAttempted': False,
        'longLivedExchangeEndpoint': 'GET https://graph.instagram.com/access_token',
        'longLivedExchangeStatus': None,
        'longLivedExchangeResponseKeys': [],
        'longLivedExchangeError': None,
        'finalTokenStoredSource': None,
        'finalTokenLength': None,
        'finalTokenPrefix': None,
        'finalIgUserIdStoredSource': None,
        'verification': None,
        'debugToken': None,
        'stateValid': False,
        'mode': None,
        'returnTo': None,
        'createdAt': datetime.utcnow(),
    }

    state_payload = _verify_instagram_oauth_state(state)
    user_id = state_payload.get('userId') if state_payload else None
    oauth_mode = state_payload.get('mode') if state_payload else None
    return_to = state_payload.get('returnTo') if state_payload else '/app/settings?tab=instagram'
    audit['stateValid'] = bool(state_payload)
    audit['mode'] = oauth_mode
    audit['returnTo'] = return_to

    async def _store_oauth_failure(
        uid: Optional[str],
        blocker: str,
        detail: Any = None,
        clear_existing_connection: bool = True,
    ):
        if not uid:
            return
        update = {
            '$set': {
                'instagram_connection_blocker': blocker,
                'last_instagram_connect_error': blocker,
                'ig_oauth_last_audit': _redact_secrets({**audit, 'failureDetail': detail}),
                'updated': datetime.utcnow(),
            },
        }
        if clear_existing_connection:
            update['$set'].update({
                'instagramConnected': False,
                'instagram_connection_valid': False,
                'instagramConnectionValid': False,
            })
            update['$unset'] = {
                'meta_access_token': '',
                'ig_user_id': '',
                'instagramHandle': '',
                'instagram_account_type': '',
                'instagram_graph_me_id': '',
                'instagram_graph_me_user_id': '',
            }
        await db.users.update_one({'id': uid}, update)

    clear_existing_on_failure = oauth_mode != 'add_account'
    if error:
        logger.warning('IG OAuth denied: %s — %s', error, error_description)
        await _store_oauth_failure(
            user_id,
            'oauth_denied',
            {'error': error},
            clear_existing_connection=clear_existing_on_failure,
        )
        return RedirectResponse(_frontend_redirect_url(return_to, {'ig': 'error', 'reason': error}))
    if not state_payload:
        return RedirectResponse(_frontend_redirect_url(
            '/app/settings?tab=instagram',
            {'ig': 'error', 'reason': 'invalid_state'},
        ))
    if not code:
        await _store_oauth_failure(
            user_id,
            'oauth_code_missing',
            clear_existing_connection=clear_existing_on_failure,
        )
        return RedirectResponse(_frontend_redirect_url(return_to, {'ig': 'error', 'reason': 'missing_code'}))
    redirect_uri = audit['redirectUriUsed']
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            # 1) Exchange code for short-lived IG user token (form-encoded POST)
            r = await c.post(
                'https://api.instagram.com/oauth/access_token',
                data={
                    'client_id': IG_APP_ID,
                    'client_secret': IG_APP_SECRET,
                    'grant_type': 'authorization_code',
                    'redirect_uri': redirect_uri,
                    'code': code,
                },
            )
            data = r.json() if r.status_code == 200 else {'raw': r.text, 'status': r.status_code}
            audit['tokenExchangeStatus'] = r.status_code
            audit['tokenExchangeResponseKeys'] = sorted(data.keys()) if isinstance(data, dict) else []
            token = data.get('access_token')
            ig_user_id_from_oauth = str(data.get('user_id') or '')
            audit['shortLivedAccessTokenExists'] = bool(token)
            audit['userIdReturnedFromTokenExchange'] = ig_user_id_from_oauth or None
            audit['permissionsReturned'] = data.get('permissions') or data.get('scope') or []
            if not token:
                safe = _redact_secrets(data)
                logger.error('IG token exchange failed: %s', safe)
                await _store_oauth_failure(
                    user_id,
                    'token_exchange_failed',
                    safe,
                    clear_existing_connection=clear_existing_on_failure,
                )
                return RedirectResponse(_frontend_redirect_url(
                    return_to,
                    {'ig': 'error', 'reason': 'token_exchange_failed'},
                ))

            # OAuth validation path: verify the short token first, then try to
            # upgrade to long-lived without making that upgrade a blocker.
            audit['apiVersionsTested'] = ['unversioned', 'v25.0']
            short_me = await _run_instagram_me_probes(c, token)
            audit['shortTokenLength'] = len(token)
            audit['shortTokenMeResults'] = short_me['results']
            if not short_me.get('ok'):
                audit['failureStage'] = 'me_verification'
                audit['blocker'] = 'token_exchange_returns_unusable_instagram_token'
                audit['whichTokenWorks'] = 'none'
                audit['whichMeVariantWorks'] = None
                audit['finalTokenSource'] = None
                audit['finalTokenStoredSource'] = None
                audit['connectionSaved'] = False
                await _store_oauth_failure(
                    user_id,
                    'token_exchange_returns_unusable_instagram_token',
                    {'shortTokenMeResults': short_me['results']},
                    clear_existing_connection=clear_existing_on_failure,
                )
                return RedirectResponse(_frontend_redirect_url(
                    return_to,
                    {'ig': 'error', 'reason': 'token_cannot_call_graph_me'},
                ))

            audit['debugToken'] = await _debug_token_with_ig_app(token)
            audit['longLivedExchangeAttempted'] = True
            audit['longLivedExchangeMethodUsed'] = 'GET'
            audit['longLivedExchangeEndpoint'] = 'GET https://graph.instagram.com/access_token'
            ll = await c.get(
                'https://graph.instagram.com/access_token',
                params={
                    'grant_type': 'ig_exchange_token',
                    'client_secret': IG_APP_SECRET,
                    'access_token': token,
                },
            )
            try:
                ll_data = ll.json()
            except Exception:
                ll_data = {'raw': ll.text[:300]}
            audit['longLivedExchangeStatus'] = ll.status_code
            audit['longLivedExchangeResponseKeys'] = sorted(ll_data.keys()) if isinstance(ll_data, dict) else []
            if ll.status_code != 200:
                audit['longLivedExchangeError'] = _safe_graph_error(ll_data) or ll_data

            final_token = token
            final_token_source = 'short_lived'
            final_me = short_me
            long_token = ll_data.get('access_token') if ll.status_code == 200 and isinstance(ll_data, dict) else None
            audit['longTokenExists'] = bool(long_token)
            audit['longTokenMeResults'] = None
            if long_token:
                long_me = await _run_instagram_me_probes(c, long_token)
                audit['longTokenMeResults'] = long_me['results']
                if long_me.get('ok'):
                    final_token = long_token
                    final_token_source = 'long_lived'
                    final_me = long_me
                else:
                    audit['warning'] = 'long_token_me_failed'
            else:
                audit['warning'] = 'long_lived_exchange_failed'

            audit['whichTokenWorks'] = final_token_source
            audit['whichMeVariantWorks'] = final_me.get('whichMeVariantWorks')
            audit['finalTokenSource'] = final_token_source
            audit['finalTokenStoredSource'] = final_token_source
            audit['finalTokenLength'] = len(final_token)
            audit['finalTokenPrefix'] = _token_prefix(final_token)
            audit['finalIgUserIdStoredSource'] = 'me_user_id_or_id'
            final_token_expires_at = None
            try:
                final_expires_in = (
                    int(ll_data.get('expires_in') or 0)
                    if final_token_source == 'long_lived' and isinstance(ll_data, dict)
                    else int(data.get('expires_in') or 0)
                )
            except Exception:
                final_expires_in = 0
            if final_expires_in > 0:
                final_token_expires_at = datetime.utcnow() + timedelta(seconds=final_expires_in)
            audit['tokenExpiresAt'] = final_token_expires_at.isoformat() if final_token_expires_at else None
            audit['verification'] = {
                'ok': True,
                'canonicalIgId': final_me['canonicalIgUserId'],
                'username': final_me.get('username') or '',
                'probeUsed': final_me.get('whichMeVariantWorks'),
            }
            audit['connectionSaved'] = True

            await db.users.update_one(
                {'id': user_id},
                {'$set': {
                    'instagramConnected': True,
                    'instagram_connection_valid': True,
                    'instagramConnectionValid': True,
                    'instagram_connection_blocker': None,
                    'instagramHandle': final_me.get('username') or '',
                    'ig_user_id': final_me['canonicalIgUserId'],
                    'meta_access_token': final_token,
                    'ig_auth_kind': 'instagram_business_login',
                    'instagramTokenSource': final_token_source,
                    'instagram_token_source': final_token_source,
                    'tokenExpiresAt': final_token_expires_at,
                    'instagram_token_expires_at': final_token_expires_at,
                    'lastRefreshedAt': datetime.utcnow() if final_token_source == 'long_lived' else None,
                    'refreshStatus': 'ok' if final_token_source == 'long_lived' else 'not_long_lived',
                    'refreshError': None,
                    'refreshAttempts': 0,
                    'ig_oauth_last_audit': _redact_secrets(audit),
                }},
            )
            account_doc = await _sync_user_instagram_account_doc({
                'id': user_id,
                'instagramConnected': True,
                'instagram_connection_valid': True,
                'instagramHandle': final_me.get('username') or '',
                'ig_user_id': final_me['canonicalIgUserId'],
                'meta_access_token': final_token,
                'ig_auth_kind': 'instagram_business_login',
                'instagram_token_source': final_token_source,
                'tokenExpiresAt': final_token_expires_at,
            }, access_token=final_token, token_expires_at=final_token_expires_at,
                token_source=final_token_source,
                refresh_status='ok' if final_token_source == 'long_lived' else 'not_long_lived',
                last_refreshed_at=datetime.utcnow() if final_token_source == 'long_lived' else None)
            connected_account_id = (account_doc or {}).get('id')
            if connected_account_id:
                audit['connectedInstagramAccountId'] = connected_account_id
                await db.users.update_one(
                    {'id': user_id},
                    {'$set': {'ig_oauth_last_audit': _redact_secrets(audit)}},
                )
                await instagram_account_activate(connected_account_id, user_id=user_id)
            logger.info('IG connected (Business Login) for user %s via %s',
                        user_id, audit['whichMeVariantWorks'])
            return RedirectResponse(_frontend_redirect_url(
                return_to,
                {'ig': 'connected', 'accountId': connected_account_id or ''},
            ))
    except HTTPException:
        raise
    except Exception as e:
        logger.exception('IG callback failed')
        from fastapi.responses import RedirectResponse
        await _store_oauth_failure(
            user_id,
            'server_error',
            str(e)[:200],
            clear_existing_connection=clear_existing_on_failure,
        )
        return RedirectResponse(_frontend_redirect_url(return_to, {'ig': 'error', 'reason': 'server_error'}))
    from fastapi.responses import RedirectResponse
    return RedirectResponse(_frontend_redirect_url(return_to, {'ig': 'connected'}))







@api.get('/instagram/oauth/last-attempt')
async def oauth_last_attempt(user_id: str = Depends(get_current_user_id)):
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    audit = u.get('ig_oauth_last_audit') or {}
    
    return {
        "callbackPath": audit.get("callbackPath", "/api/instagram/oauth/callback"),
        "codeReceived": audit.get("codeReceived", False),
        "codeLength": audit.get("codeLength", 0),
        "redirectUriUsedInAuthorize": audit.get("redirectUriUsedInAuthorize", ""),
        "redirectUriUsedInTokenExchange": audit.get("redirectUriUsedInTokenExchange", ""),
        "redirectUriExactMatch": audit.get("redirectUriExactMatch", False),
        "clientIdSource": audit.get("clientIdSource", "UNKNOWN"),
        "clientIdLast4": audit.get("clientIdLast4", ""),
        "clientSecretSource": audit.get("clientSecretSource", "UNKNOWN"),
        "tokenExchangeEndpoint": audit.get("tokenExchangeEndpoint", ""),
        "tokenExchangeStatus": audit.get("tokenExchangeStatus", 0),
        "tokenExchangeResponseKeys": audit.get("tokenExchangeResponseKeys", []),
        "shortTokenExists": audit.get("shortTokenExists", False),
        "shortTokenLength": audit.get("shortTokenLength", 0),
        "tokenExchangeUserId": audit.get("tokenExchangeUserId", ""),
        "longLivedExchangeAttempted": audit.get("longLivedExchangeAttempted", False),
        "longLivedExchangeEndpoint": audit.get("longLivedExchangeEndpoint", ""),
        "longLivedExchangeStatus": audit.get("longLivedExchangeStatus", 0),
        "longLivedResponseKeys": audit.get("longLivedResponseKeys", []),
        "longTokenExists": audit.get("longTokenExists", False),
        "longTokenLength": audit.get("longTokenLength", 0),
        "finalTokenSource": audit.get("finalTokenSource", "none"),
        "shortTokenMeStatus": audit.get("shortTokenMeStatus", 0),
        "shortTokenMeBody": audit.get("shortTokenMeBody", {}),
        "longTokenMeStatus": audit.get("longTokenMeStatus", 0),
        "longTokenMeBody": audit.get("longTokenMeBody", {}),
        "minimalMeStatus": audit.get("minimalMeStatus", 0),
        "minimalMeBody": audit.get("minimalMeBody", {}),
        "whichTokenWorks": audit.get("whichTokenWorks", "none"),
        "connectionSaved": audit.get("connectionSaved", False),
        "failureStage": audit.get("failureStage", audit.get("blocker", "unknown"))
    }

@api.delete('/admin/users/{email}')
async def admin_delete_user(email: str, user_id: str = Depends(get_current_user_id)):
    """Delete a user by email. Only allows self-deletion or deletion of test
    accounts (those with @test.com or @example.com emails). Never deletes the
    primary production user."""
    requester = await db.users.find_one({'id': user_id})
    if not requester:
        raise HTTPException(404, 'requester not found')
    target = await db.users.find_one({'email': email.lower()})
    if not target:
        target = await db.users.find_one({'email': email})
    if not target:
        raise HTTPException(404, f'user {email} not found')
    target_email = (target.get('email') or '').lower()
    is_self = target['id'] == user_id
    is_test = target_email.endswith('@test.com') or target_email.endswith('@example.com')
    if not is_self and not is_test:
        raise HTTPException(403, 'Cannot delete another production user')
    target_id = target['id']
    # Clean up associated data
    await db.dm_rules.delete_many({'user_id': target_id})
    await db.dm_logs.delete_many({'user_id': target_id})
    await db.automations.delete_many({'user_id': target_id})
    await db.conversations.delete_many({'user_id': target_id})
    await db.users.delete_one({'id': target_id})
    logger.info('admin_delete_user email=%s by=%s', email, user_id)
    return {'ok': True, 'deleted': email}


@api.get('/instagram/status')
async def instagram_status(user_id: str = Depends(get_current_user_id)):
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    connected = bool(u.get('instagramConnected') and u.get('instagram_connection_valid'))
    return {
        'connected': connected,
        'handle': u.get('instagramHandle'),
        'profilePictureUrl': u.get('instagram_profile_picture_url'),
        'followers': u.get('instagramFollowers', 0),
        'ig_user_id': u.get('ig_user_id'),
        'connectionValid': bool(u.get('instagram_connection_valid')),
        'connectionBlocker': u.get('instagram_connection_blocker'),
        'accountType': u.get('instagram_account_type'),
        'meta_configured': bool(META_APP_ID and META_APP_SECRET),
    }


@api.get('/instagram/profile')
async def instagram_profile(user_id: str = Depends(get_current_user_id)):
    """Return safe, user-scoped Instagram profile data for UI previews.

    Never returns the access token. If Graph does not expose a profile picture
    for the current token, we fall back to the stored account metadata.
    """
    account = await getActiveInstagramAccount(user_id)
    connected = bool(account.get('connectionValid') and account.get('accessToken'))
    out = {
        'connected': connected,
        'accountId': account.get('id'),
        'username': account.get('username') or None,
        'profilePictureUrl': account.get('profilePictureUrl') or account.get('profile_picture_url') or None,
        'igUserId': account.get('instagramAccountId') or account.get('igUserId') or None,
        'accountType': account.get('accountType') or None,
    }
    token = account.get('accessToken') or ''
    if not connected or not token:
        return out

    try:
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(
                'https://graph.instagram.com/me',
                params={
                    'access_token': token,
                    'fields': 'id,user_id,username,profile_picture_url,account_type',
                },
            )
            if r.status_code == 200:
                body = r.json() or {}
                username = body.get('username') or out['username']
                profile_picture_url = body.get('profile_picture_url') or out['profilePictureUrl']
                account_type = body.get('account_type') or out['accountType']
                canonical_id = str(body.get('user_id') or body.get('id') or out['igUserId'] or '')
                out.update({
                    'username': username,
                    'profilePictureUrl': profile_picture_url,
                    'igUserId': canonical_id or out['igUserId'],
                    'accountType': account_type,
                })
                await db.users.update_one(
                    {'id': user_id},
                    {'$set': {
                        'instagramHandle': username,
                        'instagram_profile_picture_url': profile_picture_url,
                        'instagram_account_type': account_type,
                    }},
                )
                await db.instagram_accounts.update_one(
                    {'id': account['id'], 'userId': user_id},
                    {'$set': {
                        'username': username,
                        'profilePictureUrl': profile_picture_url,
                        'accountType': account_type,
                        'updatedAt': datetime.utcnow(),
                    }},
                )
            else:
                out['profilePictureUnavailable'] = True
    except Exception as e:
        out['profilePictureUnavailable'] = True
        out['error'] = str(e)[:160]
    return out


@api.post('/instagram/subscribe-webhook')
async def instagram_subscribe_webhook(user_id: str = Depends(get_current_user_id)):
    """Force-subscribe the user's connected IG user to webhook fields via
    Instagram API (graph.instagram.com). Requires an IG user access token
    obtained through Instagram Business Login."""
    account = await getActiveInstagramAccount(user_id)
    token = account.get('accessToken') or ''
    ig_user_id = account.get('instagramAccountId') or account.get('igUserId') or ''
    if not ig_user_id:
        raise HTTPException(400, 'ig_user_id missing — reconnect Instagram')
    async with httpx.AsyncClient(timeout=20) as c:
        try:
            ig_sub = await c.post(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={
                    'access_token': token,
                    'subscribed_fields': (
                        'comments,messages,messaging_postbacks,'
                        'messaging_seen,message_reactions,live_comments'
                    ),
                },
            )
            verify = await c.get(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={'access_token': token},
            )
            return {
                'ok': ig_sub.status_code == 200,
                'ig_user_id': ig_user_id,
                'subscribe_status': ig_sub.status_code,
                'subscribe_body': ig_sub.text[:500],
                'verify_status': verify.status_code,
                'verify_body': verify.json() if verify.status_code == 200 else verify.text[:500],
            }
        except Exception as e:
            raise HTTPException(500, str(e))


@api.post('/instagram/subscribe-webhook-legacy')
async def instagram_subscribe_webhook_legacy(user_id: str = Depends(get_current_user_id)):
    """Legacy Page-based subscribe (kept for old Facebook-Login-flow users)."""
    u = await db.users.find_one({'id': user_id})
    if not _has_valid_instagram_connection(u):
        raise HTTPException(400, _instagram_connection_error(u))
    token = u.get('meta_access_token', '')
    # Look up page id fresh. The stored token is usually a PAGE access token
    # (set during OAuth callback) so /me/accounts may return nothing — fall
    # back to /me which, for a page token, returns the Page itself.
    async with httpx.AsyncClient(timeout=20) as c:
        page_id = None
        page_token = token
        accs = await c.get('https://graph.facebook.com/v21.0/me/accounts',
                           params={'access_token': token,
                                   'fields': 'id,name,access_token,instagram_business_account'})
        data = accs.json().get('data', []) if accs.status_code == 200 else []
        for acc in data:
            if acc.get('instagram_business_account'):
                page_id = acc.get('id')
                page_token = acc.get('access_token') or token
                break
        if not page_id:
            # Page-token fallback: /me returns the page itself
            me = await c.get('https://graph.facebook.com/v21.0/me',
                             params={'access_token': token,
                                     'fields': 'id,name,instagram_business_account'})
            if me.status_code == 200:
                mb = me.json()
                if mb.get('instagram_business_account') or mb.get('id'):
                    page_id = mb.get('id')
                    page_token = token
        if not page_id and u.get('fb_page_id'):
            page_id = u['fb_page_id']
            page_token = token
        if not page_id:
            raise HTTPException(404, f'No page found. /me/accounts={accs.text[:200]}')
        # Try each field independently so a missing permission on one
        # doesn't block the others.
        candidate_fields = [
            'feed',
            'messages', 'messaging_postbacks', 'messaging_optins',
            'message_deliveries', 'message_reads',
            'message_reactions', 'messaging_referrals',
        ]
        field_results = {}
        any_ok = False
        for f in candidate_fields:
            try:
                r = await c.post(
                    f"https://graph.facebook.com/v21.0/{page_id}/subscribed_apps",
                    params={'access_token': page_token, 'subscribed_fields': f},
                )
                field_results[f] = {'status': r.status_code, 'body': r.text[:300]}
                if r.status_code == 200:
                    any_ok = True
            except Exception as e:
                field_results[f] = {'status': 0, 'body': str(e)}
        ok = any_ok
        import json as _json
        body = _json.dumps(field_results)[:2000]
        # Keep `sub` defined for the legacy return keys
        class _S: pass
        sub = _S()
        sub.status_code = 200 if any_ok else 403
        sub.text = body
        # Verify the subscription
        verify = await c.get(f"https://graph.facebook.com/v21.0/{page_id}/subscribed_apps",
                             params={'access_token': page_token})
        # Also subscribe the Instagram user itself (required for comments/mentions
        # webhooks to route to our app under the new IG Graph API).
        ig_user_id = u.get('ig_user_id', '')
        ig_sub_status = None
        ig_sub_body = None
        if ig_user_id:
            try:
                ig_sub = await c.post(
                    f"https://graph.facebook.com/v21.0/{ig_user_id}/subscribed_apps",
                    params={'access_token': page_token,
                            'subscribed_fields': 'comments,messages,mentions,message_reactions,live_comments'},
                )
                ig_sub_status = ig_sub.status_code
                ig_sub_body = ig_sub.text
            except Exception as e:
                ig_sub_body = str(e)
        # Persist legacy Page credentials separately. Do not overwrite
        # meta_access_token, which must remain the verified IG user token.
        await db.users.update_one(
            {'id': user_id},
            {'$set': {'fb_page_id': page_id, 'fb_page_access_token': page_token}},
        )
        return {'ok': ok, 'status': sub.status_code, 'body': body,
                'page_id': page_id, 'ig_user_id': ig_user_id,
                'ig_subscribe_status': ig_sub_status, 'ig_subscribe_body': ig_sub_body,
                'subscribed_apps': verify.json()}


@api.get('/instagram/force-resubscribe')
async def instagram_force_resubscribe(email: str, key: str, fields: str = ''):
    """Admin tool: DELETE then POST the user's webhook subscription to force
    Meta to re-establish delivery. DISABLED in production — use the
    authenticated /api/instagram/dm/resubscribe endpoint instead."""
    raise HTTPException(403, 'Disabled in production. Use /api/instagram/dm/resubscribe with JWT auth.')
    u = await db.users.find_one({'email': email.lower()})
    if not u:
        u = await db.users.find_one({'email': email})
    if not u:
        raise HTTPException(404, 'user not found')
    token = u.get('meta_access_token', '')
    ig_user_id = u.get('ig_user_id', '')
    if not (token and ig_user_id):
        raise HTTPException(400, 'user missing token or ig_user_id')
    want_fields = fields or 'comments,messages,messaging_postbacks,messaging_seen,message_reactions,live_comments'
    out = {'email': email, 'ig_user_id': ig_user_id, 'requested_fields': want_fields}
    async with httpx.AsyncClient(timeout=30) as c:
        try:
            d = await c.delete(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={'access_token': token},
            )
            out['delete'] = {'status': d.status_code, 'body': d.text[:500]}
        except Exception as e:
            out['delete'] = {'error': str(e)}
        try:
            p = await c.post(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={'access_token': token, 'subscribed_fields': want_fields},
            )
            out['post'] = {'status': p.status_code, 'body': p.text[:500]}
        except Exception as e:
            out['post'] = {'error': str(e)}
        try:
            g = await c.get(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={'access_token': token},
            )
            out['verify'] = {'status': g.status_code,
                             'body': g.json() if g.status_code == 200 else g.text[:500]}
        except Exception as e:
            out['verify'] = {'error': str(e)}
    return out


@api.get('/instagram/debug-dump')
async def instagram_debug_dump(email: str = '', key: str = '', media_id: str = ''):
    """FULL diagnostic dump — DISABLED in production.
    Use the authenticated diagnostic endpoints instead:
      GET /api/instagram/credentials/diagnostics
      GET /api/instagram/dm/debug-latest
      GET /api/instagram/oauth/last-attempt
    """
    raise HTTPException(403, 'Disabled in production. Use authenticated diagnostic endpoints with JWT auth.')
    token = u.get('meta_access_token', '')
    ig_user_id = u.get('ig_user_id', '')
    page_id = u.get('fb_page_id', '')
    automations = await db.automations.find({'user_id': u.get('id')}).to_list(100)
    for a in automations:
        a.pop('_id', None)
    out = {
        'user': {
            'id': u.get('id'),
            'email': u.get('email'),
            'instagramConnected': u.get('instagramConnected'),
            'instagramHandle': u.get('instagramHandle'),
            'ig_user_id': ig_user_id,
            'fb_page_id': page_id,
            'has_meta_token': bool(token),
            'meta_token_prefix': (token or '')[:20],
        },
        'automations': automations,
    }
    async with httpx.AsyncClient(timeout=20) as c:
        try:
            me = await c.get('https://graph.instagram.com/me',
                             params={'access_token': token,
                                     'fields': 'user_id,username,name,account_type,followers_count'})
            out['graph_me'] = {'status': me.status_code,
                               'body': me.json() if me.status_code == 200 else me.text[:300]}
        except Exception as e:
            out['graph_me'] = {'error': str(e)}
        if ig_user_id:
            try:
                igs = await c.get(f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                                  params={'access_token': token})
                out['ig_subscribed_apps'] = {
                    'status': igs.status_code,
                    'body': igs.json() if igs.status_code == 200 else igs.text[:500]
                }
            except Exception as e:
                out['ig_subscribed_apps'] = {'error': str(e)}
            try:
                igp = await c.post(
                    f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                    params={'access_token': token,
                            'subscribed_fields': 'comments,messages,messaging_postbacks,messaging_seen,message_reactions,live_comments'},
                )
                out['ig_subscribe_attempt'] = {'status': igp.status_code, 'body': igp.text[:500]}
            except Exception as e:
                out['ig_subscribe_attempt'] = {'error': str(e)}
    # Include recent webhook deliveries for diagnosis
    try:
        hooks = await db.webhook_log.find().sort('received', -1).limit(30).to_list(30)
        for h in hooks:
            h.pop('_id', None)
            if isinstance(h.get('received'), datetime):
                h['received'] = h['received'].isoformat()
        out['recent_webhooks'] = hooks
        out['webhook_count'] = await db.webhook_log.count_documents({})
    except Exception as e:
        out['recent_webhooks'] = {'error': str(e)}
    # Fetch comments directly from IG Graph API to see whether the "HI" comment
    # even reached IG. Use media_id from query if provided, else fall back to
    # every automation's trigger_media_id.
    target_media_ids: list = []
    if media_id:
        target_media_ids.append(media_id)
    for a in automations:
        mid = a.get('trigger_media_id') or a.get('media_id')
        if mid and mid not in target_media_ids:
            target_media_ids.append(mid)
    if token and target_media_ids:
        media_checks = {}
        async with httpx.AsyncClient(timeout=20) as c:
            for mid in target_media_ids[:5]:
                try:
                    # Fetch comments on the media
                    cr = await c.get(
                        f'https://graph.instagram.com/{mid}/comments',
                        params={'access_token': token,
                                'fields': 'id,text,username,timestamp,from',
                                'limit': 25},
                    )
                    body = cr.json() if cr.status_code == 200 else cr.text[:500]
                    # Also fetch basic media info
                    mr = await c.get(
                        f'https://graph.instagram.com/{mid}',
                        params={'access_token': token,
                                'fields': 'id,caption,comments_count,like_count,timestamp,permalink,media_type'},
                    )
                    minfo = mr.json() if mr.status_code == 200 else mr.text[:300]
                    media_checks[mid] = {
                        'comments_status': cr.status_code,
                        'comments': body,
                        'media_info_status': mr.status_code,
                        'media_info': minfo,
                    }
                except Exception as e:
                    media_checks[mid] = {'error': str(e)}
        out['media_checks'] = media_checks
    return out


@api.get('/instagram/media')
async def instagram_media(user_id: str = Depends(get_current_user_id), limit: int = 25):
    """List the user's recent Instagram posts via Graph API.

    Always uses /me/media as the primary endpoint (Instagram Business Login).
    If /me/media fails, optionally tries /{ig_user_id}/media — but the
    response is always shaped consistently:

      ok=true   when /me/media returned 200 (count may still be 0)
      ok=false  when /me/media failed; error details + optional fallback info

    The wizard never sees the /{ig_user_id}/media error if /me/media succeeded.
    """
    account = await getActiveInstagramAccount(user_id)
    if not account.get('connectionValid'):
        raise HTTPException(400, 'Instagram account is not connected')
    token = account.get('accessToken', '')
    ig_id = str(account.get('instagramAccountId') or account.get('igUserId') or '')
    if not token:
        raise HTTPException(400, 'Missing access token')
    fields = 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,comments_count'
    lim = max(1, min(limit, 50))
    me_id_for_debug = None
    
    endpoints = []
    endpoints.append(('https://graph.instagram.com/me/media', '/me/media'))
    endpoints.append(('https://graph.instagram.com/v21.0/me/media', '/v21.0/me/media'))
    if ig_id:
        endpoints.append((f'https://graph.instagram.com/{ig_id}/media', f'/{ig_id}/media'))
        endpoints.append((f'https://graph.facebook.com/v21.0/{ig_id}/media', f'graph.facebook.com/v21.0/{ig_id}/media'))

    errors = {}
    
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            try:
                mer = await c.get('https://graph.instagram.com/me', params={'access_token': token, 'fields': 'user_id,id,username'})
                if mer.status_code == 200:
                    me_id_for_debug = str((mer.json() or {}).get('user_id') or (mer.json() or {}).get('id') or '') or None
            except Exception:
                pass

            for url, label in endpoints:
                try:
                    r = await c.get(url, params={'access_token': token, 'fields': fields, 'limit': lim})
                    if r.status_code == 200:
                        items = (r.json() or {}).get('data') or []
                        return {
                            'ok': True,
                            'accountId': account.get('id'),
                            'endpointUsed': label,
                            'media': items,
                            'items': items,
                            'count': len(items),
                            'warning': None if items else f'No media returned from {label}',
                            'errors': errors,
                            'graphMeId': me_id_for_debug,
                            'dbIgUserId': ig_id or None,
                            'idMatch': (bool(me_id_for_debug) and me_id_for_debug == ig_id) if ig_id else None,
                        }
                    else:
                        errors[label] = {'status': r.status_code, 'body': r.text[:500]}
                except Exception as e:
                    errors[label] = {'exception': str(e)[:200]}

        return {
            'ok': False,
            'accountId': account.get('id'),
            'endpointUsed': None,
            'media': [],
            'items': [],
            'count': 0,
            'error': {'body': 'All media endpoints failed', 'details': errors},
            'graphMeId': me_id_for_debug,
            'dbIgUserId': ig_id or None,
        }
    except Exception as e:
        logger.exception('IG media fetch failed')
        return {
            'ok': False,
            'media': [],
            'items': [],
            'count': 0,
            'error': {'body': str(e)},
        }


@api.get('/instagram/media/diagnostics')
async def instagram_media_diagnostics(user_id: str = Depends(get_current_user_id)):
    """Self-diagnose why /instagram/media may be returning empty.

    Hits /me, /me/media, and /{ig_user_id}/media against graph.instagram.com
    using the stored long-lived IG user token. Compares the Graph /me id
    with the value persisted in users.ig_user_id and returns a structured
    blocker classification. Never returns the raw token.
    """
    u = await db.users.find_one({'id': user_id})
    out: Dict[str, Any] = {
        'connected': False,
        'dbIgUserId': None,
        'graphMeId': None,
        'idMatch': None,
        'username': None,
        'accountType': None,
        'tokenExists': False,
        'tokenValid': None,
        'tokenLength': 0,
        'authKind': None,
        'mediaEndpointUsed': None,
        'meMediaCount': None,
        'igUserMediaCount': None,
        'mediaCount': 0,
        'firstMediaPreview': None,
        'errors': {},
        'blocker': None,
    }
    if not u:
        out['blocker'] = 'user_not_found'
        return out
    out['connected'] = _has_valid_instagram_connection(u)
    db_ig_id = str(u.get('ig_user_id') or '')
    token = u.get('meta_access_token', '') or ''
    out['dbIgUserId'] = db_ig_id or None
    out['tokenExists'] = bool(token)
    out['tokenLength'] = len(token)
    out['authKind'] = u.get('ig_auth_kind')
    if not out['connected']:
        out['blocker'] = u.get('instagram_connection_blocker') or 'token_cannot_call_graph_me'
        out['errors']['connection'] = _instagram_connection_error(u)
        return out
    if not token:
        out['blocker'] = 'token_missing'
        return out

    async with httpx.AsyncClient(timeout=20) as c:
        # 1) /me
        try:
            r = await c.get(
                'https://graph.instagram.com/me',
                params={
                    'access_token': token,
                    'fields': 'user_id,id,username,account_type',
                },
            )
            if r.status_code == 200:
                me = r.json() or {}
                out['graphMeId'] = str(me.get('user_id') or me.get('id') or '') or None
                out['username'] = me.get('username')
                out['accountType'] = me.get('account_type')
                out['tokenValid'] = True
            else:
                out['tokenValid'] = False
                out['errors']['me'] = {'status': r.status_code, 'body': r.text[:400]}
        except Exception as e:
            out['errors']['me'] = {'exception': str(e)}

        # 2) /me/media
        try:
            r = await c.get(
                'https://graph.instagram.com/me/media',
                params={
                    'access_token': token,
                    'fields': 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp',
                    'limit': 5,
                },
            )
            if r.status_code == 200:
                items = (r.json() or {}).get('data') or []
                out['meMediaCount'] = len(items)
                if items and not out['firstMediaPreview']:
                    p = items[0]
                    out['firstMediaPreview'] = {
                        'id': p.get('id'),
                        'media_type': p.get('media_type'),
                        'permalink': p.get('permalink'),
                        'timestamp': p.get('timestamp'),
                        'caption': (p.get('caption') or '')[:120],
                        'has_thumbnail': bool(p.get('thumbnail_url') or p.get('media_url')),
                    }
                    out['mediaEndpointUsed'] = '/me/media'
            else:
                out['errors']['me_media'] = {'status': r.status_code, 'body': r.text[:400]}
        except Exception as e:
            out['errors']['me_media'] = {'exception': str(e)}

        # 3) /{ig_user_id}/media using the DB id
        if db_ig_id:
            try:
                r = await c.get(
                    f'https://graph.instagram.com/{db_ig_id}/media',
                    params={
                        'access_token': token,
                        'fields': 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp',
                        'limit': 5,
                    },
                )
                if r.status_code == 200:
                    items = (r.json() or {}).get('data') or []
                    out['igUserMediaCount'] = len(items)
                    if items and not out['firstMediaPreview']:
                        p = items[0]
                        out['firstMediaPreview'] = {
                            'id': p.get('id'),
                            'media_type': p.get('media_type'),
                            'permalink': p.get('permalink'),
                            'timestamp': p.get('timestamp'),
                            'caption': (p.get('caption') or '')[:120],
                            'has_thumbnail': bool(p.get('thumbnail_url') or p.get('media_url')),
                        }
                        out['mediaEndpointUsed'] = f'/{{ig_user_id}}/media'
                else:
                    out['errors']['ig_user_media'] = {'status': r.status_code, 'body': r.text[:400]}
            except Exception as e:
                out['errors']['ig_user_media'] = {'exception': str(e)}

    if out['graphMeId'] and db_ig_id:
        out['idMatch'] = (out['graphMeId'] == db_ig_id)

    me_n = out['meMediaCount']
    ig_n = out['igUserMediaCount']
    out['mediaCount'] = max(me_n or 0, ig_n or 0)

    # Blocker classification
    if out['tokenValid'] is False:
        out['blocker'] = 'token_invalid_or_expired'
    elif out['idMatch'] is False:
        out['blocker'] = 'db_ig_user_id_mismatch_with_graph_me'
    elif (me_n is None) and (ig_n is None):
        out['blocker'] = 'graph_media_call_failed'
    elif (me_n == 0) and (ig_n in (0, None)):
        out['blocker'] = 'graph_returned_zero_media_check_account_type_or_posts_visibility'
    else:
        out['blocker'] = None

    return out


@api.get('/instagram/identity-matrix')
async def instagram_identity_matrix(user_id: str = Depends(get_current_user_id)):
    """Full token + identity + media endpoint matrix.

    Always returns 200 with a structured JSON envelope so the frontend
    can render the failure stage instead of a bare toast. Tokens are
    never echoed; only the env source names are reported.
    """
    logger.info('identity_matrix_started user=%s', user_id)
    partial: Dict[str, Any] = {
        'connected': False,
        'tokenExists': False,
        'dbIgUserId': None,
        'authKind': None,
        'igAppIdSource': INSTAGRAM_APP_ID_SOURCE,
    }

    def _fail(stage: str, exc: Optional[BaseException] = None,
              status: int = 500, message: Optional[str] = None) -> Dict[str, Any]:
        logger.exception('identity_matrix_failed stage=%s', stage)
        return {
            'ok': False,
            'stage': stage,
            'error': {
                'type': type(exc).__name__ if exc else 'Error',
                'message': message or (str(exc) if exc else 'unknown'),
                'safeDetail': (str(exc)[:300] if exc else None),
                'status': status,
            },
            'partial': partial,
        }

    try:
        u = await db.users.find_one({'id': user_id})
    except Exception as e:
        return _fail('db_lookup', e)
    if not u:
        return _fail('user_lookup', None, 404, 'user not found')
    logger.info('identity_matrix_user_loaded user=%s', user_id)

    token = u.get('meta_access_token', '') or ''
    db_ig_id = str(u.get('ig_user_id') or '')
    partial.update({
        'connected': bool(u.get('instagramConnected')),
        'tokenExists': bool(token),
        'dbIgUserId': db_ig_id or None,
        'authKind': u.get('ig_auth_kind'),
    })
    if not token:
        return _fail('token_missing', None, 400, 'No stored access token')
    logger.info('identity_matrix_token_present user=%s len=%d', user_id, len(token))

    fields_me = 'id,user_id,username,account_type'
    fields_media = (
        'id,caption,media_type,media_url,thumbnail_url,'
        'permalink,timestamp,comments_count'
    )

    async def _probe(c: httpx.AsyncClient, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            r = await c.get(url, params=params)
            entry: Dict[str, Any] = {'status': r.status_code}
            try:
                entry['body'] = r.json()
            except Exception:
                entry['body'] = r.text[:600]
            return entry
        except Exception as e:
            logger.warning('identity_matrix_me_probe_failed url=%s err=%s', url, e)
            return {'status': 0, 'body': {'exception': str(e)}}

    me_unver: Dict[str, Any] = {}
    me_ver: Dict[str, Any] = {}
    me_fb: Dict[str, Any] = {}
    matrix: List[Dict[str, Any]] = []
    ig_me_id: Optional[str] = None
    ig_me_user_id: Optional[str] = None
    username: Optional[str] = None
    account_type: Optional[str] = None

    try:
      async with httpx.AsyncClient(timeout=20) as c:
        # ---- /me probes ----
        logger.info('identity_matrix_me_probe_started user=%s', user_id)
        me_unver = await _probe(c, 'https://graph.instagram.com/me',
                                {'access_token': token, 'fields': fields_me})
        me_ver = await _probe(c, 'https://graph.instagram.com/v21.0/me',
                              {'access_token': token, 'fields': fields_me})
        me_fb = await _probe(c, 'https://graph.facebook.com/v21.0/me',
                             {'access_token': token, 'fields': 'id,name'})

        # Pick whichever IG /me worked
        for src in (me_unver, me_ver):
            if src.get('status') == 200 and isinstance(src.get('body'), dict):
                b = src['body']
                ig_me_id = ig_me_id or (str(b.get('id')) if b.get('id') is not None else None)
                ig_me_user_id = ig_me_user_id or (str(b.get('user_id')) if b.get('user_id') is not None else None)
                username = username or b.get('username')
                account_type = account_type or b.get('account_type')

        canonical = ig_me_user_id or ig_me_id or None
        id_match = bool(canonical and db_ig_id and canonical == db_ig_id)
        mismatch_reason = None
        if not id_match:
            if not canonical:
                mismatch_reason = 'graph_me_failed_or_did_not_return_id'
            elif not db_ig_id:
                mismatch_reason = 'db_ig_user_id_empty'
            else:
                mismatch_reason = f'canonical_{canonical}_!=_db_{db_ig_id}'

        # ---- /media probes ----
        async def add_media_probe(label: str, url: str):
            r = await _probe(c, url, {'access_token': token, 'fields': fields_media, 'limit': 5})
            count = 0
            err_code = None
            err_msg = None
            works = False
            body = r.get('body')
            if r.get('status') == 200 and isinstance(body, dict):
                count = len(body.get('data') or [])
                works = True
            elif isinstance(body, dict) and isinstance(body.get('error'), dict):
                err_code = body['error'].get('code')
                err_msg = body['error'].get('message')
            matrix.append({
                'endpoint': label,
                'status': r.get('status'),
                'count': count,
                'errorCode': err_code,
                'errorMessage': err_msg,
                'works': works,
            })

        if canonical:
            logger.info('identity_matrix_media_probe_started user=%s', user_id)
            await add_media_probe('GET graph.instagram.com/me/media',
                                  'https://graph.instagram.com/me/media')
            await add_media_probe('GET graph.instagram.com/v21.0/me/media',
                                  'https://graph.instagram.com/v21.0/me/media')
            if ig_me_id:
                await add_media_probe(f'GET graph.instagram.com/{{ig_me_id}}/media',
                                      f'https://graph.instagram.com/{ig_me_id}/media')
            if ig_me_user_id and ig_me_user_id != ig_me_id:
                await add_media_probe(f'GET graph.instagram.com/{{ig_me_user_id}}/media',
                                      f'https://graph.instagram.com/{ig_me_user_id}/media')
        else:
            await db.users.update_one(
                {'id': user_id},
                {'$set': {
                    'instagramConnected': False,
                    'instagram_connection_valid': False,
                    'instagram_connection_blocker': 'token_cannot_call_graph_me',
                    'updated': datetime.utcnow(),
                }},
            )
    except Exception as e:
        # Probe loop crashed mid-way. Return whatever we collected with a
        # structured failure envelope so the wizard can render it.
        partial.update({
            'meUnverStatus': me_unver.get('status') if me_unver else None,
            'matrixSoFar': matrix,
        })
        return _fail('me_probe' if not matrix else 'media_matrix', e)

    working = [m for m in matrix if m['works']]
    chosen = working[0]['endpoint'] if working else None

    # Surface OAuth credentials info (already redacted helpers exist but we
    # only echo source names + booleans, never values).
    cred_info = {
        'instagramAppIdSource': INSTAGRAM_APP_ID_SOURCE,
        'instagramAppSecretSource': INSTAGRAM_APP_SECRET_SOURCE,
        'instagramAppIdConfigured': bool(INSTAGRAM_APP_ID),
        'instagramAppSecretConfigured': bool(INSTAGRAM_APP_SECRET),
    }
    blocker = 'token_cannot_call_graph_me' if not canonical else None
    reconnect_recommended = bool(blocker or (canonical and db_ig_id and canonical != db_ig_id))
    debug_token = await _debug_token_with_ig_app(token)

    logger.info('identity_matrix_success user=%s chosen=%s', user_id, chosen)
    return {
        'ok': True,
        'blocker': blocker,
        'tokenLength': len(token),
        'authKind': u.get('ig_auth_kind'),
        'credentials': cred_info,
        'debugToken': debug_token,
        'oauthLastAudit': _redact_secrets(u.get('ig_oauth_last_audit')) if u.get('ig_oauth_last_audit') else None,
        'meProbes': {
            'graphInstagramMeUnversioned': me_unver,
            'graphInstagramMeVersioned': me_ver,
            'graphFacebookMeVersioned': me_fb,
        },
        'identity': {
            'dbIgUserId': db_ig_id or None,
            'instagramMeId': ig_me_id,
            'instagramMeUserId': ig_me_user_id,
            'bestCanonicalIgId': canonical,
            'username': username,
            'accountType': account_type,
            'idMatch': id_match,
            'mismatchReason': mismatch_reason,
            'sourceField': 'user_id' if ig_me_user_id else ('id' if ig_me_id else None),
        },
        'mediaMatrix': matrix,
        'chosenEndpoint': chosen,
        'reconnectRecommended': reconnect_recommended,
    }


async def _fetch_latest_media_id(access_token: str, ig_user_id: str) -> Optional[str]:
    if not access_token or not ig_user_id:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f'https://graph.instagram.com/{ig_user_id}/media',
                params={'access_token': access_token, 'fields': 'id,timestamp', 'limit': 1},
            )
            if r.status_code != 200:
                return None
            data = r.json().get('data') or []
            return data[0]['id'] if data else None
    except Exception:
        return None


async def _fetch_recent_media_ids(access_token: str, ig_user_id: str, limit: int = 10) -> list:
    if not access_token or not ig_user_id:
        return []
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f'https://graph.instagram.com/{ig_user_id}/media',
                params={
                    'access_token': access_token,
                    'fields': 'id,timestamp',
                    'limit': max(1, min(int(limit or 10), 25)),
                },
            )
            if r.status_code != 200:
                return []
            return [
                str(item.get('id'))
                for item in ((r.json() or {}).get('data') or [])
                if item.get('id')
            ]
    except Exception:
        return []


@api.post('/instagram/disconnect')
async def instagram_disconnect(user_id: str = Depends(get_current_user_id)):
    u = await db.users.find_one({'id': user_id})
    await db.users.update_one(
        {'id': user_id},
        {
            '$set': {
                'instagramConnected': False,
                'instagram_connection_valid': False,
                'instagram_connection_blocker': 'disconnected_by_user',
                'instagramHandle': None,
            },
            '$unset': {
                'meta_access_token': '',
                'ig_user_id': '',
                'instagram_account_type': '',
                'instagram_graph_me_id': '',
                'instagram_graph_me_user_id': '',
            },
        }
    )
    if u and u.get('ig_user_id'):
        await db.instagram_accounts.update_one(
            {
                'userId': user_id,
                'instagramAccountId': str(u.get('ig_user_id') or ''),
            },
            {'$set': {
                'isActive': False,
                'connectionValid': False,
                'refreshStatus': 'disconnected',
                'refreshLockedUntil': None,
                'updatedAt': datetime.utcnow(),
            }},
        )
    return {'ok': True}


@api.get('/instagram/webhook')
async def instagram_webhook_verify(request: Request):
    params = dict(request.query_params)
    mode = params.get('hub.mode')
    token = params.get('hub.verify_token')
    challenge = params.get('hub.challenge', '')
    if mode == 'subscribe' and token == META_VERIFY_TOKEN:
        return PlainTextResponse(challenge)
    raise HTTPException(403, 'Verification failed')


def _verify_webhook_signature(request_body: bytes, signature_header: str) -> dict:
    """Verify the X-Hub-Signature-256 header from Meta.
    Returns {valid, reason, signature_present, computed_prefix, received_prefix}.
    """
    out = {
        'valid': False,
        'reason': None,
        'signature_present': bool(signature_header),
        'secret_configured': bool(META_WEBHOOK_APP_SECRET),
        'computed_prefix': None,
        'received_prefix': None,
    }
    if not META_WEBHOOK_APP_SECRET:
        out['reason'] = 'no_secret_configured'
        return out
    if not signature_header:
        out['reason'] = 'no_signature_header'
        return out
    # Header format: "sha256=<hex>"
    if not signature_header.startswith('sha256='):
        out['reason'] = 'bad_signature_format'
        out['received_prefix'] = signature_header[:20]
        return out
    received_sig = signature_header[7:]  # strip "sha256="
    computed_sig = hmac.new(
        META_WEBHOOK_APP_SECRET.encode('utf-8'),
        request_body,
        hashlib.sha256,
    ).hexdigest()
    out['computed_prefix'] = computed_sig[:8]
    out['received_prefix'] = received_sig[:8]
    if hmac.compare_digest(computed_sig, received_sig):
        out['valid'] = True
        out['reason'] = 'signature_valid'
    else:
        out['reason'] = 'signature_mismatch'
    return out


@api.post('/instagram/webhook')
async def instagram_webhook(request: Request):
    raw_body = await request.body()
    sig_header = request.headers.get('x-hub-signature-256') or ''
    sig_result = _verify_webhook_signature(raw_body, sig_header)
    if not sig_result['valid']:
        logger.warning('webhook_signature_check reason=%s received_prefix=%s '
                       'computed_prefix=%s enforce=%s',
                       sig_result['reason'],
                       sig_result.get('received_prefix'),
                       sig_result.get('computed_prefix'),
                       META_WEBHOOK_HMAC_ENFORCE)
        if META_WEBHOOK_HMAC_ENFORCE and sig_result['secret_configured']:
            return JSONResponse(
                status_code=403,
                content={'error': 'invalid_signature',
                         'reason': sig_result['reason']},
            )
    else:
        logger.info('webhook_signature_valid prefix=%s', sig_result['computed_prefix'])
    import json as _json
    payload = _json.loads(raw_body)
    logger.info('IG webhook: %s', payload)
    # Store raw payload for debugging — keep only most recent 50
    try:
        await db.webhook_log.insert_one({
            'received': datetime.utcnow(),
            'payload': payload,
            'object': payload.get('object'),
            'signature_valid': sig_result['valid'],
            'signature_reason': sig_result['reason'],
        })
        count = await db.webhook_log.count_documents({})
        if count > 50:
            oldest = await db.webhook_log.find().sort('received', 1).limit(count - 50).to_list(count)
            for o in oldest:
                await db.webhook_log.delete_one({'_id': o['_id']})
    except Exception:
        logger.exception('webhook_log write failed')
    asyncio.create_task(_process_webhook(payload))
    return {'ok': True}


@api.get('/instagram/webhook-log')
async def instagram_webhook_log(request: Request, limit: int = 20, token: Optional[str] = None):
    """Return the most recent raw webhook payloads — for debugging deliveries.
    Accepts JWT via Authorization header OR ?token=... query for easy browser debug."""
    # Accept either Authorization header or ?token= query
    auth = request.headers.get('authorization') or request.headers.get('Authorization') or ''
    jwt_token = None
    if auth.lower().startswith('bearer '):
        jwt_token = auth.split(' ', 1)[1]
    elif token:
        jwt_token = token
    if not jwt_token:
        raise HTTPException(401, 'Provide JWT via Authorization header or ?token=')
    try:
        decode_token(jwt_token)
    except Exception:
        raise HTTPException(401, 'Invalid token')
    docs = await db.webhook_log.find().sort('received', -1).limit(max(1, min(limit, 50))).to_list(50)
    for d in docs:
        d.pop('_id', None)
        if isinstance(d.get('received'), datetime):
            d['received'] = d['received'].isoformat()
    return {'items': docs, 'count': await db.webhook_log.count_documents({})}


async def _handle_new_comment(user_doc: dict, comment_data: dict, source: str = 'webhook'):
    """Process one incoming Instagram comment (shared by webhook + polling).

    Returns a dict with keys:
      processed (bool) — True if a new comment doc was inserted
      matched (bool)   — True if any automation rule fired
      action_status    — 'pending'|'success'|'failed'|'skipped'
      rule_id          — id of the matched automation, when matched
      already_processed (bool) — True if dedup hit
    """
    import uuid as _uuid
    user_id = user_doc['id']
    ig_account_id = user_doc.get('ig_user_id') or ''
    ig_comment_id = comment_data.get('ig_comment_id')
    commenter_id = comment_data.get('commenter_id')
    media_id = comment_data.get('media_id')
    comment_text = comment_data.get('text') or ''

    # log: comment_seen (every comment we observe, before dedup)
    logger.info('comment_seen ig_comment_id=%s media=%s source=%s text=%r',
                ig_comment_id, media_id, source, comment_text[:80])

    if not ig_comment_id or not commenter_id:
        return {'processed': False, 'matched': False, 'action_status': 'skipped',
                'reason': 'missing_id'}
    if commenter_id == ig_account_id:
        return {'processed': False, 'matched': False, 'action_status': 'skipped',
                'reason': 'self_comment'}

    ts_raw = comment_data.get('timestamp')
    comment_ts = _parse_graph_datetime(ts_raw)
    now = datetime.utcnow()
    retry_existing_missing_timestamp = False
    existing_doc_id = None
    existing_created = None

    # Dedupe
    existing = await db.comments.find_one({
        'user_id': user_id, 'ig_comment_id': ig_comment_id,
        '$or': [
            {'instagramAccountId': ig_account_id},
            {'igUserId': ig_account_id},
        ],
    })
    if existing:
        previous_skip = existing.get('skip_reason') or existing.get('skipReason')
        if previous_skip == 'missing_comment_timestamp' and comment_ts:
            retry_existing_missing_timestamp = True
            existing_doc_id = existing.get('id')
            existing_created = existing.get('created')
            logger.info('comment_reprocessing_after_timestamp ig_comment_id=%s user=%s source=%s',
                        ig_comment_id, user_doc.get('email'), source)
        else:
            logger.info('comment_already_processed ig_comment_id=%s user=%s',
                        ig_comment_id, user_doc.get('email'))
            return {'processed': False, 'already_processed': True, 'matched': False,
                    'action_status': 'skipped', 'reason': 'duplicate'}

    commenter_username = comment_data.get('commenter_username') or f'ig_{commenter_id[:8]}'

    def _automation_keywords(auto: dict) -> list:
        raw = auto.get('keywords')
        if isinstance(raw, list):
            parts = raw
        else:
            parts = str(auto.get('keyword') or '').split(',')
        return [str(item or '').strip() for item in parts if str(item or '').strip()]

    def _comment_matches_keywords(auto: dict) -> bool:
        text = comment_text.lower()
        return any(kw.lower() in text for kw in _automation_keywords(auto))

    # Match automations to determine rule_id BEFORE insert
    automations = await db.automations.find(
        {**_account_scoped_query(user_id, ig_account_id), 'status': 'active'}
    ).to_list(100)
    latest_media_id = None
    matched_rule = None
    cutoff_rule = None
    cutoff_skip_reason = None
    for auto in automations:
        raw_trigger = auto.get('trigger') or ''
        trigger = raw_trigger.lower()
        fire = False

        def apply_activation_cutoff() -> Optional[str]:
            if auto.get('processExistingComments') is True:
                return None
            activation = _parse_graph_datetime(
                auto.get('activationStartedAt') or auto.get('createdAt') or auto.get('created')
            ) or now
            logger.info('rule_activation_cutoff_applied rule_id=%s comment=%s activation=%s process_existing=%s',
                        auto.get('id'), ig_comment_id, activation, bool(auto.get('processExistingComments')))
            if not comment_ts:
                logger.info('comment_skipped_missing_timestamp ig_comment_id=%s rule_id=%s source=%s',
                            ig_comment_id, auto.get('id'), source)
                return 'missing_comment_timestamp'
            # Instagram comment timestamps are second-precision, while our
            # activation time includes microseconds. Compare at second
            # precision so a fresh test comment in the same second as rule
            # creation is not treated as historical.
            activation_floor = activation.replace(microsecond=0) if activation else None
            comment_floor = comment_ts.replace(microsecond=0)
            if activation_floor and comment_floor < activation_floor:
                logger.info('comment_skipped_historical ig_comment_id=%s rule_id=%s comment_ts=%s activation=%s source=%s',
                            ig_comment_id, auto.get('id'), comment_ts, activation, source)
                return 'historical_before_rule_activation'
            logger.info('comment_processed_after_activation ig_comment_id=%s rule_id=%s comment_ts=%s activation=%s source=%s',
                        ig_comment_id, auto.get('id'), comment_ts, activation, source)
            return None

        if trigger.startswith('keyword:'):
            cutoff_skip_reason = apply_activation_cutoff()
            if cutoff_skip_reason:
                cutoff_rule = auto
                break
            keyword = trigger.split(':', 1)[1].strip()
            if keyword and keyword.lower() in comment_text.lower():
                fire = True
        elif trigger.startswith('comment:'):
            target = raw_trigger.split(':', 1)[1].strip()
            media_hit = False
            if target.lower() == 'any':
                media_hit = bool(media_id)
            elif target.lower() == 'latest':
                if latest_media_id is None:
                    latest_media_id = await _fetch_latest_media_id(
                        user_doc.get('meta_access_token', ''),
                        user_doc.get('ig_user_id', ''),
                    ) or ''
                if media_id and latest_media_id and media_id == latest_media_id:
                    media_hit = True
            elif target and media_id and target == media_id:
                media_hit = True
            if media_hit:
                cutoff_skip_reason = apply_activation_cutoff()
                if cutoff_skip_reason:
                    cutoff_rule = auto
                    break
                match_mode = (auto.get('match') or 'any').lower()
                kws = _automation_keywords(auto)
                if match_mode == 'keyword' and kws:
                    if _comment_matches_keywords(auto):
                        fire = True
                elif match_mode == 'keyword':
                    fire = False
                else:
                    fire = True
        if fire:
            matched_rule = auto
            break

    rule_id = matched_rule.get('id') if matched_rule else (cutoff_rule.get('id') if cutoff_rule else None)
    matched = bool(matched_rule)
    if matched:
        logger.info('rule_matched ig_comment_id=%s rule_id=%s user=%s',
                    ig_comment_id, rule_id, user_doc.get('email'))
    elif cutoff_skip_reason:
        logger.info('rule_skipped_by_activation_cutoff ig_comment_id=%s rule_id=%s reason=%s user=%s',
                    ig_comment_id, rule_id, cutoff_skip_reason, user_doc.get('email'))
    else:
        logger.info('rule_not_matched ig_comment_id=%s user=%s',
                    ig_comment_id, user_doc.get('email'))

    rule_activation_started_at = (
        cutoff_rule.get('activationStartedAt') if cutoff_rule else
        (matched_rule.get('activationStartedAt') if matched_rule else None)
    )
    process_existing_comments = (
        bool(cutoff_rule.get('processExistingComments')) if cutoff_rule else
        (bool(matched_rule.get('processExistingComments')) if matched_rule else False)
    )
    action_status = 'pending' if matched else 'skipped'
    doc = {
        'id': existing_doc_id or str(_uuid.uuid4()),
        'user_id': user_id,
        'instagramAccountId': ig_account_id,
        'igUserId': ig_account_id,
        'instagramUsername': (user_doc.get('instagramHandle') or '').replace('@', ''),
        'ig_comment_id': ig_comment_id,
        'igCommentId': ig_comment_id,
        'media_id': media_id,
        'mediaId': media_id,
        'commenter_id': commenter_id,
        'commenter_username': commenter_username,
        'text': comment_text,
        'replied': False,
        'source': source,                # 'webhook' or 'polling'
        'rule_id': rule_id,
        'ruleId': rule_id,
        'matched': matched,
        'action_status': action_status,
        'actionStatus': action_status,
        'skip_reason': cutoff_skip_reason,
        'skipReason': cutoff_skip_reason,
        'error': None,
        'timestamp': ts_raw,
        'commentTimestamp': comment_ts or ts_raw,
        'ruleActivationStartedAt': rule_activation_started_at,
        'processExistingComments': process_existing_comments,
        'processed_at': now,
        'reprocessed_after_missing_timestamp': retry_existing_missing_timestamp,
        'updated': now,
        'created': existing_created or now,
    }
    if retry_existing_missing_timestamp:
        await db.comments.update_one(
            {'id': doc['id'], 'user_id': user_id},
            {'$set': doc},
        )
    else:
        try:
            await db.comments.insert_one(doc)
        except Exception as e:
            # Race against unique index; another worker inserted first.
            logger.info('comment_insert_race ig_comment_id=%s err=%s', ig_comment_id, e)
            return {'processed': False, 'already_processed': True, 'matched': False,
                    'action_status': 'skipped', 'reason': 'race'}

    await ws_manager.send(user_id, {'type': 'comment', 'comment': _strip_mongo({**doc})})

    if matched:
        try:
            logger.info('action_execution_started ig_comment_id=%s rule_id=%s',
                        ig_comment_id, rule_id)
            ok = await _run_and_record_action(
                user_doc, matched_rule, commenter_id, comment_text,
                comment_doc_id=doc['id'], ig_comment_id=ig_comment_id,
            )
            action_status = 'success' if ok else 'failed'
        except Exception as e:
            logger.exception('action_execution_failed ig_comment_id=%s err=%s',
                             ig_comment_id, e)
            await db.comments.update_one(
                {'id': doc['id']},
                {'$set': {'action_status': 'failed', 'actionStatus': 'failed',
                          'error': str(e)[:500]}}
            )
            action_status = 'failed'

    return {'processed': True, 'reprocessed': retry_existing_missing_timestamp,
            'matched': matched, 'action_status': action_status,
            'rule_id': rule_id, 'comment_doc_id': doc['id']}


async def _run_and_record_action(user_doc, automation, commenter_id, comment_text,
                                 comment_doc_id: str, ig_comment_id: str):
    """Wrap execute_flow so we record success/failure on the comment doc."""
    try:
        ok = await execute_flow(
            user_doc, automation, commenter_id, comment_text,
            comment_context={'ig_comment_id': ig_comment_id, 'comment_doc_id': comment_doc_id}
        )
        if not ok:
            await db.comments.update_one(
                {'id': comment_doc_id},
                {'$set': {'action_status': 'failed', 'actionStatus': 'failed',
                          'error': 'automation_action_send_failed'}}
            )
            logger.warning('action_execution_send_failed ig_comment_id=%s rule_id=%s',
                           ig_comment_id, automation.get('id'))
            return False
        await db.comments.update_one(
            {'id': comment_doc_id},
            {'$set': {'action_status': 'success', 'actionStatus': 'success'}}
        )
        logger.info('action_execution_success ig_comment_id=%s rule_id=%s',
                    ig_comment_id, automation.get('id'))
        return True
    except Exception as e:
        await db.comments.update_one(
            {'id': comment_doc_id},
            {'$set': {'action_status': 'failed', 'actionStatus': 'failed',
                      'error': str(e)[:500]}}
        )
        logger.exception('action_execution_failed ig_comment_id=%s rule_id=%s err=%s',
                         ig_comment_id, automation.get('id'), e)
        return False


# ---------------- DM Automation (independent from Comments) ----------------
def _dm_match(text: str, keyword: str, mode: str) -> bool:
    """Match incoming DM text against a rule keyword. Case-insensitive."""
    if not text or not keyword:
        return False
    t = text.strip().lower()
    k = keyword.strip().lower()
    if not t or not k:
        return False
    if mode == 'exact':
        return t == k
    if mode == 'starts_with':
        return t.startswith(k)
    # default: contains
    return k in t


def _classify_messaging_event(event: dict) -> dict:
    """Classify a raw Instagram messaging-item dict into our internal kind.
    Returns: {kind, sender_id, recipient_id, message_id, text, is_echo,
              has_message, has_read, has_delivery, has_postback, has_reaction,
              has_referral, has_attachments, message_keys, item_keys, timestamp}
    eventKind ∈ message_text | message_echo | message_attachment |
                read | delivery | postback | reaction | referral | unknown
    """
    item_keys = sorted(list(event.keys())) if isinstance(event, dict) else []
    sender = event.get('sender') if isinstance(event, dict) else None
    recipient = event.get('recipient') if isinstance(event, dict) else None
    message = event.get('message') if isinstance(event, dict) else None
    postback = event.get('postback') if isinstance(event, dict) else None
    sender_id = (sender or {}).get('id') if isinstance(sender, dict) else None
    recipient_id = (recipient or {}).get('id') if isinstance(recipient, dict) else None
    message_keys = sorted(list(message.keys())) if isinstance(message, dict) else []

    has_read = 'read' in event if isinstance(event, dict) else False
    has_delivery = 'delivery' in event if isinstance(event, dict) else False
    has_postback = 'postback' in event if isinstance(event, dict) else False
    has_reaction = 'reaction' in event if isinstance(event, dict) else False
    has_referral = 'referral' in event if isinstance(event, dict) else False
    has_message = isinstance(message, dict)
    is_echo = bool(message.get('is_echo')) if has_message else False
    text = (message.get('text') if has_message else None) or ''
    quick_reply = message.get('quick_reply') if has_message and isinstance(message.get('quick_reply'), dict) else None
    quick_reply_payload = (quick_reply or {}).get('payload') if quick_reply else None
    postback_payload = (postback or {}).get('payload') if isinstance(postback, dict) else None
    postback_title = (postback or {}).get('title') if isinstance(postback, dict) else None
    attachments = message.get('attachments') if has_message else None
    has_attachments = bool(attachments)
    message_id = (message.get('mid') or message.get('id')) if has_message else None
    timestamp = event.get('timestamp') if isinstance(event, dict) else None

    if has_read:
        kind = 'read'
    elif has_delivery:
        kind = 'delivery'
    elif has_reaction:
        kind = 'reaction'
    elif has_referral:
        kind = 'referral'
    elif has_postback:
        kind = 'postback'
    elif has_message:
        if is_echo:
            kind = 'message_echo'
        elif quick_reply_payload:
            kind = 'quick_reply'
        elif text:
            kind = 'message_text'
        elif has_attachments:
            kind = 'message_attachment'
        else:
            kind = 'unknown'
    else:
        kind = 'unknown'

    return {
        'kind': kind,
        'sender_id': sender_id,
        'recipient_id': recipient_id,
        'message_id': message_id,
        'text': text,
        'quick_reply_payload': quick_reply_payload,
        'postback_payload': postback_payload,
        'postback_title': postback_title,
        'is_echo': is_echo,
        'has_message': has_message,
        'has_read': has_read,
        'has_delivery': has_delivery,
        'has_postback': has_postback,
        'has_reaction': has_reaction,
        'has_referral': has_referral,
        'has_attachments': has_attachments,
        'message_keys': message_keys,
        'item_keys': item_keys,
        'timestamp': timestamp,
    }


@api.get('/instagram/webhook/diagnostics')
async def webhook_diagnostics(user_id: str = Depends(get_current_user_id)):
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    token = u.get('meta_access_token')
    ig_id = str(u.get('ig_user_id') or '')
    connected = _has_valid_instagram_connection(u)
    
    out = {
        "connected": connected,
        "connectionValid": connected,
        "canonicalIgUserId": ig_id or None,
        "dbIgUserId": ig_id or None,
        "idMatch": True,
        "callbackUrl": f"{BACKEND_PUBLIC_URL}/api/instagram/webhook",
        "verifyTokenConfigured": bool(META_VERIFY_TOKEN),
        "signatureValidationEnabled": META_WEBHOOK_HMAC_ENFORCE,
        "subscribedFields": [],
        "recentWebhookEntryIds": [],
        "recentWebhookRecipientIds": [],
        "webhookAccountMatch": False,
        "recentEventKinds": []
    }
    
    if connected and token and ig_id:
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(f"https://graph.facebook.com/v21.0/{ig_id}/subscribed_apps", params={'access_token': token})
                if r.status_code == 200:
                    data = r.json().get('data', [])
                    if data:
                        out['subscribedFields'] = data[0].get('subscribed_fields', [])
        except Exception:
            pass
            
    recent_logs = await db.dm_logs.find({'user_id': user_id}).sort('created', -1).limit(10).to_list(10)
    out['recentEventKinds'] = list(set([L.get('event_kind') for L in recent_logs]))
    out['recentWebhookEntryIds'] = list(set([L.get('ig_user_id') for L in recent_logs if L.get('ig_user_id')]))
    out['recentWebhookRecipientIds'] = list(set([L.get('recipient_id') for L in recent_logs if L.get('recipient_id')]))
    
    if ig_id and out['recentWebhookEntryIds']:
        out['webhookAccountMatch'] = ig_id in out['recentWebhookEntryIds']
    elif not out['recentWebhookEntryIds']:
        out['webhookAccountMatch'] = True # None yet
        
    return out

@api.post('/instagram/webhook/resubscribe')
async def webhook_resubscribe(user_id: str = Depends(get_current_user_id)):
    u = await db.users.find_one({'id': user_id})
    if not u or not _has_valid_instagram_connection(u):
        raise HTTPException(400, 'Instagram not connected')
    token = u.get('meta_access_token')
    ig_id = str(u.get('ig_user_id') or '')
    if not token or not ig_id:
        raise HTTPException(400, 'Missing IG identity')
        
    fields = 'messages,messaging_postbacks,messaging_seen,message_reactions,comments'
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"https://graph.facebook.com/v21.0/{ig_id}/subscribed_apps", params={'access_token': token, 'subscribed_fields': fields})
            if r.status_code != 200:
                raise HTTPException(400, f'Subscription failed: {r.text}')
                
            vr = await c.get(f"https://graph.facebook.com/v21.0/{ig_id}/subscribed_apps", params={'access_token': token})
            subs = []
            if vr.status_code == 200:
                data = vr.json().get('data', [])
                if data:
                    subs = data[0].get('subscribed_fields', [])
            
            return {
                "igUserIdUsed": ig_id,
                "subscribedFields": subs,
                "messagesSubscribed": 'messages' in subs,
                "commentsSubscribed": 'comments' in subs,
                "idMatch": True
            }
    except Exception as e:
        logger.error(f'Resubscribe error: {e}')
        raise HTTPException(500, str(e))

async def _handle_new_dm_message(user_doc: dict, event: dict, source: str = 'webhook'):
    """Process one incoming Instagram DM messaging-item.

    `event` is the RAW messaging-item dict from
      payload.entry[].messaging[]
    OR a legacy flattened dict with keys
      sender_id, message_id, text, timestamp, is_echo
    Returns dict: {processed, matched, status, rule_id, log_id, event_kind}
    Status values: received | matched | replied | failed | skipped
    skip_reason values: duplicate | echo | self_message | missing_sender |
                       missing_text | no_active_rules | no_rule_match | send_failed |
                       read_receipt | delivery_receipt | reaction |
                       postback_unsupported | attachment_unsupported |
                       non_message_event
    """
    import uuid as _uuid
    import hashlib as _hashlib
    user_id = user_doc['id']
    ig_account_id = user_doc.get('ig_user_id') or ''
    now = datetime.utcnow()

    # Accept both raw shape (sender/recipient/message) and legacy flattened.
    if isinstance(event, dict) and ('sender' in event or 'message' in event
                                    or 'read' in event or 'delivery' in event
                                    or 'postback' in event or 'reaction' in event
                                    or 'referral' in event):
        cls = _classify_messaging_event(event)
        sender_id = cls['sender_id']
        recipient_id = cls['recipient_id']
        message_id = cls['message_id']
        text = cls['text']
        quick_reply_payload = cls.get('quick_reply_payload')
        postback_payload = cls.get('postback_payload')
        postback_title = cls.get('postback_title')
        is_echo = cls['is_echo']
        ts = cls['timestamp']
        event_kind = cls['kind']
        message_keys = cls['message_keys']
        item_keys = cls['item_keys']
    else:
        # Legacy flattened input
        sender_id = event.get('sender_id')
        recipient_id = event.get('recipient_id')
        message_id = event.get('message_id')
        text = event.get('text') or ''
        quick_reply_payload = event.get('quick_reply_payload')
        postback_payload = event.get('postback_payload')
        postback_title = event.get('postback_title')
        is_echo = bool(event.get('is_echo'))
        ts = event.get('timestamp')
        event_kind = 'message_echo' if is_echo else ('message_text' if text else 'unknown')
        message_keys = []
        item_keys = sorted(list(event.keys())) if isinstance(event, dict) else []

    logger.info('dm_processor_invoked user_id=%s ig_account=%s source=%s',
                user_id, ig_account_id, source)
    logger.info('dm_event_kind_classified kind=%s sender=%s msg_id=%s echo=%s '
                'item_keys=%s message_keys=%s',
                event_kind, sender_id, message_id, is_echo, item_keys, message_keys)
    logger.info('dm_webhook_received ig_account=%s sender=%s msg_id=%s echo=%s kind=%s',
                ig_account_id, sender_id, message_id, is_echo, event_kind)

    # Compute a dedup key that NEVER collides on null. Prefer Meta's mid/id;
    # otherwise hash the available identifying surface for THIS event kind so
    # read/delivery/reaction events also dedup deterministically.
    if message_id:
        dedup_key = f'mid:{message_id}'
    else:
        watermark = ''
        try:
            if isinstance(event, dict):
                if isinstance(event.get('read'), dict):
                    watermark = str(event['read'].get('watermark') or '')
                elif isinstance(event.get('delivery'), dict):
                    watermark = str(event['delivery'].get('watermark') or '')
                elif isinstance(event.get('reaction'), dict):
                    watermark = str(event['reaction'].get('mid') or '') + ':' + \
                                str(event['reaction'].get('action') or '')
                elif isinstance(event.get('postback'), dict):
                    watermark = str(event['postback'].get('mid') or event['postback'].get('payload') or '')
        except Exception:
            watermark = ''
        h = _hashlib.sha256(
            f'{ig_account_id}|{sender_id or ""}|{ts or ""}|{text or ""}|{event_kind}|{watermark}'.encode('utf-8')
        ).hexdigest()
        dedup_key = f'sha:{h}'

    def _mk_log(status: str, skip_reason=None, matched_rule=None, error=None):
        return {
            'id': str(_uuid.uuid4()),
            'user_id': user_id,
            **_current_instagram_context(user_doc),
            'ig_user_id': ig_account_id,
            'sender_id': sender_id,
            'recipient_id': locals().get('recipient_id') if 'recipient_id' in locals() else None,
            'message_id': message_id,
            'dedup_key': dedup_key,
            'event_kind': event_kind,
            'message_keys': message_keys,
            'item_keys': item_keys,
            'incoming_text': text if text else None,
            'quick_reply_payload': quick_reply_payload,
            'postback_payload': postback_payload,
            'postback_title': postback_title,
            'matched_rule_id': matched_rule.get('id') if matched_rule else None,
            'matched_rule_name': matched_rule.get('name') if matched_rule else None,
            'reply_text': matched_rule.get('reply_text') if matched_rule else None,
            'status': status,
            'skip_reason': skip_reason,
            'error': error,
            'source': source,
            'is_echo': is_echo,
            'timestamp': ts,
            'created': now,
        }

    async def _persist(doc):
        logger.info('dm_log_insert_started dedup_key=%s kind=%s status=%s',
                    dedup_key, event_kind, doc.get('status'))
        try:
            await db.dm_logs.insert_one(doc)
            logger.info('dm_log_insert_success dedup_key=%s id=%s', dedup_key, doc.get('id'))
            return True
        except Exception as e:
            # DuplicateKeyError on unique (user_id, dedup_key) — that is fine,
            # someone else inserted first. Anything else is a real failure.
            logger.warning('dm_log_insert_failed dedup_key=%s err=%s', dedup_key, str(e)[:200])
            return False

    # Dedup FIRST so replays never double-reply.
    existing = await db.dm_logs.find_one({
        'user_id': user_id,
        'dedup_key': dedup_key,
        '$or': [
            {'instagramAccountId': ig_account_id},
            {'igUserId': ig_account_id},
            {'instagramAccountId': {'$exists': False}},
        ],
    })
    if existing:
        logger.info('dm_message_duplicate_skipped dedup_key=%s', dedup_key)
        return {'processed': False, 'status': 'skipped', 'reason': 'duplicate',
                'log_id': existing.get('id'), 'event_kind': event_kind}

    # Classify non-text messaging events explicitly. Always log them so the
    # debug panel can show why nothing was sent.
    if event_kind == 'read':
        await _persist(_mk_log('skipped', skip_reason='read_receipt'))
        return {'processed': False, 'status': 'skipped', 'reason': 'read_receipt',
                'event_kind': event_kind}
    if event_kind == 'delivery':
        await _persist(_mk_log('skipped', skip_reason='delivery_receipt'))
        return {'processed': False, 'status': 'skipped', 'reason': 'delivery_receipt',
                'event_kind': event_kind}
    if event_kind == 'reaction':
        await _persist(_mk_log('skipped', skip_reason='reaction'))
        return {'processed': False, 'status': 'skipped', 'reason': 'reaction',
                'event_kind': event_kind}
    if event_kind == 'referral':
        await _persist(_mk_log('skipped', skip_reason='referral'))
        return {'processed': False, 'status': 'skipped', 'reason': 'referral',
                'event_kind': event_kind}
    if event_kind == 'postback' and not text:
        session = await _find_pending_comment_dm_session(
            user_doc, sender_id, payload=postback_payload
        )
        if session:
            log_doc = _mk_log('matched')
            log_doc['comment_flow_session_id'] = session.get('id')
            if not await _persist(log_doc):
                return {'processed': False, 'status': 'skipped', 'reason': 'race',
                        'event_kind': event_kind}
            ok = await _send_comment_dm_flow_completion(user_doc, session)
            await db.dm_logs.update_one(
                {'id': log_doc['id']},
                {'$set': {'status': 'replied' if ok else 'failed',
                          'skip_reason': None if ok else 'comment_flow_send_failed'}}
            )
            return {'processed': True, 'matched': True,
                    'status': 'replied' if ok else 'failed',
                    'reason': 'comment_flow_response',
                    'log_id': log_doc['id'], 'event_kind': event_kind}
        await _persist(_mk_log('skipped', skip_reason='postback_unsupported'))
        return {'processed': False, 'status': 'skipped', 'reason': 'postback_unsupported',
                'event_kind': event_kind}
    if event_kind == 'message_attachment':
        await _persist(_mk_log('skipped', skip_reason='attachment_unsupported'))
        return {'processed': False, 'status': 'skipped', 'reason': 'attachment_unsupported',
                'event_kind': event_kind}
    if event_kind == 'unknown' and not text:
        await _persist(_mk_log('skipped', skip_reason='non_message_event'))
        return {'processed': False, 'status': 'skipped', 'reason': 'non_message_event',
                'event_kind': event_kind}

    # Real text messages from this point on.
    if is_echo or event_kind == 'message_echo':
        await _persist(_mk_log('skipped', skip_reason='echo'))
        return {'processed': False, 'status': 'skipped', 'reason': 'echo',
                'event_kind': event_kind}
    if sender_id and sender_id == ig_account_id:
        await _persist(_mk_log('skipped', skip_reason='self_message'))
        return {'processed': False, 'status': 'skipped', 'reason': 'self_message',
                'event_kind': event_kind}
    if not sender_id:
        await _persist(_mk_log('skipped', skip_reason='missing_sender'))
        return {'processed': False, 'status': 'skipped', 'reason': 'missing_sender',
                'event_kind': event_kind}
    if not text and not quick_reply_payload:
        await _persist(_mk_log('skipped', skip_reason='missing_text'))
        return {'processed': False, 'status': 'skipped', 'reason': 'missing_text',
                'event_kind': event_kind}

    logger.info('dm_sender_extracted sender=%s', sender_id)
    logger.info('dm_text_extracted len=%s preview=%r', len(text), text[:80])

    session = await _find_pending_comment_dm_session(
        user_doc, sender_id, payload=quick_reply_payload or postback_payload
    )
    if session:
        log_doc = _mk_log('matched')
        log_doc['comment_flow_session_id'] = session.get('id')
        if not await _persist(log_doc):
            return {'processed': False, 'status': 'skipped', 'reason': 'race',
                    'event_kind': event_kind}
        ok = await _send_comment_dm_flow_completion(user_doc, session)
        await db.dm_logs.update_one(
            {'id': log_doc['id']},
            {'$set': {'status': 'replied' if ok else 'failed',
                      'skip_reason': None if ok else 'comment_flow_send_failed'}}
        )
        return {'processed': True, 'matched': True,
                'status': 'replied' if ok else 'failed',
                'reason': 'comment_flow_response',
                'log_id': log_doc['id'], 'event_kind': event_kind}

    # Load active DM rules for this user
    rules = await db.dm_rules.find(
        {**_account_scoped_query(user_id, _current_instagram_context(user_doc)), 'is_active': True}
    ).to_list(200)
    logger.info('dm_rule_loaded count=%s user=%s', len(rules), user_doc.get('email'))

    if not rules:
        log_doc = _mk_log('skipped', skip_reason='no_active_rules')
        await _persist(log_doc)
        return {'processed': True, 'matched': False, 'status': 'skipped',
                'reason': 'no_active_rules', 'log_id': log_doc['id']}

    matched_rule = None
    for r in rules:
        if _dm_match(text, r.get('keyword') or '', (r.get('match_mode') or 'contains').lower()):
            matched_rule = r
            break

    if not matched_rule:
        log_doc = _mk_log('skipped', skip_reason='no_rule_match')
        await _persist(log_doc)
        logger.info('dm_rule_not_matched dedup_key=%s', dedup_key)
        return {'processed': True, 'matched': False, 'status': 'skipped',
                'reason': 'no_rule_match', 'log_id': log_doc['id']}

    rule_id = matched_rule.get('id')
    logger.info('dm_rule_matched rule_id=%s dedup_key=%s', rule_id, dedup_key)
    log_doc = _mk_log('matched', matched_rule=matched_rule)
    if not await _persist(log_doc):
        return {'processed': False, 'status': 'skipped', 'reason': 'race'}

    reply_text = (matched_rule.get('reply_text') or '').strip()
    if not reply_text:
        await db.dm_logs.update_one(
            {'id': log_doc['id']},
            {'$set': {'status': 'failed', 'skip_reason': 'send_failed',
                      'error': 'rule_has_empty_reply_text'}}
        )
        return {'processed': True, 'matched': True, 'status': 'failed',
                'rule_id': rule_id, 'log_id': log_doc['id']}

    logger.info('dm_reply_started rule_id=%s dedup_key=%s', rule_id, dedup_key)
    ok = False
    err = None
    try:
        ok = await send_ig_dm(
            user_doc.get('meta_access_token', ''),
            ig_account_id,
            sender_id,
            reply_text,
        )
    except Exception as e:
        err = str(e)[:500]

    if ok:
        await db.dm_logs.update_one(
            {'id': log_doc['id']},
            {'$set': {'status': 'replied'}}
        )
        logger.info('dm_reply_success rule_id=%s dedup_key=%s', rule_id, dedup_key)
        return {'processed': True, 'matched': True, 'status': 'replied',
                'rule_id': rule_id, 'log_id': log_doc['id']}
    else:
        await db.dm_logs.update_one(
            {'id': log_doc['id']},
            {'$set': {'status': 'failed', 'skip_reason': 'send_failed',
                      'error': err or 'meta_send_returned_false'}}
        )
        logger.warning('dm_reply_failed rule_id=%s dedup_key=%s err=%s',
                       rule_id, dedup_key, err)
        return {'processed': True, 'matched': True, 'status': 'failed',
                'rule_id': rule_id, 'log_id': log_doc['id'], 'error': err}


async def _process_webhook(payload: dict):
    """Process Instagram webhook events asynchronously."""
    try:
        for entry in payload.get('entry', []):
            ig_account_id = entry.get('id')
            logger.info('dm_user_mapping_started entry_id=%s', ig_account_id)
            # Find which user owns this IG account — entry.id can be either the
            # Instagram Business account id OR the Facebook Page id depending on
            # how the subscription was created, so try both.
            user_doc, mapping_via = await _find_user_doc_for_instagram_account_id(ig_account_id)
            if user_doc and mapping_via == 'users.ig_user_id':
                mapping_via = 'entry.id'
            # Fallback: if entry.id doesn't match any user, try the recipient.id
            # of any messaging-item in this entry. Read/delivery events from a
            # business account use sender=business, recipient=user — but for
            # incoming text DMs, recipient.id == business IG account.
            if not user_doc:
                for ev in entry.get('messaging', []) or []:
                    rid = (ev.get('recipient') or {}).get('id')
                    if rid:
                        user_doc, account_mapping_via = await _find_user_doc_for_instagram_account_id(rid)
                        if user_doc:
                            mapping_via = f'recipient.id:{account_mapping_via}'
                            ig_account_id = rid
                            break
            # Last-resort fallback for single-tenant deployments: if exactly
            # one user has an IG account connected, attribute the event to it.
            if not user_doc:
                connected_accounts = await db.instagram_accounts.find(
                    {'isActive': {'$ne': False}, 'connectionValid': {'$ne': False}}
                ).limit(2).to_list(2)
                if len(connected_accounts) == 1:
                    account_doc = connected_accounts[0]
                    owner = await db.users.find_one({'id': account_doc.get('userId') or account_doc.get('user_id')})
                    if owner:
                        user_doc = _with_instagram_account_context(owner, account_doc)
                        mapping_via = 'single_tenant_instagram_account_fallback'
                        ig_account_id = account_doc.get('instagramAccountId') or account_doc.get('igUserId') or ig_account_id
            if not user_doc:
                logger.warning('dm_user_mapping_failed entry_id=%s', entry.get('id'))
                continue
            # Normalize so downstream code uses the real IG account id
            ig_account_id = user_doc.get('ig_user_id') or ig_account_id
            user_id = user_doc['id']
            logger.info('dm_user_mapping_success entry_id=%s user_id=%s via=%s',
                        entry.get('id'), user_id, mapping_via)

            for event in entry.get('messaging', []):
                # ALWAYS feed the DM automation handler first, with the raw
                # messaging item, so every event (read/delivery/reaction/
                # postback/text/echo) produces an explicit dm_logs row.
                try:
                    await _handle_new_dm_message(user_doc, event, source='webhook')
                except Exception:
                    logger.exception('DM automation handler error')

                sender_id = event.get('sender', {}).get('id')
                if sender_id == ig_account_id:
                    continue  # skip own messages for legacy conv/flow path
                msg_obj = event.get('message', {})
                msg_text = msg_obj.get('text', '')
                if not msg_text:
                    continue  # legacy conv/flow path is text-only

                # Save incoming message to conversation
                import uuid as _uuid
                conv = await db.conversations.find_one({
                    'user_id': user_id,
                    'contact.ig_id': sender_id,
                    '$or': [
                        {'instagramAccountId': ig_account_id},
                        {'igUserId': ig_account_id},
                        {'instagramAccountId': {'$exists': False}},
                    ],
                })
                if not conv:
                    # Create new conversation for this contact
                    conv_id = str(_uuid.uuid4())
                    conv = {
                        'id': conv_id, 'user_id': user_id,
                        'instagramAccountId': ig_account_id,
                        'igUserId': ig_account_id,
                        'instagramUsername': (user_doc.get('instagramHandle') or '').replace('@', ''),
                        'contact': {'name': f'User {sender_id[:8]}', 'username': f'@ig_{sender_id[:8]}',
                                    'avatar': f'https://i.pravatar.cc/150?u={sender_id}',
                                    'ig_id': sender_id},
                        'messages': [], 'lastMessage': msg_text, 'time': 'now', 'unread': 0,
                        'created': datetime.utcnow(),
                    }
                    await db.conversations.insert_one(conv)
                else:
                    conv_id = conv['id']

                incoming = {'id': str(_uuid.uuid4()), 'from': 'contact', 'text': msg_text,
                            'time': datetime.utcnow().strftime('%I:%M %p')}
                await db.conversations.update_one(
                    {'id': conv_id},
                    {'$push': {'messages': incoming},
                     '$set': {'lastMessage': msg_text, 'time': 'now', 'unread': 1}}
                )
                # Push to live WS if user is connected
                await ws_manager.send(user_id, {'type': 'incoming', 'conv_id': conv_id, 'message': incoming})

                # Match automations by keyword trigger (legacy flow builder)
                automations = await db.automations.find(
                    {**_account_scoped_query(user_id, ig_account_id), 'status': 'active'}
                ).to_list(100)
                for auto in automations:
                    trigger = (auto.get('trigger') or '').lower()
                    if trigger.startswith('keyword:'):
                        keyword = trigger.split(':', 1)[1].strip()
                        if keyword and keyword.lower() in msg_text.lower():
                            asyncio.create_task(execute_flow(user_doc, auto, sender_id, msg_text))
                    elif trigger == 'new follower' and event.get('follow'):
                        asyncio.create_task(execute_flow(user_doc, auto, sender_id, msg_text))

                # DM Automation handler was already called at the top of the
                # loop with the raw messaging item.

            for change in entry.get('changes', []):
                field = change.get('field')
                value = change.get('value', {})
                # Normalize: IG sends field='comments'; FB Page feed sends field='feed' with item='comment'
                is_comment = field == 'comments' or (field == 'feed' and value.get('item') == 'comment')
                if is_comment:
                    commenter = value.get('from', {}) or {}
                    media_obj = value.get('media') or {}
                    await _handle_new_comment(user_doc, {
                        'ig_comment_id': value.get('comment_id') or value.get('id'),
                        'media_id': media_obj.get('id') or value.get('post_id') or value.get('parent_id'),
                        'commenter_id': commenter.get('id'),
                        'commenter_username': commenter.get('username') or commenter.get('name'),
                        'text': value.get('text') or value.get('message', ''),
                        'timestamp': (
                            value.get('timestamp') or value.get('created_time') or
                            value.get('created_at') or value.get('time')
                        ),
                    }, source='webhook')
                elif field == 'story_insights' or (field == 'feed' and value.get('item') == 'story_insights'):
                    replier_id = value.get('from', {}).get('id')
                    if replier_id:
                        automations = await db.automations.find(
                            {'user_id': user_id, 'status': 'active', 'trigger': 'Story Reply'}
                        ).to_list(20)
                        for auto in automations:
                            asyncio.create_task(execute_flow(user_doc, auto, replier_id, ''))
    except Exception:
        logger.exception('Webhook processing error')


# ---------------- Comment polling service ----------------
# Works around the Meta limitation that comment webhooks only fire when the app
# is in Live mode with Advanced Access for instagram_business_manage_comments.
# Until App Review completes, we poll GET /{media_id}/comments directly.
# Source: https://developers.facebook.com/docs/instagram-platform/webhooks/ —
# "Advanced Access is required to receive comments and live_comments webhook notifications."
IG_POLL_INTERVAL_SECONDS = int(os.environ.get('IG_POLL_INTERVAL_SECONDS', '60'))
IG_POLL_ENABLED = os.environ.get('IG_POLL_ENABLED', '1') == '1'
_poll_task: Optional[asyncio.Task] = None


async def _collect_target_media_ids(user_doc: dict, automations: list) -> list:
    """Resolve the set of media IDs we need to poll for this user."""
    target: list = []
    needs_latest = False
    needs_any = False
    for a in automations:
        raw_trigger = a.get('trigger') or ''
        trigger = raw_trigger.lower()
        if trigger.startswith('comment:'):
            t = raw_trigger.split(':', 1)[1].strip()
            if t.lower() == 'latest':
                needs_latest = True
            elif t.lower() == 'any':
                needs_any = True
            elif t and t not in target:
                target.append(t)
        # Also honor explicit trigger_media_id on the automation doc
        mid = a.get('trigger_media_id') or a.get('media_id')
        if mid and mid not in target:
            target.append(mid)
    if needs_latest:
        latest = await _fetch_latest_media_id(
            user_doc.get('meta_access_token', ''),
            user_doc.get('ig_user_id', ''),
        )
        if latest and latest not in target:
            target.append(latest)
    if needs_any:
        for mid in await _fetch_recent_media_ids(
            user_doc.get('meta_access_token', ''),
            user_doc.get('ig_user_id', ''),
            limit=10,
        ):
            if mid and mid not in target:
                target.append(mid)
    return target


async def _poll_user_comments(user_doc: dict) -> dict:
    """Poll comments for one user's automations. Returns aggregated stats."""
    user_id = user_doc['id']
    token = user_doc.get('meta_access_token', '')
    ig_id = user_doc.get('ig_user_id', '')
    stats: dict = {
        'user_id': user_id,
        'mediaChecked': 0,
        'commentsSeen': 0,
        'newComments': 0,
        'matched': 0,
        'actionsSucceeded': 0,
        'actionsFailed': 0,
        'media': {},
        'errors': [],
    }

    if not token or not ig_id:
        stats['errors'].append('missing_token_or_ig_id')
        return stats

    automations = await db.automations.find(
        {'user_id': user_id, 'status': 'active'}
    ).to_list(200)
    if not automations:
        return stats

    media_ids = await _collect_target_media_ids(user_doc, automations)
    if not media_ids:
        return stats

    async with httpx.AsyncClient(timeout=20) as c:
        for mid in media_ids[:10]:  # cap per-user per-tick
            stats['mediaChecked'] += 1
            logger.info('media_comments_fetch_started user=%s media_id=%s',
                        user_doc.get('email'), mid)
            try:
                r = await c.get(
                    f'https://graph.instagram.com/{mid}/comments',
                    params={
                        'access_token': token,
                        'fields': 'id,text,username,timestamp,from',
                        'limit': 25,
                    },
                )
                if r.status_code != 200:
                    logger.warning('media_comments_fetch_failed user=%s media_id=%s http=%s body=%s',
                                   user_doc.get('email'), mid, r.status_code, r.text[:200])
                    stats['media'][mid] = {'http': r.status_code, 'error': r.text[:200]}
                    stats['errors'].append({'media_id': mid, 'http': r.status_code, 'error': r.text[:200]})
                    continue
                data = (r.json() or {}).get('data') or []
                logger.info('media_comments_fetch_success user=%s media_id=%s count=%s',
                            user_doc.get('email'), mid, len(data))
                stats['commentsSeen'] += len(data)
                new_count = 0
                matched_count = 0
                succeeded = 0
                failed = 0
                for cm in data:
                    ig_comment_id = cm.get('id')
                    from_obj = cm.get('from') or {}
                    commenter_id = from_obj.get('id')
                    commenter_username = (
                        from_obj.get('username') or cm.get('username') or
                        (f'ig_{commenter_id[:8]}' if commenter_id else None)
                    )
                    if not commenter_id and commenter_username:
                        commenter_id = f'u:{commenter_username}'
                    res = await _handle_new_comment(user_doc, {
                        'ig_comment_id': ig_comment_id,
                        'media_id': mid,
                        'commenter_id': commenter_id,
                        'commenter_username': commenter_username,
                        'text': cm.get('text') or '',
                        'timestamp': cm.get('timestamp'),
                    }, source='polling') or {}
                    if res.get('processed'):
                        new_count += 1
                    if res.get('matched'):
                        matched_count += 1
                    st = res.get('action_status')
                    if st == 'success':
                        succeeded += 1
                    elif st == 'failed':
                        failed += 1
                stats['newComments'] += new_count
                stats['matched'] += matched_count
                stats['actionsSucceeded'] += succeeded
                stats['actionsFailed'] += failed
                stats['media'][mid] = {'total': len(data), 'new': new_count,
                                       'matched': matched_count}
            except Exception as e:
                logger.exception('media_comments_fetch_failed user=%s media_id=%s exc=%s',
                                 user_doc.get('email'), mid, e)
                stats['media'][mid] = {'error': f'exc:{e}'}
                stats['errors'].append({'media_id': mid, 'error': f'exc:{e}'})
    return stats


async def _comment_poller_loop():
    """Runs forever: every IG_POLL_INTERVAL_SECONDS, poll comments for all
    connected users that have active comment automations."""
    logger.info('Comment poller started (interval=%ss)', IG_POLL_INTERVAL_SECONDS)
    while True:
        try:
            cursor = db.users.find({'instagramConnected': True})
            users = await cursor.to_list(500)
            logger.info('polling_started accounts=%s', len(users))
            for u in users:
                try:
                    s = await _poll_user_comments(u)
                    if s.get('newComments'):
                        logger.info('polling_user_summary user=%s new=%s matched=%s ok=%s fail=%s',
                                    u.get('email'), s['newComments'], s['matched'],
                                    s['actionsSucceeded'], s['actionsFailed'])
                except Exception:
                    logger.exception('Poller per-user error for %s', u.get('id'))
        except Exception:
            logger.exception('Poller outer loop error')
        await asyncio.sleep(IG_POLL_INTERVAL_SECONDS)


@api.get('/instagram/poll-now')
async def instagram_poll_now(email: str = '', key: str = ''):
    """Manually trigger a single poll for one user — DISABLED in production.
    Use the authenticated POST /api/instagram/comments/poll-now instead."""
    raise HTTPException(403, 'Disabled in production. Use POST /api/instagram/comments/poll-now with JWT auth.')
    u = await db.users.find_one({'email': email})
    if not u:
        raise HTTPException(404, 'user not found')
    stats = await _poll_user_comments(u)
    return stats


@api.post('/instagram/comments/poll-now')
async def instagram_comments_poll_now(user_id: str = Depends(get_current_user_id)):
    """Authenticated trigger: poll comments for the calling user right now.
    Returns a summary in the documented shape."""
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'user not found')
    if not u.get('instagramConnected'):
        return {
            'ok': False, 'accountsChecked': 0, 'mediaChecked': 0,
            'commentsSeen': 0, 'newComments': 0, 'matched': 0,
            'actionsSucceeded': 0, 'actionsFailed': 0,
            'errors': [{'error': 'instagram_not_connected'}],
        }
    s = await _poll_user_comments(u)
    return {
        'ok': True,
        'accountsChecked': 1,
        'mediaChecked': s.get('mediaChecked', 0),
        'commentsSeen': s.get('commentsSeen', 0),
        'newComments': s.get('newComments', 0),
        'matched': s.get('matched', 0),
        'actionsSucceeded': s.get('actionsSucceeded', 0),
        'actionsFailed': s.get('actionsFailed', 0),
        'errors': s.get('errors', []),
    }


@api.get('/instagram/comments/processed')
async def instagram_comments_processed(
    limit: int = 50,
    user_id: str = Depends(get_current_user_id),
):
    """Diagnostic: list recently processed comments for the calling user."""
    limit = max(1, min(limit, 200))
    cur = db.comments.find({'user_id': user_id}).sort('created', -1).limit(limit)
    items = await cur.to_list(limit)
    out = []
    for d in items:
        d.pop('_id', None)
        for k in ('created', 'processed_at', 'commentTimestamp', 'ruleActivationStartedAt'):
            if isinstance(d.get(k), datetime):
                d[k] = d[k].isoformat()
        out.append({
            'id': d.get('id'),
            'igCommentId': d.get('ig_comment_id'),
            'mediaId': d.get('media_id'),
            'ruleId': d.get('rule_id'),
            'commentTimestamp': d.get('commentTimestamp') or d.get('timestamp'),
            'ruleActivationStartedAt': d.get('ruleActivationStartedAt'),
            'processExistingComments': bool(d.get('processExistingComments')),
            'commenterUsername': d.get('commenter_username'),
            'text': d.get('text'),
            'source': d.get('source'),
            'matched': bool(d.get('matched')),
            'actionStatus': d.get('action_status'),
            'skipReason': d.get('skipReason') or d.get('skip_reason'),
            'error': d.get('error'),
            'timestamp': d.get('timestamp'),
            'processedAt': d.get('processed_at'),
            'created': d.get('created'),
        })
    return {'count': len(out), 'items': out}


@api.get('/instagram/diagnostics/full')
async def instagram_diagnostics_full(user_id: str = Depends(get_current_user_id)):
    """Comprehensive end-to-end diagnostic.
    Hits Graph API directly with the stored token and reports every layer.
    Token values are never returned."""
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'user not found')

    token = u.get('meta_access_token') or ''
    ig_user_id = u.get('ig_user_id') or ''
    redirect_uri = f"{BACKEND_PUBLIC_URL}/api/instagram/callback"
    webhook_url = f"{BACKEND_PUBLIC_URL}/api/meta/webhook"

    runtime = {
        'appIdConfigured': bool(IG_APP_ID),
        'appSecretConfigured': bool(IG_APP_SECRET),
        'verifyTokenConfigured': bool(META_VERIFY_TOKEN),
        'graphApiVersion': 'v21.0',
        'graphHost': 'graph.instagram.com',
        'redirectUri': redirect_uri,
        'webhookUrl': webhook_url,
        'frontendUrl': FRONTEND_URL,
        'pollingEnabled': IG_POLL_ENABLED,
        'pollingIntervalSeconds': IG_POLL_INTERVAL_SECONDS,
    }

    account = {
        'connected': bool(u.get('instagramConnected')),
        'igUserId': ig_user_id,
        'username': u.get('instagramHandle'),
        'followers': u.get('instagramFollowers', 0),
        'tokenExists': bool(token),
        'tokenExpired': None,
        'tokenAppId': None,
        'scopes': [],
        'accountType': None,
        'authKind': u.get('ig_auth_kind'),
        'lastSubscribeStatus': u.get('ig_subscribe_status'),
    }

    # --- Active comment rules ---
    automations = await db.automations.find(
        {'user_id': user_id, 'status': 'active'}
    ).to_list(200)
    media_ids = await _collect_target_media_ids(u, automations) if (token and ig_user_id) else []
    rules = {
        'activeCount': len(automations),
        'activeCommentRules': sum(
            1 for a in automations
            if (a.get('trigger') or '').lower().startswith('comment:')
        ),
        'mediaIds': media_ids,
        'rulesPreview': [
            {
                'id': a.get('id'),
                'name': a.get('name'),
                'trigger': a.get('trigger'),
                'mediaId': a.get('trigger_media_id') or a.get('media_id'),
                'keyword': a.get('keyword'),
                'match': a.get('match'),
            } for a in automations[:10]
        ],
    }

    subscriptions = {'subscribedFields': [], 'raw': None, 'error': None}
    media_list = []
    comments_readability = []
    recent_errors = []

    if token and ig_user_id:
        async with httpx.AsyncClient(timeout=20) as c:
            # 1) debug_token — diagnostic-only, never affects blockerReason.
            #    IG Business Login tokens are issued by graph.instagram.com; the
            #    Facebook-graph debug_token endpoint frequently 400s for them.
            #    Try the IG host first, then fall back to FB host. Both errors
            #    are recorded but classified as 'diagnostic_only'.
            app_access_token = f'{IG_APP_ID}|{IG_APP_SECRET}'
            debug_attempts = []
            for host in ('graph.instagram.com', 'graph.facebook.com'):
                try:
                    r = await c.get(
                        f'https://{host}/debug_token',
                        params={'input_token': token,
                                'access_token': app_access_token},
                    )
                    if r.status_code == 200:
                        d = (r.json() or {}).get('data') or {}
                        account['tokenAppId'] = d.get('app_id')
                        scopes = d.get('scopes')
                        if not scopes and d.get('granular_scopes'):
                            scopes = [s.get('scope') for s in d.get('granular_scopes', []) if s.get('scope')]
                        account['scopes'] = scopes or []
                        expires_at = d.get('expires_at') or d.get('data_access_expires_at')
                        if expires_at:
                            account['tokenExpired'] = (
                                expires_at != 0 and expires_at < int(datetime.utcnow().timestamp())
                            )
                        account['tokenIsValid'] = bool(d.get('is_valid'))
                        account['debugTokenHost'] = host
                        debug_attempts = []  # success — drop earlier failures
                        break
                    else:
                        # Sanitize the error body before recording — Meta's
                        # error responses don't normally include the token,
                        # but redact defensively.
                        try:
                            body = _redact_secrets(r.json())
                        except Exception:
                            body = r.text[:300]
                        debug_attempts.append({'host': host, 'http': r.status_code, 'body': body})
                except Exception as e:
                    debug_attempts.append({'host': host, 'error': str(e)[:200]})
            for a in debug_attempts:
                # Mark diagnostic-only so UI/log readers don't treat it as a blocker.
                a['classification'] = 'diagnostic_only'
                recent_errors.append({'step': 'debug_token', **a})

            # 2) /me
            try:
                r = await c.get(
                    'https://graph.instagram.com/me',
                    params={'access_token': token,
                            'fields': 'user_id,username,account_type'},
                )
                if r.status_code == 200:
                    d = r.json() or {}
                    account['username'] = '@' + d.get('username') if d.get('username') else account['username']
                    account['accountType'] = d.get('account_type')
                else:
                    recent_errors.append({'step': 'me', 'http': r.status_code, 'body': r.text[:300]})
            except Exception as e:
                recent_errors.append({'step': 'me', 'error': str(e)[:200]})

            # 3) /{ig_user_id}/media
            try:
                r = await c.get(
                    f'https://graph.instagram.com/{ig_user_id}/media',
                    params={'access_token': token,
                            'fields': 'id,caption,comments_count,media_type,permalink,timestamp',
                            'limit': 25},
                )
                if r.status_code == 200:
                    media_list = (r.json() or {}).get('data') or []
                else:
                    recent_errors.append({'step': 'media', 'http': r.status_code, 'body': r.text[:300]})
            except Exception as e:
                recent_errors.append({'step': 'media', 'error': str(e)[:200]})

            # 4) /{media_id}/comments for each active rule's media (or fall back to first 5 media)
            check_media = list(media_ids) if media_ids else [m.get('id') for m in media_list[:5] if m.get('id')]
            count_lookup = {m.get('id'): m.get('comments_count', 0) for m in media_list}
            for mid in check_media[:10]:
                row = {'mediaId': mid, 'commentsCount': count_lookup.get(mid),
                       'commentsReturned': 0, 'readable': False,
                       'mismatch': False, 'likelyCause': None, 'http': None}
                try:
                    r = await c.get(
                        f'https://graph.instagram.com/{mid}/comments',
                        params={'access_token': token,
                                'fields': 'id,text,username,timestamp,from',
                                'limit': 25},
                    )
                    row['http'] = r.status_code
                    if r.status_code == 200:
                        items = (r.json() or {}).get('data') or []
                        row['commentsReturned'] = len(items)
                        row['readable'] = True
                        # If counter is missing, try fetching it now
                        if row['commentsCount'] is None:
                            try:
                                rm = await c.get(
                                    f'https://graph.instagram.com/{mid}',
                                    params={'access_token': token,
                                            'fields': 'comments_count'},
                                )
                                if rm.status_code == 200:
                                    row['commentsCount'] = (rm.json() or {}).get('comments_count')
                            except Exception:
                                pass
                        cc = row['commentsCount'] or 0
                        if cc > 0 and len(items) == 0:
                            row['mismatch'] = True
                            row['gated'] = True
                            row['likelyCause'] = (
                                'Meta can see comment count, but this app cannot read '
                                'comment contents. This indicates Meta access gate / '
                                'Advanced Access requirement for '
                                'instagram_business_manage_comments.'
                            )
                    else:
                        row['error'] = r.text[:300]
                        row['likelyCause'] = f'Graph API returned {r.status_code}.'
                except Exception as e:
                    row['error'] = str(e)[:200]
                comments_readability.append(row)

            # 5) /{ig_user_id}/subscribed_apps
            try:
                r = await c.get(
                    f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                    params={'access_token': token},
                )
                if r.status_code == 200:
                    body = r.json() or {}
                    fields = []
                    for app in body.get('data') or []:
                        for f in app.get('subscribed_fields') or []:
                            if f not in fields:
                                fields.append(f)
                    subscriptions['subscribedFields'] = fields
                    subscriptions['raw'] = body
                else:
                    subscriptions['error'] = f'http {r.status_code}: {r.text[:300]}'
            except Exception as e:
                subscriptions['error'] = str(e)[:200]

    # Recent processed comments
    recent_cur = db.comments.find({'user_id': user_id}).sort('created', -1).limit(20)
    recent_items = await recent_cur.to_list(20)
    recent_processed = []
    for d in recent_items:
        d.pop('_id', None)
        for k in ('created', 'processed_at'):
            if isinstance(d.get(k), datetime):
                d[k] = d[k].isoformat()
        recent_processed.append({
            'igCommentId': d.get('ig_comment_id'),
            'mediaId': d.get('media_id'),
            'commenterUsername': d.get('commenter_username'),
            'text': (d.get('text') or '')[:120],
            'source': d.get('source'),
            'matched': bool(d.get('matched')),
            'actionStatus': d.get('action_status'),
            'error': d.get('error'),
            'created': d.get('created'),
        })

    # Recent webhook events (counts by field)
    wh_cursor = db.webhook_log.find().sort('received', -1).limit(50)
    wh_recent = await wh_cursor.to_list(50)
    wh_field_counts: dict = {}
    for w in wh_recent:
        try:
            for entry in (w.get('payload') or {}).get('entry', []):
                for ch in entry.get('changes', []):
                    f = ch.get('field') or 'unknown'
                    wh_field_counts[f] = wh_field_counts.get(f, 0) + 1
                if entry.get('messaging'):
                    wh_field_counts['messaging'] = wh_field_counts.get('messaging', 0) + len(entry['messaging'])
        except Exception:
            pass

    # ---- Status logic ----
    instagram_connected = account['connected'] and account['tokenExists']
    comments_readable = any(r.get('readable') and not r.get('mismatch') and (r.get('commentsReturned', 0) > 0 or (r.get('commentsCount') or 0) == 0)
                            for r in comments_readability)
    any_mismatch = any(r.get('mismatch') for r in comments_readability)
    comments_webhook_subscribed = 'comments' in subscriptions['subscribedFields']
    has_active_comment_rule = rules['activeCommentRules'] > 0
    valid_media = bool(media_ids)

    blocker_reason = None
    if not instagram_connected:
        blocker_reason = 'instagram_not_connected'
    elif not has_active_comment_rule:
        blocker_reason = 'no_active_comment_rule'
    elif not valid_media:
        blocker_reason = 'no_media_id_resolved_for_rule'
    elif not comments_webhook_subscribed:
        blocker_reason = 'webhook_not_subscribed_to_comments_field'
    elif any_mismatch:
        blocker_reason = 'meta_access_gate_filtering_comments'
    elif not comments_readable and any(r.get('http') and r['http'] != 200 for r in comments_readability):
        blocker_reason = 'graph_api_comments_endpoint_error'

    comments_automation_ready = (
        instagram_connected and has_active_comment_rule and valid_media
        and comments_webhook_subscribed and (comments_readable or not any_mismatch)
    )

    # ---- Final classification panel (high-level, human-readable) ----
    def _verdict(ok, blocked=False):
        return 'BLOCKED' if blocked else ('OK' if ok else 'NOT_READY')
    classification = {
        'appConnection': _verdict(instagram_connected),
        'mediaMapping': _verdict(valid_media),
        'commentWebhookSubscription': _verdict(comments_webhook_subscribed),
        'graphCommentsReadability': _verdict(comments_readable, blocked=any_mismatch),
        'requiredNextStep': (
            'App Review / Advanced Access for instagram_business_manage_comments'
            if any_mismatch else (
                None if comments_automation_ready else
                ('Connect Instagram' if not instagram_connected else
                 'Add an active comment automation rule' if not has_active_comment_rule else
                 'Resolve a valid mediaId for the rule' if not valid_media else
                 'Subscribe webhook to comments field' if not comments_webhook_subscribed else
                 'Investigate Graph API errors')
            )
        ),
        'note': (
            'Token, code, media mapping, webhook subscription and polling are all OK. '
            'Comment contents are filtered at the Meta access-tier gate.'
            if any_mismatch and instagram_connected and has_active_comment_rule
               and valid_media and comments_webhook_subscribed
            else None
        ),
    }

    return {
        'runtime': runtime,
        'account': account,
        'rules': rules,
        'subscriptions': subscriptions,
        'classification': classification,
        'media': [
            {
                'id': m.get('id'),
                'mediaType': m.get('media_type'),
                'commentsCount': m.get('comments_count'),
                'permalink': m.get('permalink'),
                'timestamp': m.get('timestamp'),
                'caption': (m.get('caption') or '')[:80],
            } for m in media_list[:10]
        ],
        'commentsReadability': comments_readability,
        'polling': {
            'enabled': IG_POLL_ENABLED,
            'intervalSeconds': IG_POLL_INTERVAL_SECONDS,
        },
        'webhookFieldCountsRecent50': wh_field_counts,
        'recentProcessedComments': recent_processed,
        'recentErrors': recent_errors,
        'status': {
            'instagramConnected': instagram_connected,
            'commentsReadable': comments_readable,
            'commentsWebhookSubscribed': comments_webhook_subscribed,
            'hasActiveCommentRule': has_active_comment_rule,
            'validMediaId': valid_media,
            'commentsAutomationReady': comments_automation_ready,
            'blockerReason': blocker_reason,
        },
    }


# ---------------- DM Automation API ----------------
_DM_VALID_MODES = {'exact', 'contains', 'starts_with'}


def _dm_rule_out(d: dict) -> dict:
    if not d:
        return d
    return {
        'id': d.get('id'),
        'instagramAccountId': d.get('instagramAccountId') or d.get('igUserId'),
        'instagramAccountDbId': d.get('instagramAccountDbId') or d.get('instagram_account_id'),
        'instagramUsername': d.get('instagramUsername'),
        'name': d.get('name'),
        'keyword': d.get('keyword'),
        'matchMode': d.get('match_mode'),
        'replyText': d.get('reply_text'),
        'isActive': bool(d.get('is_active')),
        'createdAt': d.get('created_at').isoformat() if isinstance(d.get('created_at'), datetime) else d.get('created_at'),
        'updatedAt': d.get('updated_at').isoformat() if isinstance(d.get('updated_at'), datetime) else d.get('updated_at'),
    }


@api.get('/instagram/dm/rules')
async def list_dm_rules(user_id: str = Depends(get_current_user_id)):
    account = await getActiveInstagramAccount(user_id)
    rows = await db.dm_rules.find(_account_scoped_query(user_id, account)).sort('created_at', -1).to_list(500)
    return {'items': [_dm_rule_out(r) for r in rows], 'count': len(rows)}


@api.post('/instagram/dm/rules')
async def create_dm_rule(data: DmRuleIn, user_id: str = Depends(get_current_user_id)):
    import uuid as _uuid
    mode = (data.matchMode or 'contains').lower()
    if mode not in _DM_VALID_MODES:
        raise HTTPException(400, f'matchMode must be one of {sorted(_DM_VALID_MODES)}')
    if not data.name.strip() or not data.keyword.strip() or not data.replyText.strip():
        raise HTTPException(400, 'name, keyword and replyText are required')
    account = await getActiveInstagramAccount(user_id)
    ctx = _instagram_context_from_account(account)
    now = datetime.utcnow()
    doc = {
        'id': str(_uuid.uuid4()),
        'user_id': user_id,
        **ctx,
        'ig_user_id': ctx.get('igUserId') or None,
        'name': data.name.strip(),
        'keyword': data.keyword.strip(),
        'match_mode': mode,
        'reply_text': data.replyText,
        'is_active': bool(data.isActive),
        'created_at': now,
        'updated_at': now,
    }
    await db.dm_rules.insert_one(doc)
    return _dm_rule_out(doc)


@api.patch('/instagram/dm/rules/{rid}')
async def patch_dm_rule(rid: str, data: DmRulePatch, user_id: str = Depends(get_current_user_id)):
    account = await getActiveInstagramAccount(user_id)
    scoped = _account_scoped_query(user_id, account)
    update: dict = {'updated_at': datetime.utcnow()}
    if data.name is not None:
        update['name'] = data.name.strip()
    if data.keyword is not None:
        update['keyword'] = data.keyword.strip()
    if data.matchMode is not None:
        mode = data.matchMode.lower()
        if mode not in _DM_VALID_MODES:
            raise HTTPException(400, f'matchMode must be one of {sorted(_DM_VALID_MODES)}')
        update['match_mode'] = mode
    if data.replyText is not None:
        update['reply_text'] = data.replyText
    if data.isActive is not None:
        update['is_active'] = bool(data.isActive)
    res = await db.dm_rules.update_one({'id': rid, **scoped}, {'$set': update})
    if res.matched_count == 0:
        raise HTTPException(404, 'rule not found')
    doc = await db.dm_rules.find_one({'id': rid, **scoped})
    return _dm_rule_out(doc)


@api.delete('/instagram/dm/rules/{rid}')
async def delete_dm_rule(rid: str, user_id: str = Depends(get_current_user_id)):
    account = await getActiveInstagramAccount(user_id)
    res = await db.dm_rules.delete_one({'id': rid, **_account_scoped_query(user_id, account)})
    if res.deleted_count == 0:
        raise HTTPException(404, 'rule not found')
    return {'ok': True}


@api.post('/instagram/dm/test-rule')
async def test_dm_rule(data: DmTestIn, user_id: str = Depends(get_current_user_id)):
    """Match `text` against the user's active rules without sending anything."""
    account = await getActiveInstagramAccount(user_id)
    rules = await db.dm_rules.find(
        {**_account_scoped_query(user_id, account), 'is_active': True}
    ).to_list(200)
    matches = []
    for r in rules:
        if _dm_match(data.text, r.get('keyword') or '',
                     (r.get('match_mode') or 'contains').lower()):
            matches.append({
                'ruleId': r.get('id'),
                'name': r.get('name'),
                'keyword': r.get('keyword'),
                'matchMode': r.get('match_mode'),
                'replyText': r.get('reply_text'),
            })
    return {
        'inputText': data.text,
        'matchCount': len(matches),
        'firstMatch': matches[0] if matches else None,
        'allMatches': matches,
    }


@api.get('/instagram/dm/logs')
async def list_dm_logs(limit: int = 50, user_id: str = Depends(get_current_user_id)):
    limit = max(1, min(limit, 200))
    account = await getActiveInstagramAccount(user_id)
    rows = await db.dm_logs.find(_account_scoped_query(user_id, account)).sort('created', -1).limit(limit).to_list(limit)
    out = []
    for d in rows:
        d.pop('_id', None)
        created = d.get('created')
        out.append({
            'id': d.get('id'),
            'instagramAccountId': d.get('instagramAccountId') or d.get('igUserId'),
            'instagramAccountDbId': d.get('instagramAccountDbId') or d.get('instagram_account_id'),
            'instagramUsername': d.get('instagramUsername'),
            'senderId': d.get('sender_id'),
            'messageId': d.get('message_id'),
            'dedupKey': d.get('dedup_key'),
            'eventKind': d.get('event_kind'),
            'incomingText': d.get('incoming_text'),
            'matchedRuleId': d.get('matched_rule_id'),
            'matchedRuleName': d.get('matched_rule_name'),
            'replyText': d.get('reply_text'),
            'status': d.get('status'),
            'skipReason': d.get('skip_reason'),
            'error': d.get('error'),
            'source': d.get('source'),
            'isEcho': d.get('is_echo'),
            'created': created.isoformat() if isinstance(created, datetime) else created,
        })
    return {'items': out, 'count': len(out)}


@api.get('/instagram/dm/diagnostics')
async def dm_diagnostics(user_id: str = Depends(get_current_user_id)):
    account = await getActiveInstagramAccount(user_id)
    u = _with_instagram_account_context(await db.users.find_one({'id': user_id}) or {}, account)
    ig_user_id = account.get('instagramAccountId') or account.get('igUserId') or ''
    token = account.get('accessToken') or ''
    connected = bool(account.get('connectionValid') and token and ig_user_id)

    # messaging webhook subscription state — read live from Graph
    messaging_subscribed = False
    subscription_error = None
    if connected:
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.get(
                    f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                    params={'access_token': token},
                )
                if r.status_code == 200:
                    body = r.json() or {}
                    fields: list = []
                    for app in body.get('data') or []:
                        for f in app.get('subscribed_fields') or []:
                            fields.append(f)
                    messaging_subscribed = 'messages' in fields
                else:
                    subscription_error = f'http {r.status_code}'
        except Exception as e:
            subscription_error = str(e)[:200]

    active_rules = await db.dm_rules.count_documents({**_account_scoped_query(user_id, account), 'is_active': True})

    # Recent messaging events (count from last 50 webhook log rows)
    wh = await db.webhook_log.find().sort('received', -1).limit(50).to_list(50)
    msg_events = 0
    last_msg_at = None
    for w in wh:
        try:
            for entry in (w.get('payload') or {}).get('entry', []):
                ms = entry.get('messaging') or []
                if ms:
                    msg_events += len(ms)
                    rec = w.get('received')
                    if isinstance(rec, datetime) and (last_msg_at is None or rec > last_msg_at):
                        last_msg_at = rec
        except Exception:
            pass

    last_log = await db.dm_logs.find(_account_scoped_query(user_id, account)).sort('created', -1).limit(1).to_list(1)
    last_reply_status = last_log[0].get('status') if last_log else 'none'

    blocker_reason = None
    if not connected:
        blocker_reason = 'instagram_not_connected'
    elif not messaging_subscribed:
        blocker_reason = 'webhook_not_subscribed_to_messages_field'
    elif active_rules == 0:
        blocker_reason = 'no_active_dm_rule'

    return {
        'connected': connected,
        'igUserId': ig_user_id,
        'messagingWebhookSubscribed': messaging_subscribed,
        'subscriptionError': subscription_error,
        'activeDmRules': active_rules,
        'recentMessagingEvents': msg_events,
        'lastMessageAt': last_msg_at.isoformat() if last_msg_at else None,
        'lastReplyStatus': last_reply_status,
        'blockerReason': blocker_reason,
    }


def _redact_id(s):
    if not s or not isinstance(s, str):
        return s
    if len(s) <= 6:
        return s[:2] + '***'
    return s[:4] + '***' + s[-2:]


@api.get('/instagram/credentials/diagnostics')
async def instagram_credentials_diagnostics(user_id: str = Depends(get_current_user_id)):
    """Audit which credential set is wired into each integration step.
    Never returns the secret values themselves — only presence flags and the
    env-var name that supplied each one.
    """
    u = await db.users.find_one({'id': user_id})
    token = (u or {}).get('meta_access_token') or ''

    # Run debug_token using the Instagram App credential pair (which is the
    # pair that issues IG Business Login user tokens).
    token_app_id = None
    debug_token_works = False
    debug_token_error = None
    debug_token_host = None
    if token and INSTAGRAM_APP_ID and INSTAGRAM_APP_SECRET:
        app_access_token = f'{INSTAGRAM_APP_ID}|{INSTAGRAM_APP_SECRET}'
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                for host in ('graph.instagram.com', 'graph.facebook.com'):
                    try:
                        r = await c.get(
                            f'https://{host}/debug_token',
                            params={'input_token': token,
                                    'access_token': app_access_token},
                        )
                        if r.status_code == 200:
                            d = (r.json() or {}).get('data') or {}
                            token_app_id = str(d.get('app_id') or '') or None
                            debug_token_works = bool(d.get('is_valid'))
                            debug_token_host = host
                            break
                        else:
                            debug_token_error = f'{host} http {r.status_code}'
                    except Exception as e:
                        debug_token_error = f'{host} {str(e)[:120]}'
        except Exception as e:
            debug_token_error = str(e)[:200]

    warnings: list = []
    # OAuth integrity: warn loudly if the Instagram credential pair fell back
    # to the Meta App pair — that means INSTAGRAM_APP_ID/IG_APP_ID is unset and
    # we're driving Instagram OAuth with Facebook App credentials, which only
    # works if the user explicitly configured the same id+secret to be used
    # for both products on the Meta dashboard.
    if INSTAGRAM_APP_ID_SOURCE == 'META_APP_ID' or INSTAGRAM_APP_SECRET_SOURCE == 'META_APP_SECRET':
        warnings.append(
            'instagram_credentials_falling_back_to_meta_app: '
            f'INSTAGRAM_APP_ID resolved from {INSTAGRAM_APP_ID_SOURCE}, '
            f'INSTAGRAM_APP_SECRET resolved from {INSTAGRAM_APP_SECRET_SOURCE}. '
            'If the Instagram product on the Meta dashboard uses a different '
            'App ID/Secret pair than the Facebook product, set INSTAGRAM_APP_ID '
            'and INSTAGRAM_APP_SECRET (or IG_APP_ID/IG_APP_SECRET) explicitly.'
        )
    if not INSTAGRAM_APP_ID or not INSTAGRAM_APP_SECRET:
        warnings.append('instagram_app_credentials_missing')
    if not META_VERIFY_TOKEN:
        warnings.append('meta_webhook_verify_token_missing')
    if not META_WEBHOOK_APP_SECRET:
        warnings.append('meta_webhook_app_secret_missing')
    # debug_token cross-check
    matches_instagram = bool(token_app_id and INSTAGRAM_APP_ID
                             and token_app_id == INSTAGRAM_APP_ID)
    matches_meta = bool(token_app_id and META_APP_ID
                        and token_app_id == META_APP_ID)
    if token and token_app_id and not matches_instagram:
        if matches_meta:
            warnings.append(
                'token_was_issued_by_meta_app_id_not_instagram_app_id: '
                'the stored Instagram user token reports a Meta/Facebook App ID '
                'in debug_token, not the Instagram App ID. OAuth was likely '
                'driven by Facebook Login instead of Instagram Business Login.'
            )
        else:
            warnings.append(
                f'token_app_id_unknown_app: tokenAppId={token_app_id} matches '
                'neither INSTAGRAM_APP_ID nor META_APP_ID. Reconnect Instagram.'
            )

    return {
        'oauth': {
            'usesInstagramAppId': bool(INSTAGRAM_APP_ID),
            'instagramAppIdConfigured': bool(INSTAGRAM_APP_ID),
            'instagramAppSecretConfigured': bool(INSTAGRAM_APP_SECRET),
            'authorizeUrlClientIdSource': INSTAGRAM_APP_ID_SOURCE,
            'tokenExchangeSecretSource': INSTAGRAM_APP_SECRET_SOURCE,
            'authorizeHost': 'api.instagram.com',
            'tokenExchangeHost': 'api.instagram.com / graph.instagram.com',
        },
        'webhook': {
            'verifyTokenConfigured': bool(META_VERIFY_TOKEN),
            'verifyTokenSource': META_VERIFY_TOKEN_SOURCE,
            'signatureSecretSource': META_WEBHOOK_APP_SECRET_SOURCE,
            'signatureValidationEnabled': bool(META_WEBHOOK_APP_SECRET),
            'signatureEnforceMode': META_WEBHOOK_HMAC_ENFORCE,
            'metaAppIdConfigured': bool(META_APP_ID),
            'metaAppSecretConfigured': bool(META_APP_SECRET),
            'callbackUrl': f'{BACKEND_PUBLIC_URL}/api/instagram/webhook',
        },
        'graph': {
            'host': 'graph.instagram.com',
            'version': 'v21.0',
            'tokenSource': 'users.meta_access_token (per-user)',
        },
        'debugToken': {
            'appAccessTokenSource': f'{INSTAGRAM_APP_ID_SOURCE}|{INSTAGRAM_APP_SECRET_SOURCE}',
            'debugTokenWorks': debug_token_works,
            'debugTokenHost': debug_token_host,
            'debugTokenError': debug_token_error,
            'tokenAppId': token_app_id,
            'matchesInstagramAppId': matches_instagram,
            'matchesMetaAppId': matches_meta,
            'instagramAppIdSnapshot': INSTAGRAM_APP_ID[-4:] if INSTAGRAM_APP_ID else None,
            'metaAppIdSnapshot': META_APP_ID[-4:] if META_APP_ID else None,
        },
        'warnings': warnings,
    }


@api.get('/instagram/dm/debug-latest')
async def dm_debug_latest(user_id: str = Depends(get_current_user_id)):
    """Self-diagnostic: reads live DB collections + Graph subscription state.
    Never exposes tokens or full webhook payloads. Sender IDs are partially
    redacted. Used by the DM Automation page "Run DM debug" button.
    """
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'user not found')
    ig_user_id = u.get('ig_user_id') or ''
    token = u.get('meta_access_token') or ''
    connected = bool(u.get('instagramConnected') and token and ig_user_id)

    messaging_subscribed = False
    subscribed_fields_list: list = []
    sub_error = None
    if connected:
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                r = await c.get(
                    f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                    params={'access_token': token},
                )
                if r.status_code == 200:
                    body = r.json() or {}
                    for app in body.get('data') or []:
                        for f in app.get('subscribed_fields') or []:
                            subscribed_fields_list.append(f)
                    messaging_subscribed = 'messages' in subscribed_fields_list
                else:
                    sub_error = f'http {r.status_code}'
        except Exception as e:
            sub_error = str(e)[:200]

    rules_rows = await db.dm_rules.find(
        {'user_id': user_id, 'is_active': True}
    ).sort('created_at', -1).to_list(50)
    active_rules = [{
        'id': r.get('id'),
        'name': r.get('name'),
        'keyword': r.get('keyword'),
        'matchMode': r.get('match_mode'),
        'isActive': r.get('is_active'),
        'createdAt': r.get('created_at').isoformat()
                     if isinstance(r.get('created_at'), datetime) else r.get('created_at'),
    } for r in rules_rows]

    # Recent webhook events: scan last 50 webhook_log rows, surface only the
    # messaging events that belong to this IG account, with safe summaries.
    wh_rows = await db.webhook_log.find().sort('received', -1).limit(50).to_list(50)
    recent_events = []
    for w in wh_rows:
        try:
            payload = w.get('payload') or {}
            obj_kind = payload.get('object')
            received = w.get('received')
            for entry in payload.get('entry', []):
                entry_id = entry.get('id')
                if entry_id and entry_id != ig_user_id and entry_id != u.get('fb_page_id'):
                    continue
                changes = entry.get('changes') or []
                fields = sorted({c.get('field') for c in changes if c.get('field')})
                ms = entry.get('messaging') or []
                if not ms and not fields:
                    continue
                if ms:
                    for ev in ms:
                        cls = _classify_messaging_event(ev)
                        msg_text = cls['text'] or ''
                        recent_events.append({
                            'createdAt': received.isoformat()
                                         if isinstance(received, datetime) else received,
                            'object': obj_kind,
                            'fields': fields,
                            'hasMessagingArray': True,
                            'messagingItemShape': cls['item_keys'],
                            'messagingItemKeys': cls['item_keys'],
                            'messageKeys': cls['message_keys'],
                            'senderPresent': bool(cls['sender_id']),
                            'senderIdPresent': bool(cls['sender_id']),
                            'senderIdRedacted': _redact_id(cls['sender_id']),
                            'recipientPresent': bool(cls['recipient_id']),
                            'recipientIdPresent': bool(cls['recipient_id']),
                            'hasMessage': cls['has_message'],
                            'messageIdPresent': bool(cls['message_id']),
                            'messageTextPresent': bool(msg_text),
                            'hasRead': cls['has_read'],
                            'hasDelivery': cls['has_delivery'],
                            'hasPostback': cls['has_postback'],
                            'hasReaction': cls['has_reaction'],
                            'hasReferral': cls['has_referral'],
                            'hasAttachments': cls['has_attachments'],
                            'isEcho': cls['is_echo'],
                            'eventKind': cls['kind'],
                            'textPreview': (msg_text[:40] if msg_text else ''),
                        })
                else:
                    recent_events.append({
                        'createdAt': received.isoformat()
                                     if isinstance(received, datetime) else received,
                        'object': obj_kind,
                        'fields': fields,
                        'hasMessagingArray': False,
                        'messagingItemShape': [],
                        'messagingItemKeys': [],
                        'messageKeys': [],
                        'senderPresent': False,
                        'senderIdPresent': False,
                        'recipientPresent': False,
                        'recipientIdPresent': False,
                        'hasMessage': False,
                        'messageIdPresent': False,
                        'messageTextPresent': False,
                        'hasRead': False,
                        'hasDelivery': False,
                        'hasPostback': False,
                        'hasReaction': False,
                        'hasReferral': False,
                        'hasAttachments': False,
                        'isEcho': False,
                        'eventKind': 'unknown',
                        'textPreview': '',
                    })
        except Exception:
            continue
    recent_events = recent_events[:20]

    log_rows = await db.dm_logs.find({'user_id': user_id}).sort('created', -1).limit(20).to_list(20)
    recent_logs = []
    for d in log_rows:
        created = d.get('created')
        recent_logs.append({
            'createdAt': created.isoformat() if isinstance(created, datetime) else created,
            'senderId': _redact_id(d.get('sender_id')),
            'messageId': d.get('message_id'),
            'dedupKey': d.get('dedup_key'),
            'eventKind': d.get('event_kind'),
            'incomingText': (d.get('incoming_text') or '')[:120] if d.get('incoming_text') else None,
            'matchedRuleId': d.get('matched_rule_id'),
            'matchedRuleName': d.get('matched_rule_name'),
            'status': d.get('status'),
            'skipReason': d.get('skip_reason'),
            'error': d.get('error'),
        })

    # Build lastDecision from the most-recent messaging webhook event + most-recent log
    last_msg_event = next((e for e in recent_events if e.get('hasMessagingArray')), None)
    last_text_event = next(
        (e for e in recent_events
         if e.get('hasMessagingArray') and e.get('eventKind') == 'message_text'),
        None,
    )
    last_log = recent_logs[0] if recent_logs else None
    webhook_received = bool(last_msg_event)
    message_parsed = bool(last_text_event and last_text_event.get('senderIdPresent')
                          and last_text_event.get('messageTextPresent'))
    rule_matched = bool(last_log and last_log.get('matchedRuleId'))
    send_attempted = bool(last_log and last_log.get('status') in ('replied', 'failed'))
    reply_sent = bool(last_log and last_log.get('status') == 'replied')

    # Classification
    blocker = None
    fix = None
    if not connected:
        blocker = 'instagram_not_connected'
        fix = 'Reconnect Instagram from Settings.'
    elif not messaging_subscribed:
        blocker = 'webhook_not_subscribed_to_messages_field'
        fix = 'POST /api/instagram/dm/resubscribe to subscribe the messages field on this IG account.'
    elif not active_rules:
        blocker = 'no_active_dm_rule'
        fix = 'Create or activate a rule on the DM Automation page.'
    elif not webhook_received:
        blocker = 'no_messaging_webhook_received'
        fix = 'Send a test DM from a different IG account. If still nothing arrives, the IG account is not subscribed for messages — call resubscribe.'
    elif not last_text_event:
        kinds = sorted({e.get('eventKind') for e in recent_events
                        if e.get('hasMessagingArray') and e.get('eventKind')})
        blocker = 'no_message_text_event'
        fix = ('Meta delivered messaging events but none were message_text. '
               f'Observed kinds: {kinds}. '
               'If you only see read/delivery/reaction, send a brand-new text DM '
               'from another IG account that has not previously messaged you. '
               'If you only see message_echo, the test DM was sent FROM the '
               'connected business account itself.')
    elif not message_parsed:
        blocker = 'webhook_payload_shape_mismatch'
        fix = 'A message_text event arrived but sender/text fields were missing. Inspect recentWebhookEvents.messagingItemKeys / messageKeys.'
    elif last_log and last_log.get('skipReason') == 'no_rule_match':
        blocker = 'rule_did_not_match_text'
        fix = f'Incoming text did not satisfy any active rule. Check keyword + matchMode against text="{(last_log.get("incomingText") or "")[:40]}".'
    elif last_log and last_log.get('skipReason') == 'duplicate':
        blocker = 'duplicate_event'
        fix = 'This was a webhook replay. Send a fresh DM.'
    elif last_log and last_log.get('skipReason') in ('echo', 'self_message'):
        blocker = 'echo_or_self_message'
        fix = 'The DM came from the connected business account itself or was an echo. Send from a different IG account.'
    elif last_log and last_log.get('skipReason') in ('missing_sender', 'missing_text'):
        blocker = f'webhook_{last_log.get("skipReason")}'
        fix = 'Meta delivered an event without required fields. See recentWebhookEvents.'
    elif last_log and last_log.get('status') == 'failed':
        blocker = 'graph_send_error'
        fix = f'Graph send failed: {last_log.get("error") or "unknown"}. Common causes: 24h messaging window closed, instagram_business_manage_messages permission missing, or invalid recipient.'
    elif rule_matched and reply_sent:
        blocker = None
        fix = 'Working — last DM was replied to.'

    # ---------------- Identity panel ----------------
    graph_me_id = None
    graph_username = None
    graph_account_type = None
    graph_me_error = None
    if connected:
        try:
            async with httpx.AsyncClient(timeout=15) as c:
                me = await c.get(
                    'https://graph.instagram.com/me',
                    params={'fields': 'id,username,account_type',
                            'access_token': token},
                )
                if me.status_code == 200:
                    mb = me.json() or {}
                    graph_me_id = mb.get('id')
                    graph_username = mb.get('username')
                    graph_account_type = mb.get('account_type')
                else:
                    graph_me_error = f'http {me.status_code}'
        except Exception as e:
            graph_me_error = str(e)[:200]

    # Scan webhook_log GLOBALLY (not filtered to this user) to find what IG
    # account ids Meta is actually addressing in `entry.id` / `recipient.id`.
    wh_all = await db.webhook_log.find().sort('received', -1).limit(50).to_list(50)
    entry_ids: list = []
    recipient_ids: list = []
    sender_ids: list = []
    unmapped_count = 0
    messaging_event_count = 0
    for w in wh_all:
        try:
            for entry in (w.get('payload') or {}).get('entry', []):
                eid = entry.get('id')
                if eid:
                    entry_ids.append(eid)
                ms = entry.get('messaging') or []
                for ev in ms:
                    messaging_event_count += 1
                    rid = (ev.get('recipient') or {}).get('id')
                    sid = (ev.get('sender') or {}).get('id')
                    if rid:
                        recipient_ids.append(rid)
                    if sid:
                        sender_ids.append(sid)
                # Unmapped: entry.id doesn't match this user's ig_user_id/fb_page_id
                # and no recipient.id in messaging items matches either.
                matched = (eid == ig_user_id) or (eid == u.get('fb_page_id'))
                if not matched:
                    rids = {(ev.get('recipient') or {}).get('id') for ev in ms}
                    if not (ig_user_id in rids or u.get('fb_page_id') in rids):
                        unmapped_count += len(ms) or 1
        except Exception:
            continue

    entry_ids_unique = sorted(set(entry_ids))
    recipient_ids_unique = sorted(set(recipient_ids))
    sender_ids_unique = sorted(set(sender_ids))

    id_match = bool(graph_me_id and ig_user_id and graph_me_id == ig_user_id)
    mismatch_reason = None
    if not graph_me_id:
        mismatch_reason = graph_me_error or 'graph_me_unavailable'
    elif graph_me_id != ig_user_id:
        mismatch_reason = 'graph_me_id_does_not_match_db_ig_user_id'
    elif entry_ids_unique and ig_user_id not in entry_ids_unique \
            and ig_user_id not in recipient_ids_unique \
            and (not u.get('fb_page_id') or u['fb_page_id'] not in entry_ids_unique):
        mismatch_reason = 'webhook_entry_id_does_not_match_connected_ig_user_id'

    identity = {
        'dbIgUserId': ig_user_id,
        'dbIgUserIdRedacted': _redact_id(ig_user_id),
        'graphMeId': graph_me_id,
        'graphMeIdRedacted': _redact_id(graph_me_id),
        'graphUsername': graph_username,
        'graphAccountType': graph_account_type,
        'graphMeError': graph_me_error,
        'subscribedAppsCheckedForIgUserId': ig_user_id,
        'subscribedAppsCheckedForIgUserIdRedacted': _redact_id(ig_user_id),
        'latestWebhookEntryIds': [_redact_id(x) for x in entry_ids_unique[:10]],
        'latestWebhookRecipientIds': [_redact_id(x) for x in recipient_ids_unique[:10]],
        'latestWebhookSenderIds': [_redact_id(x) for x in sender_ids_unique[:10]],
        'idMatch': id_match,
        'mismatchReason': mismatch_reason,
    }

    # ---------------- Webhook config panel ----------------
    webhook_config = {
        'expectedWebhookPath': '/api/instagram/webhook',
        'verifyTokenConfigured': bool(META_VERIFY_TOKEN),
        'appIdConfigured': bool(IG_APP_ID),
        'appSecretConfigured': bool(IG_APP_SECRET),
        'signatureValidationEnabled': bool(META_WEBHOOK_APP_SECRET),
        'signatureEnforceMode': META_WEBHOOK_HMAC_ENFORCE,
        'graphApiVersion': 'v21.0',
        'graphHost': 'graph.instagram.com',
        'callbackUrlUsedByRuntime': f'{BACKEND_PUBLIC_URL}/api/instagram/webhook',
        'oauthRedirectUri': f'{BACKEND_PUBLIC_URL}/api/instagram/callback',
        'backendPublicUrl': BACKEND_PUBLIC_URL,
        'webhookEventsStored': len(wh_all),
    }

    # ---------------- Processor panel ----------------
    dm_logs_for_user = await db.dm_logs.count_documents({'user_id': user_id})
    dm_logs_global = await db.dm_logs.count_documents({})
    skip_reasons_recent = sorted({l.get('skipReason') for l in recent_logs if l.get('skipReason')})
    processor = {
        'webhookEventsCount': messaging_event_count,
        'dmLogsForCurrentUser': dm_logs_for_user,
        'dmLogsGlobalRecent': dm_logs_global,
        'unmappedMessagingEvents': unmapped_count,
        'recentSkipReasons': list(skip_reasons_recent),
    }

    # ---------------- lastDecision (smarter blocker order) ----------------
    blocker = None
    fix = None
    if not connected:
        blocker = 'instagram_not_connected'
        fix = 'Reconnect Instagram from Settings.'
    elif graph_me_error:
        blocker = 'instagram_token_invalid_or_expired'
        fix = ('Graph /me could not validate the stored Instagram token. '
               'Disconnect and reconnect Instagram from Settings so the app '
               'stores a fresh token, ig_user_id, and webhook subscription.')
    elif mismatch_reason == 'graph_me_id_does_not_match_db_ig_user_id':
        blocker = 'id_mismatch'
        fix = ('The access_token in DB returns a different IG user id from '
               'the stored ig_user_id. Disconnect and reconnect Instagram so '
               'the token, ig_user_id, and webhook subscription all reference '
               'the same account.')
    elif mismatch_reason == 'webhook_entry_id_does_not_match_connected_ig_user_id':
        blocker = 'webhook_account_mismatch'
        fix = ('Webhook events are arriving for a different IG account id than '
               'the one stored in users.ig_user_id. Confirm the same Meta App '
               'is used for OAuth and webhook subscription, and that the same '
               'IG account id was passed to subscribed_apps.')
    elif not messaging_subscribed:
        blocker = 'webhook_not_subscribed_to_messages_field'
        fix = 'POST /api/instagram/dm/resubscribe to subscribe the messages field on this IG account.'
    elif not active_rules:
        blocker = 'no_active_dm_rule'
        fix = 'Create or activate a rule on the DM Automation page.'
    elif not webhook_received:
        blocker = 'no_messaging_webhook_received'
        fix = 'Send a test DM from a different IG account. If still nothing arrives, the IG account is not subscribed for messages — call resubscribe.'
    elif messaging_event_count > 0 and dm_logs_for_user == 0:
        blocker = 'processor_not_logging_events'
        fix = ('Webhook messaging events exist but the processor wrote zero '
               'dm_logs rows for this user. Likely user mapping failure '
               '(entry.id and recipient.id never matched users.ig_user_id) '
               'or the events were stored before the logging change deployed. '
               f'unmappedMessagingEvents={unmapped_count}. Send a fresh DM '
               'and re-run debug.')
    elif not last_text_event:
        kinds = sorted({e.get('eventKind') for e in recent_events
                        if e.get('hasMessagingArray') and e.get('eventKind')})
        blocker = 'no_message_text_event'
        fix = ('Meta delivered messaging events but none were message_text. '
               f'Observed kinds: {kinds}. Verify ID mapping and webhook payload '
               'shape first. In Development mode, app roles may affect some tests, '
               'but do not assume this is the cause until ID mapping and '
               'payload handling are proven correct.')
    elif not message_parsed:
        blocker = 'webhook_payload_shape_mismatch'
        fix = 'A message_text event arrived but sender/text fields were missing. Inspect recentWebhookEvents.messagingItemKeys / messageKeys.'
    elif last_log and last_log.get('skipReason') == 'no_rule_match':
        blocker = 'message_text_received_but_no_rule_match'
        fix = f'Incoming text did not satisfy any active rule. Check keyword + matchMode against text="{(last_log.get("incomingText") or "")[:40]}".'
    elif last_log and last_log.get('skipReason') == 'duplicate':
        blocker = 'duplicate_event'
        fix = 'This was a webhook replay. Send a fresh DM.'
    elif last_log and last_log.get('skipReason') in ('echo', 'self_message'):
        blocker = 'echo_or_self_message'
        fix = 'The DM came from the connected business account itself or was an echo. Send from a different IG account.'
    elif last_log and last_log.get('skipReason') in ('missing_sender', 'missing_text'):
        blocker = f'webhook_{last_log.get("skipReason")}'
        fix = 'Meta delivered an event without required fields. See recentWebhookEvents.'
    elif last_log and last_log.get('status') == 'failed':
        blocker = 'send_api_failed'
        fix = f'Graph send failed: {last_log.get("error") or "unknown"}.'
    elif rule_matched and reply_sent:
        blocker = None
        fix = 'replied_successfully — last DM was replied to.'

    return {
        'connected': connected,
        'igUserId': ig_user_id,
        'messagingWebhookSubscribed': messaging_subscribed,
        'subscribedFields': subscribed_fields_list,
        'subscriptionError': sub_error,
        'identity': identity,
        'webhookConfig': webhook_config,
        'processor': processor,
        'activeRules': active_rules,
        'recentWebhookEvents': recent_events,
        'recentDmLogs': recent_logs,
        'lastDecision': {
            'webhookReceived': webhook_received,
            'messageParsed': message_parsed,
            'ruleMatched': rule_matched,
            'sendAttempted': send_attempted,
            'replySent': reply_sent,
            'blocker': blocker,
            'fix': fix,
        },
    }


@api.post('/instagram/dm/resubscribe')
async def dm_resubscribe(user_id: str = Depends(get_current_user_id)):
    """Re-subscribe the connected IG account to the messaging webhook fields.
    Calls POST /{ig_user_id}/subscribed_apps with the messaging field set,
    then GETs the current state and returns it.
    """
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'user not found')
    ig_user_id = u.get('ig_user_id') or ''
    token = u.get('meta_access_token') or ''
    if not (ig_user_id and token):
        raise HTTPException(400, 'instagram not connected')

    fields = 'messages,messaging_postbacks,messaging_seen,message_reactions'
    post_status = None
    post_body = None
    get_status = None
    subscribed_fields_list: list = []
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            pr = await c.post(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={'subscribed_fields': fields, 'access_token': token},
            )
            post_status = pr.status_code
            try:
                post_body = pr.json()
            except Exception:
                post_body = {'raw': pr.text[:300]}
            gr = await c.get(
                f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                params={'access_token': token},
            )
            get_status = gr.status_code
            if gr.status_code == 200:
                body = gr.json() or {}
                for app in body.get('data') or []:
                    for f in app.get('subscribed_fields') or []:
                        subscribed_fields_list.append(f)
    except Exception as e:
        raise HTTPException(502, f'graph error: {str(e)[:200]}')

    # Cross-check: ask /me with the same token and confirm the IG account id
    # we just subscribed actually matches the token's IG identity.
    graph_me_id = None
    graph_me_error = None
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            mr = await c.get('https://graph.instagram.com/me',
                             params={'fields': 'id', 'access_token': token})
            if mr.status_code == 200:
                graph_me_id = (mr.json() or {}).get('id')
            else:
                graph_me_error = f'http {mr.status_code}'
    except Exception as e:
        graph_me_id = None
        graph_me_error = str(e)[:200]

    id_match = bool(graph_me_id and graph_me_id == ig_user_id)
    messages_subscribed = 'messages' in subscribed_fields_list
    blocker = None
    fix = None
    if graph_me_error:
        blocker = 'instagram_token_invalid_or_expired'
        fix = ('Graph /me could not validate the stored Instagram token. '
               'Disconnect and reconnect Instagram from Settings, then run '
               'resubscribe again.')
    elif graph_me_id and not id_match:
        blocker = 'id_mismatch'
        fix = ('The stored Instagram token resolves to a different IG user id. '
               'Disconnect and reconnect Instagram from Settings.')
    elif not messages_subscribed:
        blocker = 'webhook_not_subscribed_to_messages_field'
        fix = 'Graph did not confirm the messages field. Check postStatus/getStatus and retry after reconnecting if needed.'

    return {
        'igUserIdUsed': ig_user_id,
        'igUserId': ig_user_id,
        'graphMeId': graph_me_id,
        'graphMeError': graph_me_error,
        'idMatch': id_match,
        'requestedFields': fields.split(','),
        'postStatus': post_status,
        'postResponse': _redact_secrets(post_body) if isinstance(post_body, (dict, list)) else post_body,
        'getStatus': get_status,
        'subscribedFields': subscribed_fields_list,
        'messagesSubscribed': messages_subscribed,
        'ok': bool(id_match and messages_subscribed and not graph_me_error),
        'blocker': blocker,
        'fix': fix,
    }


@api.get('/instagram/accounts')
async def instagram_accounts(user_id: str = Depends(get_current_user_id)):
    user_doc = await db.users.find_one({'id': user_id}) or {}
    if user_doc:
        await _sync_user_instagram_account_doc(user_doc)
    try:
        active_account = await getActiveInstagramAccount(user_id)
        active_account_id = active_account.get('id') or ''
    except HTTPException as e:
        if e.status_code != 400:
            raise
        active_account_id = user_doc.get('active_instagram_account_id') or ''
    rows = await db.instagram_accounts.find({'userId': user_id}).sort('updatedAt', -1).to_list(100)
    return {
        'accounts': [_instagram_account_public_row(row, active_account_id) for row in rows],
        'activeInstagramAccountId': active_account_id or None,
        'count': len(rows),
    }


@api.post('/instagram/accounts/{account_id}/activate')
async def instagram_account_activate(account_id: str, user_id: str = Depends(get_current_user_id)):
    user_doc = await db.users.find_one({'id': user_id})
    if user_doc:
        await _sync_user_instagram_account_doc(user_doc)
    account = await db.instagram_accounts.find_one({'id': account_id, 'userId': user_id})
    if not account:
        raise HTTPException(404, 'Instagram account not found')
    token = account.get('accessToken') or ''
    instagram_account_id = account.get('instagramAccountId') or account.get('igUserId') or ''
    if not (token and instagram_account_id):
        raise HTTPException(400, 'Instagram account is missing token or id')
    if account.get('refreshStatus') == 'expired':
        raise HTTPException(400, 'Instagram token expired. Reconnect this account.')
    now = datetime.utcnow()
    await db.instagram_accounts.update_many(
        {'userId': user_id},
        {'$set': {'isCurrent': False, 'updatedAt': now}},
    )
    await db.instagram_accounts.update_one(
        {'id': account_id, 'userId': user_id},
        {'$set': {'isCurrent': True, 'isActive': True, 'updatedAt': now}},
    )
    await db.users.update_one(
        {'id': user_id},
        {'$set': {
            'active_instagram_account_id': account_id,
            'instagramConnected': True,
            'instagram_connection_valid': bool(account.get('connectionValid')),
            'instagramConnectionValid': bool(account.get('connectionValid')),
            'instagram_connection_blocker': None if account.get('connectionValid') else 'selected_account_invalid',
            'instagramHandle': account.get('username') or '',
            'instagram_account_type': account.get('accountType'),
            'ig_user_id': instagram_account_id,
            'meta_access_token': token,
            'instagramTokenSource': account.get('tokenSource'),
            'instagram_token_source': account.get('tokenSource'),
            'tokenExpiresAt': _parse_graph_datetime(account.get('tokenExpiresAt')),
            'instagram_token_expires_at': _parse_graph_datetime(account.get('tokenExpiresAt')),
            'lastRefreshedAt': _parse_graph_datetime(account.get('lastRefreshedAt')),
            'refreshStatus': account.get('refreshStatus'),
            'refreshAttempts': int(account.get('refreshAttempts') or 0),
            'updated': now,
        }},
    )
    refreshed = await db.instagram_accounts.find_one({'id': account_id}) or account
    return {'ok': True, 'account': _instagram_account_public_row(refreshed, account_id)}


@api.post('/cron/refresh-instagram-tokens')
async def cron_refresh_instagram_tokens(request: Request):
    if not _cron_secret_is_valid(_cron_secret_from_request(request)):
        raise HTTPException(403, 'Invalid cron secret')
    return await runInstagramTokenRefreshCron()


@api.get('/instagram/token-refresh/status')
async def instagram_token_refresh_status(user_id: str = Depends(get_current_user_id)):
    user_doc = await db.users.find_one({'id': user_id})
    if user_doc:
        await _sync_user_instagram_account_doc(user_doc)
    rows = await db.instagram_accounts.find({'userId': user_id}).sort('updatedAt', -1).to_list(100)
    return {
        'accounts': [_token_refresh_public_row(row) for row in rows],
        'count': len(rows),
    }


# ---------------- root ----------------
@api.get('/')
async def root():
    return {'app': 'mychat', 'status': 'ok'}


app.include_router(api)


# ---------------- WebSocket ----------------
@app.websocket('/ws/{user_id}')
async def websocket_endpoint(ws: WebSocket, user_id: str, token: str = Query(...)):
    try:
        uid = decode_token(token)
        if uid != user_id:
            await ws.close(code=4003)
            return
    except Exception:
        await ws.close(code=4001)
        return

    await ws_manager.connect(user_id, ws)
    try:
        while True:
            data = await ws.receive_json()
            msg_type = data.get('type')

            if msg_type == 'message':
                conv_id = data.get('conv_id')
                text = (data.get('text') or '').strip()
                if not conv_id or not text:
                    continue
                import uuid as _uuid
                conv = await db.conversations.find_one({'id': conv_id, 'user_id': user_id})
                if not conv:
                    continue
                # Try Graph API first so we can report delivery status
                user_doc = await db.users.find_one({'id': user_id})
                ig_recipient = conv.get('contact', {}).get('ig_id')
                delivered = False
                if user_doc and user_doc.get('instagramConnected') and ig_recipient:
                    delivered = await send_ig_dm(
                        user_doc.get('meta_access_token', ''),
                        user_doc.get('ig_user_id', ''),
                        ig_recipient, text
                    )
                msg = {'id': str(_uuid.uuid4()), 'from': 'me', 'text': text,
                       'time': datetime.utcnow().strftime('%I:%M %p'),
                       'delivered': delivered}
                await db.conversations.update_one(
                    {'id': conv_id},
                    {'$push': {'messages': msg},
                     '$set': {'lastMessage': text, 'time': 'now', 'unread': 0}}
                )
                await ws_manager.send(user_id, {'type': 'message', 'conv_id': conv_id, 'message': msg})

            elif msg_type == 'ping':
                await ws_manager.send(user_id, {'type': 'pong'})

    except WebSocketDisconnect:
        ws_manager.disconnect(user_id)
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=['*'],
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.on_event('startup')
async def _startup():
    global _poll_task
    # Ensure a unique index on (user_id, ig_comment_id) so dedup is fast and safe
    try:
        await db.comments.drop_index('uniq_user_ig_comment')
    except Exception:
        pass
    try:
        await db.comments.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('ig_comment_id', 1)],
            unique=True, sparse=True, name='uniq_user_account_ig_comment'
        )
    except Exception as e:
        logger.warning('comments index create: %s', e)
    # DM automation: dedup index on (user_id, message_id) so the same incoming
    # DM is never replied to twice even if the webhook is replayed.
    # Drop legacy non-unique-safe index on (user_id, message_id) which collided
    # on null mid and silently deduped real messages. Replaced with a unique
    # index on (user_id, dedup_key) where dedup_key is mid|id|content-hash.
    try:
        await db.dm_logs.drop_index('uniq_user_dm_message')
    except Exception:
        pass
    try:
        await db.dm_logs.drop_index('uniq_user_dm_dedup_key')
    except Exception:
        pass
    try:
        await db.dm_logs.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('dedup_key', 1)],
            unique=True, sparse=True, name='uniq_user_ig_dm_dedup_key',
        )
    except Exception as e:
        logger.warning('dm_logs dedup_key index create: %s', e)
    try:
        await db.dm_rules.create_index([('user_id', 1), ('is_active', 1)],
                                       name='dm_rules_user_active')
    except Exception as e:
        logger.warning('dm_rules index create: %s', e)
    try:
        await db.automations.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('status', 1)],
            name='automations_user_ig_status',
        )
        await db.comments.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('created', -1)],
            name='comments_user_ig_created',
        )
        await db.conversations.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('created', -1)],
            name='conversations_user_ig_created',
        )
        await db.dm_rules.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('is_active', 1)],
            name='dm_rules_user_ig_active',
        )
        await db.dm_logs.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('created', -1)],
            name='dm_logs_user_ig_created',
        )
        await db.comment_dm_sessions.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('created', -1)],
            name='comment_dm_sessions_user_ig_created',
        )
        await db.contacts.create_index(
            [('user_id', 1), ('instagramAccountId', 1), ('created', -1)],
            name='contacts_user_ig_created',
        )
    except Exception as e:
        logger.warning('account scoped index create: %s', e)
    try:
        await db.comment_dm_sessions.create_index(
            [('user_id', 1), ('recipient_id', 1), ('status', 1), ('created', -1)],
            name='comment_dm_sessions_pending_lookup',
        )
    except Exception as e:
        logger.warning('comment_dm_sessions index create: %s', e)
    try:
        await db.instagram_accounts.create_index([('id', 1)], unique=True,
                                                 name='instagram_accounts_id_unique')
        await db.instagram_accounts.create_index(
            [('userId', 1), ('instagramAccountId', 1)],
            unique=True,
            sparse=True,
            name='instagram_accounts_user_ig_unique',
        )
        await db.instagram_accounts.create_index(
            [('isActive', 1), ('connectionValid', 1), ('tokenExpiresAt', 1)],
            name='instagram_accounts_refresh_due',
        )
    except Exception as e:
        logger.warning('instagram_accounts index create: %s', e)
    try:
        migrated = await _ensure_instagram_account_docs_for_connected_users()
        if migrated:
            logger.info('instagram_accounts_migrated_from_users count=%s', migrated)
        scoped_users = await db.users.find({
            'instagramConnected': True,
            'ig_user_id': {'$nin': [None, '']},
        }).limit(1000).to_list(1000)
        scoped_rules = 0
        for user_doc in scoped_users:
            scoped_rules += await _ensure_automation_account_scope_for_user(user_doc)
        if scoped_rules:
            logger.info('instagram_automation_account_scope_migrated count=%s', scoped_rules)
    except Exception as e:
        logger.warning('instagram_accounts migration: %s', e)
    try:
        now = datetime.utcnow()
        comment_rules = {
            '$or': [
                {'trigger': {'$regex': '^comment:', '$options': 'i'}},
                {'nodes.data.trigger': {'$regex': '^comment:', '$options': 'i'}},
            ],
        }
        await db.automations.update_many(
            {**comment_rules, 'activationStartedAt': {'$exists': False}},
            {'$set': {'activationStartedAt': now}}
        )
        await db.automations.update_many(
            {**comment_rules, 'processExistingComments': {'$exists': False}},
            {'$set': {'processExistingComments': False}}
        )
        await db.automations.update_many(
            {**comment_rules, 'createdAt': {'$exists': False}},
            {'$set': {'createdAt': now}}
        )
        await db.automations.update_many(
            {**comment_rules, 'updatedAt': {'$exists': False}},
            {'$set': {'updatedAt': now}}
        )
    except Exception as e:
        logger.warning('comment rule activation migration: %s', e)
    if IG_POLL_ENABLED:
        _poll_task = asyncio.create_task(_comment_poller_loop())
    else:
        logger.info('Comment poller disabled via IG_POLL_ENABLED=0')


@app.on_event('shutdown')
async def shutdown_db_client():
    global _poll_task
    if _poll_task:
        _poll_task.cancel()
        try:
            await _poll_task
        except Exception:
            pass
    client.close()
