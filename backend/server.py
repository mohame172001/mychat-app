import os
import asyncio
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
import httpx

from models import (
    SignupIn, LoginIn, AuthOut, UserPublic,
    AutomationIn, AutomationPatch, Automation,
    ContactIn, ContactPatch, Contact,
    BroadcastIn, BroadcastPatch, Broadcast,
    MessageIn, Conversation,
    DmRuleIn, DmRulePatch, DmTestIn,
)
from auth_utils import hash_password, verify_password, create_token, get_current_user_id, decode_token

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
# it; otherwise we fall back to META_APP_SECRET. Validation is currently OFF
# (no signature check happens) but the resolved secret + its source are
# surfaced by the credentials diagnostics so misconfiguration is visible.
META_WEBHOOK_APP_SECRET, META_WEBHOOK_APP_SECRET_SOURCE = _resolve_env(
    'META_WEBHOOK_APP_SECRET', 'META_APP_SECRET')
FRONTEND_URL = os.environ.get('FRONTEND_URL', 'http://localhost:3000')
BACKEND_PUBLIC_URL = os.environ.get('BACKEND_PUBLIC_URL', 'http://localhost:8001')

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
    SECRET_KEYS = {'access_token', 'meta_access_token', 'client_secret',
                   'app_secret', 'refresh_token', 'token', 'authorization'}
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
async def send_ig_dm(access_token: str, ig_user_id: str, recipient_ig_id: str, text: str) -> bool:
    """Send a DM via Instagram Graph API. Returns True on success."""
    if not access_token or not ig_user_id:
        logger.warning('send_ig_dm: missing access_token or ig_user_id')
        return False
    url = f'https://graph.instagram.com/{ig_user_id}/messages'
    payload = {
        'recipient': {'id': recipient_ig_id},
        'message': {'text': text},
    }
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(url, json=payload, params={'access_token': access_token})
            if r.status_code == 200:
                return True
            logger.error('send_ig_dm error %s: %s', r.status_code, r.text)
            return False
    except Exception as e:
        logger.exception('send_ig_dm exception: %s', e)
        return False


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
        return

    node_map = {n['id']: n for n in nodes}
    edge_map: Dict[str, list] = {}
    for e in edges:
        edge_map.setdefault(e['source'], []).append(e['target'])

    start = next((n for n in nodes if n.get('type') == 'trigger'), None)
    if not start:
        return

    access_token = user.get('meta_access_token', '')
    ig_user_id = user.get('ig_user_id', '')
    current_ids = [start['id']]
    visited: set = set()

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
                ok = await send_ig_dm(access_token, ig_user_id, sender_ig_id, msg_text)
                logger.info('Flow message to %s: %s (ok=%s)', sender_ig_id, msg_text[:40], ok)
        elif ntype == 'reply_comment':
            msg_text = data.get('text') or data.get('message', '')
            if msg_text and comment_context and comment_context.get('ig_comment_id'):
                ok = await reply_to_ig_comment(access_token, comment_context['ig_comment_id'], msg_text)
                logger.info('Flow comment reply on %s: %s (ok=%s)',
                            comment_context['ig_comment_id'], msg_text[:40], ok)
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

    await db.automations.update_one(
        {'id': automation['id']},
        {'$inc': {'sent': 1}, '$set': {'updated': datetime.utcnow()}}
    )


# ---------------- helpers ----------------
def _strip_mongo(doc):
    if doc and '_id' in doc:
        doc.pop('_id', None)
    return doc


def _public_user(u: dict) -> UserPublic:
    instagram_valid = bool(u.get('instagramConnected') and u.get('instagram_connection_valid'))
    return UserPublic(
        id=u['id'], username=u['username'], name=u['name'], email=u['email'],
        avatar=u.get('avatar') or f"https://i.pravatar.cc/150?u={u['username']}",
        instagramConnected=instagram_valid,
        instagramHandle=u.get('instagramHandle'),
        instagramConnectionValid=instagram_valid,
        instagramAccountType=u.get('instagram_account_type'),
    )


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
    cursor = db.automations.find({'user_id': user_id}).sort('updated', -1)
    return [_strip_mongo(d) for d in await cursor.to_list(1000)]


@api.post('/automations')
async def create_automation(data: AutomationIn, user_id: str = Depends(get_current_user_id)):
    automation_data = data.model_dump()
    # Handle None values for nodes and edges
    if automation_data.get('nodes') is None:
        automation_data['nodes'] = []
    if automation_data.get('edges') is None:
        automation_data['edges'] = []
    a = Automation(user_id=user_id, **automation_data)
    await db.automations.insert_one(a.model_dump())
    return a.model_dump()


@api.get('/automations/{aid}')
async def get_automation(aid: str, user_id: str = Depends(get_current_user_id)):
    d = await db.automations.find_one({'id': aid, 'user_id': user_id})
    if not d:
        raise HTTPException(404, 'Not found')
    return _strip_mongo(d)


@api.patch('/automations/{aid}')
async def patch_automation(aid: str, data: AutomationPatch, user_id: str = Depends(get_current_user_id)):
    update = {k: v for k, v in data.model_dump().items() if v is not None}
    update['updated'] = datetime.utcnow()
    res = await db.automations.update_one({'id': aid, 'user_id': user_id}, {'$set': update})
    if res.matched_count == 0:
        raise HTTPException(404, 'Not found')
    d = await db.automations.find_one({'id': aid})
    return _strip_mongo(d)


@api.delete('/automations/{aid}')
async def delete_automation(aid: str, user_id: str = Depends(get_current_user_id)):
    res = await db.automations.delete_one({'id': aid, 'user_id': user_id})
    if res.deleted_count == 0:
        raise HTTPException(404, 'Not found')
    return {'ok': True}


@api.post('/automations/{aid}/duplicate')
async def duplicate_automation(aid: str, user_id: str = Depends(get_current_user_id)):
    d = await db.automations.find_one({'id': aid, 'user_id': user_id})
    if not d:
        raise HTTPException(404, 'Not found')
    import uuid
    copy = _strip_mongo({**d})
    copy['id'] = str(uuid.uuid4())
    copy['name'] = d['name'] + ' (Copy)'
    copy['status'] = 'draft'
    copy['sent'] = 0
    copy['clicks'] = 0
    copy['created'] = datetime.utcnow()
    copy['updated'] = datetime.utcnow()
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
    media_id = (data.get('media_id') or '').strip() or None
    latest = bool(data.get('latest'))
    if not media_id and not latest:
        raise HTTPException(400, 'Provide media_id or set latest=true')

    mode = (data.get('mode') or 'reply_and_dm').strip()
    if mode not in ('reply_and_dm', 'reply_only'):
        raise HTTPException(400, "mode must be 'reply_and_dm' or 'reply_only'")

    match = (data.get('match') or 'any').strip()
    keyword = (data.get('keyword') or '').strip()
    if match == 'keyword' and not keyword:
        raise HTTPException(400, 'keyword is required when match=keyword')
    if match not in ('any', 'keyword'):
        raise HTTPException(400, "match must be 'any' or 'keyword'")

    comment_reply = (data.get('comment_reply') or '').strip()
    dm_text = (data.get('dm_text') or '').strip() if mode == 'reply_and_dm' else ''
    if not comment_reply:
        raise HTTPException(400, 'comment_reply is required')
    if mode == 'reply_and_dm' and not dm_text:
        dm_text = 'شكرا'

    trigger = 'comment:latest' if latest else f'comment:{media_id}'
    preview = data.get('media_preview') or {}
    if latest:
        default_name = 'Latest post — ' + (f'keyword "{keyword}"' if match == 'keyword' else 'any comment')
    else:
        label = (preview.get('caption') or '')[:30] or (media_id[:10] if media_id else '')
        default_name = f'{label} — ' + (f'keyword "{keyword}"' if match == 'keyword' else 'any comment')
    name = (data.get('name') or default_name).strip()

    nodes = [{'id': 'n_trigger', 'type': 'trigger',
              'data': {'label': 'Comment trigger', 'trigger': trigger,
                       'match': match, 'keyword': keyword}}]
    edges = []
    prev = 'n_trigger'
    nodes.append({'id': 'n_reply', 'type': 'reply_comment',
                  'data': {'text': comment_reply}})
    edges.append({'id': 'e1', 'source': prev, 'target': 'n_reply'})
    prev = 'n_reply'
    if dm_text:
        nodes.append({'id': 'n_dm', 'type': 'message', 'data': {'text': dm_text}})
        edges.append({'id': f'e{len(edges)+1}', 'source': prev, 'target': 'n_dm'})

    doc = {
        'id': str(uuid.uuid4()),
        'user_id': user_id,
        'name': name,
        'status': 'active',
        'trigger': trigger,
        'match': match,
        'keyword': keyword,
        'mode': mode,
        'comment_reply': comment_reply,
        'dm_text': dm_text,
        'media_id': media_id,
        'latest': latest,
        'media_preview': preview,
        'nodes': nodes,
        'edges': edges,
        'sent': 0,
        'clicks': 0,
        'created': datetime.utcnow(),
        'updated': datetime.utcnow(),
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
    docs = await db.conversations.find({'user_id': user_id}).sort('created', -1).to_list(500)
    return [_strip_mongo(d) for d in docs]


@api.get('/conversations/{cid}')
async def get_conversation(cid: str, user_id: str = Depends(get_current_user_id)):
    d = await db.conversations.find_one({'id': cid, 'user_id': user_id})
    if not d:
        raise HTTPException(404, 'Not found')
    return _strip_mongo(d)


@api.post('/conversations/{cid}/messages')
async def send_message(cid: str, data: MessageIn, user_id: str = Depends(get_current_user_id)):
    """Send a message. If the conversation is tied to a real IG contact and the
    user is connected to Instagram, send via Graph API. No fake auto-reply."""
    import uuid
    conv = await db.conversations.find_one({'id': cid, 'user_id': user_id})
    if not conv:
        raise HTTPException(404, 'Not found')
    text = (data.text or '').strip()
    if not text:
        raise HTTPException(400, 'Empty message')

    msg_me = {'id': str(uuid.uuid4()), 'from': 'me', 'text': text,
              'time': datetime.utcnow().strftime('%I:%M %p')}
    new_messages = conv['messages'] + [msg_me]

    # Try to deliver to Instagram if we have a real recipient
    user_doc = await db.users.find_one({'id': user_id})
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
        {'id': cid, 'user_id': user_id},
        {'$set': {'messages': new_messages, 'lastMessage': text,
                  'time': 'now', 'unread': 0}}
    )
    # Push to WS so other tabs stay in sync
    await ws_manager.send(user_id, {'type': 'message', 'conv_id': cid, 'message': msg_me})
    return {'messages': new_messages, 'delivered': delivered, 'error': delivery_error}


# ---------------- comments ----------------
@api.get('/comments')
async def list_comments(user_id: str = Depends(get_current_user_id)):
    docs = await db.comments.find({'user_id': user_id}).sort('created', -1).to_list(500)
    return [_strip_mongo(d) for d in docs]


@api.post('/comments/{cid}/reply')
async def reply_to_comment(cid: str, data: MessageIn, user_id: str = Depends(get_current_user_id)):
    """Reply to an Instagram comment via Graph API.
    POST /{comment-id}/replies with message=..."""
    comment = await db.comments.find_one({'id': cid, 'user_id': user_id})
    if not comment:
        raise HTTPException(404, 'Comment not found')
    user_doc = await db.users.find_one({'id': user_id})
    if not user_doc or not user_doc.get('instagramConnected'):
        raise HTTPException(400, 'Instagram not connected')
    ig_comment_id = comment.get('ig_comment_id')
    if not ig_comment_id:
        raise HTTPException(400, 'Comment has no Instagram ID (seed data cannot be replied to)')
    access_token = user_doc.get('meta_access_token', '')
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
@api.get('/dashboard/stats')
async def dashboard_stats(user_id: str = Depends(get_current_user_id)):
    total_contacts = await db.contacts.count_documents({'user_id': user_id})
    active_automations = await db.automations.count_documents({'user_id': user_id, 'status': 'active'})
    autos = await db.automations.find({'user_id': user_id}).to_list(1000)
    messages_sent = sum(a.get('sent', 0) for a in autos)
    clicks = sum(a.get('clicks', 0) for a in autos)
    conv_rate = round((clicks / messages_sent * 100), 1) if messages_sent else 0.0

    # Real 7-day chart built from actual conversation messages
    from collections import OrderedDict
    today = datetime.utcnow().date()
    buckets = OrderedDict()
    for i in range(6, -1, -1):
        d = today - timedelta(days=i)
        buckets[d.isoformat()] = {
            'day': d.strftime('%a'), 'messages': 0, 'conversions': 0,
        }
    convs = await db.conversations.find({'user_id': user_id}).to_list(5000)
    for conv in convs:
        for m in conv.get('messages', []) or []:
            ts = m.get('ts') or m.get('created')
            try:
                if isinstance(ts, datetime):
                    dkey = ts.date().isoformat()
                elif isinstance(ts, str):
                    dkey = datetime.fromisoformat(ts.replace('Z', '+00:00')).date().isoformat()
                else:
                    continue
            except Exception:
                continue
            if dkey in buckets:
                buckets[dkey]['messages'] += 1
    chart = list(buckets.values())
    return {
        'total_contacts': total_contacts,
        'active_automations': active_automations,
        'messages_sent': messages_sent,
        'conversion_rate': conv_rate,
        'weekly_chart': chart,
    }


# ---------------- Instagram OAuth (Business Login) ----------------
# Uses Instagram API with Business Login flow — required for the
# /{ig_user_id}/subscribed_apps endpoint to accept our access token.
# Facebook Login for Business (Pages) returns a Page token that the
# new IG Graph API rejects with "Application does not have the capability".
IG_SCOPES = (
    'instagram_business_basic,'
    'instagram_business_manage_messages,'
    'instagram_business_manage_comments,'
    'instagram_business_content_publish'
)
VALID_IG_ACCOUNT_TYPES = {'BUSINESS', 'CREATOR', 'MEDIA_CREATOR'}


def _token_prefix(token: str) -> Optional[str]:
    return token[:6] if token else None


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


async def _verify_instagram_token(c: httpx.AsyncClient, token: str) -> Dict[str, Any]:
    fields = 'id,user_id,username,account_type'
    r = await c.get(
        'https://graph.instagram.com/me',
        params={'fields': fields, 'access_token': token},
    )
    try:
        body = r.json()
    except Exception:
        body = {'raw': r.text[:300]}
    if r.status_code != 200:
        return {
            'ok': False,
            'status': r.status_code,
            'bodyKeys': sorted(body.keys()) if isinstance(body, dict) else [],
            'error': _safe_graph_error(body) or {'message': 'graph_me_failed'},
            'blocker': 'token_cannot_call_graph_me',
            'fix': 'Disconnect and reconnect Instagram, then verify /me before saving the token.',
        }

    canonical_id = str(body.get('user_id') or body.get('id') or '')
    username = body.get('username') or ''
    account_type = body.get('account_type') or ''
    if not canonical_id:
        return {
            'ok': False,
            'status': r.status_code,
            'bodyKeys': sorted(body.keys()),
            'error': {'message': 'Graph /me did not return id or user_id'},
            'blocker': 'graph_me_missing_canonical_id',
        }
    if not username:
        return {
            'ok': False,
            'status': r.status_code,
            'bodyKeys': sorted(body.keys()),
            'error': {'message': 'Graph /me did not return username'},
            'blocker': 'graph_me_missing_username',
        }
    if account_type not in VALID_IG_ACCOUNT_TYPES:
        return {
            'ok': False,
            'status': r.status_code,
            'bodyKeys': sorted(body.keys()),
            'error': {'message': f'Unsupported account_type: {account_type or "missing"}'},
            'blocker': 'instagram_account_type_not_supported',
        }
    return {
        'ok': True,
        'status': r.status_code,
        'bodyKeys': sorted(body.keys()),
        'canonicalIgId': canonical_id,
        'graphMeId': str(body.get('id') or ''),
        'graphMeUserId': str(body.get('user_id') or ''),
        'username': username,
        'accountType': account_type,
    }


@api.get('/instagram/auth-url')
async def instagram_auth_url(user_id: str = Depends(get_current_user_id)):
    if not IG_APP_ID or not IG_APP_SECRET:
        raise HTTPException(503, 'IG_APP_ID and IG_APP_SECRET are not configured. Set them in .env')
    redirect_uri = f"{BACKEND_PUBLIC_URL}/api/instagram/callback"
    params = {
        'enable_fb_login': '0',
        'force_authentication': '1',
        'client_id': IG_APP_ID,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': IG_SCOPES,
        'state': user_id,
    }
    url = f"https://www.instagram.com/oauth/authorize?{urlencode(params)}"
    return {
        'url': url,
        'configured': True,
        'redirect_uri': redirect_uri,
        'authorizeUrlDebug': {
            'host': 'www.instagram.com',
            'clientIdLast4': IG_APP_ID[-4:] if IG_APP_ID else None,
            'redirect_uri': redirect_uri,
            'scope': IG_SCOPES,
            'response_type': 'code',
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
        'longLivedExchangeEndpoint': 'https://graph.instagram.com/access_token',
        'longLivedExchangeStatus': None,
        'longLivedExchangeResponseKeys': [],
        'finalTokenStoredSource': None,
        'finalTokenLength': None,
        'finalTokenPrefix': None,
        'finalIgUserIdStoredSource': None,
        'verification': None,
        'debugToken': None,
        'createdAt': datetime.utcnow(),
    }

    async def _store_oauth_failure(uid: Optional[str], blocker: str, detail: Any = None):
        if not uid:
            return
        await db.users.update_one(
            {'id': uid},
            {
                '$set': {
                    'instagramConnected': False,
                    'instagram_connection_valid': False,
                    'instagram_connection_blocker': blocker,
                    'ig_oauth_last_audit': _redact_secrets({**audit, 'failureDetail': detail}),
                    'updated': datetime.utcnow(),
                },
                '$unset': {
                    'meta_access_token': '',
                    'ig_user_id': '',
                    'instagramHandle': '',
                    'instagram_account_type': '',
                    'instagram_graph_me_id': '',
                    'instagram_graph_me_user_id': '',
                },
            },
        )

    if error:
        logger.warning('IG OAuth denied: %s — %s', error, error_description)
        return RedirectResponse(f"{FRONTEND_URL}/app/settings?ig=error&reason={error}")
    if not state:
        return RedirectResponse(f"{FRONTEND_URL}/app/settings?ig=error&reason=missing_state")
    if not code:
        await _store_oauth_failure(state, 'oauth_code_missing')
        return RedirectResponse(f"{FRONTEND_URL}/app/settings?ig=error&reason=missing_code")
    user_id = state
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
                await _store_oauth_failure(user_id, 'token_exchange_failed', safe)
                return RedirectResponse(f"{FRONTEND_URL}/app/settings?ig=error&reason=token_exchange_failed")
            # 2) Exchange short-lived for long-lived IG user token (60 days)
            audit['debugToken'] = await _debug_token_with_ig_app(token)
            audit['longLivedExchangeAttempted'] = True
            ll = await c.get(
                'https://graph.instagram.com/access_token',
                params={
                    'grant_type': 'ig_exchange_token',
                    'client_secret': IG_APP_SECRET,
                    'access_token': token,
                },
            )
            ll_data = ll.json() if ll.status_code == 200 else {}
            audit['longLivedExchangeStatus'] = ll.status_code
            audit['longLivedExchangeResponseKeys'] = sorted(ll_data.keys()) if isinstance(ll_data, dict) else []
            long_token = ll_data.get('access_token') or None
            final_token = long_token or token
            audit['finalTokenStoredSource'] = 'long_lived' if long_token else 'short_lived'
            audit['finalTokenLength'] = len(final_token)
            audit['finalTokenPrefix'] = _token_prefix(final_token)
            # 3) Verify /me before saving the token or connected state.
            verification = await _verify_instagram_token(c, final_token)
            audit['verification'] = verification
            if not verification.get('ok'):
                graph_error = verification.get('error') or {}
                logger.error('IG /me verification failed before save: %s',
                             _redact_secrets(graph_error))
                await _store_oauth_failure(
                    user_id,
                    verification.get('blocker') or 'token_cannot_call_graph_me',
                    graph_error,
                )
                return RedirectResponse(
                    f"{FRONTEND_URL}/app/settings?ig=error&reason=token_cannot_call_graph_me"
                )
            ig_user_id = verification['canonicalIgId']
            handle = '@' + verification['username']
            followers = 0
            audit['finalIgUserIdStoredSource'] = (
                'graph_me_user_id' if verification.get('graphMeUserId') else 'graph_me_id'
            )
            # 4) Subscribe the IG user to webhook fields — this is THE key call
            #    that the Facebook-Login flow couldn't perform. With an IG user
            #    token it's accepted.
            ig_sub_status = None
            ig_sub_body = None
            if ig_user_id:
                try:
                    ig_sub = await c.post(
                        f'https://graph.instagram.com/{ig_user_id}/subscribed_apps',
                        params={
                            'access_token': final_token,
                            'subscribed_fields': (
                                'comments,messages,messaging_postbacks,'
                                'messaging_seen,message_reactions,live_comments'
                            ),
                        },
                    )
                    ig_sub_status = ig_sub.status_code
                    ig_sub_body = ig_sub.text
                    logger.info('IG-user subscribe status=%s body=%s',
                                ig_sub.status_code, ig_sub.text[:300])
                except Exception as e:
                    logger.warning('IG-user subscribe failed: %s', e)
            await db.users.update_one(
                {'id': user_id},
                {'$set': {
                    'instagramConnected': True,
                    'instagram_connection_valid': True,
                    'instagram_connection_blocker': None,
                    'instagramHandle': handle,
                    'instagramFollowers': followers,
                    'ig_user_id': ig_user_id,
                    'meta_access_token': final_token,
                    'ig_auth_kind': 'instagram_business_login',
                    'instagram_account_type': verification.get('accountType'),
                    'instagram_graph_me_id': verification.get('graphMeId'),
                    'instagram_graph_me_user_id': verification.get('graphMeUserId'),
                    'ig_subscribe_status': ig_sub_status,
                    'ig_subscribe_body': (ig_sub_body or '')[:500],
                    'ig_oauth_last_audit': _redact_secrets(audit),
                }},
            )
            logger.info('IG connected (Business Login) for user %s: %s (ig_id=%s)',
                        user_id, handle, ig_user_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception('IG callback failed')
        from fastapi.responses import RedirectResponse
        await _store_oauth_failure(user_id, 'server_error', str(e)[:200])
        return RedirectResponse(f"{FRONTEND_URL}/app/settings?ig=error&reason=server_error")
    from fastapi.responses import RedirectResponse
    return RedirectResponse(f"{FRONTEND_URL}/app/settings?ig=connected")


@api.get('/instagram/status')
async def instagram_status(user_id: str = Depends(get_current_user_id)):
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    connected = bool(u.get('instagramConnected') and u.get('instagram_connection_valid'))
    return {
        'connected': connected,
        'handle': u.get('instagramHandle'),
        'followers': u.get('instagramFollowers', 0),
        'ig_user_id': u.get('ig_user_id'),
        'connectionValid': bool(u.get('instagram_connection_valid')),
        'connectionBlocker': u.get('instagram_connection_blocker'),
        'accountType': u.get('instagram_account_type'),
        'meta_configured': bool(META_APP_ID and META_APP_SECRET),
    }


@api.post('/instagram/subscribe-webhook')
async def instagram_subscribe_webhook(user_id: str = Depends(get_current_user_id)):
    """Force-subscribe the user's connected IG user to webhook fields via
    Instagram API (graph.instagram.com). Requires an IG user access token
    obtained through Instagram Business Login."""
    u = await db.users.find_one({'id': user_id})
    if not u or not u.get('instagramConnected'):
        raise HTTPException(400, 'Instagram not connected')
    token = u.get('meta_access_token', '')
    ig_user_id = u.get('ig_user_id', '')
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
    if not u or not u.get('instagramConnected'):
        raise HTTPException(400, 'Instagram not connected')
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
        # Persist the fresh page id + page token on the user
        await db.users.update_one(
            {'id': user_id},
            {'$set': {'fb_page_id': page_id, 'meta_access_token': page_token}},
        )
        return {'ok': ok, 'status': sub.status_code, 'body': body,
                'page_id': page_id, 'ig_user_id': ig_user_id,
                'ig_subscribe_status': ig_sub_status, 'ig_subscribe_body': ig_sub_body,
                'subscribed_apps': verify.json()}


@api.get('/instagram/force-resubscribe')
async def instagram_force_resubscribe(email: str, key: str, fields: str = ''):
    """Admin tool: DELETE then POST the user's webhook subscription to force
    Meta to re-establish delivery. Accepts optional `fields` override; by
    default subscribes to `comments,messages,messaging_postbacks,messaging_seen,message_reactions,live_comments`.
    Protected by META_APP_SECRET as admin key."""
    if not META_APP_SECRET or key != META_APP_SECRET:
        raise HTTPException(403, 'bad key')
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
async def instagram_debug_dump(email: str, key: str, media_id: str = ''):
    """FULL diagnostic dump — returns everything about a user's IG link and
    live Meta subscription state. Protected by META_APP_SECRET as admin key.

    If `media_id` is provided, also fetches that post's comments via Graph API
    to verify whether IG actually recorded a comment (and thus whether the
    missing webhook is an IG-side spam filter or a subscription-wiring issue)."""
    if not META_APP_SECRET or key != META_APP_SECRET:
        raise HTTPException(403, 'bad key')
    u = await db.users.find_one({'email': email.lower()})
    if not u:
        u = await db.users.find_one({'email': email})
    if not u:
        all_users = await db.users.find({}, {'email': 1, 'username': 1, 'instagramConnected': 1}).to_list(50)
        for x in all_users:
            x.pop('_id', None)
        return {'error': 'user not found', 'email': email, 'available_users': all_users}
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
    u = await db.users.find_one({'id': user_id})
    if not u or not u.get('instagramConnected'):
        raise HTTPException(400, 'Instagram not connected')
    token = u.get('meta_access_token', '')
    ig_id = str(u.get('ig_user_id') or '')
    if not token:
        raise HTTPException(400, 'Missing access token')
    fields = 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,comments_count'
    lim = max(1, min(limit, 50))
    primary_url = 'https://graph.instagram.com/me/media'
    primary_label = '/me/media'

    me_id_for_debug: Optional[str] = None
    primary_error: Optional[Dict[str, Any]] = None
    fallback_error: Optional[Dict[str, Any]] = None

    try:
        async with httpx.AsyncClient(timeout=20) as c:
            # Best-effort /me lookup for diagnostics shown in the wizard.
            try:
                mer = await c.get(
                    'https://graph.instagram.com/me',
                    params={'access_token': token, 'fields': 'user_id,id,username'},
                )
                if mer.status_code == 200:
                    md = mer.json() or {}
                    me_id_for_debug = str(md.get('user_id') or md.get('id') or '') or None
            except Exception:
                pass

            r = await c.get(primary_url, params={
                'access_token': token, 'fields': fields, 'limit': lim,
            })
            if r.status_code == 200:
                items = (r.json() or {}).get('data') or []
                return {
                    'ok': True,
                    'endpointUsed': primary_label,
                    'media': items,
                    'items': items,  # backwards-compat
                    'count': len(items),
                    'warning': None if items else 'No Instagram media returned from /me/media',
                    'fallbackError': None,
                    'graphMeId': me_id_for_debug,
                    'dbIgUserId': ig_id or None,
                    'idMatch': (bool(me_id_for_debug) and me_id_for_debug == ig_id) if ig_id else None,
                }

            primary_error = {'status': r.status_code, 'body': r.text[:500]}
            logger.error('IG /me/media failed %s: %s', r.status_code, r.text[:300])

            # Optional fallback — only attempted, never surfaced as the
            # primary error if /me/media is the documented path.
            if ig_id:
                try:
                    fr = await c.get(
                        f'https://graph.instagram.com/{ig_id}/media',
                        params={'access_token': token, 'fields': fields, 'limit': lim},
                    )
                    if fr.status_code == 200:
                        items = (fr.json() or {}).get('data') or []
                        return {
                            'ok': True,
                            'endpointUsed': '/{ig_user_id}/media (fallback)',
                            'media': items,
                            'items': items,
                            'count': len(items),
                            'warning': '/me/media failed; succeeded via /{ig_user_id}/media fallback',
                            'fallbackError': None,
                            'primaryError': primary_error,
                            'graphMeId': me_id_for_debug,
                            'dbIgUserId': ig_id or None,
                            'idMatch': (bool(me_id_for_debug) and me_id_for_debug == ig_id),
                        }
                    fallback_error = {'status': fr.status_code, 'body': fr.text[:500]}
                except Exception as e:
                    fallback_error = {'exception': str(e)}
    except Exception as e:
        logger.exception('IG media fetch failed')
        return {
            'ok': False,
            'endpointUsed': primary_label,
            'media': [],
            'items': [],
            'count': 0,
            'error': {'status': 0, 'body': str(e)},
            'fallbackError': fallback_error,
            'graphMeId': me_id_for_debug,
            'dbIgUserId': ig_id or None,
            'idMatch': None,
        }

    return {
        'ok': False,
        'endpointUsed': primary_label,
        'media': [],
        'items': [],
        'count': 0,
        'error': primary_error or {'status': 0, 'body': 'unknown'},
        'fallbackError': fallback_error,
        'graphMeId': me_id_for_debug,
        'dbIgUserId': ig_id or None,
        'idMatch': (bool(me_id_for_debug) and me_id_for_debug == ig_id) if ig_id else None,
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
    out['connected'] = bool(u.get('instagramConnected'))
    db_ig_id = str(u.get('ig_user_id') or '')
    token = u.get('meta_access_token', '') or ''
    out['dbIgUserId'] = db_ig_id or None
    out['tokenExists'] = bool(token)
    out['tokenLength'] = len(token)
    out['authKind'] = u.get('ig_auth_kind')
    if not out['connected']:
        out['blocker'] = 'instagram_not_connected'
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


@api.post('/instagram/disconnect')
async def instagram_disconnect(user_id: str = Depends(get_current_user_id)):
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


@api.post('/instagram/webhook')
async def instagram_webhook(request: Request):
    payload = await request.json()
    logger.info('IG webhook: %s', payload)
    # Store raw payload for debugging — keep only most recent 50
    try:
        await db.webhook_log.insert_one({
            'received': datetime.utcnow(),
            'payload': payload,
            'object': payload.get('object'),
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

    # Dedupe
    existing = await db.comments.find_one({
        'user_id': user_id, 'ig_comment_id': ig_comment_id
    })
    if existing:
        logger.info('comment_already_processed ig_comment_id=%s user=%s',
                    ig_comment_id, user_doc.get('email'))
        return {'processed': False, 'already_processed': True, 'matched': False,
                'action_status': 'skipped', 'reason': 'duplicate'}

    commenter_username = comment_data.get('commenter_username') or f'ig_{commenter_id[:8]}'
    ts_raw = comment_data.get('timestamp')
    now = datetime.utcnow()

    # Match automations to determine rule_id BEFORE insert
    automations = await db.automations.find(
        {'user_id': user_id, 'status': 'active'}
    ).to_list(100)
    latest_media_id = None
    matched_rule = None
    for auto in automations:
        raw_trigger = auto.get('trigger') or ''
        trigger = raw_trigger.lower()
        fire = False
        if trigger.startswith('keyword:'):
            keyword = trigger.split(':', 1)[1].strip()
            if keyword and keyword.lower() in comment_text.lower():
                fire = True
        elif trigger.startswith('comment:'):
            target = raw_trigger.split(':', 1)[1].strip()
            media_hit = False
            if target.lower() == 'latest':
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
                match_mode = (auto.get('match') or 'any').lower()
                kw = (auto.get('keyword') or '').strip()
                if match_mode == 'keyword' and kw:
                    if kw.lower() in comment_text.lower():
                        fire = True
                else:
                    fire = True
        if fire:
            matched_rule = auto
            break

    rule_id = matched_rule.get('id') if matched_rule else None
    matched = bool(matched_rule)
    if matched:
        logger.info('rule_matched ig_comment_id=%s rule_id=%s user=%s',
                    ig_comment_id, rule_id, user_doc.get('email'))
    else:
        logger.info('rule_not_matched ig_comment_id=%s user=%s',
                    ig_comment_id, user_doc.get('email'))

    doc = {
        'id': str(_uuid.uuid4()),
        'user_id': user_id,
        'ig_comment_id': ig_comment_id,
        'media_id': media_id,
        'commenter_id': commenter_id,
        'commenter_username': commenter_username,
        'text': comment_text,
        'replied': False,
        'source': source,                # 'webhook' or 'polling'
        'rule_id': rule_id,
        'matched': matched,
        'action_status': 'pending' if matched else 'skipped',
        'error': None,
        'timestamp': ts_raw,
        'processed_at': now,
        'created': now,
    }
    try:
        await db.comments.insert_one(doc)
    except Exception as e:
        # Race against unique index — another worker inserted first.
        logger.info('comment_insert_race ig_comment_id=%s err=%s', ig_comment_id, e)
        return {'processed': False, 'already_processed': True, 'matched': False,
                'action_status': 'skipped', 'reason': 'race'}

    await ws_manager.send(user_id, {'type': 'comment', 'comment': _strip_mongo({**doc})})

    action_status = 'skipped'
    if matched:
        try:
            logger.info('action_execution_started ig_comment_id=%s rule_id=%s',
                        ig_comment_id, rule_id)
            asyncio.create_task(_run_and_record_action(
                user_doc, matched_rule, commenter_id, comment_text,
                comment_doc_id=doc['id'], ig_comment_id=ig_comment_id,
            ))
            action_status = 'pending'
        except Exception as e:
            logger.exception('action_execution_failed ig_comment_id=%s err=%s',
                             ig_comment_id, e)
            await db.comments.update_one(
                {'id': doc['id']},
                {'$set': {'action_status': 'failed', 'error': str(e)[:500]}}
            )
            action_status = 'failed'

    return {'processed': True, 'matched': matched, 'action_status': action_status,
            'rule_id': rule_id, 'comment_doc_id': doc['id']}


async def _run_and_record_action(user_doc, automation, commenter_id, comment_text,
                                 comment_doc_id: str, ig_comment_id: str):
    """Wrap execute_flow so we record success/failure on the comment doc."""
    try:
        await execute_flow(
            user_doc, automation, commenter_id, comment_text,
            comment_context={'ig_comment_id': ig_comment_id, 'comment_doc_id': comment_doc_id}
        )
        await db.comments.update_one(
            {'id': comment_doc_id},
            {'$set': {'action_status': 'success'}}
        )
        logger.info('action_execution_success ig_comment_id=%s rule_id=%s',
                    ig_comment_id, automation.get('id'))
    except Exception as e:
        await db.comments.update_one(
            {'id': comment_doc_id},
            {'$set': {'action_status': 'failed', 'error': str(e)[:500]}}
        )
        logger.exception('action_execution_failed ig_comment_id=%s rule_id=%s err=%s',
                         ig_comment_id, automation.get('id'), e)


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
        message_id = cls['message_id']
        text = cls['text']
        is_echo = cls['is_echo']
        ts = cls['timestamp']
        event_kind = cls['kind']
        message_keys = cls['message_keys']
        item_keys = cls['item_keys']
    else:
        # Legacy flattened input
        sender_id = event.get('sender_id')
        message_id = event.get('message_id')
        text = event.get('text') or ''
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
            'ig_user_id': ig_account_id,
            'sender_id': sender_id,
            'recipient_id': locals().get('recipient_id') if 'recipient_id' in locals() else None,
            'message_id': message_id,
            'dedup_key': dedup_key,
            'event_kind': event_kind,
            'message_keys': message_keys,
            'item_keys': item_keys,
            'incoming_text': text if text else None,
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
    existing = await db.dm_logs.find_one({'user_id': user_id, 'dedup_key': dedup_key})
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
    if not text:
        await _persist(_mk_log('skipped', skip_reason='missing_text'))
        return {'processed': False, 'status': 'skipped', 'reason': 'missing_text',
                'event_kind': event_kind}

    logger.info('dm_sender_extracted sender=%s', sender_id)
    logger.info('dm_text_extracted len=%s preview=%r', len(text), text[:80])

    # Load active DM rules for this user
    rules = await db.dm_rules.find(
        {'user_id': user_id, 'is_active': True}
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
            user_doc = await db.users.find_one({'$or': [
                {'ig_user_id': ig_account_id},
                {'fb_page_id': ig_account_id},
            ]})
            mapping_via = 'entry.id' if user_doc else None
            # Fallback: if entry.id doesn't match any user, try the recipient.id
            # of any messaging-item in this entry. Read/delivery events from a
            # business account use sender=business, recipient=user — but for
            # incoming text DMs, recipient.id == business IG account.
            if not user_doc:
                for ev in entry.get('messaging', []) or []:
                    rid = (ev.get('recipient') or {}).get('id')
                    if rid:
                        user_doc = await db.users.find_one({'$or': [
                            {'ig_user_id': rid}, {'fb_page_id': rid},
                        ]})
                        if user_doc:
                            mapping_via = 'recipient.id'
                            ig_account_id = rid
                            break
            # Last-resort fallback for single-tenant deployments: if exactly
            # one user has an IG account connected, attribute the event to it.
            if not user_doc:
                connected_users = await db.users.find(
                    {'instagramConnected': True, 'ig_user_id': {'$ne': None, '$ne': ''}}
                ).limit(2).to_list(2)
                if len(connected_users) == 1:
                    user_doc = connected_users[0]
                    mapping_via = 'single_tenant_fallback'
                    ig_account_id = user_doc.get('ig_user_id') or ig_account_id
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
                conv = await db.conversations.find_one({'user_id': user_id, 'contact.ig_id': sender_id})
                if not conv:
                    # Create new conversation for this contact
                    conv_id = str(_uuid.uuid4())
                    conv = {
                        'id': conv_id, 'user_id': user_id,
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
                    {'user_id': user_id, 'status': 'active'}
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
    for a in automations:
        raw_trigger = a.get('trigger') or ''
        trigger = raw_trigger.lower()
        if trigger.startswith('comment:'):
            t = raw_trigger.split(':', 1)[1].strip()
            if t.lower() == 'latest':
                needs_latest = True
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
async def instagram_poll_now(email: str, key: str):
    """Manually trigger a single poll for one user (debug endpoint).
    Protected by META_APP_SECRET like the other debug endpoints."""
    if not META_APP_SECRET or key != META_APP_SECRET:
        raise HTTPException(403, 'bad key')
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
        for k in ('created', 'processed_at'):
            if isinstance(d.get(k), datetime):
                d[k] = d[k].isoformat()
        out.append({
            'id': d.get('id'),
            'igCommentId': d.get('ig_comment_id'),
            'mediaId': d.get('media_id'),
            'commenterUsername': d.get('commenter_username'),
            'text': d.get('text'),
            'source': d.get('source'),
            'ruleId': d.get('rule_id'),
            'matched': bool(d.get('matched')),
            'actionStatus': d.get('action_status'),
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
    rows = await db.dm_rules.find({'user_id': user_id}).sort('created_at', -1).to_list(500)
    return {'items': [_dm_rule_out(r) for r in rows], 'count': len(rows)}


@api.post('/instagram/dm/rules')
async def create_dm_rule(data: DmRuleIn, user_id: str = Depends(get_current_user_id)):
    import uuid as _uuid
    mode = (data.matchMode or 'contains').lower()
    if mode not in _DM_VALID_MODES:
        raise HTTPException(400, f'matchMode must be one of {sorted(_DM_VALID_MODES)}')
    if not data.name.strip() or not data.keyword.strip() or not data.replyText.strip():
        raise HTTPException(400, 'name, keyword and replyText are required')
    u = await db.users.find_one({'id': user_id})
    now = datetime.utcnow()
    doc = {
        'id': str(_uuid.uuid4()),
        'user_id': user_id,
        'ig_user_id': u.get('ig_user_id') if u else None,
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
    res = await db.dm_rules.update_one({'id': rid, 'user_id': user_id}, {'$set': update})
    if res.matched_count == 0:
        raise HTTPException(404, 'rule not found')
    doc = await db.dm_rules.find_one({'id': rid, 'user_id': user_id})
    return _dm_rule_out(doc)


@api.delete('/instagram/dm/rules/{rid}')
async def delete_dm_rule(rid: str, user_id: str = Depends(get_current_user_id)):
    res = await db.dm_rules.delete_one({'id': rid, 'user_id': user_id})
    if res.deleted_count == 0:
        raise HTTPException(404, 'rule not found')
    return {'ok': True}


@api.post('/instagram/dm/test-rule')
async def test_dm_rule(data: DmTestIn, user_id: str = Depends(get_current_user_id)):
    """Match `text` against the user's active rules without sending anything."""
    rules = await db.dm_rules.find(
        {'user_id': user_id, 'is_active': True}
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
    rows = await db.dm_logs.find({'user_id': user_id}).sort('created', -1).limit(limit).to_list(limit)
    out = []
    for d in rows:
        d.pop('_id', None)
        created = d.get('created')
        out.append({
            'id': d.get('id'),
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
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'user not found')
    ig_user_id = u.get('ig_user_id') or ''
    token = u.get('meta_access_token') or ''
    connected = bool(u.get('instagramConnected') and token and ig_user_id)

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

    active_rules = await db.dm_rules.count_documents({'user_id': user_id, 'is_active': True})

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

    last_log = await db.dm_logs.find({'user_id': user_id}).sort('created', -1).limit(1).to_list(1)
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
            'signatureValidationEnabled': False,
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
        'signatureValidationEnabled': False,  # not currently implemented
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
        await db.comments.create_index(
            [('user_id', 1), ('ig_comment_id', 1)],
            unique=True, sparse=True, name='uniq_user_ig_comment'
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
        await db.dm_logs.create_index(
            [('user_id', 1), ('dedup_key', 1)],
            unique=True, sparse=True, name='uniq_user_dm_dedup_key',
        )
    except Exception as e:
        logger.warning('dm_logs dedup_key index create: %s', e)
    try:
        await db.dm_rules.create_index([('user_id', 1), ('is_active', 1)],
                                       name='dm_rules_user_active')
    except Exception as e:
        logger.warning('dm_rules index create: %s', e)
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
