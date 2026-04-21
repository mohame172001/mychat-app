import os
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict

from fastapi import FastAPI, APIRouter, HTTPException, Depends, Query, Request, WebSocket, WebSocketDisconnect
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
)
from auth_utils import hash_password, verify_password, create_token, get_current_user_id, decode_token

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

MONGO_URL = os.environ['MONGO_URL']
DB_NAME = os.environ.get('DB_NAME', 'mychat')
META_APP_ID = os.environ.get('META_APP_ID', '')
META_APP_SECRET = os.environ.get('META_APP_SECRET', '')
META_VERIFY_TOKEN = os.environ.get('META_VERIFY_TOKEN', 'mychat_verify_123')
FRONTEND_URL = os.environ.get('FRONTEND_URL', 'http://localhost:3000')
BACKEND_PUBLIC_URL = os.environ.get('BACKEND_PUBLIC_URL', 'http://localhost:8001')

client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]

app = FastAPI(title='mychat API')
api = APIRouter(prefix='/api')

logger = logging.getLogger('mychat')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


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
    url = f'https://graph.facebook.com/v21.0/{ig_user_id}/messages'
    payload = {
        'recipient': {'id': recipient_ig_id},
        'message': {'text': text},
        'messaging_type': 'RESPONSE',
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


# ---------------- Automation engine ----------------
async def execute_flow(user: dict, automation: dict, sender_ig_id: str, trigger_text: str = ''):
    """Walk the flow graph and execute each node in order."""
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
    return UserPublic(
        id=u['id'], username=u['username'], name=u['name'], email=u['email'],
        avatar=u.get('avatar') or f"https://i.pravatar.cc/150?u={u['username']}",
        instagramConnected=bool(u.get('instagramConnected', False)),
        instagramHandle=u.get('instagramHandle'),
    )


async def _seed_user(user_id: str):
    """Seed starter data so dashboard feels alive after signup."""
    contacts = [
        {'id': 'c1', 'user_id': user_id, 'name': 'Jessica Martinez', 'username': '@jessicam',
         'avatar': 'https://i.pravatar.cc/150?img=1', 'tags': ['Customer', 'VIP'], 'subscribed': True,
         'lastActive': datetime.utcnow(), 'created': datetime.utcnow()},
        {'id': 'c2', 'user_id': user_id, 'name': 'Michael Brown', 'username': '@mikebrown',
         'avatar': 'https://i.pravatar.cc/150?img=3', 'tags': ['Lead'], 'subscribed': True,
         'lastActive': datetime.utcnow(), 'created': datetime.utcnow()},
        {'id': 'c3', 'user_id': user_id, 'name': 'Olivia Wilson', 'username': '@oliviaw',
         'avatar': 'https://i.pravatar.cc/150?img=5', 'tags': ['Customer'], 'subscribed': True,
         'lastActive': datetime.utcnow(), 'created': datetime.utcnow()},
        {'id': 'c4', 'user_id': user_id, 'name': 'Daniel Garcia', 'username': '@dangarcia',
         'avatar': 'https://i.pravatar.cc/150?img=7', 'tags': ['Prospect'], 'subscribed': False,
         'lastActive': datetime.utcnow(), 'created': datetime.utcnow()},
    ]
    await db.contacts.insert_many(contacts)

    automations = [
        {'id': 'a1', 'user_id': user_id, 'name': 'Welcome New Followers', 'trigger': 'New Follower',
         'status': 'active', 'sent': 2847, 'clicks': 892, 'nodes': [], 'edges': [],
         'updated': datetime.utcnow(), 'created': datetime.utcnow()},
        {'id': 'a2', 'user_id': user_id, 'name': 'Comment to DM - Product Launch', 'trigger': 'Keyword: LAUNCH',
         'status': 'active', 'sent': 5621, 'clicks': 1834, 'nodes': [], 'edges': [],
         'updated': datetime.utcnow(), 'created': datetime.utcnow()},
        {'id': 'a3', 'user_id': user_id, 'name': 'Story Reply Auto-Response', 'trigger': 'Story Reply',
         'status': 'active', 'sent': 1203, 'clicks': 421, 'nodes': [], 'edges': [],
         'updated': datetime.utcnow(), 'created': datetime.utcnow()},
    ]
    await db.automations.insert_many(automations)

    broadcasts = [
        {'id': 'b1', 'user_id': user_id, 'name': 'Black Friday Sale', 'message': 'Sale live now!',
         'status': 'sent', 'audience': 8421, 'openRate': '68%', 'clickRate': '32%', 'date': 'Nov 24, 2025',
         'created': datetime.utcnow()},
        {'id': 'b2', 'user_id': user_id, 'name': 'Weekly Newsletter', 'message': 'Tips this week',
         'status': 'sent', 'audience': 7832, 'openRate': '54%', 'clickRate': '21%', 'date': 'Nov 18, 2025',
         'created': datetime.utcnow()},
    ]
    await db.broadcasts.insert_many(broadcasts)

    conversations = [
        {'id': 'conv1', 'user_id': user_id,
         'contact': {'name': 'Jessica Martinez', 'username': '@jessicam', 'avatar': 'https://i.pravatar.cc/150?img=1'},
         'messages': [
             {'id': 'm1', 'from': 'contact', 'text': 'Hey! Saw your latest post', 'time': '10:42 AM'},
             {'id': 'm2', 'from': 'contact', 'text': 'Is this still in stock?', 'time': '10:43 AM'},
             {'id': 'm3', 'from': 'me', 'text': 'Hi Jessica! Yes it is available in all sizes.', 'time': '10:45 AM'},
         ],
         'lastMessage': 'Is this still in stock?', 'time': '2m', 'unread': 2,
         'created': datetime.utcnow()},
        {'id': 'conv2', 'user_id': user_id,
         'contact': {'name': 'Michael Brown', 'username': '@mikebrown', 'avatar': 'https://i.pravatar.cc/150?img=3'},
         'messages': [
             {'id': 'm1', 'from': 'contact', 'text': 'LAUNCH', 'time': '9:30 AM'},
             {'id': 'm2', 'from': 'me', 'text': 'Welcome! Here is your 20% off code: LAUNCH20', 'time': '9:30 AM'},
             {'id': 'm3', 'from': 'contact', 'text': 'Thanks, got the discount code!', 'time': '9:31 AM'},
         ],
         'lastMessage': 'Thanks, got the discount code!', 'time': '15m', 'unread': 0,
         'created': datetime.utcnow()},
    ]
    await db.conversations.insert_many(conversations)


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
            sent += 1  # simulate send when not connected to IG

    total = sent + failed
    open_rate = f'{round(sent / total * 68)}%' if total else '0%'
    click_rate = f'{round(sent / total * 28)}%' if total else '0%'
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
    import uuid
    conv = await db.conversations.find_one({'id': cid, 'user_id': user_id})
    if not conv:
        raise HTTPException(404, 'Not found')
    msg_me = {'id': str(uuid.uuid4()), 'from': 'me', 'text': data.text, 'time': 'now'}
    reply = {'id': str(uuid.uuid4()), 'from': 'contact', 'text': 'Got it, thanks! 🙏', 'time': 'now'}
    new_messages = conv['messages'] + [msg_me, reply]
    await db.conversations.update_one(
        {'id': cid, 'user_id': user_id},
        {'$set': {'messages': new_messages, 'lastMessage': reply['text'], 'time': 'now', 'unread': 0}}
    )
    return {'messages': new_messages}


# ---------------- dashboard ----------------
@api.get('/dashboard/stats')
async def dashboard_stats(user_id: str = Depends(get_current_user_id)):
    total_contacts = await db.contacts.count_documents({'user_id': user_id})
    active_automations = await db.automations.count_documents({'user_id': user_id, 'status': 'active'})
    autos = await db.automations.find({'user_id': user_id}).to_list(1000)
    messages_sent = sum(a.get('sent', 0) for a in autos)
    clicks = sum(a.get('clicks', 0) for a in autos)
    conv_rate = round((clicks / messages_sent * 100), 1) if messages_sent else 0.0

    # static sample weekly chart (derived)
    chart = [
        {'day': 'Mon', 'messages': 3200, 'conversions': 980},
        {'day': 'Tue', 'messages': 4100, 'conversions': 1240},
        {'day': 'Wed', 'messages': 3800, 'conversions': 1180},
        {'day': 'Thu', 'messages': 5200, 'conversions': 1680},
        {'day': 'Fri', 'messages': 6100, 'conversions': 2020},
        {'day': 'Sat', 'messages': 4800, 'conversions': 1520},
        {'day': 'Sun', 'messages': 4300, 'conversions': 1380},
    ]
    return {
        'total_contacts': total_contacts,
        'active_automations': active_automations,
        'messages_sent': messages_sent,
        'conversion_rate': conv_rate,
        'weekly_chart': chart,
    }


# ---------------- Instagram OAuth ----------------
IG_SCOPES = 'instagram_basic,instagram_manage_messages,instagram_manage_comments,pages_show_list,pages_manage_metadata,business_management'


@api.get('/instagram/auth-url')
async def instagram_auth_url(user_id: str = Depends(get_current_user_id)):
    if not META_APP_ID or not META_APP_SECRET:
        raise HTTPException(503, 'META_APP_ID and META_APP_SECRET are not configured. Set them in .env')
    redirect_uri = f"{BACKEND_PUBLIC_URL}/api/instagram/callback"
    url = (
        f"https://www.facebook.com/v21.0/dialog/oauth?"
        f"client_id={META_APP_ID}&redirect_uri={redirect_uri}&state={user_id}&scope={IG_SCOPES}"
    )
    return {'url': url, 'configured': True, 'redirect_uri': redirect_uri}


@api.get('/instagram/callback')
async def instagram_callback(code: str = Query(...), state: str = Query(...),
                              error: str = Query(None), error_description: str = Query(None)):
    if error:
        from fastapi.responses import RedirectResponse
        logger.warning('IG OAuth denied: %s — %s', error, error_description)
        return RedirectResponse(f"{FRONTEND_URL}/app/settings?ig=error&reason={error}")
    user_id = state
    redirect_uri = f"{BACKEND_PUBLIC_URL}/api/instagram/callback"
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(
                'https://graph.facebook.com/v21.0/oauth/access_token',
                params={
                    'client_id': META_APP_ID,
                    'client_secret': META_APP_SECRET,
                    'redirect_uri': redirect_uri,
                    'code': code,
                },
            )
            data = r.json()
            token = data.get('access_token')
            if not token:
                logger.error('Token exchange failed: %s', data)
                raise HTTPException(400, f'Token exchange failed: {data}')
            # Exchange for long-lived token
            ll = await c.get(
                'https://graph.facebook.com/v21.0/oauth/access_token',
                params={
                    'grant_type': 'fb_exchange_token',
                    'client_id': META_APP_ID,
                    'client_secret': META_APP_SECRET,
                    'fb_exchange_token': token,
                },
            )
            ll_data = ll.json()
            long_token = ll_data.get('access_token', token)
            # Get IG handle via /me/accounts → instagram_business_account
            u = await c.get('https://graph.facebook.com/v21.0/me/accounts',
                            params={'access_token': long_token,
                                    'fields': 'id,name,instagram_business_account{id,username,followers_count}'})
            accounts = u.json().get('data', [])
            handle = None
            ig_user_id = None
            followers = 0
            page_access_token = None
            for acc in accounts:
                ig = acc.get('instagram_business_account')
                if ig and ig.get('username'):
                    handle = '@' + ig['username']
                    ig_user_id = ig.get('id')
                    followers = ig.get('followers_count', 0)
                    # Get page-level access token for messaging
                    page_r = await c.get(
                        f"https://graph.facebook.com/v21.0/{acc['id']}",
                        params={'fields': 'access_token', 'access_token': long_token}
                    )
                    page_access_token = page_r.json().get('access_token', long_token)
                    break
            await db.users.update_one(
                {'id': user_id},
                {'$set': {
                    'instagramConnected': True,
                    'instagramHandle': handle or '@instagram',
                    'instagramFollowers': followers,
                    'ig_user_id': ig_user_id,
                    'meta_access_token': page_access_token or long_token,
                }},
            )
            logger.info('IG connected for user %s: %s (%s followers)', user_id, handle, followers)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception('IG callback failed')
        from fastapi.responses import RedirectResponse
        return RedirectResponse(f"{FRONTEND_URL}/app/settings?ig=error&reason=server_error")
    from fastapi.responses import RedirectResponse
    return RedirectResponse(f"{FRONTEND_URL}/app/settings?ig=connected")


@api.get('/instagram/status')
async def instagram_status(user_id: str = Depends(get_current_user_id)):
    u = await db.users.find_one({'id': user_id})
    if not u:
        raise HTTPException(404, 'User not found')
    return {
        'connected': bool(u.get('instagramConnected')),
        'handle': u.get('instagramHandle'),
        'followers': u.get('instagramFollowers', 0),
        'ig_user_id': u.get('ig_user_id'),
        'meta_configured': bool(META_APP_ID and META_APP_SECRET),
    }


@api.post('/instagram/disconnect')
async def instagram_disconnect(user_id: str = Depends(get_current_user_id)):
    await db.users.update_one(
        {'id': user_id},
        {'$set': {'instagramConnected': False, 'instagramHandle': None}, '$unset': {'meta_access_token': ''}}
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
    asyncio.create_task(_process_webhook(payload))
    return {'ok': True}


async def _process_webhook(payload: dict):
    """Process Instagram webhook events asynchronously."""
    try:
        for entry in payload.get('entry', []):
            ig_account_id = entry.get('id')
            # Find which user owns this IG account
            user_doc = await db.users.find_one({'ig_user_id': ig_account_id})
            if not user_doc:
                logger.warning('No user found for ig_user_id %s', ig_account_id)
                continue
            user_id = user_doc['id']

            for event in entry.get('messaging', []):
                sender_id = event.get('sender', {}).get('id')
                if sender_id == ig_account_id:
                    continue  # skip own messages
                msg_obj = event.get('message', {})
                msg_text = msg_obj.get('text', '')

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

                # Match automations by keyword trigger
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

            for change in entry.get('changes', []):
                field = change.get('field')
                value = change.get('value', {})
                if field == 'comments':
                    comment_text = value.get('text', '')
                    commenter_id = value.get('from', {}).get('id')
                    if commenter_id and commenter_id != ig_account_id:
                        automations = await db.automations.find(
                            {'user_id': user_id, 'status': 'active'}
                        ).to_list(100)
                        for auto in automations:
                            trigger = (auto.get('trigger') or '').lower()
                            if trigger.startswith('keyword:'):
                                keyword = trigger.split(':', 1)[1].strip()
                                if keyword and keyword.lower() in comment_text.lower():
                                    asyncio.create_task(execute_flow(user_doc, auto, commenter_id, comment_text))
                elif field == 'story_insights':
                    replier_id = value.get('from', {}).get('id')
                    if replier_id:
                        automations = await db.automations.find(
                            {'user_id': user_id, 'status': 'active', 'trigger': 'Story Reply'}
                        ).to_list(20)
                        for auto in automations:
                            asyncio.create_task(execute_flow(user_doc, auto, replier_id, ''))
    except Exception:
        logger.exception('Webhook processing error')


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
                msg = {'id': str(_uuid.uuid4()), 'from': 'me', 'text': text,
                       'time': datetime.utcnow().strftime('%I:%M %p')}
                await db.conversations.update_one(
                    {'id': conv_id},
                    {'$push': {'messages': msg},
                     '$set': {'lastMessage': text, 'time': 'now', 'unread': 0}}
                )
                await ws_manager.send(user_id, {'type': 'message', 'conv_id': conv_id, 'message': msg})

                # If user has IG connected, send via Meta API
                user_doc = await db.users.find_one({'id': user_id})
                if user_doc and user_doc.get('instagramConnected'):
                    ig_recipient = conv.get('contact', {}).get('ig_id')
                    if ig_recipient:
                        await send_ig_dm(
                            user_doc.get('meta_access_token', ''),
                            user_doc.get('ig_user_id', ''),
                            ig_recipient, text
                        )

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


@app.on_event('shutdown')
async def shutdown_db_client():
    client.close()
