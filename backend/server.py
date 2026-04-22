import os
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Dict

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


# ---------------- Comment reply helper ----------------
async def reply_to_ig_comment(access_token: str, ig_comment_id: str, text: str) -> bool:
    """Reply to an Instagram comment via Graph API."""
    if not access_token or not ig_comment_id:
        return False
    url = f'https://graph.facebook.com/v21.0/{ig_comment_id}/replies'
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
    return UserPublic(
        id=u['id'], username=u['username'], name=u['name'], email=u['email'],
        avatar=u.get('avatar') or f"https://i.pravatar.cc/150?u={u['username']}",
        instagramConnected=bool(u.get('instagramConnected', False)),
        instagramHandle=u.get('instagramHandle'),
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
    """Create an automation that:
      - watches comments on a specific IG media (or the latest post)
      - replies to the comment with `comment_reply`
      - sends a DM to the commenter with `dm_text`
    Body: {media_id?: str, latest?: bool, comment_reply: str, dm_text?: str, name?: str}
    """
    import uuid
    media_id = (data.get('media_id') or '').strip() or None
    latest = bool(data.get('latest'))
    if not media_id and not latest:
        raise HTTPException(400, 'Provide media_id or set latest=true')
    comment_reply = (data.get('comment_reply') or '').strip()
    dm_text = (data.get('dm_text') or 'شكرا').strip()
    if not comment_reply and not dm_text:
        raise HTTPException(400, 'comment_reply or dm_text is required')

    trigger = 'comment:latest' if latest else f'comment:{media_id}'
    name = data.get('name') or (
        'Reply to latest post comments' if latest
        else f'Reply to comments on {media_id[:10]}'
    )

    # Build a simple flow: trigger -> reply_comment -> message
    nodes = [{'id': 'n_trigger', 'type': 'trigger',
              'data': {'label': 'Comment trigger', 'trigger': trigger}}]
    edges = []
    prev = 'n_trigger'
    if comment_reply:
        nodes.append({'id': 'n_reply', 'type': 'reply_comment',
                      'data': {'text': comment_reply}})
        edges.append({'id': 'e1', 'source': prev, 'target': 'n_reply'})
        prev = 'n_reply'
    if dm_text:
        nodes.append({'id': 'n_dm', 'type': 'message',
                      'data': {'text': dm_text}})
        edges.append({'id': f'e{len(edges)+1}', 'source': prev, 'target': 'n_dm'})

    doc = {
        'id': str(uuid.uuid4()),
        'user_id': user_id,
        'name': name,
        'status': 'active',
        'trigger': trigger,
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
    url = f'https://graph.facebook.com/v21.0/{ig_comment_id}/replies'
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
            page_id = None
            for acc in accounts:
                ig = acc.get('instagram_business_account')
                if ig and ig.get('username'):
                    handle = '@' + ig['username']
                    ig_user_id = ig.get('id')
                    followers = ig.get('followers_count', 0)
                    page_id = acc.get('id')
                    # Get page-level access token for messaging
                    page_r = await c.get(
                        f"https://graph.facebook.com/v21.0/{page_id}",
                        params={'fields': 'access_token', 'access_token': long_token}
                    )
                    page_access_token = page_r.json().get('access_token', long_token)
                    # Subscribe the Page to webhook events so Meta forwards DMs + comments
                    try:
                        sub = await c.post(
                            f"https://graph.facebook.com/v21.0/{page_id}/subscribed_apps",
                            params={
                                'access_token': page_access_token,
                                'subscribed_fields': (
                                    'messages,messaging_postbacks,messaging_optins,'
                                    'message_deliveries,message_reads,feed'
                                ),
                            },
                        )
                        logger.info('Page subscribe status=%s body=%s', sub.status_code, sub.text[:200])
                    except Exception as e:
                        logger.warning('Page subscribe failed: %s', e)
                    break
            await db.users.update_one(
                {'id': user_id},
                {'$set': {
                    'instagramConnected': True,
                    'instagramHandle': handle or '@instagram',
                    'instagramFollowers': followers,
                    'ig_user_id': ig_user_id,
                    'fb_page_id': page_id,
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


@api.post('/instagram/subscribe-webhook')
async def instagram_subscribe_webhook(user_id: str = Depends(get_current_user_id)):
    """Force-subscribe the user's connected Page to webhook fields. Used after
    the initial OAuth (pre-fix) where subscription didn't happen yet."""
    u = await db.users.find_one({'id': user_id})
    if not u or not u.get('instagramConnected'):
        raise HTTPException(400, 'Instagram not connected')
    token = u.get('meta_access_token', '')
    # Look up page id fresh
    async with httpx.AsyncClient(timeout=20) as c:
        accs = await c.get('https://graph.facebook.com/v21.0/me/accounts',
                           params={'access_token': token,
                                   'fields': 'id,name,access_token,instagram_business_account'})
        data = accs.json().get('data', [])
        page_id = None
        page_token = token
        for acc in data:
            if acc.get('instagram_business_account'):
                page_id = acc.get('id')
                page_token = acc.get('access_token') or token
                break
        if not page_id:
            raise HTTPException(404, 'No page with linked IG business account')
        sub = await c.post(
            f"https://graph.facebook.com/v21.0/{page_id}/subscribed_apps",
            params={
                'access_token': page_token,
                'subscribed_fields': (
                    'messages,messaging_postbacks,messaging_optins,'
                    'message_deliveries,message_reads,feed,'
                    'comments,mentions,message_reactions'
                ),
            },
        )
        body = sub.text
        ok = sub.status_code == 200
        # Verify the subscription
        verify = await c.get(f"https://graph.facebook.com/v21.0/{page_id}/subscribed_apps",
                             params={'access_token': page_token})
        # Persist the fresh page id + page token on the user
        await db.users.update_one(
            {'id': user_id},
            {'$set': {'fb_page_id': page_id, 'meta_access_token': page_token}},
        )
        return {'ok': ok, 'status': sub.status_code, 'body': body,
                'page_id': page_id, 'subscribed_apps': verify.json()}


@api.get('/instagram/media')
async def instagram_media(user_id: str = Depends(get_current_user_id), limit: int = 25):
    """List the user's recent Instagram posts via Graph API."""
    u = await db.users.find_one({'id': user_id})
    if not u or not u.get('instagramConnected'):
        raise HTTPException(400, 'Instagram not connected')
    token = u.get('meta_access_token', '')
    ig_id = u.get('ig_user_id', '')
    if not ig_id:
        raise HTTPException(400, 'Missing ig_user_id')
    url = f'https://graph.facebook.com/v21.0/{ig_id}/media'
    params = {
        'access_token': token,
        'fields': 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp',
        'limit': max(1, min(limit, 50)),
    }
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(url, params=params)
            if r.status_code != 200:
                logger.error('IG media fetch error %s: %s', r.status_code, r.text)
                raise HTTPException(r.status_code, f'Graph API error: {r.text}')
            body = r.json()
    except HTTPException:
        raise
    except Exception as e:
        logger.exception('IG media fetch failed')
        raise HTTPException(500, str(e))
    return {'items': body.get('data', [])}


async def _fetch_latest_media_id(access_token: str, ig_user_id: str) -> Optional[str]:
    if not access_token or not ig_user_id:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f'https://graph.facebook.com/v21.0/{ig_user_id}/media',
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


async def _process_webhook(payload: dict):
    """Process Instagram webhook events asynchronously."""
    try:
        for entry in payload.get('entry', []):
            ig_account_id = entry.get('id')
            # Find which user owns this IG account — entry.id can be either the
            # Instagram Business account id OR the Facebook Page id depending on
            # how the subscription was created, so try both.
            user_doc = await db.users.find_one({'$or': [
                {'ig_user_id': ig_account_id},
                {'fb_page_id': ig_account_id},
            ]})
            if not user_doc:
                logger.warning('No user found for webhook entry id %s', ig_account_id)
                continue
            # Normalize so downstream code uses the real IG account id
            ig_account_id = user_doc.get('ig_user_id') or ig_account_id
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
                # Normalize: IG sends field='comments'; FB Page feed sends field='feed' with item='comment'
                is_comment = field == 'comments' or (field == 'feed' and value.get('item') == 'comment')
                if is_comment:
                    comment_text = value.get('text') or value.get('message', '')
                    commenter = value.get('from', {}) or {}
                    commenter_id = commenter.get('id')
                    commenter_username = commenter.get('username') or commenter.get('name') or f'ig_{(commenter_id or "")[:8]}'
                    ig_comment_id = value.get('comment_id') or value.get('id')
                    media_obj = value.get('media') or {}
                    media_id = media_obj.get('id') or value.get('post_id') or value.get('parent_id')
                    if commenter_id and commenter_id != ig_account_id and ig_comment_id:
                        import uuid as _uuid
                        # Store the comment so UI can list + reply
                        doc = {
                            'id': str(_uuid.uuid4()),
                            'user_id': user_id,
                            'ig_comment_id': ig_comment_id,
                            'media_id': media_id,
                            'commenter_id': commenter_id,
                            'commenter_username': commenter_username,
                            'text': comment_text,
                            'replied': False,
                            'created': datetime.utcnow(),
                        }
                        await db.comments.insert_one(doc)
                        await ws_manager.send(user_id, {'type': 'comment', 'comment': _strip_mongo({**doc})})

                        # Run matching automations
                        automations = await db.automations.find(
                            {'user_id': user_id, 'status': 'active'}
                        ).to_list(100)
                        latest_media_id = None  # resolved lazily
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
                                if target.lower() == 'latest':
                                    if latest_media_id is None:
                                        latest_media_id = await _fetch_latest_media_id(
                                            user_doc.get('meta_access_token', ''),
                                            user_doc.get('ig_user_id', ''),
                                        ) or ''
                                    if media_id and latest_media_id and media_id == latest_media_id:
                                        fire = True
                                elif target and media_id and target == media_id:
                                    fire = True
                            if fire:
                                asyncio.create_task(execute_flow(
                                    user_doc, auto, commenter_id, comment_text,
                                    comment_context={'ig_comment_id': ig_comment_id, 'comment_doc_id': doc['id']}
                                ))
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


@app.on_event('shutdown')
async def shutdown_db_client():
    client.close()
