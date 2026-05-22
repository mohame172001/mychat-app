from datetime import datetime
from typing import Any, Optional


INSTAGRAM_ACCOUNT_IDENTITY_FIELDS = (
    'id',
    'instagramAccountId',
    'igUserId',
    'ig_user_id',
    'accountId',
    'instagram_account_id',
    'instagramAccountDbId',
    'graphMeId',
    'graph_me_id',
    'graph_me_user_id',
    'instagram_graph_me_id',
    'instagram_graph_me_user_id',
    'pageId',
    'page_id',
    'fb_page_id',
    'businessAccountId',
    'instagram_business_account_id',
)


def _clean_identity(value: Any) -> str:
    return str(value or '').strip()


def _instagram_account_identity_values(account_doc: Optional[dict]) -> list:
    """Return every safe account identity alias stored on an IG account row.

    Production has accumulated several field names across OAuth, token
    refresh, webhook diagnostics, and legacy user mirroring. Webhook routing
    must be account-first and alias-tolerant, otherwise later connected
    accounts can fall through to polling or fail to resolve entirely.
    """
    values = []
    if not isinstance(account_doc, dict):
        return values
    for field in INSTAGRAM_ACCOUNT_IDENTITY_FIELDS:
        value = _clean_identity(account_doc.get(field))
        if value and value not in values:
            values.append(value)
    metadata = account_doc.get('metadata') if isinstance(account_doc.get('metadata'), dict) else {}
    for field in INSTAGRAM_ACCOUNT_IDENTITY_FIELDS:
        value = _clean_identity(metadata.get(field))
        if value and value not in values:
            values.append(value)
    return values


def _instagram_account_identity_clauses(identity: Any) -> list:
    value = _clean_identity(identity)
    if not value:
        return []
    return [{field: value} for field in INSTAGRAM_ACCOUNT_IDENTITY_FIELDS]


def _instagram_account_identity_query(identity: Any, *, active_only: bool = True) -> dict:
    clauses = _instagram_account_identity_clauses(identity)
    if not clauses:
        return {'_id': '__no_instagram_identity__'}
    query = {'$or': clauses}
    if active_only:
        query['isActive'] = {'$ne': False}
    return query


def _instagram_context_from_account(account_doc: Optional[dict]) -> dict:
    account_doc = account_doc or {}
    instagram_account_id = str(
        account_doc.get('instagramAccountId')
        or account_doc.get('igUserId')
        or account_doc.get('ig_user_id')
        or account_doc.get('accountId')
        or ''
    )
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
        instagram_account_id = str(
            account_or_ig_id.get('instagramAccountId')
            or account_or_ig_id.get('igUserId')
            or account_or_ig_id.get('ig_user_id')
            or account_or_ig_id.get('accountId')
            or ''
        )
    else:
        account_id = ''
        instagram_account_id = str(account_or_ig_id or '')
    clauses = []
    if account_id:
        clauses.extend([
            {'instagramAccountDbId': account_id},
            {'instagram_account_id': account_id},
        ])
    if instagram_account_id:
        clauses.extend([
            {'instagramAccountId': instagram_account_id},
            {'igUserId': instagram_account_id},
            {'ig_user_id': instagram_account_id},
            {'accountId': instagram_account_id},
            {'graphMeId': instagram_account_id},
            {'graph_me_id': instagram_account_id},
            {'instagram_graph_me_id': instagram_account_id},
            {'pageId': instagram_account_id},
            {'page_id': instagram_account_id},
            {'fb_page_id': instagram_account_id},
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
    instagram_account_id = (
        account_doc.get('instagramAccountId')
        or account_doc.get('igUserId')
        or account_doc.get('ig_user_id')
        or account_doc.get('accountId')
        or ''
    )
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


async def get_active_instagram_account(
    user_id: str,
    *,
    db,
    sync_user_instagram_account_doc,
    parse_graph_datetime,
    http_exception_cls,
) -> dict:
    user_doc = await db.users.find_one({'id': user_id})
    if not user_doc:
        raise http_exception_cls(404, 'User not found')
    await sync_user_instagram_account_doc(user_doc)

    active_id = user_doc.get('active_instagram_account_id') or ''
    account = None
    if active_id:
        account = await db.instagram_accounts.find_one({
            'id': active_id,
            '$or': [{'userId': user_id}, {'user_id': user_id}],
            'isActive': {'$ne': False},
        })
    if not account:
        account = await db.instagram_accounts.find_one({
            '$or': [{'userId': user_id}, {'user_id': user_id}],
            'isCurrent': True,
            'isActive': {'$ne': False},
        })
    if not account and user_doc.get('ig_user_id'):
        account = await db.instagram_accounts.find_one({
            '$and': [
                {'$or': [{'userId': user_id}, {'user_id': user_id}]},
                {'$or': [
                    {'instagramAccountId': str(user_doc.get('ig_user_id'))},
                    {'igUserId': str(user_doc.get('ig_user_id'))},
                ]},
            ],
            'isActive': {'$ne': False},
        })
    if not account:
        account = await db.instagram_accounts.find_one({
            '$or': [{'userId': user_id}, {'user_id': user_id}],
            'isActive': {'$ne': False},
        })
    if not account:
        raise http_exception_cls(400, 'No Instagram account connected')

    instagram_account_id = account.get('instagramAccountId') or account.get('igUserId') or ''
    token = account.get('accessToken') or ''
    now = datetime.utcnow()
    await db.instagram_accounts.update_many(
        {'$or': [{'userId': user_id}, {'user_id': user_id}]},
        {'$set': {'isCurrent': False}},
    )
    await db.instagram_accounts.update_one(
        {'id': account['id'], '$or': [{'userId': user_id}, {'user_id': user_id}]},
        {'$set': {'userId': user_id, 'user_id': user_id, 'isCurrent': True, 'updatedAt': now}},
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
            'tokenExpiresAt': parse_graph_datetime(account.get('tokenExpiresAt')),
            'instagram_token_expires_at': parse_graph_datetime(account.get('tokenExpiresAt')),
            'lastRefreshedAt': parse_graph_datetime(account.get('lastRefreshedAt')),
            'refreshStatus': account.get('refreshStatus'),
            'refreshAttempts': int(account.get('refreshAttempts') or 0),
            'updated': now,
        }},
    )
    return account


async def _find_user_doc_for_instagram_account_id(
    instagram_account_id: str,
    *,
    db,
    logger,
) -> tuple:
    if not instagram_account_id:
        return None, None
    account_doc = await db.instagram_accounts.find_one(
        _instagram_account_identity_query(instagram_account_id)
    )
    if account_doc:
        owner = await db.users.find_one({'id': account_doc.get('userId') or account_doc.get('user_id')})
        if owner:
            return _with_instagram_account_context(owner, account_doc), 'instagram_accounts'

    user_doc = await db.users.find_one({'$or': [
        {'ig_user_id': instagram_account_id},
        {'fb_page_id': instagram_account_id},
        {'instagram_graph_me_id': instagram_account_id},
        {'instagram_graph_me_user_id': instagram_account_id},
    ]})
    if user_doc:
        scoped = user_doc
        try:
            account_identity = _instagram_account_identity_clauses(instagram_account_id)
            matching_account = await db.instagram_accounts.find_one({
                '$and': [
                    {'$or': [
                        {'userId': user_doc.get('id')},
                        {'user_id': user_doc.get('id')},
                    ]},
                    {'$or': account_identity},
                ],
                'isActive': {'$ne': False},
            })
            if matching_account:
                scoped = _with_instagram_account_context(user_doc, matching_account)
        except Exception as exc:
            logger.info('users_ig_user_id_fallback_scope_failed err=%s',
                        type(exc).__name__)
        return scoped, 'users.ig_user_id'
    return None, None


async def _active_instagram_account_owner(
    instagram_account_id: str,
    *,
    db,
    canonical_instagram_account_id,
) -> Optional[dict]:
    canonical = canonical_instagram_account_id(instagram_account_id)
    if not canonical:
        return None
    account = await db.instagram_accounts.find_one({
        **_instagram_account_identity_query(canonical),
        'connectionValid': True,
    })
    if not account:
        return None
    owner_id = account.get('userId') or account.get('user_id')
    if not owner_id:
        return None
    owner = await db.users.find_one({'id': owner_id}) or {}
    if owner.get('status') in ('deleted', 'suspended'):
        return None
    return account
