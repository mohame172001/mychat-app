from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .rule_normalizer import normalize_comment_dm_rule, _materialize_comment_dm_rule


_DEFERRED_FLOW_FIELDS = (
    'opening_dm_text',
    'opening_dm_button_text',
    'link_dm_text',
    'link_url',
    'follow_request_enabled',
    'email_request_enabled',
    'follow_up_enabled',
    'follow_up_text',
)


def _comment_dm_flow_enabled(automation: dict) -> bool:
    return bool(normalize_comment_dm_rule(automation).get('enabled'))


def _comment_dm_flow_classification(automation: dict) -> Dict[str, Any]:
    normalized = normalize_comment_dm_rule(automation)
    mode = normalized['mode']
    mode_ok = normalized['mode_ok']
    present: List[str] = []
    missing: List[str] = []
    for field in _DEFERRED_FLOW_FIELDS:
        value = normalized.get(field)
        if field == 'follow_up_enabled':
            paired = bool(value) and bool(normalized.get('follow_up_text'))
            if paired:
                present.append('follow_up_enabled+text')
            else:
                missing.append('follow_up_enabled+text')
            continue
        if field == 'follow_up_text':
            continue
        if value:
            present.append(field)
        else:
            missing.append(field)
    return {
        'enabled': bool(normalized['enabled']),
        'mode': mode,
        'mode_ok': mode_ok,
        'present_deferred_fields': present,
        'missing_deferred_fields': missing,
        'button_flow_ready': bool(normalized['button_flow_ready']),
        'button_flow_missing': list(normalized['missing_fields']),
        'has_legacy_dm_text': bool((automation or {}).get('dm_text') or (automation or {}).get('dmText')),
        'one_shot_dm_only': bool(normalized['one_shot_dm_only']),
        'has_opening_dm': bool(normalized.get('has_opening_dm')),
        'has_button': bool(normalized.get('has_button')),
        'has_next_step': bool(normalized.get('has_next_step')),
        'source_fields_used': normalized.get('source_fields_used') or {},
    }


async def _create_comment_dm_session(
    user_doc: dict,
    automation: dict,
    recipient_ig_id: str,
    comment_context: Optional[dict],
    payload: str,
    *,
    db,
    logger,
    normalize_follow_gate_config,
    current_instagram_context,
    conversion_tracking_enabled,
    safe_partial_identifier,
) -> dict:
    import uuid as _uuid

    now = datetime.utcnow()
    automation = _materialize_comment_dm_rule(automation)
    follow_gate = normalize_follow_gate_config(automation)
    link_url = (automation.get('link_url') or '').strip()
    session = {
        'id': payload.split(':')[1] if ':' in payload else str(_uuid.uuid4()),
        'user_id': user_doc['id'],
        **current_instagram_context(user_doc),
        'ig_user_id': user_doc.get('ig_user_id') or '',
        'recipient_id': recipient_ig_id,
        'automation_id': automation.get('id'),
        'automation_name': automation.get('name'),
        'comment_doc_id': (comment_context or {}).get('comment_doc_id'),
        'ig_comment_id': (comment_context or {}).get('ig_comment_id'),
        'source_comment_id': (comment_context or {}).get('source_comment_id')
        or (comment_context or {}).get('ig_comment_id'),
        'media_id': (comment_context or {}).get('media_id'),
        'mediaId': (comment_context or {}).get('media_id'),
        'commenter_id': (comment_context or {}).get('commenter_id') or recipient_ig_id,
        'opening_dedupe_key': (comment_context or {}).get('opening_dedupe_key'),
        'openingDedupeKey': (comment_context or {}).get('opening_dedupe_key'),
        'payload': payload,
        'status': 'pending',
        'stage': 'awaiting_user_action',
        'link_dm_text': (automation.get('link_dm_text') or '').strip(),
        'link_button_text': (automation.get('link_button_text') or '').strip(),
        'link_url': link_url,
        'conversionTrackingEnabled': conversion_tracking_enabled(automation, link_url),
        **follow_gate,
        'follow_confirmed': False,
        'follow_confirmation_attempts': 0,
        'follow_verified': False,
        'follow_verification_attempts': 0,
        'followLastCheckedAt': None,
        'followReminderCount': 0,
        'lastFollowVerificationError': None,
        'finalDmSentAt': None,
        'expiresAt': now + timedelta(minutes=follow_gate['follow_gate_expires_after_minutes']),
        'email_request_enabled': bool(automation.get('email_request_enabled')),
        'follow_up_enabled': bool(automation.get('follow_up_enabled')),
        'follow_up_text': (automation.get('follow_up_text') or '').strip(),
        'created': now,
        'updated': now,
    }
    await db.comment_dm_sessions.insert_one(session)
    logger.info(
        'comment_opening_flow_state_created user_id=%s instagram_account_id=%s '
        'automation_id=%s media_id=%s commenter_id=%s dedupe_key=%s session_id=%s',
        user_doc.get('id'),
        safe_partial_identifier(user_doc.get('ig_user_id')),
        automation.get('id'),
        safe_partial_identifier((comment_context or {}).get('media_id')),
        safe_partial_identifier((comment_context or {}).get('commenter_id') or recipient_ig_id),
        safe_partial_identifier((comment_context or {}).get('opening_dedupe_key')),
        session.get('id'),
    )
    return session


async def _send_comment_dm_flow_entry(
    user_doc: dict,
    automation: dict,
    recipient_ig_id: str,
    comment_context: Optional[dict] = None,
    *,
    db,
    logger,
    safe_partial_identifier,
    create_tracked_task,
    verify_and_heal_ig_subscription_async,
    comment_dm_subscription_cache_max_age_seconds: int,
    create_comment_dm_session,
    send_ig_quick_reply,
    send_ig_dm,
    send_text_dm_with_optional_tracking,
    send_comment_dm_flow_completion,
    current_instagram_context,
    conversion_tracking_enabled,
    extract_first_url,
) -> bool:
    import time as _time
    import uuid as _uuid

    _entry_start = _time.monotonic()
    access_token = user_doc.get('meta_access_token', '')
    ig_user_id = user_doc.get('ig_user_id', '')
    logger.info(
        'comment_opening_flow_started user_id=%s instagram_account_id=%s '
        'automation_id=%s media_id=%s commenter_id=%s dedupe_key=%s',
        user_doc.get('id'),
        safe_partial_identifier(ig_user_id),
        automation.get('id'),
        safe_partial_identifier((comment_context or {}).get('media_id')),
        safe_partial_identifier((comment_context or {}).get('commenter_id') or recipient_ig_id),
        safe_partial_identifier((comment_context or {}).get('opening_dedupe_key')),
    )

    subscription_cache_age_s: Optional[float] = None
    needs_inline_verify = True
    cache_doc: Dict[str, Any] = {}
    if ig_user_id:
        try:
            cache_doc = await db.instagram_accounts.find_one(
                {'$or': [
                    {'instagramAccountId': ig_user_id},
                    {'igUserId': ig_user_id},
                ]},
                {
                    'webhookSubscriptionLastCheckedAt': 1,
                    'webhookSubscriptionMissing': 1,
                    'webhookSubscriptionFields': 1,
                },
            ) or {}
        except Exception:
            cache_doc = {}
    last_checked = cache_doc.get('webhookSubscriptionLastCheckedAt')
    if isinstance(last_checked, datetime):
        subscription_cache_age_s = (datetime.utcnow() - last_checked).total_seconds()
    missing_cached = cache_doc.get('webhookSubscriptionMissing') or []
    critical_required = {'messages', 'messaging_postbacks'}
    critical_missing_in_cache = bool(critical_required.intersection(missing_cached or []))
    if (
        subscription_cache_age_s is not None
        and subscription_cache_age_s <= comment_dm_subscription_cache_max_age_seconds
        and not critical_missing_in_cache
    ):
        needs_inline_verify = False
        logger.info(
            'comment_dm_opening_subscription_cache_hit ig_user_id=%s age_s=%s missing_in_cache=%s',
            safe_partial_identifier(ig_user_id),
            int(subscription_cache_age_s),
            sorted(missing_cached or []),
        )
    if needs_inline_verify and ig_user_id and access_token:
        logger.info(
            'comment_dm_opening_subscription_recheck_scheduled '
            'ig_user_id=%s reason=%s',
            safe_partial_identifier(ig_user_id),
            (
                'cache_stale' if (subscription_cache_age_s is None
                                  or subscription_cache_age_s > comment_dm_subscription_cache_max_age_seconds)
                else 'cache_missing_critical'
            ),
        )
        try:
            create_tracked_task(
                verify_and_heal_ig_subscription_async(ig_user_id, access_token),
                'comment_dm_subscription_recheck',
            )
        except Exception as exc:
            logger.info(
                'comment_dm_opening_subscription_recheck_schedule_failed err=%s',
                type(exc).__name__,
            )
    _gate_ms = int((_time.monotonic() - _entry_start) * 1000)
    if _gate_ms > 50:
        logger.info(
            'comment_dm_opening_subscription_gate_ms=%s ig_user_id=%s needs_inline_verify=%s',
            _gate_ms,
            safe_partial_identifier(ig_user_id),
            needs_inline_verify,
        )

    automation = _materialize_comment_dm_rule(automation)
    normalized_rule = normalize_comment_dm_rule(automation)
    opening_text = normalized_rule['opening_dm_text'] or str(
        automation.get('dm_text') or automation.get('dmText') or ''
    ).strip()
    button_text = (normalized_rule['opening_dm_button_text'] or 'Send me the link').strip()
    has_deferred_step = bool(normalized_rule['has_next_step'])

    if opening_text and has_deferred_step:
        payload = f'comment_flow:{str(_uuid.uuid4())}:continue'
        await create_comment_dm_session(user_doc, automation, recipient_ig_id, comment_context, payload)
        result = await send_ig_quick_reply(
            access_token, ig_user_id, recipient_ig_id,
            opening_text, button_text, payload,
            allow_workspace_recipient=True,
        )
        if result.get('ok'):
            logger.info('comment_dm_opening_quick_reply_sent rule_id=%s recipient=%s',
                        automation.get('id'), recipient_ig_id)
            return True
        logger.warning('comment_dm_quick_reply_failed rule_id=%s err=%s; falling back to text',
                       automation.get('id'), result.get('error'))
        return await send_ig_dm(
            access_token,
            ig_user_id,
            recipient_ig_id,
            opening_text,
            allow_workspace_recipient=True,
        )

    if opening_text:
        return await send_text_dm_with_optional_tracking(
            user_doc,
            {
                'user_id': user_doc['id'],
                **current_instagram_context(user_doc),
                'recipient_id': recipient_ig_id,
                'automation_id': automation.get('id'),
                'ig_comment_id': (comment_context or {}).get('ig_comment_id'),
                'comment_doc_id': (comment_context or {}).get('comment_doc_id'),
                'conversionTrackingEnabled': conversion_tracking_enabled(
                    automation, extract_first_url(opening_text)
                ),
            },
            opening_text,
            allow_workspace_recipient=True,
        )

    if has_deferred_step:
        payload = f'comment_flow:{str(_uuid.uuid4())}:continue'
        session = await create_comment_dm_session(
            user_doc, automation, recipient_ig_id, comment_context, payload
        )
        return await send_comment_dm_flow_completion(user_doc, session)

    return await send_comment_dm_flow_completion(
        user_doc,
        {
            'user_id': user_doc['id'],
            'ig_user_id': ig_user_id,
            'recipient_id': recipient_ig_id,
            'automation_id': automation.get('id'),
            'link_dm_text': normalized_rule['link_dm_text'],
            'link_button_text': normalized_rule['link_button_text'],
            'link_url': normalized_rule['link_url'],
            'conversionTrackingEnabled': conversion_tracking_enabled(
                automation, normalized_rule['link_url']
            ),
            'follow_request_enabled': bool(normalized_rule['follow_request_enabled']),
            'follow_verified': False,
            'email_request_enabled': bool(normalized_rule['email_request_enabled']),
            'follow_up_enabled': bool(normalized_rule['follow_up_enabled']),
            'follow_up_text': normalized_rule['follow_up_text'],
        },
    )
