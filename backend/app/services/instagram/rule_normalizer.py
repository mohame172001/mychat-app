from datetime import datetime
from typing import Any, Dict, List


_COMMENT_DM_FIELD_ALIASES: Dict[str, tuple] = {
    'opening_dm_text': (
        'opening_dm_text', 'openingDmText', 'opening_dm_message',
        'openingDmMessage',
    ),
    'opening_dm_button_text': (
        'opening_dm_button_text', 'openingDmButtonText',
        'button_text', 'buttonText', 'quick_reply_text',
        'quickReplyText', 'cta_text', 'ctaText',
    ),
    'link_dm_text': (
        'link_dm_text', 'linkDmText', 'link_message',
        'linkMessage', 'final_dm_text', 'finalDmText',
    ),
    'link_button_text': (
        'link_button_text', 'linkButtonText', 'link_cta_text',
        'linkCtaText',
    ),
    'link_url': (
        'link_url', 'linkUrl', 'url', 'final_url', 'finalUrl',
    ),
    'follow_request_enabled': (
        'follow_request_enabled', 'followRequestEnabled',
        'followGateEnabled',
    ),
    'follow_request_message': (
        'follow_request_message', 'followRequestMessage',
        'follow_gate_message', 'followGateMessage',
    ),
    'follow_request_button_text': (
        'follow_request_button_text', 'followRequestButtonText',
        'follow_gate_button_text', 'followGateButtonText',
    ),
    'follow_confirmation_keywords': (
        'follow_confirmation_keywords', 'followConfirmationKeywords',
        'followGateConfirmationKeywords',
    ),
    'follow_gate_fallback_message': (
        'follow_gate_fallback_message', 'followGateFallbackMessage',
    ),
    'verify_actual_follow': ('verify_actual_follow', 'verifyActualFollow'),
    'follow_not_detected_message': (
        'follow_not_detected_message', 'followNotDetectedMessage',
    ),
    'follow_verification_failed_message': (
        'follow_verification_failed_message', 'followVerificationFailedMessage',
    ),
    'follow_retry_button_text': (
        'follow_retry_button_text', 'followRetryButtonText',
    ),
    'follow_cooldown_message': (
        'follow_cooldown_message', 'followCooldownMessage',
    ),
    'max_follow_verification_attempts': (
        'max_follow_verification_attempts', 'maxFollowVerificationAttempts',
    ),
    'email_request_enabled': (
        'email_request_enabled', 'emailRequestEnabled',
    ),
    'follow_up_enabled': (
        'follow_up_enabled', 'followUpEnabled',
    ),
    'follow_up_text': (
        'follow_up_text', 'followUpText',
    ),
}


_BUTTON_FLOW_NEXT_STEP_FIELDS = (
    'link_url',
    'link_dm_text',
    'follow_up_enabled',
    'follow_request_enabled',
)


def _comment_dm_node_data(automation: dict) -> List[dict]:
    nodes = automation.get('nodes') if isinstance(automation.get('nodes'), list) else []
    data_rows: List[dict] = []
    for node in nodes:
        if not isinstance(node, dict) or node.get('type') != 'message':
            continue
        data = node.get('data') if isinstance(node.get('data'), dict) else {}
        if data:
            data_rows.append(data)
    return data_rows


def _first_rule_value(automation: dict, field: str) -> tuple:
    aliases = _COMMENT_DM_FIELD_ALIASES.get(field, (field,))
    for key in aliases:
        value = automation.get(key)
        if value not in (None, '', [], {}):
            return value, f'top_level.{key}'
    for data in _comment_dm_node_data(automation):
        for key in aliases:
            value = data.get(key)
            if value not in (None, '', [], {}):
                return value, f'nodes[].data.{key}'
        if field == 'opening_dm_text':
            value = data.get('text') or data.get('message')
            if value not in (None, '', [], {}):
                return value, 'nodes[].data.text'
        if field == 'link_dm_text':
            value = data.get('link_text') or data.get('linkText')
            if value not in (None, '', [], {}):
                return value, 'nodes[].data.linkText'
    return None, ''


def _comment_dm_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() not in ('', '0', 'false', 'no', 'off', 'none', 'null')
    return bool(value)


def normalize_comment_dm_rule(automation: dict) -> Dict[str, Any]:
    """Canonical comment-to-DM flow interpretation."""
    automation = automation or {}
    mode = str(automation.get('mode') or '').strip() or 'unset'
    mode_ok = mode == 'reply_and_dm'
    source_fields: Dict[str, str] = {}

    def field(name: str, default: Any = '') -> Any:
        value, source = _first_rule_value(automation, name)
        if source:
            source_fields[name] = source
        return default if value is None else value

    legacy_dm_text = str(automation.get('dm_text') or automation.get('dmText') or '').strip()
    opening_dm_text = str(field('opening_dm_text') or '').strip()
    opening_dm_button_text = str(field('opening_dm_button_text') or '').strip()
    link_dm_text = str(field('link_dm_text') or '').strip()
    link_button_text = str(field('link_button_text') or '').strip()
    link_url = str(field('link_url') or '').strip()
    follow_request_enabled = _comment_dm_bool(field('follow_request_enabled', False))
    email_request_enabled = _comment_dm_bool(field('email_request_enabled', False))
    follow_up_enabled = _comment_dm_bool(field('follow_up_enabled', False))
    follow_up_text = str(field('follow_up_text') or '').strip()
    if not opening_dm_text and legacy_dm_text and (
        opening_dm_button_text or link_dm_text or link_url
        or follow_request_enabled or email_request_enabled
        or (follow_up_enabled and follow_up_text)
    ):
        opening_dm_text = legacy_dm_text
        source_fields['opening_dm_text'] = 'top_level.dm_text'

    has_opening_dm = bool(opening_dm_text)
    has_button = bool(opening_dm_button_text)
    has_next_step = bool(
        link_dm_text
        or link_url
        or follow_request_enabled
        or email_request_enabled
        or (follow_up_enabled and follow_up_text)
    )
    button_flow_ready = mode_ok and has_opening_dm and has_next_step
    enabled = button_flow_ready

    missing_fields: List[str] = []
    if not mode_ok:
        missing_fields.append('mode != reply_and_dm')
    if not has_opening_dm:
        missing_fields.append('opening_dm_text')
    if not has_next_step:
        missing_fields.append(
            f'at least one of: {", ".join(_BUTTON_FLOW_NEXT_STEP_FIELDS)}'
        )
    if not has_button:
        missing_fields.append('opening_dm_button_text (optional - defaults to a sensible label)')

    return {
        'rule_id': automation.get('id'),
        'account_id': (
            automation.get('instagram_account_id')
            or automation.get('instagramAccountDbId')
            or automation.get('account_id')
            or automation.get('accountId')
        ),
        'instagram_account_id': (
            automation.get('instagramAccountId')
            or automation.get('igUserId')
            or automation.get('ig_user_id')
        ),
        'trigger': automation.get('trigger'),
        'scope': automation.get('post_scope') or automation.get('scope'),
        'selected_media_id': (
            automation.get('media_id')
            or automation.get('mediaId')
            or automation.get('trigger_media_id')
            or automation.get('triggerMediaId')
        ),
        'mode': mode,
        'mode_ok': mode_ok,
        'reply_under_post': automation.get('reply_under_post') is not False,
        'public_reply_text': (automation.get('comment_reply') or ''),
        'opening_dm_text': opening_dm_text,
        'opening_dm_button_text': opening_dm_button_text,
        'link_dm_text': link_dm_text,
        'link_button_text': link_button_text,
        'link_url': link_url,
        'follow_request_enabled': follow_request_enabled,
        'follow_request_message': str(field('follow_request_message') or '').strip(),
        'follow_request_button_text': str(field('follow_request_button_text') or '').strip(),
        'follow_confirmation_keywords': field('follow_confirmation_keywords', None),
        'follow_gate_fallback_message': str(field('follow_gate_fallback_message') or '').strip(),
        'verify_actual_follow': field('verify_actual_follow', None),
        'follow_not_detected_message': str(field('follow_not_detected_message') or '').strip(),
        'follow_verification_failed_message': str(field('follow_verification_failed_message') or '').strip(),
        'follow_retry_button_text': str(field('follow_retry_button_text') or '').strip(),
        'follow_cooldown_message': str(field('follow_cooldown_message') or '').strip(),
        'max_follow_verification_attempts': field('max_follow_verification_attempts', None),
        'email_request_enabled': email_request_enabled,
        'follow_up_enabled': follow_up_enabled,
        'follow_up_text': follow_up_text,
        'has_opening_dm': has_opening_dm,
        'has_button': has_button,
        'has_next_step': has_next_step,
        'button_flow_ready': button_flow_ready,
        'enabled': enabled,
        'one_shot_dm_only': mode_ok and bool(opening_dm_text or legacy_dm_text) and not button_flow_ready,
        'missing_fields': missing_fields,
        'source_fields_used': source_fields,
    }


def _materialize_comment_dm_rule(automation: dict) -> dict:
    normalized = normalize_comment_dm_rule(automation)
    materialized = dict(automation or {})
    for key in (
        'opening_dm_text', 'opening_dm_button_text', 'link_dm_text',
        'link_button_text', 'link_url', 'follow_up_text',
        'follow_request_message', 'follow_request_button_text',
        'follow_gate_fallback_message', 'follow_not_detected_message',
        'follow_verification_failed_message', 'follow_retry_button_text',
        'follow_cooldown_message',
    ):
        if normalized.get(key):
            materialized[key] = normalized[key]
    for key in (
        'follow_request_enabled', 'email_request_enabled',
        'follow_up_enabled',
    ):
        materialized[key] = bool(normalized.get(key))
    for key in ('follow_confirmation_keywords', 'verify_actual_follow', 'max_follow_verification_attempts'):
        if normalized.get(key) is not None:
            materialized[key] = normalized[key]
    materialized.setdefault('mode', 'reply_and_dm')
    if normalized.get('has_opening_dm'):
        materialized['opening_dm_enabled'] = True
    return materialized


def _comment_dm_backfill_patch(automation: dict) -> Dict[str, Any]:
    normalized = normalize_comment_dm_rule(automation)
    if not normalized.get('button_flow_ready'):
        return {}
    materialized = _materialize_comment_dm_rule(automation)
    patch: Dict[str, Any] = {}
    for key in (
        'mode', 'opening_dm_enabled', 'opening_dm_text',
        'opening_dm_button_text', 'link_dm_text', 'link_button_text',
        'link_url', 'follow_request_enabled', 'follow_request_message',
        'follow_request_button_text', 'follow_confirmation_keywords',
        'follow_gate_fallback_message', 'verify_actual_follow',
        'follow_not_detected_message',
        'follow_verification_failed_message', 'follow_retry_button_text',
        'follow_cooldown_message', 'max_follow_verification_attempts',
        'email_request_enabled', 'follow_up_enabled', 'follow_up_text',
    ):
        if key in materialized and automation.get(key) in (None, '', [], {}):
            patch[key] = materialized[key]
    if patch:
        now = datetime.utcnow()
        patch['deferred_flow_normalized_at'] = now
        patch['updated'] = now
        patch['updatedAt'] = now
    return patch


async def _ensure_comment_dm_rule_normalized(automation: dict, *, db=None, logger=None) -> dict:
    patch = _comment_dm_backfill_patch(automation)
    if not patch:
        return automation
    rule_id = automation.get('id')
    user_id = automation.get('user_id')
    if not (rule_id and user_id and db is not None):
        return {**automation, **patch}
    try:
        await db.automations.update_one(
            {'id': rule_id, 'user_id': user_id},
            {'$set': patch},
        )
        if logger is not None:
            logger.info(
                'comment_dm_rule_auto_normalized user_id=%s rule_id=%s fields=%s',
                user_id,
                rule_id,
                ','.join(sorted(k for k in patch.keys() if k not in ('updated', 'updatedAt'))),
            )
    except Exception as exc:
        if logger is not None:
            logger.warning(
                'comment_dm_rule_auto_normalize_failed user_id=%s rule_id=%s exception=%s',
                user_id,
                rule_id,
                type(exc).__name__,
            )
    return {**automation, **patch}
