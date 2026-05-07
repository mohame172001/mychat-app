"""Repair script: specific-rule comments with reply_status=disabled while
DM succeeded but the matched rule has a public reply configured.

Context:
    Production produced comments where dm_status=success but
    reply_status=disabled because of a runtime bug that has now been
    fixed. This script flags those zombie comments so the queue retries
    the public reply only — DM is NOT touched.

Usage:
    Dry-run (default — prints, does not write):
        python -m scripts.repair_specific_rule_disabled_public_replies

    Single comment dry-run:
        python -m scripts.repair_specific_rule_disabled_public_replies \\
            --ig-comment-id 18004285310876247

    Apply (only writes when CONFIRM_REPAIR_SPECIFIC_REPLIES=true):
        CONFIRM_REPAIR_SPECIFIC_REPLIES=true python -m scripts.repair_specific_rule_disabled_public_replies

Safety guarantees:
    - DRY-RUN BY DEFAULT. Writes only when the env var
      CONFIRM_REPAIR_SPECIFIC_REPLIES is exactly the string "true".
    - Does NOT call Instagram. Does NOT resend DM. Does NOT generate
      tokens. Only adjusts DB fields so the existing queue worker
      will retry the public reply step.
    - Skips any comment with reply_provider_response_ok=True (real
      proof exists) or where the matched rule no longer requires a
      public reply (truly DM-only rules).
    - Prints only ids/status/counts. Never prints raw comment text,
      reply text, DM text, tokens, or Graph error bodies.
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

# Allow running as `python repair_specific_rule_disabled_public_replies.py`
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import server  # type: ignore  # noqa: E402


CONFIRM_ENV = 'CONFIRM_REPAIR_SPECIFIC_REPLIES'


def _is_disabled(value):
    return server._status_is_disabled(value) or not value or str(value).lower() == 'skipped'


def _is_success(value):
    return server._status_is_success(value)


async def find_candidates(ig_comment_id_filter=None):
    """Return list of repairable comment summaries (no raw text)."""
    query = {}
    if ig_comment_id_filter:
        query['$or'] = [
            {'ig_comment_id': ig_comment_id_filter},
            {'igCommentId': ig_comment_id_filter},
        ]
    cursor = server.db.comments.find(query)
    candidates = []
    async for doc in cursor:
        if doc.get('reply_provider_response_ok') is True:
            continue
        reply_status = doc.get('reply_status') or doc.get('replyStatus') or ''
        dm_status = doc.get('dm_status') or doc.get('dmStatus') or ''
        if not (_is_disabled(reply_status) and _is_success(dm_status)):
            continue
        rule_id = doc.get('rule_id') or doc.get('ruleId')
        if not rule_id:
            continue
        rule = await server.db.automations.find_one({'id': rule_id})
        if not rule:
            continue
        if not server._automation_public_reply_required(rule):
            continue
        scope = (
            doc.get('matched_rule_scope')
            or doc.get('matchedRuleScope')
            or rule.get('post_scope')
            or ''
        )
        candidates.append({
            'comment_doc_id': doc.get('id'),
            'ig_comment_id': doc.get('ig_comment_id') or doc.get('igCommentId'),
            'media_id': doc.get('media_id') or doc.get('mediaId'),
            'rule_id': rule_id,
            'matched_rule_scope': scope,
            'reply_status': reply_status or 'disabled',
            'dm_status': dm_status,
            'action_status': doc.get('action_status') or doc.get('actionStatus'),
            'attempts': doc.get('attempts') or 0,
        })
    return candidates


async def apply_repair(candidate):
    """Re-queue a comment for public reply only. DM is untouched."""
    now = datetime.utcnow()
    await server.db.comments.update_one(
        {'id': candidate['comment_doc_id']},
        {'$set': {
            'reply_status': 'failed_retryable',
            'replyStatus': 'failed_retryable',
            'reply_failure_reason': 'public_reply_required_not_attempted',
            'reply_failure_retryable': True,
            'reply_skip_reason': 'public_reply_required_not_attempted',
            'action_status': 'failed_retryable',
            'actionStatus': 'failed_retryable',
            'queued': True,
            'next_retry_at': now,
            'updated': now,
        }},
    )


async def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--ig-comment-id', dest='ig_comment_id', default=None,
                        help='Restrict scan to one ig_comment_id.')
    args = parser.parse_args()

    confirm = os.environ.get(CONFIRM_ENV, '').strip().lower() == 'true'
    mode = 'APPLY' if confirm else 'DRY-RUN'

    print(f'[repair] mode={mode}')
    if args.ig_comment_id:
        print(f'[repair] filter ig_comment_id={args.ig_comment_id}')

    candidates = await find_candidates(args.ig_comment_id)
    print(f'[repair] found {len(candidates)} candidate(s)')

    repaired = 0
    for c in candidates:
        print(
            f"[repair] candidate "
            f"comment_doc_id={c['comment_doc_id']} "
            f"ig_comment_id={c['ig_comment_id']} "
            f"media_id={c['media_id']} "
            f"rule_id={c['rule_id']} "
            f"scope={c['matched_rule_scope']} "
            f"reply_status={c['reply_status']} "
            f"dm_status={c['dm_status']} "
            f"action_status={c['action_status']} "
            f"attempts={c['attempts']}"
        )
        if confirm:
            await apply_repair(c)
            repaired += 1
            print(f"[repair]   -> repaired comment_doc_id={c['comment_doc_id']}")
        else:
            print(f"[repair]   -> would repair (dry-run, set {CONFIRM_ENV}=true to apply)")

    print(f'[repair] candidates={len(candidates)} repaired={repaired} mode={mode}')


if __name__ == '__main__':
    asyncio.run(main())
