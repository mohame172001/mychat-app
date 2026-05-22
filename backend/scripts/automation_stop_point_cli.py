"""Local/admin CLI for the Instagram automation stop-point summary.

Runs against the configured production MongoDB (read-only) and prints the
same sanitized per-account stop-point summary the protected admin endpoint
``/api/admin/instagram/automation-stop-point`` returns.

Why this script exists
----------------------
The endpoint is admin-protected and not always practical to call from a
browser session during incident triage. This CLI gives the operator (or
an on-call engineer running ``railway run``) a way to read the same
sanitized summary directly, without exposing secrets, without manual
IDs, and without spinning up a frontend diagnostics page.

Output contract — IDENTICAL to the HTTP endpoint
------------------------------------------------
For each isActive linked Instagram account on the configured DB:
  - username
  - instagram_account_id_partial
  - last_comment_reached_backend
  - source                       (webhook / polling / none)
  - account_resolved
  - polling_scanned_account
  - rules_loaded
  - rules_loaded_count
  - rule_matched
  - selected_media_matched
  - reply_attempted
  - dm_attempted
  - session_attempted
  - exact_stop_reason
  - last_send_error              (sanitized: stage / reason / error_code / error_message)
  - next_recommended_action      (one-sentence operator-facing recommendation)
  - last_event_at

Privacy contract
----------------
Reads the same partial-redacted fields that
``_record_instagram_automation_event`` writes. Never prints tokens,
Authorization headers, raw webhook payloads, full comment text, or full
DM text. Never logs the MONGO_URL.

Usage
-----
::

    MONGO_URL='mongodb+srv://...' DB_NAME=mychat \
      python backend/scripts/automation_stop_point_cli.py

    # Single-account drilldown:
    MONGO_URL=...  DB_NAME=mychat  USERNAME=mogehad17 \
      python backend/scripts/automation_stop_point_cli.py

    # Pretty JSON output (default) vs compact table:
    OUTPUT=table python backend/scripts/automation_stop_point_cli.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def _read_env() -> Dict[str, str]:
    mongo_url = os.environ.get('MONGO_URL') or os.environ.get('MONGODB_URI') or ''
    db_name = os.environ.get('DB_NAME') or os.environ.get('MONGO_DB_NAME') or 'mychat'
    if not mongo_url:
        print(
            'ERROR: MONGO_URL (or MONGODB_URI) is required.\n'
            'Set it to your production read access connection string, e.g.\n'
            "  MONGO_URL='mongodb+srv://...' python backend/scripts/automation_stop_point_cli.py",
            file=sys.stderr,
        )
        sys.exit(2)
    return {
        'mongo_url': mongo_url,
        'db_name': db_name,
        'username': os.environ.get('USERNAME') or os.environ.get('IG_USERNAME') or '',
        'output': (os.environ.get('OUTPUT') or 'json').lower(),
        'limit_per_account': int(os.environ.get('LIMIT_PER_ACCOUNT') or '100'),
    }


def _safe_partial_identifier(value: Any) -> Optional[str]:
    """Mirror the backend helper so this script never logs full ids.
    Same shape as _safe_partial_identifier in server.py: first 3 chars +
    '...' + last 3 chars, or '***<len>' for short values."""
    if value is None:
        return None
    s = str(value)
    if not s:
        return None
    if len(s) <= 6:
        return '***' + str(len(s))
    return f'{s[:3]}...{s[-3:]}'


def _server_module():
    """Import server.py without triggering FastAPI startup hooks.
    The module-level FastAPI app is created on import; we just need
    the pure helpers ``summarize_account_automation_stop_point`` and
    ``_automation_flight_username_key``."""
    backend_dir = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(backend_dir))
    # Force test-friendly env so import doesn't reject missing secrets.
    os.environ.setdefault('JWT_SECRET', 'cli-readonly-noop')
    os.environ.setdefault('BACKEND_PUBLIC_URL', 'https://localhost')
    os.environ.setdefault('FRONTEND_URL', 'https://localhost')
    os.environ.setdefault('IG_APP_ID', '0')
    os.environ.setdefault('IG_APP_SECRET', 'noop')
    os.environ.setdefault('CRON_SECRET', 'noop')
    os.environ.setdefault('META_VERIFY_TOKEN', 'noop')
    import server  # type: ignore  # noqa: E402
    return server


async def _run() -> int:
    env = _read_env()
    server = _server_module()

    # Connect to Mongo with the configured URL. We swap server.db to
    # point at the production database for the duration of this call
    # so summarize_account_automation_stop_point reads from there.
    from motor.motor_asyncio import AsyncIOMotorClient  # noqa: WPS433
    client = AsyncIOMotorClient(env['mongo_url'])
    db_handle = client[env['db_name']]
    original_db = getattr(server, 'db', None)
    server.db = db_handle
    try:
        if env['username']:
            username_key = server._automation_flight_username_key(env['username'])
            summary = await server.summarize_account_automation_stop_point(
                username_key, limit=env['limit_per_account'],
            )
            account = await db_handle.instagram_accounts.find_one({
                '$or': [
                    {'username': env['username'].lstrip('@')},
                    {'username': env['username']},
                ],
            }, {'instagramAccountId': 1, 'igUserId': 1, 'username': 1, 'userId': 1})
            summary['instagram_account_id_partial'] = _safe_partial_identifier(
                (account or {}).get('instagramAccountId')
                or (account or {}).get('igUserId')
            )
            summary['owner_user_id_partial'] = _safe_partial_identifier(
                (account or {}).get('userId')
            )
            _emit([summary], env['output'])
            return 0

        cursor = db_handle.instagram_accounts.find(
            {'isActive': {'$ne': False}},
            {'instagramAccountId': 1, 'igUserId': 1, 'username': 1, 'userId': 1,
             'isActive': 1, 'connectionValid': 1},
        ).limit(200)
        accounts: List[Dict[str, Any]] = []
        async for row in cursor:
            accounts.append(row)
        summaries: List[Dict[str, Any]] = []
        for account in accounts:
            username = account.get('username') or ''
            username_key = server._automation_flight_username_key(username)
            if not username_key:
                continue
            summary = await server.summarize_account_automation_stop_point(
                username_key, limit=env['limit_per_account'],
            )
            summary['username_displayed'] = username
            summary['instagram_account_id_partial'] = _safe_partial_identifier(
                account.get('instagramAccountId') or account.get('igUserId')
            )
            summary['owner_user_id_partial'] = _safe_partial_identifier(account.get('userId'))
            summary['account_connection_valid'] = bool(account.get('connectionValid'))
            summaries.append(summary)
        _emit(summaries, env['output'])
        return 0
    finally:
        if original_db is not None:
            server.db = original_db
        try:
            client.close()
        except Exception:
            pass


def _emit(summaries: List[Dict[str, Any]], output: str) -> None:
    if output == 'table':
        _emit_table(summaries)
    else:
        print(json.dumps({'count': len(summaries), 'summaries': summaries},
                         default=str, indent=2))


def _emit_table(summaries: List[Dict[str, Any]]) -> None:
    if not summaries:
        print('(no linked Instagram accounts found)')
        return
    for s in summaries:
        print('=' * 72)
        print(f"username:       @{s.get('username') or s.get('username_displayed') or '-'}")
        print(f"ig_account_id:  {s.get('instagram_account_id_partial') or '-'}")
        print(f"comment seen:   {s.get('last_comment_reached_backend')}    source={s.get('source')}")
        print(f"polling scan:   {s.get('polling_scanned_account')}")
        print(f"account ok:     {s.get('account_resolved')}")
        print(f"rules loaded:   {s.get('rules_loaded')} (count={s.get('rules_loaded_count')})")
        print(f"rule matched:   {s.get('rule_matched')}    selected_media_matched={s.get('selected_media_matched')}")
        print(f"reply attempt:  {s.get('reply_attempted')}   dm_attempt={s.get('dm_attempted')}   session_attempt={s.get('session_attempted')}")
        print(f"stop_reason:    {s.get('exact_stop_reason')}")
        err = s.get('last_send_error')
        if err:
            print(f"last_send_error: stage={err.get('stage')} reason={err.get('reason')} "
                  f"code={err.get('error_code')} message={(err.get('error_message') or '')[:80]}")
        print('next action:    ' + (s.get('next_recommended_action') or '-'))
        print()


if __name__ == '__main__':
    try:
        sys.exit(asyncio.run(_run()))
    except KeyboardInterrupt:
        sys.exit(130)
