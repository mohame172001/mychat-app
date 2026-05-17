/**
 * Phase 2.18Y — user-friendly labels for the Instagram DM failure
 * reasons emitted by backend `classify_instagram_send_error`.
 *
 * The raw `reason` strings (e.g. `messaging_window_expired`) come from
 * Meta's Graph API; mapping them here keeps every UI surface (admin
 * failures table, user-facing comment detail, dashboard tooltip) in
 * sync without duplicating copy.
 *
 * Each entry returns:
 *   - label  → short, sentence-case (for chips / table cells)
 *   - detail → one-line plain-English explanation
 *   - tone   → 'expected' | 'transient' | 'unknown'
 *              expected  = normal Instagram policy outcome, no action
 *              transient = retry will likely succeed
 *              unknown   = needs operator investigation
 */
const FAILURE_REASON_MAP = {
  messaging_window_expired: {
    label: 'Recipient unreachable',
    detail:
      "Instagram won't deliver this DM because the user hasn't messaged your business in the past 24 hours, " +
      'or they restrict promotional messages. This is normal — automation only reaches users who started a conversation.',
    tone: 'expected',
  },
  recipient_unavailable: {
    label: 'Recipient unreachable',
    detail:
      "Instagram couldn't reach this user. They may have deleted their account, blocked your business, or " +
      'their conversation thread is no longer accessible.',
    tone: 'expected',
  },
  messaging_not_allowed: {
    label: 'Messaging not allowed',
    detail:
      'Instagram declined the DM because your business is currently outside the allowed messaging window with this user.',
    tone: 'expected',
  },
  user_blocked_messages: {
    label: 'User blocked you',
    detail: 'This user has blocked promotional messages from your business.',
    tone: 'expected',
  },
  rate_limited: {
    label: 'Rate limited',
    detail: 'Instagram throttled the send. It will retry automatically in a few minutes.',
    tone: 'transient',
  },
  temporary_graph_error: {
    label: 'Temporary error',
    detail: 'Instagram returned a transient error. It will retry automatically.',
    tone: 'transient',
  },
  permission_error: {
    label: 'Permission error',
    detail:
      'The access token is missing the permission required to send DMs. Reconnect this Instagram account in Settings.',
    tone: 'unknown',
  },
  missing_access_token_or_ig_user_id: {
    label: 'Account not connected',
    detail: 'No active Instagram connection. Reconnect in Settings.',
    tone: 'unknown',
  },
  unknown_graph_error: {
    label: 'Unknown error',
    detail:
      "Instagram returned an error we don't recognise yet. The team has been notified — please check back later.",
    tone: 'unknown',
  },
};

export function describeDmFailureReason(reason) {
  if (!reason || typeof reason !== 'string') return null;
  const entry = FAILURE_REASON_MAP[reason];
  if (entry) return { reason, ...entry };
  // Unknown reason string — surface raw key with a generic explanation.
  return {
    reason,
    label: 'Delivery failed',
    detail: `Instagram declined the DM (reason: ${reason}).`,
    tone: 'unknown',
  };
}

export function dmFailureToneClasses(tone) {
  switch (tone) {
    case 'expected':
      return 'text-slate-600 bg-slate-100 border-slate-200';
    case 'transient':
      return 'text-amber-700 bg-amber-50 border-amber-200';
    case 'unknown':
    default:
      return 'text-rose-700 bg-rose-50 border-rose-200';
  }
}
