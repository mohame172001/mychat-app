/**
 * Phase 2.18Y — user-friendly labels for the Instagram DM failure
 * reasons emitted by backend `classify_instagram_send_error`.
 * Bilingual EN/AR copy is selected based on document direction or
 * the persisted `mychat_lang` preference.
 */

function isArabic() {
  try {
    if (typeof document !== 'undefined' && document.documentElement?.lang === 'ar') return true;
    if (typeof localStorage !== 'undefined' && localStorage.getItem('mychat_lang') === 'ar') return true;
  } catch (_) { /* ignore */ }
  return false;
}

const FAILURE_REASON_MAP = {
  en: {
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
  },
  ar: {
    messaging_window_expired: {
      label: 'تعذّر الوصول للمستلم',
      detail:
        'لن يُسلِّم Instagram هذه الرسالة لأن المستخدم لم يراسل حسابك خلال آخر 24 ساعة، أو لأنه قيّد الرسائل الترويجية. ' +
        'هذا أمر طبيعي — الأتمتة لا تصل إلا للمستخدمين الذين بدؤوا المحادثة.',
      tone: 'expected',
    },
    recipient_unavailable: {
      label: 'تعذّر الوصول للمستلم',
      detail:
        'لم يستطع Instagram الوصول إلى هذا المستخدم. ربما حذف حسابه أو حظر حسابك أو لم تعد المحادثة متاحة.',
      tone: 'expected',
    },
    messaging_not_allowed: {
      label: 'الإرسال غير مسموح',
      detail: 'رفض Instagram الرسالة لأن حسابك خارج نافذة المراسلة المسموح بها مع هذا المستخدم.',
      tone: 'expected',
    },
    user_blocked_messages: {
      label: 'حظر المستخدم رسائلك',
      detail: 'هذا المستخدم حظر الرسائل الترويجية من حسابك.',
      tone: 'expected',
    },
    rate_limited: {
      label: 'تجاوز الحدّ',
      detail: 'قيّد Instagram الإرسال مؤقّتاً. ستُعاد المحاولة تلقائياً خلال دقائق.',
      tone: 'transient',
    },
    temporary_graph_error: {
      label: 'خطأ مؤقّت',
      detail: 'أعاد Instagram خطأً مؤقّتاً. ستُعاد المحاولة تلقائياً.',
      tone: 'transient',
    },
    permission_error: {
      label: 'خطأ في الصلاحيات',
      detail:
        'يفتقد رمز الوصول للصلاحية المطلوبة لإرسال الرسائل. أعد ربط حساب Instagram من الإعدادات.',
      tone: 'unknown',
    },
    missing_access_token_or_ig_user_id: {
      label: 'الحساب غير مربوط',
      detail: 'لا يوجد ربط Instagram نشط. أعد الربط من الإعدادات.',
      tone: 'unknown',
    },
    unknown_graph_error: {
      label: 'خطأ غير معروف',
      detail:
        'أعاد Instagram خطأً لم نتعرّف عليه بعد. تم إبلاغ الفريق — يرجى المحاولة لاحقاً.',
      tone: 'unknown',
    },
  },
};

export function describeDmFailureReason(reason) {
  if (!reason || typeof reason !== 'string') return null;
  const ar = isArabic();
  const map = FAILURE_REASON_MAP[ar ? 'ar' : 'en'];
  const entry = map[reason];
  if (entry) return { reason, ...entry };
  // Unknown reason string — surface raw key with a generic explanation.
  return {
    reason,
    label: ar ? 'فشل التسليم' : 'Delivery failed',
    detail: ar
      ? `رفض Instagram الرسالة (السبب: ${reason}).`
      : `Instagram declined the DM (reason: ${reason}).`,
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
