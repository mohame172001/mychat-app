const AUTH_MESSAGES = {
  en: {
    account_suspended: 'Your account is suspended. Contact support.',
    account_deleted: 'Your account has been deleted or disabled. Contact support.',
    invalid_credentials: 'Invalid email or password.',
    email_verification_required: 'Please verify your email before continuing.',
    email_verification_not_configured: 'Email verification is not configured. Contact support.',
    session_revoked: 'Your session expired. Please sign in again.',
    generic: 'Could not sign in. Please try again.',
  },
  ar: {
    account_suspended: 'تم إيقاف حسابك مؤقتًا. تواصل مع الدعم.',
    account_deleted: 'تم حذف الحساب أو تعطيله. تواصل مع الدعم.',
    invalid_credentials: 'البريد الإلكتروني أو كلمة المرور غير صحيحة.',
    email_verification_required: 'يرجى توثيق بريدك الإلكتروني قبل المتابعة.',
    email_verification_not_configured: 'توثيق البريد الإلكتروني غير مُهيّأ. تواصل مع الدعم.',
    session_revoked: 'انتهت صلاحية الجلسة. يرجى تسجيل الدخول من جديد.',
    generic: 'تعذر تسجيل الدخول. حاول مرة أخرى.',
  },
};

function normalizeDetail(detail) {
  if (!detail) return '';
  if (typeof detail === 'string') return detail;
  if (typeof detail === 'object') {
    return String(detail.detail || detail.error || detail.code || detail.message || '');
  }
  return String(detail);
}

export function authErrorCode(detail) {
  const s = normalizeDetail(detail).toLowerCase();
  if (!s) return 'generic';
  if (s.includes('account_suspended')) return 'account_suspended';
  if (s.includes('account_deleted')) return 'account_deleted';
  if (s.includes('email_verification_required')) return 'email_verification_required';
  if (s.includes('email_verification_not_configured')) return 'email_verification_not_configured';
  if (s.includes('session_revoked')) return 'session_revoked';
  if (
    s.includes('invalid_credentials')
    || s.includes('invalid username or password')
    || s.includes('invalid email or password')
  ) {
    return 'invalid_credentials';
  }
  return 'generic';
}

export function authLocale(locale) {
  if (locale) {
    const raw = String(locale).toLowerCase();
    return raw.startsWith('ar') ? 'ar' : 'en';
  }
  try {
    if (typeof document !== 'undefined' && document.documentElement?.lang === 'ar') return 'ar';
    if (typeof localStorage !== 'undefined' && localStorage.getItem('mychat_lang') === 'ar') return 'ar';
  } catch (_) { /* ignore */ }
  const raw = String((typeof navigator !== 'undefined' ? navigator.language : 'en') || 'en').toLowerCase();
  return raw.startsWith('ar') ? 'ar' : 'en';
}

export function authErrorMessage(detail, options = {}) {
  const lang = authLocale(options.locale);
  const code = authErrorCode(detail);
  return (AUTH_MESSAGES[lang] || AUTH_MESSAGES.en)[code] || AUTH_MESSAGES[lang].generic;
}

export function authErrorMessageFromApiError(err, options = {}) {
  const data = err?.response?.data || {};
  return authErrorMessage(data.detail || data.error || data.code || err?.message, options);
}
