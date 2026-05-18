// Phase 2.18L: clear, actionable messages for every Instagram-connect
// failure mode the backend can return. Bilingual EN/AR copy.

function isArabic() {
  try {
    if (typeof document !== 'undefined' && document.documentElement?.lang === 'ar') return true;
    if (typeof localStorage !== 'undefined' && localStorage.getItem('mychat_lang') === 'ar') return true;
  } catch (_) { /* ignore */ }
  return false;
}

const MESSAGES = {
  en: {
    instagram_account_already_connected:
      'This Instagram account is already linked to a different MyChat account. ' +
      'Sign in to that account and disconnect it first, or contact support if you ' +
      'own this Instagram account and lost access to the original MyChat account.',
    oauth_denied:
      'You declined the Instagram permissions, so we could not finish linking the account. ' +
      'Click Connect again and accept the requested permissions.',
    access_denied:
      'You declined the Instagram permissions. Click Connect and accept the requested permissions to continue.',
    invalid_state:
      'The Instagram sign-in link expired before you finished. Click Connect again to start a fresh link.',
    missing_code:
      'Instagram did not return an authorization code. Click Connect again — this almost always fixes it.',
    token_exchange_failed:
      'Instagram refused to issue an access token. Click Connect again, and make sure you log in with the SAME Instagram username you intended to link.',
    token_cannot_call_graph_me:
      'Instagram gave us a token but rejected our profile lookup. The account must be a Business or Creator account with the requested permissions accepted.',
    server_error:
      'Something went wrong while finishing the Instagram link. Click Connect again. If it still fails, sign out, sign back in, and retry — and contact support if it persists.',
    generic: 'Instagram connection failed. Please try again.',
    withReason: (reason) => `Instagram connection failed (${reason}). Click Connect again to retry.`,
    fallback: 'Failed',
  },
  ar: {
    instagram_account_already_connected:
      'حساب Instagram هذا مربوط بحساب مايتشات آخر بالفعل. سجّل الدخول إلى ذلك الحساب وافصله أولاً، أو تواصل مع الدعم إذا كنت تمتلك حساب Instagram وفقدت الوصول إلى حساب مايتشات الأصلي.',
    oauth_denied:
      'لم توافق على صلاحيات Instagram، لذلك لم نتمكّن من إتمام الربط. اضغط "ربط" مرّة أخرى ووافق على الصلاحيات المطلوبة.',
    access_denied:
      'لم توافق على صلاحيات Instagram. اضغط "ربط" ووافق على الصلاحيات المطلوبة لإتمام العملية.',
    invalid_state:
      'انتهت صلاحية رابط تسجيل الدخول قبل إتمام العملية. اضغط "ربط" مرّة أخرى لبدء رابط جديد.',
    missing_code:
      'لم يُرجع Instagram رمز التفويض. اضغط "ربط" مرّة أخرى — هذا يحلّ المشكلة في الغالب.',
    token_exchange_failed:
      'رفض Instagram إصدار رمز الوصول. اضغط "ربط" مجدّداً، وتأكّد من تسجيل الدخول بنفس اسم المستخدم الذي تريد ربطه.',
    token_cannot_call_graph_me:
      'أعطانا Instagram رمزاً لكنه رفض جلب الملف الشخصي. يجب أن يكون الحساب تجارياً أو إبداعياً مع قبول الصلاحيات المطلوبة.',
    server_error:
      'حدث خطأ أثناء إتمام ربط Instagram. اضغط "ربط" مرّة أخرى. إذا استمرّ الخطأ، سجّل الخروج وادخل من جديد، وتواصل مع الدعم إذا تكرّرت المشكلة.',
    generic: 'تعذّر ربط Instagram. حاول مرّة أخرى.',
    withReason: (reason) => `تعذّر ربط Instagram (${reason}). اضغط "ربط" مجدّداً للمحاولة.`,
    fallback: 'فشل العملية',
  },
};

export const instagramErrorMessage = (reason) => {
  const m = MESSAGES[isArabic() ? 'ar' : 'en'];
  if (!reason) return m.generic;
  return m[reason] || m.withReason(reason);
};

export const instagramConnectExceptionMessage = (error, fallback) => {
  const m = MESSAGES[isArabic() ? 'ar' : 'en'];
  const fb = fallback || m.fallback;
  const detail = error?.response?.data?.detail;
  if (detail?.code) return instagramErrorMessage(detail.code);
  if (typeof detail === 'string') return instagramErrorMessage(detail);
  if (typeof detail?.message === 'string') return detail.message;
  return fb;
};
