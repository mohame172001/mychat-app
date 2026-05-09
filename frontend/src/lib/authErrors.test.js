import {
  authErrorCode,
  authErrorMessage,
  authErrorMessageFromApiError,
} from './authErrors';

describe('auth login error mapping', () => {
  test('maps suspended account errors to professional English and Arabic messages', () => {
    expect(authErrorCode('account_suspended')).toBe('account_suspended');
    expect(authErrorMessage('account_suspended', { locale: 'en-US' }))
      .toBe('Your account is suspended. Contact support.');
    expect(authErrorMessage('account_suspended', { locale: 'ar-EG' }))
      .toBe('تم إيقاف حسابك مؤقتًا. تواصل مع الدعم.');
  });

  test('maps deleted account errors to professional English and Arabic messages', () => {
    expect(authErrorCode({ detail: 'account_deleted' })).toBe('account_deleted');
    expect(authErrorMessage('account_deleted', { locale: 'en' }))
      .toBe('Your account has been deleted or disabled. Contact support.');
    expect(authErrorMessage('account_deleted', { locale: 'ar' }))
      .toBe('تم حذف الحساب أو تعطيله. تواصل مع الدعم.');
  });

  test('maps invalid credentials without leaking account status', () => {
    expect(authErrorMessage('Invalid username or password', { locale: 'en' }))
      .toBe('Invalid email or password.');
    expect(authErrorMessage('invalid_credentials', { locale: 'ar' }))
      .toBe('البريد الإلكتروني أو كلمة المرور غير صحيحة.');
  });

  test('falls back safely for unknown login errors', () => {
    expect(authErrorMessage('unexpected', { locale: 'en' }))
      .toBe('Could not sign in. Please try again.');
    expect(authErrorMessage('', { locale: 'ar' }))
      .toBe('تعذر تسجيل الدخول. حاول مرة أخرى.');
  });

  test('extracts error code from axios-style response data', () => {
    const err = { response: { data: { detail: 'account_suspended' } } };
    expect(authErrorMessageFromApiError(err, { locale: 'en' }))
      .toBe('Your account is suspended. Contact support.');
  });
});

