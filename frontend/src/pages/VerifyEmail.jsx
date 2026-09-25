import React, { useEffect, useState } from 'react';
import { Link, useLocation, useSearchParams } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import LangSwitcher from '../components/LangSwitcher';
import { useTranslation } from '../lib/i18n';
import { authDestination } from '../lib/navigation';
import api from '../lib/api';

export default function VerifyEmail() {
  const { lang } = useTranslation();
  const ar = lang === 'ar';
  const location = useLocation();
  const [params, setParams] = useSearchParams();
  const [token, setToken] = useState(() => params.get('token') || '');
  const [email, setEmail] = useState(location.state?.email || '');
  const [status, setStatus] = useState('idle');
  const [error, setError] = useState('');
  const busy = status === 'verifying' || status === 'sending';
  useEffect(() => {
    if (params.has('token')) {
      const next = new URLSearchParams(params);
      next.delete('token');
      setParams(next, { replace: true });
    }
    // The token stays in memory only. Never auto-consume it on page load.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const submit = async (event) => {
    event.preventDefault();
    setError('');
    const verifying = Boolean(token);
    setStatus(verifying ? 'verifying' : 'sending');
    try {
      if (verifying) {
        await api.post('/auth/verify-email', { token });
        setToken('');
        setStatus('verified');
      } else {
        await api.post('/auth/resend-verification', { email: email.trim() });
        setStatus('sent');
      }
    } catch (err) {
      const detail = err?.response?.data?.detail;
      if (verifying && /invalid_email_verification_token|email_verification_token_(expired|used)/.test(String(detail))) {
        setToken('');
        setError(ar ? 'الرابط غير صالح أو انتهت صلاحيته. اطلب رسالة جديدة بالأسفل.' : 'This link is invalid or expired. Request a new email below.');
      } else {
        setError(ar ? 'تعذّر إكمال الطلب. حاول لاحقًا أو تواصل مع الدعم.' : 'Could not complete the request. Try later or contact support.');
      }
      setStatus('idle');
    }
  };

  return (
    <main className="min-h-screen bg-white px-6 py-10">
      <div className="max-w-md mx-auto">
        <header className="flex items-center justify-between"><Link to="/" className="font-display text-xl font-bold">MyChaat</Link><LangSwitcher /></header>
        <h1 className="mt-12 font-display text-3xl font-bold">{ar ? 'تأكيد البريد الإلكتروني' : 'Verify your email'}</h1>
        <p className="mt-3 text-slate-600">{ar ? 'أكد ملكية بريدك قبل تسجيل الدخول. لن نطلب كلمة المرور في رسالة بريد.' : 'Confirm your email ownership before logging in. We never ask for your password by email.'}</p>
        {status === 'verified' ? (
          <p role="status" className="mt-8 rounded-xl bg-emerald-50 p-4 text-emerald-800">{ar ? 'تم تأكيد بريدك. يمكنك تسجيل الدخول الآن.' : 'Your email is verified. You can log in now.'}</p>
        ) : (
          <form onSubmit={submit} className="mt-8 space-y-5">
            {!token && <div className="space-y-2"><Label htmlFor="verification-email">{ar ? 'البريد الإلكتروني' : 'Email address'}</Label><Input id="verification-email" type="email" autoComplete="email" required maxLength={254} dir="ltr" value={email} onChange={e => setEmail(e.target.value)} /></div>}
            <Button type="submit" disabled={busy} className="w-full rounded-xl bg-slate-900 text-white">{busy ? (ar ? 'جارٍ التنفيذ...' : 'Please wait...') : token ? (ar ? 'تأكيد بريدي' : 'Verify my email') : (ar ? 'إرسال رابط التأكيد' : 'Send verification link')}</Button>
          </form>
        )}
        {error && <p role="alert" className="mt-4 text-sm text-red-700">{error}</p>}
        {status === 'sent' && <p role="status" className="mt-4 text-sm text-slate-600">{ar ? 'لو الحساب يحتاج تأكيدًا، ستصلك رسالة. راجع أيضًا مجلد الرسائل غير المرغوب فيها.' : 'If this account needs verification, you will receive an email. Check your spam folder too.'}</p>}
        <div className="mt-8 flex justify-between text-sm font-medium"><Link to={authDestination('/login', location.state?.returnTo)} className="underline">{ar ? 'تسجيل الدخول' : 'Log in'}</Link><Link to="/support" className="underline">{ar ? 'الدعم' : 'Contact support'}</Link></div>
      </div>
    </main>
  );
}
