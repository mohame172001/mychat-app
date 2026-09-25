import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { Mail, ArrowLeft } from 'lucide-react';
import { useTranslation } from '../lib/i18n';
import { SUPPORT_EMAIL, buildSupportMailtoHref, handleContactClick } from '../lib/contactSupport';
import LangSwitcher from '../components/LangSwitcher';
import api from '../lib/api';

export default function Support() {
  const { lang } = useTranslation();
  const ar = lang === 'ar';
  const [form, setForm] = useState({ email: '', category: 'account', message: '', website: '' });
  const [state, setState] = useState('idle');
  const [error, setError] = useState('');
  const update = (event) => setForm({ ...form, [event.target.name]: event.target.value });
  const submit = async (event) => {
    event.preventDefault();
    if (state === 'sending') return;
    setState('sending');
    setError('');
    try {
      const { data } = await api.post('/support', { ...form, email: form.email.trim(), message: form.message.trim() });
      if (data?.status !== 'accepted') throw new Error('not accepted');
      setState('accepted');
      setForm({ email: '', category: 'account', message: '', website: '' });
    } catch (err) {
      setState('idle');
      setError(err?.response?.status === 429
        ? (ar ? 'وصلت للحد الأقصى للمحاولات. حاول مرة أخرى لاحقًا.' : 'Too many requests. Please try again later.')
        : (ar ? 'تعذر إرسال الرسالة الآن. حاول لاحقًا أو راسل بريد الدعم مباشرة.' : 'We could not send your message. Try again later or email support directly.'));
    }
  };
  return (
    <main className="min-h-screen bg-slate-50 px-5 py-10">
      <div className="max-w-2xl mx-auto">
        <header className="flex items-center justify-between mb-12">
          <Link to="/" className="font-display text-xl font-bold">MyChaat</Link>
          <LangSwitcher />
        </header>
        <section className="bg-white border border-slate-200 rounded-2xl p-6 sm:p-10">
          <Mail className="w-7 h-7 text-blue-600 mb-5" />
          <h1 className="font-display text-3xl font-bold">{ar ? 'كيف نقدر نساعدك؟' : 'How can we help?'}</h1>
          <p className="mt-3 text-slate-600">{ar ? 'للمساعدة في حسابك أو ربط Instagram أو الأتمتة، تواصل مع فريق MyChaat.' : 'Contact MyChaat for help with your account, Instagram connection, or automations.'}</p>
          <form onSubmit={submit} className="mt-7 space-y-5" dir={ar ? 'rtl' : 'ltr'}>
            <label className="block font-medium">{ar ? 'بريدك الإلكتروني للرد' : 'Your email for our reply'}
              <input name="email" type="email" dir="ltr" autoComplete="email" required maxLength={254} value={form.email} onChange={update} className="mt-2 w-full rounded-xl border border-slate-300 p-3" />
            </label>
            <label className="block font-medium">{ar ? 'نوع المشكلة' : 'Issue category'}
              <select name="category" value={form.category} onChange={update} className="mt-2 w-full rounded-xl border border-slate-300 p-3">
                {[['account', 'الحساب وتسجيل الدخول', 'Account and sign-in'], ['instagram', 'ربط Instagram', 'Instagram connection'], ['automation', 'الأتمتة', 'Automation'], ['billing', 'الاشتراك والفواتير', 'Billing'], ['privacy', 'الخصوصية والبيانات', 'Privacy and data'], ['other', 'أخرى', 'Other']].map(([value, arabic, english]) => <option key={value} value={value}>{ar ? arabic : english}</option>)}
              </select>
            </label>
            <label className="block font-medium">{ar ? 'اشرح المشكلة' : 'Describe your issue'}
              <textarea name="message" required minLength={20} maxLength={5000} rows={6} value={form.message} onChange={update} className="mt-2 w-full rounded-xl border border-slate-300 p-3" />
            </label>
            <div hidden aria-hidden="true"><input name="website" tabIndex={-1} autoComplete="off" value={form.website} onChange={update} /></div>
            <p className="text-sm text-slate-500">{ar ? 'تُرسل بيانات رسالتك وبريدك عبر Resend لفريق دعم MyChaat. لا ترسل كلمات مرور أو رموز دخول.' : 'Your message and email are sent through Resend to MyChaat support. Do not include passwords or sign-in codes.'}</p>
            {error && <p role="alert" className="text-red-700">{error}</p>}
            {state === 'accepted' && <p role="status" className="text-green-700">{ar ? 'تم قبول رسالتك للإرسال إلى فريق الدعم. سنرد على البريد الذي أدخلته.' : 'Your message was accepted for delivery to support. We will reply to the email you provided.'}</p>}
            <button type="submit" disabled={state === 'sending'} className="rounded-xl bg-slate-900 px-6 py-3 font-semibold text-white disabled:opacity-50">{state === 'sending' ? (ar ? 'جارٍ الإرسال…' : 'Sending…') : (ar ? 'إرسال للدعم' : 'Send to support')}</button>
          </form>
          <a href={buildSupportMailtoHref()} onClick={handleContactClick} className="mt-7 inline-block font-semibold text-blue-700 underline break-all" dir="ltr">{SUPPORT_EMAIL}</a>
          <p className="mt-6 rounded-xl bg-slate-50 p-4 text-sm text-slate-600">{ar ? 'اشرح المشكلة وأرفق لقطة شاشة بدون بيانات حساسة. لا ترسل كلمة المرور أو رموز الدخول أو مفاتيح API.' : 'Describe the issue and include a screenshot with sensitive information hidden. Never send passwords, sign-in codes, or API keys.'}</p>
          <nav aria-label={ar ? 'روابط المساعدة' : 'Help links'} className="mt-8 grid sm:grid-cols-2 gap-4 text-sm font-medium">
            <Link to="/login" className="underline">{ar ? 'تسجيل الدخول' : 'Log in'}</Link>
            <Link to="/forgot-password" className="underline">{ar ? 'استعادة كلمة المرور' : 'Reset your password'}</Link>
            <Link to="/verify-email" className="underline">{ar ? 'تأكيد البريد الإلكتروني' : 'Verify your email'}</Link>
            <Link to="/status" className="underline">{ar ? 'حالة الخدمة' : 'Service status'}</Link>
            <Link to="/data-deletion" className="underline">{ar ? 'حذف البيانات' : 'Data deletion'}</Link>
            <Link to="/privacy" className="underline">{ar ? 'الخصوصية' : 'Privacy policy'}</Link>
          </nav>
        </section>
        <Link to="/" className="mt-6 inline-flex items-center gap-2 text-sm text-slate-600"><ArrowLeft className="h-4 w-4" />{ar ? 'العودة للرئيسية' : 'Back to home'}</Link>
      </div>
    </main>
  );
}
