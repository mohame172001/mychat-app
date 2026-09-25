import React from 'react';
import { Link } from 'react-router-dom';
import { Mail, ArrowLeft } from 'lucide-react';
import { useTranslation } from '../lib/i18n';
import { SUPPORT_EMAIL, buildSupportMailtoHref, handleContactClick } from '../lib/contactSupport';
import LangSwitcher from '../components/LangSwitcher';

export default function Support() {
  const { lang } = useTranslation();
  const ar = lang === 'ar';
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
          <a href={buildSupportMailtoHref()} onClick={handleContactClick} className="mt-7 inline-block font-semibold text-blue-700 underline break-all" dir="ltr">{SUPPORT_EMAIL}</a>
          <p className="mt-3 text-sm text-slate-500">{ar ? 'الزر يفتح تطبيق البريد وينسخ العنوان. لو التطبيق لم يفتح، الصق العنوان في بريدك وأرسل الرسالة.' : 'This opens your email app and copies the address. If no app opens, paste the address into your email service and send your message.'}</p>
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
