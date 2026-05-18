import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { PasswordInput } from '../components/ui/password-input';
import { Label } from '../components/ui/label';
import { MessageCircle, Check } from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import { toast } from 'sonner';
import GoogleSignInButton from '../components/auth/GoogleSignInButton';
import { authErrorMessageFromApiError } from '../lib/authErrors';
import { useTranslation } from '../lib/i18n';
import LangSwitcher from '../components/LangSwitcher';

const Signup = () => {
  const [username, setUsername] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const { signup } = useAuth();
  const navigate = useNavigate();
  const { t, lang } = useTranslation();

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!username || !email || !password) {
      toast.error(lang === 'ar' ? 'يرجى تعبئة جميع الحقول' : 'Please fill in all fields');
      return;
    }
    if (password.length < 8) {
      toast.error(lang === 'ar' ? 'يجب ألا تقلّ كلمة المرور عن ٨ أحرف' : 'Password must be at least 8 characters');
      return;
    }
    setLoading(true);
    try {
      await signup(username, email, password);
      toast.success(lang === 'ar' ? 'تم إنشاء الحساب — أهلاً بك في MyChat' : 'Account created! Welcome to mychat');
      navigate('/app');
    } catch (err) {
      toast.error(authErrorMessageFromApiError(err) || (lang === 'ar' ? 'تعذّر إنشاء الحساب' : 'Signup failed'));
    } finally {
      setLoading(false);
    }
  };

  const sidePerks = lang === 'ar'
    ? [
        'اربط Instagram بأمان',
        'صمّم قواعد للتعليقات',
        'أرسل ردوداً ورسائل مهيّأة مسبقاً',
        'تابع النشاط الفعلي من لوحة التحكم',
      ]
    : [
        'Connect Instagram securely',
        'Create comment rules',
        'Send configured DM replies',
        'Review real activity in the dashboard',
      ];

  return (
    <div className="min-h-screen grid md:grid-cols-2 bg-white">
      <div className="flex flex-col p-8 md:p-12">
        <div className="flex items-center justify-between">
          <Link to="/" className="flex items-center gap-2">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
              <MessageCircle className="w-5 h-5 text-white" strokeWidth={2.5} />
            </div>
            <span className="text-xl font-bold font-display">mychat</span>
          </Link>
          <LangSwitcher />
        </div>
        <div className="flex-1 flex items-center justify-center">
          <div className="w-full max-w-sm">
            <h1 className="font-display text-3xl md:text-4xl font-extrabold tracking-tight">{t('auth.signup.title')}</h1>
            <p className="mt-2 text-slate-600">{t('auth.signup.subtitle')}</p>
            <div className="mt-6">
              <GoogleSignInButton redirectTo="/app" />
            </div>
            <form onSubmit={handleSubmit} className="mt-6 space-y-4">
              <div className="space-y-2">
                <Label htmlFor="username">{t('auth.signup.username')}</Label>
                <Input id="username" autoComplete="username" value={username} onChange={e => setUsername(e.target.value)} className="h-12 rounded-xl" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="email">{t('auth.signup.email')}</Label>
                <Input id="email" type="email" autoComplete="email" value={email} onChange={e => setEmail(e.target.value)} className="h-12 rounded-xl" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="password">{t('auth.signup.password')}</Label>
                <PasswordInput id="password" autoComplete="new-password" placeholder={t('auth.signup.passwordHint')} value={password} onChange={e => setPassword(e.target.value)} inputClassName="h-12 rounded-xl" />
              </div>
              <Button type="submit" disabled={loading} className="w-full h-12 rounded-xl bg-slate-900 hover:bg-slate-800 text-white">
                {loading ? t('common.loading') : t('auth.signup.submit')}
              </Button>
            </form>
            <p className="mt-6 text-sm text-center text-slate-600">
              {t('auth.signup.haveAccount')} <Link to="/login" className="font-semibold text-slate-900 hover:underline">{t('auth.signup.loginLink')}</Link>
            </p>
            <p className="mt-4 text-xs text-center text-slate-500">
              {lang === 'ar' ? (
                <>
                  بإنشاء الحساب فأنت توافق على{' '}
                  <Link to="/terms" className="hover:text-slate-900 underline">الشروط والأحكام</Link>
                  {' '}و{' '}
                  <Link to="/privacy" className="hover:text-slate-900 underline">سياسة الخصوصية</Link>.
                  {' '}تعليمات حذف البيانات{' '}
                  <Link to="/data-deletion" className="hover:text-slate-900 underline">من هنا</Link>.
                </>
              ) : (
                <>
                  By signing up you agree to our{' '}
                  <Link to="/terms" className="hover:text-slate-900 underline">Terms</Link>
                  {' '}and{' '}
                  <Link to="/privacy" className="hover:text-slate-900 underline">Privacy Policy</Link>.
                  {' '}Data deletion instructions are available{' '}
                  <Link to="/data-deletion" className="hover:text-slate-900 underline">here</Link>.
                </>
              )}
            </p>
          </div>
        </div>
      </div>
      <div className="hidden md:flex relative bg-gradient-to-br from-pink-500 via-orange-400 to-amber-400 overflow-hidden items-center justify-center p-12">
        <div className="text-white max-w-md">
          <h2 className="font-display text-4xl font-extrabold leading-tight">
            {lang === 'ar'
              ? 'صمّم أتمتاتك من نشاط Instagram الخاص بك.'
              : 'Build automations from your own Instagram activity.'}
          </h2>
          <ul className="mt-8 space-y-4">
            {sidePerks.map(label => (
              <li key={label} className="flex items-center gap-3 bg-white/10 backdrop-blur-sm rounded-xl px-4 py-3 border border-white/20">
                <div className="w-6 h-6 rounded-full bg-white/20 flex items-center justify-center"><Check className="w-4 h-4" /></div>
                <span className="font-medium">{label}</span>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
};

export default Signup;
