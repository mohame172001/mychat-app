import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { PasswordInput } from '../components/ui/password-input';
import { Label } from '../components/ui/label';
import { MessageCircle } from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import { toast } from 'sonner';
import GoogleSignInButton from '../components/auth/GoogleSignInButton';
import { authErrorMessageFromApiError } from '../lib/authErrors';
import { useTranslation } from '../lib/i18n';
import LangSwitcher from '../components/LangSwitcher';

const Login = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [capsLock, setCapsLock] = useState(false);
  const { login } = useAuth();
  const navigate = useNavigate();
  const { t, lang } = useTranslation();

  const handlePasswordKey = (e) => {
    // KeyboardEvent.getModifierState exists in every evergreen
    // browser; .getModifierState('CapsLock') flips between focus
    // and key events, so we update on every keystroke for accuracy.
    if (typeof e?.getModifierState === 'function') {
      setCapsLock(e.getModifierState('CapsLock'));
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const ar = lang === 'ar';
    const u = username.trim();
    if (!u || !password) {
      toast.error(ar ? 'يرجى تعبئة جميع الحقول' : 'Please fill in all fields');
      return;
    }
    // Mirror the backend's LoginIn caps so an oversize value gets a
    // clear inline message instead of a 422 round-trip.
    if (u.length > 64 || password.length > 128) {
      toast.error(ar ? 'القيمة المُدخَلة طويلة جداً' : 'Input too long');
      return;
    }
    setLoading(true);
    try {
      await login(u, password);
      toast.success(t('auth.login.title'));
      navigate('/app');
    } catch (err) {
      toast.error(authErrorMessageFromApiError(err));
    } finally {
      setLoading(false);
    }
  };

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
            <h1 className="font-display text-3xl md:text-4xl font-extrabold tracking-tight">{t('auth.login.title')}</h1>
            <p className="mt-2 text-slate-600">{t('auth.login.subtitle')}</p>
            <div className="mt-6">
              <GoogleSignInButton redirectTo="/app" />
            </div>
            <form onSubmit={handleSubmit} className="mt-6 space-y-5">
              <div className="space-y-2">
                <Label htmlFor="username">{t('auth.login.emailOrUsername')}</Label>
                <Input id="username" autoComplete="username" value={username} onChange={e => setUsername(e.target.value)} maxLength={64} className="h-12 rounded-xl" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="password">{t('auth.login.password')}</Label>
                <PasswordInput
                  id="password"
                  autoComplete="current-password"
                  value={password}
                  onChange={e => setPassword(e.target.value)}
                  onKeyUp={handlePasswordKey}
                  onKeyDown={handlePasswordKey}
                  maxLength={128}
                  inputClassName="h-12 rounded-xl"
                />
                {capsLock && (
                  <p className="text-xs text-amber-700 flex items-center gap-1.5" role="status" aria-live="polite">
                    <span aria-hidden="true">⚠️</span>
                    {lang === 'ar' ? 'مفتاح Caps Lock مُفعّل' : 'Caps Lock is on'}
                  </p>
                )}
              </div>
              <Button type="submit" disabled={loading} aria-busy={loading} className="w-full h-12 rounded-xl bg-slate-900 hover:bg-slate-800 text-white">
                {loading ? t('common.loading') : t('auth.login.submit')}
              </Button>
              <p className="text-xs text-center text-slate-500">
                <Link
                  to="/forgot-password"
                  className="hover:text-slate-900 underline"
                  data-testid="login-forgot-password-link"
                >
                  {t('auth.login.forgot')}
                </Link>
              </p>
            </form>
            <p className="mt-6 text-sm text-center text-slate-600">
              {t('auth.login.noAccount')} <Link to="/signup" className="font-semibold text-slate-900 hover:underline">{t('auth.login.signupLink')}</Link>
            </p>
            <p className="mt-4 text-xs text-center text-slate-500">
              <Link to="/privacy" className="hover:text-slate-900 underline">{t('common.privacy')}</Link>
              <span className="mx-2">-</span>
              <Link to="/terms" className="hover:text-slate-900 underline">{t('common.terms')}</Link>
              <span className="mx-2">-</span>
              <Link to="/data-deletion" className="hover:text-slate-900 underline">{t('common.dataDeletion')}</Link>
            </p>
          </div>
        </div>
      </div>
      <div className="hidden md:block relative bg-gradient-to-br from-blue-500 via-purple-500 to-pink-500 overflow-hidden">
        <div className="absolute inset-0 flex items-center justify-center p-12">
          <div className="text-white max-w-md">
            <h2 className="font-display text-4xl font-extrabold leading-tight">
              {lang === 'ar'
                ? 'أتمت المحادثات التي تنمّي عملك.'
                : 'Automate the conversations that grow your business.'}
            </h2>
            <p className="mt-4 text-white/90 text-lg">
              {lang === 'ar'
                ? 'اربط Instagram، صمّم قواعد التحوّل من تعليق إلى رسالة، وراقب المحادثات الحقيقية وهي تتحول إلى نتائج.'
                : 'Connect your Instagram, build comment-to-DM rules, and watch real conversations turn into action.'}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Login;
