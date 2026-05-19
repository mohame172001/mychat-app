import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { MessageCircle } from 'lucide-react';
import { toast } from 'sonner';
import api from '../lib/api';
import { useTranslation } from '../lib/i18n';
import LangSwitcher from '../components/LangSwitcher';

/**
 * Phase 2.14 account recovery.
 *
 * Always shows the same generic success copy regardless of whether the
 * email exists in our DB — the backend returns the same shape too, so
 * neither layer leaks user enumeration info.
 */
const ForgotPassword = () => {
  const [email, setEmail] = useState('');
  const [loading, setLoading] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const { t, lang } = useTranslation();

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!email.trim()) {
      toast.error(lang === 'ar' ? 'يرجى إدخال البريد الإلكتروني' : 'Please enter your email');
      return;
    }
    setLoading(true);
    try {
      await api.post('/auth/forgot-password', { email: email.trim() });
      setSubmitted(true);
    } catch (err) {
      const status = err?.response?.status;
      if (status === 429) {
        toast.error(lang === 'ar' ? 'محاولات كثيرة جداً. يرجى المحاولة لاحقاً.' : 'Too many reset requests. Please try again later.');
      } else {
        // Any other error still resolves to the generic UX so we never
        // tell the caller whether the email is registered.
        setSubmitted(true);
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-white" data-testid="forgot-password-page">
      <div className="max-w-md mx-auto p-8 md:p-12">
        <div className="flex items-center justify-between">
          <Link to="/" className="flex items-center gap-2">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
              <MessageCircle className="w-5 h-5 text-white" strokeWidth={2.5} />
            </div>
            <span className="text-xl font-bold font-display">mychat</span>
          </Link>
          <LangSwitcher />
        </div>
        <div className="mt-12">
          <h1 className="font-display text-3xl md:text-4xl font-extrabold tracking-tight">
            {t('auth.forgot.title')}
          </h1>
          <p className="mt-2 text-slate-600 text-sm">
            {t('auth.forgot.subtitle')}
          </p>

          {submitted ? (
            <div
              className="mt-8 p-4 rounded-xl border border-emerald-200 bg-emerald-50 text-sm text-emerald-800"
              data-testid="forgot-password-success"
            >
              {t('auth.forgot.sent')}
            </div>
          ) : (
            <form onSubmit={handleSubmit} className="mt-8 space-y-5">
              <div className="space-y-2">
                <Label htmlFor="email">{t('auth.forgot.email')}</Label>
                <Input
                  id="email"
                  type="email"
                  autoComplete="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  maxLength={254}
                  className="h-12 rounded-xl"
                />
              </div>
              <Button
                type="submit"
                disabled={loading}
                aria-busy={loading}
                className="w-full h-12 rounded-xl bg-slate-900 hover:bg-slate-800 text-white"
                data-testid="forgot-password-submit"
              >
                {loading ? t('common.loading') : t('auth.forgot.submit')}
              </Button>
            </form>
          )}

          <p className="mt-6 text-sm text-center text-slate-600">
            <Link to="/login" className="font-semibold text-slate-900 hover:underline">
              {t('auth.forgot.back')}
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
};

export default ForgotPassword;
