import React, { useEffect, useState } from 'react';
import { Link, useNavigate, useSearchParams } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { PasswordInput } from '../components/ui/password-input';
import { Label } from '../components/ui/label';
import { MessageCircle } from 'lucide-react';
import { toast } from 'sonner';
import api from '../lib/api';
import { useTranslation } from '../lib/i18n';
import LangSwitcher from '../components/LangSwitcher';

const ResetPassword = () => {
  const { lang } = useTranslation();
  const ar = lang === 'ar';
  const [params, setParams] = useSearchParams();
  const [token, setToken] = useState(() => params.get('token') || '');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    if (params.get('token')) {
      const next = new URLSearchParams(params);
      next.delete('token');
      setParams(next, { replace: true });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!token) {
      toast.error(ar ? 'رابط الاستعادة مفقود أو غير صالح' : 'Missing or invalid reset link');
      return;
    }
    if (!newPassword || !confirmPassword) {
      toast.error(ar ? 'يرجى ملء حقلَي كلمة المرور' : 'Please fill both password fields');
      return;
    }
    if (newPassword.length < 8) {
      toast.error(ar ? 'كلمة المرور يجب ألّا تقلّ عن 8 أحرف' : 'Password must be at least 8 characters');
      return;
    }
    if (newPassword !== confirmPassword) {
      toast.error(ar ? 'كلمتا المرور غير متطابقتين' : 'Passwords do not match');
      return;
    }
    setLoading(true);
    try {
      await api.post('/auth/reset-password', {
        token,
        new_password: newPassword,
      });
      setToken('');
      setNewPassword('');
      setConfirmPassword('');
      toast.success(ar ? 'تم تعيين كلمة المرور بنجاح. يرجى تسجيل الدخول.' : 'Password reset successfully. Please sign in.');
      navigate('/login', { replace: true });
    } catch (err) {
      const detail = err?.response?.data?.detail;
      let msg = ar ? 'فشل تعيين كلمة المرور. اطلب رابطاً جديداً.' : 'Reset failed. Request a fresh link.';
      if (detail === 'password_too_short') msg = ar ? 'كلمة المرور يجب ألّا تقلّ عن 8 أحرف' : 'Password must be at least 8 characters';
      else if (detail === 'password_reset_token_expired') msg = ar ? 'انتهت صلاحية رابط الاستعادة. اطلب رابطاً جديداً.' : 'Reset link expired. Request a new one.';
      else if (detail === 'password_reset_token_used') msg = ar ? 'هذا الرابط مُستخدَم بالفعل.' : 'This reset link was already used.';
      else if (detail === 'invalid_password_reset_token') msg = ar ? 'رابط الاستعادة غير صالح. اطلب رابطاً جديداً.' : 'Reset link is invalid. Request a new one.';
      toast.error(msg);
    } finally {
      setLoading(false);
    }
  };

  const tokenMissing = !token;

  return (
    <div className="min-h-screen bg-white" data-testid="reset-password-page">
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
            {ar ? 'تعيين كلمة مرور جديدة' : 'Set a new password'}
          </h1>
          <p className="mt-2 text-slate-600 text-sm">
            {ar ? 'اختر كلمة مرور جديدة. سيتمّ تسجيل الخروج من جميع الجلسات الحالية على هذا الحساب.' : 'Pick a new password. Existing sessions on this account will sign out.'}
          </p>

          {tokenMissing && (
            <div
              className="mt-8 p-4 rounded-xl border border-amber-200 bg-amber-50 text-sm text-amber-900"
              data-testid="reset-password-missing-token"
            >
              {ar ? (
                <>رابط الاستعادة مفقود أو سبق استخدامه. اطلب رابطاً جديداً من صفحة <Link to="/forgot-password" className="font-semibold underline">استعادة كلمة المرور</Link>.</>
              ) : (
                <>The reset link is missing or has been used. Request a fresh link from the <Link to="/forgot-password" className="font-semibold underline">forgot password</Link> page.</>
              )}
            </div>
          )}

          {!tokenMissing && (
            <form onSubmit={handleSubmit} className="mt-8 space-y-5">
              <div className="space-y-2">
                <Label htmlFor="new-password">{ar ? 'كلمة المرور الجديدة' : 'New password'}</Label>
                <PasswordInput
                  id="new-password"
                  autoComplete="new-password"
                  placeholder={ar ? '٨ أحرف على الأقل' : 'At least 8 characters'}
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  inputClassName="h-12 rounded-xl"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="confirm-password">{ar ? 'تأكيد كلمة المرور' : 'Confirm new password'}</Label>
                <PasswordInput
                  id="confirm-password"
                  autoComplete="new-password"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  inputClassName="h-12 rounded-xl"
                />
              </div>
              <Button
                type="submit"
                disabled={loading}
                className="w-full h-12 rounded-xl bg-slate-900 hover:bg-slate-800 text-white"
                data-testid="reset-password-submit"
              >
                {loading
                  ? (ar ? 'جارٍ الحفظ...' : 'Resetting...')
                  : (ar ? 'تعيين كلمة المرور' : 'Reset password')}
              </Button>
            </form>
          )}

          <p className="mt-6 text-sm text-center text-slate-600">
            {ar ? 'العودة إلى ' : 'Back to '}
            <Link to="/login" className="font-semibold text-slate-900 hover:underline">
              {ar ? 'تسجيل الدخول' : 'Sign in'}
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
};

export default ResetPassword;
