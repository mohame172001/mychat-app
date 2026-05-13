import React, { useEffect, useState } from 'react';
import { Link, useNavigate, useSearchParams } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { MessageCircle } from 'lucide-react';
import { toast } from 'sonner';
import api from '../lib/api';

/**
 * Phase 2.14 account recovery — reset page.
 *
 * Privacy contract:
 *   - The reset token is consumed from `?token=` once at mount and
 *     held only in a ref-style local variable. It is never logged,
 *     never stored in localStorage / sessionStorage, never echoed
 *     back into the DOM.
 *   - On success we redirect to /login and the token-bearing URL is
 *     replaced so a Back navigation doesn't expose it.
 */
const ResetPassword = () => {
  const [params, setParams] = useSearchParams();
  // Read the token once and immediately scrub it from the URL.
  const [token, setToken] = useState(() => params.get('token') || '');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    if (params.get('token')) {
      // Remove the token from the visible URL.
      const next = new URLSearchParams(params);
      next.delete('token');
      setParams(next, { replace: true });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!token) {
      toast.error('Missing or invalid reset link');
      return;
    }
    if (!newPassword || !confirmPassword) {
      toast.error('Please fill both password fields');
      return;
    }
    if (newPassword.length < 6) {
      toast.error('Password must be at least 6 characters');
      return;
    }
    if (newPassword !== confirmPassword) {
      toast.error('Passwords do not match');
      return;
    }
    setLoading(true);
    try {
      await api.post('/auth/reset-password', {
        token,
        new_password: newPassword,
      });
      // Drop the token from memory immediately.
      setToken('');
      setNewPassword('');
      setConfirmPassword('');
      toast.success('Password reset successfully. Please sign in.');
      navigate('/login', { replace: true });
    } catch (err) {
      const detail = err?.response?.data?.detail;
      let msg = 'Reset failed. Request a fresh link.';
      if (detail === 'password_too_short') msg = 'Password must be at least 6 characters';
      else if (detail === 'password_reset_token_expired') msg = 'Reset link expired. Request a new one.';
      else if (detail === 'password_reset_token_used') msg = 'This reset link was already used.';
      else if (detail === 'invalid_password_reset_token') msg = 'Reset link is invalid. Request a new one.';
      toast.error(msg);
    } finally {
      setLoading(false);
    }
  };

  const tokenMissing = !token;

  return (
    <div className="min-h-screen bg-white" data-testid="reset-password-page">
      <div className="max-w-md mx-auto p-8 md:p-12">
        <Link to="/" className="flex items-center gap-2">
          <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
            <MessageCircle className="w-5 h-5 text-white" strokeWidth={2.5} />
          </div>
          <span className="text-xl font-bold font-display">mychat</span>
        </Link>
        <div className="mt-12">
          <h1 className="font-display text-3xl md:text-4xl font-extrabold tracking-tight">
            Set a new password
          </h1>
          <p className="mt-2 text-slate-600 text-sm">
            Pick a new password. Existing sessions on this account will sign out.
          </p>

          {tokenMissing && (
            <div
              className="mt-8 p-4 rounded-xl border border-amber-200 bg-amber-50 text-sm text-amber-900"
              data-testid="reset-password-missing-token"
            >
              The reset link is missing or has been used. Request a fresh
              link from the <Link to="/forgot-password" className="font-semibold underline">forgot password</Link> page.
            </div>
          )}

          {!tokenMissing && (
            <form onSubmit={handleSubmit} className="mt-8 space-y-5">
              <div className="space-y-2">
                <Label htmlFor="new-password">New password</Label>
                <Input
                  id="new-password"
                  type="password"
                  autoComplete="new-password"
                  placeholder="At least 6 characters"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  className="h-12 rounded-xl"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="confirm-password">Confirm new password</Label>
                <Input
                  id="confirm-password"
                  type="password"
                  autoComplete="new-password"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  className="h-12 rounded-xl"
                />
              </div>
              <Button
                type="submit"
                disabled={loading}
                className="w-full h-12 rounded-xl bg-slate-900 hover:bg-slate-800 text-white"
                data-testid="reset-password-submit"
              >
                {loading ? 'Resetting...' : 'Reset password'}
              </Button>
            </form>
          )}

          <p className="mt-6 text-sm text-center text-slate-600">
            Back to <Link to="/login" className="font-semibold text-slate-900 hover:underline">Sign in</Link>
          </p>
        </div>
      </div>
    </div>
  );
};

export default ResetPassword;
