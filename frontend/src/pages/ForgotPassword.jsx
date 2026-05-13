import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { MessageCircle } from 'lucide-react';
import { toast } from 'sonner';
import api from '../lib/api';

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

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!email.trim()) {
      toast.error('Please enter your email');
      return;
    }
    setLoading(true);
    try {
      await api.post('/auth/forgot-password', { email: email.trim() });
      setSubmitted(true);
    } catch (err) {
      const status = err?.response?.status;
      if (status === 429) {
        toast.error('Too many reset requests. Please try again later.');
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
        <Link to="/" className="flex items-center gap-2">
          <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
            <MessageCircle className="w-5 h-5 text-white" strokeWidth={2.5} />
          </div>
          <span className="text-xl font-bold font-display">mychat</span>
        </Link>
        <div className="mt-12">
          <h1 className="font-display text-3xl md:text-4xl font-extrabold tracking-tight">
            Reset your password
          </h1>
          <p className="mt-2 text-slate-600 text-sm">
            Enter the email on your account and we'll send you a reset link.
          </p>

          {submitted ? (
            <div
              className="mt-8 p-4 rounded-xl border border-emerald-200 bg-emerald-50 text-sm text-emerald-800"
              data-testid="forgot-password-success"
            >
              If an account exists for that email, we sent a reset link.
              Check your inbox and spam folder.
            </div>
          ) : (
            <form onSubmit={handleSubmit} className="mt-8 space-y-5">
              <div className="space-y-2">
                <Label htmlFor="email">Email</Label>
                <Input
                  id="email"
                  type="email"
                  autoComplete="email"
                  placeholder="you@company.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="h-12 rounded-xl"
                />
              </div>
              <Button
                type="submit"
                disabled={loading}
                className="w-full h-12 rounded-xl bg-slate-900 hover:bg-slate-800 text-white"
                data-testid="forgot-password-submit"
              >
                {loading ? 'Sending...' : 'Send reset link'}
              </Button>
            </form>
          )}

          <p className="mt-6 text-sm text-center text-slate-600">
            Remembered it?{' '}
            <Link to="/login" className="font-semibold text-slate-900 hover:underline">
              Sign in
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
};

export default ForgotPassword;
