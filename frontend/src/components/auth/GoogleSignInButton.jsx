import React, { useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import {
  isGoogleAuthEnabled, renderGoogleButton, googleErrorMessage,
} from '../../lib/googleAuth';
import { Button } from '../ui/button';

/**
 * Phase 2.7 reusable Google sign-in button.
 *
 * Uses Google Identity Services. Hidden when REACT_APP_GOOGLE_CLIENT_ID
 * is unset. Forwards the Google ID token to /api/auth/google via
 * AuthContext.loginWithGoogle. Never logs the credential.
 */
export default function GoogleSignInButton({ onComplete, redirectTo = '/app' }) {
  const slotRef = useRef(null);
  const [enabled] = useState(() => isGoogleAuthEnabled());
  const [sdkUnavailable, setSdkUnavailable] = useState(false);
  const [busy, setBusy] = useState(false);
  const { loginWithGoogle } = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    if (!enabled) return;
    let cancelled = false;
    (async () => {
      const ok = await renderGoogleButton(slotRef.current, {
        onCredential: async (credential) => {
          if (cancelled) return;
          setBusy(true);
          try {
            await loginWithGoogle(credential);
            toast.success('Signed in with Google');
            if (onComplete) onComplete();
            else navigate(redirectTo);
          } catch (err) {
            const detail = err?.response?.data?.detail;
            toast.error(googleErrorMessage(detail));
          } finally {
            if (!cancelled) setBusy(false);
          }
        },
        onError: () => {
          if (!cancelled) toast.error('Google sign-in is temporarily unavailable');
        },
      });
      // ok=false is fine — the slot stays empty and the email/password
      // form remains usable below.
      if (!ok && !cancelled) {
        setSdkUnavailable(true);
        // Only log a warning; we never log the credential.
        // eslint-disable-next-line no-console
        console.warn('[GoogleSignInButton] sdk_unavailable');
      }
    })();
    return () => { cancelled = true; };
  }, [enabled, loginWithGoogle, navigate, onComplete, redirectTo]);

  if (!enabled) {
    return (
      <div className="space-y-2" data-testid="google-signin-wrapper" data-google-configured="false">
        <Button
          type="button"
          variant="outline"
          disabled
          className="w-full h-12 rounded-xl border-slate-200 text-slate-400 bg-slate-50"
          aria-label="Continue with Google"
        >
          Continue with Google
        </Button>
        <div className="text-center text-xs text-slate-400">
          Google sign-in is not configured
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-2" data-testid="google-signin-wrapper" data-google-configured="true">
      <div
        ref={slotRef}
        className={busy ? 'opacity-60 pointer-events-none' : ''}
        aria-label="Continue with Google"
      />
      {sdkUnavailable && (
        <Button
          type="button"
          variant="outline"
          disabled
          className="w-full h-12 rounded-xl border-slate-200 text-slate-400 bg-slate-50"
        >
          Continue with Google
        </Button>
      )}
      <div className="text-center text-xs text-slate-400">
        or continue with email below
      </div>
    </div>
  );
}
