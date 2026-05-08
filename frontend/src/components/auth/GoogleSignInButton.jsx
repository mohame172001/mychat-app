import React, { useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import {
  isGoogleAuthEnabled, renderGoogleButton, googleErrorMessage,
} from '../../lib/googleAuth';

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
        // Only log a warning; we never log the credential.
        // eslint-disable-next-line no-console
        console.warn('[GoogleSignInButton] sdk_unavailable');
      }
    })();
    return () => { cancelled = true; };
  }, [enabled, loginWithGoogle, navigate, onComplete, redirectTo]);

  if (!enabled) return null;

  return (
    <div className="space-y-2" data-testid="google-signin-wrapper">
      <div ref={slotRef} className={busy ? 'opacity-60 pointer-events-none' : ''} />
      <div className="text-center text-xs text-slate-400">
        or continue with email below
      </div>
    </div>
  );
}
