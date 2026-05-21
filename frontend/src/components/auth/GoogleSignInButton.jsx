import React, { useEffect, useRef, useState } from 'react';
import { toast } from 'sonner';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import {
  googleClientId, googleStatus, loadGoogleRuntimeConfig, renderGoogleButton, googleErrorMessage,
} from '../../lib/googleAuth';
import { Button } from '../ui/button';
import { useTranslation } from '../../lib/i18n';

/**
 * Phase 2.7 reusable Google sign-in button.
 *
 * Uses Google Identity Services. If the build-time client id is unset,
 * it asks the backend for the public browser config and only falls back
 * to the disabled state if both sources are missing. Forwards the Google
 * ID token to /api/auth/google via AuthContext.loginWithGoogle. Never
 * logs the credential.
 */
export default function GoogleSignInButton({ onComplete, redirectTo = '/app' }) {
  const { lang } = useTranslation();
  const ar = lang === 'ar';
  const slotRef = useRef(null);
  const [configStatus, setConfigStatus] = useState(() => (googleClientId() ? 'enabled' : 'loading'));
  const [configDiagnostics, setConfigDiagnostics] = useState(() => googleStatus());
  const [sdkUnavailable, setSdkUnavailable] = useState(false);
  const [busy, setBusy] = useState(false);
  const { loginWithGoogle } = useAuth();
  const navigate = useNavigate();

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const cfg = await loadGoogleRuntimeConfig();
      if (!cancelled) {
        setConfigDiagnostics(cfg);
        setConfigStatus(cfg.enabled ? 'enabled' : (cfg.google_config_error_code ? 'error' : 'disabled'));
      }
    })();
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    if (configStatus !== 'enabled') return;
    let cancelled = false;
    (async () => {
      const ok = await renderGoogleButton(slotRef.current, {
        onCredential: async (credential) => {
          if (cancelled) return;
          setBusy(true);
          try {
            await loginWithGoogle(credential);
            toast.success(ar ? 'تم تسجيل الدخول عبر Google' : 'Signed in with Google');
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
          if (!cancelled) toast.error(ar ? 'تسجيل الدخول عبر Google غير متاح مؤقتاً' : 'Google sign-in is temporarily unavailable');
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
  }, [ar, configStatus, loginWithGoogle, navigate, onComplete, redirectTo]);

  if (configStatus !== 'enabled') {
    const diagnostics = {
      google_config_request_attempted: Boolean(configDiagnostics.google_config_request_attempted),
      google_config_request_ok: Boolean(configDiagnostics.google_config_request_ok),
      google_config_response_enabled: Boolean(configDiagnostics.google_config_response_enabled),
      google_config_response_was_json: Boolean(configDiagnostics.google_config_response_was_json),
      google_config_error_code: configDiagnostics.google_config_error_code || 'none',
    };
    return (
      <div className="space-y-2" data-testid="google-signin-wrapper" data-google-configured="false">
        <Button
          type="button"
          variant="outline"
          disabled
          className="w-full h-12 rounded-xl border-slate-200 text-slate-400 bg-slate-50"
          aria-label={ar ? 'المتابعة باستخدام Google' : 'Continue with Google'}
        >
          {ar ? 'المتابعة باستخدام Google' : 'Continue with Google'}
        </Button>
        <div className="text-center text-xs text-slate-400">
          {configStatus === 'loading'
            ? (ar ? 'جارٍ التحقّق من تسجيل الدخول عبر Google…' : 'Checking Google sign-in…')
            : (ar ? 'تسجيل الدخول عبر Google غير متاح حالياً. استخدم البريد وكلمة المرور بالأسفل.' : 'Google sign-in is unavailable right now. Use email and password below.')}
        </div>
        {/* Phase 2.18R: keep the diagnostic line in the DOM for our
            own test fixtures and operator debugging — but stop
            rendering raw `google_config_*` keys to end users. Visible
            visually-hidden node is targetable by data-testid for
            tests but does not leak debug noise into the UI. */}
        <div
          className="sr-only"
          data-testid="google-config-diagnostics"
          aria-hidden="true"
        >
          {`google_config_request_attempted=${diagnostics.google_config_request_attempted ? 'yes' : 'no'} · google_config_request_ok=${diagnostics.google_config_request_ok ? 'yes' : 'no'} · google_config_response_enabled=${diagnostics.google_config_response_enabled ? 'yes' : 'no'} · google_config_response_was_json=${diagnostics.google_config_response_was_json ? 'yes' : 'no'} · google_config_error_code=${diagnostics.google_config_error_code}`}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-2" data-testid="google-signin-wrapper" data-google-configured="true">
      <div
        ref={slotRef}
        className={busy ? 'opacity-60 pointer-events-none' : ''}
        aria-label={ar ? 'المتابعة باستخدام Google' : 'Continue with Google'}
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
        {ar ? 'أو تابع باستخدام البريد الإلكتروني بالأسفل' : 'or continue with email below'}
      </div>
    </div>
  );
}
