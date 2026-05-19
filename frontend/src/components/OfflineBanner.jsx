/**
 * Tiny offline banner. Watches navigator.onLine + the browser's
 * online/offline events and shows a single-line banner at the top
 * of the viewport when the user loses connectivity. Keeps the
 * surrounding UI responsive (clicks/typing still work) but signals
 * that anything they do won't reach the server until they're back.
 */
import React, { useEffect, useState } from 'react';
import { WifiOff } from 'lucide-react';
import { useTranslation } from '../lib/i18n';

export default function OfflineBanner() {
  const { lang } = useTranslation();
  const ar = lang === 'ar';
  const [online, setOnline] = useState(
    typeof navigator === 'undefined' ? true : navigator.onLine !== false
  );

  useEffect(() => {
    const handleOnline = () => setOnline(true);
    const handleOffline = () => setOnline(false);
    window.addEventListener('online', handleOnline);
    window.addEventListener('offline', handleOffline);
    return () => {
      window.removeEventListener('online', handleOnline);
      window.removeEventListener('offline', handleOffline);
    };
  }, []);

  if (online) return null;
  return (
    <div
      role="status"
      aria-live="polite"
      className="fixed inset-x-0 top-0 z-[60] bg-amber-50 border-b border-amber-200 text-amber-900 text-sm py-2 px-4 flex items-center justify-center gap-2"
      data-testid="offline-banner"
    >
      <WifiOff className="w-4 h-4 shrink-0" aria-hidden="true" />
      <span>
        {ar
          ? 'لا يوجد اتصال بالإنترنت — التغييرات ستُحفظ عند عودة الاتصال.'
          : "You're offline — changes will sync when the connection returns."}
      </span>
    </div>
  );
}
