import React, { Suspense, lazy, useEffect } from 'react';
import './App.css';
import { BrowserRouter, Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { AuthProvider, useAuth } from './context/AuthContext';
import { I18nProvider, useTranslation } from './lib/i18n';
import { Toaster } from './components/ui/sonner';

// Toaster wrapper that flips the corner the toast slides in from
// based on the active locale: top-right in LTR, top-left in RTL so
// the toast still appears at the start of the reading direction.
function LocaleAwareToaster() {
  const { lang } = useTranslation();
  return <Toaster position={lang === 'ar' ? 'top-left' : 'top-right'} dir={lang === 'ar' ? 'rtl' : 'ltr'} />;
}
import analytics from './lib/analytics';
import { registerRoute } from './lib/routePreloader';
import { APP_CHILD_ROUTES, ROUTES } from './constants/routes';

import DashboardLayout from './components/layout/DashboardLayout';
import OfflineBanner from './components/OfflineBanner';

const landingFn = () => import('./pages/Landing');
const loginFn = () => import('./pages/Login');
const signupFn = () => import('./pages/Signup');
const dashboardFn = () => import('./pages/Dashboard');
const automationsFn = () => import('./pages/Automations');
const flowBuilderFn = () => import('./pages/FlowBuilder');
const settingsFn = () => import('./pages/Settings');
const dmAutomationFn = () => import('./pages/DmAutomation');
const privacyFn = () => import('./pages/PrivacyPolicy');
const termsFn = () => import('./pages/Terms');
const dataDeletionFn = () => import('./pages/DataDeletion');
const statusPageFn = () => import('./pages/StatusPage');
const billingFn = () => import('./pages/Billing');
const adminFn = () => import('./pages/admin/AdminConsole');
const specificReplyDebugFn = () => import('./pages/admin/SpecificReplyDebug');
const notFoundFn = () => import('./pages/NotFound');
const forgotPasswordFn = () => import('./pages/ForgotPassword');
const resetPasswordFn = () => import('./pages/ResetPassword');

const Landing = lazy(landingFn);
const Login = lazy(loginFn);
const Signup = lazy(signupFn);
const Dashboard = lazy(dashboardFn);
const Automations = lazy(automationsFn);
const FlowBuilder = lazy(flowBuilderFn);
const Settings = lazy(settingsFn);
const DmAutomation = lazy(dmAutomationFn);
const PrivacyPolicy = lazy(privacyFn);
const Terms = lazy(termsFn);
const DataDeletion = lazy(dataDeletionFn);
const StatusPage = lazy(statusPageFn);
const Billing = lazy(billingFn);
const AdminConsole = lazy(adminFn);
const SpecificReplyDebug = lazy(specificReplyDebugFn);
const NotFound = lazy(notFoundFn);
const ForgotPassword = lazy(forgotPasswordFn);
const ResetPassword = lazy(resetPasswordFn);

registerRoute('Dashboard', dashboardFn);
registerRoute('Automations', automationsFn);
registerRoute('FlowBuilder', flowBuilderFn);
registerRoute('DmAutomation', dmAutomationFn);
registerRoute('Settings', settingsFn);
registerRoute('Billing', billingFn);
registerRoute('Admin', adminFn);
registerRoute('SpecificReplyDebug', specificReplyDebugFn);

function PageViewTracker() {
  const location = useLocation();
  const navStart = React.useRef(performance.now());
  useEffect(() => {
    const elapsed = (performance.now() - navStart.current).toFixed(0);
    // Gate route-timing chatter the same way api.js does: silent in
    // production unless the user opts in via ?debug=1 or
    // localStorage.mychat_debug=1.
    let debug = process.env.NODE_ENV !== 'production';
    try {
      if (!debug) {
        const params = new URLSearchParams(window.location.search);
        debug = params.get('debug') === '1' || localStorage.getItem('mychat_debug') === '1';
      }
    } catch (_) { /* ignore */ }
    if (elapsed > 200 && debug) console.log(`[route] ${location.pathname}${location.search} rendered in ${elapsed}ms`);
    navStart.current = performance.now();
    analytics.pageView(location.pathname + (location.search || ''));
  }, [location.pathname, location.search]);
  return null;
}

const PageLoading = () => {
  const ar = typeof document !== 'undefined' && document.documentElement?.lang === 'ar';
  return (
    <div className="w-full p-6 text-sm text-slate-500">
      {ar ? 'جارٍ التحميل...' : 'Loading...'}
    </div>
  );
};

const ProtectedRoute = ({ children }) => {
  const { user, loading } = useAuth();
  if (loading) {
    const ar = typeof document !== 'undefined' && document.documentElement?.lang === 'ar';
    return (
      <div className="h-screen w-screen flex items-center justify-center text-slate-500">
        {ar ? 'جارٍ التحميل...' : 'Loading...'}
      </div>
    );
  }
  if (!user) return <Navigate to={ROUTES.LOGIN} replace />;
  return children;
};

// Top-level error boundary: catches any uncaught render error from
// the lazy-loaded route tree and shows a polite recovery card
// instead of a blank white page. Never logs user data — just the
// error name and a short stack hash so support can correlate logs.
class RootErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false };
  }
  static getDerivedStateFromError() {
    return { hasError: true };
  }
  componentDidCatch(error) {
    // eslint-disable-next-line no-console
    console.error('[App] root error boundary tripped', {
      name: error?.name || 'Error',
      message: (error?.message || '').slice(0, 200),
    });
    try { analytics.capture('app_root_error', { name: error?.name || 'Error' }); } catch (_) { /* ignore */ }
  }
  render() {
    if (!this.state.hasError) return this.props.children;
    const ar = typeof document !== 'undefined' && document.documentElement?.lang === 'ar';
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-50 p-6">
        <div className="max-w-md w-full bg-white rounded-2xl border border-slate-100 p-6 text-center">
          <h1 className="text-xl font-bold text-slate-900 mb-2">
            {ar ? 'حدث خطأ غير متوقّع' : 'Something went wrong'}
          </h1>
          <p className="text-sm text-slate-600 mb-4">
            {ar
              ? 'حدث خطأ أثناء عرض هذه الصفحة. أعد تحميل الصفحة للمتابعة.'
              : 'An unexpected error happened while rendering this page. Reload to continue.'}
          </p>
          <button
            type="button"
            onClick={() => window.location.reload()}
            className="rounded-xl bg-slate-900 hover:bg-slate-800 text-white text-sm px-5 py-2.5"
          >
            {ar ? 'إعادة تحميل الصفحة' : 'Reload page'}
          </button>
        </div>
      </div>
    );
  }
}

// Phase 2.19: surface unhandled promise rejections to telemetry so we
// see them in PostHog even if they don't trip the React error
// boundary. The browser already prints them to console; this just
// echoes a sanitized event to analytics.
if (typeof window !== 'undefined' && !window.__mychat_unhandled_installed__) {
  window.__mychat_unhandled_installed__ = true;
  window.addEventListener('unhandledrejection', (event) => {
    try {
      const reason = event?.reason;
      const name = (reason && reason.name) || 'UnhandledRejection';
      const message = String((reason && reason.message) || reason || '').slice(0, 200);
      analytics.capture('unhandled_rejection', { name, message });
    } catch (_) { /* never let the listener itself throw */ }
  });
}

function App() {
  return (
    <div className="App">
      <RootErrorBoundary>
      <I18nProvider>
      <AuthProvider>
        <BrowserRouter>
          <OfflineBanner />
          <LocaleAwareToaster />
          <PageViewTracker />
          <Suspense fallback={<PageLoading />}>
<Routes>
              <Route path={ROUTES.HOME} element={<Landing />} />
              <Route path={ROUTES.LOGIN} element={<Login />} />
              <Route path={ROUTES.SIGNUP} element={<Signup />} />
              <Route path={ROUTES.FORGOT_PASSWORD} element={<ForgotPassword />} />
              <Route path={ROUTES.RESET_PASSWORD} element={<ResetPassword />} />
              <Route path={ROUTES.PRIVACY} element={<PrivacyPolicy />} />
              <Route path={ROUTES.TERMS} element={<Terms />} />
              <Route path={ROUTES.DATA_DELETION} element={<DataDeletion />} />
              <Route path={ROUTES.STATUS} element={<StatusPage />} />
              <Route path={ROUTES.APP} element={<ProtectedRoute><DashboardLayout /></ProtectedRoute>}>
                <Route index element={<Dashboard />} />
                <Route path={APP_CHILD_ROUTES.AUTOMATIONS} element={<Automations />} />
                <Route path={APP_CHILD_ROUTES.AUTOMATION_DETAIL} element={<FlowBuilder />} />
                <Route path={APP_CHILD_ROUTES.DM_AUTOMATION} element={<DmAutomation />} />
                <Route path={APP_CHILD_ROUTES.SETTINGS} element={<Settings />} />
                <Route path={APP_CHILD_ROUTES.BILLING} element={<Billing />} />
                <Route path={APP_CHILD_ROUTES.ADMIN} element={<AdminConsole />} />
                <Route path={APP_CHILD_ROUTES.ADMIN_SPECIFIC_REPLY_DEBUG} element={<SpecificReplyDebug />} />
                <Route path="*" element={<Navigate to={ROUTES.APP} replace />} />
              </Route>
              <Route path="*" element={<NotFound />} />
            </Routes>
          </Suspense>
        </BrowserRouter>
      </AuthProvider>
      </I18nProvider>
      </RootErrorBoundary>
    </div>
  );
}

export default App;
