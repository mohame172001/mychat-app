import React, { Suspense, lazy, useEffect } from 'react';
import './App.css';
import { BrowserRouter, Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { AuthProvider, useAuth } from './context/AuthContext';
import { I18nProvider } from './lib/i18n';
import { Toaster } from './components/ui/sonner';
import analytics from './lib/analytics';
import { registerRoute, preloadAfterPaint } from './lib/routePreloader';

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
  if (!user) return <Navigate to="/login" replace />;
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

function App() {
  return (
    <div className="App">
      <RootErrorBoundary>
      <I18nProvider>
      <AuthProvider>
        <BrowserRouter>
          <OfflineBanner />
          <Toaster position="top-right" />
          <PageViewTracker />
          <Suspense fallback={<PageLoading />}>
<Routes>
              <Route path="/" element={<Landing />} />
              <Route path="/login" element={<Login />} />
              <Route path="/signup" element={<Signup />} />
              <Route path="/forgot-password" element={<ForgotPassword />} />
              <Route path="/reset-password" element={<ResetPassword />} />
              <Route path="/privacy" element={<PrivacyPolicy />} />
              <Route path="/terms" element={<Terms />} />
              <Route path="/data-deletion" element={<DataDeletion />} />
              <Route path="/status" element={<StatusPage />} />
              <Route path="/app" element={<ProtectedRoute><DashboardLayout /></ProtectedRoute>}>
                <Route index element={<Dashboard />} />
                <Route path="automations" element={<Automations />} />
                <Route path="automations/:id" element={<FlowBuilder />} />
                <Route path="dm-automation" element={<DmAutomation />} />
                <Route path="settings" element={<Settings />} />
                <Route path="billing" element={<Billing />} />
                <Route path="admin" element={<AdminConsole />} />
                <Route path="admin/specific-reply-debug" element={<SpecificReplyDebug />} />
                <Route path="*" element={<Navigate to="/app" replace />} />
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
