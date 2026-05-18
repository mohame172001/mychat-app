import React, { Suspense, lazy, useEffect } from 'react';
import './App.css';
import { BrowserRouter, Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { AuthProvider, useAuth } from './context/AuthContext';
import { I18nProvider } from './lib/i18n';
import { Toaster } from './components/ui/sonner';
import analytics from './lib/analytics';
import { registerRoute, preloadAfterPaint } from './lib/routePreloader';

import DashboardLayout from './components/layout/DashboardLayout';

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
    if (elapsed > 200) console.log(`[route] ${location.pathname}${location.search} rendered in ${elapsed}ms`);
    navStart.current = performance.now();
    analytics.pageView(location.pathname + (location.search || ''));
  }, [location.pathname, location.search]);
  return null;
}

const PageLoading = () => (
  <div className="w-full p-6 text-sm text-slate-500">Loading...</div>
);

const ProtectedRoute = ({ children }) => {
  const { user, loading } = useAuth();
  if (loading) return <div className="h-screen w-screen flex items-center justify-center text-slate-500">Loading...</div>;
  if (!user) return <Navigate to="/login" replace />;
  return children;
};

function App() {
  return (
    <div className="App">
      <I18nProvider>
      <AuthProvider>
        <BrowserRouter>
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
    </div>
  );
}

export default App;
