import React, { Suspense, lazy, useEffect } from 'react';
import './App.css';
import { BrowserRouter, Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { AuthProvider, useAuth } from './context/AuthContext';
import { Toaster } from './components/ui/sonner';
import analytics from './lib/analytics';

import DashboardLayout from './components/layout/DashboardLayout';

// Phase 2.5: emit page_view on every route change. analytics.pageView is
// a no-op when PostHog isn't configured. The route is sanitized — OAuth
// code/state and tokens are stripped before send.
function PageViewTracker() {
  const location = useLocation();
  useEffect(() => {
    analytics.pageView(location.pathname + (location.search || ''));
  }, [location.pathname, location.search]);
  return null;
}

const Landing = lazy(() => import('./pages/Landing'));
const Login = lazy(() => import('./pages/Login'));
const Signup = lazy(() => import('./pages/Signup'));
const Dashboard = lazy(() => import('./pages/Dashboard'));
const Automations = lazy(() => import('./pages/Automations'));
const FlowBuilder = lazy(() => import('./pages/FlowBuilder'));
const Comments = lazy(() => import('./pages/Comments'));
const Settings = lazy(() => import('./pages/Settings'));
const DmAutomation = lazy(() => import('./pages/DmAutomation'));
const PrivacyPolicy = lazy(() => import('./pages/PrivacyPolicy'));
const Terms = lazy(() => import('./pages/Terms'));
const DataDeletion = lazy(() => import('./pages/DataDeletion'));
const Billing = lazy(() => import('./pages/Billing'));
const AdminConsole = lazy(() => import('./pages/admin/AdminConsole'));
const SpecificReplyDebug = lazy(() => import('./pages/admin/SpecificReplyDebug'));
const NotFound = lazy(() => import('./pages/NotFound'));

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
      <AuthProvider>
        <BrowserRouter>
          <Toaster position="top-right" />
          <PageViewTracker />
          <Suspense fallback={<PageLoading />}>
<Routes>
              <Route path="/" element={<Landing />} />
              <Route path="/login" element={<Login />} />
              <Route path="/signup" element={<Signup />} />
              <Route path="/privacy" element={<PrivacyPolicy />} />
              <Route path="/terms" element={<Terms />} />
              <Route path="/data-deletion" element={<DataDeletion />} />
              <Route path="/app" element={<ProtectedRoute><DashboardLayout /></ProtectedRoute>}>
                <Route index element={<Dashboard />} />
                <Route path="automations" element={<Automations />} />
                <Route path="automations/:id" element={<FlowBuilder />} />
                <Route path="comments" element={<Comments />} />
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
    </div>
  );
}

export default App;
