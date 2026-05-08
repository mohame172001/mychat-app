import React, { Suspense, lazy } from 'react';
import './App.css';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider, useAuth } from './context/AuthContext';
import { Toaster } from './components/ui/sonner';

import DashboardLayout from './components/layout/DashboardLayout';

const Landing = lazy(() => import('./pages/Landing'));
const Login = lazy(() => import('./pages/Login'));
const Signup = lazy(() => import('./pages/Signup'));
const Dashboard = lazy(() => import('./pages/Dashboard'));
const Automations = lazy(() => import('./pages/Automations'));
const FlowBuilder = lazy(() => import('./pages/FlowBuilder'));
const Broadcasting = lazy(() => import('./pages/Broadcasting'));
const Comments = lazy(() => import('./pages/Comments'));
const Settings = lazy(() => import('./pages/Settings'));
const DmAutomation = lazy(() => import('./pages/DmAutomation'));
const PrivacyPolicy = lazy(() => import('./pages/PrivacyPolicy'));
const Terms = lazy(() => import('./pages/Terms'));
const SystemHealth = lazy(() => import('./pages/SystemHealth'));
const Billing = lazy(() => import('./pages/Billing'));
const AdminConsole = lazy(() => import('./pages/admin/AdminConsole'));
const SpecificReplyDebug = lazy(() => import('./pages/admin/SpecificReplyDebug'));

const PageLoading = () => (
  <div className="min-h-screen w-full flex items-center justify-center text-slate-500">Loading...</div>
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
          <Suspense fallback={<PageLoading />}>
            <Routes>
              <Route path="/" element={<Landing />} />
              <Route path="/login" element={<Login />} />
              <Route path="/signup" element={<Signup />} />
              <Route path="/privacy" element={<PrivacyPolicy />} />
              <Route path="/terms" element={<Terms />} />
              <Route path="/app" element={<ProtectedRoute><DashboardLayout /></ProtectedRoute>}>
                <Route index element={<Dashboard />} />
                <Route path="automations" element={<Automations />} />
                <Route path="automations/:id" element={<FlowBuilder />} />
                <Route path="broadcasting" element={<Broadcasting />} />
                <Route path="comments" element={<Comments />} />
                <Route path="dm-automation" element={<DmAutomation />} />
                <Route path="settings" element={<Settings />} />
                <Route path="system-health" element={<SystemHealth />} />
                <Route path="billing" element={<Billing />} />
                <Route path="admin" element={<AdminConsole />} />
                <Route path="admin/specific-reply-debug" element={<SpecificReplyDebug />} />
              </Route>
            </Routes>
          </Suspense>
        </BrowserRouter>
      </AuthProvider>
    </div>
  );
}

export default App;
