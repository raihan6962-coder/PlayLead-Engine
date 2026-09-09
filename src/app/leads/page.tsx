'use client';

import { AuthProvider, useAuth } from '@/components/auth/AuthProvider';
import LoginPage from '@/components/auth/LoginPage';
import AppShell from '@/components/layout/AppShell';
import LeadsPage from '@/components/leads/LeadsPage';

function Page() {
  const { user, loading } = useAuth();
  if (loading) return <div className="min-h-screen flex items-center justify-center bg-bg text-txt-3">Loading...</div>;
  if (!user) return <LoginPage />;
  return <AppShell><LeadsPage /></AppShell>;
}

export default function Leads() {
  return <AuthProvider><Page /></AuthProvider>;
}
