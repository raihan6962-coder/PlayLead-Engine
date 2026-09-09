'use client';

import { AuthProvider, useAuth } from '@/components/auth/AuthProvider';
import LoginPage from '@/components/auth/LoginPage';
import AppShell from '@/components/layout/AppShell';
import OutreachPage from '@/components/outreach/OutreachPage';

function Page() {
  const { user, loading } = useAuth();
  if (loading) return <div className="min-h-screen flex items-center justify-center bg-bg text-txt-3">Loading...</div>;
  if (!user) return <LoginPage />;
  return <AppShell><OutreachPage /></AppShell>;
}

export default function Outreach() {
  return <AuthProvider><Page /></AuthProvider>;
}
