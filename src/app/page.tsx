'use client';

import { AuthProvider, useAuth } from '@/components/auth/AuthProvider';
import LoginPage from '@/components/auth/LoginPage';
import AppShell from '@/components/layout/AppShell';
import DashboardPage from '@/components/dashboard/DashboardPage';

function Dashboard() {
  const { user, loading } = useAuth();

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-bg">
        <div className="text-txt-3 text-sm">Loading...</div>
      </div>
    );
  }

  if (!user) return <LoginPage />;

  return (
    <AppShell>
      <DashboardPage />
    </AppShell>
  );
}

export default function Home() {
  return (
    <AuthProvider>
      <Dashboard />
    </AuthProvider>
  );
}
