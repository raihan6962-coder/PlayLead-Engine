'use client';

import { ReactNode } from 'react';
import Sidebar from './Sidebar';

export default function AppShell({ children }: { children: ReactNode }) {
  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <main className="flex-1 md:ml-60 pt-14 md:pt-0 p-4 md:p-8 animate-fade-up">
        {children}
      </main>
    </div>
  );
}
