'use client';

import { useState, useEffect, ReactNode } from 'react';
import { useAuth } from '@/components/auth/AuthProvider';
import Link from 'next/link';
import { usePathname } from 'next/navigation';

const navItems = [
  { href: '/', label: 'Dashboard', icon: '⚡' },
  { href: '/keywords', label: 'Keywords', icon: '🔑' },
  { href: '/leads', label: 'Leads', icon: '📋' },
  { href: '/outreach', label: 'Outreach', icon: '📧' },
  { href: '/analytics', label: 'Analytics', icon: '📊' },
  { href: '/settings', label: 'Settings', icon: '⚙️' },
];

export default function Sidebar() {
  const { user, signOut } = useAuth();
  const pathname = usePathname();
  const [mobileOpen, setMobileOpen] = useState(false);

  return (
    <>
      {/* Mobile header */}
      <div className="md:hidden fixed top-0 left-0 right-0 h-14 bg-bg2/90 backdrop-blur-xl border-b border-border z-[200] flex items-center justify-between px-4">
        <span className="font-extrabold text-base tracking-tight">
          Play<span className="text-accent">Lead</span>
        </span>
        <button
          onClick={() => setMobileOpen(!mobileOpen)}
          className="w-9 h-9 flex items-center justify-center rounded-lg bg-card border border-border text-txt text-lg"
        >
          ☰
        </button>
      </div>
      {mobileOpen && (
        <div
          className="md:hidden fixed inset-0 bg-black/50 z-[199]"
          onClick={() => setMobileOpen(false)}
        />
      )}

      {/* Sidebar */}
      <aside className={`
        fixed left-0 top-0 bottom-0 w-60 bg-bg2 border-r border-border flex flex-col z-[100]
        transition-transform duration-300
        ${mobileOpen ? 'translate-x-0' : '-translate-x-full'}
        md:translate-x-0
      `}>
        <div className="p-6 pb-5 border-b border-border">
          <div className="text-2xl font-extrabold tracking-tight">
            Play<span className="text-accent">Lead</span>
          </div>
          <div className="text-[9px] text-txt-3 tracking-[3px] uppercase mt-1.5 font-mono">
            Automation Engine
          </div>
        </div>

        <nav className="flex-1 p-3.5">
          {navItems.map(item => {
            const active = pathname === item.href;
            return (
              <Link
                key={item.href}
                href={item.href}
                onClick={() => setMobileOpen(false)}
                className={`
                  flex items-center gap-3 px-4 py-2.5 rounded-[10px] text-[13px] font-medium mb-1 transition-colors
                  ${active
                    ? 'bg-accent/10 text-accent-light border border-accent/20'
                    : 'text-txt-3 hover:bg-card hover:text-txt-2'
                  }
                `}
              >
                <span className="text-[15px] w-5 text-center">{item.icon}</span>
                {item.label}
              </Link>
            );
          })}
        </nav>

        <div className="p-3.5 border-t border-border">
          <div className="px-4 py-2.5 bg-card rounded-[10px] border border-border text-xs text-txt-2 flex items-center gap-2.5">
            <div className="w-2 h-2 rounded-full bg-brand-green" />
            {user?.email || 'Not signed in'}
          </div>
        </div>
      </aside>
    </>
  );
}
