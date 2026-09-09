'use client';

import { useState, useEffect } from 'react';

export default function DashboardPage() {
  const [stats, setStats] = useState({
    totalLeads: 0,
    qualifiedLeads: 0,
    emailsSent: 0,
    emailsFailed: 0,
    replies: 0,
    humanReplies: 0,
  });
  const [loading, setLoading] = useState(true);
  const [automation, setAutomation] = useState<any>(null);
  const [keywords, setKeywords] = useState<any[]>([]);

  useEffect(() => {
    fetchDashboard();
    const interval = setInterval(fetchDashboard, 5000);
    return () => clearInterval(interval);
  }, []);

  async function fetchDashboard() {
    try {
      const [analyticsRes, statusRes, keywordsRes] = await Promise.all([
        fetch('/api/analytics'),
        fetch('/api/automation/status'),
        fetch('/api/keywords'),
      ]);

      const analytics = await analyticsRes.json();
      const status = await statusRes.json();
      const kws = await keywordsRes.json();

      if (analytics.ok) setStats(analytics.data.overview);
      if (status.ok) setAutomation(status);
      if (kws.ok) setKeywords(kws.data);
    } catch (e) {
      console.error('Dashboard fetch error:', e);
    }
    setLoading(false);
  }

  const statCards = [
    { label: 'Total Leads', value: stats.totalLeads, color: 'c-accent', sub: 'discovered' },
    { label: 'Qualified', value: stats.qualifiedLeads, color: 'c-green', sub: 'ready for outreach' },
    { label: 'Emails Sent', value: stats.emailsSent, color: 'c-blue', sub: 'outreach delivered' },
    { label: 'Replies', value: stats.replies, color: 'c-cyan', sub: `${stats.humanReplies} human` },
  ];

  const activeKeywords = keywords.filter(k => k.status === 'RUNNING' || k.status === 'SCHEDULED');
  const completedKeywords = keywords.filter(k => k.status === 'COMPLETED');

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-2xl font-bold tracking-tight">
          <span className="text-accent">Dashboard</span>
        </h1>
        <div className="text-xs text-txt-3 font-mono">
          {new Date().toLocaleTimeString()}
        </div>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3.5">
        {statCards.map(card => (
          <div key={card.label} className={`stat-card ${card.color}`}>
            <div className="text-[9px] font-bold tracking-[2px] uppercase text-txt-3 mb-3">{card.label}</div>
            <div className="text-[36px] font-extrabold tracking-[-1.5px] leading-none">
              {loading ? '—' : card.value}
            </div>
            <div className="text-[10px] text-txt-3 mt-2 font-mono">{card.sub}</div>
          </div>
        ))}
      </div>

      {/* Automation Status */}
      <div className="card">
        <div className="card-head">Automation Status</div>
        {automation?.running ? (
          <div className="space-y-4">
            <div className="flex items-center gap-3">
              <div className="w-2.5 h-2.5 rounded-full bg-brand-green animate-pulse-dot" />
              <span className="text-sm font-semibold text-brand-green">Running</span>
              <span className="text-xs text-txt-3 font-mono">{automation.phase}</span>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div>
                <div className="text-[9px] font-bold tracking-[2px] uppercase text-txt-3 mb-1">Keyword</div>
                <div className="text-sm font-medium">{automation.keyword}</div>
              </div>
              <div>
                <div className="text-[9px] font-bold tracking-[2px] uppercase text-txt-3 mb-1">Leads</div>
                <div className="text-sm font-mono">{automation.leadsFound} / {automation.targetLeads || '—'}</div>
              </div>
              <div>
                <div className="text-[9px] font-bold tracking-[2px] uppercase text-txt-3 mb-1">Emails Sent</div>
                <div className="text-sm font-mono">{automation.emailsSent}</div>
              </div>
              <div>
                <div className="text-[9px] font-bold tracking-[2px] uppercase text-txt-3 mb-1">Started</div>
                <div className="text-sm font-mono text-txt-2">
                  {automation.startTime ? new Date(automation.startTime).toLocaleTimeString() : '—'}
                </div>
              </div>
            </div>
          </div>
        ) : (
          <div className="text-center py-8 text-txt-3 text-sm">
            No automation currently running. Go to <strong className="text-accent">Keywords</strong> to start one.
          </div>
        )}
      </div>

      {/* Keywords Overview */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3.5">
        <div className="card">
          <div className="card-head">Active Keywords ({activeKeywords.length})</div>
          {activeKeywords.length === 0 ? (
            <div className="text-center py-6 text-txt-3 text-xs">No active keywords</div>
          ) : (
            <div className="space-y-2">
              {activeKeywords.slice(0, 5).map(kw => (
                <div key={kw.id} className="flex items-center justify-between py-2 px-3 bg-surface rounded-lg text-xs">
                  <span className="font-medium">{kw.keyword}</span>
                  <span className={`bdg ${kw.status === 'RUNNING' ? 'bdg-sent' : 'bdg-pend'}`}>
                    {kw.status}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="card">
          <div className="card-head">Recent Activity</div>
          <div className="text-center py-6 text-txt-3 text-xs">
            Activity logs appear here as the system runs.
          </div>
        </div>
      </div>
    </div>
  );
}
