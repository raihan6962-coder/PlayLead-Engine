'use client';

import { useState, useEffect } from 'react';

interface ActivityLog {
  id: string;
  event: string;
  entity: string;
  entityId: string;
  actor: string;
  metadata: Record<string, unknown>;
  createdAt: string;
}

export default function AnalyticsPage() {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => { fetchAnalytics(); }, []);

  async function fetchAnalytics() {
    try {
      const r = await fetch('/api/analytics');
      const d = await r.json();
      if (d.ok) setData(d.data);
    } catch (e) { console.error(e); }
    setLoading(false);
  }

  if (loading) return <div className="text-center py-12 text-txt-3">Loading analytics...</div>;

  const ov = data?.overview || {};

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold tracking-tight">
        <span className="text-accent">Analytics</span>
      </h1>

      <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-3.5">
        {[
          { label: 'Total Leads', value: ov.totalLeads, color: 'c-accent' },
          { label: 'Qualified', value: ov.qualifiedLeads, color: 'c-green' },
          { label: 'Emails Sent', value: ov.emailsSent, color: 'c-blue' },
          { label: 'Failed', value: ov.emailsFailed, color: 'c-red' },
          { label: 'Replies', value: ov.replies, color: 'c-cyan' },
          { label: 'Human Replies', value: ov.humanReplies, color: 'c-pink' },
        ].map(card => (
          <div key={card.label} className={`stat-card ${card.color}`}>
            <div className="text-[9px] font-bold tracking-[2px] uppercase text-txt-3 mb-3">{card.label}</div>
            <div className="text-[32px] font-extrabold tracking-[-1px] leading-none">{card.value ?? 0}</div>
          </div>
        ))}
      </div>

      {/* Activity Log */}
      <div className="card">
        <div className="card-head">Recent Activity</div>
        {!data?.recentActivity?.length ? (
          <div className="text-center py-6 text-txt-3 text-xs">No activity logged yet.</div>
        ) : (
          <div className="space-y-1.5 max-h-[400px] overflow-y-auto">
            {data.recentActivity.map((log: ActivityLog) => (
              <div key={log.id} className="flex items-start gap-3 py-2 px-3 rounded-lg hover:bg-surface text-xs">
                <span className="font-mono text-txt-3 whitespace-nowrap">
                  {new Date(log.createdAt).toLocaleTimeString()}
                </span>
                <span className="flex-1">
                  <span className="font-semibold text-txt-2">{log.event}</span>
                  <span className="text-txt-3"> on {log.entity}</span>
                  {'keyword' in (log.metadata || {}) && (
                    <span className="text-accent-light ml-1">({String((log.metadata as any).keyword)})</span>
                  )}
                </span>
                <span className="text-txt-3">{log.actor}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
