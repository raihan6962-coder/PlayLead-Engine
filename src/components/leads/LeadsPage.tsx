'use client';

import { useState, useEffect } from 'react';

interface Lead {
  id: string;
  appName: string;
  developer: string;
  email: string;
  category: string;
  installs: number;
  score: number | null;
  url: string;
  keyword: string;
  status: string;
  emailSent: boolean;
  createdAt: string;
}

export default function LeadsPage() {
  const [leads, setLeads] = useState<Lead[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('all');

  useEffect(() => { fetchLeads(); }, [filter]);

  async function fetchLeads() {
    try {
      const params = new URLSearchParams();
      if (filter !== 'all') params.set('status', filter);
      const r = await fetch(`/api/leads?${params}`);
      const d = await r.json();
      if (d.ok) setLeads(d.data);
    } catch (e) { console.error(e); }
    setLoading(false);
  }

  function exportCSV() {
    if (!leads.length) return;
    const h = ['App Name', 'Developer', 'Email', 'Category', 'Installs', 'Score', 'Keyword', 'Status'];
    const rows = leads.map(l => [l.appName, l.developer, l.email, l.category, l.installs, l.score ?? '', l.keyword, l.status]
      .map(v => `"${String(v || '').replace(/"/g, '""')}"`));
    const csv = [h, ...rows].map(r => r.join(',')).join('\n');
    const a = document.createElement('a');
    a.href = 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv);
    a.download = `leads_${Date.now()}.csv`;
    a.click();
  }

  const filters = ['all', 'qualified', 'emailed', 'replied', 'bounced', 'unsubscribed'];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <h1 className="text-2xl font-bold tracking-tight">
          <span className="text-accent">Leads</span>
          <span className="text-lg font-normal text-txt-3 ml-2">({leads.length})</span>
        </h1>
        <button className="btn-ghost" onClick={exportCSV}>⬇ Export CSV</button>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-2">
        {filters.map(f => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`px-3 py-1.5 rounded-lg text-[11px] font-mono transition-colors ${
              filter === f
                ? 'bg-accent/15 text-accent-light border border-accent/25'
                : 'bg-card border border-border text-txt-3 hover:text-txt-2'
            }`}
          >
            {f.charAt(0).toUpperCase() + f.slice(1)}
          </button>
        ))}
      </div>

      {/* Table */}
      <div className="card">
        {loading ? (
          <div className="text-center py-8 text-txt-3 text-sm">Loading...</div>
        ) : leads.length === 0 ? (
          <div className="text-center py-8 text-txt-3 text-sm">No leads found.</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">#</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">App</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Developer</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Email</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Category</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Installs</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Score</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Status</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Link</th>
                </tr>
              </thead>
              <tbody>
                {leads.map((lead, i) => (
                  <tr key={lead.id} className="border-b border-border/50 hover:bg-surface/50">
                    <td className="py-3 px-4 font-mono text-txt-3">{i + 1}</td>
                    <td className="py-3 px-4 font-medium max-w-[150px] truncate">{lead.appName}</td>
                    <td className="py-3 px-4 text-txt-2 max-w-[110px] truncate">{lead.developer}</td>
                    <td className="py-3 px-4 font-mono text-accent-light text-[10.5px]">{lead.email}</td>
                    <td className="py-3 px-4"><span className="bdg bdg-cat">{lead.category || '—'}</span></td>
                    <td className="py-3 px-4 font-mono">{lead.installs.toLocaleString()}</td>
                    <td className="py-3 px-4 font-mono">{lead.score != null ? lead.score.toFixed(1) : 'new'}</td>
                    <td className="py-3 px-4">
                      <span className={`bdg ${lead.emailSent ? 'bdg-sent' : 'bdg-pend'}`}>
                        {lead.emailSent ? '✓ Sent' : lead.status}
                      </span>
                    </td>
                    <td className="py-3 px-4">
                      <a href={lead.url} target="_blank" rel="noopener noreferrer" className="text-txt-3 hover:text-accent-light transition-colors">
                        ↗
                      </a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
