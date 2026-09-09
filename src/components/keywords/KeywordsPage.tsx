'use client';

import { useState, useEffect } from 'react';

interface Keyword {
  id: string;
  keyword: string;
  targetLeads: number;
  status: string;
  maxRating: number;
  maxInstalls: number;
  scheduledDate: string;
  actualCount: number;
  createdAt: string;
}

export default function KeywordsPage() {
  const [keywords, setKeywords] = useState<Keyword[]>([]);
  const [loading, setLoading] = useState(true);
  const [newKeyword, setNewKeyword] = useState('');
  const [target, setTarget] = useState(300);
  const [maxRating, setMaxRating] = useState(2.5);
  const [maxInstalls, setMaxInstalls] = useState(5000);
  const [creating, setCreating] = useState(false);

  useEffect(() => { fetchKeywords(); }, []);

  async function fetchKeywords() {
    try {
      const r = await fetch('/api/keywords');
      const d = await r.json();
      if (d.ok) setKeywords(d.data);
    } catch (e) { console.error(e); }
    setLoading(false);
  }

  async function createKeyword(e: React.FormEvent) {
    e.preventDefault();
    if (!newKeyword.trim()) return;
    setCreating(true);
    try {
      await fetch('/api/keywords', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          keyword: newKeyword.trim(),
          targetLeads: target,
          maxRating,
          maxInstalls,
        }),
      });
      setNewKeyword('');
      fetchKeywords();
    } catch (e) { console.error(e); }
    setCreating(false);
  }

  async function startKeyword(id: string) {
    try {
      await fetch('/api/automation/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keywordId: id, action: 'start' }),
      });
      fetchKeywords();
    } catch (e) { console.error(e); }
  }

  async function cancelKeyword(id: string) {
    try {
      await fetch('/api/automation/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keywordId: id, action: 'cancel' }),
      });
      fetchKeywords();
    } catch (e) { console.error(e); }
  }

  function statusColor(s: string) {
    const map: Record<string, string> = {
      DRAFT: 'bg-txt-3/10 text-txt-3 border border-txt-3/20',
      SCHEDULED: 'bdg-pend',
      RUNNING: 'bdg-sent',
      COMPLETED: 'bg-accent/10 text-accent-light border border-accent/20',
      FAILED: 'bg-brand-red/10 text-brand-red border border-brand-red/20',
      CANCELLED: 'bg-txt-3/10 text-txt-3 border border-txt-3/20',
    };
    return map[s] || 'bdg-pend';
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold tracking-tight">
        <span className="text-accent">Keywords</span>
      </h1>

      {/* Create Form */}
      <div className="card">
        <div className="card-head">Add New Keyword</div>
        <form onSubmit={createKeyword} className="flex flex-wrap gap-3 items-end">
          <div className="flex-1 min-w-[200px]">
            <label className="form-label mb-1.5 block">Keyword</label>
            <input
              className="form-input"
              value={newKeyword}
              onChange={e => setNewKeyword(e.target.value)}
              placeholder="e.g. budget tracker, meditation app"
              required
            />
          </div>
          <div className="w-28">
            <label className="form-label mb-1.5 block">Target</label>
            <input
              className="form-input"
              type="number"
              value={target}
              onChange={e => setTarget(parseInt(e.target.value) || 300)}
              min={10} max={5000}
            />
          </div>
          <div className="w-28">
            <label className="form-label mb-1.5 block">Max Rating</label>
            <input
              className="form-input"
              type="number"
              step="0.1"
              value={maxRating}
              onChange={e => setMaxRating(parseFloat(e.target.value) || 2.5)}
              min={0} max={5}
            />
          </div>
          <div className="w-32">
            <label className="form-label mb-1.5 block">Max Installs</label>
            <input
              className="form-input"
              type="number"
              value={maxInstalls}
              onChange={e => setMaxInstalls(parseInt(e.target.value) || 5000)}
              min={0}
            />
          </div>
          <button type="submit" className="btn-primary" disabled={creating}>
            {creating ? 'Adding...' : '+ Add Keyword'}
          </button>
        </form>
      </div>

      {/* Keywords List */}
      <div className="card">
        <div className="card-head">All Keywords ({keywords.length})</div>
        {loading ? (
          <div className="text-center py-8 text-txt-3 text-sm">Loading...</div>
        ) : keywords.length === 0 ? (
          <div className="text-center py-8 text-txt-3 text-sm">No keywords yet. Add one above.</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">#</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Keyword</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Target</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Found</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Status</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Actions</th>
                </tr>
              </thead>
              <tbody>
                {keywords.map((kw, i) => (
                  <tr key={kw.id} className="border-b border-border/50 hover:bg-surface/50">
                    <td className="py-3 px-4 font-mono text-txt-3">{i + 1}</td>
                    <td className="py-3 px-4 font-medium">{kw.keyword}</td>
                    <td className="py-3 px-4 font-mono">{kw.targetLeads}</td>
                    <td className="py-3 px-4 font-mono">{kw.actualCount}</td>
                    <td className="py-3 px-4">
                      <span className={`bdg ${statusColor(kw.status)}`}>{kw.status}</span>
                    </td>
                    <td className="py-3 px-4">
                      <div className="flex gap-2">
                        {(kw.status === 'DRAFT' || kw.status === 'SCHEDULED') && (
                          <button
                            onClick={() => startKeyword(kw.id)}
                            className="btn-primary text-[10px] py-1.5 px-3"
                          >
                            ▶ Start
                          </button>
                        )}
                        {kw.status === 'RUNNING' && (
                          <button
                            onClick={() => cancelKeyword(kw.id)}
                            className="btn-danger text-[10px] py-1.5 px-3"
                          >
                            ⏹ Stop
                          </button>
                        )}
                      </div>
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
