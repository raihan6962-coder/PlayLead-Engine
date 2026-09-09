'use client';

import { useState, useEffect } from 'react';

interface OutreachMessage {
  id: string;
  leadId: string;
  subject: string;
  status: string;
  sentAt: string | null;
  createdAt: string;
}

export default function OutreachPage() {
  const [messages, setMessages] = useState<OutreachMessage[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => { fetchMessages(); }, []);

  async function fetchMessages() {
    try {
      const r = await fetch('/api/outreach/send', { method: 'GET' });
      const d = await r.json();
      if (d.ok) setMessages(d.data || []);
    } catch (e) { console.error(e); }
    setLoading(false);
  }

  const sent = messages.filter(m => m.status === 'sent').length;
  const queued = messages.filter(m => m.status === 'queued').length;
  const failed = messages.filter(m => m.status === 'failed').length;

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold tracking-tight">
        <span className="text-accent">Outreach</span>
      </h1>

      <div className="grid grid-cols-3 gap-3.5">
        <div className="stat-card c-green">
          <div className="text-[9px] font-bold tracking-[2px] uppercase text-txt-3 mb-3">Sent</div>
          <div className="text-[36px] font-extrabold tracking-[-1.5px] leading-none text-brand-green">{sent}</div>
        </div>
        <div className="stat-card c-amber">
          <div className="text-[9px] font-bold tracking-[2px] uppercase text-txt-3 mb-3">Queued</div>
          <div className="text-[36px] font-extrabold tracking-[-1.5px] leading-none text-amber">{queued}</div>
        </div>
        <div className="stat-card c-red">
          <div className="text-[9px] font-bold tracking-[2px] uppercase text-txt-3 mb-3">Failed</div>
          <div className="text-[36px] font-extrabold tracking-[-1.5px] leading-none text-brand-red">{failed}</div>
        </div>
      </div>

      <div className="card">
        <div className="card-head">Outreach Messages</div>
        {loading ? (
          <div className="text-center py-8 text-txt-3 text-sm">Loading...</div>
        ) : messages.length === 0 ? (
          <div className="text-center py-8 text-txt-3 text-sm">
            No outreach messages yet. Start an automation to begin sending.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Subject</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Status</th>
                  <th className="text-left py-3 px-4 text-[9px] font-bold tracking-wider uppercase text-txt-3">Sent At</th>
                </tr>
              </thead>
              <tbody>
                {messages.map(msg => (
                  <tr key={msg.id} className="border-b border-border/50 hover:bg-surface/50">
                    <td className="py-3 px-4 max-w-[250px] truncate">{msg.subject}</td>
                    <td className="py-3 px-4">
                      <span className={`bdg ${msg.status === 'sent' ? 'bdg-sent' : msg.status === 'failed' ? 'bg-brand-red/10 text-brand-red border border-brand-red/20' : 'bdg-pend'}`}>
                        {msg.status}
                      </span>
                    </td>
                    <td className="py-3 px-4 font-mono text-txt-3">
                      {msg.sentAt ? new Date(msg.sentAt).toLocaleString() : '—'}
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
