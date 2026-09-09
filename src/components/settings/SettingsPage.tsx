'use client';

import { useState, useEffect } from 'react';

export default function SettingsPage() {
  const [settings, setSettings] = useState({
    dailyStartTime: '09:00',
    dailyEndTime: '18:00',
    timezone: 'UTC',
    schedulerEnabled: false,
    maxEmailsPerDay: 50,
    minSendDelay: 40,
    maxSendDelay: 60,
    forwardingEnabled: false,
    forwardingEmail: '',
    googleSheetUrl: '',
    googleSheetWebAppUrl: '',
    telegramBotToken: '',
    telegramChatId: '',
  });
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [telegramTest, setTelegramTest] = useState<{ ok?: boolean; msg?: string } | null>(null);

  useEffect(() => { fetchSettings(); }, []);

  async function fetchSettings() {
    try {
      const r = await fetch('/api/settings');
      const d = await r.json();
      if (d.ok && d.data) {
        setSettings(prev => ({ ...prev, ...d.data }));
      }
    } catch (e) { console.error(e); }
  }

  async function saveSettings() {
    setSaving(true);
    try {
      await fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(settings),
      });
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    } catch (e) { console.error(e); }
    setSaving(false);
  }

  async function testTelegram() {
    setTelegramTest(null);
    try {
      const r = await fetch('/api/telegram/test', { method: 'POST' });
      const d = await r.json();
      setTelegramTest(d);
    } catch (e: any) {
      setTelegramTest({ ok: false, msg: e.message });
    }
  }

  const inputCls = 'form-input';

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold tracking-tight">
        <span className="text-accent">Settings</span>
      </h1>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Scheduler */}
        <div className="card">
          <div className="card-head">Scheduler</div>
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="form-label mb-1.5 block">Start Time</label>
                <input className={inputCls} type="time" value={settings.dailyStartTime}
                  onChange={e => setSettings(p => ({ ...p, dailyStartTime: e.target.value }))} />
              </div>
              <div>
                <label className="form-label mb-1.5 block">End Time</label>
                <input className={inputCls} type="time" value={settings.dailyEndTime}
                  onChange={e => setSettings(p => ({ ...p, dailyEndTime: e.target.value }))} />
              </div>
            </div>
            <div>
              <label className="form-label mb-1.5 block">Timezone</label>
              <input className={inputCls} value={settings.timezone}
                onChange={e => setSettings(p => ({ ...p, timezone: e.target.value }))} />
            </div>
            <label className="flex items-center gap-3 cursor-pointer">
              <input type="checkbox" checked={settings.schedulerEnabled}
                onChange={e => setSettings(p => ({ ...p, schedulerEnabled: e.target.checked }))}
                className="w-4 h-4 accent-accent" />
              <span className="text-sm">Enable daily scheduler</span>
            </label>
          </div>
        </div>

        {/* Throttling */}
        <div className="card">
          <div className="card-head">Send Throttling</div>
          <div className="space-y-4">
            <div>
              <label className="form-label mb-1.5 block">Max Emails Per Day</label>
              <input className={inputCls} type="number" value={settings.maxEmailsPerDay}
                onChange={e => setSettings(p => ({ ...p, maxEmailsPerDay: parseInt(e.target.value) || 50 }))} />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="form-label mb-1.5 block">Min Delay (sec)</label>
                <input className={inputCls} type="number" value={settings.minSendDelay}
                  onChange={e => setSettings(p => ({ ...p, minSendDelay: parseInt(e.target.value) || 40 }))} />
              </div>
              <div>
                <label className="form-label mb-1.5 block">Max Delay (sec)</label>
                <input className={inputCls} type="number" value={settings.maxSendDelay}
                  onChange={e => setSettings(p => ({ ...p, maxSendDelay: parseInt(e.target.value) || 60 }))} />
              </div>
            </div>
          </div>
        </div>

        {/* Google Sheet */}
        <div className="card">
          <div className="card-head">Google Sheet</div>
          <div className="space-y-4">
            <div>
              <label className="form-label mb-1.5 block">Sheet URL</label>
              <input className={inputCls} type="url" value={settings.googleSheetUrl}
                onChange={e => setSettings(p => ({ ...p, googleSheetUrl: e.target.value }))}
                placeholder="https://docs.google.com/spreadsheets/d/..." />
            </div>
            <div>
              <label className="form-label mb-1.5 block">Apps Script Web App URL</label>
              <input className={inputCls} type="url" value={settings.googleSheetWebAppUrl}
                onChange={e => setSettings(p => ({ ...p, googleSheetWebAppUrl: e.target.value }))}
                placeholder="https://script.google.com/macros/s/.../exec" />
            </div>
          </div>
        </div>

        {/* Telegram */}
        <div className="card">
          <div className="card-head">Telegram Notifications</div>
          <div className="space-y-4">
            <div>
              <label className="form-label mb-1.5 block">Bot Token</label>
              <input className={inputCls} type="password" value={settings.telegramBotToken}
                onChange={e => setSettings(p => ({ ...p, telegramBotToken: e.target.value }))}
                placeholder="123456:ABC..." />
            </div>
            <div>
              <label className="form-label mb-1.5 block">Chat ID</label>
              <input className={inputCls} value={settings.telegramChatId}
                onChange={e => setSettings(p => ({ ...p, telegramChatId: e.target.value }))}
                placeholder="-1001234567890" />
            </div>
            <div className="flex items-center gap-3">
              <button className="btn-ghost" onClick={testTelegram}>Test Connection</button>
              {telegramTest && (
                <span className={`text-xs ${telegramTest.ok ? 'text-brand-green' : 'text-brand-red'}`}>
                  {telegramTest.ok ? '✓ Sent!' : telegramTest.msg}
                </span>
              )}
            </div>
          </div>
        </div>

        {/* Forwarding */}
        <div className="card">
          <div className="card-head">Reply Forwarding</div>
          <div className="space-y-4">
            <label className="flex items-center gap-3 cursor-pointer">
              <input type="checkbox" checked={settings.forwardingEnabled}
                onChange={e => setSettings(p => ({ ...p, forwardingEnabled: e.target.checked }))}
                className="w-4 h-4 accent-accent" />
              <span className="text-sm">Enable reply forwarding</span>
            </label>
            <div>
              <label className="form-label mb-1.5 block">Forwarding Email</label>
              <input className={inputCls} type="email" value={settings.forwardingEmail}
                onChange={e => setSettings(p => ({ ...p, forwardingEmail: e.target.value }))}
                placeholder="forward@example.com" />
            </div>
          </div>
        </div>
      </div>

      <div className="flex items-center gap-4">
        <button className="btn-primary" onClick={saveSettings} disabled={saving}>
          {saving ? 'Saving...' : '💾 Save Settings'}
        </button>
        {saved && <span className="text-brand-green text-xs">✓ Saved!</span>}
      </div>
    </div>
  );
}
