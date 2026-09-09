'use client';

import { useState } from 'react';
import { useAuth } from '@/components/auth/AuthProvider';

export default function LoginPage() {
  const { signIn } = useAuth();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      await signIn(email, password);
    } catch (e: any) {
      setError(e.message || 'Login failed');
    }
    setLoading(false);
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-bg p-4">
      <div className="w-full max-w-md">
        <div className="text-center mb-8">
          <h1 className="text-3xl font-extrabold tracking-tight text-txt">
            Play<span className="text-accent">Lead</span>
          </h1>
          <p className="text-txt-3 text-xs mt-2 font-mono tracking-widest uppercase">Automation Engine</p>
        </div>

        <div className="card">
          <div className="card-head">Sign In</div>

          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label className="form-label mb-2 block">Email</label>
              <input
                className="form-input"
                type="email"
                value={email}
                onChange={e => setEmail(e.target.value)}
                placeholder="admin@playlead.io"
                required
              />
            </div>
            <div>
              <label className="form-label mb-2 block">Password</label>
              <input
                className="form-input"
                type="password"
                value={password}
                onChange={e => setPassword(e.target.value)}
                placeholder="Enter password"
                required
              />
            </div>

            {error && (
              <div className="bg-brand-red/5 border border-brand-red/20 rounded-[10px] p-3 text-brand-red text-xs">
                {error}
              </div>
            )}

            <button
              type="submit"
              className="btn-primary w-full"
              disabled={loading}
            >
              {loading ? 'Signing in...' : 'Sign In'}
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}
