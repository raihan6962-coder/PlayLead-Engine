import { NextResponse } from 'next/server';
import * as ai from '@/services/ai';
import * as telegram from '@/services/telegram';

export const dynamic = 'force-dynamic';

export async function GET() {
  const health = {
    ok: true,
    timestamp: new Date().toISOString(),
    services: {
      groq: ai.isConfigured(),
      telegram: telegram.isConfigured(),
      firebase: true,
    },
  };

  return NextResponse.json(health);
}
