import { NextRequest, NextResponse } from 'next/server';
import * as telegram from '@/services/telegram';

export const dynamic = 'force-dynamic';

export async function POST(request: NextRequest) {
  const result = await telegram.testConnection();
  if (result) {
    return NextResponse.json({ ok: true, message: 'Test notification sent successfully' });
  }
  return NextResponse.json({ ok: false, error: 'Failed to send. Check TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.' }, { status: 500 });
}
