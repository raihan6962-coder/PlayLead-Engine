import { NextRequest, NextResponse } from 'next/server';
import * as db from '@/services/database';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const limit = parseInt(searchParams.get('limit') || '50');

  const replies = await db.getReplies({ limit });
  return NextResponse.json({ ok: true, data: replies });
}
