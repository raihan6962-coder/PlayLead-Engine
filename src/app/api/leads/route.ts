import { NextRequest, NextResponse } from 'next/server';
import * as db from '@/services/database';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const status = searchParams.get('status') || undefined;
  const keyword = searchParams.get('keyword') || undefined;
  const pageSize = parseInt(searchParams.get('limit') || '50');

  const result = await db.getLeads(undefined, { status, keyword, pageSize });
  return NextResponse.json({ ok: true, data: result.leads, total: result.total });
}
