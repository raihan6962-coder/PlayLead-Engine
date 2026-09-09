import { NextRequest, NextResponse } from 'next/server';
import { requireAdmin } from '@/lib/auth-middleware';
import * as db from '@/services/database';
import { keywordSchema } from '@/lib/validation';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const auth = await requireAdmin(request);
  if (auth instanceof NextResponse) return auth;

  const keywords = await db.getKeywords(auth.uid!);
  return NextResponse.json({ ok: true, data: keywords });
}

export async function POST(request: NextRequest) {
  const auth = await requireAdmin(request);
  if (auth instanceof NextResponse) return auth;

  const body = await request.json();
  const parsed = keywordSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.flatten().fieldErrors }, { status: 400 });
  }

  const id = await db.createKeyword({
    userId: auth.uid!,
    keyword: parsed.data.keyword,
    templateId: '',
    targetLeads: parsed.data.targetLeads,
    scheduledDate: parsed.data.scheduledDate || new Date().toISOString().split('T')[0],
    status: 'DRAFT',
    maxRating: parsed.data.maxRating,
    maxInstalls: parsed.data.maxInstalls,
    startedAt: null,
    completedAt: null,
    actualCount: 0,
    searchQueries: [],
  });

  await db.logActivity({
    event: 'KEYWORD_CREATED',
    entity: 'keyword',
    entityId: id,
    actor: auth.email || 'unknown',
    metadata: { keyword: parsed.data.keyword },
  });

  return NextResponse.json({ ok: true, id });
}
