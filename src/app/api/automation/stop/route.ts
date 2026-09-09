import { NextRequest, NextResponse } from 'next/server';
import { requireAdmin } from '@/lib/auth-middleware';
import * as db from '@/services/database';

export const dynamic = 'force-dynamic';

export async function POST(request: NextRequest) {
  const auth = await requireAdmin(request);
  if (auth instanceof NextResponse) return auth;

  const runningRuns = await db.getRunningRuns();
  if (runningRuns.length === 0) {
    return NextResponse.json({ error: 'No running job to stop' }, { status: 404 });
  }

  for (const run of runningRuns) {
    await db.updateKeywordRun(run.id, { status: 'CANCELLED', completedAt: new Date().toISOString() });
    await db.updateKeyword(run.keywordId, { status: 'CANCELLED' });
  }

  await db.logActivity({
    event: 'JOB_CANCELLED',
    entity: 'keyword_run',
    entityId: runningRuns[0].id,
    actor: auth.email || 'unknown',
    metadata: { keyword: runningRuns[0].keyword },
  });

  return NextResponse.json({ ok: true });
}
