import { NextRequest, NextResponse } from 'next/server';
import * as db from '@/services/database';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const runningRuns = await db.getRunningRuns();

  if (runningRuns.length === 0) {
    return NextResponse.json({
      ok: true,
      running: false,
      phase: 'idle',
      keyword: '',
      leadsFound: 0,
      emailsSent: 0,
    });
  }

  const run = runningRuns[0];
  return NextResponse.json({
    ok: true,
    running: true,
    phase: run.phase,
    keyword: run.keyword,
    leadsFound: run.actualLeads,
    targetLeads: run.targetLeads,
    emailsSent: run.emailsSent,
    searchQueries: run.searchQueriesUsed,
    startTime: run.startedAt,
    isOverdue: run.overdueNotified,
  });
}
