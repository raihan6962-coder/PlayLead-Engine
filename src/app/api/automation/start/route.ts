import { NextRequest, NextResponse } from 'next/server';
import { requireAdmin } from '@/lib/auth-middleware';
import * as db from '@/services/database';
import { isValidTransition } from '@/config/constants';

export const dynamic = 'force-dynamic';

export async function POST(request: NextRequest) {
  const auth = await requireAdmin(request);
  if (auth instanceof NextResponse) return auth;

  const { keywordId, action } = await request.json();

  if (!keywordId || !action) {
    return NextResponse.json({ error: 'keywordId and action required' }, { status: 400 });
  }

  const keyword = await db.getDocById<any>('keywords', keywordId);
  if (!keyword) {
    return NextResponse.json({ error: 'Keyword not found' }, { status: 404 });
  }

  if (action === 'start') {
    if (!isValidTransition(keyword.status, 'RUNNING')) {
      return NextResponse.json({ error: `Cannot start from status ${keyword.status}` }, { status: 400 });
    }

    // Check for existing running job
    const runningRuns = await db.getRunningRuns();
    if (runningRuns.length > 0) {
      return NextResponse.json({ error: 'Another job is already running' }, { status: 409 });
    }

    await db.updateKeyword(keywordId, { status: 'RUNNING', startedAt: new Date().toISOString() });

    // Create keyword run
    const runId = await db.createKeywordRun({
      keywordId,
      keyword: keyword.keyword,
      status: 'RUNNING' as any,
      phase: 'loading_sheet' as any,
      startedAt: new Date().toISOString(),
      completedAt: null,
      targetLeads: keyword.targetLeads,
      actualLeads: 0,
      totalDiscovered: 0,
      totalRejected: 0,
      totalDuplicates: 0,
      emailsSent: 0,
      emailsFailed: 0,
      searchQueriesUsed: [],
      expectedEndTime: null,
      overdueNotified: false,
      error: null,
    });

    // Start automation in background
    startAutomationJob(runId, keywordId, keyword);

    await db.logActivity({
      event: 'KEYWORD_STARTED',
      entity: 'keyword',
      entityId: keywordId,
      actor: auth.email || 'unknown',
      metadata: { keyword: keyword.keyword, runId },
    });

    return NextResponse.json({ ok: true, runId });
  }

  if (action === 'cancel') {
    if (!isValidTransition(keyword.status, 'CANCELLED')) {
      return NextResponse.json({ error: `Cannot cancel from status ${keyword.status}` }, { status: 400 });
    }
    await db.updateKeyword(keywordId, { status: 'CANCELLED' });
    return NextResponse.json({ ok: true });
  }

  return NextResponse.json({ error: 'Invalid action' }, { status: 400 });
}

async function startAutomationJob(runId: string, keywordId: string, keyword: any) {
  // This runs server-side in the background
  // In production, this would be triggered by a separate worker process
  try {
    const { PlayStoreDiscovery, qualifyLead, normaliseLead } = await import('@/services/discovery');
    const { generateSearchQueries } = await import('@/services/ai');
    const { sendAutomationStarted, sendAutomationCompleted, sendAutomationFailed, sendTargetReached } = await import('@/services/telegram');

    await sendAutomationStarted(keyword.keyword, keyword.targetLeads);

    const discovery = new PlayStoreDiscovery();
    let qualifiedCount = 0;
    const searchQueries = [keyword.keyword];
    const seenIds = new Set<string>();
    const seenEmails = new Set<string>();

    // Get existing leads for dedup
    const existingLeads = await db.getLeadsByRun(keywordId);
    existingLeads.forEach(l => {
      seenIds.add(l.appId);
      seenEmails.add(l.normalisedEmail);
    });

    // Discovery loop
    for (let i = 0; i < searchQueries.length && qualifiedCount < keyword.targetLeads; i++) {
      const query = searchQueries[i];
      await db.updateKeywordRun(runId, { phase: 'scraping', searchQueriesUsed: searchQueries });

      const results = await discovery.search(query);

      for (const item of results) {
        if (seenIds.has(item.appId)) {
          await db.updateKeywordRun(runId, { totalDuplicates: (await db.getDocById<any>('keyword_runs', runId))?.totalDuplicates + 1 || 1 });
          continue;
        }
        seenIds.add(item.appId);

        const { qualified, reason } = qualifyLead(item, {
          maxRating: keyword.maxRating,
          maxInstalls: keyword.maxInstalls,
        });

        if (!qualified) {
          await db.updateKeywordRun(runId, { totalRejected: (await db.getDocById<any>('keyword_runs', runId))?.totalRejected + 1 || 1 });
          continue;
        }

        if (seenEmails.has(item.email.toLowerCase())) {
          await db.updateKeywordRun(runId, { totalDuplicates: (await db.getDocById<any>('keyword_runs', runId))?.totalDuplicates + 1 || 1 });
          continue;
        }
        seenEmails.add(item.email.toLowerCase());

        const leadData = normaliseLead(item, keyword.keyword, runId);
        await db.createLead(leadData);
        qualifiedCount++;

        await db.updateKeywordRun(runId, {
          actualLeads: qualifiedCount,
          totalDiscovered: (await db.getDocById<any>('keyword_runs', runId))?.totalDiscovered + 1 || 1,
        });

        if (qualifiedCount >= keyword.targetLeads) {
          await sendTargetReached(keyword.keyword, keyword.targetLeads, qualifiedCount);
          break;
        }
      }

      // Generate more queries if needed
      if (i === searchQueries.length - 1 && qualifiedCount < keyword.targetLeads) {
        const newQueries = await generateSearchQueries(keyword.keyword, searchQueries);
        searchQueries.push(...newQueries);
      }
    }

    // Complete
    await db.updateKeywordRun(runId, {
      status: qualifiedCount >= keyword.targetLeads ? 'COMPLETED' : 'PARTIAL',
      phase: 'done',
      completedAt: new Date().toISOString(),
    });
    await db.updateKeyword(keywordId, {
      status: qualifiedCount >= keyword.targetLeads ? 'COMPLETED' : 'PARTIAL',
      completedAt: new Date().toISOString(),
      actualCount: qualifiedCount,
    });

    await sendAutomationCompleted(keyword.keyword, qualifiedCount, 0);
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : 'Unknown error';
    await db.updateKeywordRun(runId, { status: 'FAILED', error: msg });
    await db.updateKeyword(keywordId, { status: 'FAILED' });

    const { sendAutomationFailed } = await import('@/services/telegram');
    await sendAutomationFailed(keyword.keyword, msg);
  }
}
