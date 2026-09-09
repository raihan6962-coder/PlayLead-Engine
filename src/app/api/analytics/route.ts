import { NextRequest, NextResponse } from 'next/server';
import * as db from '@/services/database';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const overview = await db.getAnalyticsOverview();
  const logs = await db.getActivityLogs(50);
  const notifications = await db.getNotifications(20);

  return NextResponse.json({
    ok: true,
    data: {
      overview,
      recentActivity: logs,
      notifications,
    },
  });
}
