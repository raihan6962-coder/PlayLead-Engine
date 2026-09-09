import { NextRequest, NextResponse } from 'next/server';
import { requireAdmin } from '@/lib/auth-middleware';
import * as db from '@/services/database';
import { outreachSchema } from '@/lib/validation';

export const dynamic = 'force-dynamic';

export async function POST(request: NextRequest) {
  const auth = await requireAdmin(request);
  if (auth instanceof NextResponse) return auth;

  const body = await request.json();
  const parsed = outreachSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.flatten().fieldErrors }, { status: 400 });
  }

  const template = await db.getDocById<any>('email_templates', parsed.data.templateId);
  if (!template) {
    return NextResponse.json({ error: 'Template not found' }, { status: 404 });
  }

  const accounts = await db.getSendingAccounts();
  const activeAccount = accounts.find(a => a.status === 'active');

  const sentCount = 0;
  const failedCount = 0;

  for (const leadId of parsed.data.leadIds) {
    const lead = await db.getDocById<any>('leads', leadId);
    if (!lead || lead.status === 'emailed' || lead.status === 'unsubscribed') continue;

    const idempotencyKey = `${leadId}:${parsed.data.templateId}:${Date.now()}`;

    await db.createOutreachMessage({
      leadId,
      templateId: parsed.data.templateId,
      keywordRunId: lead.keywordRunId || '',
      subject: template.subject.replace(/\{\{app_name\}\}/g, lead.appName),
      body: template.body
        .replace(/\{\{app_name\}\}/g, lead.appName)
        .replace(/\{\{developer\}\}/g, lead.developer)
        .replace(/\{\{category\}\}/g, lead.category),
      htmlBody: '',
      status: 'queued',
      sendingAccountId: activeAccount?.id || null,
      idempotencyKey,
      sentAt: null,
      error: null,
    });

    await db.updateLeadStatus(leadId, 'emailed');
  }

  await db.logActivity({
    event: 'OUTREACH_QUEUED',
    entity: 'outreach',
    entityId: parsed.data.templateId,
    actor: auth.email || 'unknown',
    metadata: { leadCount: parsed.data.leadIds.length },
  });

  return NextResponse.json({ ok: true, queued: parsed.data.leadIds.length });
}
