import { NextRequest, NextResponse } from 'next/server';
import { requireAdmin } from '@/lib/auth-middleware';
import * as db from '@/services/database';
import { settingsSchema } from '@/lib/validation';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const auth = await requireAdmin(request);
  if (auth instanceof NextResponse) return auth;

  const settings = await db.getSettings(auth.uid!);
  return NextResponse.json({ ok: true, data: settings });
}

export async function POST(request: NextRequest) {
  const auth = await requireAdmin(request);
  if (auth instanceof NextResponse) return auth;

  const body = await request.json();
  const parsed = settingsSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json({ error: parsed.error.flatten().fieldErrors }, { status: 400 });
  }

  await db.upsertSettings(auth.uid!, parsed.data);

  await db.logActivity({
    event: 'CONFIG_CHANGED',
    entity: 'settings',
    entityId: auth.uid!,
    actor: auth.email || 'unknown',
    metadata: { changed: Object.keys(parsed.data) },
  });

  return NextResponse.json({ ok: true });
}
