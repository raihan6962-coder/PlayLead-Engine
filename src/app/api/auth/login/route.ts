import { NextRequest, NextResponse } from 'next/server';
import { getFirebaseAdmin } from '@/lib/firebase-admin';

export const dynamic = 'force-dynamic';

export async function POST(request: NextRequest) {
  try {
    const { email, password } = await request.json();

    if (!email || !password) {
      return NextResponse.json({ error: 'Email and password required' }, { status: 400 });
    }

    if (email !== 'admin@playlead.io' || password !== 'changeme') {
      return NextResponse.json({ error: 'Invalid credentials' }, { status: 401 });
    }

    getFirebaseAdmin();
    const admin = await import('firebase-admin/auth');

    let userRecord;
    try {
      userRecord = await admin.getAuth().getUserByEmail(email);
    } catch {
      userRecord = await admin.getAuth().createUser({
        email,
        password,
        displayName: 'Admin',
      });
    }

    const customToken = await admin.getAuth().createCustomToken(userRecord.uid);

    const response = NextResponse.json({
      ok: true,
      uid: userRecord.uid,
      email: userRecord.email,
      customToken,
    });

    return response;
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : 'Login failed';
    return NextResponse.json({ error: msg }, { status: 500 });
  }
}
