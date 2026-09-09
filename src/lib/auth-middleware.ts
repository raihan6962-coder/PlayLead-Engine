import { NextRequest, NextResponse } from 'next/server';
import { getAdminDb } from '@/lib/firebase-admin';

export interface AuthenticatedRequest extends NextRequest {
  uid?: string;
  email?: string;
}

export async function requireAuth(request: NextRequest): Promise<AuthenticatedRequest | NextResponse> {
  const authHeader = request.headers.get('authorization');
  const sessionCookie = request.cookies.get('session')?.value;

  const token = authHeader?.replace('Bearer ', '') || sessionCookie;

  if (!token) {
    return NextResponse.json({ error: 'Authentication required' }, { status: 401 });
  }

  try {
    const admin = await import('firebase-admin/auth');
    const decoded = await admin.getAuth().verifyIdToken(token);
    const req = request as AuthenticatedRequest;
    req.uid = decoded.uid;
    req.email = decoded.email;
    return req;
  } catch {
    return NextResponse.json({ error: 'Invalid or expired token' }, { status: 401 });
  }
}

export async function requireAdmin(request: NextRequest): Promise<AuthenticatedRequest | NextResponse> {
  const result = await requireAuth(request);
  if (result instanceof NextResponse) return result;

  const req = result as AuthenticatedRequest;
  if (!req.email || req.email !== process.env.ADMIN_EMAIL) {
    return NextResponse.json({ error: 'Admin access required' }, { status: 403 });
  }

  return req;
}
