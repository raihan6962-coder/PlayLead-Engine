import { db } from '@/lib/firebase';
import {
  collection, doc, getDoc, getDocs, setDoc, updateDoc, deleteDoc,
  query, where, orderBy, limit, startAfter, onSnapshot,
  writeBatch, increment, serverTimestamp, Timestamp,
  DocumentSnapshot, QueryConstraint,
} from 'firebase/firestore';
import { COLLECTIONS, isValidTransition } from '@/config/constants';
import type {
  Keyword, KeywordRun, Lead, EmailTemplate, OutreachMessage,
  SendingAccount, Reply, Notification, ActivityLog, AnalyticsDaily,
  Settings, AutomationState,
} from '@/types';

// ── Generic CRUD ────────────────────────────────────────────────────────────

async function createDoc<T>(collectionName: string, data: T, id?: string): Promise<string> {
  const ref = id ? doc(db, collectionName, id) : doc(collection(db, collectionName));
  await setDoc(ref, { ...data, createdAt: new Date().toISOString() });
  return ref.id;
}

async function getDocById<T>(collectionName: string, id: string): Promise<T | null> {
  const snap = await getDoc(doc(db, collectionName, id));
  return snap.exists() ? ({ id: snap.id, ...snap.data() } as T) : null;
}

async function updateDocById(collectionName: string, id: string, data: Record<string, unknown>): Promise<void> {
  await updateDoc(doc(db, collectionName, id), { ...data, updatedAt: new Date().toISOString() });
}

async function deleteDocById(collectionName: string, id: string): Promise<void> {
  await deleteDoc(doc(db, collectionName, id));
}

// ── Settings ────────────────────────────────────────────────────────────────

export async function getSettings(userId: string): Promise<Settings | null> {
  const q = query(collection(db, COLLECTIONS.SETTINGS), where('userId', '==', userId), limit(1));
  const snap = await getDocs(q);
  if (snap.empty) return null;
  const doc = snap.docs[0];
  return { id: doc.id, ...doc.data() } as Settings;
}

export async function upsertSettings(userId: string, data: Partial<Settings>): Promise<string> {
  const existing = await getSettings(userId);
  if (existing) {
    await updateDocById(COLLECTIONS.SETTINGS, existing.id, data);
    return existing.id;
  }
  return createDoc(COLLECTIONS.SETTINGS, { ...data, userId } as Settings);
}

// ── Keywords ────────────────────────────────────────────────────────────────

export async function getKeywords(userId: string): Promise<Keyword[]> {
  const q = query(
    collection(db, COLLECTIONS.KEYWORDS),
    where('userId', '==', userId),
    orderBy('createdAt', 'desc')
  );
  const snap = await getDocs(q);
  return snap.docs.map(d => ({ id: d.id, ...d.data() } as Keyword));
}

export async function createKeyword(data: Record<string, unknown>): Promise<string> {
  return createDoc(COLLECTIONS.KEYWORDS, data);
}

export async function updateKeyword(id: string, data: Record<string, unknown>): Promise<void> {
  await updateDocById(COLLECTIONS.KEYWORDS, id, data);
}

export async function deleteKeyword(id: string): Promise<void> {
  await deleteDocById(COLLECTIONS.KEYWORDS, id);
}

// ── Keyword Runs ────────────────────────────────────────────────────────────

export async function createKeywordRun(data: Record<string, unknown>): Promise<string> {
  return createDoc(COLLECTIONS.KEYWORD_RUNS, data);
}

export async function updateKeywordRun(id: string, data: Record<string, unknown>): Promise<void> {
  await updateDocById(COLLECTIONS.KEYWORD_RUNS, id, data);
}

export async function getRunningRuns(): Promise<KeywordRun[]> {
  const q = query(
    collection(db, COLLECTIONS.KEYWORD_RUNS),
    where('status', '==', 'RUNNING')
  );
  const snap = await getDocs(q);
  return snap.docs.map(d => ({ id: d.id, ...d.data() } as KeywordRun));
}

export async function getKeywordRuns(keywordId: string): Promise<KeywordRun[]> {
  const q = query(
    collection(db, COLLECTIONS.KEYWORD_RUNS),
    where('keywordId', '==', keywordId),
    orderBy('startedAt', 'desc')
  );
  const snap = await getDocs(q);
  return snap.docs.map(d => ({ id: d.id, ...d.data() } as KeywordRun));
}

// ── Leads ───────────────────────────────────────────────────────────────────

export async function createLead(data: Record<string, unknown>): Promise<string> {
  return createDoc(COLLECTIONS.LEADS, data);
}

export async function getLeads(userId?: string, opts?: { page?: number; pageSize?: number; status?: string; keyword?: string }): Promise<{ leads: Lead[]; total: number }> {
  const pageSize = opts?.pageSize || 50;
  const constraints: QueryConstraint[] = [];

  if (opts?.status && opts.status !== 'all') {
    constraints.push(where('status', '==', opts.status));
  }
  if (opts?.keyword) {
    constraints.push(where('keyword', '==', opts.keyword));
  }
  constraints.push(orderBy('createdAt', 'desc'));
  constraints.push(limit(pageSize));

  const q = query(collection(db, COLLECTIONS.LEADS), ...constraints);
  const snap = await getDocs(q);
  return {
    leads: snap.docs.map(d => ({ id: d.id, ...d.data() } as Lead)),
    total: snap.size,
  };
}

export async function findLeadByEmail(normalisedEmail: string): Promise<Lead | null> {
  const q = query(
    collection(db, COLLECTIONS.LEADS),
    where('normalisedEmail', '==', normalisedEmail),
    limit(1)
  );
  const snap = await getDocs(q);
  if (snap.empty) return null;
  const doc = snap.docs[0];
  return { id: doc.id, ...doc.data() } as Lead;
}

export async function findLeadByAppId(appId: string): Promise<Lead | null> {
  const q = query(
    collection(db, COLLECTIONS.LEADS),
    where('appId', '==', appId),
    limit(1)
  );
  const snap = await getDocs(q);
  if (snap.empty) return null;
  const doc = snap.docs[0];
  return { id: doc.id, ...doc.data() } as Lead;
}

export async function updateLeadStatus(id: string, status: string): Promise<void> {
  await updateDocById(COLLECTIONS.LEADS, id, { status } as Record<string, unknown>);
}

export async function getLeadsByRun(keywordRunId: string): Promise<Lead[]> {
  const q = query(
    collection(db, COLLECTIONS.LEADS),
    where('keywordRunId', '==', keywordRunId)
  );
  const snap = await getDocs(q);
  return snap.docs.map(d => ({ id: d.id, ...d.data() } as Lead));
}

export async function countQualifiedLeads(keywordRunId: string): Promise<number> {
  const q = query(
    collection(db, COLLECTIONS.LEADS),
    where('keywordRunId', '==', keywordRunId),
    where('status', '==', 'qualified')
  );
  const snap = await getDocs(q);
  return snap.size;
}

// ── Email Templates ─────────────────────────────────────────────────────────

export async function getTemplates(keywordId?: string): Promise<EmailTemplate[]> {
  const constraints: QueryConstraint[] = [];
  if (keywordId) {
    constraints.push(where('keywordId', '==', keywordId));
  }
  constraints.push(orderBy('updatedAt', 'desc'));

  const q = query(collection(db, COLLECTIONS.EMAIL_TEMPLATES), ...constraints);
  const snap = await getDocs(q);
  return snap.docs.map(d => ({ id: d.id, ...d.data() } as EmailTemplate));
}

export async function createTemplate(data: Record<string, unknown>): Promise<string> {
  return createDoc(COLLECTIONS.EMAIL_TEMPLATES, data);
}

export async function updateTemplate(id: string, data: Partial<EmailTemplate>): Promise<void> {
  await updateDocById(COLLECTIONS.EMAIL_TEMPLATES, id, data);
}

// ── Outreach Messages ───────────────────────────────────────────────────────

export async function createOutreachMessage(data: Record<string, unknown>): Promise<string> {
  return createDoc(COLLECTIONS.OUTREACH_MESSAGES, data);
}

export async function updateOutreachMessage(id: string, data: Partial<OutreachMessage>): Promise<void> {
  await updateDocById(COLLECTIONS.OUTREACH_MESSAGES, id, data);
}

export async function getOutreachMessages(opts?: { status?: string; limit?: number }): Promise<OutreachMessage[]> {
  const constraints: QueryConstraint[] = [];
  if (opts?.status) {
    constraints.push(where('status', '==', opts.status));
  }
  constraints.push(orderBy('createdAt', 'desc'));
  constraints.push(limit(opts?.limit || 50));

  const q = query(collection(db, COLLECTIONS.OUTREACH_MESSAGES), ...constraints);
  const snap = await getDocs(q);
  return snap.docs.map(d => ({ id: d.id, ...d.data() } as OutreachMessage));
}

// ── Sending Accounts ────────────────────────────────────────────────────────

export async function getSendingAccounts(): Promise<SendingAccount[]> {
  const q = query(collection(db, COLLECTIONS.SENDING_ACCOUNTS), orderBy('priority', 'asc'));
  const snap = await getDocs(q);
  return snap.docs.map(d => ({ id: d.id, ...d.data() } as SendingAccount));
}

export async function createSendingAccount(data: Record<string, unknown>): Promise<string> {
  return createDoc(COLLECTIONS.SENDING_ACCOUNTS, data);
}

export async function updateSendingAccount(id: string, data: Partial<SendingAccount>): Promise<void> {
  await updateDocById(COLLECTIONS.SENDING_ACCOUNTS, id, data);
}

// ── Replies ─────────────────────────────────────────────────────────────────

export async function createReply(data: Record<string, unknown>): Promise<string> {
  return createDoc(COLLECTIONS.REPLIES, data);
}

export async function getReplies(opts?: { leadId?: string; limit?: number }): Promise<Reply[]> {
  const constraints: QueryConstraint[] = [];
  if (opts?.leadId) {
    constraints.push(where('leadId', '==', opts.leadId));
  }
  constraints.push(orderBy('receivedAt', 'desc'));
  constraints.push(limit(opts?.limit || 50));

  const q = query(collection(db, COLLECTIONS.REPLIES), ...constraints);
  const snap = await getDocs(q);
  return snap.docs.map(d => ({ id: d.id, ...d.data() } as Reply));
}

// ── Notifications ───────────────────────────────────────────────────────────

export async function createNotification(data: Record<string, unknown>): Promise<string> {
  return createDoc(COLLECTIONS.NOTIFICATIONS, data);
}

export async function getNotifications(limitCount: number = 20): Promise<Notification[]> {
  const q = query(
    collection(db, COLLECTIONS.NOTIFICATIONS),
    orderBy('createdAt', 'desc'),
    limit(limitCount)
  );
  const snap = await getDocs(q);
  return snap.docs.map(d => ({ id: d.id, ...d.data() } as Notification));
}

// ── Activity Logs ───────────────────────────────────────────────────────────

export async function logActivity(data: Record<string, unknown>): Promise<string> {
  return createDoc(COLLECTIONS.ACTIVITY_LOGS, data);
}

export async function getActivityLogs(limitCount: number = 100): Promise<ActivityLog[]> {
  const q = query(
    collection(db, COLLECTIONS.ACTIVITY_LOGS),
    orderBy('createdAt', 'desc'),
    limit(limitCount)
  );
  const snap = await getDocs(q);
  return snap.docs.map(d => ({ id: d.id, ...d.data() } as ActivityLog));
}

// ── Analytics ───────────────────────────────────────────────────────────────

export async function getAnalyticsOverview(): Promise<{
  totalLeads: number;
  qualifiedLeads: number;
  emailsSent: number;
  emailsFailed: number;
  replies: number;
  humanReplies: number;
}> {
  const leadsSnap = await getDocs(collection(db, COLLECTIONS.LEADS));
  const messagesSnap = await getDocs(collection(db, COLLECTIONS.OUTREACH_MESSAGES));
  const repliesSnap = await getDocs(collection(db, COLLECTIONS.REPLIES));

  let qualifiedLeads = 0;
  let emailsSent = 0;
  let emailsFailed = 0;
  let humanReplies = 0;

  leadsSnap.forEach(doc => {
    const data = doc.data();
    if (data.status === 'qualified') qualifiedLeads++;
  });

  messagesSnap.forEach(doc => {
    const data = doc.data();
    if (data.status === 'sent') emailsSent++;
    if (data.status === 'failed') emailsFailed++;
  });

  repliesSnap.forEach(doc => {
    const data = doc.data();
    if (data.classification === 'HUMAN_REPLY') humanReplies++;
  });

  return {
    totalLeads: leadsSnap.size,
    qualifiedLeads,
    emailsSent,
    emailsFailed,
    replies: repliesSnap.size,
    humanReplies,
  };
}

// ── Real-time Listener ──────────────────────────────────────────────────────

export function onAutomationStateChange(callback: (state: AutomationState | null) => void): () => void {
  const docRef = doc(db, 'system', 'automation_state');
  return onSnapshot(docRef, (snap) => {
    if (snap.exists()) {
      callback({ id: snap.id, ...snap.data() } as unknown as AutomationState);
    } else {
      callback(null);
    }
  });
}

export {
  createDoc, getDocById, updateDocById, deleteDocById,
  increment, serverTimestamp, Timestamp, writeBatch, db,
};
