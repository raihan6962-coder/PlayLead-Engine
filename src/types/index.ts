// ── Job & Phase Enums ──────────────────────────────────────────────────────
export enum JobStatus {
  DRAFT = 'DRAFT',
  SCHEDULED = 'SCHEDULED',
  PENDING = 'PENDING',
  RUNNING = 'RUNNING',
  COMPLETED = 'COMPLETED',
  PARTIAL = 'PARTIAL',
  EXHAUSTED = 'EXHAUSTED',
  FAILED = 'FAILED',
  CANCELLED = 'CANCELLED',
  OVERDUE = 'OVERDUE',
  DISABLED = 'DISABLED',
}

export enum AutomationPhase {
  IDLE = 'idle',
  LOADING_SHEET = 'loading_sheet',
  SCRAPING = 'scraping',
  AI_KEYWORDS = 'ai_keywords',
  QUALIFYING = 'qualifying',
  EMAILING = 'emailing',
  DONE = 'done',
  FAILED = 'failed',
}

export enum LeadStatus {
  DISCOVERED = 'discovered',
  QUALIFIED = 'qualified',
  REJECTED = 'rejected',
  DUPLICATE = 'duplicate',
  EMAILED = 'emailed',
  REPLIED = 'replied',
  BOUNCED = 'bounced',
  UNSUBSCRIBED = 'unsubscribed',
}

export enum ReplyClassification {
  HUMAN_REPLY = 'HUMAN_REPLY',
  AUTOMATED_REPLY = 'AUTOMATED_REPLY',
  OUT_OF_OFFICE = 'OUT_OF_OFFICE',
  BOUNCE = 'BOUNCE',
  UNCLEAR = 'UNCLEAR',
}

// ── Core Document Types ────────────────────────────────────────────────────
export interface User {
  uid: string;
  email: string;
  displayName: string;
  role: 'admin' | 'viewer';
  createdAt: string;
  lastLogin: string;
}

export interface Settings {
  id: string;
  userId: string;
  dailyStartTime: string;
  dailyEndTime: string;
  timezone: string;
  schedulerEnabled: boolean;
  maxEmailsPerDay: number;
  minSendDelay: number;
  maxSendDelay: number;
  forwardingEnabled: boolean;
  forwardingEmail: string;
  googleSheetUrl: string;
  googleSheetWebAppUrl: string;
  groqApiKey: string;
  telegramBotToken: string;
  telegramChatId: string;
  updatedAt: string;
}

export interface Keyword {
  id: string;
  userId: string;
  keyword: string;
  templateId: string;
  targetLeads: number;
  scheduledDate: string;
  status: JobStatus;
  maxRating: number;
  maxInstalls: number;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
  actualCount: number;
  searchQueries: string[];
}

export interface KeywordRun {
  id: string;
  keywordId: string;
  keyword: string;
  status: JobStatus;
  phase: AutomationPhase;
  startedAt: string;
  completedAt: string | null;
  targetLeads: number;
  actualLeads: number;
  totalDiscovered: number;
  totalRejected: number;
  totalDuplicates: number;
  emailsSent: number;
  emailsFailed: number;
  searchQueriesUsed: string[];
  expectedEndTime: string | null;
  overdueNotified: boolean;
  error: string | null;
}

export interface Lead {
  id: string;
  email: string;
  normalisedEmail: string;
  appId: string;
  appName: string;
  developer: string;
  category: string;
  installs: number;
  score: number | null;
  url: string;
  icon: string;
  keyword: string;
  keywordRunId: string;
  status: LeadStatus;
  emailSent: boolean;
  emailSentAt: string | null;
  emailOpened: boolean;
  emailOpenedAt: string | null;
  repliedAt: string | null;
  createdAt: string;
  scrapedAt: string;
  country: string;
}

export interface EmailTemplate {
  id: string;
  keywordId: string;
  keyword: string;
  subject: string;
  body: string;
  variables: string[];
  version: number;
  status: 'active' | 'draft' | 'archived';
  createdAt: string;
  updatedAt: string;
}

export interface OutreachMessage {
  id: string;
  leadId: string;
  templateId: string;
  keywordRunId: string;
  subject: string;
  body: string;
  htmlBody: string;
  status: 'queued' | 'sending' | 'sent' | 'failed';
  sendingAccountId: string | null;
  idempotencyKey: string;
  sentAt: string | null;
  error: string | null;
  createdAt: string;
}

export interface SendingAccount {
  id: string;
  name: string;
  webAppUrl: string;
  status: 'active' | 'cooldown' | 'disabled';
  priority: number;
  dailyCapacity: number;
  sentToday: number;
  lastSuccess: string | null;
  lastError: string | null;
  cooldownUntil: string | null;
  createdAt: string;
}

export interface Reply {
  id: string;
  leadId: string;
  outreachMessageId: string;
  fromEmail: string;
  subject: string;
  body: string;
  receivedAt: string;
  classification: ReplyClassification;
  confidence: number;
  classificationMethod: 'rules' | 'ai' | 'manual';
  forwarded: boolean;
  forwardedAt: string | null;
  createdAt: string;
}

export interface Notification {
  id: string;
  type: 'automation_started' | 'automation_completed' | 'automation_failed' | 'overdue' | 'target_reached' | 'reply_received' | 'error' | 'daily_summary';
  title: string;
  message: string;
  sent: boolean;
  sentAt: string | null;
  error: string | null;
  metadata: Record<string, unknown>;
  createdAt: string;
}

export interface ActivityLog {
  id: string;
  event: string;
  entity: string;
  entityId: string;
  actor: string;
  metadata: Record<string, unknown>;
  createdAt: string;
}

export interface AnalyticsDaily {
  id: string;
  date: string;
  totalDiscovered: number;
  qualified: number;
  rejected: number;
  duplicates: number;
  emailsGenerated: number;
  emailsSent: number;
  emailsFailed: number;
  replies: number;
  humanReplies: number;
  automatedReplies: number;
  overdueRuns: number;
  failedRuns: number;
  activeKeywords: number;
}

export interface AutomationState {
  running: boolean;
  phase: AutomationPhase;
  keywordId: string | null;
  keyword: string;
  leadsFound: number;
  targetLeads: number;
  emailsSent: number;
  searchQueries: string[];
  logs: { time: string; msg: string }[];
  startTime: string | null;
  expectedEndTime: string | null;
  isOverdue: boolean;
}

export interface SystemHealth {
  firebase: boolean;
  groq: boolean;
  googleSheet: boolean;
  telegram: boolean;
  emailService: boolean;
  scheduler: boolean;
  lastChecked: string;
}
