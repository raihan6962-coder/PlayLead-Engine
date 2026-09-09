// Firestore Collection Names
export const COLLECTIONS = {
  USERS: 'users',
  SETTINGS: 'settings',
  KEYWORDS: 'keywords',
  KEYWORD_RUNS: 'keyword_runs',
  LEADS: 'leads',
  LEAD_EVENTS: 'lead_events',
  EMAIL_TEMPLATES: 'email_templates',
  OUTREACH_MESSAGES: 'outreach_messages',
  SENDING_ACCOUNTS: 'sending_accounts',
  REPLIES: 'replies',
  NOTIFICATIONS: 'notifications',
  ACTIVITY_LOGS: 'activity_logs',
  ANALYTICS_DAILY: 'analytics_daily',
  INTEGRATION_LOGS: 'integration_logs',
} as const;

// Job Status State Machine — valid transitions
export const VALID_TRANSITIONS: Record<string, string[]> = {
  DRAFT: ['SCHEDULED', 'DISABLED'],
  SCHEDULED: ['PENDING', 'RUNNING', 'DISABLED'],
  PENDING: ['RUNNING', 'CANCELLED'],
  RUNNING: ['COMPLETED', 'PARTIAL', 'EXHAUSTED', 'FAILED', 'CANCELLED', 'OVERDUE'],
  PARTIAL: ['RUNNING', 'FAILED', 'DISABLED'],
  EXHAUSTED: ['RUNNING', 'DISABLED'],
  FAILED: ['RUNNING', 'SCHEDULED', 'DISABLED'],
  OVERDUE: ['COMPLETED', 'PARTIAL', 'FAILED', 'CANCELLED'],
  CANCELLED: ['SCHEDULED', 'DISABLED'],
  COMPLETED: ['SCHEDULED', 'DISABLED'],
  DISABLED: ['DRAFT'],
};

export function isValidTransition(from: string, to: string): boolean {
  return VALID_TRANSITIONS[from]?.includes(to) ?? false;
}

// Discovery Provider Interface
export interface LeadDiscoveryProvider {
  name: string;
  search(query: string, options: SearchOptions): Promise<DiscoveryResult>;
}

export interface SearchOptions {
  lang?: string;
  country?: string;
  maxResults?: number;
}

export interface DiscoveryResult {
  items: DiscoveryItem[];
  hasMore: boolean;
  cursor?: string;
}

export interface DiscoveryItem {
  appId: string;
  title: string;
  developer: string;
  email: string;
  genre: string;
  installs: number;
  score: number | null;
  url: string;
  icon: string;
  country?: string;
}
