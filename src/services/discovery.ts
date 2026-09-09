import { normaliseEmail } from '@/lib/utils';
import type { Lead, LeadStatus } from '@/types';

const EMAIL_RE = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/;

const BLOCKED_COUNTRIES = new Set([
  'BD', 'IN', 'PK', 'NG', 'GH', 'KE', 'TZ', 'UG', 'ET', 'EG',
  'MA', 'TN', 'DZ', 'LY', 'SD', 'SO', 'AO', 'MZ', 'ZM', 'ZW',
  'MW', 'RW', 'SN', 'CI', 'CM', 'CD', 'MG', 'MM', 'KH', 'LA',
  'NP', 'LK', 'AF', 'IQ', 'SY', 'YE', 'LB', 'JO', 'PS', 'PH',
  'ID', 'VN', 'TH', 'MY',
]);

const ALLOWED_COUNTRIES = new Set([
  'US', 'GB', 'CA', 'AU', 'NZ', 'DE', 'FR', 'NL', 'SE', 'NO',
  'DK', 'FI', 'CH', 'AT', 'BE', 'IE', 'SG', 'JP', 'KR', 'IL',
  'IT', 'ES', 'PT', 'PL', 'CZ', 'HU', 'RO', 'GR', 'ZA', 'AE',
  'SA', 'QA', 'KW', 'BH', 'MX', 'BR', 'AR', 'CL', 'CO',
]);

const DISPOSABLE_DOMAINS = new Set([
  'mailinator.com', 'guerrillamail.com', 'sharklasers.com', 'grr.la',
  'temp-mail.org', 'tempmail.org', 'throwaway.email', 'yopmail.com',
  'maildrop.cc', 'mailnesia.com', 'getairmail.com', '10minutemail.com',
  'mailcatch.com', 'spambox.us', 'trashmail.com', 'mailexpire.com',
  'dispostable.com', 'mailsac.com', 'tempinbox.com', 'mailmetrash.com',
  'emailondeck.com', 'inboxbear.com', 'mail-tester.com', 'burnermail.io',
  'fakeinbox.com', 'guerrillamail.org', 'guerrillamail.net', 'guerrillamail.biz',
  'emailtmp.com', 'mytemp.email', 'tempemail.net', 'fakemailgenerator.com',
  'mailnator.com', 'maileater.com', 'mintemail.com', 'spamgourmet.com',
]);

export interface QualificationCriteria {
  maxRating: number;
  maxInstalls: number;
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

export function extractEmail(text: string): string {
  if (!text) return '';
  const m = text.match(EMAIL_RE);
  return m ? m[0] : '';
}

export function isValidEmail(email: string): boolean {
  if (!email || !EMAIL_RE.test(email)) return false;
  const domain = email.split('@')[1]?.toLowerCase();
  if (!domain) return false;
  if (DISPOSABLE_DOMAINS.has(domain)) return false;
  return true;
}

export function isAllowedCountry(item: DiscoveryItem): boolean {
  if (item.country) {
    if (BLOCKED_COUNTRIES.has(item.country)) return false;
    if (ALLOWED_COUNTRIES.has(item.country)) return true;
  }
  return true;
}

export function qualifyLead(item: DiscoveryItem, criteria: QualificationCriteria): { qualified: boolean; reason?: string } {
  if (!isValidEmail(item.email)) {
    return { qualified: false, reason: 'invalid_email' };
  }
  if (!isAllowedCountry(item)) {
    return { qualified: false, reason: 'blocked_country' };
  }
  if (item.installs > criteria.maxInstalls) {
    return { qualified: false, reason: 'too_many_installs' };
  }
  if (item.score !== null && item.score > criteria.maxRating) {
    return { qualified: false, reason: 'rating_too_high' };
  }
  if (!item.appId || !item.title || !item.developer) {
    return { qualified: false, reason: 'incomplete_data' };
  }
  return { qualified: true };
}

export function normaliseLead(item: DiscoveryItem, keyword: string, keywordRunId: string): Omit<Lead, 'id' | 'createdAt'> {
  return {
    email: item.email,
    normalisedEmail: normaliseEmail(item.email),
    appId: item.appId,
    appName: item.title,
    developer: item.developer,
    category: item.genre || '',
    installs: item.installs,
    score: item.score,
    url: item.url,
    icon: item.icon || '',
    keyword,
    keywordRunId,
    status: 'qualified' as LeadStatus,
    emailSent: false,
    emailSentAt: null,
    emailOpened: false,
    emailOpenedAt: null,
    repliedAt: null,
    scrapedAt: new Date().toISOString(),
    country: item.country || '',
  };
}

export interface LeadDiscoveryProvider {
  name: string;
  search(query: string, lang?: string, country?: string, nHits?: number): Promise<DiscoveryItem[]>;
}

export class PlayStoreDiscovery implements LeadDiscoveryProvider {
  name = 'google_play_store';

  async search(query: string, lang: string = 'en', country: string = 'us', nHits: number = 100): Promise<DiscoveryItem[]> {
    try {
      const response = await fetch('https://playlead-worker.workers.dev/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, lang, country, n_hits: nHits }),
        signal: AbortSignal.timeout(30000),
      });

      if (!response.ok) {
        console.error(`Discovery provider error: ${response.status}`);
        return [];
      }

      const data = await response.json();
      return (data.results || []).map((item: Record<string, unknown>) => ({
        appId: (item.appId as string) || '',
        title: (item.title as string) || '',
        developer: (item.developer as string) || '',
        email: (item.developerEmail as string) || '',
        genre: (item.genre as string) || '',
        installs: (item.installs as number) || 0,
        score: (item.score as number) || null,
        url: `https://play.google.com/store/apps/details?id=${item.appId}`,
        icon: (item.icon as string) || '',
        country,
      }));
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Unknown error';
      console.error(`Discovery search failed: ${msg}`);
      return [];
    }
  }
}
