import type { SendingAccount } from '@/types';

export interface SendEmailParams {
  to: string;
  subject: string;
  body: string;
  htmlBody?: string;
  fromName?: string;
  unsubscribeUrl?: string;
}

export interface SendResult {
  success: boolean;
  error?: string;
}

export async function sendEmail(account: SendingAccount, params: SendEmailParams): Promise<SendResult> {
  if (!account.webAppUrl) {
    return { success: false, error: 'No sending endpoint configured' };
  }

  try {
    const response = await fetch(account.webAppUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        to: params.to,
        subject: params.subject,
        body: params.body,
        html_body: params.htmlBody || params.body,
        from_name: params.fromName || '',
        unsubscribe_url: params.unsubscribeUrl || '',
      }),
      signal: AbortSignal.timeout(30000),
    });

    const result = await response.json();
    if (result.status === 'ok') {
      return { success: true };
    }
    return { success: false, error: result.msg || 'Send failed' };
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : 'Network error';
    return { success: false, error: msg };
  }
}

export function selectBestAccount(accounts: SendingAccount[]): SendingAccount | null {
  const now = new Date();
  const active = accounts
    .filter(a => a.status === 'active')
    .filter(a => {
      if (a.cooldownUntil) {
        return new Date(a.cooldownUntil) <= now;
      }
      return true;
    })
    .filter(a => a.sentToday < a.dailyCapacity)
    .sort((a, b) => a.priority - b.priority || a.sentToday - b.sentToday);

  return active[0] || null;
}

export function buildUnsubscribeUrl(baseUrl: string, email: string, token: string): string {
  const base = baseUrl.replace(/\/$/, '');
  return `${base}/api/unsubscribe?email=${encodeURIComponent(email)}&token=${token}`;
}

export function buildTrackingPixelUrl(baseUrl: string, trackingId: string): string {
  const base = baseUrl.replace(/\/$/, '');
  return `${base}/api/track/open?tid=${trackingId}`;
}

export function generateHtmlBody(plainBody: string, unsubscribeUrl: string, trackingPixelUrl: string): string {
  const escaped = plainBody
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\n/g, '<br>');

  return `<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;font-size:14px;color:#333;line-height:1.5;white-space:pre-wrap;margin:0;padding:0;">
${escaped}
<br><br>
<a href="${unsubscribeUrl}" style="display:inline-block;padding:7px 16px;background:#f2f2f2;border:1px solid #ccc;border-radius:4px;color:#333;text-decoration:none;font-size:12px;">Unsubscribe</a>
<img src="${trackingPixelUrl}" width="1" height="1" alt="" />
</body></html>`;
}
