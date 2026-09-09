const TELEGRAM_API = 'https://api.telegram.org';

export interface TelegramMessage {
  text: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
}

async function sendMessage(token: string, chatId: string, message: TelegramMessage): Promise<boolean> {
  if (!token || !chatId) return false;

  try {
    const response = await fetch(`${TELEGRAM_API}/bot${token}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text: message.text,
        parse_mode: message.parseMode || 'HTML',
      }),
      signal: AbortSignal.timeout(10000),
    });

    return response.ok;
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : 'Unknown error';
    console.error(`Telegram send failed: ${msg}`);
    return false;
  }
}

export async function sendAutomationStarted(keyword: string, target: number): Promise<boolean> {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return false;

  return sendMessage(token, chatId, {
    text: `🚀 <b>Automation Started</b>\n\nKeyword: <code>${keyword}</code>\nTarget: ${target} qualified leads\n\nDiscovery and outreach are now running server-side.`,
  });
}

export async function sendAutomationCompleted(keyword: string, leadsFound: number, emailsSent: number): Promise<boolean> {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return false;

  return sendMessage(token, chatId, {
    text: `✅ <b>Automation Completed</b>\n\nKeyword: <code>${keyword}</code>\nQualified leads: ${leadsFound}\nEmails sent: ${emailsSent}\n\nAll tasks finished.`,
  });
}

export async function sendAutomationFailed(keyword: string, error: string): Promise<boolean> {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return false;

  return sendMessage(token, chatId, {
    text: `❌ <b>Automation Failed</b>\n\nKeyword: <code>${keyword}</code>\nError: ${error}\n\nPlease check the dashboard for details.`,
  });
}

export async function sendOverdueAlert(keyword: string, phase: string, expectedEnd: string): Promise<boolean> {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return false;

  return sendMessage(token, chatId, {
    text: `⚠️ <b>Expected End Time Exceeded</b>\n\nKeyword: <code>${keyword}</code>\nPhase: ${phase}\nExpected end: ${expectedEnd}\n\nThe job has NOT been stopped and will continue until its completion condition is reached.`,
  });
}

export async function sendReplyReceived(developerEmail: string, appName: string): Promise<boolean> {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return false;

  return sendMessage(token, chatId, {
    text: `📬 <b>Reply Received</b>\n\nFrom: <code>${developerEmail}</code>\nApp: ${appName}\n\nCheck the dashboard for details.`,
  });
}

export async function sendTargetReached(keyword: string, target: number, actual: number): Promise<boolean> {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return false;

  return sendMessage(token, chatId, {
    text: `🎯 <b>Target Reached</b>\n\nKeyword: <code>${keyword}</code>\nTarget: ${target}\nActual: ${actual}\n\nOutreach is now starting.`,
  });
}

export async function sendDailySummary(data: {
  keywordsProcessed: number;
  leadsFound: number;
  emailsSent: number;
  replies: number;
}): Promise<boolean> {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return false;

  return sendMessage(token, chatId, {
    text: `📊 <b>Daily Summary</b>\n\nKeywords processed: ${data.keywordsProcessed}\nLeads found: ${data.leadsFound}\nEmails sent: ${data.emailsSent}\nReplies: ${data.replies}`,
  });
}

export async function testConnection(): Promise<boolean> {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) return false;

  return sendMessage(token, chatId, {
    text: `✅ <b>PlayLead Bot Connected</b>\n\nTelegram integration is working correctly.`,
  });
}

export function isConfigured(): boolean {
  return !!(process.env.TELEGRAM_BOT_TOKEN && process.env.TELEGRAM_CHAT_ID);
}
