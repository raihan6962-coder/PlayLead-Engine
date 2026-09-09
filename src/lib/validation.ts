import { z } from 'zod';

// ── Validation Schemas ──────────────────────────────────────────────────────

export const loginSchema = z.object({
  email: z.string().email('Invalid email address'),
  password: z.string().min(6, 'Password must be at least 6 characters'),
});

export const keywordSchema = z.object({
  keyword: z.string().min(1, 'Keyword is required').max(200),
  targetLeads: z.number().int().min(10).max(5000).default(300),
  scheduledDate: z.string().optional(),
  maxRating: z.number().min(0).max(5).default(2.5),
  maxInstalls: z.number().int().min(0).max(100000).default(5000),
  templateId: z.string().optional(),
});

export const templateSchema = z.object({
  keywordId: z.string(),
  keyword: z.string(),
  subject: z.string().min(1).max(200),
  body: z.string().min(10).max(5000),
});

export const settingsSchema = z.object({
  dailyStartTime: z.string().regex(/^\d{2}:\d{2}$/),
  dailyEndTime: z.string().regex(/^\d{2}:\d{2}$/),
  timezone: z.string(),
  schedulerEnabled: z.boolean(),
  maxEmailsPerDay: z.number().int().min(1).max(1000),
  minSendDelay: z.number().int().min(10).max(300),
  maxSendDelay: z.number().int().min(20).max(600),
  forwardingEnabled: z.boolean(),
  forwardingEmail: z.string().email().optional().or(z.literal('')),
  googleSheetUrl: z.string().url().optional().or(z.literal('')),
  googleSheetWebAppUrl: z.string().url().optional().or(z.literal('')),
  telegramBotToken: z.string().optional().or(z.literal('')),
  telegramChatId: z.string().optional().or(z.literal('')),
});

export const outreachSchema = z.object({
  leadIds: z.array(z.string()).min(1),
  templateId: z.string(),
});

export const updateStatusSchema = z.object({
  leadId: z.string(),
  status: z.enum(['qualified', 'rejected', 'emailed', 'replied', 'bounced', 'unsubscribed']),
});

export type LoginInput = z.infer<typeof loginSchema>;
export type KeywordInput = z.infer<typeof keywordSchema>;
export type TemplateInput = z.infer<typeof templateSchema>;
export type SettingsInput = z.infer<typeof settingsSchema>;
export type OutreachInput = z.infer<typeof outreachSchema>;
