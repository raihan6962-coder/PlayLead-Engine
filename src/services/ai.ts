const GROQ_API_KEY = process.env.GROQ_API_KEY;

interface AIResponse {
  content: string;
  error?: string;
}

async function callGroq(prompt: string, maxTokens: number = 500, temperature: number = 0.7): Promise<AIResponse> {
  if (!GROQ_API_KEY) {
    return { content: '', error: 'GROQ_API_KEY not configured' };
  }

  try {
    const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${GROQ_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'llama-3.3-70b-versatile',
        messages: [{ role: 'user', content: prompt }],
        temperature,
        max_tokens: maxTokens,
      }),
      signal: AbortSignal.timeout(30000),
    });

    if (!response.ok) {
      const err = await response.text();
      return { content: '', error: `Groq API error: ${response.status} ${err}` };
    }

    const data = await response.json();
    return { content: data.choices?.[0]?.message?.content?.trim() || '' };
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : 'Unknown error';
    return { content: '', error: msg };
  }
}

export async function generateSearchQueries(primaryKeyword: string, usedQueries: string[], maxQueries: number = 8): Promise<string[]> {
  const prompt = `You are a Google Play Store keyword expert.
Primary keyword: "${primaryKeyword}"
Already used queries: ${usedQueries.length > 0 ? usedQueries.join(', ') : 'none'}

Generate ${maxQueries} NEW Play Store search queries closely related to "${primaryKeyword}" — synonyms, related terms, long-tail extensions, and category variations.

Rules:
- Return ONLY a JSON array of strings
- Avoid irrelevant queries
- Avoid repeating existing queries
- Each query must be a realistic Play Store search term
- Focus on finding real apps with active user bases

Return ONLY the JSON array, no explanation.`;

  const result = await callGroq(prompt, 500, 0.8);
  if (result.error) return [];

  try {
    const cleaned = result.content.replace(/```[a-z]*\n?/g, '').replace(/```/g, '').trim();
    const queries = JSON.parse(cleaned);
    if (!Array.isArray(queries)) return [];
    return queries.filter((q: string) => typeof q === 'string' && !usedQueries.includes(q));
  } catch {
    return [];
  }
}

export async function personalizeEmail(
  template: { subject: string; body: string },
  lead: { app_name: string; developer: string; category: string; score: number | null; installs: number },
  senderName: string,
  senderCompany: string
): Promise<{ subject: string; body: string }> {
  const prompt = `You are an expert email copywriter. Rewrite this email template to be unique and personal for this specific lead. Keep the same value proposition but make it natural and conversational.

Template subject: ${template.subject}
Template body:
${template.body}

Lead information:
- App name: ${lead.app_name}
- Developer: ${lead.developer}
- Category: ${lead.category}
- Rating: ${lead.score ?? 'no rating'}
- Installs: ${lead.installs.toLocaleString()}

Sender: ${senderName} from ${senderCompany}

Rules:
- Do NOT invent factual claims about the app or company
- Do NOT fabricate features, testimonials, or previous conversations
- Keep it under 150 words
- Make it feel personal and genuine
- Return JSON with "subject" and "body" keys`;

  const result = await callGroq(prompt, 400, 0.7);
  if (result.error) {
    return {
      subject: template.subject.replace(/\{\{app_name\}\}/g, lead.app_name),
      body: template.body
        .replace(/\{\{app_name\}\}/g, lead.app_name)
        .replace(/\{\{developer\}\}/g, lead.developer)
        .replace(/\{\{category\}\}/g, lead.category)
        .replace(/\{\{sender_name\}\}/g, senderName)
        .replace(/\{\{sender_company\}\}/g, senderCompany),
    };
  }

  try {
    const cleaned = result.content.replace(/```[a-z]*\n?/g, '').replace(/```/g, '').trim();
    const parsed = JSON.parse(cleaned);
    return { subject: parsed.subject || template.subject, body: parsed.body || template.body };
  } catch {
    return {
      subject: template.subject.replace(/\{\{app_name\}\}/g, lead.app_name),
      body: template.body
        .replace(/\{\{app_name\}\}/g, lead.app_name)
        .replace(/\{\{developer\}\}/g, lead.developer)
        .replace(/\{\{category\}\}/g, lead.category)
        .replace(/\{\{sender_name\}\}/g, senderName)
        .replace(/\{\{sender_company\}\}/g, senderCompany),
    };
  }
}

export async function classifyReply(subject: string, body: string): Promise<{
  classification: string;
  confidence: number;
  method: 'ai' | 'rules';
}> {
  // Deterministic rules first
  const lower = body.toLowerCase();
  if (lower.includes('out of office') || lower.includes('away from') || lower.includes('vacation')) {
    return { classification: 'OUT_OF_OFFICE', confidence: 0.95, method: 'rules' };
  }
  if (lower.includes('unsubscribe') || lower.includes('remove me') || lower.includes('stop emailing')) {
    return { classification: 'HUMAN_REPLY', confidence: 0.9, method: 'rules' };
  }
  if (lower.includes('mailer-daemon') || lower.includes('delivery failed') || lower.includes('undeliverable')) {
    return { classification: 'BOUNCE', confidence: 0.95, method: 'rules' };
  }
  if (lower.includes('auto-reply') || lower.includes('automated response') || lower.includes('do not reply')) {
    return { classification: 'AUTOMATED_REPLY', confidence: 0.85, method: 'rules' };
  }

  // AI classification as secondary layer
  const prompt = `Classify this email reply. Return JSON with "classification" and "confidence".

Classifications: HUMAN_REPLY, AUTOMATED_REPLY, OUT_OF_OFFICE, BOUNCE, UNCLEAR

Subject: ${subject}
Body (first 500 chars): ${body.slice(0, 500)}`;

  const result = await callGroq(prompt, 100, 0.3);
  if (result.error) {
    return { classification: 'UNCLEAR', confidence: 0.3, method: 'rules' };
  }

  try {
    const parsed = JSON.parse(result.content.replace(/```[a-z]*\n?/g, '').replace(/```/g, '').trim());
    const valid = ['HUMAN_REPLY', 'AUTOMATED_REPLY', 'OUT_OF_OFFICE', 'BOUNCE', 'UNCLEAR'];
    const cls = valid.includes(parsed.classification) ? parsed.classification : 'UNCLEAR';
    return { classification: cls, confidence: Math.min(1, Math.max(0, parsed.confidence || 0.5)), method: 'ai' };
  } catch {
    return { classification: 'UNCLEAR', confidence: 0.3, method: 'ai' };
  }
}

export function isConfigured(): boolean {
  return !!GROQ_API_KEY;
}
