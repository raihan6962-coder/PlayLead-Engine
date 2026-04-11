import os, time, random, threading, json, re, logging
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from google_play_scraper import search, app as gp_app
from groq import Groq
import requests

# ── Flask setup ───────────────────────────────────────────────────────────────
application = Flask(__name__, static_folder=".")
app = application
CORS(application)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Shared state ──────────────────────────────────────────────────────────────
stop_event = threading.Event()
state_lock = threading.Lock()
state = {
    "running": False, "phase": "idle", "keyword": "",
    "keywords_used": [], "leads_found": 0, "emails_sent": 0,
    "logs": [], "leads": []
}

# ── Global duplicate tracker — persists across runs until clear ───────────────
# Uses app_id as the primary unique key (most reliable dedup identifier)
global_seen_ids: set = set()
global_seen_emails: set = set()

# ── Email cooldown state ──────────────────────────────────────────────────────
email_state_lock = threading.Lock()
email_url_quotas: dict = {}          # url -> {"exhausted": bool, "failed": bool}
global_cooldown_until: float = 0.0  # epoch seconds; 0 = no cooldown active
cooldown_retry_thread = None
cooldown_retry_cancel = threading.Event()

# ── Run config ────────────────────────────────────────────────────────────────
run_cfg = {}

def get_cfg(key, fallback=""):
    return run_cfg.get(key) or os.environ.get(key, fallback)

def push_log(msg: str):
    with state_lock:
        state["logs"].append({"time": time.strftime("%H:%M:%S"), "msg": msg})
        if len(state["logs"]) > 500:
            state["logs"] = state["logs"][-500:]
    log.info(msg)

def upd(**kw):
    with state_lock:
        state.update(kw)

# ── Google Sheet via Apps Script ──────────────────────────────────────────────
def sheet_post(payload: dict):
    url = get_cfg("APPS_SCRIPT_WEB_URL")
    if not url:
        return
    try:
        requests.post(url, json=payload, timeout=15)
    except Exception as e:
        push_log(f"  Sheet error: {e}")

def sheet_append_lead(lead: dict):
    sheet_post({"action": "append", "tab": "All Leads", "row": {
        "App Name": lead["app_name"], "Developer": lead["developer"],
        "Email": lead["email"], "Category": lead["category"],
        "Installs": lead["installs"], "Score": lead["score"] or "",
        "URL": lead["url"], "Keyword": lead["keyword"],
        "Scraped At": lead["scraped_at"], "Email Sent": "No",
        "App ID": lead["app_id"],
    }})

def sheet_append_qualified(lead: dict):
    sheet_post({"action": "append", "tab": "Qualified Leads", "row": {
        "App Name": lead["app_name"], "Developer": lead["developer"],
        "Email": lead["email"], "Category": lead["category"],
        "Installs": lead["installs"], "Score": lead["score"] or "",
        "URL": lead["url"], "Keyword": lead["keyword"],
        "Scraped At": lead["scraped_at"], "Email Sent": "Pending",
        "App ID": lead["app_id"],
    }})

def sheet_mark_sent(app_id: str, email: str, app_name: str):
    # Pass both app_id AND email so Apps Script can match by either field
    sheet_post({"action": "mark_sent", "app_id": app_id, "email": email, "app_name": app_name})
    sheet_post({"action": "append", "tab": "Email Sent", "row": {
        "App ID": app_id, "App Name": app_name,
        "Email": email, "Sent At": time.strftime("%Y-%m-%d %H:%M:%S"),
    }})

def sheet_log_keyword(keyword: str, count: int):
    sheet_post({"action": "append", "tab": "Keyword Log", "row": {
        "Keyword": keyword, "Leads Found": count,
        "Logged At": time.strftime("%Y-%m-%d %H:%M:%S"),
    }})

# ── AI keyword generation ─────────────────────────────────────────────────────
def ai_gen_keywords(original: str, used: list, hunter: dict = None) -> list:
    key = get_cfg("GROQ_API_KEY")
    if not key:
        push_log("GROQ_API_KEY not set — using built-in keyword expansion")
        return _fallback_keywords(original, used)

    client = Groq(api_key=key)
    is_hunter = hunter and hunter.get("active")

    if is_hunter:
        prompt = (
            f"You are a Google Play Store keyword expert specializing in finding "
            f"underperforming apps.\n"
            f"Original keyword: '{original}'\n"
            f"Already used: {', '.join(used) if used else 'none'}\n"
            f"Generate 12 NEW diverse Play Store search keywords covering:\n"
            f"- Direct synonyms and variations\n"
            f"- Related niches and sub-categories\n"
            f"- Competitor app types\n"
            f"- Problem-based searches (e.g. 'fix', 'improve', 'track')\n"
            f"- Combination keywords (e.g. 'free', 'lite', 'simple', 'pro')\n"
            f"Return ONLY a JSON array of strings, nothing else. No duplicates."
        )
    else:
        prompt = (
            f"You are a Google Play Store keyword expert.\n"
            f"Original keyword: '{original}'\n"
            f"Already used: {', '.join(used) if used else 'none'}\n"
            f"Generate 8 NEW semantically similar Play Store search keywords "
            f"that would find small/new apps in the same niche. "
            f"Return ONLY a JSON array of strings, nothing else."
        )
    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.9, max_tokens=400
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"```[a-z]*", "", raw).replace("```", "").strip()
        kws = json.loads(raw)
        push_log(f"AI keywords: {kws}")
        new_kws = [k for k in kws if k not in used]
        # If AI returns nothing useful, also add fallback keywords
        if len(new_kws) < 3:
            push_log("AI returned few keywords — adding fallback expansion")
            new_kws.extend(_fallback_keywords(original, used + new_kws))
        return new_kws
    except Exception as e:
        push_log(f"AI keyword error: {e} — using built-in fallback")
        return _fallback_keywords(original, used)


def _fallback_keywords(original: str, used: list) -> list:
    """
    Built-in keyword expansion — used when AI fails or returns too few results.
    Generates suffix/prefix variants that reliably find different app sets.
    """
    base = original.lower().strip()
    suffixes = [
        "app", "tool", "free", "lite", "simple", "easy", "best",
        "tracker", "manager", "helper", "pro", "smart", "quick",
        "mobile", "android", "online", "fast", "top", "new",
    ]
    prefixes = ["free", "best", "simple", "easy", "smart", "quick", "top"]
    candidates = []
    for s in suffixes:
        candidates.append(f"{base} {s}")
    for p in prefixes:
        candidates.append(f"{p} {base}")
    # Add word splits / recombinations if multi-word
    words = base.split()
    if len(words) > 1:
        candidates.extend(words)          # individual words
        candidates.append(" ".join(reversed(words)))  # reversed order
    return [k for k in candidates if k not in used][:10]

# ── AI email generation per lead ──────────────────────────────────────────────
def ai_gen_email(lead: dict, base_subject: str, base_body: str):
    """
    Generate a personalized email for `lead`.

    1. Selects the correct template (NEW APP vs OLD APP) based on score presence.
    2. Formats score to 1 decimal place before injecting.
    3. Falls back to template fill if AI is unavailable or fails.
    """
    key = get_cfg("GROQ_API_KEY")
    sender_name    = get_cfg("SENDER_NAME", "Your Name")
    sender_company = get_cfg("SENDER_COMPANY", "Your Company")

    # ── Step 1: Select correct template for this lead ─────────────────────────
    tpl_subject, tpl_body = select_template(lead, base_subject, base_body)

    # ── Step 2: No AI key — use template fill directly ────────────────────────
    if not key:
        return personalize_template(tpl_subject, lead), personalize_template(tpl_body, lead)

    # ── Step 3: Build AI prompt with correct template + formatted score ────────
    client = Groq(api_key=key)

    score_fmt    = format_score(lead.get("score"))
    score_info   = f"{score_fmt} stars" if score_fmt else "no ratings yet (brand new)"
    install_info = f"{lead['installs']:,} installs" if lead.get("installs") else "just launched"

    # Pre-fill template so AI sees already-resolved variables where possible
    prefilled_subject = personalize_template(tpl_subject, lead)
    prefilled_body    = personalize_template(tpl_body, lead)

    template_type = "OLD APP (has rating)" if score_fmt else "NEW APP (no rating)"

    prompt = f"""You are a cold email personalizer. Your only job is to fill in the base template with the real app details — keeping the structure and wording almost identical.

TEMPLATE TYPE: {template_type}

BASE TEMPLATE (follow this EXACTLY):
Subject: {tpl_subject}
Body:
{tpl_body}

APP DETAILS:
- App Name: {lead.get('app_name', '')}
- Developer: {lead.get('developer', '')}
- Category: {lead.get('category', 'app')}
- Installs: {install_info}
- Rating: {score_info}
- Play Store URL: {lead.get('url', '')}

SENDER:
- Name: {sender_name}
- Company: {sender_company}

STRICT RULES:
1. Copy the template EXACTLY — same structure, same sentences, same flow
2. Replace ALL {{{{variable}}}} placeholders with real values. score = "{score_fmt or 'N/A'}", installs = "{install_info}"
3. You may change at most 2-3 words in the entire body to naturally fit this specific app — nothing more
4. Do NOT rewrite sentences, do NOT add new sentences, do NOT remove any sentences
5. Do NOT change the greeting format, CTA, or sign-off
6. CRITICAL: Preserve every line break and blank line from the template exactly as-is. Use \\n for newlines inside the JSON string.
7. NEVER leave any {{{{variable}}}} placeholder in your output — replace every single one
8. Return ONLY valid JSON: {{"subject": "...", "body": "..."}}
No markdown, no explanation, just the JSON object."""

    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3, max_tokens=600
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"```[a-z]*", "", raw).replace("```", "").strip()
        data = json.loads(raw)

        subject = data.get("subject") or prefilled_subject
        body    = data.get("body")    or prefilled_body
        body    = body.replace("\\n", "\n")

        # Final safety pass: strip any leftover raw placeholders
        subject = re.sub(r"\{\{[a-zA-Z_]+\}\}", "", subject)
        body    = re.sub(r"\{\{[a-zA-Z_]+\}\}", "", body)

        return subject, body

    except Exception as e:
        push_log(f"  AI email error (using template fallback): {e}")
        return prefilled_subject, prefilled_body

# ── Dual-template defaults ────────────────────────────────────────────────────
# NEW APP TEMPLATE — used when lead has NO score/rating
DEFAULT_NEW_APP_SUBJECT = "Quick question about {{app_name}}"
DEFAULT_NEW_APP_BODY = """Hi {{developer}} team,

I came across {{app_name}} on Google Play — a {{category}} app that's still in its early growth phase with {{installs}} installs.

As a new app, building a strong reputation from day one is critical. I run a Play Store growth service that helps developers like you boost visibility, gather early positive reviews, and establish credibility before the competition catches on.

Would you be open to a quick 15-minute chat this week?

Best regards,
{{sender_name}}
{{sender_company}}

App: {{url}}"""

# OLD APP TEMPLATE — used when lead HAS a score/rating
DEFAULT_OLD_APP_SUBJECT = "Noticed {{app_name}}'s {{score}}★ rating — quick idea"
DEFAULT_OLD_APP_BODY = """Hi {{developer}} team,

I came across {{app_name}} on Google Play and noticed it currently holds a {{score}}★ rating in the {{category}} category with {{installs}} installs.

A rating in this range often means there are fixable issues hurting your reputation. I run a Play Store review recovery service that helps developers like you quickly clean up rating problems, respond to bad reviews professionally, and turn things around before it impacts downloads.

Would you be open to a quick 15-minute chat this week?

Best regards,
{{sender_name}}
{{sender_company}}

App: {{url}}"""

# Legacy single-template fallback (kept for backward compatibility)
DEFAULT_EMAIL_SUBJECT = DEFAULT_NEW_APP_SUBJECT
DEFAULT_EMAIL_BODY    = DEFAULT_NEW_APP_BODY


# ── Score formatting helper ───────────────────────────────────────────────────
def format_score(score) -> str:
    """
    Convert a raw score value to a 1-decimal-place string.
    Returns empty string if score is NULL / empty / falsy.

    Examples:
        1.222222 → "1.2"
        4.98765  → "5.0"
        None     → ""
        ""       → ""
        0        → ""   (treat as no rating)
    """
    if score is None or score == "" or score == 0:
        return ""
    try:
        val = float(score)
        if val <= 0:
            return ""
        return f"{val:.1f}"
    except (TypeError, ValueError):
        return ""


# ── Template selector ─────────────────────────────────────────────────────────
def select_template(lead: dict, base_subject: str = "", base_body: str = "") -> tuple:
    """
    Choose the correct subject+body pair based on whether the lead has a score.

    If custom templates were provided via config they take priority — but we
    still route between the custom NEW-APP vs OLD-APP variants if both are
    stored.  When only one custom template exists we fall back to it for both
    paths (backward-compatible behaviour).

    Returns: (subject: str, body: str)
    """
    has_rating = bool(format_score(lead.get("score")))

    # Custom NEW/OLD app templates from run config (set via dashboard)
    custom_new_subject = get_cfg("NEW_APP_EMAIL_SUBJECT")
    custom_new_body    = get_cfg("NEW_APP_EMAIL_BODY")
    custom_old_subject = get_cfg("OLD_APP_EMAIL_SUBJECT")
    custom_old_body    = get_cfg("OLD_APP_EMAIL_BODY")

    if has_rating:
        subject = custom_old_subject or base_subject or DEFAULT_OLD_APP_SUBJECT
        body    = custom_old_body    or base_body    or DEFAULT_OLD_APP_BODY
    else:
        subject = custom_new_subject or base_subject or DEFAULT_NEW_APP_SUBJECT
        body    = custom_new_body    or base_body    or DEFAULT_NEW_APP_BODY

    return subject, body


# ── Personalization engine ────────────────────────────────────────────────────
def personalize_template(tpl: str, lead: dict) -> str:
    """
    Replace all {{variable}} placeholders with lead-specific values.
    - Score is always formatted to 1 decimal place.
    - Missing values fall back to safe empty strings.
    - No raw placeholder is ever left in the output.
    """
    sender_name    = get_cfg("SENDER_NAME", "Your Name")
    sender_company = get_cfg("SENDER_COMPANY", "Your Company")

    score_fmt = format_score(lead.get("score"))

    installs_raw = lead.get("installs")
    if installs_raw:
        try:
            installs_str = f"{int(installs_raw):,}"
        except (TypeError, ValueError):
            installs_str = str(installs_raw)
    else:
        installs_str = "growing app"

    category  = lead.get("category", "") or "app"
    developer = lead.get("developer", "") or ""

    filled = (tpl
        .replace("{{app_name}}",       lead.get("app_name", ""))
        .replace("{{developer}}",      developer)
        .replace("{{category}}",       category)
        .replace("{{installs}}",       installs_str)
        .replace("{{score}}",          score_fmt)
        .replace("{{url}}",            lead.get("url", ""))
        .replace("{{sender_name}}",    sender_name)
        .replace("{{sender_company}}", sender_company)
    )

    # Safety net: strip any leftover raw placeholders so nothing leaks into email
    filled = re.sub(r"\{\{[a-zA-Z_]+\}\}", "", filled)
    return filled


# ── Legacy fill_template (kept for backward compatibility) ────────────────────
def fill_template(tpl: str, lead: dict) -> str:
    """Backward-compatible wrapper — now delegates to personalize_template."""
    return personalize_template(tpl, lead)

# ── Play Store scraper ────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Normal mode search regions
SEARCH_COMBOS = [
    ("en", "us"), ("en", "gb"), ("en", "in"), ("en", "au"), ("en", "ca"),
]

# Hunter mode — broader region coverage for maximum app discovery
HUNTER_SEARCH_COMBOS = [
    ("en", "us"), ("en", "gb"), ("en", "in"), ("en", "au"), ("en", "ca"),
    ("en", "ng"), ("en", "za"), ("en", "ph"), ("en", "pk"), ("en", "bd"),
    ("en", "ke"), ("en", "gh"), ("en", "nz"), ("en", "sg"), ("en", "ie"),
]

def extract_email(text):
    if not text:
        return ""
    m = EMAIL_RE.search(str(text))
    return m.group(0) if m else ""

def passes_filter(installs: int, score, ratings_count: int, hunter: dict) -> bool:
    """
    Filter logic for both Normal Mode and Hunter Mode.

    Hunter Mode strict rules (only weak-performing EXISTING apps):
      - Must have at least 1 review (ratings_count > 0)
      - Rating must be > 0 AND <= max_score (user input)
      - Installs must be <= max_installs (user input)
      - EXCLUDES: apps with 0 reviews, 0 rating, brand-new apps with no data

    Normal Mode:
      - installs <= 10,000
      - score <= 3.5 OR no score yet (new apps are OK in normal mode)
    """
    if hunter and hunter.get("active"):
        max_inst  = int(hunter.get("max_installs") or 5000)
        max_score = float(hunter.get("max_score") or 2.5)

        # Must have at least 1 real review
        if not ratings_count or ratings_count < 1:
            return False
        # Rating must exist and be > 0
        if not score or score <= 0:
            return False
        # Rating must not exceed user's threshold
        if score > max_score:
            return False
        # Installs must be within user's threshold
        if installs > max_inst:
            return False
        return True

    # Normal Mode — new apps (no score) are allowed; cap on installs + bad ratings
    if installs > 10_000:
        return False
    if score is not None and score > 3.5:
        return False
    return True

def scrape_keyword(keyword: str, hunter: dict = None) -> list:
    """Scrape Google Play for one keyword; return qualifying, non-duplicate leads."""
    global global_seen_ids, global_seen_emails
    push_log(f"Scraping: '{keyword}'")
    leads = []

    # Hunter mode uses broader region set for maximum discovery
    combos = HUNTER_SEARCH_COMBOS if (hunter and hunter.get("active")) else SEARCH_COMBOS
    for lang, country in combos:
        if stop_event.is_set():
            break
        try:
            results = search(keyword, lang=lang, country=country, n_hits=500)
        except Exception as e:
            push_log(f"  Search error ({country}): {e}")
            continue

        for item in results:
            if stop_event.is_set():
                break

            app_id = item.get("appId", "")
            # DUPLICATE PREVENTION: skip if app_id was seen in any previous keyword
            if not app_id or app_id in global_seen_ids:
                continue

            try:
                details = gp_app(app_id, lang="en", country="us")
            except Exception:
                global_seen_ids.add(app_id)
                continue

            installs      = details.get("minInstalls") or 0
            score         = details.get("score")
            ratings_count = details.get("ratings") or 0  # total review count

            if not passes_filter(installs, score, ratings_count, hunter):
                global_seen_ids.add(app_id)
                continue

            email = (
                extract_email(details.get("developerEmail", ""))
                or extract_email(details.get("privacyPolicy", ""))
                or extract_email(details.get("description", ""))
                or extract_email(details.get("recentChanges", ""))
            )
            # DUPLICATE PREVENTION: skip if email already collected
            if not email or email in global_seen_emails:
                global_seen_ids.add(app_id)
                continue

            lead = {
                "app_id":      app_id,
                "app_name":    details.get("title", ""),
                "developer":   details.get("developer", ""),
                "email":       email,
                "category":    details.get("genre", ""),
                "installs":    installs,
                "score":       score,
                "ratings":     ratings_count,
                "description": (details.get("description") or "")[:300],
                "url":         f"https://play.google.com/store/apps/details?id={app_id}",
                "icon":        details.get("icon", ""),
                "keyword":     keyword,
                "scraped_at":  time.strftime("%Y-%m-%d %H:%M:%S"),
                "email_sent":  False,
            }
            leads.append(lead)
            global_seen_ids.add(app_id)
            global_seen_emails.add(email)
            score_str = f"{score:.1f}★" if score else "new"
            push_log(f"  OK {lead['app_name']} | {installs:,} installs | {score_str} | {email}")

            if stop_event.wait(0.25):
                break

        push_log(f"  [{country}] done. Leads so far: {len(leads)}")
        if stop_event.wait(0.5):
            break

    push_log(f"  {len(leads)} new leads from '{keyword}'")
    sheet_log_keyword(keyword, len(leads))
    return leads

# ── Email URL helpers ─────────────────────────────────────────────────────────
def get_email_urls() -> list:
    raw = get_cfg("EMAIL_SCRIPT_URL").replace(",", "\n").split("\n")
    return [u.strip() for u in raw if u.strip()]

def reset_email_quotas(urls: list):
    """Reset all URL quota state at the start of each run."""
    with email_state_lock:
        global email_url_quotas
        email_url_quotas = {u: {"exhausted": False, "failed": False} for u in urls}

def mark_url_exhausted(url: str):
    with email_state_lock:
        if url in email_url_quotas:
            email_url_quotas[url]["exhausted"] = True

def mark_url_failed(url: str):
    with email_state_lock:
        if url in email_url_quotas:
            email_url_quotas[url]["failed"] = True

def all_urls_exhausted(urls: list) -> bool:
    with email_state_lock:
        return all(email_url_quotas.get(u, {}).get("exhausted", False) for u in urls)

def reset_exhausted_urls(urls: list):
    """Clear quota flags after cooldown so retry can attempt sending again."""
    with email_state_lock:
        for u in urls:
            if u in email_url_quotas:
                email_url_quotas[u]["exhausted"] = False

# ── Email send (quota-aware, multi-URL) ──────────────────────────────────────
def send_email(lead: dict, subject: str, body: str):
    """
    Try each URL in order, skipping ones already marked exhausted.
    Returns: ("ok","") | ("quota","All URLs exhausted") | ("error","...")
    """
    urls = get_email_urls()
    if not urls or not lead.get("email"):
        push_log("EMAIL_SCRIPT_URL not set or no email")
        return "error", "Missing config"

    quota_hits = 0

    for url in urls:
        with email_state_lock:
            if email_url_quotas.get(url, {}).get("exhausted", False):
                quota_hits += 1
                continue

        try:
            r = requests.post(url, json={
                "to":      lead["email"],
                "subject": subject,
                "body":    body,
            }, timeout=30, allow_redirects=True)

            if "html" in r.headers.get("Content-Type", "").lower():
                push_log("  Email URL deployed incorrectly (must be 'Execute as: Me', 'Access: Anyone').")
                mark_url_failed(url)
                continue

            result = r.json() if r.text else {}

            if result.get("status") == "ok":
                push_log(f"  Email sent: {lead['email']}")
                return "ok", ""

            err_msg = result.get("msg", "?")

            if "Service invoked too many times" in err_msg:
                push_log("  Quota limit hit on a URL. Trying next…")
                mark_url_exhausted(url)
                quota_hits += 1
                continue
            elif "permission" in err_msg.lower() or "authorize" in err_msg.lower():
                push_log("  URL needs authorization — run the script once in Google Script Editor.")
                mark_url_failed(url)
                continue
            else:
                push_log(f"  Email failed: {err_msg}. Trying next…")
                continue

        except Exception as e:
            push_log(f"  Email error: {e}")
            continue

    if quota_hits >= len(urls):
        push_log("  All email scripts have hit Google's daily limit.")
        return "quota", "All URLs exhausted"

    push_log("  All email scripts failed for this lead.")
    return "error", "All URLs failed"

# ── Cooldown / retry scheduler ────────────────────────────────────────────────
COOLDOWN_SECONDS = 3600  # 1 hour

def _is_automation_running() -> bool:
    with state_lock:
        return state.get("running", False)

def _cancel_cooldown_retry():
    """Signal any active cooldown retry thread to stop."""
    global cooldown_retry_thread
    cooldown_retry_cancel.set()
    cooldown_retry_thread = None

def _schedule_email_retry(leads_to_send: list, base_subject: str, base_body: str):
    """
    Background retry thread:
      1. Waits COOLDOWN_SECONDS (1 hour), waking every second to check for cancel.
      2. If cancel signal received (manual task started) → exit silently.
      3. After cooldown, if another automation is running → skip retry.
      4. Attempt to send remaining leads.
      5. If quota still exhausted → re-enter another cooldown cycle.
    """
    global global_cooldown_until

    cooldown_retry_cancel.clear()
    with email_state_lock:
        global_cooldown_until = time.time() + COOLDOWN_SECONDS

    push_log(f"  Email cooldown started. Will retry in 1 hour.")

    for _ in range(COOLDOWN_SECONDS):
        if cooldown_retry_cancel.is_set():
            push_log("  Email cooldown retry cancelled (user started a task manually).")
            with email_state_lock:
                global_cooldown_until = 0.0
            return
        time.sleep(1)

    with email_state_lock:
        global_cooldown_until = 0.0

    if _is_automation_running():
        push_log("  Cooldown over, but automation is running — skipping auto-resume.")
        return

    push_log("  Cooldown over. Resetting quota flags and retrying emails…")
    urls = get_email_urls()
    reset_exhausted_urls(urls)

    sent = 0
    for i, lead in enumerate(leads_to_send):
        if stop_event.is_set() or cooldown_retry_cancel.is_set():
            break

        subject, body = ai_gen_email(lead, base_subject, base_body)
        status, _ = send_email(lead, subject, body)

        if status == "ok":
            sent += 1
            sheet_mark_sent(lead["app_id"], lead["email"], lead["app_name"])
            with state_lock:
                state["emails_sent"] = state.get("emails_sent", 0) + 1
        elif status == "quota":
            push_log(f"  Limits still exhausted. Re-entering cooldown for {len(leads_to_send) - i} remaining leads…")
            # Re-schedule for the remaining unsent leads
            global cooldown_retry_thread
            cooldown_retry_thread = threading.Thread(
                target=_schedule_email_retry,
                args=(leads_to_send[i:], base_subject, base_body),
                daemon=True
            )
            cooldown_retry_thread.start()
            return
        else:
            push_log("  Could not send email. Moving to next lead…")

        if i < len(leads_to_send) - 1:
            wait = random.uniform(30, 60)
            push_log(f"  Waiting {wait:.0f}s … ({i+1}/{len(leads_to_send)})")
            if stop_event.wait(wait):
                break

    push_log(f"  Retry complete. {sent} additional emails sent.")

# ── Email sending loop ────────────────────────────────────────────────────────
def email_loop(leads: list, base_subject: str, base_body: str):
    """
    Iterate leads and send emails.
    On global quota exhaustion: queue remaining leads and start cooldown retry thread.
    """
    global cooldown_retry_thread

    for i, lead in enumerate(leads):
        if stop_event.is_set():
            push_log("Stopped during email phase.")
            return

        # Safety guard: skip any lead already marked sent — never send twice
        if lead.get("email_sent"):
            push_log(f"  Skipping {lead['app_name']} — already sent.")
            continue

        if not lead.get("email"):
            push_log(f"  Skipping {lead['app_name']} — no email address.")
            continue

        push_log(f"  AI writing email for {lead['app_name']} … [{'OLD APP' if format_score(lead.get('score')) else 'NEW APP'} template]")
        subject, body = ai_gen_email(lead, base_subject, base_body)

        status, _ = send_email(lead, subject, body)

        if status == "ok":
            lead["email_sent"] = True
            with state_lock:
                state["emails_sent"] = state.get("emails_sent", 0) + 1
                state["leads"] = [l.copy() for l in leads]
            sheet_mark_sent(lead["app_id"], lead["email"], lead["app_name"])

        elif status == "quota":
            # All URLs exhausted — hand off remaining leads to the retry scheduler
            remaining = leads[i:]
            push_log(f"  All email quotas exhausted. {len(remaining)} leads queued for 1-hour retry.")
            cooldown_retry_thread = threading.Thread(
                target=_schedule_email_retry,
                args=(remaining, base_subject, base_body),
                daemon=True
            )
            cooldown_retry_thread.start()
            return

        else:
            push_log("  Could not send email. Moving to next lead…")

        if stop_event.is_set():
            return

        if i < len(leads) - 1:
            wait = random.uniform(30, 60)
            push_log(f"  Waiting {wait:.0f}s … ({i+1}/{len(leads)})")
            if stop_event.wait(wait):
                return

# ── Master automation ─────────────────────────────────────────────────────────
def run_automation(initial_kw: str, target: int, hunter: dict = None):
    global cooldown_retry_thread

    # Cancel pending cooldown retry before starting fresh
    if cooldown_retry_thread and cooldown_retry_thread.is_alive():
        _cancel_cooldown_retry()
        push_log("  Cancelled pending email retry (new automation starting).")

    upd(running=True, phase="scraping", keyword=initial_kw,
        keywords_used=[], leads_found=0, emails_sent=0, logs=[], leads=[])
    stop_event.clear()

    mode = "Hunter" if (hunter and hunter.get("active")) else "Normal"
    push_log(f"Started | kw='{initial_kw}' | target={target} | mode={mode}")

    # base_subject / base_body kept for backward compat; select_template() will
    # override per-lead with the correct NEW/OLD variant at send time.
    base_subject = get_cfg("EMAIL_SUBJECT") or ""
    base_body    = get_cfg("EMAIL_BODY")    or ""

    urls = get_email_urls()
    reset_email_quotas(urls)

    all_leads = []
    kws_used  = [initial_kw]
    kw_queue  = [initial_kw]
    is_hunter = bool(hunter and hunter.get("active"))

    # Hunter mode: track consecutive empty rounds to trigger filter relaxation
    empty_rounds     = 0       # consecutive keywords that returned 0 leads
    ai_refill_count  = 0       # how many times we asked AI for more keywords
    MAX_EMPTY_ROUNDS = 3       # after this many dry keywords → expand aggressively
    MAX_AI_REFILLS   = 20      # hard cap on AI calls to prevent infinite loop
    # Relaxed hunter filter thresholds (applied after MAX_EMPTY_ROUNDS)
    relaxed_max_installs = None
    relaxed_max_score    = None

    # ── Phase 1: Scrape ───────────────────────────────────────────────────────
    while len(all_leads) < target and not stop_event.is_set():

        # ── Refill keyword queue when empty ───────────────────────────────────
        if not kw_queue:
            if ai_refill_count >= MAX_AI_REFILLS:
                push_log(f"⚠️ Reached AI refill limit ({MAX_AI_REFILLS}). "
                         f"Collected {len(all_leads)}/{target} leads.")
                break

            push_log(f"🔄 Keyword queue empty — requesting AI expansion "
                     f"(attempt {ai_refill_count + 1})…")
            new_kws = ai_gen_keywords(initial_kw, kws_used, hunter)
            ai_refill_count += 1

            if new_kws:
                kw_queue.extend(new_kws)
                push_log(f"  Added {len(new_kws)} new keywords to queue.")
            else:
                # AI and fallback both dry — generate suffix variants of used kws
                push_log("  AI + fallback exhausted — generating suffix variants…")
                base_pool = kws_used[-5:] if len(kws_used) > 5 else kws_used
                for base in base_pool:
                    variants = _fallback_keywords(base, kws_used + kw_queue)
                    kw_queue.extend(variants[:3])
                if not kw_queue:
                    push_log("❌ No more keywords possible. Stopping scrape.")
                    break

        # ── Apply adaptive filter relaxation in Hunter Mode ───────────────────
        # After many consecutive dry runs, loosen filters to capture more leads
        active_hunter = dict(hunter) if is_hunter and hunter else None
        if is_hunter and active_hunter and empty_rounds >= MAX_EMPTY_ROUNDS:
            orig_inst  = int(hunter.get("max_installs") or 5000)
            orig_score = float(hunter.get("max_score") or 2.5)

            # Progressive relaxation: +50% installs, +0.5 score per relaxation round
            relax_steps = max(1, empty_rounds - MAX_EMPTY_ROUNDS + 1)
            relaxed_inst  = min(orig_inst  * (1 + 0.5 * relax_steps), 50_000)
            relaxed_score = min(orig_score + 0.5 * relax_steps, 4.0)

            if (relaxed_inst  != relaxed_max_installs or
                    relaxed_score != relaxed_max_score):
                relaxed_max_installs = relaxed_inst
                relaxed_max_score    = relaxed_score
                push_log(f"  🔓 Filter relaxed → max_installs={int(relaxed_inst):,} "
                         f"max_score={relaxed_score:.1f} "
                         f"(original: {orig_inst:,} / {orig_score:.1f})")

            active_hunter = dict(hunter)
            active_hunter["max_installs"] = int(relaxed_inst)
            active_hunter["max_score"]    = relaxed_score

        # ── Process next keyword ───────────────────────────────────────────────
        kw = kw_queue.pop(0)
        if kw in kws_used:
            continue           # already processed — skip silently
        kws_used.append(kw)
        upd(keywords_used=kws_used[:], phase="scraping")

        remaining = target - len(all_leads)
        push_log(f"🔍 Searching: '{kw}' | Need {remaining} more leads …")

        batch = scrape_keyword(kw, active_hunter if is_hunter else hunter)
        all_leads.extend(batch)
        upd(leads_found=len(all_leads), leads=[l.copy() for l in all_leads])

        for lead in batch:
            sheet_append_lead(lead)
            sheet_append_qualified(lead)

        if batch:
            empty_rounds = 0    # reset dry-run counter on any success
            push_log(f"  ✅ +{len(batch)} leads | Total: {len(all_leads)} / {target}")
        else:
            empty_rounds += 1
            push_log(f"  ⚠️ No leads from '{kw}' "
                     f"({empty_rounds} consecutive dry run(s)) | "
                     f"Total: {len(all_leads)} / {target}")
            if is_hunter and empty_rounds >= MAX_EMPTY_ROUNDS:
                push_log(f"  🔄 {MAX_EMPTY_ROUNDS} dry runs — expanding search …")

        # ── Progress check ─────────────────────────────────────────────────────
        if len(all_leads) >= target:
            push_log(f"🎯 Target reached! {len(all_leads)} / {target} leads collected.")
        elif len(all_leads) < target and not kw_queue:
            push_log(f"  📊 {len(all_leads)}/{target} — continuing to expand search …")

    if stop_event.is_set():
        push_log("Stopped during scraping.")
        upd(running=False, phase="stopped")
        return

    push_log(f"✅ Scraping complete. {len(all_leads)} leads collected. Starting emails …")

    # ── Phase 2: AI Email + Send ──────────────────────────────────────────────
    upd(phase="emailing")
    email_loop(all_leads, base_subject, base_body)

    if stop_event.is_set():
        upd(running=False, phase="stopped")
    else:
        push_log("Automation complete!")
        upd(running=False, phase="done")

# ── Send pending ──────────────────────────────────────────────────────────────
def run_send_pending(leads: list):
    global cooldown_retry_thread

    # Cancel pending cooldown retry before starting manual send
    if cooldown_retry_thread and cooldown_retry_thread.is_alive():
        _cancel_cooldown_retry()
        push_log("  Cancelled pending email retry (manual send starting).")

    upd(running=True, phase="emailing")
    stop_event.clear()
    push_log(f"Sending pending: {len(leads)} leads")

    base_subject = get_cfg("EMAIL_SUBJECT") or ""
    base_body    = get_cfg("EMAIL_BODY")    or ""

    urls = get_email_urls()
    reset_email_quotas(urls)

    email_loop(leads, base_subject, base_body)

    push_log("Pending send complete.")
    upd(running=False, phase="done")

# ── Routes ────────────────────────────────────────────────────────────────────
@application.route("/")
def index():
    return send_from_directory(".", "dashboard.html")

@application.route("/api/start", methods=["POST"])
def api_start():
    data    = request.get_json(silent=True) or {}
    keyword = (data.get("keyword") or "").strip()
    if not keyword:
        return jsonify({"error": "keyword required"}), 400
    with state_lock:
        if state["running"]:
            return jsonify({"error": "Already running"}), 409
    global run_cfg
    run_cfg = {
        "GROQ_API_KEY":          data.get("groq_key")             or os.environ.get("GROQ_API_KEY", ""),
        "APPS_SCRIPT_WEB_URL":   data.get("sheet_url")            or os.environ.get("APPS_SCRIPT_WEB_URL", ""),
        "EMAIL_SCRIPT_URL":      data.get("email_script_url")     or os.environ.get("EMAIL_SCRIPT_URL", ""),
        "SENDER_NAME":           data.get("sender_name")          or os.environ.get("SENDER_NAME", ""),
        "SENDER_COMPANY":        data.get("sender_company")       or os.environ.get("SENDER_COMPANY", ""),
        # Legacy single-template (used as fallback inside select_template)
        "EMAIL_SUBJECT":         data.get("email_subject")        or os.environ.get("EMAIL_SUBJECT", ""),
        "EMAIL_BODY":            data.get("email_body")           or os.environ.get("EMAIL_BODY", ""),
        # Dual-template: NEW APP (no rating)
        "NEW_APP_EMAIL_SUBJECT": data.get("new_app_email_subject") or os.environ.get("NEW_APP_EMAIL_SUBJECT", ""),
        "NEW_APP_EMAIL_BODY":    data.get("new_app_email_body")    or os.environ.get("NEW_APP_EMAIL_BODY", ""),
        # Dual-template: OLD APP (has rating)
        "OLD_APP_EMAIL_SUBJECT": data.get("old_app_email_subject") or os.environ.get("OLD_APP_EMAIL_SUBJECT", ""),
        "OLD_APP_EMAIL_BODY":    data.get("old_app_email_body")    or os.environ.get("OLD_APP_EMAIL_BODY", ""),
    }
    target = int(data.get("target") or os.environ.get("TARGET_LEADS", 300))
    hunter = data.get("hunter") or {}
    threading.Thread(target=run_automation, args=(keyword, target, hunter), daemon=True).start()
    return jsonify({"ok": True, "keyword": keyword})

@application.route("/api/stop", methods=["POST"])
def api_stop():
    """
    Instant stop: sets stop_event immediately.
    All loops check stop_event at every iteration and on every wait(),
    so they halt at the next checkpoint with no long blocking delays.
    Also cancels any pending cooldown retry.
    """
    stop_event.set()
    _cancel_cooldown_retry()
    upd(running=False, phase="stopped")
    push_log("Stop requested.")
    return jsonify({"ok": True})

@application.route("/api/status")
def api_status():
    with state_lock:
        s = dict(state)
    with email_state_lock:
        remaining = max(0, global_cooldown_until - time.time())
    s["cooldown_remaining_seconds"] = int(remaining)
    return jsonify(s)

@application.route("/api/clear", methods=["POST"])
def api_clear():
    global global_seen_ids, global_seen_emails
    with state_lock:
        if state["running"]:
            return jsonify({"error": "Cannot clear while running"}), 409
        state.update({
            "running": False, "phase": "idle", "keyword": "",
            "keywords_used": [], "leads_found": 0, "emails_sent": 0,
            "logs": [], "leads": []
        })
    global_seen_ids    = set()
    global_seen_emails = set()
    log.info("History cleared.")
    return jsonify({"ok": True})

@application.route("/api/ping", methods=["GET", "POST"])
def api_ping():
    return jsonify({"ok": True, "ts": time.time()})

@application.route("/api/send_pending", methods=["POST"])
def api_send_pending():
    with state_lock:
        if state["running"]:
            return jsonify({"error": "Automation is running"}), 409
    data  = request.get_json(silent=True) or {}
    leads = data.get("leads") or []
    if not leads:
        return jsonify({"error": "No leads provided"}), 400
    global run_cfg
    run_cfg = {
        "GROQ_API_KEY":          data.get("groq_key")             or os.environ.get("GROQ_API_KEY", ""),
        "EMAIL_SCRIPT_URL":      data.get("email_script_url")     or os.environ.get("EMAIL_SCRIPT_URL", ""),
        "SENDER_NAME":           data.get("sender_name")          or os.environ.get("SENDER_NAME", ""),
        "SENDER_COMPANY":        data.get("sender_company")       or os.environ.get("SENDER_COMPANY", ""),
        "EMAIL_SUBJECT":         data.get("email_subject")        or os.environ.get("EMAIL_SUBJECT", ""),
        "EMAIL_BODY":            data.get("email_body")           or os.environ.get("EMAIL_BODY", ""),
        "NEW_APP_EMAIL_SUBJECT": data.get("new_app_email_subject") or os.environ.get("NEW_APP_EMAIL_SUBJECT", ""),
        "NEW_APP_EMAIL_BODY":    data.get("new_app_email_body")    or os.environ.get("NEW_APP_EMAIL_BODY", ""),
        "OLD_APP_EMAIL_SUBJECT": data.get("old_app_email_subject") or os.environ.get("OLD_APP_EMAIL_SUBJECT", ""),
        "OLD_APP_EMAIL_BODY":    data.get("old_app_email_body")    or os.environ.get("OLD_APP_EMAIL_BODY", ""),
        "APPS_SCRIPT_WEB_URL":   data.get("sheet_url")            or os.environ.get("APPS_SCRIPT_WEB_URL", ""),
    }
    # Filter out any leads already marked sent before passing to backend
    # email_loop also has this guard, but filtering here avoids unnecessary AI calls
    fresh_leads = [l for l in leads if not l.get("email_sent") and l.get("email")]
    if not fresh_leads:
        return jsonify({"error": "No unsent leads with email in provided list"}), 400

    threading.Thread(target=run_send_pending, args=(fresh_leads,), daemon=True).start()
    return jsonify({"ok": True, "count": len(fresh_leads)})

@application.route("/api/spam_test", methods=["POST"])
def api_spam_test():
    data    = request.get_json(silent=True) or {}
    test_to = (data.get("test_email") or "").strip()
    if not test_to:
        return jsonify({"error": "test_email required"}), 400
    global run_cfg
    run_cfg = {
        "GROQ_API_KEY":          data.get("groq_key")             or os.environ.get("GROQ_API_KEY", ""),
        "EMAIL_SCRIPT_URL":      data.get("email_script_url")     or os.environ.get("EMAIL_SCRIPT_URL", ""),
        "SENDER_NAME":           data.get("sender_name")          or os.environ.get("SENDER_NAME", ""),
        "SENDER_COMPANY":        data.get("sender_company")       or os.environ.get("SENDER_COMPANY", ""),
        "EMAIL_SUBJECT":         data.get("email_subject")        or os.environ.get("EMAIL_SUBJECT", ""),
        "EMAIL_BODY":            data.get("email_body")           or os.environ.get("EMAIL_BODY", ""),
        "NEW_APP_EMAIL_SUBJECT": data.get("new_app_email_subject") or os.environ.get("NEW_APP_EMAIL_SUBJECT", ""),
        "NEW_APP_EMAIL_BODY":    data.get("new_app_email_body")    or os.environ.get("NEW_APP_EMAIL_BODY", ""),
        "OLD_APP_EMAIL_SUBJECT": data.get("old_app_email_subject") or os.environ.get("OLD_APP_EMAIL_SUBJECT", ""),
        "OLD_APP_EMAIL_BODY":    data.get("old_app_email_body")    or os.environ.get("OLD_APP_EMAIL_BODY", ""),
    }
    raw_score = data.get("sample_score")
    sample_score = float(raw_score) if raw_score else None
    sample = {
        "app_name":   data.get("sample_app_name", "MyApp Pro"),
        "developer":  data.get("sample_developer", "John Dev"),
        "category":   "Productivity",
        "installs":   1500,
        "score":      sample_score,
        "email":      test_to,
        "url":        "https://play.google.com/store/apps/details?id=com.example",
    }

    raw_urls = get_cfg("EMAIL_SCRIPT_URL").replace(",", "\n").split("\n")
    urls = [u.strip() for u in raw_urls if u.strip()]
    url = urls[0] if urls else None

    if not url:
        return jsonify({"error": "EMAIL_SCRIPT_URL not set"}), 400

    # Pass empty base strings so select_template() routes purely by lead score
    base_subject = get_cfg("EMAIL_SUBJECT") or ""
    base_body    = get_cfg("EMAIL_BODY")    or ""
    template_type = "OLD APP" if format_score(sample.get("score")) else "NEW APP"
    push_log(f"  Spam test → {template_type} template (score={sample.get('score')})")

    subject, body = ai_gen_email(sample, base_subject, base_body)
    try:
        r = requests.post(url, json={"to": test_to, "subject": subject, "body": body}, timeout=30)
        result = r.json() if r.text else {}
        if result.get("status") == "ok":
            return jsonify({
                "ok": True,
                "msg": f"Test sent to {test_to} [{template_type} template]",
                "template_type": template_type,
                "subject": subject,
                "body": body,
            })
        return jsonify({"error": result.get("msg", "Failed")}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@application.route("/api/sheet_pending", methods=["POST"])
def api_sheet_pending():
    data = request.get_json(silent=True) or {}
    sheet_url = data.get("sheet_url") or os.environ.get("APPS_SCRIPT_WEB_URL", "")
    if not sheet_url:
        return jsonify({"error": "sheet_url not set"}), 400
    try:
        r = requests.post(sheet_url, json={"action": "get_pending"}, timeout=20)
        result = r.json() if r.text else {}
        leads = result.get("leads", [])
        return jsonify({"ok": True, "count": len(leads), "leads": leads})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@application.route("/api/sheet_all", methods=["POST"])
def api_sheet_all():
    data = request.get_json(silent=True) or {}
    sheet_url = data.get("sheet_url") or os.environ.get("APPS_SCRIPT_WEB_URL", "")
    if not sheet_url:
        return jsonify({"error": "sheet_url not set"}), 400
    try:
        r = requests.post(sheet_url, json={"action": "get_all_leads"}, timeout=20)
        result = r.json() if r.text else {}
        leads = result.get("leads", [])
        return jsonify({"ok": True, "leads": leads})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@application.route("/api/send_single", methods=["POST"])
def api_send_single():
    """
    Send email to exactly one specific lead identified by app_id.
    Accepts the full lead object from the frontend so no sheet lookup is needed.
    Updates only that lead's email_sent status — no other records are touched.
    """
    with state_lock:
        if state["running"]:
            return jsonify({"error": "Automation is running"}), 409

    data = request.get_json(silent=True) or {}
    lead = data.get("lead")
    if not lead or not lead.get("app_id") or not lead.get("email"):
        return jsonify({"error": "Valid lead with app_id and email required"}), 400

    global run_cfg
    run_cfg = {
        "GROQ_API_KEY":          data.get("groq_key")             or os.environ.get("GROQ_API_KEY", ""),
        "EMAIL_SCRIPT_URL":      data.get("email_script_url")     or os.environ.get("EMAIL_SCRIPT_URL", ""),
        "SENDER_NAME":           data.get("sender_name")          or os.environ.get("SENDER_NAME", ""),
        "SENDER_COMPANY":        data.get("sender_company")       or os.environ.get("SENDER_COMPANY", ""),
        "EMAIL_SUBJECT":         data.get("email_subject")        or os.environ.get("EMAIL_SUBJECT", ""),
        "EMAIL_BODY":            data.get("email_body")           or os.environ.get("EMAIL_BODY", ""),
        "NEW_APP_EMAIL_SUBJECT": data.get("new_app_email_subject") or os.environ.get("NEW_APP_EMAIL_SUBJECT", ""),
        "NEW_APP_EMAIL_BODY":    data.get("new_app_email_body")    or os.environ.get("NEW_APP_EMAIL_BODY", ""),
        "OLD_APP_EMAIL_SUBJECT": data.get("old_app_email_subject") or os.environ.get("OLD_APP_EMAIL_SUBJECT", ""),
        "OLD_APP_EMAIL_BODY":    data.get("old_app_email_body")    or os.environ.get("OLD_APP_EMAIL_BODY", ""),
        "APPS_SCRIPT_WEB_URL":   data.get("sheet_url")            or os.environ.get("APPS_SCRIPT_WEB_URL", ""),
    }

    def _send_single_lead(lead_data: dict):
        upd(running=True, phase="emailing")
        stop_event.clear()
        push_log(f"Manual send: {lead_data['app_name']} <{lead_data['email']}>")

        urls = get_email_urls()
        reset_email_quotas(urls)

        base_subject = get_cfg("EMAIL_SUBJECT") or ""
        base_body    = get_cfg("EMAIL_BODY")    or ""

        push_log(f"  AI writing email for {lead_data['app_name']} … [{'OLD APP' if format_score(lead_data.get('score')) else 'NEW APP'} template]")
        subject, body = ai_gen_email(lead_data, base_subject, base_body)

        status, _ = send_email(lead_data, subject, body)

        if status == "ok":
            # Update only this specific lead in global state by matching app_id
            with state_lock:
                for l in state.get("leads", []):
                    if l.get("app_id") == lead_data["app_id"]:
                        l["email_sent"] = True
                        break
                state["emails_sent"] = state.get("emails_sent", 0) + 1
            sheet_mark_sent(lead_data["app_id"], lead_data["email"], lead_data["app_name"])
            push_log(f"  ✅ Manual send complete: {lead_data['email']}")
        elif status == "quota":
            push_log(f"  ⚠️ Quota exhausted — could not send to {lead_data['email']}")
        else:
            push_log(f"  ❌ Send failed for {lead_data['email']}")

        upd(running=False, phase="done")

    threading.Thread(target=_send_single_lead, args=(lead,), daemon=True).start()
    return jsonify({"ok": True, "app_id": lead["app_id"], "email": lead["email"]})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    application.run(host="0.0.0.0", port=port, debug=False)
