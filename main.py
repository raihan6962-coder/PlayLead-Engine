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
stop_event  = threading.Event()
state_lock  = threading.Lock()
state = {
    "running": False, "phase": "idle", "keyword": "",
    "keywords_used": [], "leads_found": 0, "emails_sent": 0,
    "logs": [], "leads": []
}

# ── Global duplicate tracker — persists across runs until clear ───────────────
global_seen_ids:    set = set()
global_seen_emails: set = set()

# ── Qualified sheet duplicate tracker (loaded fresh at start + cached) ────────
qualified_sheet_ids:    set = set()
qualified_sheet_emails: set = set()
qualified_cache_lock = threading.Lock()

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

# ── Multiple email script URL support ────────────────────────────────────────
# cfg: "EMAIL_SCRIPT_URL" can be newline-separated list of URLs.
# We rotate to the next URL on daily-limit errors.
_email_url_index = 0
_email_url_lock  = threading.Lock()

def get_email_urls() -> list:
    raw = get_cfg("EMAIL_SCRIPT_URL", "")
    urls = [u.strip() for u in raw.splitlines() if u.strip()]
    return urls if urls else []

def get_active_email_url() -> str:
    urls = get_email_urls()
    if not urls:
        return ""
    with _email_url_lock:
        idx = _email_url_index % len(urls)
    return urls[idx]

def rotate_email_url(reason: str = ""):
    global _email_url_index
    urls = get_email_urls()
    if len(urls) <= 1:
        return
    with _email_url_lock:
        _email_url_index = (_email_url_index + 1) % len(urls)
    push_log(f"  🔄 Email URL rotated → #{_email_url_index + 1} ({reason})")

# ── Google Sheet via Apps Script ──────────────────────────────────────────────
def sheet_post(payload: dict, url: str = None):
    target = url or get_cfg("APPS_SCRIPT_WEB_URL")
    if not target:
        return None
    try:
        r = requests.post(target, json=payload, timeout=15)
        return r.json() if r.text else {}
    except Exception as e:
        push_log(f"  Sheet error: {e}")
        return None

def sheet_append_lead(lead: dict):
    sheet_post({"action": "append", "tab": "All Leads", "row": {
        "App ID":     lead["app_id"],
        "App Name":   lead["app_name"],
        "Developer":  lead["developer"],
        "Email":      lead["email"],
        "Category":   lead["category"],
        "Installs":   lead["installs"],
        "Score":      lead["score"] or "",
        "URL":        lead["url"],
        "Keyword":    lead["keyword"],
        "Scraped At": lead["scraped_at"],
        "Email Sent": "No",
    }})

def sheet_append_qualified(lead: dict):
    sheet_post({"action": "append", "tab": "Qualified Leads", "row": {
        "App ID":     lead["app_id"],
        "App Name":   lead["app_name"],
        "Developer":  lead["developer"],
        "Email":      lead["email"],
        "Category":   lead["category"],
        "Installs":   lead["installs"],
        "Score":      lead["score"] or "",
        "URL":        lead["url"],
        "Keyword":    lead["keyword"],
        "Scraped At": lead["scraped_at"],
        "Email Sent": "Pending",
    }})

def sheet_mark_sent(app_id: str, email: str, app_name: str):
    sheet_post({"action": "mark_sent", "app_id": app_id,
                "email": email, "app_name": app_name})

def sheet_log_keyword(keyword: str, count: int):
    sheet_post({"action": "append", "tab": "Keyword Log", "row": {
        "Keyword":    keyword,
        "Leads Found": count,
        "Logged At":  time.strftime("%Y-%m-%d %H:%M:%S"),
    }})

# ── Load Qualified Sheet leads for duplicate filtering ────────────────────────
def load_qualified_sheet_duplicates():
    """
    Fetch all existing leads from the Qualified Leads sheet tab and
    populate qualified_sheet_ids / qualified_sheet_emails so we never
    re-add the same lead again.
    """
    global qualified_sheet_ids, qualified_sheet_emails
    sheet_url = get_cfg("APPS_SCRIPT_WEB_URL")
    if not sheet_url:
        return
    push_log("🔎 Loading Qualified Sheet for duplicate filter …")
    try:
        r = requests.post(sheet_url, json={"action": "get_all_leads"}, timeout=25)
        result = r.json() if r.text else {}
        leads = result.get("leads", [])
        with qualified_cache_lock:
            qualified_sheet_ids    = {str(l.get("app_id",  "")).strip() for l in leads if l.get("app_id")}
            qualified_sheet_emails = {str(l.get("email",   "")).strip().lower() for l in leads if l.get("email")}
        push_log(f"  ✅ Qualified Sheet: {len(qualified_sheet_ids)} existing leads loaded.")
    except Exception as e:
        push_log(f"  ⚠️  Could not load Qualified Sheet duplicates: {e}")

def is_qualified_duplicate(app_id: str, email: str) -> bool:
    with qualified_cache_lock:
        if app_id and app_id in qualified_sheet_ids:
            return True
        if email and email.lower() in qualified_sheet_emails:
            return True
    return False

def mark_qualified_seen(app_id: str, email: str):
    with qualified_cache_lock:
        if app_id:
            qualified_sheet_ids.add(app_id)
        if email:
            qualified_sheet_emails.add(email.lower())

# ── AI keyword generation ─────────────────────────────────────────────────────
def ai_gen_keywords(original: str, used: list) -> list:
    key = get_cfg("GROQ_API_KEY")
    if not key:
        push_log("GROQ_API_KEY not set")
        return []
    client = Groq(api_key=key)
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
            temperature=0.8, max_tokens=300
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"```[a-z]*", "", raw).replace("```", "").strip()
        kws = json.loads(raw)
        push_log(f"AI keywords: {kws}")
        return [k for k in kws if k not in used]
    except Exception as e:
        push_log(f"AI keyword error: {e}")
        return []

# ── Template defaults ─────────────────────────────────────────────────────────
DEFAULT_NEW_APP_SUBJECT = "Quick question about {{app_name}}"
DEFAULT_NEW_APP_BODY = """Hi {{developer}} team,

I came across {{app_name}} on Google Play — a {{category}} app that's still in its early growth phase with {{installs}} installs.

As a new app, building a strong reputation from day one is critical. I run a Play Store growth service that helps developers like you boost visibility, gather early positive reviews, and establish credibility before the competition catches on.

Would you be open to a quick 15-minute chat this week?

Best regards,
{{sender_name}}
{{sender_company}}

App: {{url}}"""

DEFAULT_OLD_APP_SUBJECT = "Noticed {{app_name}}'s {{score}}★ rating — quick idea"
DEFAULT_OLD_APP_BODY = """Hi {{developer}} team,

I came across {{app_name}} on Google Play and noticed it currently holds a {{score}}★ rating in the {{category}} category with {{installs}} installs.

A rating in this range often means there are fixable issues hurting your reputation. I run a Play Store review recovery service that helps developers like you quickly clean up rating problems, respond to bad reviews professionally, and turn things around before it impacts downloads.

Would you be open to a quick 15-minute chat this week?

Best regards,
{{sender_name}}
{{sender_company}}

App: {{url}}"""

# Legacy aliases
DEFAULT_EMAIL_SUBJECT = DEFAULT_NEW_APP_SUBJECT
DEFAULT_EMAIL_BODY    = DEFAULT_NEW_APP_BODY

# ── Score formatting ──────────────────────────────────────────────────────────
def format_score(score) -> str:
    """Return formatted score string or empty string if no valid score."""
    if score is None or score == "" or score == 0:
        return ""
    try:
        val = float(score)
        if val <= 0:
            return ""
        return f"{val:.1f}"
    except (TypeError, ValueError):
        return ""

# ── Choose correct template for a lead ───────────────────────────────────────
def pick_template(lead: dict, run_data: dict) -> tuple[str, str, str]:
    """
    Returns (base_subject, base_body, template_type).
    template_type = "NEW APP" | "OLD APP"
    Decision: if lead has no score (None / 0 / empty) → NEW APP template,
              else → OLD APP template.
    """
    score = lead.get("score")
    score_str = format_score(score)
    is_new_app = not score_str  # no valid score → new app

    if is_new_app:
        subject = (run_data.get("new_app_email_subject")
                   or get_cfg("NEW_APP_EMAIL_SUBJECT")
                   or DEFAULT_NEW_APP_SUBJECT)
        body    = (run_data.get("new_app_email_body")
                   or get_cfg("NEW_APP_EMAIL_BODY")
                   or DEFAULT_NEW_APP_BODY)
        return subject, body, "NEW APP"
    else:
        subject = (run_data.get("old_app_email_subject")
                   or get_cfg("OLD_APP_EMAIL_SUBJECT")
                   or DEFAULT_OLD_APP_SUBJECT)
        body    = (run_data.get("old_app_email_body")
                   or get_cfg("OLD_APP_EMAIL_BODY")
                   or DEFAULT_OLD_APP_BODY)
        return subject, body, "OLD APP"

# ── Template fill ─────────────────────────────────────────────────────────────
def fill_template(tpl: str, lead: dict) -> str:
    sender_name    = get_cfg("SENDER_NAME",    "Your Name")
    sender_company = get_cfg("SENDER_COMPANY", "Your Company")
    score_str      = format_score(lead.get("score"))
    installs_str   = (f"{lead['installs']:,}" if lead.get("installs")
                      else "growing app")
    return (tpl
        .replace("{{app_name}}",       lead.get("app_name",  ""))
        .replace("{{developer}}",      lead.get("developer", ""))
        .replace("{{category}}",       lead.get("category",  ""))
        .replace("{{installs}}",       installs_str)
        .replace("{{score}}",          score_str)
        .replace("{{url}}",            lead.get("url", ""))
        .replace("{{sender_name}}",    sender_name)
        .replace("{{sender_company}}", sender_company)
    )

# ── AI email generation per lead ──────────────────────────────────────────────
def ai_gen_email(lead: dict, base_subject: str, base_body: str) -> tuple[str, str]:
    """Generate a personalized email keeping the template structure intact."""
    key = get_cfg("GROQ_API_KEY")
    sender_name    = get_cfg("SENDER_NAME",    "Your Name")
    sender_company = get_cfg("SENDER_COMPANY", "Your Company")

    if not key:
        return fill_template(base_subject, lead), fill_template(base_body, lead)

    client = Groq(api_key=key)
    score_str    = format_score(lead.get("score"))
    score_info   = f"{score_str} stars" if score_str else "no ratings yet (brand new)"
    install_info = (f"{lead['installs']:,} installs"
                    if lead.get("installs") else "just launched")

    prompt = f"""You are a cold email personalizer. Your only job is to fill in the base template with the real app details — keeping the structure and wording almost identical.

BASE TEMPLATE (follow this EXACTLY):
Subject: {base_subject}
Body:
{base_body}

APP DETAILS:
- App Name: {lead.get('app_name', '')}
- Developer: {lead.get('developer', '')}
- Category: {lead.get('category', '')}
- Installs: {install_info}
- Rating: {score_info}
- Play Store URL: {lead.get('url', '')}

SENDER:
- Name: {sender_name}
- Company: {sender_company}

STRICT RULES:
1. Copy the template EXACTLY — same structure, same sentences, same flow
2. Only replace placeholder values (app name, developer name, installs, rating, url) with the real app details above
3. You may change at most 2-3 words in the entire body to naturally fit this specific app — nothing more
4. Do NOT rewrite sentences, do NOT add new sentences, do NOT remove any sentences
5. Do NOT change the greeting format, CTA, or sign-off
6. CRITICAL: Preserve every line break and blank line from the template exactly as-is. Each paragraph must stay as a separate paragraph. Use \\n for newlines inside the JSON string.
7. Return ONLY valid JSON: {{"subject": "...", "body": "..."}}
No markdown, no explanation, just the JSON object."""

    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3, max_tokens=500
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"```[a-z]*", "", raw).replace("```", "").strip()
        data = json.loads(raw)
        subject = data.get("subject") or fill_template(base_subject, lead)
        body    = data.get("body")    or fill_template(base_body, lead)
        body    = body.replace("\\n", "\n")
        return subject, body
    except Exception as e:
        push_log(f"  AI email error (using template fallback): {e}")
        return fill_template(base_subject, lead), fill_template(base_body, lead)

# ── Play Store scraper ────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

SEARCH_COMBOS = [
    ("en", "us"), ("en", "gb"), ("en", "in"), ("en", "au"), ("en", "ca"),
]

def extract_email(text):
    if not text:
        return ""
    m = EMAIL_RE.search(str(text))
    return m.group(0) if m else ""

def passes_filter(installs: int, score, hunter: dict) -> bool:
    if hunter and hunter.get("active"):
        max_inst  = int(hunter.get("max_installs") or 5000)
        max_score = float(hunter.get("max_score") or 2.5)
        if installs > max_inst:
            return False
        if score is not None and score > max_score:
            return False
        return True
    # Normal mode: <=10 000 installs, rating absent OR <=3.5
    if installs > 10_000:
        return False
    if score is not None and score > 3.5:
        return False
    return True

def scrape_keyword(keyword: str, hunter: dict = None) -> list:
    """Scrape across multiple country combos; deduplicate globally + against Qualified Sheet."""
    global global_seen_ids, global_seen_emails
    push_log(f"🔍 Scraping: '{keyword}'")
    leads = []

    for lang, country in SEARCH_COMBOS:
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
            if not app_id or app_id in global_seen_ids:
                continue
            try:
                details = gp_app(app_id, lang="en", country="us")
            except Exception:
                global_seen_ids.add(app_id)
                continue

            installs = details.get("minInstalls") or 0
            score    = details.get("score")

            if not passes_filter(installs, score, hunter):
                global_seen_ids.add(app_id)
                continue

            email = (
                extract_email(details.get("developerEmail", ""))
                or extract_email(details.get("privacyPolicy", ""))
                or extract_email(details.get("description", ""))
                or extract_email(details.get("recentChanges", ""))
            )
            if not email or email in global_seen_emails:
                global_seen_ids.add(app_id)
                continue

            # ── Qualified Sheet duplicate check ───────────────────────────
            if is_qualified_duplicate(app_id, email):
                global_seen_ids.add(app_id)
                global_seen_emails.add(email)
                push_log(f"  ⏭️  Skip (already in Qualified Sheet): {app_id}")
                continue

            lead = {
                "app_id":      app_id,
                "app_name":    details.get("title", ""),
                "developer":   details.get("developer", ""),
                "email":       email,
                "category":    details.get("genre", ""),
                "installs":    installs,
                "score":       score,
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
            mark_qualified_seen(app_id, email)   # add to in-memory qualified cache

            score_str = f"{score:.1f}★" if score else "new"
            push_log(f"  ✅ {lead['app_name']} | {installs:,} installs | {score_str} | {email}")
            time.sleep(0.25)

        push_log(f"  [{country}] done. Leads so far: {len(leads)}")
        time.sleep(0.5)

    push_log(f"  📦 {len(leads)} new leads from '{keyword}'")
    sheet_log_keyword(keyword, len(leads))
    return leads

# ── Email send (with multi-URL rotation) ─────────────────────────────────────
LIMIT_KEYWORDS = ("quota", "limit", "exceeded", "daily", "rate", "too many")

def send_email(lead: dict, subject: str, body: str) -> bool:
    if not lead.get("email"):
        push_log("  ⚠️  No email address — skip")
        return False

    urls = get_email_urls()
    if not urls:
        push_log("  ❌ EMAIL_SCRIPT_URL not set")
        return False

    # Try each URL once before giving up
    for attempt in range(len(urls)):
        url = get_active_email_url()
        try:
            r = requests.post(url, json={
                "to":      lead["email"],
                "subject": subject,
                "body":    body,
            }, timeout=30)
            result = r.json() if r.text else {}
            msg_lower = str(result.get("msg", "")).lower()

            if result.get("status") == "ok":
                push_log(f"  📧 Sent: {lead['email']} ({lead['app_name']})")
                return True

            # Daily limit hit → rotate and retry with next URL
            if any(kw in msg_lower for kw in LIMIT_KEYWORDS):
                rotate_email_url(f"limit: {result.get('msg','?')}")
                continue

            push_log(f"  ❌ Email failed: {lead['email']}: {result.get('msg','?')}")
            return False

        except Exception as e:
            push_log(f"  ❌ Email error (attempt {attempt+1}): {e}")
            rotate_email_url(str(e))
            continue

    push_log(f"  ❌ All email URLs exhausted for {lead['email']}")
    return False

# ── Master automation ─────────────────────────────────────────────────────────
def run_automation(initial_kw: str, target: int, hunter: dict = None, run_data: dict = None):
    run_data = run_data or {}
    upd(running=True, phase="scraping", keyword=initial_kw,
        keywords_used=[], leads_found=0, emails_sent=0, logs=[], leads=[])
    stop_event.clear()
    mode = "Hunter" if (hunter and hunter.get("active")) else "Normal"
    push_log(f"🚀 Started | kw='{initial_kw}' | target={target} | mode={mode}")

    # Load qualified sheet duplicates before scraping
    load_qualified_sheet_duplicates()

    all_leads = []
    kws_used  = [initial_kw]
    kw_queue  = [initial_kw]

    # ── Phase 1: Scrape ───────────────────────────────────────────────────────
    while len(all_leads) < target and not stop_event.is_set():
        if not kw_queue:
            push_log("🤖 Requesting AI keywords …")
            new_kws = ai_gen_keywords(initial_kw, kws_used)
            if not new_kws:
                push_log("⚠️  No more keywords. Stopping scrape.")
                break
            kw_queue.extend(new_kws)

        kw = kw_queue.pop(0)
        if kw not in kws_used:
            kws_used.append(kw)
        upd(keywords_used=kws_used[:], phase="scraping")

        batch = scrape_keyword(kw, hunter)
        all_leads.extend(batch)
        upd(leads_found=len(all_leads), leads=[l.copy() for l in all_leads])

        for lead in batch:
            sheet_append_lead(lead)
            sheet_append_qualified(lead)

        push_log(f"📊 Total: {len(all_leads)} / {target}")

    if stop_event.is_set():
        push_log("🛑 Stopped during scraping.")
        upd(running=False, phase="stopped")
        return

    push_log(f"✅ Scraping done. {len(all_leads)} leads. Starting emails …")

    # ── Phase 2: AI Email + Send ──────────────────────────────────────────────
    upd(phase="emailing")

    for i, lead in enumerate(all_leads):
        if stop_event.is_set():
            push_log("🛑 Stopped during email phase.")
            break

        base_subject, base_body, tpl_type = pick_template(lead, run_data)
        push_log(f"  🤖 [{tpl_type}] AI writing email for {lead['app_name']} …")
        subject, body = ai_gen_email(lead, base_subject, base_body)

        ok = send_email(lead, subject, body)
        lead["email_sent"] = ok
        with state_lock:
            if ok:
                state["emails_sent"] += 1
            state["leads"] = [l.copy() for l in all_leads]

        if ok:
            sheet_mark_sent(lead["app_id"], lead["email"], lead["app_name"])

        if i < len(all_leads) - 1:
            wait = random.uniform(60, 120)
            push_log(f"  ⏳ Waiting {wait:.0f}s … ({i+1}/{len(all_leads)})")
            for _ in range(int(wait)):
                if stop_event.is_set():
                    break
                time.sleep(1)

    if stop_event.is_set():
        upd(running=False, phase="stopped")
    else:
        push_log("🎉 Automation complete!")
        upd(running=False, phase="done")

# ── Send pending ──────────────────────────────────────────────────────────────
def run_send_pending(leads: list, run_data: dict = None):
    run_data = run_data or {}
    upd(running=True, phase="emailing")
    stop_event.clear()
    push_log(f"📬 Sending pending: {len(leads)} leads")
    sent = 0
    for i, lead in enumerate(leads):
        if stop_event.is_set():
            push_log("🛑 Stopped.")
            break

        base_subject, base_body, tpl_type = pick_template(lead, run_data)
        push_log(f"  🤖 [{tpl_type}] AI writing email for {lead.get('app_name','')} …")
        subject, body = ai_gen_email(lead, base_subject, base_body)

        ok = send_email(lead, subject, body)
        if ok:
            sent += 1
            sheet_mark_sent(lead["app_id"], lead["email"], lead["app_name"])
            with state_lock:
                state["emails_sent"] = state.get("emails_sent", 0) + 1

        if i < len(leads) - 1:
            wait = random.uniform(60, 120)
            push_log(f"  ⏳ Waiting {wait:.0f}s … ({i+1}/{len(leads)})")
            for _ in range(int(wait)):
                if stop_event.is_set():
                    break
                time.sleep(1)

    push_log(f"✅ Pending done. {sent} sent.")
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

    global run_cfg, _email_url_index
    _email_url_index = 0  # reset rotation on new run

    run_cfg = {
        "GROQ_API_KEY":          data.get("groq_key")         or os.environ.get("GROQ_API_KEY", ""),
        "APPS_SCRIPT_WEB_URL":   data.get("sheet_url")        or os.environ.get("APPS_SCRIPT_WEB_URL", ""),
        "EMAIL_SCRIPT_URL":      data.get("email_script_url") or os.environ.get("EMAIL_SCRIPT_URL", ""),
        "SENDER_NAME":           data.get("sender_name")      or os.environ.get("SENDER_NAME", ""),
        "SENDER_COMPANY":        data.get("sender_company")   or os.environ.get("SENDER_COMPANY", ""),
        # Dual-template — env vars as fallback
        "NEW_APP_EMAIL_SUBJECT": os.environ.get("NEW_APP_EMAIL_SUBJECT", ""),
        "NEW_APP_EMAIL_BODY":    os.environ.get("NEW_APP_EMAIL_BODY", ""),
        "OLD_APP_EMAIL_SUBJECT": os.environ.get("OLD_APP_EMAIL_SUBJECT", ""),
        "OLD_APP_EMAIL_BODY":    os.environ.get("OLD_APP_EMAIL_BODY", ""),
    }

    # Front-end supplied dual templates (take priority over env vars)
    run_data = {
        "new_app_email_subject": data.get("new_app_email_subject", ""),
        "new_app_email_body":    data.get("new_app_email_body", ""),
        "old_app_email_subject": data.get("old_app_email_subject", ""),
        "old_app_email_body":    data.get("old_app_email_body", ""),
    }

    target = int(data.get("target") or os.environ.get("TARGET_LEADS", 300))
    hunter = data.get("hunter") or {}
    threading.Thread(target=run_automation,
                     args=(keyword, target, hunter, run_data), daemon=True).start()
    return jsonify({"ok": True, "keyword": keyword})

@application.route("/api/stop", methods=["POST"])
def api_stop():
    stop_event.set()
    push_log("🛑 Stop requested.")
    return jsonify({"ok": True})

@application.route("/api/status")
def api_status():
    with state_lock:
        return jsonify(dict(state))

@application.route("/api/clear", methods=["POST"])
def api_clear():
    """Clear all in-memory state AND duplicate trackers. Sheet is untouched."""
    global global_seen_ids, global_seen_emails
    global qualified_sheet_ids, qualified_sheet_emails
    with state_lock:
        if state["running"]:
            return jsonify({"error": "Cannot clear while running"}), 409
        state.update({
            "running": False, "phase": "idle", "keyword": "",
            "keywords_used": [], "leads_found": 0, "emails_sent": 0,
            "logs": [], "leads": []
        })
    global_seen_ids        = set()
    global_seen_emails     = set()
    qualified_sheet_ids    = set()
    qualified_sheet_emails = set()
    log.info("History + duplicate cache cleared.")
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

    global run_cfg, _email_url_index
    _email_url_index = 0  # reset rotation

    run_cfg = {
        "GROQ_API_KEY":          data.get("groq_key")         or os.environ.get("GROQ_API_KEY", ""),
        "EMAIL_SCRIPT_URL":      data.get("email_script_url") or os.environ.get("EMAIL_SCRIPT_URL", ""),
        "SENDER_NAME":           data.get("sender_name")      or os.environ.get("SENDER_NAME", ""),
        "SENDER_COMPANY":        data.get("sender_company")   or os.environ.get("SENDER_COMPANY", ""),
        "APPS_SCRIPT_WEB_URL":   data.get("sheet_url")        or os.environ.get("APPS_SCRIPT_WEB_URL", ""),
        "NEW_APP_EMAIL_SUBJECT": os.environ.get("NEW_APP_EMAIL_SUBJECT", ""),
        "NEW_APP_EMAIL_BODY":    os.environ.get("NEW_APP_EMAIL_BODY", ""),
        "OLD_APP_EMAIL_SUBJECT": os.environ.get("OLD_APP_EMAIL_SUBJECT", ""),
        "OLD_APP_EMAIL_BODY":    os.environ.get("OLD_APP_EMAIL_BODY", ""),
    }
    run_data = {
        "new_app_email_subject": data.get("new_app_email_subject", ""),
        "new_app_email_body":    data.get("new_app_email_body", ""),
        "old_app_email_subject": data.get("old_app_email_subject", ""),
        "old_app_email_body":    data.get("old_app_email_body", ""),
    }
    threading.Thread(target=run_send_pending,
                     args=(leads, run_data), daemon=True).start()
    return jsonify({"ok": True, "count": len(leads)})

@application.route("/api/spam_test", methods=["POST"])
def api_spam_test():
    data    = request.get_json(silent=True) or {}
    test_to = (data.get("test_email") or "").strip()
    if not test_to:
        return jsonify({"error": "test_email required"}), 400

    global run_cfg
    run_cfg = {
        "GROQ_API_KEY":     data.get("groq_key")         or os.environ.get("GROQ_API_KEY", ""),
        "EMAIL_SCRIPT_URL": data.get("email_script_url") or os.environ.get("EMAIL_SCRIPT_URL", ""),
        "SENDER_NAME":      data.get("sender_name")      or os.environ.get("SENDER_NAME", ""),
        "SENDER_COMPANY":   data.get("sender_company")   or os.environ.get("SENDER_COMPANY", ""),
    }

    raw_score  = data.get("sample_score")
    sample_score = None if (raw_score is None or raw_score == "") else float(raw_score)

    sample = {
        "app_name":  data.get("sample_app_name", "MyApp Pro"),
        "developer": data.get("sample_developer", "John Dev"),
        "category":  "Productivity",
        "installs":  1500,
        "score":     sample_score,
        "email":     test_to,
        "url":       "https://play.google.com/store/apps/details?id=com.example",
    }

    run_data = {
        "new_app_email_subject": data.get("new_app_email_subject", ""),
        "new_app_email_body":    data.get("new_app_email_body", ""),
        "old_app_email_subject": data.get("old_app_email_subject", ""),
        "old_app_email_body":    data.get("old_app_email_body", ""),
    }

    base_subject, base_body, tpl_type = pick_template(sample, run_data)
    subject, body = ai_gen_email(sample, base_subject, base_body)

    url = get_active_email_url()
    if not url:
        return jsonify({"error": "EMAIL_SCRIPT_URL not set"}), 400

    try:
        r = requests.post(url, json={"to": test_to, "subject": subject, "body": body}, timeout=30)
        result = r.json() if r.text else {}
        if result.get("status") == "ok":
            return jsonify({
                "ok": True,
                "msg":           f"Test sent to {test_to}",
                "subject":       subject,
                "body":          body,
                "template_type": tpl_type,
            })
        return jsonify({"error": result.get("msg", "Failed")}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Sheet pending fetch ───────────────────────────────────────────────────────
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

# ── Sheet all leads fetch ─────────────────────────────────────────────────────
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    application.run(host="0.0.0.0", port=port, debug=False)
