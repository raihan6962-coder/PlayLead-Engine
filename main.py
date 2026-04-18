import os, time, random, threading, json, re, logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from google_play_scraper import search, app as gp_app
from groq import Groq
import requests

# ══════════════════════════════════════════════════════════════════════════════
# Flask setup
# ══════════════════════════════════════════════════════════════════════════════
application = Flask(__name__, static_folder=".")
app = application
CORS(application)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# Shared state
# ══════════════════════════════════════════════════════════════════════════════
stop_event = threading.Event()
state_lock = threading.Lock()
state = {
    "running": False, "phase": "idle", "keyword": "",
    "keywords_used": [], "leads_found": 0, "emails_sent": 0,
    "logs": [], "leads": []
}

# ══════════════════════════════════════════════════════════════════════════════
# In-memory caches — three separate sets with different purposes
#
#  qualified_ids / qualified_emails
#      Tracks confirmed qualified leads (email found + filter passed).
#      Used ONLY for deduplication — never send two emails to the same app
#      or the same email address.
#      Populated from "All Leads" sheet tab at every run start.
#      Persists until /api/clear is called.
#
#  scraped_skip_ids
#      Tracks app IDs already permanently processed in a previous run.
#      Status "qualified", "filtered", "no_detail" → add here → skip forever.
#      Status "no_email" → NOT added here → rechecked every run
#      (developer may have added an email since last check).
#      Populated from "Scraped Apps" sheet tab at every run start.
#
# THE KEY DESIGN RULE:
#   An app ID only goes into scraped_skip_ids when its outcome is PERMANENT.
#   A "no_email" result is NOT permanent — it is worth rechecking next run.
#   Therefore "no_email" apps are never added to scraped_skip_ids, and will
#   always be re-fetched and re-evaluated on the next automation run.
# ══════════════════════════════════════════════════════════════════════════════
qualified_ids:    set = set()
qualified_emails: set = set()
scraped_skip_ids: set = set()

# Sheet-loaded mirrors (merged into above at run start, kept separate so
# /api/clear can reset session without affecting what the sheet knows)
sheet_qualified_ids:    set = set()
sheet_qualified_emails: set = set()
sheet_scraped_skip_ids: set = set()

cache_lock        = threading.Lock()
cache_loaded: bool = False

# Email cooldown
email_state_lock       = threading.Lock()
email_url_quotas: dict = {}
global_cooldown_until: float = 0.0
cooldown_retry_thread        = None
cooldown_retry_cancel        = threading.Event()

# Run config
run_cfg = {}

# ══════════════════════════════════════════════════════════════════════════════
# Utilities
# ══════════════════════════════════════════════════════════════════════════════

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

def _is_rate_limit(e: Exception) -> bool:
    msg = str(e).lower()
    return "429" in msg or "too many" in msg or "rate" in msg or "blocked" in msg

# ══════════════════════════════════════════════════════════════════════════════
# Safe Play Store wrappers — retry + exponential backoff on rate limits
# ══════════════════════════════════════════════════════════════════════════════

def safe_search(keyword: str, lang: str, country: str,
                n_hits: int = 500, retries: int = 4) -> list:
    """Search Play Store with auto-retry on rate limits."""
    for attempt in range(retries):
        try:
            return search(keyword, lang=lang, country=country, n_hits=n_hits) or []
        except Exception as e:
            if _is_rate_limit(e):
                wait = (2 ** attempt) * random.uniform(6, 14)
                push_log(f"  Rate-limited [{country}] — waiting {wait:.0f}s "
                         f"(attempt {attempt + 1}/{retries})")
                if stop_event.wait(wait):
                    return []
            else:
                push_log(f"  Search error [{country}]: {e}")
                if stop_event.wait(2):
                    return []
    return []

def safe_app_detail(app_id: str, retries: int = 4) -> dict | None:
    """Fetch app detail page with auto-retry on rate limits."""
    for attempt in range(retries):
        try:
            return gp_app(app_id, lang="en", country="us")
        except Exception as e:
            if _is_rate_limit(e):
                wait = (2 ** attempt) * random.uniform(5, 12)
                push_log(f"  Rate-limited detail {app_id} — waiting {wait:.0f}s")
                if stop_event.wait(wait):
                    return None
            else:
                if attempt == retries - 1:
                    push_log(f"  Detail fetch failed {app_id}: {e}")
                if stop_event.wait(1.5):
                    return None
    return None

# ══════════════════════════════════════════════════════════════════════════════
# Google Sheet via Apps Script
# ══════════════════════════════════════════════════════════════════════════════

def sheet_post(payload: dict, timeout: int = 20):
    url = get_cfg("APPS_SCRIPT_WEB_URL")
    if not url:
        return None
    try:
        r = requests.post(url, json=payload, timeout=timeout)
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
    sheet_post({"action": "append", "tab": "Email Sent", "row": {
        "App ID":   app_id,
        "App Name": app_name,
        "Email":    email,
        "Sent At":  time.strftime("%Y-%m-%d %H:%M:%S"),
    }})

def sheet_log_keyword(keyword: str, count: int):
    sheet_post({"action": "append", "tab": "Keyword Log", "row": {
        "Keyword":     keyword,
        "Leads Found": count,
        "Logged At":   time.strftime("%Y-%m-%d %H:%M:%S"),
    }})

def sheet_record_scraped(app_id: str, app_name: str, status: str,
                          installs: int = 0, score=None,
                          category: str = "", keyword: str = ""):
    """
    Write one row to the 'Scraped Apps' tab after processing an app.

    status values:
      "qualified"  → passed filter + has email → lead created (or already dup)
      "no_email"   → passed filter but no email found → RECHECK next run
      "filtered"   → failed install/score/relevance filter → skip forever
      "no_detail"  → Play Store detail returned None → skip forever
    """
    sheet_post({"action": "append", "tab": "Scraped Apps", "row": {
        "App ID":     app_id,
        "App Name":   app_name,
        "Status":     status,
        "Installs":   installs,
        "Score":      score if score else "",
        "Category":   category,
        "Keyword":    keyword,
        "Checked At": time.strftime("%Y-%m-%d %H:%M:%S"),
    }})

# ══════════════════════════════════════════════════════════════════════════════
# Cache management
# ══════════════════════════════════════════════════════════════════════════════

# Statuses that mean "never re-fetch this app again"
_PERMANENT_SKIP_STATUSES = {"qualified", "filtered", "no_detail"}

def load_caches_from_sheet():
    """
    Called once at the start of every automation run.

    Loads two things from the sheet:
      1. All qualified lead app_ids + emails from 'All Leads' tab.
         → Used by is_duplicate() to avoid emailing the same app twice.
      2. Permanent-skip app_ids from 'Scraped Apps' tab.
         → Used by is_already_scraped() to skip re-fetching processed apps.

    'no_email' apps are intentionally NOT loaded into the skip cache —
    they get rechecked on every run in case the developer added an email.
    """
    global sheet_qualified_ids, sheet_qualified_emails
    global sheet_scraped_skip_ids, cache_loaded

    with cache_lock:
        sheet_qualified_ids    = set()
        sheet_qualified_emails = set()
        sheet_scraped_skip_ids = set()
        cache_loaded           = False

    url = get_cfg("APPS_SCRIPT_WEB_URL")
    if not url:
        push_log("  No sheet URL configured — using in-memory dedup only.")
        with cache_lock:
            cache_loaded = True
        return

    push_log("  Loading sheet caches (qualified leads + scraped DB)...")

    # 1. Qualified leads → dedup cache
    try:
        r     = requests.post(url, json={"action": "get_all_leads"}, timeout=35)
        leads = (r.json() if r.text else {}).get("leads", [])
        with cache_lock:
            for lead in leads:
                aid = (lead.get("App ID") or lead.get("app_id") or "").strip()
                em  = (lead.get("Email")  or lead.get("email")  or "").strip().lower()
                if aid: sheet_qualified_ids.add(aid)
                if em:  sheet_qualified_emails.add(em)
        push_log(f"  Qualified cache: {len(sheet_qualified_ids)} app IDs, "
                 f"{len(sheet_qualified_emails)} emails loaded.")
    except Exception as e:
        push_log(f"  Qualified cache load failed: {e} — continuing without it.")

    # 2. Scraped Apps DB → permanent-skip cache
    try:
        r    = requests.post(url, json={"action": "get_scraped_apps"}, timeout=45)
        rows = (r.json() if r.text else {}).get("apps", [])
        skip_n    = 0
        recheck_n = 0
        with cache_lock:
            for row in rows:
                aid    = (row.get("App ID") or "").strip()
                status = (row.get("Status") or "").strip().lower()
                if not aid:
                    continue
                if status in _PERMANENT_SKIP_STATUSES:
                    sheet_scraped_skip_ids.add(aid)
                    skip_n += 1
                elif status == "no_email":
                    recheck_n += 1   # intentionally NOT in skip cache
        push_log(f"  Scraped DB: {skip_n} permanent-skip IDs loaded, "
                 f"{recheck_n} 'no_email' apps will be rechecked this run.")
    except Exception as e:
        push_log(f"  Scraped DB cache load failed: {e} — continuing without it.")

    with cache_lock:
        cache_loaded = True


def is_duplicate(app_id: str, email: str) -> bool:
    """
    Returns True if this app_id OR email already exists as a qualified lead.
    Checks both session cache (this run) and sheet cache (previous runs).
    """
    el = email.strip().lower()
    if app_id in qualified_ids or el in qualified_emails:
        return True
    with cache_lock:
        return app_id in sheet_qualified_ids or el in sheet_qualified_emails

def is_already_scraped(app_id: str) -> bool:
    """
    Returns True if this app was already permanently processed in a previous run.
    'no_email' apps return False — they are always eligible for rechecking.
    """
    if app_id in scraped_skip_ids:
        return True
    with cache_lock:
        return app_id in sheet_scraped_skip_ids

def register_qualified(app_id: str, email: str):
    """Call when a lead is confirmed qualified. Updates both session + sheet caches."""
    qualified_ids.add(app_id)
    qualified_emails.add(email.strip().lower())
    with cache_lock:
        sheet_qualified_ids.add(app_id)
        sheet_qualified_emails.add(email.strip().lower())

def register_scraped_skip(app_id: str):
    """Call when an app gets a permanent-skip status. Updates both caches."""
    scraped_skip_ids.add(app_id)
    with cache_lock:
        sheet_scraped_skip_ids.add(app_id)

# ══════════════════════════════════════════════════════════════════════════════
# Mode filters
#
# NORMAL MODE — Brand-new apps with zero ratings:
#   • score must be None or 0.0 — Play Store has published no rating yet
#   • ratings must be 0 — absolutely no reviews
#   • installs: 10–10,000
#
#   WHY min installs = 10:
#     Play Store reports minInstalls=10 for apps right after first download.
#     Setting 500 as minimum (previous bug) cut off the vast majority of new
#     apps which all show as "10+" on the store. 10 is the real floor.
#
#   WHY max installs = 10,000:
#     5,000 was too tight. Apps can grow quickly in their first weeks.
#     10,000 is still clearly "small/new" and gives 2× more candidate apps.
#
# HUNTER MODE — Struggling apps with low ratings:
#   • score must be > 0 — must have a real published rating (not unrated)
#   • score ≤ max_score — rating is low (struggling app)
#   • installs: 50–max_inst
#   • ratings_count > 0 — must have at least one review (confirms it's rated)
#
#   WHY we require score > 0 in hunter mode:
#     Unrated apps (score=None or score=0) have no reviews — they belong in
#     normal mode, not hunter mode. Hunter mode targets apps that HAVE been
#     rated but got BAD ratings. Mixing the two produces wrong leads.
#
#   WHY min installs = 50 (was 100):
#     Small struggling apps often have very few installs. 50 catches more.
# ══════════════════════════════════════════════════════════════════════════════

NORMAL_MIN_INSTALLS = 10
NORMAL_MAX_INSTALLS = 10_000
HUNTER_MIN_INSTALLS = 50

def _passes_normal(score_raw, ratings_count: int, installs: int) -> bool:
    """
    Normal mode filter: strictly brand-new apps with NO rating and NO reviews.
    All values are raw from Play Store — nothing is converted before this call.
    """
    # Any published rating means the app is no longer "new" for our purposes
    if score_raw is not None:
        try:
            if float(score_raw) > 0:
                return False
        except (TypeError, ValueError):
            pass

    # Any reviews at all → not truly new
    if int(ratings_count or 0) > 0:
        return False

    # Install range check
    inst = int(installs or 0)
    if inst < NORMAL_MIN_INSTALLS or inst > NORMAL_MAX_INSTALLS:
        return False

    return True

def _passes_hunter(score_raw, ratings_count: int, installs: int,
                   max_score: float, max_inst: int) -> bool:
    """
    Hunter mode filter: struggling apps that have a real but LOW rating.
    Requires an actual positive score — unrated apps are excluded.
    All values are raw from Play Store — nothing is converted before this call.
    """
    if score_raw is None:
        return False
    try:
        s = float(score_raw)
    except (TypeError, ValueError):
        return False

    # Must have a real positive score (unrated apps → normal mode)
    if s <= 0:
        return False

    # Must have at least one review confirming the rating is real
    if int(ratings_count or 0) < 1:
        return False

    # Score must be at or below the threshold (struggling app)
    if s > max_score:
        return False

    # Install range
    inst = int(installs or 0)
    if inst < HUNTER_MIN_INSTALLS or inst > int(max_inst):
        return False

    return True

# ══════════════════════════════════════════════════════════════════════════════
# Keyword helpers
# ══════════════════════════════════════════════════════════════════════════════

STOP_WORDS = {
    "app","apps","application","tool","tools","simple","easy","free","best","top",
    "new","lite","basic","mini","pro","plus","helper","utility","tracker","manager",
    "monitor","the","a","an","for","of","in","on","and","or","with","offline",
    "local","online","mobile","android","google","play","store","indie","micro",
    "community","startup","software","platform","service",
}

def build_keyword_tokens(keyword: str) -> list:
    tokens = re.findall(r"[a-z]+", keyword.lower())
    return [t for t in tokens if t not in STOP_WORDS and len(t) > 2]

def is_keyword_relevant(title: str, genre: str, desc: str, tokens: list) -> bool:
    """
    Relaxed niche relevance check.
    App passes if ANY one keyword token appears in title + genre + description.
    If tokens list is empty (keyword was all stop words), every app passes.
    """
    if not tokens:
        return True
    combined = f"{title} {genre} {desc[:600]}".lower()
    return any(t in combined for t in tokens)

# ══════════════════════════════════════════════════════════════════════════════
# AI keyword generation
# ══════════════════════════════════════════════════════════════════════════════

def ai_gen_keywords(original: str, used: list, hunter: dict = None) -> list:
    key = get_cfg("GROQ_API_KEY")
    if not key:
        push_log("  GROQ_API_KEY not set — using fallback keywords.")
        return _fallback_keywords(original, used)

    client    = Groq(api_key=key)
    used_str  = ", ".join(used[:30]) if used else "none"
    is_hunter = bool(hunter and hunter.get("active"))

    if is_hunter:
        max_inst  = int(hunter.get("max_installs") or 5000)
        max_score = float(hunter.get("max_score")  or 2.5)
        prompt = (
            f"You are a Google Play Store keyword expert specializing in finding "
            f"struggling apps with low ratings.\n"
            f"TARGET NICHE: \"{original}\"\n"
            f"ALREADY USED (do not repeat): {used_str}\n\n"
            f"I am looking for apps that have:\n"
            f"  - A real published rating (must have reviews)\n"
            f"  - A low rating of {max_score} stars or below\n"
            f"  - Under {max_inst:,} installs\n\n"
            f"Generate 20 diverse keyword phrases (2-4 words each) that would surface "
            f"the most struggling and low-rated apps in the '{original}' niche.\n"
            f"Think about:\n"
            f"  - Sub-categories within this niche where many apps compete\n"
            f"  - Category-level broad searches (more results = more chances)\n"
            f"  - Specific use-case keywords in this niche\n"
            f"  - Competitor-adjacent keywords that surface poorly rated alternatives\n"
            f"Stay strictly within the '{original}' niche. No duplicates from used list.\n"
            f"Return ONLY a valid JSON array of strings, nothing else."
        )
    else:
        prompt = (
            f"You are a Google Play Store keyword expert specializing in finding "
            f"brand new apps with zero reviews.\n"
            f"TARGET NICHE: '{original}'\n"
            f"ALREADY USED (do not repeat): {used_str}\n\n"
            f"I am looking for apps that have:\n"
            f"  - Zero reviews and no published rating\n"
            f"  - Under 10,000 installs (brand new)\n\n"
            f"Generate 20 diverse keyword phrases (2-4 words each) that would surface "
            f"the most recently published, brand-new apps in the '{original}' niche.\n"
            f"Think about:\n"
            f"  - Long-tail specific keywords (less competitive = newer apps rank higher)\n"
            f"  - Sub-categories where new developers publish their first apps\n"
            f"  - Niche-specific use cases that attract first-time developers\n"
            f"  - Feature-specific searches where new apps appear first\n"
            f"Stay strictly within the '{original}' niche. No duplicates from used list.\n"
            f"Return ONLY a valid JSON array of strings, nothing else."
        )

    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8, max_tokens=700
        )
        raw = re.sub(r"```[a-z]*", "", resp.choices[0].message.content.strip())
        raw = raw.replace("```", "").strip()
        kws = [str(k).strip() for k in json.loads(raw) if str(k).strip() not in used]
        push_log(f"  AI keywords ({len(kws)}): {kws[:8]}{'...' if len(kws) > 8 else ''}")
        if len(kws) < 5:
            kws.extend(_fallback_keywords(original, used + kws))
        return kws
    except Exception as e:
        push_log(f"  AI keyword error: {e} — using fallback.")
        return _fallback_keywords(original, used)

def _fallback_keywords(original: str, used: list) -> list:
    base = original.lower().strip()
    candidates = []
    suffixes = [
        "lite","simple","basic","mini","micro","offline","local","tracker","logger",
        "monitor","ledger","tool","helper","assistant","companion","free","dashboard",
        "manager","diary","notes","record","log","2024","2025","app","mobile",
        "android","budget","personal","daily","planner","calculator",
    ]
    prefixes = [
        "simple","easy","offline","local","micro","indie","basic","personal","free",
        "quick","smart","tiny","pocket","handy","my","daily","best","new","top",
        "fast","clean","lightweight","minimal",
    ]
    for s in suffixes:
        c = f"{base} {s}"
        if c not in used:
            candidates.append(c)
    for p in prefixes:
        c = f"{p} {base}"
        if c not in used:
            candidates.append(c)
    words = base.split()
    if len(words) > 1:
        for i in range(len(words)):
            for j in range(i + 1, len(words)):
                c = f"{words[i]} {words[j]}"
                if c not in used and len(c) > 4:
                    candidates.append(c)
    return candidates[:20]

# ══════════════════════════════════════════════════════════════════════════════
# Email templates
# ══════════════════════════════════════════════════════════════════════════════

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

DEFAULT_EMAIL_SUBJECT = DEFAULT_NEW_APP_SUBJECT
DEFAULT_EMAIL_BODY    = DEFAULT_NEW_APP_BODY

def format_score(score) -> str:
    if score is None or score == "" or score == 0:
        return ""
    try:
        val = float(score)
        return f"{val:.1f}" if val > 0 else ""
    except:
        return ""

def select_template(lead: dict, base_subject: str = "", base_body: str = "") -> tuple:
    """Choose old-app or new-app template based on whether the lead has a rating."""
    has_rating = bool(format_score(lead.get("score")))
    if has_rating:
        return (
            get_cfg("OLD_APP_EMAIL_SUBJECT") or base_subject or DEFAULT_OLD_APP_SUBJECT,
            get_cfg("OLD_APP_EMAIL_BODY")    or base_body    or DEFAULT_OLD_APP_BODY,
        )
    return (
        get_cfg("NEW_APP_EMAIL_SUBJECT") or base_subject or DEFAULT_NEW_APP_SUBJECT,
        get_cfg("NEW_APP_EMAIL_BODY")    or base_body    or DEFAULT_NEW_APP_BODY,
    )

def fill_template(tpl: str, lead: dict) -> str:
    """Simple variable substitution without AI."""
    installs_raw = lead.get("installs")
    try:
        installs_str = f"{int(installs_raw):,}" if installs_raw else "growing app"
    except:
        installs_str = str(installs_raw)
    filled = (tpl
        .replace("{{app_name}}",       lead.get("app_name", ""))
        .replace("{{developer}}",      lead.get("developer", "") or "")
        .replace("{{category}}",       lead.get("category", "") or "app")
        .replace("{{installs}}",       installs_str)
        .replace("{{score}}",          format_score(lead.get("score")))
        .replace("{{url}}",            lead.get("url", ""))
        .replace("{{sender_name}}",    get_cfg("SENDER_NAME", "Your Name"))
        .replace("{{sender_company}}", get_cfg("SENDER_COMPANY", "Your Company"))
    )
    return re.sub(r"\{\{[a-zA-Z_]+\}\}", "", filled)

def ai_gen_email(lead: dict, base_subject: str, base_body: str) -> tuple[str, str]:
    """
    Generate a personalized email using AI, keeping the template structure intact.
    Falls back to plain template substitution if AI is unavailable or fails.
    Automatically selects the correct template (new-app vs old-app) based on
    whether the lead has a published rating.
    """
    key = get_cfg("GROQ_API_KEY")
    tpl_subject, tpl_body = select_template(lead, base_subject, base_body)

    if not key:
        return fill_template(tpl_subject, lead), fill_template(tpl_body, lead)

    client        = Groq(api_key=key)
    score_fmt     = format_score(lead.get("score"))
    prefilled_sub = fill_template(tpl_subject, lead)
    prefilled_bod = fill_template(tpl_body, lead)
    ttype         = "OLD APP (has rating)" if score_fmt else "NEW APP (no rating)"
    score_info    = f"{score_fmt} stars" if score_fmt else "no ratings yet (brand new)"
    install_info  = f"{lead['installs']:,} installs" if lead.get("installs") else "just launched"

    prompt = f"""You are a cold email personalizer. Your only job is to fill in the base template with the real app details — keeping the structure and wording almost identical.

TEMPLATE TYPE: {ttype}

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
- Name: {get_cfg("SENDER_NAME", "Your Name")}
- Company: {get_cfg("SENDER_COMPANY", "Your Company")}

STRICT RULES:
1. Copy the template EXACTLY — same structure, same sentences, same flow
2. Only replace {{{{variable}}}} placeholders with real values from APP DETAILS
3. You may change at most 2-3 words in the entire body to naturally fit this specific app
4. Do NOT rewrite, add, or remove any sentences
5. Do NOT change the greeting format, CTA, or sign-off
6. NEVER leave any {{{{variable}}}} placeholder in the output — replace them all
7. Preserve every line break and blank line from the template exactly
8. Return ONLY valid JSON: {{"subject":"...","body":"..."}} — no markdown, no explanation"""

    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3, max_tokens=600
        )
        raw = re.sub(r"```[a-z]*", "", resp.choices[0].message.content.strip())
        raw = raw.replace("```", "").strip()
        data    = json.loads(raw)
        subject = re.sub(r"\{\{[a-zA-Z_]+\}\}", "", data.get("subject") or prefilled_sub)
        body    = re.sub(r"\{\{[a-zA-Z_]+\}\}", "",
                         (data.get("body") or prefilled_bod).replace("\\n", "\n"))
        return subject, body
    except Exception as e:
        push_log(f"  AI email error (using template fallback): {e}")
        return prefilled_sub, prefilled_bod

# ══════════════════════════════════════════════════════════════════════════════
# Search combos
#
# NORMAL MODE — 12 countries to maximise discovery of newly published apps.
#   New apps appear in their developer's home country first. Casting wide
#   across diverse markets finds more apps than sticking to US/GB only.
#
# HUNTER MODE — 16 countries covering all major English-language app markets.
#   More countries → more total rated apps in the pool → more struggling ones.
# ══════════════════════════════════════════════════════════════════════════════

NORMAL_SEARCH_COMBOS = [
    ("en", "us"), ("en", "gb"), ("en", "in"), ("en", "au"), ("en", "ca"),
    ("en", "ng"), ("en", "gh"), ("en", "ke"), ("en", "ph"), ("en", "my"),
    ("en", "pk"), ("en", "bd"),
]

HUNTER_SEARCH_COMBOS = [
    ("en", "us"), ("en", "gb"), ("en", "in"), ("en", "au"), ("en", "ca"),
    ("en", "nz"), ("en", "sg"), ("en", "za"), ("en", "ng"), ("en", "gh"),
    ("en", "ke"), ("en", "ph"), ("en", "my"), ("en", "pk"), ("en", "bd"),
    ("en", "tz"),
]

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Parallel workers for detail fetching
DETAIL_WORKERS = 6

def extract_email(text: str) -> str:
    if not text:
        return ""
    m = EMAIL_RE.search(str(text))
    return m.group(0) if m else ""

# ══════════════════════════════════════════════════════════════════════════════
# Core scraper
#
# ARCHITECTURE — mirrors the original main.py scraping flow exactly, but with:
#
#   1. Persistent Scraped Apps DB (sheet tab):
#      Every app whose detail is fetched gets recorded with a status.
#      On the next run, permanently-processed apps are skipped immediately,
#      saving time and rate-limit budget for fresh apps.
#
#   2. Correct filter separation:
#      Normal mode:  no score, no reviews, small installs.
#      Hunter mode:  HAS a score > 0, score is low, small installs.
#      These two modes do NOT overlap — unrated apps never pass hunter,
#      rated apps never pass normal.
#
#   3. "no_email" apps are rechecked every run:
#      Unlike filtered/qualified/no_detail apps which are skipped forever,
#      apps that passed the filter but had no email are re-fetched each run.
#      A developer may have added contact info since the last check.
#
#   4. Parallel detail fetching (ThreadPoolExecutor):
#      Original code fetched details one by one, sequentially.
#      We fetch DETAIL_WORKERS at a time, dramatically reducing runtime.
#      Small per-request jitter (0.1–0.8s) prevents bursting the rate limit.
#
#   5. Dedup is purely based on qualified leads:
#      is_duplicate() checks qualified_ids + qualified_emails only.
#      It does NOT check the scraped DB — those are separate concerns.
#
# STEP-BY-STEP FLOW FOR EACH KEYWORD:
#   Step 1: Search all country combos → collect unique app IDs into pool.
#   Step 2: Remove IDs already permanently processed (is_already_scraped).
#           "no_email" IDs pass through — they are eligible for recheck.
#   Step 3: Fetch details in parallel for remaining IDs.
#   Step 4: Apply mode filter (_passes_normal or _passes_hunter) using
#           RAW values exactly as returned by Play Store.
#   Step 5: Check keyword relevance (relaxed — any one token match).
#   Step 6: Extract email from all available fields.
#           If no email → record "no_email", do NOT add to skip cache.
#   Step 7: Check is_duplicate (qualified leads only).
#           If dup → record "qualified", add to skip cache, skip lead creation.
#   Step 8: All checks passed → build lead, register_qualified, record "qualified".
#   Step 9: After thread pool, batch-write all scrape records to sheet.
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_one_detail(app_id: str) -> tuple[str, dict | None]:
    """Fetch detail for one app with small random stagger to avoid burst rate-limits."""
    time.sleep(random.uniform(0.1, 0.8))
    return app_id, safe_app_detail(app_id)

def scrape_keyword(keyword: str, hunter: dict = None) -> list:
    """
    Full scrape pipeline for one keyword.
    Returns a list of new qualified lead dicts.
    """
    push_log(f"🔍 Scraping: '{keyword}'")

    is_hunter      = bool(hunter and hunter.get("active"))
    max_inst       = int(hunter.get("max_installs") or 5000)   if is_hunter else NORMAL_MAX_INSTALLS
    max_score      = float(hunter.get("max_score")  or 2.5)    if is_hunter else 0.0
    combos         = HUNTER_SEARCH_COMBOS if is_hunter else NORMAL_SEARCH_COMBOS
    keyword_tokens = build_keyword_tokens(keyword)

    # ── STEP 1: Search all country combos — collect unique app IDs ────────────
    push_log(f"  Searching {len(combos)} countries...")
    seen_in_search: set = set()
    app_id_pool:   list = []

    combos_shuffled = list(combos)
    random.shuffle(combos_shuffled)

    for lang, country in combos_shuffled:
        if stop_event.is_set():
            break

        results = safe_search(keyword, lang=lang, country=country, n_hits=500)
        new_ids = 0
        for item in results:
            aid = (item.get("appId") or "").strip()
            if aid and aid not in seen_in_search:
                seen_in_search.add(aid)
                app_id_pool.append(aid)
                new_ids += 1

        push_log(f"  [{country}] {len(results)} results, "
                 f"{new_ids} new IDs (pool: {len(app_id_pool)})")

        # Brief pause between countries — polite to Play Store
        if stop_event.wait(random.uniform(0.5, 1.5)):
            break

    push_log(f"  Pool: {len(app_id_pool)} unique app IDs for '{keyword}'")

    if not app_id_pool:
        push_log(f"  No search results for '{keyword}'")
        sheet_log_keyword(keyword, 0)
        return []

    # ── STEP 2: Remove permanently-processed IDs ──────────────────────────────
    # "no_email" apps pass through here — they are eligible for recheck.
    pre_count = len(app_id_pool)
    to_fetch  = [aid for aid in app_id_pool if not is_already_scraped(aid)]
    skipped   = pre_count - len(to_fetch)
    if skipped:
        push_log(f"  Skipped {skipped} already-processed IDs (DB cache)")
    push_log(f"  Fetching details: {len(to_fetch)} apps "
             f"({DETAIL_WORKERS} parallel workers)...")

    # ── Batch scrape records — collected during processing, written after ──────
    # Writing to sheet inside the thread pool would cause concurrent HTTP calls
    # to Apps Script which it does not handle well. Collect here, write later.
    scrape_records      = []
    scrape_records_lock = threading.Lock()

    def _record(aid, aname, status, installs=0, score=None, category=""):
        """
        Thread-safe collection of one scrape outcome.
        Also immediately updates the in-memory skip cache so other threads
        running in parallel can see this app as processed right away.
        """
        with scrape_records_lock:
            scrape_records.append({
                "app_id": aid, "app_name": aname, "status": status,
                "installs": installs, "score": score,
                "category": category, "keyword": keyword,
            })
        if status in _PERMANENT_SKIP_STATUSES:
            register_scraped_skip(aid)

    # ── STEPS 3–8: Parallel detail fetch + filter + dedup + lead build ────────
    leads      = []
    leads_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
        futures = {executor.submit(_fetch_one_detail, aid): aid for aid in to_fetch}

        for future in as_completed(futures):
            if stop_event.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
                break

            app_id, details = future.result()

            # STEP 3 result: detail fetch failed
            if details is None:
                _record(app_id, app_id, "no_detail")
                continue

            # ── Raw values exactly as Play Store returned them ────────────────
            # IMPORTANT: Do NOT convert, zero, or modify score_raw before
            # passing to filter functions. Both filters use the raw value.
            installs      = int(details.get("minInstalls") or 0)
            ratings_count = int(details.get("ratings")     or 0)
            score_raw     = details.get("score")    # float | None | 0.0 — untouched
            app_name      = details.get("title", app_id)
            app_genre     = details.get("genre", "")
            app_desc      = details.get("description", "")

            # STEP 4: Mode filter
            if is_hunter:
                passes_filter = _passes_hunter(
                    score_raw, ratings_count, installs, max_score, max_inst)
            else:
                passes_filter = _passes_normal(score_raw, ratings_count, installs)

            if not passes_filter:
                _record(app_id, app_name, "filtered", installs, score_raw, app_genre)
                continue

            # STEP 5: Keyword relevance (relaxed — any one token match)
            if keyword_tokens and not is_keyword_relevant(
                    app_name, app_genre, app_desc, keyword_tokens):
                _record(app_id, app_name, "filtered", installs, score_raw, app_genre)
                continue

            # STEP 6: Email extraction — try all available fields
            email = (
                extract_email(details.get("developerEmail", ""))
                or extract_email(details.get("privacyPolicy", ""))
                or extract_email(details.get("description", ""))
                or extract_email(details.get("recentChanges", ""))
                or extract_email(details.get("developerWebsite", ""))
            )

            if not email:
                # Passed filter but no email found.
                # Record as "no_email" — intentionally NOT a permanent skip.
                # This app will be re-fetched next run in case an email appears.
                _record(app_id, app_name, "no_email", installs, score_raw, app_genre)
                score_str = f"{score_raw:.1f}★" if score_raw else "no-rating"
                push_log(f"  ⚠ no_email (will recheck): {app_name} "
                         f"| {installs:,} inst | {score_str}")
                continue

            # STEP 7: Dedup — check qualified leads only (not scraped DB)
            if is_duplicate(app_id, email):
                push_log(f"  ⟳ dup (already qualified): {app_name}")
                # Record as "qualified" so we don't re-process in future runs
                _record(app_id, app_name, "qualified", installs, score_raw, app_genre)
                continue

            # STEP 8: All checks passed — this is a new qualified lead
            register_qualified(app_id, email)
            _record(app_id, app_name, "qualified", installs, score_raw, app_genre)

            lead = {
                "app_id":        app_id,
                "app_name":      app_name,
                "developer":     (details.get("developer", "") or "").strip(),
                "email":         email,
                "category":      app_genre,
                "installs":      installs,
                "score":         score_raw,   # raw Play Store value, no conversion
                "ratings_count": ratings_count,
                "description":   (app_desc or "")[:300],
                "url":           f"https://play.google.com/store/apps/details?id={app_id}",
                "icon":          details.get("icon", ""),
                "keyword":       keyword,
                "scraped_at":    time.strftime("%Y-%m-%d %H:%M:%S"),
                "email_sent":    False,
            }

            with leads_lock:
                leads.append(lead)

            mode_tag  = "HUNTER" if is_hunter else "NORMAL"
            score_str = f"{score_raw:.1f}★" if score_raw else "no-rating"
            push_log(f"  ✅ [{mode_tag}] {app_name} "
                     f"| {installs:,} inst | {score_str} "
                     f"| {ratings_count} rev | {email}")

    # ── STEP 9: Batch-write all scrape records to sheet ───────────────────────
    # Done sequentially after the thread pool so we never have concurrent
    # writes to Apps Script. Each write is a single HTTP POST.
    if scrape_records:
        push_log(f"  Writing {len(scrape_records)} records to Scraped Apps DB...")
        for rec in scrape_records:
            if stop_event.is_set():
                break
            sheet_record_scraped(
                app_id   = rec["app_id"],
                app_name = rec["app_name"],
                status   = rec["status"],
                installs = rec["installs"],
                score    = rec["score"],
                category = rec["category"],
                keyword  = rec["keyword"],
            )

    # Summary log
    q = sum(1 for r in scrape_records if r["status"] == "qualified")
    n = sum(1 for r in scrape_records if r["status"] == "no_email")
    f = sum(1 for r in scrape_records if r["status"] == "filtered")
    d = sum(1 for r in scrape_records if r["status"] == "no_detail")
    push_log(f"  📦 '{keyword}' → {len(leads)} new leads | "
             f"{q} qualified | {n} no_email | {f} filtered | {d} no_detail")
    sheet_log_keyword(keyword, len(leads))
    return leads

# ══════════════════════════════════════════════════════════════════════════════
# Email send helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_email_urls() -> list:
    raw = get_cfg("EMAIL_SCRIPT_URL").replace(",", "\n").split("\n")
    return [u.strip() for u in raw if u.strip()]

def reset_email_quotas(urls: list):
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

def reset_exhausted_urls(urls: list):
    with email_state_lock:
        for u in urls:
            if u in email_url_quotas:
                email_url_quotas[u]["exhausted"] = False

def send_email(lead: dict, subject: str, body: str) -> tuple[str, str]:
    """
    Send one email via the configured Apps Script email URL(s).
    Supports multiple comma-separated URLs with per-URL quota tracking.

    Returns:
      ("ok",    "")              — sent successfully
      ("quota", "All exhausted") — all URLs hit Google's daily limit
      ("error", reason)          — permanent failure
    """
    urls = get_email_urls()
    if not urls or not lead.get("email"):
        push_log("  EMAIL_SCRIPT_URL not configured or lead has no email.")
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

            # HTML response means the script was deployed incorrectly
            if "html" in r.headers.get("Content-Type", "").lower():
                push_log("  Email URL deployed incorrectly "
                         "(Deploy → Execute as: Me, Access: Anyone).")
                mark_url_failed(url)
                continue

            result  = r.json() if r.text else {}
            err_msg = result.get("msg", "?")

            if result.get("status") == "ok":
                push_log(f"  📧 Sent: {lead['email']} ({lead['app_name']})")
                return "ok", ""
            elif "Service invoked too many times" in err_msg:
                push_log("  Quota limit hit on this URL. Trying next...")
                mark_url_exhausted(url)
                quota_hits += 1
            elif "permission" in err_msg.lower() or "authorize" in err_msg.lower():
                push_log("  URL needs re-authorization.")
                mark_url_failed(url)
            else:
                push_log(f"  Email failed: {err_msg}. Trying next URL...")

        except Exception as e:
            push_log(f"  Email request error: {e}")

    if quota_hits >= len(urls):
        push_log("  All email script URLs hit Google's daily quota limit.")
        return "quota", "All URLs exhausted"

    push_log("  All email script URLs failed for this lead.")
    return "error", "All URLs failed"

# ══════════════════════════════════════════════════════════════════════════════
# Cooldown / retry scheduler
# ══════════════════════════════════════════════════════════════════════════════

COOLDOWN_SECONDS = 3600  # 1 hour

def _is_automation_running() -> bool:
    with state_lock:
        return state.get("running", False)

def _cancel_cooldown_retry():
    global cooldown_retry_thread
    cooldown_retry_cancel.set()
    cooldown_retry_thread = None

def _schedule_email_retry(leads_to_send: list, base_subject: str, base_body: str):
    """
    Wait COOLDOWN_SECONDS then retry sending emails.
    If still quota-exhausted, re-enter cooldown recursively.
    Can be cancelled cleanly via _cancel_cooldown_retry().
    """
    global global_cooldown_until
    cooldown_retry_cancel.clear()
    with email_state_lock:
        global_cooldown_until = time.time() + COOLDOWN_SECONDS
    push_log("  Email quota cooldown started. Retrying in 1 hour.")

    for _ in range(COOLDOWN_SECONDS):
        if cooldown_retry_cancel.is_set():
            push_log("  Cooldown retry cancelled.")
            with email_state_lock:
                global_cooldown_until = 0.0
            return
        time.sleep(1)

    with email_state_lock:
        global_cooldown_until = 0.0

    if _is_automation_running():
        push_log("  Cooldown over — automation already running, skipping auto-resume.")
        return

    push_log("  Cooldown over. Resetting quotas and retrying emails...")
    reset_exhausted_urls(get_email_urls())
    sent = 0

    for i, lead in enumerate(leads_to_send):
        if stop_event.is_set() or cooldown_retry_cancel.is_set():
            break
        subject, body = ai_gen_email(lead, base_subject, base_body)
        status, _     = send_email(lead, subject, body)
        if status == "ok":
            sent += 1
            sheet_mark_sent(lead["app_id"], lead["email"], lead["app_name"])
            with state_lock:
                state["emails_sent"] = state.get("emails_sent", 0) + 1
        elif status == "quota":
            push_log("  Still quota-exhausted. Re-entering cooldown...")
            global cooldown_retry_thread
            cooldown_retry_thread = threading.Thread(
                target=_schedule_email_retry,
                args=(leads_to_send[i:], base_subject, base_body),
                daemon=True
            )
            cooldown_retry_thread.start()
            return
        if i < len(leads_to_send) - 1:
            if stop_event.wait(random.uniform(30, 60)):
                break

    push_log(f"  Cooldown retry done. {sent} additional emails sent.")

# ══════════════════════════════════════════════════════════════════════════════
# Email sending loop
# ══════════════════════════════════════════════════════════════════════════════

def email_loop(leads: list, base_subject: str, base_body: str):
    """
    Send emails to all pending leads.
    Handles quota exhaustion by scheduling a cooldown retry thread.
    Waits 30–60 seconds between emails to avoid spam detection.
    """
    global cooldown_retry_thread

    pending    = [l for l in leads if not l.get("email_sent") and l.get("email")]
    total      = len(pending)
    sent_count = 0

    push_log(f"📬 Email loop: {total} leads pending.")

    i = 0
    while i < len(pending):
        if stop_event.is_set():
            push_log("🛑 Stopped during email phase.")
            return

        lead  = pending[i]
        ttype = "OLD APP" if format_score(lead.get("score")) else "NEW APP"
        push_log(f"  [{i+1}/{total}] 🤖 AI writing email for "
                 f"{lead['app_name']} [{ttype}]...")
        subject, body = ai_gen_email(lead, base_subject, base_body)
        status, _     = send_email(lead, subject, body)

        if status == "ok":
            lead["email_sent"] = True
            sent_count += 1
            with state_lock:
                state["emails_sent"] = state.get("emails_sent", 0) + 1
                state["leads"]       = [l.copy() for l in leads]
            sheet_mark_sent(lead["app_id"], lead["email"], lead["app_name"])
            push_log(f"  Sent {sent_count}/{total}.")
            i += 1

        elif status == "quota":
            remaining = pending[i:]
            push_log(f"  Quota exhausted. {len(remaining)} leads queued for "
                     f"1-hour retry.")
            cooldown_retry_thread = threading.Thread(
                target=_schedule_email_retry,
                args=(remaining, base_subject, base_body),
                daemon=True
            )
            cooldown_retry_thread.start()
            return

        else:
            push_log(f"  Send failed for {lead['app_name']}. Moving on...")
            i += 1

        if stop_event.is_set():
            return

        if i < len(pending):
            wait = random.uniform(30, 60)
            push_log(f"  ⏳ Waiting {wait:.0f}s before next email "
                     f"({i}/{total} done)...")
            if stop_event.wait(wait):
                return

    push_log(f"✅ Email loop done. Sent: {sent_count}/{total}")

# ══════════════════════════════════════════════════════════════════════════════
# Master automation
# ══════════════════════════════════════════════════════════════════════════════

def run_automation(initial_kw: str, target: int, hunter: dict = None):
    global cooldown_retry_thread

    # Cancel any running cooldown retry from a previous session
    if cooldown_retry_thread and cooldown_retry_thread.is_alive():
        _cancel_cooldown_retry()
        push_log("  Cancelled pending email retry (new automation starting).")

    upd(running=True, phase="scraping", keyword=initial_kw,
        keywords_used=[], leads_found=0, emails_sent=0, logs=[], leads=[])
    stop_event.clear()

    mode = "Hunter" if (hunter and hunter.get("active")) else "Normal"
    push_log(f"🚀 Started | kw='{initial_kw}' | target={target} | mode={mode}")

    # Load sheet caches: qualified leads dedup + scraped apps permanent-skip
    load_caches_from_sheet()

    base_subject = get_cfg("EMAIL_SUBJECT") or ""
    base_body    = get_cfg("EMAIL_BODY")    or ""

    reset_email_quotas(get_email_urls())

    all_leads = []
    kws_used  = [initial_kw]
    kw_queue  = [initial_kw]

    # ── Phase 1: Scrape until target reached ─────────────────────────────────
    while len(all_leads) < target and not stop_event.is_set():

        if not kw_queue:
            push_log("  🤖 Requesting more AI keywords...")
            new_kws = ai_gen_keywords(initial_kw, kws_used, hunter)
            if not new_kws:
                push_log("  ⚠ No more keywords available. Stopping scrape.")
                break
            kw_queue.extend(new_kws)

        kw = kw_queue.pop(0)
        if kw not in kws_used:
            kws_used.append(kw)
        upd(keywords_used=kws_used[:], phase="scraping")

        batch = scrape_keyword(kw, hunter)
        all_leads.extend(batch)
        upd(leads_found=len(all_leads), leads=[l.copy() for l in all_leads])

        # Write each new lead to sheet immediately
        for lead in batch:
            sheet_append_lead(lead)
            sheet_append_qualified(lead)

        push_log(f"📊 Progress: {len(all_leads)} / {target} leads")

        # Short pause between keywords
        if not stop_event.is_set() and len(all_leads) < target:
            wait = random.uniform(4, 10)
            push_log(f"  Pausing {wait:.0f}s before next keyword...")
            stop_event.wait(wait)

    if stop_event.is_set():
        push_log("🛑 Stopped during scraping.")
        upd(running=False, phase="stopped")
        return

    push_log(f"✅ Scraping done. {len(all_leads)} leads collected. "
             f"Starting emails...")

    # ── Phase 2: Email ────────────────────────────────────────────────────────
    upd(phase="emailing")
    email_loop(all_leads, base_subject, base_body)

    if stop_event.is_set():
        upd(running=False, phase="stopped")
    else:
        push_log("🎉 Automation complete!")
        upd(running=False, phase="done")

# ══════════════════════════════════════════════════════════════════════════════
# Send pending
# ══════════════════════════════════════════════════════════════════════════════

def run_send_pending(leads: list):
    global cooldown_retry_thread

    if cooldown_retry_thread and cooldown_retry_thread.is_alive():
        _cancel_cooldown_retry()
        push_log("  Cancelled pending email retry (manual send starting).")

    upd(running=True, phase="emailing")
    stop_event.clear()
    push_log(f"📬 Sending pending: {len(leads)} leads")

    base_subject = get_cfg("EMAIL_SUBJECT") or ""
    base_body    = get_cfg("EMAIL_BODY")    or ""

    reset_email_quotas(get_email_urls())
    email_loop(leads, base_subject, base_body)
    push_log("✅ Pending send complete.")
    upd(running=False, phase="done")

# ══════════════════════════════════════════════════════════════════════════════
# Run config builder
# ══════════════════════════════════════════════════════════════════════════════

def build_run_cfg(data: dict) -> dict:
    return {
        "GROQ_API_KEY":          data.get("groq_key")              or os.environ.get("GROQ_API_KEY", ""),
        "APPS_SCRIPT_WEB_URL":   data.get("sheet_url")             or os.environ.get("APPS_SCRIPT_WEB_URL", ""),
        "EMAIL_SCRIPT_URL":      data.get("email_script_url")      or os.environ.get("EMAIL_SCRIPT_URL", ""),
        "SENDER_NAME":           data.get("sender_name")           or os.environ.get("SENDER_NAME", ""),
        "SENDER_COMPANY":        data.get("sender_company")        or os.environ.get("SENDER_COMPANY", ""),
        "EMAIL_SUBJECT":         data.get("email_subject")         or os.environ.get("EMAIL_SUBJECT", ""),
        "EMAIL_BODY":            data.get("email_body")            or os.environ.get("EMAIL_BODY", ""),
        "NEW_APP_EMAIL_SUBJECT": data.get("new_app_email_subject") or os.environ.get("NEW_APP_EMAIL_SUBJECT", ""),
        "NEW_APP_EMAIL_BODY":    data.get("new_app_email_body")    or os.environ.get("NEW_APP_EMAIL_BODY", ""),
        "OLD_APP_EMAIL_SUBJECT": data.get("old_app_email_subject") or os.environ.get("OLD_APP_EMAIL_SUBJECT", ""),
        "OLD_APP_EMAIL_BODY":    data.get("old_app_email_body")    or os.environ.get("OLD_APP_EMAIL_BODY", ""),
    }

# ══════════════════════════════════════════════════════════════════════════════
# Flask routes
# ══════════════════════════════════════════════════════════════════════════════

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
    run_cfg = build_run_cfg(data)
    target  = int(data.get("target") or os.environ.get("TARGET_LEADS", 300))
    hunter  = data.get("hunter") or {}
    threading.Thread(
        target=run_automation, args=(keyword, target, hunter), daemon=True
    ).start()
    return jsonify({"ok": True, "keyword": keyword})

@application.route("/api/stop", methods=["POST"])
def api_stop():
    stop_event.set()
    _cancel_cooldown_retry()
    upd(running=False, phase="stopped")
    push_log("🛑 Stop requested.")
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
    """Clear all in-memory session state and duplicate trackers. Sheet is untouched."""
    global qualified_ids, qualified_emails
    with state_lock:
        if state["running"]:
            return jsonify({"error": "Cannot clear while running"}), 409
        state.update({
            "running": False, "phase": "idle", "keyword": "",
            "keywords_used": [], "leads_found": 0, "emails_sent": 0,
            "logs": [], "leads": []
        })
    qualified_ids    = set()
    qualified_emails = set()
    log.info("Session history cleared.")
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
    run_cfg = build_run_cfg(data)
    fresh = [l for l in leads if not l.get("email_sent") and l.get("email")]
    if not fresh:
        return jsonify({"error": "No unsent leads with email in provided list"}), 400
    threading.Thread(target=run_send_pending, args=(fresh,), daemon=True).start()
    return jsonify({"ok": True, "count": len(fresh)})

@application.route("/api/spam_test", methods=["POST"])
def api_spam_test():
    data    = request.get_json(silent=True) or {}
    test_to = (data.get("test_email") or "").strip()
    if not test_to:
        return jsonify({"error": "test_email required"}), 400
    global run_cfg
    run_cfg      = build_run_cfg(data)
    raw_score    = data.get("sample_score")
    sample_score = float(raw_score) if raw_score else None
    sample = {
        "app_name":  data.get("sample_app_name", "MyApp Pro"),
        "developer": data.get("sample_developer", "John Dev"),
        "category":  "Productivity",
        "installs":  1500,
        "score":     sample_score,
        "email":     test_to,
        "url":       "https://play.google.com/store/apps/details?id=com.example",
    }
    urls = get_email_urls()
    url  = urls[0] if urls else None
    if not url:
        return jsonify({"error": "EMAIL_SCRIPT_URL not set"}), 400
    ttype = "OLD APP" if format_score(sample_score) else "NEW APP"
    push_log(f"  Spam test: {ttype} template (score={sample_score})")
    subject, body = ai_gen_email(
        sample, get_cfg("EMAIL_SUBJECT") or "", get_cfg("EMAIL_BODY") or "")
    try:
        r      = requests.post(url, json={"to": test_to, "subject": subject,
                                          "body": body}, timeout=30)
        result = r.json() if r.text else {}
        if result.get("status") == "ok":
            return jsonify({
                "ok": True, "msg": f"Test sent to {test_to} [{ttype}]",
                "template_type": ttype, "subject": subject, "body": body,
            })
        return jsonify({"error": result.get("msg", "Failed")}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@application.route("/api/sheet_pending", methods=["POST"])
def api_sheet_pending():
    data      = request.get_json(silent=True) or {}
    sheet_url = data.get("sheet_url") or os.environ.get("APPS_SCRIPT_WEB_URL", "")
    if not sheet_url:
        return jsonify({"error": "sheet_url not set"}), 400
    try:
        r      = requests.post(sheet_url, json={"action": "get_pending"}, timeout=25)
        result = r.json() if r.text else {}
        leads  = result.get("leads", [])
        return jsonify({"ok": True, "count": len(leads), "leads": leads})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@application.route("/api/sheet_all", methods=["POST"])
def api_sheet_all():
    data      = request.get_json(silent=True) or {}
    sheet_url = data.get("sheet_url") or os.environ.get("APPS_SCRIPT_WEB_URL", "")
    if not sheet_url:
        return jsonify({"error": "sheet_url not set"}), 400
    try:
        r      = requests.post(sheet_url, json={"action": "get_all_leads"}, timeout=25)
        result = r.json() if r.text else {}
        return jsonify({"ok": True, "leads": result.get("leads", [])})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@application.route("/api/send_single", methods=["POST"])
def api_send_single():
    with state_lock:
        if state["running"]:
            return jsonify({"error": "Automation is running"}), 409
    data = request.get_json(silent=True) or {}
    lead = data.get("lead")
    if not lead or not lead.get("app_id") or not lead.get("email"):
        return jsonify({"error": "Valid lead with app_id and email required"}), 400
    global run_cfg
    run_cfg = build_run_cfg(data)

    def _send_single(lead_data: dict):
        upd(running=True, phase="emailing")
        stop_event.clear()
        push_log(f"Manual send: {lead_data['app_name']} <{lead_data['email']}>")
        reset_email_quotas(get_email_urls())
        ttype = "OLD APP" if format_score(lead_data.get("score")) else "NEW APP"
        push_log(f"  🤖 AI writing email [{ttype} template]...")
        subject, body = ai_gen_email(
            lead_data, get_cfg("EMAIL_SUBJECT") or "", get_cfg("EMAIL_BODY") or "")
        status, _ = send_email(lead_data, subject, body)
        if status == "ok":
            with state_lock:
                for l in state.get("leads", []):
                    if l.get("app_id") == lead_data["app_id"]:
                        l["email_sent"] = True
                        break
                state["emails_sent"] = state.get("emails_sent", 0) + 1
            sheet_mark_sent(lead_data["app_id"], lead_data["email"], lead_data["app_name"])
            push_log(f"  ✅ Manual send complete: {lead_data['email']}")
        elif status == "quota":
            push_log(f"  Quota exhausted — could not send to {lead_data['email']}")
        else:
            push_log(f"  Send failed for {lead_data['email']}")
        upd(running=False, phase="done")

    threading.Thread(target=_send_single, args=(lead,), daemon=True).start()
    return jsonify({"ok": True, "app_id": lead["app_id"], "email": lead["email"]})

@application.route("/api/scraped_stats", methods=["POST"])
def api_scraped_stats():
    """Return a breakdown of the Scraped Apps DB tab by status."""
    data      = request.get_json(silent=True) or {}
    sheet_url = data.get("sheet_url") or os.environ.get("APPS_SCRIPT_WEB_URL", "")
    if not sheet_url:
        return jsonify({"error": "sheet_url not set"}), 400
    try:
        r    = requests.post(sheet_url, json={"action": "get_scraped_apps"}, timeout=45)
        rows = (r.json() if r.text else {}).get("apps", [])
        counts = {"qualified": 0, "no_email": 0, "filtered": 0,
                  "no_detail": 0, "total": len(rows)}
        for row in rows:
            st = (row.get("Status") or "").strip().lower()
            if st in counts:
                counts[st] += 1
        return jsonify({"ok": True, **counts})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@application.route("/api/clear_scraped_db", methods=["POST"])
def api_clear_scraped_db():
    """
    Clear the Scraped Apps DB tab from the sheet and reset in-memory skip cache.
    Use when you want a completely fresh scrape that ignores all previous history.
    """
    with state_lock:
        if state["running"]:
            return jsonify({"error": "Cannot clear while running"}), 409
    data      = request.get_json(silent=True) or {}
    sheet_url = data.get("sheet_url") or os.environ.get("APPS_SCRIPT_WEB_URL", "")
    if not sheet_url:
        return jsonify({"error": "sheet_url not set"}), 400
    try:
        r      = requests.post(sheet_url, json={"action": "clear_scraped_apps"},
                               timeout=25)
        result = r.json() if r.text else {}
        global scraped_skip_ids
        scraped_skip_ids = set()
        with cache_lock:
            sheet_scraped_skip_ids.clear()
        push_log("Scraped Apps DB cleared.")
        return jsonify({"ok": True, "msg": result.get("msg", "Cleared")})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    application.run(host="0.0.0.0", port=port, debug=False)
