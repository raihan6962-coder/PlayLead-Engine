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
# In-memory caches
#
# DESIGN PRINCIPLE — Three separate caches with different purposes:
#
#   qualified_ids / qualified_emails
#       → Tracks apps/emails that became actual qualified leads.
#         Used for dedup: never send two emails to same app or same email.
#         Loaded from "All Leads" tab at run start. Persists until /api/clear.
#
#   scraped_skip_ids
#       → Tracks app IDs we have FULLY processed and should NEVER re-fetch.
#         Only contains status: "qualified", "filtered", "no_detail".
#         "no_email" apps are intentionally NOT here — we recheck them next run
#         in case the developer added an email since last check.
#         Loaded from "Scraped Apps" tab at run start.
#
# WHY THIS FIXES THE MAIN BUG:
#   Previously, ANY app that failed the filter got added to global_seen_ids,
#   so it was permanently skipped even if the filter criteria changed or the
#   app was in a different mode. Now:
#     - qualified_ids only grows when a lead is actually created or confirmed.
#     - scraped_skip_ids only grows for permanent-skip statuses.
#     - "no_email" apps are rechecked every run.
# ══════════════════════════════════════════════════════════════════════════════
qualified_ids:    set = set()   # app_ids of confirmed leads
qualified_emails: set = set()   # emails of confirmed leads
scraped_skip_ids: set = set()   # app_ids to skip permanently (not no_email)

# Sheet-loaded versions (merged at run start)
sheet_qualified_ids:    set = set()
sheet_qualified_emails: set = set()
sheet_scraped_skip_ids: set = set()

cache_lock        = threading.Lock()
cache_loaded:bool = False

# ══════════════════════════════════════════════════════════════════════════════
# Email cooldown state
# ══════════════════════════════════════════════════════════════════════════════
email_state_lock       = threading.Lock()
email_url_quotas: dict = {}
global_cooldown_until: float = 0.0
cooldown_retry_thread  = None
cooldown_retry_cancel  = threading.Event()

# ══════════════════════════════════════════════════════════════════════════════
# Run config
# ══════════════════════════════════════════════════════════════════════════════
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


# ══════════════════════════════════════════════════════════════════════════════
# Safe request wrappers — retry + exponential backoff on rate limits
# ══════════════════════════════════════════════════════════════════════════════

def _is_rate_limit(e: Exception) -> bool:
    msg = str(e).lower()
    return "429" in msg or "too many" in msg or "rate" in msg or "blocked" in msg

def safe_search(keyword: str, lang: str, country: str,
                n_hits: int = 250, retries: int = 4) -> list:
    for attempt in range(retries):
        try:
            return search(keyword, lang=lang, country=country, n_hits=n_hits) or []
        except Exception as e:
            if _is_rate_limit(e):
                wait = (2 ** attempt) * random.uniform(6, 14)
                push_log(f"  Rate-limited [{country}] — waiting {wait:.0f}s (attempt {attempt+1})")
                if stop_event.wait(wait): return []
            else:
                push_log(f"  Search error [{country}/{keyword}]: {e}")
                if stop_event.wait(2): return []
    return []

def safe_app_detail(app_id: str, retries: int = 4) -> dict | None:
    for attempt in range(retries):
        try:
            return gp_app(app_id, lang="en", country="us")
        except Exception as e:
            if _is_rate_limit(e):
                wait = (2 ** attempt) * random.uniform(5, 12)
                push_log(f"  Rate-limited detail {app_id} — waiting {wait:.0f}s")
                if stop_event.wait(wait): return None
            else:
                if attempt == retries - 1:
                    push_log(f"  Detail fail {app_id}: {e}")
                if stop_event.wait(1.5): return None
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Google Sheet via Apps Script
# ══════════════════════════════════════════════════════════════════════════════

def sheet_post(payload: dict, timeout: int = 20):
    url = get_cfg("APPS_SCRIPT_WEB_URL")
    if not url: return None
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
        "Keyword":    keyword,
        "Leads Found": count,
        "Logged At":  time.strftime("%Y-%m-%d %H:%M:%S"),
    }})

def sheet_record_scraped(app_id: str, app_name: str, status: str,
                          installs: int = 0, score=None,
                          category: str = "", keyword: str = ""):
    """
    Write one row to the 'Scraped Apps' tab.
    Called for every app whose details are fetched, regardless of outcome.

    Status meanings:
      qualified  — passed filter + has email → lead created (or was already a dup)
      no_email   — passed filter but no email → will RECHECK on next run
      filtered   — failed install/score/relevance filter → skip forever
      no_detail  — Play Store returned None → skip forever
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
# Cache management — load, query, register
# ══════════════════════════════════════════════════════════════════════════════

# Statuses that mean "never re-fetch this app" in Scraped Apps tab
_PERMANENT_SKIP = {"qualified", "filtered", "no_detail"}

def load_caches_from_sheet():
    """
    Called once at the start of every automation run.
    Loads:
      1. All qualified lead app_ids + emails from 'All Leads' tab
         → used by is_duplicate() to avoid re-emailing
      2. All PERMANENT-SKIP app_ids from 'Scraped Apps' tab
         → used by is_already_scraped() to skip re-fetching
    'no_email' apps are deliberately NOT loaded into scraped_skip_ids
    so they are re-fetched and rechecked each run.
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
        push_log("  No sheet URL — in-memory dedup only.")
        with cache_lock: cache_loaded = True
        return

    push_log("  Loading sheet caches...")

    # 1. Qualified leads cache
    try:
        r     = requests.post(url, json={"action": "get_all_leads"}, timeout=30)
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
        push_log(f"  Qualified cache load failed: {e}")

    # 2. Scraped Apps permanent-skip cache
    try:
        r    = requests.post(url, json={"action": "get_scraped_apps"}, timeout=40)
        rows = (r.json() if r.text else {}).get("apps", [])
        skip_count   = 0
        recheck_count = 0
        with cache_lock:
            for row in rows:
                aid    = (row.get("App ID") or "").strip()
                status = (row.get("Status") or "").strip().lower()
                if not aid: continue
                if status in _PERMANENT_SKIP:
                    sheet_scraped_skip_ids.add(aid)
                    skip_count += 1
                elif status == "no_email":
                    recheck_count += 1   # intentionally NOT added to skip set
        push_log(f"  Scraped DB: {skip_count} permanent-skip, "
                 f"{recheck_count} no_email apps will be rechecked.")
    except Exception as e:
        push_log(f"  Scraped DB cache load failed: {e}")

    with cache_lock:
        cache_loaded = True


def is_duplicate(app_id: str, email: str) -> bool:
    """
    Returns True if this app_id OR email already exists as a qualified lead.
    Checks both in-memory session cache and sheet cache.
    """
    el = email.strip().lower()
    if app_id in qualified_ids or el in qualified_emails:
        return True
    with cache_lock:
        return app_id in sheet_qualified_ids or el in sheet_qualified_emails

def is_already_scraped(app_id: str) -> bool:
    """
    Returns True if this app was already processed in a previous run with a
    PERMANENT-SKIP status (qualified / filtered / no_detail).
    'no_email' apps return False — they are rechecked every run.
    """
    if app_id in scraped_skip_ids:
        return True
    with cache_lock:
        return app_id in sheet_scraped_skip_ids

def register_qualified(app_id: str, email: str):
    """
    Call when a lead is confirmed qualified (email found, filter passed, not dup).
    Updates both session cache and sheet cache so is_duplicate() works immediately.
    """
    qualified_ids.add(app_id)
    qualified_emails.add(email.strip().lower())
    with cache_lock:
        sheet_qualified_ids.add(app_id)
        sheet_qualified_emails.add(email.strip().lower())

def register_scraped_skip(app_id: str):
    """
    Call when an app is processed with a permanent-skip status.
    Updates both caches immediately.
    """
    scraped_skip_ids.add(app_id)
    with cache_lock:
        sheet_scraped_skip_ids.add(app_id)


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
    Relaxed relevance check — app passes if ANY one keyword token matches.
    We check title + genre + first 600 chars of description.
    If tokens list is empty (keyword was all stop words), every app passes.
    """
    if not tokens: return True
    combined = f"{title} {genre} {desc[:600]}".lower()
    return any(t in combined for t in tokens)


# ══════════════════════════════════════════════════════════════════════════════
# AI keyword generation
# ══════════════════════════════════════════════════════════════════════════════

def ai_gen_keywords(original: str, used: list, hunter: dict = None) -> list:
    key = get_cfg("GROQ_API_KEY")
    if not key:
        push_log("GROQ_API_KEY not set — using fallback keywords")
        return _fallback_keywords(original, used)

    client    = Groq(api_key=key)
    used_str  = ", ".join(used[:30]) if used else "none"
    is_hunter = bool(hunter and hunter.get("active"))

    if is_hunter:
        max_inst  = int(hunter.get("max_installs") or 5000)
        max_score = float(hunter.get("max_score") or 2.5)
        prompt = (
            f"You are a Google Play Store keyword expert specializing in finding "
            f"struggling apps with low ratings.\n"
            f"NICHE: \"{original}\" | Already used: {used_str}\n\n"
            f"Target: Apps with rating ≤{max_score} stars AND <{max_inst:,} installs "
            f"in the '{original}' niche. These are apps that have SOME users but bad reviews.\n\n"
            f"Generate 20 diverse keyword phrases (2–4 words each) that would surface:\n"
            f"  - Established but struggling apps in this niche\n"
            f"  - Apps with poor user experience in this space\n"
            f"  - Competitive keywords where many apps exist (more apps = more chances)\n"
            f"  - Category-level searches, not just specific product names\n"
            f"Stay within the '{original}' niche. No repeats from used list.\n"
            f"Return ONLY a JSON array of strings."
        )
    else:
        prompt = (
            f"You are a Google Play Store keyword expert specializing in finding "
            f"brand new apps with zero reviews.\n"
            f"NICHE: '{original}' | Already used: {used_str}\n\n"
            f"Target: Apps with 0 reviews, no rating, and <5000 installs in '{original}' niche.\n\n"
            f"Generate 20 diverse keyword phrases (2–4 words each) that would surface:\n"
            f"  - Very new, recently published apps in this niche\n"
            f"  - Niche sub-categories where new developers publish\n"
            f"  - Long-tail specific keywords (less competitive = newer apps rank)\n"
            f"  - Category-level terms that list many new apps\n"
            f"Stay within the '{original}' niche. No repeats from used list.\n"
            f"Return ONLY a JSON array of strings."
        )

    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8, max_tokens=700
        )
        raw = re.sub(r"```[a-z]*", "", resp.choices[0].message.content.strip()).replace("```","").strip()
        kws = [str(k).strip() for k in json.loads(raw) if str(k).strip() not in used]
        push_log(f"  AI keywords ({len(kws)}): {kws[:8]}...")
        if len(kws) < 5:
            kws.extend(_fallback_keywords(original, used + kws))
        return kws
    except Exception as e:
        push_log(f"  AI keyword error: {e} — fallback")
        return _fallback_keywords(original, used)

def _fallback_keywords(original: str, used: list) -> list:
    base = original.lower().strip()
    candidates = []
    suffixes = ["lite","simple","basic","mini","micro","offline","local","tracker",
                "logger","monitor","ledger","tool","helper","assistant","companion",
                "free","dashboard","manager","diary","notes","record","log","2024",
                "2025","app","mobile","android","phone","budget","personal","daily"]
    prefixes = ["simple","easy","offline","local","micro","indie","basic","personal",
                "free","quick","smart","tiny","pocket","handy","my","daily","best",
                "new","top","fast","clean","lightweight","minimal","open source"]
    for s in suffixes:
        c = f"{base} {s}"
        if c not in used: candidates.append(c)
    for p in prefixes:
        c = f"{p} {base}"
        if c not in used: candidates.append(c)
    words = base.split()
    if len(words) > 1:
        for i in range(len(words)):
            for j in range(i + 1, len(words)):
                c = f"{words[i]} {words[j]}"
                if c not in used and len(c) > 4: candidates.append(c)
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
    if score is None or score == "" or score == 0: return ""
    try:
        val = float(score)
        return f"{val:.1f}" if val > 0 else ""
    except: return ""

def select_template(lead: dict, base_subject: str = "", base_body: str = "") -> tuple:
    has_rating = bool(format_score(lead.get("score")))
    if has_rating:
        return (get_cfg("OLD_APP_EMAIL_SUBJECT") or base_subject or DEFAULT_OLD_APP_SUBJECT,
                get_cfg("OLD_APP_EMAIL_BODY")    or base_body    or DEFAULT_OLD_APP_BODY)
    return (get_cfg("NEW_APP_EMAIL_SUBJECT") or base_subject or DEFAULT_NEW_APP_SUBJECT,
            get_cfg("NEW_APP_EMAIL_BODY")    or base_body    or DEFAULT_NEW_APP_BODY)

def personalize_template(tpl: str, lead: dict) -> str:
    installs_raw = lead.get("installs")
    try:    installs_str = f"{int(installs_raw):,}" if installs_raw else "growing app"
    except: installs_str = str(installs_raw)
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

def fill_template(tpl: str, lead: dict) -> str:
    return personalize_template(tpl, lead)

def ai_gen_email(lead: dict, base_subject: str, base_body: str):
    key = get_cfg("GROQ_API_KEY")
    tpl_subject, tpl_body = select_template(lead, base_subject, base_body)
    if not key:
        return personalize_template(tpl_subject, lead), personalize_template(tpl_body, lead)

    client        = Groq(api_key=key)
    score_fmt     = format_score(lead.get("score"))
    prefilled_sub = personalize_template(tpl_subject, lead)
    prefilled_bod = personalize_template(tpl_body, lead)
    ttype         = "OLD APP (has rating)" if score_fmt else "NEW APP (no rating)"
    install_info  = f"{lead['installs']:,} installs" if lead.get("installs") else "just launched"

    prompt = f"""You are a cold email personalizer. Fill template with app details. Keep structure identical.

TEMPLATE TYPE: {ttype}
Subject: {tpl_subject}
Body:
{tpl_body}

APP: {lead.get('app_name','')} | Dev: {lead.get('developer','')} | Cat: {lead.get('category','app')}
Installs: {install_info} | Rating: {score_fmt or 'no rating'} | URL: {lead.get('url','')}
Sender: {get_cfg("SENDER_NAME","Your Name")}, {get_cfg("SENDER_COMPANY","Your Company")}

RULES:
1. Same structure — change at most 2-3 words to fit the app
2. Replace ALL {{{{variable}}}} placeholders
3. Preserve line breaks. Use \\n in JSON
4. NEVER leave any {{{{variable}}}} in output
5. Return ONLY valid JSON: {{"subject":"...","body":"..."}}"""

    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3, max_tokens=600
        )
        raw  = re.sub(r"```[a-z]*", "", resp.choices[0].message.content.strip()).replace("```","").strip()
        data = json.loads(raw)
        sub  = re.sub(r"\{\{[a-zA-Z_]+\}\}", "", data.get("subject") or prefilled_sub)
        bod  = re.sub(r"\{\{[a-zA-Z_]+\}\}", "", (data.get("body") or prefilled_bod).replace("\\n","\n"))
        return sub, bod
    except Exception as e:
        push_log(f"  AI email error (fallback): {e}")
        return prefilled_sub, prefilled_bod


# ══════════════════════════════════════════════════════════════════════════════
# Search combos
#
# NORMAL MODE — Uses more countries to find more brand-new apps.
# New apps appear in their developer's home country first, so we cast wide.
#
# HUNTER MODE — Uses countries with active app markets to find rated apps.
# More countries = more total apps = more chances of finding rated apps.
# ══════════════════════════════════════════════════════════════════════════════

# Normal mode: 10 countries — maximizes discovery of newly published apps
NORMAL_SEARCH_COMBOS = [
    ("en", "us"), ("en", "gb"), ("en", "in"), ("en", "au"), ("en", "ca"),
    ("en", "ng"), ("en", "gh"), ("en", "ke"), ("en", "ph"), ("en", "my"),
]

# Hunter mode: 16 countries — active app markets with many rated apps
HUNTER_SEARCH_COMBOS = [
    ("en", "us"), ("en", "gb"), ("en", "in"), ("en", "au"), ("en", "ca"),
    ("en", "nz"), ("en", "sg"), ("en", "za"), ("en", "ng"), ("en", "gh"),
    ("en", "ke"), ("en", "ph"), ("en", "my"), ("en", "pk"), ("en", "bd"),
    ("en", "tz"),
]

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Parallel detail fetch workers
DETAIL_WORKERS = 6


def extract_email(text: str) -> str:
    if not text: return ""
    m = EMAIL_RE.search(str(text))
    return m.group(0) if m else ""


# ══════════════════════════════════════════════════════════════════════════════
# Mode filters
#
# NORMAL MODE FILTER — Brand new apps:
#   • score must be None or 0.0 — Play Store has not published a rating yet
#   • ratings_count must be 0 — absolutely no reviews
#   • installs: 10 to 10,000 — captures apps right from launch
#
#   WHY LOWER BOUND IS 10 (not 500):
#     Many brand-new apps on Play Store show "10+" installs which maps to
#     minInstalls=10. Setting 500 as minimum was cutting off most new apps.
#     10 is the real minimum that indicates a real (not ghost) app.
#
#   WHY UPPER BOUND IS 10,000 (not 5,000):
#     Some new apps grow quickly. 10k is still clearly "small / new" and
#     gives us more leads to work with.
#
# HUNTER MODE FILTER — Struggling rated apps:
#   • score must be > 0 — must have a real published rating
#   • score ≤ max_score — rating must be low (bad reviews)
#   • installs: 50 to max_inst — real users but small audience
#
#   WHY LOWER BOUND IS 50 (not 100):
#     Some struggling apps have few installs. 50 is low enough to catch them.
# ══════════════════════════════════════════════════════════════════════════════

NORMAL_MIN_INSTALLS = 10
NORMAL_MAX_INSTALLS = 10_000

HUNTER_MIN_INSTALLS = 50


def _passes_normal(score_raw, ratings_count: int, installs: int) -> bool:
    """
    Normal mode: app must have NO rating, NO reviews, and be small (new).
    Uses raw Play Store values — no conversion applied before calling this.
    """
    # Has ANY published rating → not a new app → reject
    if score_raw is not None:
        try:
            if float(score_raw) > 0:
                return False
        except (TypeError, ValueError):
            pass

    # Has ANY reviews → not truly new → reject
    if int(ratings_count or 0) > 0:
        return False

    # Install range: 10 to 10,000
    inst = int(installs or 0)
    if inst < NORMAL_MIN_INSTALLS or inst > NORMAL_MAX_INSTALLS:
        return False

    return True


def _passes_hunter(score_raw, installs: int, max_score: float, max_inst: int) -> bool:
    """
    Hunter mode: app must have a REAL positive rating that is LOW (struggling).
    Uses raw Play Store score — never zeroed or converted before this call.
    """
    if score_raw is None:
        return False
    try:
        s = float(score_raw)
    except (TypeError, ValueError):
        return False

    # Must have a real positive score (unrated apps go to normal mode)
    if s <= 0:
        return False

    # Score must be at or below the threshold
    if s > max_score:
        return False

    # Install range
    inst = int(installs or 0)
    if inst < HUNTER_MIN_INSTALLS or inst > int(max_inst):
        return False

    return True


# ══════════════════════════════════════════════════════════════════════════════
# Search phase — collect app ID pool for a keyword
# ══════════════════════════════════════════════════════════════════════════════

def _collect_app_ids_for_keyword(keyword: str, combos: list) -> list:
    """
    Search all country combos for this keyword.
    Returns a deduplicated list of app IDs.
    Countries are shuffled so we don't always hit the same ones first.
    """
    seen_in_search: set = set()
    app_ids: list = []

    combos_shuffled = list(combos)
    random.shuffle(combos_shuffled)

    for lang, country in combos_shuffled:
        if stop_event.is_set():
            break

        results = safe_search(keyword, lang=lang, country=country, n_hits=250)
        new_ids = 0
        for item in results:
            aid = (item.get("appId") or "").strip()
            if aid and aid not in seen_in_search:
                seen_in_search.add(aid)
                app_ids.append(aid)
                new_ids += 1

        push_log(f"  [{country}] {len(results)} results, {new_ids} new IDs "
                 f"(pool: {len(app_ids)})")

        # Brief pause between country searches to be polite to Play Store
        if stop_event.wait(random.uniform(1.2, 3.0)):
            break

    push_log(f"  Pool size for '{keyword}': {len(app_ids)} unique app IDs")
    return app_ids


def _fetch_one_detail(app_id: str) -> tuple[str, dict | None]:
    """Fetch details for one app. Small stagger so threads don't all fire at once."""
    time.sleep(random.uniform(0.1, 0.8))
    details = safe_app_detail(app_id)
    return app_id, details


# ══════════════════════════════════════════════════════════════════════════════
# Process phase — fetch details, filter, dedup, record
#
# FLOW FOR EACH APP IN THE POOL:
#
#   1. Skip if already permanently processed (is_already_scraped).
#      'no_email' apps pass through — they are rechecked.
#
#   2. Fetch Play Store details in parallel (DETAIL_WORKERS threads).
#
#   3. Apply mode filter (_passes_normal or _passes_hunter) using RAW values.
#      If fails → record status="filtered", register_scraped_skip, skip.
#
#   4. Check keyword relevance.
#      If fails → record status="filtered", register_scraped_skip, skip.
#
#   5. Extract email from all available fields.
#      If no email → record status="no_email" (NOT skip-registered), continue.
#      The app will be rechecked next run.
#
#   6. Check is_duplicate (qualified leads cache only).
#      If dup → record status="qualified", register_scraped_skip, skip (no new lead).
#
#   7. All checks passed → build lead, register_qualified, register_scraped_skip.
#      Record status="qualified".
#
# BATCH SHEET WRITING:
#   Scrape records are collected during the thread pool and written to the sheet
#   sequentially AFTER the pool completes. This avoids hammering Apps Script
#   from multiple threads simultaneously and keeps sheet writes clean.
# ══════════════════════════════════════════════════════════════════════════════

def _process_pool(app_ids: list, keyword: str, hunter: dict,
                  keyword_tokens: list) -> list:

    is_hunter = bool(hunter and hunter.get("active"))
    max_inst  = int(hunter.get("max_installs") or 5000) if is_hunter else NORMAL_MAX_INSTALLS
    max_score = float(hunter.get("max_score")  or 2.5)  if is_hunter else 0.0

    leads            = []
    leads_lock       = threading.Lock()
    scrape_records   = []   # collected here, written to sheet after pool
    records_lock     = threading.Lock()

    # ── Step 1: Remove already-permanently-processed app IDs ──────────────────
    # 'no_email' apps are NOT in scraped_skip_ids, so they are included.
    pre_count = len(app_ids)
    to_fetch  = [aid for aid in app_ids if not is_already_scraped(aid)]
    skipped   = pre_count - len(to_fetch)
    if skipped:
        push_log(f"  Skipped {skipped} already-processed app IDs from DB cache")
    push_log(f"  Fetching details: {len(to_fetch)} apps "
             f"({DETAIL_WORKERS} parallel workers)...")

    # ── Internal helper: thread-safe record collector ─────────────────────────
    def _record(aid: str, aname: str, status: str,
                installs: int = 0, score=None, category: str = ""):
        """
        Collect a scrape record and update in-memory skip cache immediately.
        Sheet write happens AFTER the thread pool finishes.
        """
        with records_lock:
            scrape_records.append({
                "app_id": aid, "app_name": aname, "status": status,
                "installs": installs, "score": score,
                "category": category, "keyword": keyword,
            })
        # Immediately update in-memory skip cache so other threads see it
        if status in _PERMANENT_SKIP:
            register_scraped_skip(aid)

    # ── Steps 2–7: Thread pool ────────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
        futures = {executor.submit(_fetch_one_detail, aid): aid for aid in to_fetch}

        for future in as_completed(futures):
            if stop_event.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
                break

            app_id, details = future.result()

            # ── Step 2 result: detail fetch failed ────────────────────────────
            if details is None:
                _record(app_id, app_id, "no_detail")
                continue

            # ── Raw Play Store values — NEVER modified before filter ──────────
            installs      = int(details.get("minInstalls") or 0)
            ratings_count = int(details.get("ratings")     or 0)
            score_raw     = details.get("score")           # float | None | 0.0
            app_name      = details.get("title", app_id)
            app_genre     = details.get("genre", "")
            app_desc      = details.get("description", "")

            # ── Step 3: Mode filter ───────────────────────────────────────────
            if is_hunter:
                passes_filter = _passes_hunter(score_raw, installs, max_score, max_inst)
            else:
                passes_filter = _passes_normal(score_raw, ratings_count, installs)

            if not passes_filter:
                _record(app_id, app_name, "filtered", installs, score_raw, app_genre)
                continue

            # ── Step 4: Keyword relevance ─────────────────────────────────────
            if keyword_tokens and not is_keyword_relevant(app_name, app_genre,
                                                          app_desc, keyword_tokens):
                _record(app_id, app_name, "filtered", installs, score_raw, app_genre)
                continue

            # ── Step 5: Email extraction ──────────────────────────────────────
            # Try all available fields in order of reliability.
            email = (
                extract_email(details.get("developerEmail", ""))
                or extract_email(details.get("privacyPolicy", ""))
                or extract_email(details.get("description", ""))
                or extract_email(details.get("recentChanges", ""))
                or extract_email(details.get("developerWebsite", ""))
            )

            if not email:
                # Passed filter but no email. Record as "no_email" — this is
                # intentionally NOT added to scraped_skip_ids so that next run
                # will re-fetch this app in case the developer added an email.
                _record(app_id, app_name, "no_email", installs, score_raw, app_genre)
                push_log(f"  ⚠ no_email: {app_name} "
                         f"({installs:,} inst | "
                         f"{f'{score_raw:.1f}★' if score_raw else 'no-rating'})"
                         f" — will recheck next run")
                continue

            # ── Step 6: Dedup against qualified leads ─────────────────────────
            if is_duplicate(app_id, email):
                push_log(f"  ⟳ dup: {app_name}")
                # Record as qualified so we don't re-process in future runs
                _record(app_id, app_name, "qualified", installs, score_raw, app_genre)
                continue

            # ── Step 7: All checks passed — qualified lead ────────────────────
            register_qualified(app_id, email)
            _record(app_id, app_name, "qualified", installs, score_raw, app_genre)

            lead = {
                "app_id":        app_id,
                "app_name":      app_name,
                "developer":     (details.get("developer", "") or "").strip(),
                "email":         email,
                "category":      app_genre,
                "installs":      installs,
                "score":         score_raw,   # raw Play Store value — no conversion
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
            push_log(
                f"  ✓ [{mode_tag}] {app_name} | "
                f"{installs:,} inst | {score_str} | "
                f"{ratings_count} rev | {email}"
            )

    # ── Batch write all scrape records to sheet (sequential, after pool) ───────
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

    qualified_count = sum(1 for r in scrape_records if r["status"] == "qualified")
    no_email_count  = sum(1 for r in scrape_records if r["status"] == "no_email")
    filtered_count  = sum(1 for r in scrape_records if r["status"] == "filtered")
    push_log(
        f"  Keyword '{keyword}' summary: "
        f"{len(leads)} new leads | "
        f"{qualified_count} qualified | "
        f"{no_email_count} no_email | "
        f"{filtered_count} filtered"
    )

    return leads


# ══════════════════════════════════════════════════════════════════════════════
# Full keyword scrape
# ══════════════════════════════════════════════════════════════════════════════

def scrape_keyword(keyword: str, hunter: dict = None) -> list:
    push_log(f"▶ Keyword: '{keyword}'")

    combos         = HUNTER_SEARCH_COMBOS if (hunter and hunter.get("active")) else NORMAL_SEARCH_COMBOS
    keyword_tokens = build_keyword_tokens(keyword)

    app_ids = _collect_app_ids_for_keyword(keyword, combos)
    if not app_ids:
        push_log(f"  No search results for '{keyword}'")
        sheet_log_keyword(keyword, 0)
        return []

    leads = _process_pool(app_ids, keyword, hunter or {}, keyword_tokens)

    push_log(f"  '{keyword}' → {len(leads)} new qualified leads "
             f"(from {len(app_ids)} apps in pool)")
    sheet_log_keyword(keyword, len(leads))
    return leads


# ══════════════════════════════════════════════════════════════════════════════
# Email helpers
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
        if url in email_url_quotas: email_url_quotas[url]["exhausted"] = True

def mark_url_failed(url: str):
    with email_state_lock:
        if url in email_url_quotas: email_url_quotas[url]["failed"] = True

def reset_exhausted_urls(urls: list):
    with email_state_lock:
        for u in urls:
            if u in email_url_quotas: email_url_quotas[u]["exhausted"] = False


def send_email(lead: dict, subject: str, body: str):
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
                "to": lead["email"], "subject": subject, "body": body,
            }, timeout=30, allow_redirects=True)

            if "html" in r.headers.get("Content-Type", "").lower():
                push_log("  Email URL deployed incorrectly (Execute as: Me, Access: Anyone).")
                mark_url_failed(url)
                continue

            result  = r.json() if r.text else {}
            err_msg = result.get("msg", "?")

            if result.get("status") == "ok":
                push_log(f"  ✉ Sent: {lead['email']} ({lead['app_name']})")
                return "ok", ""
            elif "Service invoked too many times" in err_msg:
                push_log("  Quota limit hit. Trying next URL...")
                mark_url_exhausted(url)
                quota_hits += 1
            elif "permission" in err_msg.lower() or "authorize" in err_msg.lower():
                push_log("  URL needs authorization.")
                mark_url_failed(url)
            else:
                push_log(f"  Email failed: {err_msg}. Trying next...")
        except Exception as e:
            push_log(f"  Email error: {e}")

    if quota_hits >= len(urls):
        push_log("  All email scripts hit Google's daily limit.")
        return "quota", "All URLs exhausted"

    push_log("  All email scripts failed for this lead.")
    return "error", "All URLs failed"


# ══════════════════════════════════════════════════════════════════════════════
# Cooldown / retry scheduler
# ══════════════════════════════════════════════════════════════════════════════

COOLDOWN_SECONDS = 3600

def _is_automation_running() -> bool:
    with state_lock: return state.get("running", False)

def _cancel_cooldown_retry():
    global cooldown_retry_thread
    cooldown_retry_cancel.set()
    cooldown_retry_thread = None

def _schedule_email_retry(leads_to_send: list, base_subject: str, base_body: str):
    global global_cooldown_until
    cooldown_retry_cancel.clear()
    with email_state_lock:
        global_cooldown_until = time.time() + COOLDOWN_SECONDS
    push_log("  Email cooldown started. Retry in 1 hour.")

    for _ in range(COOLDOWN_SECONDS):
        if cooldown_retry_cancel.is_set():
            push_log("  Cooldown retry cancelled.")
            with email_state_lock: global_cooldown_until = 0.0
            return
        time.sleep(1)

    with email_state_lock: global_cooldown_until = 0.0
    if _is_automation_running():
        push_log("  Cooldown over, automation running — skipping auto-resume.")
        return

    push_log("  Cooldown over. Resetting quotas and retrying emails...")
    reset_exhausted_urls(get_email_urls())
    sent = 0
    for i, lead in enumerate(leads_to_send):
        if stop_event.is_set() or cooldown_retry_cancel.is_set(): break
        subject, body = ai_gen_email(lead, base_subject, base_body)
        status, _     = send_email(lead, subject, body)
        if status == "ok":
            sent += 1
            sheet_mark_sent(lead["app_id"], lead["email"], lead["app_name"])
            with state_lock: state["emails_sent"] = state.get("emails_sent", 0) + 1
        elif status == "quota":
            push_log("  Still exhausted. Re-entering cooldown...")
            global cooldown_retry_thread
            cooldown_retry_thread = threading.Thread(
                target=_schedule_email_retry,
                args=(leads_to_send[i:], base_subject, base_body), daemon=True
            )
            cooldown_retry_thread.start()
            return
        if i < len(leads_to_send) - 1:
            if stop_event.wait(random.uniform(30, 60)): break
    push_log(f"  Retry done. {sent} additional emails sent.")


# ══════════════════════════════════════════════════════════════════════════════
# Email sending loop
# ══════════════════════════════════════════════════════════════════════════════

def email_loop(leads: list, base_subject: str, base_body: str):
    global cooldown_retry_thread
    pending    = [l for l in leads if not l.get("email_sent") and l.get("email")]
    total      = len(pending)
    sent_count = 0
    push_log(f"  Email loop: {total} leads pending.")

    i = 0
    while i < len(pending):
        if stop_event.is_set():
            push_log("Stopped during email phase.")
            return

        lead  = pending[i]
        ttype = "OLD APP" if format_score(lead.get("score")) else "NEW APP"
        push_log(f"  [{i+1}/{total}] Writing email for {lead['app_name']} [{ttype}]")
        subject, body = ai_gen_email(lead, base_subject, base_body)
        status, _     = send_email(lead, subject, body)

        if status == "ok":
            lead["email_sent"] = True
            sent_count += 1
            with state_lock:
                state["emails_sent"] = state.get("emails_sent", 0) + 1
                state["leads"] = [l.copy() for l in leads]
            sheet_mark_sent(lead["app_id"], lead["email"], lead["app_name"])
            push_log(f"  Sent {sent_count}/{total}.")
            i += 1
        elif status == "quota":
            remaining = pending[i:]
            push_log(f"  Quota exhausted. {len(remaining)} leads queued for 1-hour retry.")
            cooldown_retry_thread = threading.Thread(
                target=_schedule_email_retry,
                args=(remaining, base_subject, base_body), daemon=True
            )
            cooldown_retry_thread.start()
            return
        else:
            push_log(f"  Send failed for {lead['app_name']}. Moving on...")
            i += 1

        if stop_event.is_set(): return
        if i < len(pending):
            wait = random.uniform(30, 60)
            push_log(f"  Waiting {wait:.0f}s before next email ({i}/{total} done)")
            if stop_event.wait(wait): return

    push_log(f"  Email loop done. Sent: {sent_count}/{total}")


# ══════════════════════════════════════════════════════════════════════════════
# Master automation
# ══════════════════════════════════════════════════════════════════════════════

def run_automation(initial_kw: str, target: int, hunter: dict = None):
    global cooldown_retry_thread

    if cooldown_retry_thread and cooldown_retry_thread.is_alive():
        _cancel_cooldown_retry()
        push_log("  Cancelled pending email retry (new automation starting).")

    upd(running=True, phase="scraping", keyword=initial_kw,
        keywords_used=[], leads_found=0, emails_sent=0, logs=[], leads=[])
    stop_event.clear()

    mode = "Hunter" if (hunter and hunter.get("active")) else "Normal"
    push_log(f"▶ Started | kw='{initial_kw}' | target={target} | mode={mode}")

    # Load both caches: qualified leads dedup + scraped apps permanent-skip
    load_caches_from_sheet()

    base_subject = get_cfg("EMAIL_SUBJECT") or ""
    base_body    = get_cfg("EMAIL_BODY")    or ""

    reset_email_quotas(get_email_urls())

    all_leads = []
    kws_used  = [initial_kw]
    kw_queue  = [initial_kw]

    while len(all_leads) < target and not stop_event.is_set():
        # Refill keyword queue when empty
        if not kw_queue:
            push_log("  Requesting more AI keywords...")
            new_kws = ai_gen_keywords(initial_kw, kws_used, hunter)
            if not new_kws:
                push_log("  No more keywords available. Stopping scrape.")
                break
            kw_queue.extend(new_kws)

        kw = kw_queue.pop(0)
        if kw not in kws_used:
            kws_used.append(kw)
        upd(keywords_used=kws_used[:], phase="scraping")

        batch = scrape_keyword(kw, hunter)
        all_leads.extend(batch)
        upd(leads_found=len(all_leads), leads=[l.copy() for l in all_leads])

        # Write new batch leads to sheet immediately
        for lead in batch:
            sheet_append_lead(lead)
            sheet_append_qualified(lead)

        push_log(f"  Progress: {len(all_leads)} / {target} leads found")

        # Short pause between keywords (only if there are more to process)
        if not stop_event.is_set() and len(all_leads) < target:
            wait = random.uniform(4, 10)
            push_log(f"  Pausing {wait:.0f}s before next keyword...")
            stop_event.wait(wait)

    if stop_event.is_set():
        push_log("Stopped.")
        upd(running=False, phase="stopped")
        return

    push_log(f"Scraping done. {len(all_leads)} leads collected. Starting emails...")
    upd(phase="emailing")
    email_loop(all_leads, base_subject, base_body)

    if stop_event.is_set():
        upd(running=False, phase="stopped")
    else:
        push_log("✓ Automation complete!")
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
    push_log(f"Sending pending: {len(leads)} leads")

    base_subject = get_cfg("EMAIL_SUBJECT") or ""
    base_body    = get_cfg("EMAIL_BODY")    or ""

    reset_email_quotas(get_email_urls())
    email_loop(leads, base_subject, base_body)
    push_log("Pending send complete.")
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
    threading.Thread(target=run_automation, args=(keyword, target, hunter), daemon=True).start()
    return jsonify({"ok": True, "keyword": keyword})

@application.route("/api/stop", methods=["POST"])
def api_stop():
    stop_event.set()
    _cancel_cooldown_retry()
    upd(running=False, phase="stopped")
    push_log("Stop requested.")
    return jsonify({"ok": True})

@application.route("/api/status")
def api_status():
    with state_lock: s = dict(state)
    with email_state_lock: remaining = max(0, global_cooldown_until - time.time())
    s["cooldown_remaining_seconds"] = int(remaining)
    return jsonify(s)

@application.route("/api/clear", methods=["POST"])
def api_clear():
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
    run_cfg = build_run_cfg(data)
    fresh   = [l for l in leads if not l.get("email_sent") and l.get("email")]
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
        "category":  "Productivity", "installs": 1500,
        "score":     sample_score, "email": test_to,
        "url":       "https://play.google.com/store/apps/details?id=com.example",
    }
    urls = [u.strip() for u in get_cfg("EMAIL_SCRIPT_URL").replace(",", "\n").split("\n") if u.strip()]
    url  = urls[0] if urls else None
    if not url:
        return jsonify({"error": "EMAIL_SCRIPT_URL not set"}), 400
    ttype = "OLD APP" if format_score(sample_score) else "NEW APP"
    push_log(f"  Spam test: {ttype} template (score={sample_score})")
    subject, body = ai_gen_email(sample, get_cfg("EMAIL_SUBJECT") or "", get_cfg("EMAIL_BODY") or "")
    try:
        r      = requests.post(url, json={"to": test_to, "subject": subject, "body": body}, timeout=30)
        result = r.json() if r.text else {}
        if result.get("status") == "ok":
            return jsonify({"ok": True,
                            "msg": f"Test sent to {test_to} [{ttype}]",
                            "template_type": ttype,
                            "subject": subject,
                            "body": body})
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
        push_log(f"  AI writing email [{ttype} template]")
        subject, body = ai_gen_email(lead_data,
                                     get_cfg("EMAIL_SUBJECT") or "",
                                     get_cfg("EMAIL_BODY") or "")
        status, _ = send_email(lead_data, subject, body)
        if status == "ok":
            with state_lock:
                for l in state.get("leads", []):
                    if l.get("app_id") == lead_data["app_id"]:
                        l["email_sent"] = True
                        break
                state["emails_sent"] = state.get("emails_sent", 0) + 1
            sheet_mark_sent(lead_data["app_id"], lead_data["email"], lead_data["app_name"])
            push_log(f"  Manual send complete: {lead_data['email']}")
        elif status == "quota":
            push_log(f"  Quota exhausted — could not send to {lead_data['email']}")
        else:
            push_log(f"  Send failed for {lead_data['email']}")
        upd(running=False, phase="done")

    threading.Thread(target=_send_single, args=(lead,), daemon=True).start()
    return jsonify({"ok": True, "app_id": lead["app_id"], "email": lead["email"]})

@application.route("/api/scraped_stats", methods=["POST"])
def api_scraped_stats():
    """Return stats about the Scraped Apps DB tab."""
    data      = request.get_json(silent=True) or {}
    sheet_url = data.get("sheet_url") or os.environ.get("APPS_SCRIPT_WEB_URL", "")
    if not sheet_url:
        return jsonify({"error": "sheet_url not set"}), 400
    try:
        r    = requests.post(sheet_url, json={"action": "get_scraped_apps"}, timeout=40)
        rows = (r.json() if r.text else {}).get("apps", [])
        counts = {"qualified": 0, "no_email": 0, "filtered": 0,
                  "no_detail": 0, "total": len(rows)}
        for row in rows:
            st = (row.get("Status") or "").strip().lower()
            if st in counts: counts[st] += 1
        return jsonify({"ok": True, **counts})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@application.route("/api/clear_scraped_db", methods=["POST"])
def api_clear_scraped_db():
    """
    Clear the Scraped Apps DB tab from the sheet.
    Use this when you want a completely fresh scrape ignoring all history.
    """
    with state_lock:
        if state["running"]:
            return jsonify({"error": "Cannot clear while running"}), 409
    data      = request.get_json(silent=True) or {}
    sheet_url = data.get("sheet_url") or os.environ.get("APPS_SCRIPT_WEB_URL", "")
    if not sheet_url:
        return jsonify({"error": "sheet_url not set"}), 400
    try:
        r      = requests.post(sheet_url, json={"action": "clear_scraped_apps"}, timeout=25)
        result = r.json() if r.text else {}
        # Clear in-memory caches too
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
