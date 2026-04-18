import os, time, random, threading, json, re, logging
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# ── Global duplicate tracker — persists across runs until /api/clear ─────────
# Only tracks QUALIFIED leads (email found + passed filter).
# Apps with no email are tracked in the Scraped Apps sheet, NOT here,
# so they can be re-checked in future runs if an email appears.
global_seen_ids:    set = set()   # app_ids of qualified leads (email found)
global_seen_emails: set = set()   # emails of qualified leads

# ── Sheet duplicate cache — loaded once per automation run ───────────────────
# Covers: qualified lead app_ids, qualified lead emails, AND all scraped app_ids
# so we never re-fetch details for an app we already processed this run.
sheet_known_ids:         set  = set()   # qualified lead app_ids from sheet
sheet_known_emails:      set  = set()   # qualified lead emails from sheet
sheet_scraped_ids:       set  = set()   # ALL app_ids ever scraped (Scraped Apps tab)
sheet_cache_loaded:      bool = False
sheet_cache_lock              = threading.Lock()

# ── Email cooldown state ──────────────────────────────────────────────────────
email_state_lock       = threading.Lock()
email_url_quotas: dict = {}
global_cooldown_until: float = 0.0
cooldown_retry_thread  = None
cooldown_retry_cancel  = threading.Event()

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


# ══════════════════════════════════════════════════════════════════════════════
# ── SAFE REQUEST WRAPPERS  (retry + exponential backoff on 429)
# ══════════════════════════════════════════════════════════════════════════════

def _is_rate_limit(e: Exception) -> bool:
    msg = str(e).lower()
    return "429" in msg or "too many" in msg or "rate" in msg or "blocked" in msg

def safe_search(keyword: str, lang: str, country: str, n_hits: int = 250, retries: int = 3) -> list:
    for attempt in range(retries):
        try:
            return search(keyword, lang=lang, country=country, n_hits=n_hits) or []
        except Exception as e:
            if _is_rate_limit(e):
                wait = (2 ** attempt) * random.uniform(5, 12)
                push_log(f"  Rate-limited [{country}] — waiting {wait:.0f}s (attempt {attempt+1})")
                if stop_event.wait(wait): return []
            else:
                push_log(f"  Search error [{country}]: {e}")
                if stop_event.wait(2): return []
    return []

def safe_app_detail(app_id: str, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            return gp_app(app_id, lang="en", country="us")
        except Exception as e:
            if _is_rate_limit(e):
                wait = (2 ** attempt) * random.uniform(4, 10)
                push_log(f"  Rate-limited detail {app_id} — waiting {wait:.0f}s")
                if stop_event.wait(wait): return None
            else:
                if attempt == retries - 1:
                    push_log(f"  Detail fail {app_id}: {e}")
                if stop_event.wait(1): return None
    return None


# ── Google Sheet via Apps Script ──────────────────────────────────────────────
def sheet_post(payload: dict):
    url = get_cfg("APPS_SCRIPT_WEB_URL")
    if not url: return None
    try:
        r = requests.post(url, json=payload, timeout=15)
        return r.json() if r.text else {}
    except Exception as e:
        push_log(f"  Sheet error: {e}")
        return None

def sheet_append_lead(lead: dict):
    sheet_post({"action": "append", "tab": "All Leads", "row": {
        "App Name":   lead["app_name"],   "Developer": lead["developer"],
        "Email":      lead["email"],       "Category":  lead["category"],
        "Installs":   lead["installs"],    "Score":     lead["score"] or "",
        "URL":        lead["url"],         "Keyword":   lead["keyword"],
        "Scraped At": lead["scraped_at"], "Email Sent": "No",
        "App ID":     lead["app_id"],
    }})

def sheet_append_qualified(lead: dict):
    sheet_post({"action": "append", "tab": "Qualified Leads", "row": {
        "App Name":   lead["app_name"],   "Developer": lead["developer"],
        "Email":      lead["email"],       "Category":  lead["category"],
        "Installs":   lead["installs"],    "Score":     lead["score"] or "",
        "URL":        lead["url"],         "Keyword":   lead["keyword"],
        "Scraped At": lead["scraped_at"], "Email Sent": "Pending",
        "App ID":     lead["app_id"],
    }})

def sheet_mark_sent(app_id: str, email: str, app_name: str):
    sheet_post({"action": "mark_sent", "app_id": app_id, "email": email, "app_name": app_name})
    sheet_post({"action": "append", "tab": "Email Sent", "row": {
        "App ID": app_id, "App Name": app_name,
        "Email":  email,  "Sent At":  time.strftime("%Y-%m-%d %H:%M:%S"),
    }})

def sheet_log_keyword(keyword: str, count: int):
    sheet_post({"action": "append", "tab": "Keyword Log", "row": {
        "Keyword": keyword, "Leads Found": count,
        "Logged At": time.strftime("%Y-%m-%d %H:%M:%S"),
    }})

# ── NEW: Scraped Apps tab helpers ─────────────────────────────────────────────
# Every app we fetch details for is recorded here with its outcome status:
#   "qualified"  — passed filter + has email → sent to Qualified Leads
#   "no_email"   — passed filter but no email found
#   "filtered"   — failed mode filter (wrong install count / score / etc.)
#   "no_detail"  — Play Store detail fetch returned None
#
# On the NEXT run, we skip apps whose status is "qualified" or "filtered"
# (they won't change). We DO re-check "no_email" apps — the developer may
# have added an email since we last checked.
# ─────────────────────────────────────────────────────────────────────────────

SKIP_STATUSES = {"qualified", "filtered", "no_detail"}  # never re-process these
RECHECK_STATUS = "no_email"                               # worth re-checking later

def sheet_record_scraped(app_id: str, app_name: str, status: str,
                          installs: int = 0, score=None,
                          category: str = "", keyword: str = ""):
    """
    Write one row to the 'Scraped Apps' tab.
    Called for EVERY app whose detail we fetch, regardless of outcome.
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

def sheet_load_scraped_ids() -> set:
    """
    Load all app_ids from 'Scraped Apps' tab that have a SKIP_STATUS.
    These will not be re-fetched in this run.
    Returns a set of app_id strings.
    """
    url = get_cfg("APPS_SCRIPT_WEB_URL")
    if not url:
        return set()
    try:
        r      = requests.post(url, json={"action": "get_scraped_apps"}, timeout=30)
        rows   = (r.json() if r.text else {}).get("apps", [])
        skip   = set()
        recheck = 0
        for row in rows:
            aid    = (row.get("App ID") or "").strip()
            status = (row.get("Status") or "").strip().lower()
            if aid and status in SKIP_STATUSES:
                skip.add(aid)
            elif aid and status == RECHECK_STATUS:
                recheck += 1
        push_log(f"  Scraped Apps DB: {len(skip)} to skip, {recheck} 'no_email' eligible for recheck")
        return skip
    except Exception as e:
        push_log(f"  Could not load Scraped Apps tab: {e}")
        return set()


# ══════════════════════════════════════════════════════════════════════════════
# ── SHEET DUPLICATE CACHE
# Loaded ONCE per run. is_duplicate() covers session memory + sheet in one
# O(1) call.  register_seen() updates both caches simultaneously.
# ══════════════════════════════════════════════════════════════════════════════

def load_sheet_duplicate_cache():
    """
    Load two kinds of data from the sheet:
      1. Qualified lead app_ids + emails → is_duplicate() guard
      2. All scraped app_ids with skip-eligible statuses → sheet_scraped_ids
    Both are loaded in parallel to save time.
    """
    global sheet_known_ids, sheet_known_emails, sheet_scraped_ids, sheet_cache_loaded

    with sheet_cache_lock:
        sheet_known_ids    = set()
        sheet_known_emails = set()
        sheet_scraped_ids  = set()
        sheet_cache_loaded = False

    url = get_cfg("APPS_SCRIPT_WEB_URL")
    if not url:
        push_log("  No sheet URL — in-memory dedup only.")
        with sheet_cache_lock: sheet_cache_loaded = True
        return

    push_log("  Loading sheet caches (qualified leads + scraped DB)...")

    # Load qualified leads (for email/id dedup)
    try:
        r     = requests.post(url, json={"action": "get_all_leads"}, timeout=30)
        leads = (r.json() if r.text else {}).get("leads", [])
        with sheet_cache_lock:
            for lead in leads:
                aid = (lead.get("App ID") or lead.get("app_id") or "").strip()
                em  = (lead.get("Email")  or lead.get("email")  or "").strip().lower()
                if aid: sheet_known_ids.add(aid)
                if em:  sheet_known_emails.add(em)
        push_log(f"  Qualified cache: {len(sheet_known_ids)} app IDs, {len(sheet_known_emails)} emails")
    except Exception as e:
        push_log(f"  Qualified cache failed: {e}")

    # Load scraped apps DB (for skipping already-processed apps)
    scraped_skip = sheet_load_scraped_ids()
    with sheet_cache_lock:
        sheet_scraped_ids  = scraped_skip
        sheet_cache_loaded = True

    push_log(f"  Scraped DB cache: {len(sheet_scraped_ids)} apps will be skipped this run")


def is_duplicate(app_id: str, email: str) -> bool:
    """Check if this QUALIFIED lead already exists (session + sheet cache)."""
    el = email.lower()
    if app_id in global_seen_ids or el in global_seen_emails:
        return True
    with sheet_cache_lock:
        return app_id in sheet_known_ids or el in sheet_known_emails

def is_already_scraped(app_id: str) -> bool:
    """
    Check if this app has already been fully processed in a previous run
    with a status that means we should skip it (qualified / filtered / no_detail).
    'no_email' apps are NOT skipped — we recheck them.
    """
    with sheet_cache_lock:
        return app_id in sheet_scraped_ids

def register_seen(app_id: str, email: str):
    """Update session + sheet cache when a qualified lead is found."""
    global_seen_ids.add(app_id)
    global_seen_emails.add(email.lower())
    with sheet_cache_lock:
        sheet_known_ids.add(app_id)
        sheet_known_emails.add(email.lower())

def register_scraped(app_id: str, status: str):
    """
    Update the in-memory scraped cache immediately after recording to sheet.
    Only adds to skip set if the status warrants skipping on future runs.
    """
    if status in SKIP_STATUSES:
        with sheet_cache_lock:
            sheet_scraped_ids.add(app_id)


# ── Keyword relevance ─────────────────────────────────────────────────────────
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
    if not tokens: return True
    combined = f"{title} {genre} {desc[:400]}".lower()
    return any(t in combined for t in tokens)


# ── AI keyword generation ─────────────────────────────────────────────────────
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
            f"You are a Google Play Store keyword expert.\n"
            f"NICHE: \"{original}\" | Already used: {used_str}\n\n"
            f"Find STRUGGLING apps: <{max_inst:,} installs, rating ≤{max_score} in '{original}' niche.\n"
            f"Generate 15 long-tail keyword phrases (2–4 words). Stay within niche. No repeats.\n"
            f"Return ONLY a JSON array of strings."
        )
    else:
        prompt = (
            f"You are a Google Play Store keyword expert.\n"
            f"NICHE: '{original}' | Already used: {used_str}\n\n"
            f"Find BRAND NEW apps (zero reviews, <5000 installs) in '{original}' niche.\n"
            f"Generate 15 long-tail keyword phrases (2–4 words). Stay within niche. No repeats.\n"
            f"Return ONLY a JSON array of strings."
        )

    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7, max_tokens=600
        )
        raw = re.sub(r"```[a-z]*", "", resp.choices[0].message.content.strip()).replace("```","").strip()
        kws = [str(k).strip() for k in json.loads(raw) if str(k).strip() not in used]
        push_log(f"  AI keywords ({len(kws)}): {kws}")
        if len(kws) < 4:
            kws.extend(_fallback_keywords(original, used + kws))
        return kws
    except Exception as e:
        push_log(f"  AI keyword error: {e} — fallback")
        return _fallback_keywords(original, used)

def _fallback_keywords(original: str, used: list) -> list:
    base = original.lower().strip()
    candidates = []
    for s in ["lite","simple","basic","mini","micro","offline","local","tracker","logger",
               "monitor","ledger","tool","helper","assistant","companion","free","dashboard",
               "manager","diary","notes","record","log"]:
        c = f"{base} {s}"
        if c not in used: candidates.append(c)
    for p in ["simple","easy","offline","local","micro","indie","basic","personal","free",
               "quick","smart","tiny","pocket","handy","my","daily"]:
        c = f"{p} {base}"
        if c not in used: candidates.append(c)
    words = base.split()
    if len(words) > 1:
        for i in range(len(words)):
            for j in range(i+1, len(words)):
                c = f"{words[i]} {words[j]}"
                if c not in used and len(c) > 4: candidates.append(c)
    return candidates[:15]


# ── Email templates ───────────────────────────────────────────────────────────
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
# ── CORE SCRAPING ARCHITECTURE
#
# DESIGN — "Search First, Persistent DB Filter, Then Process":
#
#   Step 1. SEARCH — Collect all app IDs from Play Store search across country
#           combos for a keyword. Store unique IDs in a pool.
#
#   Step 2. FILTER POOL — Remove app IDs that were already processed in a
#           previous run with a permanent-skip status (qualified/filtered/
#           no_detail). Apps with "no_email" status ARE included for recheck.
#
#   Step 3. FETCH DETAILS — For each remaining app ID, fetch full details in
#           parallel (ThreadPoolExecutor, max 5 workers) with small delays.
#
#   Step 4. FILTER — Apply mode-specific filter to fetched details.
#
#   Step 5. DEDUP — Check qualified leads cache (session + sheet).
#
#   Step 6. RECORD — Write every processed app to "Scraped Apps" tab with its
#           outcome status so future runs can skip or recheck intelligently.
#
# KEY IMPROVEMENTS OVER OLD DESIGN:
#   • Apps without email are recorded as "no_email" in sheet and NOT added to
#     global_seen_ids. On next run they get rechecked (email may have appeared).
#   • Apps that fail the mode filter are recorded as "filtered" and permanently
#     skipped — they will never match our criteria.
#   • Apps where detail fetch fails are recorded as "no_detail" and skipped.
#   • global_seen_ids now ONLY tracks qualified leads (email found + filtered).
#   • Automation continues cycling through new keywords until target is hit.
# ══════════════════════════════════════════════════════════════════════════════

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

SEARCH_COMBOS = [
    ("en", "us"), ("en", "gb"), ("en", "in"), ("en", "au"), ("en", "ca"),
]

HUNTER_SEARCH_COMBOS = [
    ("en", "us"), ("en", "gb"), ("en", "au"), ("en", "ca"), ("en", "nz"),
    ("en", "sg"), ("en", "za"), ("en", "ng"), ("en", "gh"), ("en", "ke"),
    ("en", "ph"), ("en", "my"), ("en", "in"),
]

# Parallel detail fetch workers — 5 is fast yet safe from rate-limits
DETAIL_WORKERS = 5


def extract_email(text: str) -> str:
    if not text: return ""
    m = EMAIL_RE.search(str(text))
    return m.group(0) if m else ""


def _passes_normal(score_raw, ratings_count: int, installs: int) -> bool:
    """
    Normal mode — strictly brand new apps only.
    • Play Store score must be None OR exactly 0.0 (no rating published yet)
    • ratings_count must be 0 (no reviews at all)
    • installs: 500–5000
    Uses raw score directly from Play Store — no conversion applied beforehand.
    """
    if score_raw is not None and float(score_raw) > 0:
        return False
    if ratings_count > 0:
        return False
    if installs < 500 or installs > 5_000:
        return False
    return True


def _passes_hunter(score_raw, installs: int, max_score: float, max_inst: int) -> bool:
    """
    Hunter mode — struggling rated apps only.
    • Must have a REAL positive score from Play Store (score > 0)
    • score ≤ max_score
    • installs: 100–max_inst
    Uses raw score directly — never zeroed or converted.
    """
    if score_raw is None:
        return False
    try:
        s = float(score_raw)
    except (TypeError, ValueError):
        return False
    if s <= 0:              return False
    if s > max_score:       return False
    if installs < 100:      return False
    if installs > max_inst: return False
    return True


def _collect_app_ids_for_keyword(keyword: str, combos: list) -> list:
    """
    Step 1: Search across all country combos, collect unique app IDs.
    Returns a deduplicated list of app IDs (strings).
    """
    seen_in_search = set()
    app_ids = []

    combos_shuffled = list(combos)
    random.shuffle(combos_shuffled)

    for lang, country in combos_shuffled:
        if stop_event.is_set():
            break

        results = safe_search(keyword, lang=lang, country=country, n_hits=250)
        new_ids = 0
        for item in results:
            aid = item.get("appId", "")
            if aid and aid not in seen_in_search:
                seen_in_search.add(aid)
                app_ids.append(aid)
                new_ids += 1

        push_log(f"  [{country}] {len(results)} results, {new_ids} new IDs (pool: {len(app_ids)})")

        if stop_event.wait(random.uniform(1.0, 2.5)):
            break

    push_log(f"  Pool size for '{keyword}': {len(app_ids)} unique app IDs")
    return app_ids


def _fetch_one_detail(app_id: str) -> tuple[str, dict | None]:
    """Fetch details for one app. Returns (app_id, details_or_None)."""
    time.sleep(random.uniform(0.1, 0.6))
    details = safe_app_detail(app_id)
    return app_id, details


def _process_pool(app_ids: list, keyword: str, hunter: dict,
                  keyword_tokens: list) -> list:
    """
    Steps 2–6: Filter pool → parallel detail fetch → mode filter →
    dedup → record to Scraped Apps tab → collect qualified leads.

    Status values written to Scraped Apps:
      "qualified"  — passed all checks, has email → lead created
      "no_email"   — passed filter but no email found → will recheck next run
      "filtered"   — failed mode/relevance filter → skip permanently
      "no_detail"  — Play Store returned None → skip permanently
    """
    global global_seen_ids, global_seen_emails

    is_hunter = bool(hunter and hunter.get("active"))
    max_inst  = int(hunter.get("max_installs")  or 5000)  if is_hunter else 5000
    max_score = float(hunter.get("max_score")   or 2.5)   if is_hunter else 0.0
    leads     = []
    fetch_lock = threading.Lock()

    # ── Step 2: Remove already-permanently-processed app IDs ──────────────────
    # Also skip ones seen as qualified in THIS session (global_seen_ids).
    # "no_email" apps are NOT in sheet_scraped_ids, so they pass through.
    pre_filter_count = len(app_ids)
    to_fetch = [
        aid for aid in app_ids
        if aid not in global_seen_ids and not is_already_scraped(aid)
    ]
    skipped = pre_filter_count - len(to_fetch)
    if skipped:
        push_log(f"  Skipped {skipped} already-processed app IDs (DB + session)")
    push_log(f"  Fetching details for {len(to_fetch)} apps ({DETAIL_WORKERS} parallel workers)...")

    # Batch recording to sheet — collect and write after thread pool to avoid
    # hammering the Apps Script endpoint from multiple threads simultaneously.
    scrape_records: list[dict] = []   # [{app_id, app_name, status, ...}]
    scrape_records_lock = threading.Lock()

    def _record(app_id: str, app_name: str, status: str,
                installs: int = 0, score=None, category: str = ""):
        with scrape_records_lock:
            scrape_records.append({
                "app_id": app_id, "app_name": app_name, "status": status,
                "installs": installs, "score": score,
                "category": category, "keyword": keyword,
            })
        # Update in-memory cache immediately so later threads see it
        register_scraped(app_id, status)

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
        futures = {executor.submit(_fetch_one_detail, aid): aid for aid in to_fetch}

        for future in as_completed(futures):
            if stop_event.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
                break

            app_id, details = future.result()

            if details is None:
                # Play Store could not return detail — record and skip
                _record(app_id, app_id, "no_detail")
                continue

            # ── Raw values from Play Store ────────────────────────────────────
            installs      = details.get("minInstalls") or 0
            ratings_count = details.get("ratings")     or 0
            score_raw     = details.get("score")           # float | None
            app_name      = details.get("title", app_id)
            app_genre     = details.get("genre", "")
            app_desc      = details.get("description", "")

            # ── Mode filter ───────────────────────────────────────────────────
            if is_hunter:
                passes = _passes_hunter(score_raw, installs, max_score, max_inst)
            else:
                passes = _passes_normal(score_raw, ratings_count, installs)

            if not passes:
                _record(app_id, app_name, "filtered", installs, score_raw, app_genre)
                continue

            # ── Keyword relevance ─────────────────────────────────────────────
            if keyword_tokens and not is_keyword_relevant(app_name, app_genre, app_desc, keyword_tokens):
                _record(app_id, app_name, "filtered", installs, score_raw, app_genre)
                continue

            # ── Email extraction ──────────────────────────────────────────────
            email = (
                extract_email(details.get("developerEmail", ""))
                or extract_email(details.get("privacyPolicy", ""))
                or extract_email(details.get("description", ""))
                or extract_email(details.get("recentChanges", ""))
            )

            if not email:
                # Passed filter but no email — record for recheck, do NOT add
                # to global_seen_ids so future runs can find a newly added email.
                _record(app_id, app_name, "no_email", installs, score_raw, app_genre)
                push_log(f"  ⚠ no_email (recheck later): {app_name}")
                continue

            # ── Dedup against qualified leads (session + sheet) ───────────────
            if is_duplicate(app_id, email):
                push_log(f"  ⟳ dup: {app_name}")
                # Still record as "qualified" so we don't re-process next run
                _record(app_id, app_name, "qualified", installs, score_raw, app_genre)
                continue

            # ── All checks passed — build lead ────────────────────────────────
            register_seen(app_id, email)
            _record(app_id, app_name, "qualified", installs, score_raw, app_genre)

            lead = {
                "app_id":        app_id,
                "app_name":      app_name,
                "developer":     details.get("developer", "") or "",
                "email":         email,
                "category":      app_genre,
                "installs":      installs,
                "score":         score_raw,
                "ratings_count": ratings_count,
                "description":   (app_desc or "")[:300],
                "url":           f"https://play.google.com/store/apps/details?id={app_id}",
                "icon":          details.get("icon", ""),
                "keyword":       keyword,
                "scraped_at":    time.strftime("%Y-%m-%d %H:%M:%S"),
                "email_sent":    False,
            }

            with fetch_lock:
                leads.append(lead)

            mode_tag  = "HUNTER" if is_hunter else "NORMAL"
            score_str = f"{score_raw:.1f}★" if score_raw else "no-rating"
            push_log(
                f"  ✓ [{mode_tag}] {app_name} | "
                f"{installs:,} inst | {score_str} | {ratings_count} rev | {email}"
            )

    # ── Batch write all scrape records to sheet (sequential, outside thread pool) ──
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

    return leads


def scrape_keyword(keyword: str, hunter: dict = None) -> list:
    """
    Full scrape for one keyword:
      1. Collect app IDs across country combos (search phase)
      2. Filter already-processed IDs via Scraped Apps DB
      3. Fetch details in parallel + filter + dedup + record (process phase)
    """
    push_log(f"▶ Keyword: '{keyword}'")

    combos         = HUNTER_SEARCH_COMBOS if (hunter and hunter.get("active")) else SEARCH_COMBOS
    keyword_tokens = build_keyword_tokens(keyword)

    app_ids = _collect_app_ids_for_keyword(keyword, combos)
    if not app_ids:
        push_log(f"  No results for '{keyword}'")
        sheet_log_keyword(keyword, 0)
        return []

    leads = _process_pool(app_ids, keyword, hunter or {}, keyword_tokens)

    push_log(f"  '{keyword}' → {len(leads)} new leads from {len(app_ids)} apps checked")
    sheet_log_keyword(keyword, len(leads))
    return leads


# ── Email URL helpers ─────────────────────────────────────────────────────────
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


# ── Email send (quota-aware, multi-URL) ──────────────────────────────────────
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


# ── Cooldown / retry scheduler ────────────────────────────────────────────────
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


# ── Email sending loop ────────────────────────────────────────────────────────
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


# ── Master automation ─────────────────────────────────────────────────────────
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

    # Load both qualified-leads dedup cache AND scraped-apps DB
    load_sheet_duplicate_cache()

    base_subject = get_cfg("EMAIL_SUBJECT") or ""
    base_body    = get_cfg("EMAIL_BODY")    or ""

    reset_email_quotas(get_email_urls())

    all_leads = []
    kws_used  = [initial_kw]
    kw_queue  = [initial_kw]

    while len(all_leads) < target and not stop_event.is_set():
        if not kw_queue:
            push_log("Requesting AI keywords...")
            new_kws = ai_gen_keywords(initial_kw, kws_used, hunter)
            if not new_kws:
                push_log("No more keywords. Stopping scrape.")
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

        push_log(f"Total: {len(all_leads)} / {target}")

        if not stop_event.is_set() and (kw_queue or len(all_leads) < target):
            wait = random.uniform(3, 8)
            push_log(f"  Pausing {wait:.0f}s before next keyword...")
            stop_event.wait(wait)

    if stop_event.is_set():
        push_log("Stopped.")
        upd(running=False, phase="stopped")
        return

    push_log(f"Scraping done. {len(all_leads)} leads. Starting emails...")
    upd(phase="emailing")
    email_loop(all_leads, base_subject, base_body)

    if stop_event.is_set():
        upd(running=False, phase="stopped")
    else:
        push_log("✓ Automation complete!")
        upd(running=False, phase="done")


# ── Send pending ──────────────────────────────────────────────────────────────
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


# ── Common run_cfg builder ────────────────────────────────────────────────────
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
            return jsonify({"ok": True, "msg": f"Test sent to {test_to} [{ttype}]",
                            "template_type": ttype, "subject": subject, "body": body})
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
        r      = requests.post(sheet_url, json={"action": "get_pending"}, timeout=20)
        result = r.json() if r.text else {}
        return jsonify({"ok": True, "count": len(result.get("leads",[])), "leads": result.get("leads",[])})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@application.route("/api/sheet_all", methods=["POST"])
def api_sheet_all():
    data      = request.get_json(silent=True) or {}
    sheet_url = data.get("sheet_url") or os.environ.get("APPS_SCRIPT_WEB_URL", "")
    if not sheet_url:
        return jsonify({"error": "sheet_url not set"}), 400
    try:
        r      = requests.post(sheet_url, json={"action": "get_all_leads"}, timeout=20)
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
        subject, body = ai_gen_email(lead_data, get_cfg("EMAIL_SUBJECT") or "", get_cfg("EMAIL_BODY") or "")
        status, _     = send_email(lead_data, subject, body)
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

# ── NEW: Scraped Apps DB API endpoints ───────────────────────────────────────
@application.route("/api/scraped_stats", methods=["POST"])
def api_scraped_stats():
    """Return stats about the Scraped Apps DB tab."""
    data      = request.get_json(silent=True) or {}
    sheet_url = data.get("sheet_url") or os.environ.get("APPS_SCRIPT_WEB_URL", "")
    if not sheet_url:
        return jsonify({"error": "sheet_url not set"}), 400
    try:
        r    = requests.post(sheet_url, json={"action": "get_scraped_apps"}, timeout=30)
        rows = (r.json() if r.text else {}).get("apps", [])
        counts = {"qualified": 0, "no_email": 0, "filtered": 0, "no_detail": 0, "total": len(rows)}
        for row in rows:
            status = (row.get("Status") or "").strip().lower()
            if status in counts: counts[status] += 1
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
        r      = requests.post(sheet_url, json={"action": "clear_scraped_apps"}, timeout=20)
        result = r.json() if r.text else {}
        # Also clear in-memory scraped cache
        with sheet_cache_lock:
            sheet_scraped_ids.clear()
        push_log("Scraped Apps DB cleared.")
        return jsonify({"ok": True, "msg": result.get("msg", "Cleared")})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    application.run(host="0.0.0.0", port=port, debug=False)
