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

# ── Sheet duplicate cache — loaded once at automation start ───────────────────
# Pulled from the Google Sheet before scraping begins.
# Every new lead is checked here (O(1)) — no per-lead HTTP call needed.
sheet_known_ids:    set  = set()
sheet_known_emails: set  = set()
sheet_cache_loaded: bool = False
sheet_cache_lock         = threading.Lock()

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
# ── ANTI-BOT / SAFE REQUEST WRAPPERS
#
# Play Store aggressively rate-limits rapid scraping (HTTP 429 / connection
# drops). Two wrappers handle this transparently:
#   safe_search()      — wraps search() with retry + exponential backoff
#   safe_app_detail()  — wraps gp_app() with retry + exponential backoff
#
# Delays between individual detail fetches (0.3–1.0s) keep us fast while
# staying under the rate-limit threshold.
# ══════════════════════════════════════════════════════════════════════════════

def _is_rate_limit_error(e: Exception) -> bool:
    msg = str(e).lower()
    return "429" in msg or "too many" in msg or "rate" in msg or "blocked" in msg

def safe_search(keyword: str, lang: str, country: str, n_hits: int = 250, retries: int = 3):
    """search() with retry + exponential backoff on rate-limit errors."""
    for attempt in range(retries):
        try:
            results = search(keyword, lang=lang, country=country, n_hits=n_hits)
            return results or []
        except Exception as e:
            if _is_rate_limit_error(e):
                wait = (2 ** attempt) * random.uniform(5, 10)
                push_log(f"  Rate-limited [{country}] '{keyword}' — waiting {wait:.0f}s (attempt {attempt+1}/{retries})")
                if stop_event.wait(wait):
                    return []
            else:
                push_log(f"  Search error [{country}]: {e}")
                if stop_event.wait(2):
                    return []
    return []

def safe_app_detail(app_id: str, retries: int = 3):
    """gp_app() with retry + exponential backoff. Returns dict or None."""
    for attempt in range(retries):
        try:
            return gp_app(app_id, lang="en", country="us")
        except Exception as e:
            if _is_rate_limit_error(e):
                wait = (2 ** attempt) * random.uniform(4, 8)
                push_log(f"  Rate-limited fetching {app_id} — waiting {wait:.0f}s")
                if stop_event.wait(wait):
                    return None
            else:
                if attempt == retries - 1:
                    push_log(f"  Detail fetch failed {app_id}: {e}")
                if stop_event.wait(1):
                    return None
    return None


# ── Google Sheet via Apps Script ──────────────────────────────────────────────
def sheet_post(payload: dict):
    url = get_cfg("APPS_SCRIPT_WEB_URL")
    if not url:
        return None
    try:
        r = requests.post(url, json=payload, timeout=15)
        return r.json() if r.text else {}
    except Exception as e:
        push_log(f"  Sheet error: {e}")
        return None

def sheet_append_lead(lead: dict):
    sheet_post({"action": "append", "tab": "All Leads", "row": {
        "App Name":   lead["app_name"],  "Developer": lead["developer"],
        "Email":      lead["email"],     "Category":  lead["category"],
        "Installs":   lead["installs"],  "Score":     lead["score"] or "",
        "URL":        lead["url"],       "Keyword":   lead["keyword"],
        "Scraped At": lead["scraped_at"],"Email Sent":"No",
        "App ID":     lead["app_id"],
    }})

def sheet_append_qualified(lead: dict):
    sheet_post({"action": "append", "tab": "Qualified Leads", "row": {
        "App Name":   lead["app_name"],  "Developer": lead["developer"],
        "Email":      lead["email"],     "Category":  lead["category"],
        "Installs":   lead["installs"],  "Score":     lead["score"] or "",
        "URL":        lead["url"],       "Keyword":   lead["keyword"],
        "Scraped At": lead["scraped_at"],"Email Sent":"Pending",
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


# ══════════════════════════════════════════════════════════════════════════════
# ── SHEET DUPLICATE CACHE
#
# Loaded ONCE before each automation run starts. Fetches all existing leads
# from the sheet so we never re-scrape or re-email anything already there.
#
# is_duplicate()  — single call checks ALL layers (session + sheet)
# register_seen() — updates BOTH caches at once after a lead is collected
# ══════════════════════════════════════════════════════════════════════════════

def load_sheet_duplicate_cache():
    global sheet_known_ids, sheet_known_emails, sheet_cache_loaded

    with sheet_cache_lock:
        sheet_known_ids    = set()
        sheet_known_emails = set()
        sheet_cache_loaded = False

    url = get_cfg("APPS_SCRIPT_WEB_URL")
    if not url:
        push_log("  No sheet URL — duplicate cache skipped (in-memory dedup still active).")
        with sheet_cache_lock:
            sheet_cache_loaded = True
        return

    push_log("  Loading sheet for duplicate detection...")
    try:
        r      = requests.post(url, json={"action": "get_all_leads"}, timeout=30)
        result = r.json() if r.text else {}
        leads  = result.get("leads", [])

        with sheet_cache_lock:
            for lead in leads:
                aid = (lead.get("App ID") or lead.get("app_id") or "").strip()
                em  = (lead.get("Email")  or lead.get("email")  or "").strip().lower()
                if aid: sheet_known_ids.add(aid)
                if em:  sheet_known_emails.add(em)
            sheet_cache_loaded = True

        push_log(
            f"  Sheet cache ready — {len(sheet_known_ids)} existing app IDs, "
            f"{len(sheet_known_emails)} existing emails will be skipped automatically."
        )
    except Exception as e:
        push_log(f"  Could not load sheet cache: {e} — continuing without it.")
        with sheet_cache_lock:
            sheet_cache_loaded = True


def is_duplicate(app_id: str, email: str) -> bool:
    """
    Unified dedup check — covers all three layers in one call:
      Layer 1: global_seen_ids / global_seen_emails  (this session, in-memory)
      Layer 2: sheet_known_ids / sheet_known_emails  (pre-loaded from sheet)

    Returns True if this app_id OR email has been seen before in either layer.
    Both app_id and email are checked so we catch:
      - Same app re-scraped via a different keyword
      - Same developer email attached to a different app
    """
    email_lower = email.lower()
    if app_id in global_seen_ids or email_lower in global_seen_emails:
        return True
    with sheet_cache_lock:
        if app_id in sheet_known_ids or email_lower in sheet_known_emails:
            return True
    return False


def register_seen(app_id: str, email: str):
    """
    Mark app_id + email as seen in both session cache and sheet cache
    simultaneously. Called immediately after a lead passes all filters.
    Ensures later keywords in the same run don't re-collect the same lead.
    """
    global_seen_ids.add(app_id)
    global_seen_emails.add(email.lower())
    with sheet_cache_lock:
        sheet_known_ids.add(app_id)
        sheet_known_emails.add(email.lower())


# ── Keyword relevance check ───────────────────────────────────────────────────
def build_keyword_tokens(keyword: str) -> list:
    STOP = {
        "app","apps","application","tool","tools","simple","easy","free","best",
        "top","new","lite","basic","mini","pro","plus","helper","utility","tracker",
        "manager","monitor","the","a","an","for","of","in","on","and","or","with",
        "offline","local","online","mobile","android","google","play","store",
        "indie","micro","community","startup","software","platform","service",
    }
    tokens = re.findall(r"[a-z]+", keyword.lower())
    return [t for t in tokens if t not in STOP and len(t) > 2]


def is_keyword_relevant(app_title: str, app_genre: str, app_desc: str,
                         keyword_tokens: list) -> bool:
    """
    RELAXED relevance check — any ONE token matching anywhere in
    title, genre, or first 400 chars of description is enough to pass.
    If keyword has no meaningful tokens everything passes automatically.
    """
    if not keyword_tokens:
        return True
    combined = " ".join([
        (app_title or "").lower(),
        (app_genre or "").lower(),
        (app_desc  or "")[:400].lower(),
    ])
    return any(t in combined for t in keyword_tokens)


# ── AI keyword generation ─────────────────────────────────────────────────────
def ai_gen_keywords(original: str, used: list, hunter: dict = None) -> list:
    key = get_cfg("GROQ_API_KEY")
    if not key:
        push_log("GROQ_API_KEY not set — using built-in keyword expansion")
        return _fallback_keywords(original, used)

    client    = Groq(api_key=key)
    used_str  = ", ".join(used[:30]) if used else "none"
    is_hunter = bool(hunter and hunter.get("active"))

    if is_hunter:
        max_inst  = int(hunter.get("max_installs") or 5000)
        max_score = float(hunter.get("max_score") or 2.5)
        prompt = (
            f"You are a Google Play Store keyword expert.\n"
            f"MAIN TOPIC/NICHE: \"{original}\"\n"
            f"Already used (DO NOT repeat): {used_str}\n\n"
            f"GOAL: Find STRUGGLING apps in the '{original}' niche "
            f"with fewer than {max_inst:,} installs and rating ≤ {max_score}.\n\n"
            f"RULES:\n"
            f"1. Generate 15 specific keyword phrases (2–4 words each)\n"
            f"2. ALL keywords MUST stay within the '{original}' topic\n"
            f"3. Think: sub-features, specific tools, niche variants\n"
            f"4. Prefer long-tail / obscure keywords — they surface smaller apps\n"
            f"5. NEVER jump to unrelated niches. Do NOT repeat used keywords.\n"
            f"Return ONLY a JSON array of strings. No explanation."
        )
    else:
        prompt = (
            f"You are a Google Play Store keyword expert.\n"
            f"MAIN TOPIC/NICHE: '{original}'\n"
            f"Already used: {used_str}\n\n"
            f"Generate 15 NEW keyword phrases for the '{original}' niche "
            f"targeting BRAND NEW apps (zero reviews, under 5000 installs).\n\n"
            f"RULES:\n"
            f"1. ALL keywords must stay within '{original}' niche\n"
            f"2. Cover sub-features, use-cases, niche variants\n"
            f"3. Long-tail / obscure keywords preferred\n"
            f"4. NEVER jump to unrelated topics. Do NOT repeat used keywords.\n"
            f"Return ONLY a JSON array of strings, nothing else."
        )

    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7, max_tokens=600
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"```[a-z]*", "", raw).replace("```", "").strip()
        kws = json.loads(raw)
        new_kws = [str(k).strip() for k in kws if str(k).strip() not in used]
        push_log(f"  AI keywords ({len(new_kws)}): {new_kws}")
        if len(new_kws) < 4:
            push_log("  AI returned few keywords — adding fallback")
            new_kws.extend(_fallback_keywords(original, used + new_kws))
        return new_kws
    except Exception as e:
        push_log(f"  AI keyword error: {e} — using fallback")
        return _fallback_keywords(original, used)


def _fallback_keywords(original: str, used: list) -> list:
    base = original.lower().strip()
    suffixes = ["lite","simple","basic","mini","micro","offline","local","tracker",
                "logger","monitor","ledger","tool","helper","assistant","companion",
                "free","dashboard","manager","diary","notes","record","log"]
    prefixes = ["simple","easy","offline","local","micro","indie","basic","personal",
                "free","quick","smart","tiny","pocket","handy","my","daily"]
    candidates = []
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
                if c not in used and len(c) > 4:
                    candidates.append(c)
    return candidates[:15]


# ── Dual-template defaults ────────────────────────────────────────────────────
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


# ── Score formatting ──────────────────────────────────────────────────────────
def format_score(score) -> str:
    if score is None or score == "" or score == 0:
        return ""
    try:
        val = float(score)
        return f"{val:.1f}" if val > 0 else ""
    except (TypeError, ValueError):
        return ""


# ── Template selector ─────────────────────────────────────────────────────────
def select_template(lead: dict, base_subject: str = "", base_body: str = "") -> tuple:
    has_rating = bool(format_score(lead.get("score")))
    if has_rating:
        subject = get_cfg("OLD_APP_EMAIL_SUBJECT") or base_subject or DEFAULT_OLD_APP_SUBJECT
        body    = get_cfg("OLD_APP_EMAIL_BODY")    or base_body    or DEFAULT_OLD_APP_BODY
    else:
        subject = get_cfg("NEW_APP_EMAIL_SUBJECT") or base_subject or DEFAULT_NEW_APP_SUBJECT
        body    = get_cfg("NEW_APP_EMAIL_BODY")    or base_body    or DEFAULT_NEW_APP_BODY
    return subject, body


# ── Personalization engine ────────────────────────────────────────────────────
def personalize_template(tpl: str, lead: dict) -> str:
    sender_name    = get_cfg("SENDER_NAME", "Your Name")
    sender_company = get_cfg("SENDER_COMPANY", "Your Company")
    score_fmt      = format_score(lead.get("score"))
    installs_raw   = lead.get("installs")
    try:
        installs_str = f"{int(installs_raw):,}" if installs_raw else "growing app"
    except (TypeError, ValueError):
        installs_str = str(installs_raw)

    filled = (tpl
        .replace("{{app_name}}",       lead.get("app_name", ""))
        .replace("{{developer}}",      lead.get("developer", "") or "")
        .replace("{{category}}",       lead.get("category", "") or "app")
        .replace("{{installs}}",       installs_str)
        .replace("{{score}}",          score_fmt)
        .replace("{{url}}",            lead.get("url", ""))
        .replace("{{sender_name}}",    sender_name)
        .replace("{{sender_company}}", sender_company)
    )
    return re.sub(r"\{\{[a-zA-Z_]+\}\}", "", filled)


def fill_template(tpl: str, lead: dict) -> str:
    return personalize_template(tpl, lead)


# ── AI email generation ───────────────────────────────────────────────────────
def ai_gen_email(lead: dict, base_subject: str, base_body: str):
    key = get_cfg("GROQ_API_KEY")
    sender_name    = get_cfg("SENDER_NAME", "Your Name")
    sender_company = get_cfg("SENDER_COMPANY", "Your Company")

    tpl_subject, tpl_body = select_template(lead, base_subject, base_body)

    if not key:
        return personalize_template(tpl_subject, lead), personalize_template(tpl_body, lead)

    client        = Groq(api_key=key)
    score_fmt     = format_score(lead.get("score"))
    score_info    = f"{score_fmt} stars" if score_fmt else "no ratings yet (brand new)"
    install_info  = f"{lead['installs']:,} installs" if lead.get("installs") else "just launched"
    prefilled_sub = personalize_template(tpl_subject, lead)
    prefilled_bod = personalize_template(tpl_body, lead)
    ttype         = "OLD APP (has rating)" if score_fmt else "NEW APP (no rating)"

    prompt = f"""You are a cold email personalizer. Fill in the template with real app details. Keep structure identical.

TEMPLATE TYPE: {ttype}
Subject: {tpl_subject}
Body:
{tpl_body}

APP: {lead.get('app_name','')} | Dev: {lead.get('developer','')} | Cat: {lead.get('category','app')}
Installs: {install_info} | Rating: {score_info} | URL: {lead.get('url','')}
Sender: {sender_name}, {sender_company}

RULES:
1. Same structure/sentences — change at most 2-3 words to fit the app
2. Replace ALL {{{{variable}}}} placeholders. score="{score_fmt or 'N/A'}", installs="{install_info}"
3. Preserve every line break. Use \\n for newlines in JSON
4. NEVER leave any {{{{variable}}}} in output
5. Return ONLY valid JSON: {{"subject":"...","body":"..."}}"""

    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3, max_tokens=600
        )
        raw  = resp.choices[0].message.content.strip()
        raw  = re.sub(r"```[a-z]*", "", raw).replace("```", "").strip()
        data = json.loads(raw)
        sub  = data.get("subject") or prefilled_sub
        bod  = (data.get("body") or prefilled_bod).replace("\\n", "\n")
        sub  = re.sub(r"\{\{[a-zA-Z_]+\}\}", "", sub)
        bod  = re.sub(r"\{\{[a-zA-Z_]+\}\}", "", bod)
        return sub, bod
    except Exception as e:
        push_log(f"  AI email error (template fallback): {e}")
        return prefilled_sub, prefilled_bod


# ══════════════════════════════════════════════════════════════════════════════
# ── PLAY STORE SCRAPER
#
# ROOT CAUSE ANALYSIS — why leads were not arriving:
#
# BUG 1 — score=0.0 destroyed Hunter mode leads
#   Original code: score=0.0 was converted to None BEFORE passes_filter().
#   Hunter mode requires score > 0, so ALL rated apps returning 0.0 from the
#   scraper were silently converted to None then rejected.
#   FIX: score kept raw. Only Normal mode converts 0.0 → None (correct there).
#
# BUG 2 — Relevance filter was blocking valid apps
#   Old check: required token match but was applied even on generic keywords.
#   Many valid apps don't use the exact keyword in their title/description.
#   FIX: Relaxed to "any one token in title+genre+first-400-chars-of-desc".
#   If keyword yields ≤1 meaningful token, the check is skipped entirely.
#
# BUG 3 — n_hits=500 triggering bot detection
#   Very high n_hits → many pagination sub-requests → Play Store blocks the IP.
#   FIX: Reduced to 250. safe_search() handles 429s with exponential backoff.
#
# BUG 4 — No delay between gp_app() calls
#   Hammering detail fetches in a tight loop causes IP rate-limiting.
#   FIX: 0.3–1.0s random sleep between each detail fetch — fast but safe.
#
# BUG 5 — Normal mode install cap was 10k (not 5k as required)
#   FIX: Hard cap changed to 5,000.
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


def extract_email(text: str) -> str:
    if not text:
        return ""
    m = EMAIL_RE.search(str(text))
    return m.group(0) if m else ""


def passes_filter(installs: int, score, ratings_count: int, hunter: dict) -> bool:
    """
    ╔══════════════════════════════════════════════════════════════════╗
    ║  NORMAL MODE — Brand-new apps only (zero reviews)               ║
    ║  • score must be None / 0.0 (no rating on Play Store)           ║
    ║  • ratings_count must be 0                                      ║
    ║  • installs: 500 – 5,000                                        ║
    ╠══════════════════════════════════════════════════════════════════╣
    ║  HUNTER MODE — Struggling rated apps                            ║
    ║  • score must be a real positive number (> 0)                   ║
    ║    Raw score is kept as-is — NOT zeroed before this check       ║
    ║  • score ≤ max_score  (default 2.5)                             ║
    ║  • 100 ≤ installs ≤ max_installs  (default 5,000)               ║
    ║  • Unrated / zero-score apps are EXCLUDED                       ║
    ╚══════════════════════════════════════════════════════════════════╝
    """
    if hunter and hunter.get("active"):
        max_inst  = int(hunter.get("max_installs") or 5000)
        max_score = float(hunter.get("max_score") or 2.5)

        if score is None or float(score) <= 0.0:
            return False
        if float(score) > max_score:
            return False
        if installs < 100 or installs > max_inst:
            return False
        return True

    # Normal mode — zero-review brand-new apps only
    if score is not None and float(score) > 0:
        return False
    if ratings_count > 0:
        return False
    if installs < 500 or installs > 5_000:
        return False
    return True


def scrape_keyword(keyword: str, hunter: dict = None) -> list:
    """
    Scrapes one keyword across country combos.

    Dedup (single is_duplicate() call covers session + sheet):
      After passing all filters, register_seen() updates both caches so
      later keywords in the same run never re-collect the same app/email.

    Speed vs. anti-bot:
      • 0.3–1.0s between gp_app() calls (fast but avoids hammering)
      • safe_search / safe_app_detail handle 429s with backoff
      • Country combos shuffled each run (no predictable pattern)
      • 1–3s pause between countries
    """
    global global_seen_ids, global_seen_emails

    push_log(f"▶ Scraping: '{keyword}'")
    leads = []

    combos         = HUNTER_SEARCH_COMBOS if (hunter and hunter.get("active")) else SEARCH_COMBOS
    keyword_tokens = build_keyword_tokens(keyword)

    combos_shuffled = list(combos)
    random.shuffle(combos_shuffled)

    for lang, country in combos_shuffled:
        if stop_event.is_set():
            break

        results = safe_search(keyword, lang=lang, country=country, n_hits=250)
        if not results:
            continue

        push_log(f"  [{country}] {len(results)} raw results")

        for item in results:
            if stop_event.is_set():
                break

            app_id = item.get("appId", "")
            if not app_id or app_id in global_seen_ids:
                continue

            # Small delay between detail fetches — keeps under rate-limit
            time.sleep(random.uniform(0.3, 1.0))

            details = safe_app_detail(app_id)
            if details is None:
                continue

            installs      = details.get("minInstalls") or 0
            ratings_count = details.get("ratings") or 0
            raw_score     = details.get("score")      # float | None from scraper

            # ── Score handling (CRITICAL FIX) ─────────────────────────────
            # Hunter mode → keep raw_score exactly as returned.
            #   passes_filter() will reject it if score <= 0 or > max_score.
            #   We must NOT zero it out here or Hunter mode gets no leads.
            #
            # Normal mode → convert 0.0 → None.
            #   Play Store sometimes returns score=0.0 for brand-new apps
            #   instead of None. Treating 0.0 as None is correct here so
            #   the "no rating" check in passes_filter() works properly.
            if hunter and hunter.get("active"):
                score = raw_score
            else:
                score = None if (raw_score is None or float(raw_score or 0) == 0.0) else raw_score

            if not passes_filter(installs, score, ratings_count, hunter):
                continue

            app_name  = details.get("title", "")
            app_genre = details.get("genre", "")
            app_desc  = details.get("description", "")

            # Relaxed relevance check — any one token match is enough
            if keyword_tokens and not is_keyword_relevant(app_name, app_genre, app_desc, keyword_tokens):
                global_seen_ids.add(app_id)
                continue

            # Extract email from all available fields
            email = (
                extract_email(details.get("developerEmail", ""))
                or extract_email(details.get("privacyPolicy", ""))
                or extract_email(details.get("description", ""))
                or extract_email(details.get("recentChanges", ""))
            )
            if not email:
                global_seen_ids.add(app_id)
                continue

            # Unified dedup check (session + sheet, O(1))
            if is_duplicate(app_id, email):
                push_log(f"  ⟳ dup: {app_name}")
                global_seen_ids.add(app_id)
                continue

            # Register in BOTH caches before appending
            register_seen(app_id, email)

            lead = {
                "app_id":        app_id,
                "app_name":      app_name,
                "developer":     details.get("developer", "") or "",
                "email":         email,
                "category":      app_genre,
                "installs":      installs,
                "score":         score,
                "ratings_count": ratings_count,
                "description":   (app_desc or "")[:300],
                "url":           f"https://play.google.com/store/apps/details?id={app_id}",
                "icon":          details.get("icon", ""),
                "keyword":       keyword,
                "scraped_at":    time.strftime("%Y-%m-%d %H:%M:%S"),
                "email_sent":    False,
            }
            leads.append(lead)

            mode_tag  = "HUNTER" if (hunter and hunter.get("active")) else "NORMAL"
            score_str = f"{score:.1f}★" if score else "no-rating"
            push_log(
                f"  ✓ [{mode_tag}] {app_name} | "
                f"{installs:,} inst | {score_str} | {ratings_count} rev | {email}"
            )

        push_log(f"  [{country}] done — leads so far: {len(leads)}")

        # Brief pause between countries
        if stop_event.wait(random.uniform(1.0, 3.0)):
            break

    push_log(f"  '{keyword}' → {len(leads)} new leads")
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
                push_log("  Email URL deployed incorrectly (must be 'Execute as: Me', 'Access: Anyone').")
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
        push_log("  All email scripts have hit Google's daily limit.")
        return "quota", "All URLs exhausted"

    push_log("  All email scripts failed for this lead.")
    return "error", "All URLs failed"


# ── Cooldown / retry scheduler ────────────────────────────────────────────────
COOLDOWN_SECONDS = 3600

def _is_automation_running() -> bool:
    with state_lock:
        return state.get("running", False)

def _cancel_cooldown_retry():
    global cooldown_retry_thread
    cooldown_retry_cancel.set()
    cooldown_retry_thread = None

def _schedule_email_retry(leads_to_send: list, base_subject: str, base_body: str):
    global global_cooldown_until

    cooldown_retry_cancel.clear()
    with email_state_lock:
        global_cooldown_until = time.time() + COOLDOWN_SECONDS

    push_log("  Email cooldown started. Will retry in 1 hour.")

    for _ in range(COOLDOWN_SECONDS):
        if cooldown_retry_cancel.is_set():
            push_log("  Email cooldown retry cancelled.")
            with email_state_lock:
                global_cooldown_until = 0.0
            return
        time.sleep(1)

    with email_state_lock:
        global_cooldown_until = 0.0

    if _is_automation_running():
        push_log("  Cooldown over, but automation is running — skipping auto-resume.")
        return

    push_log("  Cooldown over. Resetting quota flags and retrying emails...")
    urls = get_email_urls()
    reset_exhausted_urls(urls)

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
            push_log("  Limits still exhausted. Re-entering cooldown...")
            global cooldown_retry_thread
            cooldown_retry_thread = threading.Thread(
                target=_schedule_email_retry,
                args=(leads_to_send[i:], base_subject, base_body), daemon=True
            )
            cooldown_retry_thread.start()
            return
        if i < len(leads_to_send) - 1:
            wait = random.uniform(30, 60)
            push_log(f"  Waiting {wait:.0f}s ...")
            if stop_event.wait(wait):
                break

    push_log(f"  Retry complete. {sent} additional emails sent.")


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
            push_log(f"  Send failed for {lead['app_name']}. Moving to next...")
            i += 1

        if stop_event.is_set():
            return
        if i < len(pending):
            wait = random.uniform(30, 60)
            push_log(f"  Waiting {wait:.0f}s before next email ({i}/{total} done)")
            if stop_event.wait(wait):
                return

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

        # Short inter-keyword pause (anti-bot, not too slow)
        if not stop_event.is_set() and kw_queue:
            wait = random.uniform(4, 10)
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
        "score":     sample_score,   "email":    test_to,
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
        r      = requests.post(sheet_url, json={"action": "get_all_leads"}, timeout=20)
        result = r.json() if r.text else {}
        leads  = result.get("leads", [])
        return jsonify({"ok": True, "leads": leads})
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    application.run(host="0.0.0.0", port=port, debug=False)
