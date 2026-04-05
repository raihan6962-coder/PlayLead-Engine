import os, time, random, threading, json, re, logging
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from google_play_scraper import search, app as gp_app
from groq import Groq
import requests

# ── Flask setup ───────────────────────────────────────────────────────────────
application = Flask(__name__, static_folder=".")
app = application          # gunicorn looks for `app` OR `application`
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

# ── Runtime config (set per-run from dashboard or env) ────────────────────────
run_cfg = {}

def get_cfg(key, fallback=""):
    return run_cfg.get(key) or os.environ.get(key, fallback)

# ── Logging ───────────────────────────────────────────────────────────────────
def push_log(msg: str):
    with state_lock:
        state["logs"].append({"time": time.strftime("%H:%M:%S"), "msg": msg})
        if len(state["logs"]) > 400:
            state["logs"] = state["logs"][-400:]
    log.info(msg)

def upd(**kw):
    with state_lock:
        state.update(kw)

# ── Google Sheet via Apps Script ───────────────────────────────────────────────
def sheet_append(row: dict):
    url = get_cfg("APPS_SCRIPT_WEB_URL")
    if not url:
        push_log("⚠️  Apps Script URL not set — skipping sheet write")
        return
    try:
        requests.post(url, json={"action": "append", "row": row}, timeout=15)
        push_log(f"  📊 Sheet updated → {row.get('app_name','')}")
    except Exception as e:
        push_log(f"  ⚠️  Sheet error: {e}")

# ── AI keyword generation via Groq ────────────────────────────────────────────
def ai_gen_keywords(original: str, used: list) -> list:
    key = get_cfg("GROQ_API_KEY")
    if not key:
        push_log("⚠️  GROQ_API_KEY not set — cannot generate keywords")
        return []
    client = Groq(api_key=key)
    used_str = ", ".join(used) if used else "none"
    prompt = (
        f"You are a Google Play Store keyword expert.\n"
        f"Original keyword: '{original}'\n"
        f"Already used: {used_str}\n"
        f"Generate 5 NEW semantically similar Play Store search keywords. "
        f"Return ONLY a JSON array of strings, nothing else."
    )
    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7, max_tokens=200
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"```[a-z]*", "", raw).replace("```", "").strip()
        kws = json.loads(raw)
        push_log(f"🤖 AI keywords: {kws}")
        return [k for k in kws if k not in used]
    except Exception as e:
        push_log(f"🤖 AI error: {e}")
        return []

# ── Play Store scraper ────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

def extract_email(text):
    if not text:
        return ""
    m = EMAIL_RE.search(str(text))
    return m.group(0) if m else ""

def scrape_keyword(keyword: str, seen_ids: set) -> list:
    push_log(f"🔍 Scraping Play Store → '{keyword}'")
    leads = []
    try:
        results = search(keyword, lang="en", country="us", n_hits=250)
    except Exception as e:
        push_log(f"  ❌ Search error: {e}")
        return leads

    for item in results:
        if stop_event.is_set():
            break
        app_id = item.get("appId", "")
        if not app_id or app_id in seen_ids:
            continue
        try:
            details = gp_app(app_id, lang="en", country="us")
        except Exception:
            continue

        installs = details.get("minInstalls") or 0
        score    = details.get("score")
        ratings  = details.get("ratings") or 0
        if installs < 1000 or installs > 5000:
            continue
        if score is None or score >= 3.0:
            continue
        if ratings < 3:
            continue

        email = (
            extract_email(details.get("developerEmail", ""))
            or extract_email(details.get("privacyPolicy", ""))
        )
        if not email:
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
        seen_ids.add(app_id)
        push_log(f"  ✅ {lead['app_name']} ({email})")
        time.sleep(0.4)

    push_log(f"  📦 {len(leads)} new leads for '{keyword}'")
    return leads

# ── Email outreach via Apps Script ───────────────────────────────────────────
def build_email_body(lead: dict) -> str:
    sender_name    = get_cfg("SENDER_NAME", "Your Name")
    sender_company = get_cfg("SENDER_COMPANY", "Your Company")
    return (
        f"Hi {lead['developer']} team,\n\n"
        f"I came across {lead['app_name']} on Google Play and I'm genuinely impressed "
        f"with what you've built in the {lead['category']} space.\n\n"
        f"I wanted to reach out personally — I run a Play Store review recovery service "
        f"that helps app developers quickly recover from negative review spikes, protect "
        f"their ratings, and improve their Play Store presence.\n\n"
        f"Would you be open to a quick 15-minute call this week to see if there's a fit?\n\n"
        f"Best regards,\n{sender_name}\n{sender_company}\n\nApp: {lead['url']}"
    )

def send_email(lead: dict) -> bool:
    email_script_url = get_cfg("EMAIL_SCRIPT_URL")   # separate Apps Script for email
    if not email_script_url:
        push_log("⚠️  EMAIL_SCRIPT_URL not set — skipping email")
        return False
    try:
        payload = {
            "to":      lead["email"],
            "subject": f"Quick question about {lead['app_name']} 🚀",
            "body":    build_email_body(lead),
        }
        r = requests.post(email_script_url, json=payload, timeout=30)
        result = r.json() if r.text else {}
        if result.get("status") == "ok":
            push_log(f"  📧 Sent → {lead['email']} ({lead['app_name']})")
            return True
        else:
            push_log(f"  ❌ Email failed → {lead['email']}: {result.get('msg','unknown')}")
            return False
    except Exception as e:
        push_log(f"  ❌ Email error → {lead['email']}: {e}")
        return False

# ── Master automation ─────────────────────────────────────────────────────────
def run_automation(initial_kw: str, target: int):
    upd(running=True, phase="scraping", keyword=initial_kw,
        keywords_used=[], leads_found=0, emails_sent=0, logs=[], leads=[])
    stop_event.clear()
    push_log(f"🚀 Started | keyword: '{initial_kw}' | target: {target}")

    all_leads   = []
    seen_ids    = set()
    kws_used    = [initial_kw]
    kw_queue    = [initial_kw]

    # ── PHASE 1: SCRAPE ───────────────────────────────────────────────────────
    while len(all_leads) < target and not stop_event.is_set():
        if not kw_queue:
            push_log("🤖 Requesting AI keywords …")
            new_kws = ai_gen_keywords(initial_kw, kws_used)
            if not new_kws:
                push_log("⚠️  No more keywords available. Stopping scrape.")
                break
            kw_queue.extend(new_kws)

        kw = kw_queue.pop(0)
        if kw not in kws_used:
            kws_used.append(kw)
        upd(keywords_used=kws_used[:], phase="scraping")

        batch = scrape_keyword(kw, seen_ids)
        all_leads.extend(batch)
        upd(leads_found=len(all_leads), leads=[l.copy() for l in all_leads])

        for lead in batch:
            sheet_append(lead)

        push_log(f"📊 Leads: {len(all_leads)} / {target}")

    if stop_event.is_set():
        push_log("🛑 Stopped during scraping.")
        upd(running=False, phase="stopped")
        return

    push_log(f"✅ Scraping done. Total leads: {len(all_leads)}")

    # ── PHASE 2: EMAIL ────────────────────────────────────────────────────────
    upd(phase="emailing")
    push_log("📬 Starting email outreach …")

    for i, lead in enumerate(all_leads):
        if stop_event.is_set():
            push_log("🛑 Stopped during email phase.")
            break

        ok = send_email(lead)
        lead["email_sent"] = ok
        with state_lock:
            if ok:
                state["emails_sent"] += 1
            state["leads"] = [l.copy() for l in all_leads]

        sheet_append({**lead, "phase": "email_done"})

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
        "GROQ_API_KEY":        data.get("groq_key")        or os.environ.get("GROQ_API_KEY", ""),
        "APPS_SCRIPT_WEB_URL": data.get("sheet_url")       or os.environ.get("APPS_SCRIPT_WEB_URL", ""),
        "EMAIL_SCRIPT_URL":    data.get("email_script_url") or os.environ.get("EMAIL_SCRIPT_URL", ""),
        "SENDER_NAME":         data.get("sender_name")      or os.environ.get("SENDER_NAME", ""),
        "SENDER_COMPANY":      data.get("sender_company")   or os.environ.get("SENDER_COMPANY", ""),
    }
    target = int(data.get("target") or os.environ.get("TARGET_LEADS", 300))

    threading.Thread(target=run_automation, args=(keyword, target), daemon=True).start()
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    application.run(host="0.0.0.0", port=port, debug=False)
