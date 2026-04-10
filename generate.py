#!/usr/bin/env python3
"""
ME Campaign Performance Review — Auto-generating dashboard.
Pulls data from Google Sheets, runs funnel + T2P + school board analysis,
and writes index.html for GitHub Pages.
Runs daily at 12 PM IST via GitHub Actions.
"""

import json, os, sys
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Config ──────────────────────────────────────────────────────────
LEAD_SHEET = "145i4wIlnQN6yxJQiMO_GLq1li2WSbUW4jvHhQGJPqP4"
BOARD_SHEET = "1bEhKuEXffrbRXe-5ZrUoXxYlqJdtw3YHcOtIviw4Thk"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# FY27 targets
TARGET_QL_TD = 45
TARGET_T2P = 35
TARGET_ABV = 85000
TARGET_LTV_CAC = 3.0

# ── Auth ────────────────────────────────────────────────────────────
def get_sheets_service():
    sa_json = os.environ.get("GOOGLE_SA_KEY")
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        # Local development fallback
        key_path = os.path.expanduser("~/.secrets/google-service-account.json")
        creds = service_account.Credentials.from_service_account_file(key_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def fetch_sheet(service, spreadsheet_id, range_str):
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_str
    ).execute()
    return result.get("values", [])

# ── Helpers ─────────────────────────────────────────────────────────
def sg(row, idx):
    return row[idx].strip() if idx < len(row) and row[idx] else ""

def safe_int(val):
    try: return int(val)
    except: return 0

def safe_float(val):
    try: return float(str(val).replace(",", ""))
    except: return 0.0

def pct(num, den):
    return num / den * 100 if den else 0

def fmt_pct(num, den):
    v = pct(num, den)
    return f"{v:.0f}%"

def sig_color(val, green_above=30, red_below=20):
    if val >= green_above: return "g"
    if val < red_below: return "r"
    return "y"

def sig_span(val, green_above=30, red_below=20):
    c = sig_color(val, green_above, red_below)
    return f'<span class="sig {c}">{val:.0f}%</span>'

def dot(color):
    return f'<span class="dot {color}"></span>'

def badge(text, cls):
    return f'<span class="badge {cls}">{text}</span>'

# ── Data normalization ──────────────────────────────────────────────
def channel_group(ch):
    ch = ch.lower()
    if "meta" in ch: return "Meta"
    if "google_brand" in ch: return "Google Brand"
    if "google_other" in ch or "google" in ch: return "Google Other"
    if "referral" in ch: return "Referrals"
    if "organic" in ch: return "Organic"
    return "Others"

def channel_group_broad(ch):
    ch = ch.lower()
    if "meta" in ch: return "Meta"
    if "google" in ch: return "Google"
    if "referral" in ch: return "Referrals"
    if "organic" in ch: return "Organic"
    return "Others"

def classify_stage(stage):
    s = stage.lower()
    if any(x in s for x in ["enrolled", "onboarding done", "onboarding query"]): return "Paid"
    if any(x in s for x in ["payment pending", "payment interested", "payment fpt"]): return "Pipeline"
    return "Lost"

def norm_reason(r):
    r = r.strip(); rl = r.lower()
    if not r: return "(No reason logged)"
    if "fee high" in rl: return "Fee High"
    if "paid" == rl: return "Paid"
    if "non responsive" in rl or "rnr" in rl: return "Non-Responsive"
    if "decision pending" in rl: return "Decision Pending"
    if "didn" in rl and "trial" in rl: return "Didn't Like Trial"
    if "invalid" in rl: return "Invalid Trial"
    if "child" in rl and "young" in rl: return "Child Too Young"
    if "interested to enrol" in rl or "interested to pay" in rl: return "Interested to Enrol"
    if "will enrol" in rl: return "Will Enrol Later"
    if "war" in rl or "travel" in rl: return "War/Travel"
    if "re-trial" in rl: return "Re-Trial Pending"
    if "not looking" in rl or "future potential" in rl: return "Not Looking Now"
    if "group" in rl or "subject" in rl: return "Wants Other Subjects"
    if "higher grade" in rl: return "Higher Grade"
    return r

def norm_board(b):
    b = b.strip(); bl = b.lower()
    if not b or "na" == bl or "other" in bl or "not listed" in bl: return "(Other/Unknown)"
    if "cbse" in bl and "icse" not in bl: return "CBSE"
    if "icse" in bl and "cbse" not in bl: return "ICSE"
    if "indian" in bl or ("cbse" in bl and "icse" in bl): return "Indian (CBSE/ICSE)"
    if "ib" in bl or "baccalaureate" in bl: return "IB"
    if "british" in bl or "uk" in bl or "uknc" in bl: return "British/UK"
    if "igcse" in bl or "cambridge" in bl: return "IGCSE/Cambridge"
    if "us" in bl: return "US Curriculum"
    if "anc" in bl: return "ANC"
    return b

# ── Data Pull ───────────────────────────────────────────────────────
def pull_lead_analysis(service):
    """Pull all cohort tabs from Lead Analysis sheet."""
    meta = service.spreadsheets().get(spreadsheetId=LEAD_SHEET).execute()
    sheets = meta.get("sheets", [])

    months = {}
    for s in sheets:
        title = s["properties"]["title"]
        tl = title.lower()
        # Pull Cohort tabs
        if "cohort" in tl or "lead analysis" in tl:
            if "feb" in tl: key = "feb"
            elif "mar" in tl: key = "mar"
            elif "apr" in tl: key = "apr"
            else: continue
            try:
                rows = fetch_sheet(service, LEAD_SHEET, f"'{title}'!A1:W1000")
                if rows:
                    months[key] = {"title": title, "rows": rows}
            except Exception as e:
                print(f"Warning: Could not fetch {title}: {e}", file=sys.stderr)

    return months

def parse_lead_rows(rows):
    """Parse lead analysis rows into structured dicts."""
    leads = []
    seen = set()
    for r in rows[1:]:
        pid = sg(r, 0)
        if not pid or len(r) < 5: continue
        if pid in seen: continue  # dedup
        seen.add(pid)
        leads.append({
            "ch": sg(r, 4),
            "ch_grp": channel_group_broad(sg(r, 4)),
            "grade": sg(r, 6),
            "board": sg(r, 7),
            "demo_sch": sg(r, 8),
            "demo_done": sg(r, 9),
            "stage": sg(r, 14),
            "reason": sg(r, 15),
            "campaign": sg(r, 17),
            "owner": sg(r, 20) if len(r) > 20 else "",
        })
    return leads

def pull_board_data(service):
    """Pull school board sheet."""
    try:
        rows = fetch_sheet(service, BOARD_SHEET, "Sheet1!A1:BV1000")
        return rows
    except Exception as e:
        print(f"Warning: Could not fetch board data: {e}", file=sys.stderr)
        return []

def parse_board_rows(rows):
    """Parse board sheet rows."""
    leads = []
    seen = set()
    for r in rows[1:]:
        pid = sg(r, 2)  # prospectid
        if not pid or len(r) < 20: continue
        if pid in seen: continue
        seen.add(pid)
        board = norm_board(sg(r, 70) if len(r) > 70 else "")
        leads.append({
            "month": sg(r, 0),
            "ch": channel_group(sg(r, 6)),  # utm_medium
            "ch_broad": channel_group_broad(sg(r, 6)),
            "campaign": sg(r, 37) if len(r) > 37 else "",
            "grade": sg(r, 28),
            "board": board,
            "ql": safe_int(sg(r, 12)),
            "ts": safe_int(sg(r, 14)),
            "td": safe_int(sg(r, 17)),
            "paid": safe_int(sg(r, 20)),
            "revenue": safe_float(sg(r, 21)),
            "reason": sg(r, 29),
        })
    return leads

# ── Analysis ────────────────────────────────────────────────────────
def analyze_leads(leads):
    """Run full funnel analysis on a set of leads."""
    total = len(leads)
    td_leads = [l for l in leads if l["demo_done"] == "1"]
    paid_leads = [l for l in td_leads if classify_stage(l["stage"]) == "Paid"]
    pipe_leads = [l for l in td_leads if classify_stage(l["stage"]) == "Pipeline"]
    lost_leads = [l for l in td_leads if classify_stage(l["stage"]) == "Lost"]
    nonpaid = [l for l in td_leads if classify_stage(l["stage"]) != "Paid"]

    # Channel breakdown
    channels = {}
    for ch in ["Meta", "Google", "Referrals", "Organic", "Others"]:
        ch_leads = [l for l in leads if l["ch_grp"] == ch]
        ch_td = [l for l in ch_leads if l["demo_done"] == "1"]
        ch_paid = [l for l in ch_td if classify_stage(l["stage"]) == "Paid"]
        ch_pipe = [l for l in ch_td if classify_stage(l["stage"]) == "Pipeline"]
        ch_lost = [l for l in ch_td if classify_stage(l["stage"]) == "Lost"]
        ch_nonpaid = [l for l in ch_td if classify_stage(l["stage"]) != "Paid"]
        ch_reasons = Counter(norm_reason(l["reason"]) for l in ch_nonpaid)
        channels[ch] = {
            "ql": len(ch_leads), "td": len(ch_td), "paid": len(ch_paid),
            "pipe": len(ch_pipe), "lost": len(ch_lost),
            "t2p": pct(len(ch_paid), len(ch_td)),
            "qltd": pct(len(ch_td), len(ch_leads)),
            "reasons": ch_reasons,
        }

    # Campaign breakdown within channels
    campaigns = {}
    for l in leads:
        c = l["campaign"] or "(No Campaign)"
        if c not in campaigns:
            campaigns[c] = {"ql": 0, "td": 0, "paid": 0, "pipe": 0, "lost": 0, "reasons": [], "ch": l["ch_grp"]}
        campaigns[c]["ql"] += 1
    for l in td_leads:
        c = l["campaign"] or "(No Campaign)"
        if c not in campaigns:
            campaigns[c] = {"ql": 0, "td": 0, "paid": 0, "pipe": 0, "lost": 0, "reasons": [], "ch": l["ch_grp"]}
        campaigns[c]["td"] += 1
        cat = classify_stage(l["stage"])
        if cat == "Paid": campaigns[c]["paid"] += 1
        elif cat == "Pipeline": campaigns[c]["pipe"] += 1
        else: campaigns[c]["lost"] += 1
        if cat != "Paid": campaigns[c]["reasons"].append(norm_reason(l["reason"]))

    # Grade breakdown
    grades = defaultdict(lambda: {"ql": 0, "td": 0, "paid": 0, "reasons": []})
    for l in leads:
        g = l["grade"] or "?"
        grades[g]["ql"] += 1
    for l in td_leads:
        g = l["grade"] or "?"
        grades[g]["td"] += 1
        if classify_stage(l["stage"]) == "Paid":
            grades[g]["paid"] += 1
        else:
            grades[g]["reasons"].append(norm_reason(l["reason"]))

    # NI reasons overall
    overall_reasons = Counter(norm_reason(l["reason"]) for l in nonpaid)

    return {
        "total": total, "td": len(td_leads), "paid": len(paid_leads),
        "pipe": len(pipe_leads), "lost": len(lost_leads),
        "t2p": pct(len(paid_leads), len(td_leads)),
        "qltd": pct(len(td_leads), total),
        "channels": channels, "campaigns": campaigns,
        "grades": dict(grades), "reasons": overall_reasons,
    }

def analyze_board(board_leads):
    """Run school board analysis."""
    boards = defaultdict(lambda: {"ql": 0, "ts": 0, "td": 0, "paid": 0})
    for l in board_leads:
        b = l["board"]
        boards[b]["ql"] += l["ql"]
        boards[b]["ts"] += l["ts"]
        boards[b]["td"] += l["td"]
        boards[b]["paid"] += l["paid"]

    # Board x channel
    board_channels = {}
    for ch in ["Meta", "Google Brand", "Google Other"]:
        bch = defaultdict(lambda: {"ql": 0, "ts": 0, "td": 0, "paid": 0})
        for l in board_leads:
            if l["ch"] == ch:
                b = l["board"]
                bch[b]["ql"] += l["ql"]
                bch[b]["ts"] += l["ts"]
                bch[b]["td"] += l["td"]
                bch[b]["paid"] += l["paid"]
        board_channels[ch] = dict(bch)

    # Board x campaign
    board_campaigns = {}
    top_camps = ["Google_Brand_KWs", "Google_Generic_KWs", "Dubai_location2_Signup",
                 "Interest_Maths_Engaged", "High_ROAS_Creative_Scaling", "Dubai Location 2",
                 "MEA_Int-Female", "Int_Premium_Brands-New-Creatives", "Advantage+Non-OTP", "Search_Competitors"]
    for camp in top_camps:
        bc = defaultdict(lambda: {"ql": 0, "ts": 0, "td": 0, "paid": 0})
        for l in board_leads:
            if l["campaign"] == camp:
                bc[l["board"]]["ql"] += l["ql"]
                bc[l["board"]]["ts"] += l["ts"]
                bc[l["board"]]["td"] += l["td"]
                bc[l["board"]]["paid"] += l["paid"]
        if any(d["ql"] > 0 for d in bc.values()):
            board_campaigns[camp] = dict(bc)

    return {"boards": dict(boards), "board_channels": board_channels, "board_campaigns": board_campaigns}


# ── HTML Generation ─────────────────────────────────────────────────
def generate_html(months_data, board_analysis, now):
    """Generate the full HTML dashboard."""

    css = """<style>
:root{--bg:#0f1117;--s1:#1a1d27;--s2:#232733;--bd:#2e3345;--t1:#e4e6ed;--t2:#9ca0af;--ac:#6c8cff;--r:#ff5c5c;--rb:rgba(255,92,92,.12);--g:#4ecb71;--gb:rgba(78,203,113,.12);--y:#fbbf24;--yb:rgba(251,191,36,.12);--b:#60a5fa;--bb:rgba(96,165,250,.12);--p:#a78bfa}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:var(--bg);color:var(--t1);line-height:1.6}
.wrap{max-width:1240px;margin:0 auto;padding:24px}
.hdr{background:linear-gradient(135deg,#1e2235,#162040);border-bottom:1px solid var(--bd);padding:36px 24px 28px}
.hdr-in{max-width:1240px;margin:0 auto}
.hdr h1{font-size:26px;font-weight:700;letter-spacing:-.5px}
.hdr .sub{font-size:14px;color:var(--t2);margin:4px 0 14px}
.hdr .meta{display:flex;gap:20px;font-size:12px;color:var(--t2);flex-wrap:wrap}
.month-tabs{display:flex;gap:0;margin-bottom:24px;background:var(--s1);border-radius:10px;padding:4px;border:1px solid var(--bd)}
.month-tab{flex:1;padding:12px;text-align:center;font-size:14px;font-weight:600;color:var(--t2);background:none;border:none;border-radius:8px;cursor:pointer;transition:.2s}
.month-tab:hover{color:var(--t1)}
.month-tab.active{background:var(--ac);color:#fff}
.month-panel{display:none}.month-panel.active{display:block}
.card{background:var(--s1);border:1px solid var(--bd);border-radius:12px;padding:24px;margin-bottom:16px}
.card h2{font-size:16px;font-weight:700;margin-bottom:4px;display:flex;align-items:center;gap:8px}
.card h2 .n{background:var(--ac);color:#fff;font-size:10px;font-weight:700;width:22px;height:22px;border-radius:6px;display:inline-flex;align-items:center;justify-content:center}
.card .desc{font-size:12px;color:var(--t2);margin-bottom:16px}
.card h3{font-size:13px;font-weight:600;color:var(--t2);margin:20px 0 10px;padding-bottom:6px;border-bottom:1px solid var(--bd)}
.ch-hdr{display:flex;align-items:center;gap:10px;margin:22px 0 14px;padding:10px 14px;border-radius:8px;font-size:14px;font-weight:700}
.ch-hdr.meta-h{background:var(--bb);color:var(--b)}
.ch-hdr.google-h{background:var(--gb);color:var(--g)}
.ch-hdr .ch-tag{font-size:11px;font-weight:600;padding:2px 8px;border-radius:4px;margin-left:auto}
.ch-hdr.meta-h .ch-tag{background:rgba(96,165,250,.2)}
.ch-hdr.google-h .ch-tag{background:rgba(78,203,113,.2)}
.kpis{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin:14px 0}
.kpi{background:var(--s2);border-radius:8px;padding:14px;text-align:center}
.kpi .l{font-size:10px;color:var(--t2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:2px}
.kpi .v{font-size:22px;font-weight:700}
.kpi .d{font-size:10px;margin-top:1px}
.kpi .d.r{color:var(--r)}.kpi .d.g{color:var(--g)}.kpi .d.y{color:var(--y)}
.kpis4{grid-template-columns:repeat(4,1fr)}
table{width:100%;border-collapse:collapse;font-size:12px;margin:10px 0}
thead th{background:var(--s2);padding:8px 10px;text-align:left;font-weight:600;font-size:10px;text-transform:uppercase;letter-spacing:.4px;color:var(--t2);border-bottom:2px solid var(--bd);white-space:nowrap}
tbody td{padding:8px 10px;border-bottom:1px solid var(--bd);vertical-align:middle}
tbody tr:hover{background:rgba(108,140,255,.04)}
.trow td{font-weight:700;background:var(--s2)}
.sig{font-weight:600;white-space:nowrap}
.r{color:var(--r)}.g{color:var(--g)}.y{color:var(--y)}.b{color:var(--b)}
.dot{width:7px;height:7px;border-radius:50%;display:inline-block}
.dot.r{background:var(--r)}.dot.g{background:var(--g)}.dot.y{background:var(--y)}
.badge{display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600;white-space:nowrap}
.badge.sc{background:var(--gb);color:var(--g)}.badge.cp{background:var(--rb);color:var(--r)}.badge.mn{background:var(--yb);color:var(--y)}.badge.st{background:rgba(251,191,36,.15);color:var(--y)}.badge.ps{background:var(--rb);color:var(--r)}
.co{background:var(--s2);border-left:3px solid var(--ac);border-radius:0 8px 8px 0;padding:12px 16px;margin:12px 0;font-size:12px;color:var(--t2);line-height:1.7}
.co strong{color:var(--t1)}.co.r{border-left-color:var(--r);background:var(--rb)}.co.g{border-left-color:var(--g);background:var(--gb)}.co.y{border-left-color:var(--y);background:var(--yb)}
.bar-c{margin:12px 0}.bar-r{display:flex;align-items:center;gap:10px;margin-bottom:6px}
.bar-l{width:140px;font-size:11px;color:var(--t2);text-align:right;flex-shrink:0}
.bar-t{flex:1;height:24px;background:var(--s2);border-radius:5px;overflow:hidden}
.bar-f{height:100%;border-radius:5px;display:flex;align-items:center;padding-left:8px;font-size:10px;font-weight:600;color:#fff;min-width:35px}
.bar-f.r{background:linear-gradient(90deg,var(--r),#ff7b7b)}.bar-f.g{background:linear-gradient(90deg,var(--g),#6ee08d)}.bar-f.y{background:linear-gradient(90deg,var(--y),#fcd34d);color:#1a1d27}.bar-f.b{background:linear-gradient(90deg,var(--ac),#8ba8ff)}.bar-f.p{background:linear-gradient(90deg,var(--p),#c4b5fd)}
.pill{display:inline-block;padding:2px 10px;border-radius:16px;font-size:11px;font-weight:500;border:1px solid var(--bd);background:var(--s2);color:var(--t2);margin:2px}.pill.top{border-color:var(--r);background:var(--rb);color:var(--r)}.pill .c{font-weight:700}
.cols{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin:12px 0}
.col-card{background:var(--s2);border-radius:8px;padding:16px}
.col-card h4{font-size:12px;font-weight:600;margin-bottom:10px;color:var(--t2)}
.act{display:flex;gap:10px;margin:6px 0;font-size:12px}
.act .num{background:var(--ac);color:#fff;width:20px;height:20px;border-radius:5px;display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;flex-shrink:0;margin-top:2px}
.act p{color:var(--t2);line-height:1.5}.act strong{color:var(--t1)}
.foot{text-align:center;padding:24px 0 12px;font-size:10px;color:var(--t2)}
@media(max-width:768px){.kpis,.kpis4{grid-template-columns:repeat(2,1fr)}.cols{grid-template-columns:1fr}.bar-l{width:90px;font-size:10px}.hdr h1{font-size:20px}table{font-size:11px}thead th,tbody td{padding:6px 5px}}
</style>"""

    ist = timezone(timedelta(hours=5, minutes=30))
    updated = now.astimezone(ist).strftime("%B %d, %Y at %I:%M %p IST")

    # Build month panels
    panels_html = ""
    month_tabs_html = ""

    available_months = sorted(months_data.keys())
    for i, mkey in enumerate(available_months):
        mdata = months_data[mkey]
        analysis = mdata["analysis"]
        month_name = mdata["label"]
        is_active = (i == len(available_months) - 1)  # latest month active
        active_cls = " active" if is_active else ""

        month_tabs_html += f'<button class="month-tab{active_cls}" onclick="showMonth(\'{mkey}\')">{month_name}</button>\n'

        # Build panel
        panel = build_month_panel(mkey, mdata, analysis, board_analysis if mkey == "mar" else None)
        panels_html += f'<div class="month-panel{active_cls}" id="panel-{mkey}">\n{panel}\n</div>\n'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ME Campaign Performance Review</title>
{css}
</head>
<body>
<div class="hdr"><div class="hdr-in">
  <h1>ME Campaign Performance Review</h1>
  <div class="sub">Google + Meta | Plan vs Actual | Channel-Wise T2P Deep Dive</div>
  <div class="meta">
    <span>Alex Brachen, AD — Middle East</span>
    <span>Auto-updated: {updated}</span>
    <span>Targets: {TARGET_QL_TD}%+ QL-TD% | {TARGET_T2P}%+ T2P | ₹{TARGET_ABV//1000}K+ ABV | {TARGET_LTV_CAC}:1 LTV:CAC</span>
  </div>
</div></div>
<div class="wrap">
<div class="month-tabs">
{month_tabs_html}
</div>
{panels_html}
<div class="foot">
  Auto-generated from Google Sheets data. Refreshes daily at 12:00 PM IST.<br>
  Data: ME Lead Analysis (Cohort tabs) + School Board Sheet. Targets: FY27 ₹7.35Cr budget.
</div>
</div>
<script>
function showMonth(m){{
  document.querySelectorAll('.month-tab').forEach((b,i)=>b.classList.remove('active'));
  document.querySelectorAll('.month-panel').forEach(p=>p.classList.remove('active'));
  document.getElementById('panel-'+m).classList.add('active');
  document.querySelectorAll('.month-tab').forEach(b=>{{if(b.textContent.toLowerCase().includes(m))b.classList.add('active')}});
}}
</script>
</body></html>"""
    return html


def build_month_panel(mkey, mdata, analysis, board_analysis):
    """Build HTML for one month's panel."""
    label = mdata["label"]
    total = analysis["total"]
    td = analysis["td"]
    paid = analysis["paid"]
    pipe = analysis["pipe"]
    t2p = analysis["t2p"]
    qltd = analysis["qltd"]
    channels = analysis["channels"]

    # Section 1: KPIs
    s1 = f"""
<div class="card">
  <h2><span class="n">1</span> {label} — Overview</h2>
  <div class="kpis">
    <div class="kpi"><div class="l">Total QLs</div><div class="v">{total}</div></div>
    <div class="kpi"><div class="l">Trial Done</div><div class="v">{td}</div><div class="d {'g' if qltd>=TARGET_QL_TD else 'r'}">{qltd:.0f}% QL-TD (target {TARGET_QL_TD}%)</div></div>
    <div class="kpi"><div class="l">Paid</div><div class="v" style="color:var(--g)">{paid}</div></div>
    <div class="kpi"><div class="l">Pipeline</div><div class="v" style="color:var(--y)">{pipe}</div></div>
    <div class="kpi"><div class="l">T2P</div><div class="v" style="color:var({'--g' if t2p>=TARGET_T2P else '--r'})">{t2p:.0f}%</div><div class="d {'g' if t2p>=TARGET_T2P else 'r'}">target {TARGET_T2P}%</div></div>
  </div>
</div>"""

    # Section 2: Channel table
    ch_rows = ""
    for ch in ["Meta", "Google", "Referrals", "Organic", "Others"]:
        d = channels.get(ch, {"ql":0,"td":0,"paid":0,"pipe":0,"t2p":0,"qltd":0})
        if d["ql"] == 0: continue
        t2p_s = sig_span(d["t2p"], TARGET_T2P, 20)
        qltd_s = sig_span(d["qltd"], TARGET_QL_TD, 25)
        ch_rows += f'<tr><td><strong>{ch}</strong></td><td>{d["ql"]}</td><td>{d["td"]}</td><td>{qltd_s}</td><td>{d["paid"]}</td><td>{d["pipe"]}</td><td>{t2p_s}</td></tr>\n'
    ch_rows += f'<tr class="trow"><td>TOTAL</td><td>{total}</td><td>{td}</td><td>{sig_span(qltd, TARGET_QL_TD, 25)}</td><td>{paid}</td><td>{pipe}</td><td>{sig_span(t2p, TARGET_T2P, 20)}</td></tr>'

    s2 = f"""
<div class="card">
  <h2><span class="n">2</span> Channel Breakdown</h2>
  <table>
    <thead><tr><th>Channel</th><th>QLs</th><th>TD</th><th>QL-TD%</th><th>Paid</th><th>Pipeline</th><th>T2P%</th></tr></thead>
    <tbody>{ch_rows}</tbody>
  </table>
</div>"""

    # Section 3: T2P Deep Dive by Meta and Google
    s3 = build_t2p_section(analysis, 3)

    # Section 4: Campaign breakdown
    s4 = build_campaign_section(analysis, 4)

    # Section 5: Grade breakdown
    s5 = build_grade_section(analysis, 5)

    # Section 6: School board (if available)
    s6 = ""
    if board_analysis:
        s6 = build_board_section(board_analysis, 6)

    # Section 7: Auto-generated insights
    s7 = build_insights_section(analysis, board_analysis, 7 if board_analysis else 6)

    return s1 + s2 + s3 + s4 + s5 + s6 + s7


def build_t2p_section(analysis, num):
    """Build T2P deep dive by Meta and Google."""
    html = f"""
<div class="card">
  <h2><span class="n">{num}</span> T2P Deep Dive — Why Leads Don't Pay After Trial</h2>
  <div class="desc">Broken down by Meta and Google separately.</div>"""

    for ch in ["Meta", "Google"]:
        d = analysis["channels"].get(ch, {"ql":0,"td":0,"paid":0,"pipe":0,"lost":0,"t2p":0,"reasons":Counter()})
        if d["td"] == 0: continue
        cls = "meta-h" if ch == "Meta" else "google-h"
        html += f"""
  <div class="ch-hdr {cls}">{ch.upper()} — {d['td']} TDs → {d['paid']} Paid <span class="ch-tag">T2P: {d['t2p']:.0f}%</span></div>
  <div class="kpis kpis4">
    <div class="kpi"><div class="l">Trial Done</div><div class="v">{d['td']}</div></div>
    <div class="kpi"><div class="l">Paid</div><div class="v" style="color:var(--g)">{d['paid']}</div></div>
    <div class="kpi"><div class="l">Pipeline</div><div class="v" style="color:var(--y)">{d['pipe']}</div></div>
    <div class="kpi"><div class="l">Lost</div><div class="v" style="color:var(--r)">{d['lost']}</div></div>
  </div>"""

        # NI reasons bar chart
        nonpaid_count = d["td"] - d["paid"]
        if nonpaid_count > 0 and d["reasons"]:
            html += f'\n  <h3>{ch} — Why {nonpaid_count} TDs Did Not Convert</h3>\n  <div class="bar-c">'
            max_count = d["reasons"].most_common(1)[0][1] if d["reasons"] else 1
            for reason, count in d["reasons"].most_common(7):
                p = count / nonpaid_count * 100
                w = count / max_count * 90
                color = "r" if "fee" in reason.lower() else ("y" if "decision" in reason.lower() else ("g" if "interested" in reason.lower() else "b"))
                html += f'\n    <div class="bar-r"><div class="bar-l">{reason}</div><div class="bar-t"><div class="bar-f {color}" style="width:{w:.0f}%">{count} ({p:.0f}%)</div></div></div>'
            html += '\n  </div>'

        # Campaign drill-down within channel
        camp_rows = ""
        for cname in sorted(analysis["campaigns"], key=lambda x: analysis["campaigns"][x]["td"], reverse=True):
            cd = analysis["campaigns"][cname]
            if cd["td"] == 0 or cd.get("ch", "") != ch: continue
            t2p_v = pct(cd["paid"], cd["td"])
            reasons_top = Counter(cd["reasons"]).most_common(3)
            pills = " ".join(
                f'<span class="pill{" top" if i==0 else ""}"><span class="c">{cnt}</span> {r}</span>'
                for i, (r, cnt) in enumerate(reasons_top)
            )
            camp_rows += f'<tr><td><strong>{cname}</strong></td><td>{cd["ql"]}</td><td>{cd["td"]}</td><td>{cd["paid"]}</td><td>{cd["pipe"]}</td><td>{cd["lost"]}</td><td>{sig_span(t2p_v, TARGET_T2P, 20)}</td><td>{pills}</td></tr>\n'

        if camp_rows:
            html += f"""
  <h3>{ch} — Campaign Drill-Down</h3>
  <table>
    <thead><tr><th>Campaign</th><th>QLs</th><th>TD</th><th>Paid</th><th>Pipe</th><th>Lost</th><th>T2P%</th><th>NI Reasons</th></tr></thead>
    <tbody>{camp_rows}</tbody>
  </table>"""

    html += "\n</div>"
    return html


def build_campaign_section(analysis, num):
    """Build campaign performance table."""
    rows = ""
    for cname in sorted(analysis["campaigns"], key=lambda x: analysis["campaigns"][x]["ql"], reverse=True):
        cd = analysis["campaigns"][cname]
        if cd["ql"] < 3: continue
        t2p_v = pct(cd["paid"], cd["td"]) if cd["td"] else 0
        qltd_v = pct(cd["td"], cd["ql"])
        reasons_top = Counter(cd["reasons"]).most_common(1)
        top_r = f'{reasons_top[0][0]} ({reasons_top[0][1]})' if reasons_top else "—"

        # Auto-verdict
        if t2p_v >= 35: verdict = badge("SCALE", "sc")
        elif t2p_v >= 20: verdict = badge("OK", "mn")
        elif cd["td"] > 0 and t2p_v < 15: verdict = badge("CAP", "cp")
        elif cd["td"] == 0: verdict = badge("NO TD", "ps")
        else: verdict = ""

        rows += f'<tr><td>{cname}</td><td>{cd["ql"]}</td><td>{cd["td"]}</td><td>{fmt_pct(cd["td"], cd["ql"])}</td><td>{cd["paid"]}</td><td>{sig_span(t2p_v, TARGET_T2P, 15) if cd["td"] else "—"}</td><td>{top_r}</td><td>{verdict}</td></tr>\n'

    return f"""
<div class="card">
  <h2><span class="n">{num}</span> Campaign Performance</h2>
  <table>
    <thead><tr><th>Campaign</th><th>QLs</th><th>TD</th><th>QL-TD%</th><th>Paid</th><th>T2P%</th><th>#1 NI Reason</th><th>Verdict</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>"""


def build_grade_section(analysis, num):
    """Build grade-wise T2P table."""
    rows = ""
    for g in sorted(analysis["grades"], key=lambda x: (int(x) if x.isdigit() else 99)):
        d = analysis["grades"][g]
        if d["td"] == 0 and d["ql"] < 5: continue
        t2p_v = pct(d["paid"], d["td"]) if d["td"] else 0
        top_r = Counter(d["reasons"]).most_common(1)
        top_reason = f'{top_r[0][0]} ({top_r[0][1]})' if top_r else "—"
        c = sig_color(t2p_v, TARGET_T2P, 15) if d["td"] else "y"
        rows += f'<tr><td>Grade {g}</td><td>{d["ql"]}</td><td>{d["td"]}</td><td>{d["paid"]}</td><td>{sig_span(t2p_v, TARGET_T2P, 15) if d["td"] else "—"}</td><td>{top_reason}</td><td>{dot(c)}</td></tr>\n'

    return f"""
<div class="card">
  <h2><span class="n">{num}</span> Grade-Wise T2P</h2>
  <table>
    <thead><tr><th>Grade</th><th>QLs</th><th>TD</th><th>Paid</th><th>T2P%</th><th>Top NI Reason</th><th></th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>"""


def build_board_section(ba, num):
    """Build school board analysis section."""
    boards = ba["boards"]
    total_ql = sum(d["ql"] for d in boards.values())
    total_td = sum(d["td"] for d in boards.values())
    total_paid = sum(d["paid"] for d in boards.values())

    rows = ""
    for b in sorted(boards, key=lambda x: boards[x]["ql"], reverse=True):
        d = boards[b]
        if d["ql"] < 3: continue
        t2p_v = pct(d["paid"], d["td"]) if d["td"] else 0
        qltd_v = pct(d["td"], d["ql"])
        ql_pct = pct(d["ql"], total_ql)
        paid_pct = pct(d["paid"], total_paid) if total_paid else 0
        c = sig_color(t2p_v, 25, 15) if d["td"] else "y"
        rows += f'<tr><td><strong>{b}</strong></td><td>{d["ql"]}</td><td>{ql_pct:.0f}%</td><td>{d["td"]}</td><td>{sig_span(qltd_v, TARGET_QL_TD, 20)}</td><td>{d["paid"]}</td><td>{paid_pct:.0f}%</td><td>{sig_span(t2p_v, 25, 15) if d["td"] else "—"}</td><td>{dot(c)}</td></tr>\n'

    # Board x Campaign cards
    camp_cards = ""
    for camp, bc in ba["board_campaigns"].items():
        total_c_td = sum(d["td"] for d in bc.values())
        total_c_paid = sum(d["paid"] for d in bc.values())
        if total_c_td == 0: continue
        t2p_c = pct(total_c_paid, total_c_td)
        badge_cls = "sc" if t2p_c >= 30 else ("mn" if t2p_c >= 15 else "cp")
        badge_txt = "GOOD" if t2p_c >= 30 else ("OK" if t2p_c >= 15 else "WEAK")

        inner_rows = ""
        for b in sorted(bc, key=lambda x: bc[x]["ql"], reverse=True):
            d = bc[b]
            if d["ql"] < 2: continue
            t = pct(d["paid"], d["td"]) if d["td"] else 0
            inner_rows += f'<tr><td>{b}</td><td>{d["ql"]}</td><td>{d["td"]}</td><td>{d["paid"]}</td><td>{sig_span(t, 30, 15) if d["td"] else "—"}</td></tr>'

        if inner_rows:
            camp_cards += f"""<div class="col-card"><h4>{camp} — {badge(badge_txt, badge_cls)} T2P: {t2p_c:.0f}%</h4>
<table><thead><tr><th>Board</th><th>QLs</th><th>TD</th><th>Paid</th><th>T2P</th></tr></thead><tbody>{inner_rows}</tbody></table></div>\n"""

    return f"""
<div class="card">
  <h2><span class="n">{num}</span> School Board Analysis</h2>
  <div class="desc">Board-level conversion analysis. Which curriculum parents convert?</div>
  <h3>Board T2P Overview</h3>
  <table>
    <thead><tr><th>Board</th><th>QLs</th><th>% of QLs</th><th>TD</th><th>QL-TD%</th><th>Paid</th><th>% of Paid</th><th>T2P%</th><th></th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
  <h3>Board × Campaign</h3>
  <div class="cols">{camp_cards}</div>
</div>"""


def build_insights_section(analysis, board_analysis, num):
    """Auto-generate insights based on thresholds."""
    insights = []

    # T2P insight
    if analysis["t2p"] < TARGET_T2P:
        gap = TARGET_T2P - analysis["t2p"]
        insights.append(f'<strong>T2P at {analysis["t2p"]:.0f}% vs {TARGET_T2P}% target (-{gap:.0f}pp).</strong> Top NI reason: {analysis["reasons"].most_common(1)[0][0] if analysis["reasons"] else "unknown"}.')

    # Channel insights
    for ch in ["Meta", "Google"]:
        d = analysis["channels"].get(ch, {})
        if d.get("td", 0) > 0 and d["t2p"] < TARGET_T2P:
            top_reason = d["reasons"].most_common(1)[0] if d["reasons"] else ("unknown", 0)
            insights.append(f'<strong>{ch} T2P: {d["t2p"]:.0f}%</strong> — #{1} NI reason: {top_reason[0]} ({top_reason[1]} cases, {pct(top_reason[1], d["td"]-d["paid"]):.0f}% of non-paid).')

    # Campaign flags
    for cname, cd in sorted(analysis["campaigns"].items(), key=lambda x: x[1]["td"], reverse=True):
        if cd["td"] >= 5:
            t2p_v = pct(cd["paid"], cd["td"])
            if t2p_v < 15:
                insights.append(f'<strong>{cname}: {t2p_v:.0f}% T2P from {cd["td"]} TDs.</strong> Below threshold. Review or cap budget.')
            elif t2p_v >= 35:
                insights.append(f'<strong>{cname}: {t2p_v:.0f}% T2P</strong> — above target. Consider scaling.')

    # Board insights
    if board_analysis:
        boards = board_analysis["boards"]
        for b in sorted(boards, key=lambda x: boards[x]["td"], reverse=True):
            d = boards[b]
            if d["td"] >= 5:
                t2p_v = pct(d["paid"], d["td"])
                if t2p_v >= 25:
                    insights.append(f'<strong>{b} board: {t2p_v:.0f}% T2P</strong> — high-value segment. Consider targeted creatives.')
                elif t2p_v < 10 and d["td"] >= 5:
                    insights.append(f'<strong>{b} board: {t2p_v:.0f}% T2P from {d["td"]} TDs.</strong> Low conversion — review targeting or pitch.')

    items = "\n".join(f'<div class="act"><div class="num">{i+1}</div><p>{ins}</p></div>' for i, ins in enumerate(insights[:10]))

    return f"""
<div class="card">
  <h2><span class="n">{num}</span> Auto-Generated Insights</h2>
  <div class="desc">Flagged against FY27 targets: {TARGET_QL_TD}%+ QL-TD%, {TARGET_T2P}%+ T2P, ₹{TARGET_ABV//1000}K+ ABV</div>
  {items}
</div>"""


# ── Main ────────────────────────────────────────────────────────────
def main():
    print("Connecting to Google Sheets...", file=sys.stderr)
    service = get_sheets_service()

    # Pull lead analysis
    print("Pulling lead analysis data...", file=sys.stderr)
    lead_months = pull_lead_analysis(service)

    months_data = {}
    month_labels = {"feb": "February 2026", "mar": "March 2026", "apr": "April 2026"}

    for mkey, minfo in lead_months.items():
        leads = parse_lead_rows(minfo["rows"])
        if not leads: continue
        analysis = analyze_leads(leads)
        months_data[mkey] = {
            "label": month_labels.get(mkey, mkey.title()),
            "title": minfo["title"],
            "analysis": analysis,
        }
        print(f"  {mkey}: {analysis['total']} QLs, {analysis['td']} TDs, {analysis['paid']} Paid ({analysis['t2p']:.0f}% T2P)", file=sys.stderr)

    # Pull board data
    print("Pulling school board data...", file=sys.stderr)
    board_rows = pull_board_data(service)
    board_leads = parse_board_rows(board_rows) if board_rows else []
    board_analysis = analyze_board(board_leads) if board_leads else None
    if board_analysis:
        total_bl = sum(d["ql"] for d in board_analysis["boards"].values())
        print(f"  Board data: {total_bl} leads, {len(board_analysis['boards'])} boards", file=sys.stderr)

    # Generate HTML
    print("Generating HTML...", file=sys.stderr)
    now = datetime.now(timezone.utc)
    html = generate_html(months_data, board_analysis, now)

    with open("index.html", "w") as f:
        f.write(html)

    print(f"Done! index.html written ({len(html):,} bytes)", file=sys.stderr)


if __name__ == "__main__":
    main()
