import json
import uuid
import traceback
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from flask import Flask, render_template, request, Response, jsonify
import plotly.express as px
import plotly.io as pio

# =========================
# CONFIG
# =========================
SPIKES_FILE  = r"Floodlight_Report_20260130_125843_1605197602_5503673738_Spikes.csv"
MISSING_FILE = r"Floodlight_Report_20260130_125843_1605197602_5503673738_Missing.csv"
GA4_FILE     = r"GA4_Sample_Traffic_from_Floodlight_60days.csv"

CHANNEL_COL = "GA4 Default Channel Group"
SESSIONS_COL = "Sessions (sampled)"
DATE_COL = "Date"
IMPR_TOTAL_COL = "Floodlight Impressions (total/day)"
CHANNELS_ORDER = ["Organic Search", "Direct", "Referral", "Paid Search", "Organic Social"]

# Ollama (local)
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "gemma3"
OLLAMA_TIMEOUT_SEC =None  # a bit safer

# Deep links
GTM_URL = "https://tagmanager.google.com/"
GA4_URL = "https://analytics.google.com/"

# ✅ Always run LLM on page load
USE_LLM_DEFAULT_ON_LOAD = True 

# =========================
# Flask
# =========================
app = Flask(__name__)

DATA_CACHE = {}
CHART_CACHE = {}   # adv_id -> {"charts": {...}, "totals": dict}
JOBS = {}          # job_id -> {"status": running|done|error, "adv_id": int, "result": dict|None, "error": str|None}
EXEC = ThreadPoolExecutor(max_workers=2)

# =========================
# Helpers
# =========================
def safe_int_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def read_inputs(spikes_path: str, missing_path: str, ga4_path: str):
    spikes = pd.read_csv(spikes_path)
    missing = pd.read_csv(missing_path)
    ga4 = pd.read_csv(ga4_path)

    spikes.columns = [c.strip() for c in spikes.columns]
    missing.columns = [c.strip() for c in missing.columns]
    ga4.columns = [c.strip() for c in ga4.columns]

    if "Date" in spikes.columns:
        spikes["Date"] = pd.to_datetime(spikes["Date"], errors="coerce")
    if "Missing Date" in missing.columns:
        missing["Missing Date"] = pd.to_datetime(missing["Missing Date"], errors="coerce")
    ga4[DATE_COL] = pd.to_datetime(ga4[DATE_COL], errors="coerce")

    for df in (spikes, missing, ga4):
        if "Advertiser ID" in df.columns:
            df["Advertiser ID"] = safe_int_series(df["Advertiser ID"])
        if "Floodlight Activity ID" in df.columns:
            df["Floodlight Activity ID"] = safe_int_series(df["Floodlight Activity ID"])

    needed_ga4 = {"Advertiser", "Advertiser ID", DATE_COL, CHANNEL_COL, SESSIONS_COL, IMPR_TOTAL_COL}
    missing_cols = needed_ga4 - set(ga4.columns)
    if missing_cols:
        raise ValueError(f"GA4 file missing columns: {sorted(list(missing_cols))}")

    return spikes, missing, ga4

def advertiser_options(ga4: pd.DataFrame) -> pd.DataFrame:
    return (
        ga4[["Advertiser", "Advertiser ID"]]
        .dropna(subset=["Advertiser ID"])
        .drop_duplicates()
        .sort_values(["Advertiser", "Advertiser ID"])
        .reset_index(drop=True)
    )

def compute_health_score(spikes_adv: pd.DataFrame, missing_adv: pd.DataFrame, days_in_window: int) -> dict:
    spike_days = spikes_adv["Date"].dt.normalize().nunique() if (not spikes_adv.empty and "Date" in spikes_adv.columns) else 0
    missing_days = missing_adv["Missing Date"].dt.normalize().nunique() if (not missing_adv.empty and "Missing Date" in missing_adv.columns) else 0

    spike_penalty = min(40, spike_days * 3)
    missing_penalty = min(60, missing_days * 1)
    score = max(0, 100 - spike_penalty - missing_penalty)

    if score >= 90:
        band = "Excellent"
    elif score >= 75:
        band = "Good"
    elif score >= 55:
        band = "Fair"
    else:
        band = "Poor"

    return {
        "score": int(score),
        "band": band,
        "spike_days": int(spike_days),
        "missing_days": int(missing_days),
        "days_in_window": int(days_in_window),
    }

def build_spike_problems_table(spikes_adv: pd.DataFrame) -> pd.DataFrame:
    if spikes_adv.empty:
        return pd.DataFrame(columns=["Problem Type", "Date", "Floodlight Activity Name", "Impressions"])

    needed = {"Date", "Floodlight Activity Name", "Floodlight Impressions"}
    if not needed.issubset(set(spikes_adv.columns)):
        return pd.DataFrame(columns=["Problem Type", "Date", "Floodlight Activity Name", "Impressions"])

    t = spikes_adv.copy()
    t["Problem Type"] = "Sudden spike"
    t["Date"] = pd.to_datetime(t["Date"], errors="coerce").dt.date
    t.rename(columns={"Floodlight Impressions": "Impressions"}, inplace=True)
    t = t[["Problem Type", "Date", "Floodlight Activity Name", "Impressions"]]
    return t.sort_values(["Date", "Impressions"], ascending=[False, False]).reset_index(drop=True)

def _continuous_date_ranges(dates: pd.Series):
    if dates.empty:
        return []
    d = pd.to_datetime(dates, errors="coerce").dropna().dt.normalize().sort_values().unique()
    d = pd.to_datetime(d)
    ranges = []
    start, prev = d[0], d[0]
    for cur in d[1:]:
        if (cur - prev).days == 1:
            prev = cur
        else:
            ranges.append((start, prev))
            start, prev = cur, cur
    ranges.append((start, prev))
    return ranges

def build_missing_problems_table(missing_adv: pd.DataFrame) -> pd.DataFrame:
    if missing_adv.empty:
        return pd.DataFrame(columns=["Problem Type", "Floodlight Activity Name", "Start Date", "End Date", "Missing Days"])

    needed = {"Floodlight Activity Name", "Missing Date"}
    if not needed.issubset(set(missing_adv.columns)):
        return pd.DataFrame(columns=["Problem Type", "Floodlight Activity Name", "Start Date", "End Date", "Missing Days"])

    rows = []
    for fl_name, g in missing_adv.groupby("Floodlight Activity Name", dropna=False):
        for (s, e) in _continuous_date_ranges(g["Missing Date"]):
            rows.append({
                "Problem Type": "Floodlight not working",
                "Floodlight Activity Name": fl_name,
                "Start Date": s.date(),
                "End Date": e.date(),
                "Missing Days": int((e - s).days + 1),
            })

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["Problem Type", "Floodlight Activity Name", "Start Date", "End Date", "Missing Days"])
    return out.sort_values(["Missing Days", "Start Date"], ascending=[False, False]).reset_index(drop=True)

def fig_to_html(fig):
    return pio.to_html(fig, include_plotlyjs="cdn", full_html=False)

def get_data():
    if "loaded" not in DATA_CACHE:
        spikes_df, missing_df, ga4_df = read_inputs(SPIKES_FILE, MISSING_FILE, GA4_FILE)
        opts = advertiser_options(ga4_df)
        if opts.empty:
            raise ValueError("No advertisers found in GA4 file.")
        DATA_CACHE["loaded"] = True
        DATA_CACHE["spikes"] = spikes_df
        DATA_CACHE["missing"] = missing_df
        DATA_CACHE["ga4"] = ga4_df
        DATA_CACHE["opts"] = opts
    return DATA_CACHE["spikes"], DATA_CACHE["missing"], DATA_CACHE["ga4"], DATA_CACHE["opts"]

# =========================
# GA4 helper (channels)
# =========================
def channel_breakdown_on_date(ga4_adv: pd.DataFrame, d: pd.Timestamp) -> pd.DataFrame:
    g = ga4_adv[ga4_adv[DATE_COL].dt.normalize() == pd.to_datetime(d).normalize()].copy()
    if g.empty:
        return pd.DataFrame(columns=[CHANNEL_COL, SESSIONS_COL])
    out = g.groupby(CHANNEL_COL, as_index=False)[SESSIONS_COL].sum().sort_values(SESSIONS_COL, ascending=False)
    return out

def channel_baseline_stats(ga4_adv: pd.DataFrame) -> pd.DataFrame:
    tmp = ga4_adv.copy()
    tmp["_date_norm"] = tmp[DATE_COL].dt.normalize()
    daily = tmp.groupby(["_date_norm", CHANNEL_COL], as_index=False)[SESSIONS_COL].sum()
    stats = daily.groupby(CHANNEL_COL)[SESSIONS_COL].agg(["mean", "std", "max"]).reset_index()
    stats["std"] = stats["std"].fillna(0)
    return stats

def infer_spike_cause_from_ga4(ga4_adv: pd.DataFrame, spike_date: pd.Timestamp) -> dict:
    day = pd.to_datetime(spike_date).normalize()
    breakdown = channel_breakdown_on_date(ga4_adv, day)
    if breakdown.empty:
        return {
            "dominant_channel": None,
            "dominant_sessions": 0,
            "zscore": 0.0,
            "cause": "GA4 sample has no rows for this date; cannot infer channel driver.",
            "evidence": "No GA4 sampled sessions for the spike date."
        }

    dominant_channel = str(breakdown.iloc[0][CHANNEL_COL])
    dominant_sessions = int(breakdown.iloc[0][SESSIONS_COL])

    stats = channel_baseline_stats(ga4_adv)
    row = stats[stats[CHANNEL_COL] == dominant_channel]
    mu = float(row.iloc[0]["mean"]) if not row.empty else 0.0
    sd = float(row.iloc[0]["std"]) if not row.empty else 0.0
    z = (dominant_sessions - mu) / (sd if sd > 0 else 1.0)

    lc = dominant_channel.lower()
    if "referral" in lc:
        cause = "Referrals surged on this date. Likely bot crawl / spam referrals / sudden partner link exposure."
    elif "direct" in lc:
        cause = "Direct traffic surged. Possible tagging/attribution change, UTM loss, or brand/bot burst."
    elif "paid" in lc:
        cause = "Paid Search surged. Possible budget increase, campaign re-launch, or tracking duplication."
    elif "organic" in lc:
        cause = "Organic channel surged. Possible SEO spike, crawler traffic, or reporting window shift."
    else:
        cause = "A single channel dominated the GA4 session mix on this date, suggesting a channel-specific anomaly."

    evidence = f"{dominant_channel} sessions on {day.date()} ~{dominant_sessions:,} vs baseline mean ~{mu:,.0f} (z≈{z:.1f})."

    return {
        "dominant_channel": dominant_channel,
        "dominant_sessions": dominant_sessions,
        "zscore": float(z),
        "cause": cause,
        "evidence": evidence
    }

def band_dot_class(band: str) -> str:
    b = (band or "").lower()
    if "excellent" in b: return "excellent"
    if "good" in b: return "good"
    if "fair" in b: return "fair"
    return "poor"

# =========================
# LLM (only 2 calls total)
# =========================
def ollama_generate(prompt: str, temperature: float = 0.2, max_tokens: int = 350) -> str:
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": float(temperature), "num_predict": int(max_tokens)},
    }
    r = requests.post(OLLAMA_URL, json=payload, timeout=OLLAMA_TIMEOUT_SEC)
    r.raise_for_status()
    data = r.json()
    return (data.get("response") or "").strip()

def build_missing_prompt(adv_name: str, adv_id: int, missing_table: pd.DataFrame) -> str:
    rows = missing_table.head(10).to_dict(orient="records")
    return f"""You are a senior measurement analyst.

Advertiser: {adv_name} (ID {adv_id})

We detected missing Floodlight delivery ranges.
Return ONLY JSON in this schema:
{{
  "summary": "...",
  "likely_root_cause": "...",
  "recommendations": ["...", "...", "..."],
  "cta": {{"type": "GTM" or "NONE", "label": "..."}}
}}

Rules:
- Summarize overall issue; do NOT write per-activity cards.
- Recommendations must be concrete and GTM/Floodlight focused.
- CTA should be GTM.

Missing ranges sample (top 10):
{json.dumps(rows, indent=2, default=str)}
"""

def build_spike_prompt(adv_name: str, adv_id: int, spike_table: pd.DataFrame, ga4_adv: pd.DataFrame) -> str:
    s = spike_table.head(10).copy()
    drivers = []
    for _, r in s.iterrows():
        dt = pd.to_datetime(r.get("Date", None), errors="coerce")
        driver = infer_spike_cause_from_ga4(ga4_adv, dt) if pd.notna(dt) else None
        drivers.append({
            "Date": str(r.get("Date", "")),
            "Activity": str(r.get("Floodlight Activity Name", "")),
            "Impressions": int(pd.to_numeric(r.get("Impressions", 0), errors="coerce") or 0),
            "GA4_driver_cause": (driver or {}).get("cause"),
            "GA4_driver_evidence": (driver or {}).get("evidence"),
        })

    return f"""You are a senior measurement analyst.

Advertiser: {adv_name} (ID {adv_id})

We detected spikes in Floodlight impressions.
Return ONLY JSON in this schema:
{{
  "summary": "...",
  "likely_root_cause": "...",
  "recommendations": ["...", "...", "..."],
  "cta": {{"type": "GA4" or "NONE", "label": "..."}}
}}

Rules:
- Summarize overall spike behavior; do NOT write per-activity cards.
- Use GA4 driver hints if present.
- CTA should be GA4.

Spike sample (top 10 with GA4 hints):
{json.dumps(drivers, indent=2, default=str)}
"""

def run_llm_job(job_id: str, adv_id: int):
    try:
        JOBS[job_id]["status"] = "running"
        spikes_df, missing_df, ga4_df, opts = get_data()

        row = opts[opts["Advertiser ID"] == adv_id]
        if row.empty:
            adv_id = int(opts.iloc[0]["Advertiser ID"])
            row = opts[opts["Advertiser ID"] == adv_id]
        adv_name = str(row.iloc[0]["Advertiser"])

        ga4_adv = ga4_df[ga4_df["Advertiser ID"] == adv_id].copy()
        spikes_adv = spikes_df[spikes_df["Advertiser ID"] == adv_id].copy() if "Advertiser ID" in spikes_df.columns else pd.DataFrame()
        missing_adv = missing_df[missing_df["Advertiser ID"] == adv_id].copy() if "Advertiser ID" in missing_df.columns else pd.DataFrame()

        spike_table = build_spike_problems_table(spikes_adv)
        missing_table = build_missing_problems_table(missing_adv)

        result = {}

        # Missing summary
        if missing_table.empty:
            result["missing"] = {
                "summary": "No missing Floodlight delivery ranges detected in this window.",
                "likely_root_cause": "N/A",
                "recommendations": ["Continue monitoring and keep GTM publish discipline."],
                "cta": {"type": "NONE", "label": ""}
            }
        else:
            mp = build_missing_prompt(adv_name, int(adv_id), missing_table)
            text = ollama_generate(mp, temperature=0.2, max_tokens=350)
            rec = json.loads(text) if text.strip().startswith("{") else None
            if not rec:
                rec = {
                    "summary": f"Missing Floodlight delivery detected across {missing_table['Floodlight Activity Name'].nunique()} activities and {missing_table['Missing Days'].sum()} missing day(s).",
                    "likely_root_cause": "Common causes: GTM tag not firing, consent/CMP blocking, or container changes.",
                    "recommendations": [
                        "Open GTM → confirm Floodlight tags + triggers on key pages for affected activities.",
                        "Check container publish history around the missing start dates; republish if needed.",
                        "Validate consent mode/CMP settings and test with consent granted."
                    ],
                    "cta": {"type": "GTM", "label": "Go to Google Tag Manager →"}
                }
            result["missing"] = rec

        # Spike summary
        if spike_table.empty:
            result["spike"] = {
                "summary": "No sudden spikes detected in this window.",
                "likely_root_cause": "N/A",
                "recommendations": ["Continue monitoring and review acquisition weekly."],
                "cta": {"type": "NONE", "label": ""}
            }
        else:
            sp = build_spike_prompt(adv_name, int(adv_id), spike_table, ga4_adv)
            text = ollama_generate(sp, temperature=0.2, max_tokens=350)
            rec = json.loads(text) if text.strip().startswith("{") else None
            if not rec:
                rec = {
                    "summary": f"Spike behavior detected on {pd.Series(spike_table['Date']).nunique()} day(s) across {spike_table['Floodlight Activity Name'].nunique()} activities.",
                    "likely_root_cause": "Likely traffic mix anomaly (spam/bots), attribution change, or campaign surge.",
                    "recommendations": [
                        "Open GA4 → inspect acquisition for spike dates (source/medium, referrals).",
                        "If Referral/Direct dominates, check spam/bot sources and apply referral exclusions/filters.",
                        "Confirm no duplicate Floodlight firing via GTM preview/debug on key pages."
                    ],
                    "cta": {"type": "GA4", "label": "Go to GA4 Analytics →"}
                }
            result["spike"] = rec

        JOBS[job_id]["result"] = result
        JOBS[job_id]["status"] = "done"

    except Exception as e:
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"] = f"{e}\n\n{traceback.format_exc()}"

# =========================
# Summary + Charts (CSV truth)
# =========================
def spikes_impressions_by_day(spikes_adv: pd.DataFrame) -> pd.DataFrame:
    if spikes_adv.empty or "Date" not in spikes_adv.columns:
        return pd.DataFrame(columns=["day", "spike_impressions"])
    if "Floodlight Impressions" not in spikes_adv.columns:
        return pd.DataFrame(columns=["day", "spike_impressions"])

    tmp = spikes_adv.copy()
    tmp["_day"] = pd.to_datetime(tmp["Date"], errors="coerce").dt.normalize()
    tmp["Floodlight Impressions"] = pd.to_numeric(tmp["Floodlight Impressions"], errors="coerce").fillna(0)
    out = tmp.groupby("_day", as_index=False)["Floodlight Impressions"].sum()
    out.rename(columns={"_day": "day", "Floodlight Impressions": "spike_impressions"}, inplace=True)
    return out.sort_values("day")

def missing_events_by_day(missing_adv: pd.DataFrame) -> pd.DataFrame:
    if missing_adv.empty or "Missing Date" not in missing_adv.columns:
        return pd.DataFrame(columns=["day", "missing_events"])
    tmp = missing_adv.copy()
    tmp["_day"] = pd.to_datetime(tmp["Missing Date"], errors="coerce").dt.normalize()
    out = tmp.groupby("_day", as_index=False).size()
    out.rename(columns={"_day": "day", "size": "missing_events"}, inplace=True)
    return out.sort_values("day")

def ga4_impressions_by_day(ga4_adv: pd.DataFrame) -> pd.DataFrame:
    # GA4 file usually repeats IMPR_TOTAL_COL across channel rows for the day
    # safest is max per day, not sum (avoids double-counting).
    if ga4_adv.empty or DATE_COL not in ga4_adv.columns or IMPR_TOTAL_COL not in ga4_adv.columns:
        return pd.DataFrame(columns=["day", "ga4_impressions"])
    tmp = ga4_adv.copy()
    tmp["_day"] = pd.to_datetime(tmp[DATE_COL], errors="coerce").dt.normalize()
    tmp[IMPR_TOTAL_COL] = pd.to_numeric(tmp[IMPR_TOTAL_COL], errors="coerce")
    out = tmp.groupby("_day", as_index=False)[IMPR_TOTAL_COL].max()
    out.rename(columns={"_day": "day", IMPR_TOTAL_COL: "ga4_impressions"}, inplace=True)
    return out.sort_values("day")

def compute_overall_summary(adv_name: str, health: dict, spikes_adv: pd.DataFrame, missing_adv: pd.DataFrame, ga4_adv: pd.DataFrame) -> dict:
    # total FL activities (union)
    fl_names = set()
    if not spikes_adv.empty and "Floodlight Activity Name" in spikes_adv.columns:
        fl_names |= set(spikes_adv["Floodlight Activity Name"].dropna().astype(str).unique())
    if not missing_adv.empty and "Floodlight Activity Name" in missing_adv.columns:
        fl_names |= set(missing_adv["Floodlight Activity Name"].dropna().astype(str).unique())
    total_activities = int(len(fl_names))

    missing_events_total = int(len(missing_adv))  # ✅ requested: total missing events
    spike_rows_total = int(len(spikes_adv))

    # spike channel drivers (GA4 dominant channel on spike dates)
    dominant_counts = {}
    spike_dates = []
    if not spikes_adv.empty and "Date" in spikes_adv.columns:
        spike_dates = pd.to_datetime(spikes_adv["Date"], errors="coerce").dropna().dt.normalize().unique().tolist()

    for d in spike_dates:
        driver = infer_spike_cause_from_ga4(ga4_adv, d) if not ga4_adv.empty else {"dominant_channel": None}
        ch = driver.get("dominant_channel") or "Unknown"
        dominant_counts[ch] = dominant_counts.get(ch, 0) + 1

    dominant_sorted = sorted(dominant_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    top_drivers = [{"channel": k, "spike_days": int(v)} for k, v in dominant_sorted]

    # reliability verdict
    band = (health.get("band") or "").lower()
    if "excellent" in band or "good" in band:
        verdict = "✅ Generally reliable"
        verdict_reason = "Tracking appears stable overall. Issues are limited and likely recoverable with routine checks."
    elif "fair" in band:
        verdict = "⚠️ Needs attention"
        verdict_reason = "There are repeated missing/spike signals. Use data carefully until fixes are validated."
    else:
        verdict = "🛑 Not reliable right now"
        verdict_reason = "Frequent missing delivery and/or strong spikes suggest tag firing or traffic quality problems."

    # history stats
    miss_by_day = missing_events_by_day(missing_adv)
    spike_by_day = spikes_impressions_by_day(spikes_adv)

    last_missing = None
    if not miss_by_day.empty:
        last_missing = str(miss_by_day["day"].max().date())

    last_spike = None
    if not spike_by_day.empty:
        last_spike = str(spike_by_day["day"].max().date())

    return {
        "adv_name": adv_name,
        "total_activities": total_activities,
        "missing_events_total": missing_events_total,
        "spike_rows_total": spike_rows_total,
        "top_drivers": top_drivers,
        "verdict": verdict,
        "verdict_reason": verdict_reason,
        "last_missing_date": last_missing,
        "last_spike_date": last_spike,
    }

# =========================
# Dashboard payload
# =========================
def compute_dashboard_payload(adv_id: int):
    spikes_df, missing_df, ga4_df, opts = get_data()

    row = opts[opts["Advertiser ID"] == adv_id]
    if row.empty:
        adv_id = int(opts.iloc[0]["Advertiser ID"])
        row = opts[opts["Advertiser ID"] == adv_id]
    adv_name = str(row.iloc[0]["Advertiser"])

    ga4_adv = ga4_df[ga4_df["Advertiser ID"] == adv_id].copy()
    spikes_adv = spikes_df[spikes_df["Advertiser ID"] == adv_id].copy() if "Advertiser ID" in spikes_df.columns else pd.DataFrame()
    missing_adv = missing_df[missing_df["Advertiser ID"] == adv_id].copy() if "Advertiser ID" in missing_df.columns else pd.DataFrame()

    days_in_window = int(ga4_adv[DATE_COL].dt.normalize().nunique()) if not ga4_adv.empty else 0
    health = compute_health_score(spikes_adv, missing_adv, days_in_window)

    spike_table = build_spike_problems_table(spikes_adv)
    missing_table = build_missing_problems_table(missing_adv)

    # ✅ requested metrics
    missing_events_total = int(len(missing_adv))

    # Charts caching
    charts = {
        "spike_impr": None,
        "ga4_impr": None,
        "issue_history": None,
        "channels": None,
    }
    totals = None

    cache_key = adv_id
    if cache_key in CHART_CACHE:
        charts = CHART_CACHE[cache_key]["charts"]
        totals = CHART_CACHE[cache_key]["totals"]
    else:
        # Chart A: spike impressions by day (from spikes CSV)
        sbd = spikes_impressions_by_day(spikes_adv)
        if not sbd.empty:
            fig_spike = px.line(sbd, x="day", y="spike_impressions", title="Floodlight impressions from Spikes CSV (sum/day)")
            fig_spike.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(255,255,255,0.04)",
                margin=dict(l=12, r=12, t=55, b=12),
            )
            fig_spike.update_xaxes(showgrid=False)
            fig_spike.update_yaxes(gridcolor="rgba(255,255,255,0.10)", zeroline=False)
            charts["spike_impr"] = fig_to_html(fig_spike)

        # Chart B: GA4 impressions by day (from GA4 file)
        gbd = ga4_impressions_by_day(ga4_adv)
        if not gbd.empty:
            fig_ga4 = px.line(gbd, x="day", y="ga4_impressions", title="Floodlight impressions from GA4 file (max/day)")
            fig_ga4.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(255,255,255,0.04)",
                margin=dict(l=12, r=12, t=55, b=12),
            )
            fig_ga4.update_xaxes(showgrid=False)
            fig_ga4.update_yaxes(gridcolor="rgba(255,255,255,0.10)", zeroline=False)
            charts["ga4_impr"] = fig_to_html(fig_ga4)

        # Chart C: Issue history combined (missing events + spike impressions)
        miss = missing_events_by_day(missing_adv)
        spike = spikes_impressions_by_day(spikes_adv)
        merged = pd.merge(miss, spike, on="day", how="outer").fillna(0).sort_values("day")
        if not merged.empty:
            melted = merged.melt(id_vars=["day"], value_vars=["missing_events", "spike_impressions"], var_name="metric", value_name="value")
            fig_hist = px.line(melted, x="day", y="value", color="metric", title="Issue history (Missing events + Spike impressions)")
            fig_hist.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(255,255,255,0.04)",
                margin=dict(l=12, r=12, t=55, b=12),
            )
            fig_hist.update_xaxes(showgrid=False)
            fig_hist.update_yaxes(gridcolor="rgba(255,255,255,0.10)", zeroline=False)
            charts["issue_history"] = fig_to_html(fig_hist)

        # GA4 channel totals + channel trend (same as before but correct ordering)
        if not ga4_adv.empty:
            totals_series = ga4_adv.groupby(CHANNEL_COL)[SESSIONS_COL].sum()
            for ch in CHANNELS_ORDER:
                if ch not in totals_series.index:
                    totals_series.loc[ch] = 0
            totals = totals_series[CHANNELS_ORDER].to_dict()

            ts = ga4_adv.groupby([DATE_COL, CHANNEL_COL], as_index=False)[SESSIONS_COL].sum()
            ts[CHANNEL_COL] = pd.Categorical(ts[CHANNEL_COL], categories=CHANNELS_ORDER, ordered=True)
            ts = ts.sort_values([DATE_COL, CHANNEL_COL])

            fig_ch = px.line(ts, x=DATE_COL, y=SESSIONS_COL, color=CHANNEL_COL, title="5-channel traffic trend (GA4 sampled sessions)")
            fig_ch.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(255,255,255,0.04)",
                margin=dict(l=12, r=12, t=55, b=12),
            )
            fig_ch.update_xaxes(showgrid=False)
            fig_ch.update_yaxes(gridcolor="rgba(255,255,255,0.10)", zeroline=False)
            charts["channels"] = fig_to_html(fig_ch)

        CHART_CACHE[cache_key] = {"charts": charts, "totals": totals}

    missing_count = int(missing_table.shape[0])
    missing_activities = int(missing_table["Floodlight Activity Name"].nunique()) if missing_count else 0
    spike_count = int(spike_table.shape[0])
    spike_days = int(pd.Series(spike_table["Date"]).nunique()) if spike_count else 0

    overall_summary = compute_overall_summary(
        adv_name=adv_name,
        health=health,
        spikes_adv=spikes_adv,
        missing_adv=missing_adv,
        ga4_adv=ga4_adv
    )

    return {
        "opts_df": opts,
        "adv_id": adv_id,
        "adv_name": adv_name,
        "health": health,
        "dot_class": band_dot_class(health["band"]),
        "spike_table_html": spike_table.head(50).to_html(index=False, classes="table", border=0),
        "missing_table_html": missing_table.head(50).to_html(index=False, classes="table", border=0),
        "charts": charts,
        "totals": totals,
        "missing_count": missing_count,
        "missing_activities": missing_activities,
        "spike_count": spike_count,
        "spike_days": spike_days,
        "missing_events_total": missing_events_total,   # ✅ used in metric tile
        "overall_summary": overall_summary,             # ✅ new summary tab
    }

# =========================
# Routes
# =========================
@app.get("/")
def index():
    _, _, _, opts = get_data()
    adv_id = request.args.get("adv_id")
    adv_id = int(adv_id) if adv_id is not None else int(opts.iloc[0]["Advertiser ID"])

    use_llm = USE_LLM_DEFAULT_ON_LOAD

    payload = compute_dashboard_payload(adv_id)
    opts_records = payload["opts_df"].to_dict(orient="records")

    return render_template(
        "index.html",
        opts=opts_records,
        adv_id=payload["adv_id"],
        adv_name=payload["adv_name"],
        use_llm=use_llm,
        health=payload["health"],
        dot_class=payload["dot_class"],
        spike_table_html=payload["spike_table_html"],
        missing_table_html=payload["missing_table_html"],
        charts=payload["charts"],
        totals=payload["totals"],
        CHANNELS_ORDER=CHANNELS_ORDER,
        GTM_URL=GTM_URL,
        GA4_URL=GA4_URL,
        missing_count=payload["missing_count"],
        missing_activities=payload["missing_activities"],
        spike_count=payload["spike_count"],
        spike_days=payload["spike_days"],
        missing_events_total=payload["missing_events_total"],
        overall_summary=payload["overall_summary"],
    )

@app.post("/start_job")
def start_job():
    data = request.get_json(force=True, silent=True) or {}
    adv_id = int(data.get("adv_id", 0))
    job_id = str(uuid.uuid4())

    JOBS[job_id] = {"status": "running", "adv_id": adv_id, "result": None, "error": None}
    EXEC.submit(run_llm_job, job_id, adv_id)
    return jsonify({"job_id": job_id})

@app.get("/job_status")
def job_status():
    job_id = request.args.get("job_id", "")
    if not job_id or job_id not in JOBS:
        return jsonify({"status": "missing"}), 404
    j = JOBS[job_id]
    return jsonify({"status": j["status"], "error": j.get("error")})

@app.get("/job_result")
def job_result():
    job_id = request.args.get("job_id", "")
    if not job_id or job_id not in JOBS:
        return jsonify({"status": "missing"}), 404
    j = JOBS[job_id]
    if j["status"] != "done":
        return jsonify({"status": j["status"]}), 400
    return jsonify({"status": "done", "result": j["result"]})

@app.get("/export/<kind>")
def export(kind: str):
    spikes_df, missing_df, ga4_df, opts = get_data()
    adv_id = int(request.args.get("adv_id", opts.iloc[0]["Advertiser ID"]))

    ga4_adv = ga4_df[ga4_df["Advertiser ID"] == adv_id].copy()
    spikes_adv = spikes_df[spikes_df["Advertiser ID"] == adv_id].copy() if "Advertiser ID" in spikes_df.columns else pd.DataFrame()
    missing_adv = missing_df[missing_df["Advertiser ID"] == adv_id].copy() if "Advertiser ID" in missing_df.columns else pd.DataFrame()

    if kind == "ga4":
        csv = ga4_adv.to_csv(index=False)
        name = f"GA4_{adv_id}.csv"
    elif kind == "spikes":
        csv = spikes_adv.to_csv(index=False) if not spikes_adv.empty else "Problem Type,Date\n"
        name = f"Spikes_{adv_id}.csv"
    elif kind == "missing":
        csv = missing_adv.to_csv(index=False) if not missing_adv.empty else "Problem Type,Date\n"
        name = f"Missing_{adv_id}.csv"
    else:
        return "Unknown export kind", 400

    return Response(csv, mimetype="text/csv", headers={"Content-Disposition": f"attachment; filename={name}"})

if __name__ == "__main__":
    print("Starting Flask app...")
    app.run(host="127.0.0.1", port=5000, debug=True)
