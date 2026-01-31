"""
Anomaly Detection Agent - Detects Floodlight spikes and missing data
Ported from Flask app to work with Streamlit
"""
import os
import json
from pathlib import Path
from typing import List, Generator
from datetime import datetime

import pandas as pd
import plotly.express as px

# =========================
# CONFIG
# =========================
DATA_DIR = Path(__file__).parent.parent / "data" / "anomalies"
SPIKES_FILE = DATA_DIR / "Floodlight_Report_20260130_125843_1605197602_5503673738_Spikes.csv"
MISSING_FILE = DATA_DIR / "Floodlight_Report_20260130_125843_1605197602_5503673738_Missing.csv"
GA4_FILE = DATA_DIR / "GA4_Sample_Traffic_from_Floodlight_60days.csv"

CHANNEL_COL = "GA4 Default Channel Group"
SESSIONS_COL = "Sessions (sampled)"
DATE_COL = "Date"
IMPR_TOTAL_COL = "Floodlight Impressions (total/day)"
CHANNELS_ORDER = ["Organic Search", "Direct", "Referral", "Paid Search", "Organic Social"]


def get_secret(key: str, default: str = None) -> str:
    """Get secret from Streamlit secrets or environment."""
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except:
        pass
    return os.getenv(key, default)


# =========================
# Data Loading
# =========================
def safe_int_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def read_inputs():
    """Read and prepare all input CSVs."""
    # Check if files exist
    if not SPIKES_FILE.exists() or not MISSING_FILE.exists() or not GA4_FILE.exists():
        # Only error if all are missing, otherwise try to load what we can
        pass

    try:
        spikes = pd.read_csv(SPIKES_FILE) if SPIKES_FILE.exists() else pd.DataFrame()
        missing = pd.read_csv(MISSING_FILE) if MISSING_FILE.exists() else pd.DataFrame()
        ga4 = pd.read_csv(GA4_FILE) if GA4_FILE.exists() else pd.DataFrame()
    except Exception as e:
        print(f"Error loading CSVs: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if not spikes.empty:
        spikes.columns = [c.strip() for c in spikes.columns]
        if "Date" in spikes.columns:
            spikes["Date"] = pd.to_datetime(spikes["Date"], errors="coerce")
        if "Advertiser ID" in spikes.columns:
            spikes["Advertiser ID"] = safe_int_series(spikes["Advertiser ID"])
        if "Floodlight Activity ID" in spikes.columns:
            spikes["Floodlight Activity ID"] = safe_int_series(spikes["Floodlight Activity ID"])

    if not missing.empty:
        missing.columns = [c.strip() for c in missing.columns]
        if "Missing Date" in missing.columns:
            missing["Missing Date"] = pd.to_datetime(missing["Missing Date"], errors="coerce")
        if "Advertiser ID" in missing.columns:
            missing["Advertiser ID"] = safe_int_series(missing["Advertiser ID"])
        if "Floodlight Activity ID" in missing.columns:
            missing["Floodlight Activity ID"] = safe_int_series(missing["Floodlight Activity ID"])

    if not ga4.empty:
        ga4.columns = [c.strip() for c in ga4.columns]
        if DATE_COL in ga4.columns:
            ga4[DATE_COL] = pd.to_datetime(ga4[DATE_COL], errors="coerce")
        if "Advertiser ID" in ga4.columns:
            ga4["Advertiser ID"] = safe_int_series(ga4["Advertiser ID"])

    return spikes, missing, ga4


def get_advertiser_options(ga4: pd.DataFrame) -> pd.DataFrame:
    """Get unique advertiser options from GA4 data."""
    return (
        ga4[["Advertiser", "Advertiser ID"]]
        .dropna(subset=["Advertiser ID"])
        .drop_duplicates()
        .sort_values(["Advertiser", "Advertiser ID"])
        .reset_index(drop=True)
    )


# =========================
# Health Score
# =========================
def compute_health_score(spikes_adv: pd.DataFrame, missing_adv: pd.DataFrame, days_in_window: int) -> dict:
    """Calculate account health score based on spike and missing data."""
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


# =========================
# Problem Tables
# =========================
def build_spike_problems_table(spikes_adv: pd.DataFrame) -> pd.DataFrame:
    """Build a table of spike problems."""
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
    """Find continuous date ranges in a series."""
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
    """Build a table of missing floodlight problems."""
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


# =========================
# GA4 Channel Analysis
# =========================
def channel_breakdown_on_date(ga4_adv: pd.DataFrame, d: pd.Timestamp) -> pd.DataFrame:
    """Get channel breakdown for a specific date."""
    g = ga4_adv[ga4_adv[DATE_COL].dt.normalize() == pd.to_datetime(d).normalize()].copy()
    if g.empty:
        return pd.DataFrame(columns=[CHANNEL_COL, SESSIONS_COL])
    out = g.groupby(CHANNEL_COL, as_index=False)[SESSIONS_COL].sum().sort_values(SESSIONS_COL, ascending=False)
    return out


def channel_baseline_stats(ga4_adv: pd.DataFrame) -> pd.DataFrame:
    """Calculate baseline statistics for each channel."""
    tmp = ga4_adv.copy()
    tmp["_date_norm"] = tmp[DATE_COL].dt.normalize()
    daily = tmp.groupby(["_date_norm", CHANNEL_COL], as_index=False)[SESSIONS_COL].sum()
    stats = daily.groupby(CHANNEL_COL)[SESSIONS_COL].agg(["mean", "std", "max"]).reset_index()
    stats["std"] = stats["std"].fillna(0)
    return stats


def infer_spike_cause_from_ga4(ga4_adv: pd.DataFrame, spike_date: pd.Timestamp) -> dict:
    """Infer the likely cause of a spike from GA4 channel data."""
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


# =========================
# Time Series for Charts
# =========================
def spikes_impressions_by_day(spikes_adv: pd.DataFrame) -> pd.DataFrame:
    """Aggregate spike impressions by day."""
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
    """Count missing events by day."""
    if missing_adv.empty or "Missing Date" not in missing_adv.columns:
        return pd.DataFrame(columns=["day", "missing_events"])
    tmp = missing_adv.copy()
    tmp["_day"] = pd.to_datetime(tmp["Missing Date"], errors="coerce").dt.normalize()
    out = tmp.groupby("_day", as_index=False).size()
    out.rename(columns={"_day": "day", "size": "missing_events"}, inplace=True)
    return out.sort_values("day")


def ga4_impressions_by_day(ga4_adv: pd.DataFrame) -> pd.DataFrame:
    """Get GA4 impressions by day."""
    if ga4_adv.empty or DATE_COL not in ga4_adv.columns or IMPR_TOTAL_COL not in ga4_adv.columns:
        return pd.DataFrame(columns=["day", "ga4_impressions"])
    tmp = ga4_adv.copy()
    tmp["_day"] = pd.to_datetime(tmp[DATE_COL], errors="coerce").dt.normalize()
    tmp[IMPR_TOTAL_COL] = pd.to_numeric(tmp[IMPR_TOTAL_COL], errors="coerce")
    out = tmp.groupby("_day", as_index=False)[IMPR_TOTAL_COL].max()
    out.rename(columns={"_day": "day", IMPR_TOTAL_COL: "ga4_impressions"}, inplace=True)
    return out.sort_values("day")


# =========================
# Chart Builders
# =========================
def build_issue_history_chart(spikes_adv: pd.DataFrame, missing_adv: pd.DataFrame):
    """Build combined issue history chart."""
    miss = missing_events_by_day(missing_adv)
    spike = spikes_impressions_by_day(spikes_adv)
    merged = pd.merge(miss, spike, on="day", how="outer").fillna(0).sort_values("day")
    if merged.empty:
        return None
    melted = merged.melt(id_vars=["day"], value_vars=["missing_events", "spike_impressions"], var_name="metric", value_name="value")
    fig = px.line(melted, x="day", y="value", color="metric", title="Issue History (Missing Events + Spike Impressions)")
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.04)",
        margin=dict(l=12, r=12, t=55, b=12),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.10)", zeroline=False)
    return fig


def build_ga4_impressions_chart(ga4_adv: pd.DataFrame):
    """Build GA4 impressions trend chart."""
    gbd = ga4_impressions_by_day(ga4_adv)
    if gbd.empty:
        return None
    fig = px.line(gbd, x="day", y="ga4_impressions", title="Floodlight Impressions from GA4 (max/day)")
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.04)",
        margin=dict(l=12, r=12, t=55, b=12),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.10)", zeroline=False)
    return fig


def build_channel_trend_chart(ga4_adv: pd.DataFrame):
    """Build channel trend chart."""
    if ga4_adv.empty:
        return None
    ts = ga4_adv.groupby([DATE_COL, CHANNEL_COL], as_index=False)[SESSIONS_COL].sum()
    ts[CHANNEL_COL] = pd.Categorical(ts[CHANNEL_COL], categories=CHANNELS_ORDER, ordered=True)
    ts = ts.sort_values([DATE_COL, CHANNEL_COL])
    fig = px.line(ts, x=DATE_COL, y=SESSIONS_COL, color=CHANNEL_COL, title="5-Channel Traffic Trend (GA4 Sampled Sessions)")
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.04)",
        margin=dict(l=12, r=12, t=55, b=12),
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.10)", zeroline=False)
    return fig


# =========================
# Overall Summary
# =========================
def compute_overall_summary(adv_name: str, health: dict, spikes_adv: pd.DataFrame, missing_adv: pd.DataFrame, ga4_adv: pd.DataFrame) -> dict:
    """Compute overall summary statistics."""
    fl_names = set()
    if not spikes_adv.empty and "Floodlight Activity Name" in spikes_adv.columns:
        fl_names |= set(spikes_adv["Floodlight Activity Name"].dropna().astype(str).unique())
    if not missing_adv.empty and "Floodlight Activity Name" in missing_adv.columns:
        fl_names |= set(missing_adv["Floodlight Activity Name"].dropna().astype(str).unique())
    total_activities = int(len(fl_names))

    missing_events_total = int(len(missing_adv))
    spike_rows_total = int(len(spikes_adv))

    # Spike channel drivers
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

    # Reliability verdict
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

    # History stats
    miss_by_day = missing_events_by_day(missing_adv)
    spike_by_day = spikes_impressions_by_day(spikes_adv)

    last_missing = str(miss_by_day["day"].max().date()) if not miss_by_day.empty else None
    last_spike = str(spike_by_day["day"].max().date()) if not spike_by_day.empty else None

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
# LLM Prompts (for Groq)
# =========================
def build_missing_prompt(adv_name: str, adv_id: int, missing_table: pd.DataFrame) -> str:
    """Build prompt for missing floodlight analysis."""
    rows = missing_table.head(10).to_dict(orient="records")
    return f"""You are a senior measurement analyst.

Advertiser: {adv_name} (ID {adv_id})

We detected missing Floodlight delivery ranges.
Return ONLY a JSON object (no markdown) with this schema:
{{
  "summary": "...",
  "likely_root_cause": "...",
  "recommendations": ["...", "...", "..."]
}}

Rules:
- Summarize overall issue; do NOT write per-activity cards.
- Recommendations must be concrete and GTM/Floodlight focused.
- Keep it under 150 words total.

Missing ranges sample (top 10):
{json.dumps(rows, indent=2, default=str)}
"""


def build_spike_prompt(adv_name: str, adv_id: int, spike_table: pd.DataFrame, ga4_adv: pd.DataFrame) -> str:
    """Build prompt for spike analysis."""
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
Return ONLY a JSON object (no markdown) with this schema:
{{
  "summary": "...",
  "likely_root_cause": "...",
  "recommendations": ["...", "...", "..."]
}}

Rules:
- Summarize overall spike behavior; do NOT write per-activity cards.
- Use GA4 driver hints if present.
- Keep it under 150 words total.

Spike sample (top 10 with GA4 hints):
{json.dumps(drivers, indent=2, default=str)}
"""


# =========================
# Main Agent Class
# =========================
class AnomalyAgent:
    """Agent for detecting and analyzing data anomalies."""
    
    def __init__(self):
        self.spikes_df = None
        self.missing_df = None
        self.ga4_df = None
        self.opts = None
        self._loaded = False
        
        # Groq client (lazy init)
        self.groq_client = None
        self.groq_model = "llama-3.1-8b-instant"
    
    def _ensure_data_loaded(self):
        """Lazy load data."""
        if self._loaded:
            return
        self.spikes_df, self.missing_df, self.ga4_df = read_inputs()
        self.opts = get_advertiser_options(self.ga4_df)
        self._loaded = True
    
    def _ensure_groq_initialized(self):
        """Lazy init Groq client."""
        if self.groq_client:
            return True
        api_key = get_secret("GROQ_API_KEY")
        if not api_key:
            return False
        try:
            from groq import Groq
            self.groq_client = Groq(api_key=api_key)
            return True
        except Exception as e:
            print(f"Failed to init Groq: {e}")
            return False
    
    def _generate_with_groq(self, prompt: str) -> str:
        """Generate text using Groq."""
        if not self._ensure_groq_initialized():
            return None
        try:
            response = self.groq_client.chat.completions.create(
                model=self.groq_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=350,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"Groq generation failed: {e}")
            return None
    
    def get_advertisers(self) -> list:
        """Get list of advertiser options."""
        self._ensure_data_loaded()
        return self.opts.to_dict(orient="records")
    
    def analyze(self, adv_id: int) -> Generator[dict, None, None]:
        """Run full analysis for an advertiser."""
        self._ensure_data_loaded()
        
        # Get advertiser info
        row = self.opts[self.opts["Advertiser ID"] == adv_id]
        if row.empty:
            adv_id = int(self.opts.iloc[0]["Advertiser ID"])
            row = self.opts[self.opts["Advertiser ID"] == adv_id]
        adv_name = str(row.iloc[0]["Advertiser"])
        
        yield {"type": "step", "step": f"📊 Anomaly Agent: Analyzing {adv_name}..."}
        
        # Filter data for this advertiser
        ga4_adv = self.ga4_df[self.ga4_df["Advertiser ID"] == adv_id].copy()
        spikes_adv = self.spikes_df[self.spikes_df["Advertiser ID"] == adv_id].copy() if "Advertiser ID" in self.spikes_df.columns else pd.DataFrame()
        missing_adv = self.missing_df[self.missing_df["Advertiser ID"] == adv_id].copy() if "Advertiser ID" in self.missing_df.columns else pd.DataFrame()
        
        yield {"type": "step", "step": "📈 Computing health score..."}
        
        # Compute metrics
        days_in_window = int(ga4_adv[DATE_COL].dt.normalize().nunique()) if not ga4_adv.empty else 0
        health = compute_health_score(spikes_adv, missing_adv, days_in_window)
        
        yield {"type": "step", "step": f"   Health Score: {health['score']}/100 ({health['band']})"}
        
        # Build tables
        spike_table = build_spike_problems_table(spikes_adv)
        missing_table = build_missing_problems_table(missing_adv)
        
        # Build charts
        yield {"type": "step", "step": "📊 Building visualization charts..."}
        charts = {
            "issue_history": build_issue_history_chart(spikes_adv, missing_adv),
            "ga4_impressions": build_ga4_impressions_chart(ga4_adv),
            "channel_trend": build_channel_trend_chart(ga4_adv),
        }
        
        # Compute summary
        overall_summary = compute_overall_summary(adv_name, health, spikes_adv, missing_adv, ga4_adv)
        
        # Channel totals
        channel_totals = None
        if not ga4_adv.empty:
            totals_series = ga4_adv.groupby(CHANNEL_COL)[SESSIONS_COL].sum()
            for ch in CHANNELS_ORDER:
                if ch not in totals_series.index:
                    totals_series.loc[ch] = 0
            channel_totals = totals_series[CHANNELS_ORDER].to_dict()
        
        # Generate AI summaries
        yield {"type": "step", "step": "🤖 Generating AI analysis with Groq..."}
        
        ai_summaries = {"missing": None, "spike": None}
        
        # Missing summary
        if not missing_table.empty:
            prompt = build_missing_prompt(adv_name, adv_id, missing_table)
            response = self._generate_with_groq(prompt)
            if response:
                try:
                    # Try to parse JSON
                    if response.strip().startswith("{"):
                        ai_summaries["missing"] = json.loads(response)
                    else:
                        # Find JSON in response
                        import re
                        match = re.search(r'\{.*\}', response, re.DOTALL)
                        if match:
                            ai_summaries["missing"] = json.loads(match.group())
                except:
                    pass
            
            if not ai_summaries["missing"]:
                ai_summaries["missing"] = {
                    "summary": f"Missing Floodlight delivery detected across {missing_table['Floodlight Activity Name'].nunique()} activities.",
                    "likely_root_cause": "Common causes: GTM tag not firing, consent/CMP blocking, or container changes.",
                    "recommendations": [
                        "Confirm Floodlight tags + triggers in GTM for affected activities.",
                        "Check container publish history around missing start dates.",
                        "Validate consent mode settings."
                    ]
                }
        else:
            ai_summaries["missing"] = {
                "summary": "No missing Floodlight delivery ranges detected.",
                "likely_root_cause": "N/A",
                "recommendations": ["Continue monitoring."]
            }
        
        # Spike summary
        if not spike_table.empty:
            prompt = build_spike_prompt(adv_name, adv_id, spike_table, ga4_adv)
            response = self._generate_with_groq(prompt)
            if response:
                try:
                    if response.strip().startswith("{"):
                        ai_summaries["spike"] = json.loads(response)
                    else:
                        import re
                        match = re.search(r'\{.*\}', response, re.DOTALL)
                        if match:
                            ai_summaries["spike"] = json.loads(match.group())
                except:
                    pass
            
            if not ai_summaries["spike"]:
                ai_summaries["spike"] = {
                    "summary": f"Spike behavior detected on {pd.Series(spike_table['Date']).nunique()} day(s).",
                    "likely_root_cause": "Likely traffic mix anomaly (spam/bots), attribution change, or campaign surge.",
                    "recommendations": [
                        "Inspect GA4 acquisition for spike dates.",
                        "Check spam/bot sources and apply filters.",
                        "Confirm no duplicate Floodlight firing."
                    ]
                }
        else:
            ai_summaries["spike"] = {
                "summary": "No sudden spikes detected.",
                "likely_root_cause": "N/A",
                "recommendations": ["Continue monitoring."]
            }
        
        yield {"type": "step", "step": "✅ Anomaly Agent analysis complete."}
        
        # Yield final report
        yield {
            "type": "anomaly_report",
            "data": {
                "adv_id": adv_id,
                "adv_name": adv_name,
                "health": health,
                "spike_table": spike_table,
                "missing_table": missing_table,
                "charts": charts,
                "channel_totals": channel_totals,
                "overall_summary": overall_summary,
                "ai_summaries": ai_summaries,
                "missing_events_total": int(len(missing_adv)),
            }
        }
