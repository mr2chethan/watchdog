"""
Watchdog UI - Streamlit Frontend
Glass Box Reasoning Display with Real-time Agent Updates

Features:
- Real-time streaming of agent reasoning steps
- Priority-coded findings display
- Health score visualization
- Executive narrative display
- Fix recommendations
"""
import streamlit as st
import time
import json
from datetime import datetime
import pandas as pd
import sys
from pathlib import Path

# Add backend to path for direct imports
backend_path = Path(__file__).parent.parent / "backend"
sys.path.insert(0, str(backend_path))

# Lazy imports to avoid startup issues
TechnicianAgent = None
AuditorAgent = None
CFOAgent = None
AnomalyAgent = None

def load_agents():
    """Lazy load agents to avoid import issues during startup."""
    global TechnicianAgent, AuditorAgent, CFOAgent, AnomalyAgent
    if TechnicianAgent is None:
        from technician_agent import TechnicianAgent as TA
        from auditor_agent import AuditorAgent as AA
        from cfo_agent import CFOAgent as CA
        from anomaly_agent import AnomalyAgent as AnomA
        TechnicianAgent = TA
        AuditorAgent = AA
        CFOAgent = CA
        AnomalyAgent = AnomA

# Page config
st.set_page_config(
    page_title="Watchdog - Account Health Agent",
    page_icon="🐕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS - matching friend's dark gradient design
st.markdown("""
<style>
    .stApp {
        background:
            radial-gradient(1200px 600px at 20% 0%, rgba(99,102,241,0.35), transparent 60%),
            radial-gradient(900px 500px at 100% 20%, rgba(34,197,94,0.25), transparent 55%),
            radial-gradient(800px 500px at 70% 100%, rgba(14,165,233,0.25), transparent 55%),
            linear-gradient(180deg, #070b14 0%, #0b1220 50%, #070b14 100%);
    }
    .finding-p0 {
        background-color: #ff4b4b;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
        color: white;
    }
    .finding-p1 {
        background-color: #ffa500;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
        color: black;
    }
    .finding-p2 {
        background-color: #00cc00;
        padding: 10px;
        border-radius: 5px;
        margin: 5px 0;
        color: black;
    }
    .agent-log {
        font-family: 'Courier New', monospace;
        font-size: 12px;
        background-color: #1a1a2e;
        padding: 10px;
        border-radius: 5px;
        color: #00ff00;
        max-height: 400px;
        overflow-y: auto;
    }
    .health-score-good {
        color: #22c55e;
        font-size: 48px;
        font-weight: bold;
    }
    .health-score-warning {
        color: #f59e0b;
        font-size: 48px;
        font-weight: bold;
    }
    .health-score-critical {
        color: #ef4444;
        font-size: 48px;
        font-weight: bold;
    }
    .metric-card {
        background: rgba(255,255,255,0.06);
        border: 1px solid rgba(255,255,255,0.10);
        padding: 20px;
        border-radius: 18px;
        text-align: center;
        box-shadow: 0 10px 30px rgba(0,0,0,.35);
    }
    /* Friend's design elements */
    .anomaly-card {
        background: rgba(255,255,255,0.06);
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 18px;
        box-shadow: 0 10px 30px rgba(0,0,0,.35);
        padding: 16px 18px;
        margin: 8px 0;
    }
    .ai-box {
        border: 1px solid rgba(96,165,250,0.22);
        background: rgba(96,165,250,0.06);
        border-radius: 18px;
        padding: 16px 18px;
        margin: 8px 0;
    }
    .pill {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 6px 12px;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,0.14);
        background: rgba(255,255,255,0.06);
        font-size: 12px;
        color: rgba(255,255,255,0.86);
    }
    .dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        display: inline-block;
    }
    .dot-excellent { background: #60a5fa; box-shadow: 0 0 0 3px rgba(96,165,250,0.15); }
    .dot-good { background: #22c55e; box-shadow: 0 0 0 3px rgba(34,197,94,0.15); }
    .dot-fair { background: #f59e0b; box-shadow: 0 0 0 3px rgba(245,158,11,0.15); }
    .dot-poor { background: #ef4444; box-shadow: 0 0 0 3px rgba(239,68,68,0.15); }
    .kpi-card {
        background: rgba(255,255,255,0.05);
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 16px;
        padding: 12px;
        text-align: center;
    }
    .kpi-value {
        font-size: 28px;
        font-weight: 800;
        margin-top: 4px;
    }
    .kpi-label {
        font-size: 11px;
        letter-spacing: .08em;
        text-transform: uppercase;
        color: rgba(255,255,255,0.65);
    }
    .section-title {
        font-size: 14px;
        text-transform: uppercase;
        letter-spacing: .10em;
        color: rgba(255,255,255,0.70);
        margin: 8px 0 10px 0;
    }
    .verdict-box {
        padding: 12px 14px;
        border-radius: 16px;
        border: 1px solid rgba(255,255,255,0.10);
        background: rgba(255,255,255,0.04);
    }
    .btn-gtm {
        display: inline-block;
        padding: 10px 14px;
        border-radius: 14px;
        text-decoration: none;
        border: 1px solid rgba(34,197,94,0.45);
        background: rgba(34,197,94,0.18);
        color: white;
        font-weight: 650;
    }
    .btn-ga4 {
        display: inline-block;
        padding: 10px 14px;
        border-radius: 14px;
        text-decoration: none;
        border: 1px solid rgba(239,68,68,0.45);
        background: rgba(239,68,68,0.16);
        color: white;
        font-weight: 650;
    }
</style>
""", unsafe_allow_html=True)


def get_health_score_color(score: int) -> str:
    """Get color class based on health score."""
    if score >= 70:
        return "health-score-good"
    elif score >= 40:
        return "health-score-warning"
    else:
        return "health-score-critical"


def display_finding(finding: dict):
    """Display a single finding with appropriate styling."""
    priority = finding.get('priority', 'P2')
    priority_label = finding.get('priority_label', 'UNKNOWN')
    
    if priority == 'P0':
        icon = "🔴"
        bg_color = "#ff4b4b"
        text_color = "white"
    elif priority == 'P1':
        icon = "🟡"
        bg_color = "#ffa500"
        text_color = "black"
    else:
        icon = "🟢"
        bg_color = "#00cc00"
        text_color = "black"
    
    with st.expander(f"{icon} {priority} {priority_label}: {finding.get('issue', 'Unknown Issue')}", expanded=False):
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("**Reasoning Chain:**")
            reasoning = finding.get('reasoning', [])
            for i, step in enumerate(reasoning, 1):
                st.markdown(f"{i}. {step}")
            
            st.markdown("**Technical Proof:**")
            st.code(finding.get('technical_proof', 'N/A'))
        
        with col2:
            st.markdown("**Details:**")
            st.write(f"Agent: {finding.get('agent', 'Unknown')}")
            st.write(f"Check: {finding.get('check', 'Unknown')}")
            if finding.get('daily_spend'):
                st.write(f"Daily Spend: ${finding.get('daily_spend', 0):,.2f}")
            if finding.get('advertiser_id'):
                st.write(f"Advertiser: {finding.get('advertiser_id')}")
            
            st.markdown("**Recommendation:**")
            st.info(finding.get('recommendation', 'Review and fix'))


def run_audit_with_streaming(limit: int = 100):
    """Run the audit with continuous streaming display."""
    
    # Load agents (lazy loading)
    load_agents()
    
    # Initialize session state for persistent tracking across re-runs
    if 'findings' not in st.session_state:
        st.session_state.findings = []
    if 'logs' not in st.session_state:
        st.session_state.logs = []
    
    # Create layout
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("🔍 Agent Reasoning (Live Stream)")
        log_container = st.empty()
        
    with col2:
        st.subheader("📊 Live Stats")
        stats_container = st.empty()
    
    st.divider()
    
    # Container for live findings
    findings_container = st.container()
    
    # Initialize agents
    technician = TechnicianAgent()
    auditor = AuditorAgent()
    cfo = CFOAgent()
    
    def update_logs(message: str):
        """Append log and handle rolling window."""
        st.session_state.logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
        # Keep last 100 logs
        if len(st.session_state.logs) > 100:
            st.session_state.logs.pop(0)
        log_container.markdown(f"<div class='agent-log'>{'<br>'.join(st.session_state.logs[-50:])}</div>", unsafe_allow_html=True)

    def update_stats(findings_list):
        p0 = len([f for f in findings_list if f.get('priority') == 'P0'])
        p1 = len([f for f in findings_list if f.get('priority') == 'P1'])
        p2 = len([f for f in findings_list if f.get('priority') == 'P2'])
        stats_container.markdown(f"""
        **Total Findings:** {len(findings_list)}  
        🔴 P0 Critical: {p0}  
        🟡 P1 High: {p1}  
        🟢 P2 Medium: {p2}
        """)

    update_logs("🚀 Entering Continuous Interleaved Mode...")
    progress_bar = st.progress(0, text="Initializing Agents...")
    
    # Permanent Batch Status Indicator in Sidebar (Fixed position)
    with st.sidebar:
        st.divider()
        st.markdown("### 🔄 Live Status")
        batch_status_container = st.empty()
        batch_status_container.info("Initializing...")
    
    # Generators (Dynamic Batch Size 20-30)
    tech_gen = technician.run_audit(limit=limit, min_batch_size=20, max_batch_size=30)
    audit_gen = auditor.run_audit(limit=limit, min_batch_size=20, max_batch_size=30)
    
    # Infinite Interleaved Loop with Monotonic Batch ID
    global_batch_id = 1
    
    while True:
        # Much slower simulation as requested
        import random
        sleep_time = random.randint(10, 15)
        time.sleep(sleep_time)
        # --- Technician Agent Turn ---
        batch_findings = []
        try:
            while True:
                event = next(tech_gen)
                event_type = event.get("type")
                
                if event_type == "finding":
                    st.session_state.findings.insert(0, event["data"]) # Add to top
                    # Memory Cap: Keep only last 100 findings to prevent crash
                    if len(st.session_state.findings) > 100:
                        st.session_state.findings.pop()
                    
                    batch_findings.append(event["data"])
                    # Display finding if P0 or P1
                    if event["data"].get("priority") in ["P0", "P1"]:
                        with findings_container:
                            display_finding(event["data"])
                
                elif event_type == "batch_start":
                    # Internal batch_id from agent is ignored for display
                    # internal_id = event.get("batch_id")
                    batch_size = event.get("size", 0)
                    batch_status_container.info(f"**Batch #{global_batch_id}**\n\n📊 Size: {batch_size} rows")
                    progress_bar.progress(1.0, text=f"🔧 Technician: Analyzing Batch #{global_batch_id} ({batch_size} rows)...")
                
                elif event.get("step"):
                    update_logs(f"[Batch {global_batch_id}] 🔧 Technician Agent: {event['step']}")
                
                elif event_type == "batch_complete":
                     break
                     
                time.sleep(0.05)
                
        except StopIteration:
            update_logs(f"Note: Data source exhausted. Resetting Technician stream...")
            tech_gen = technician.run_audit(limit=limit, min_batch_size=20, max_batch_size=30)
            time.sleep(0.5)
            continue # Skip CFO for this partial/empty turn
            
        # Run CFO on Technician Batch Findings (Limited frequency to save API quota)
        # Only run every 3rd batch OR if there are Critical (P0) issues
        should_run_chob = (global_batch_id % 3 == 0) or any(f.get('priority') == 'P0' for f in batch_findings)
        
        if batch_findings and should_run_chob:
             update_logs(f"💰 CFO Agent: Analyzing financial impact of Batch #{global_batch_id}...")
             for event in cfo.analyze(batch_findings, batch_id=global_batch_id, batch_size=len(batch_findings)):
                 if event.get("step"):
                     # CFO steps already contain agent name, just show batch context if not redundant
                     update_logs(f"[Batch {global_batch_id}] {event['step']}")
                 if event.get("type") == "cfo_report":
                     reportn = event["data"]
                     b_size = reportn.get('batch_size', len(batch_findings))
                     with findings_container:
                         st.markdown(f"""
                         <div style="background-color: #1e1e2e; padding: 15px; border-left: 5px solid #00ff00; margin: 10px 0;">
                             <strong>💰 CFO Analysis (Batch #{global_batch_id} | {b_size} Records)</strong><br>
                             {reportn.get('executive_narrative', 'No financial impact detected.')}
                         </div>
                         """, unsafe_allow_html=True)
        elif batch_findings:
             update_logs(f"💰 CFO Agent: Performing standard risk assessment for Batch #{global_batch_id} (Rapid Mode)")
                         
        global_batch_id += 1
        
        # --- Auditor Agent Turn ---
        batch_findings = []
        try:
             while True:
                event = next(audit_gen)
                event_type = event.get("type")
                
                if event_type == "finding":
                    st.session_state.findings.insert(0, event["data"])
                    if len(st.session_state.findings) > 100: st.session_state.findings.pop()
                    batch_findings.append(event["data"])
                    if event["data"].get("priority") in ["P0", "P1"]:
                        with findings_container:
                            display_finding(event["data"])

                elif event_type == "batch_start":
                    batch_size = event.get("size", 0)
                    batch_status_container.info(f"**Batch #{global_batch_id}**\n\n📊 Size: {batch_size} rows")
                    progress_bar.progress(1.0, text=f"📋 Auditor: Analyzing Batch #{global_batch_id} ({batch_size} rows)...")
                
                elif event.get("step"):
                    update_logs(f"[Batch {global_batch_id}] 📋 Auditor Agent: {event['step']}")
                    
                elif event_type == "batch_complete":
                     break
                     
                time.sleep(0.05)
        
        except StopIteration:
            update_logs(f"Note: Data source exhausted. Resetting Auditor stream...")
            audit_gen = auditor.run_audit(limit=limit, min_batch_size=20, max_batch_size=30)
            time.sleep(0.5)
            continue

        # Run CFO on Auditor Batch Findings (Limited frequency)
        should_run_audit_cfo = (global_batch_id % 3 == 0) or any(f.get('priority') == 'P0' for f in batch_findings)
        
        if batch_findings and should_run_audit_cfo:
             update_logs(f"💰 CFO Agent: Analyzing governance risk of Batch #{global_batch_id}...")
             for event in cfo.analyze(batch_findings, batch_id=global_batch_id, batch_size=len(batch_findings)):
                 if event.get("step"):
                     update_logs(f"[Batch {global_batch_id}] {event['step']}")
                 if event.get("type") == "cfo_report":
                     reportn = event["data"]
                     b_size = reportn.get('batch_size', len(batch_findings))
                     with findings_container:
                         st.markdown(f"""
                         <div style="background-color: #1e1e2e; padding: 15px; border-left: 5px solid #00ff00; margin: 10px 0;">
                             <strong>💰 CFO Analysis (Batch #{global_batch_id} | {b_size} Records)</strong><br>
                             {reportn.get('executive_narrative', 'No compliance risk details.')}
                         </div>
                         """, unsafe_allow_html=True)

        global_batch_id += 1

        # Update global stats
        if len(st.session_state.findings) > 200:
             st.session_state.findings = st.session_state.findings[:200]
        
        update_stats(st.session_state.findings)
        time.sleep(0.1)


def main():
    # Header
    st.title("🐕 Watchdog")
    st.markdown("### Account Health & Readiness Agent")
    st.markdown("*Continuously evaluates ad accounts for invisible setup, tracking, and measurement issues*")
    
    st.divider()
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Limit not used in infinite mode, set to None for full initial load
        limit = None
        
        st.info("Running in Continuous Simulation Mode")
        
        st.divider()
        
        st.markdown("### 🤖 Agents")
        st.markdown("""
        - **Technician**: Decision tree walker (DV360/GTM/Website)
        - **Auditor**: Governance rules checker (GA4)
        - **CFO**: Financial impact narrator (LLM)
        - **Anomaly**: Data quality monitor (Floodlight/GA4)
        """)
        
        st.divider()
        
        st.markdown("### 📁 Data Sources")
        st.markdown("""
        - DV360 Audit Data
        - GTM Tag Data
        - GA4 Property Data
        - Website Scan Data
        - Floodlight Reports
        """)
    
    # Main content - TABBED INTERFACE
    main_tab1, main_tab2 = st.tabs(["🔧 Setup & Tracking Audit", "📈 Data Anomaly Detection"])
    
    # ============================================
    # TAB 1: SETUP & TRACKING AUDIT (Original)
    # ============================================
    with main_tab1:
        run_button = st.button("🚀 Run Account Audit", type="primary", use_container_width=True, key="run_audit_btn")
        
        if run_button:
            # Continuous streaming mode - runs indefinitely displaying findings live
            run_audit_with_streaming(limit=limit)
        
        else:
            # Show placeholder content when not running
            st.info("👆 Click 'Run Account Audit' to start continuous account monitoring.")
            
            st.markdown("""
            ### What Watchdog Checks:
            
            **🔧 Technical Issues (Technician Agent)**
            - Missing or dead Floodlight pixels
            - Cookie consent blocking data
            - GTM and DV360 ID mismatches
            - Counting method discrepancies
            - Blocked network calls
            
            **📋 Governance Issues (Auditor Agent)**
            - PII in URL parameters (GDPR risk)
            - Data retention settings
            - Google Signals configuration
            - Campaign naming conventions
            - Referral exclusion lists
            - Consent Mode status
            
            **💰 Financial Impact (CFO Agent)**
            - Calculates daily spend at risk
            - Generates executive narratives
            - Prioritizes fixes by business impact
            """)
    
    # ============================================
    # TAB 2: DATA ANOMALY DETECTION (Friend's Design)
    # ============================================
    with main_tab2:
        # Load agents
        load_agents()
        
        try:
            anomaly_agent = AnomalyAgent()
            advertisers = anomaly_agent.get_advertisers()
            
            if not advertisers:
                st.warning("No advertisers found in the data files.")
            else:
                # Header card matching friend's design
                st.markdown("""
                <div class='anomaly-card'>
                    <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                        <div>
                            <div class='section-title'>Account Health • Fix AI Agent</div>
                            <div style="font-size:28px; font-weight:900; line-height:1.15;">
                                Problem → Cause → Fix
                            </div>
                            <div style="margin-top:8px; font-size:14px; color: rgba(255,255,255,0.65);">
                                Fast dashboard + AI Agent Insights for Floodlight anomalies
                            </div>
                        </div>
                        <div class='pill'>
                            <span class='dot dot-excellent'></span>
                            <span>LLM Ready</span>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                # Advertiser selector
                adv_options = {f"{a['Advertiser']} | {a['Advertiser ID']}": a['Advertiser ID'] for a in advertisers}
                selected_adv = st.selectbox("Select Advertiser", list(adv_options.keys()), key="adv_selector")
                adv_id = adv_options[selected_adv]
                
                # Run analysis button
                if st.button("🔍 Analyze Data Anomalies", type="primary", use_container_width=True, key="run_anomaly_btn"):
                    
                    # Log container
                    log_placeholder = st.empty()
                    logs = []
                    
                    # Run analysis
                    report_data = None
                    for event in anomaly_agent.analyze(adv_id):
                        if event.get("type") == "step":
                            logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {event['step']}")
                            log_placeholder.markdown(f"<div class='agent-log'>{'<br>'.join(logs[-10:])}</div>", unsafe_allow_html=True)
                            time.sleep(0.1)
                        elif event.get("type") == "anomaly_report":
                            report_data = event["data"]
                    
                    if report_data:
                        log_placeholder.empty()
                        
                        health = report_data["health"]
                        summary = report_data["overall_summary"]
                        
                        # Get dot class based on health band
                        band = health.get("band", "").lower()
                        dot_class = "dot-excellent" if "excellent" in band else ("dot-good" if "good" in band else ("dot-fair" if "fair" in band else "dot-poor"))
                        score_class = "health-score-good" if health["score"] >= 75 else ("health-score-warning" if health["score"] >= 55 else "health-score-critical")
                        
                        # KPI Cards row (matching friend's grid-4)
                        st.markdown("<div class='section-title'>Key Metrics</div>", unsafe_allow_html=True)
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            st.markdown(f"""
                            <div class='kpi-card'>
                                <div class='kpi-label'>Health Score</div>
                                <div class='kpi-value'><span class='{score_class}'>{health['score']}</span>/100</div>
                                <div class='pill' style='margin-top:8px;'><span class='dot {dot_class}'></span>{health['band']}</div>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        with col2:
                            st.markdown(f"""
                            <div class='kpi-card'>
                                <div class='kpi-label'>Spike Days</div>
                                <div class='kpi-value'>{health['spike_days']}</div>
                                <div style='font-size:12px; color:rgba(255,255,255,0.65);'>Days with spike flags</div>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        with col3:
                            st.markdown(f"""
                            <div class='kpi-card'>
                                <div class='kpi-label'>Missing Events</div>
                                <div class='kpi-value'>{report_data['missing_events_total']}</div>
                                <div style='font-size:12px; color:rgba(255,255,255,0.65);'>Total missing rows</div>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        with col4:
                            st.markdown(f"""
                            <div class='kpi-card'>
                                <div class='kpi-label'>Days in Window</div>
                                <div class='kpi-value'>{health['days_in_window']}</div>
                                <div style='font-size:12px; color:rgba(255,255,255,0.65);'>Distinct GA4 dates</div>
                            </div>
                            """, unsafe_allow_html=True)
                        
                        st.markdown("<br>", unsafe_allow_html=True)
                        
                        # Overall Summary Card (matching friend's design)
                        st.markdown(f"""
                        <div class='anomaly-card'>
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <div>
                                    <div class='section-title'>Overall Summary</div>
                                    <div style="font-size:22px; font-weight:950; line-height:1.2;">
                                        {summary['adv_name']} — Tracking reliability snapshot
                                    </div>
                                </div>
                                <div class='pill'>
                                    <span class='dot {dot_class}'></span>
                                    <span>{summary['verdict']}</span>
                                </div>
                            </div>
                            <div style="height:12px;"></div>
                            <div class='verdict-box'>
                                <div style="font-weight:900; font-size:14px;">Reliability Verdict</div>
                                <div style="margin-top:6px; font-size:13px; color:rgba(255,255,255,0.85);">
                                    {summary['verdict_reason']}
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Top Spike Drivers
                        if summary.get("top_drivers"):
                            st.markdown("<div class='section-title'>Spike Drivers (GA4 Channel Dominance)</div>", unsafe_allow_html=True)
                            driver_df = pd.DataFrame(summary["top_drivers"])
                            st.dataframe(driver_df, use_container_width=True, hide_index=True)
                        
                        st.markdown("<br>", unsafe_allow_html=True)
                        
                        # AI Summaries (matching friend's ai-box design)
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            ai_missing = report_data["ai_summaries"].get("missing", {})
                            missing_table = report_data.get("missing_table")
                            missing_count = len(missing_table) if missing_table is not None else 0
                            st.markdown(f"""
                            <div class='ai-box'>
                                <div style="display:flex; justify-content:space-between; align-items:center;">
                                    <div style="font-size:16px; font-weight:900;">🛑 Missing Floodlights — AI Summary</div>
                                    <div class='pill'><span class='dot dot-poor'></span>{missing_count} ranges</div>
                                </div>
                                <div style="margin-top:12px;">
                                    <div><b>Summary:</b> {ai_missing.get('summary', 'N/A')}</div>
                                    <div style="margin-top:8px;"><b>Root Cause:</b> {ai_missing.get('likely_root_cause', 'N/A')}</div>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                            recs = ai_missing.get("recommendations", [])
                            if recs:
                                st.markdown("**Recommendations:**")
                                for r in recs[:5]:
                                    st.markdown(f"- {r}")
                            st.markdown("""<a class='btn-gtm' href='https://tagmanager.google.com/' target='_blank'>🟢 Go to Google Tag Manager →</a>""", unsafe_allow_html=True)
                        
                        with col2:
                            ai_spike = report_data["ai_summaries"].get("spike", {})
                            spike_table = report_data.get("spike_table")
                            spike_count = len(spike_table) if spike_table is not None else 0
                            st.markdown(f"""
                            <div class='ai-box'>
                                <div style="display:flex; justify-content:space-between; align-items:center;">
                                    <div style="font-size:16px; font-weight:900;">📈 Spikes — AI Summary</div>
                                    <div class='pill'><span class='dot dot-fair'></span>{spike_count} spikes</div>
                                </div>
                                <div style="margin-top:12px;">
                                    <div><b>Summary:</b> {ai_spike.get('summary', 'N/A')}</div>
                                    <div style="margin-top:8px;"><b>Root Cause:</b> {ai_spike.get('likely_root_cause', 'N/A')}</div>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                            recs = ai_spike.get("recommendations", [])
                            if recs:
                                st.markdown("**Recommendations:**")
                                for r in recs[:5]:
                                    st.markdown(f"- {r}")
                            st.markdown("""<a class='btn-ga4' href='https://analytics.google.com/' target='_blank'>🔴 Go to GA4 Analytics →</a>""", unsafe_allow_html=True)
                        
                        st.markdown("<br>", unsafe_allow_html=True)
                        
                        # Charts section
                        st.markdown("<div class='section-title'>Trends & Visualizations</div>", unsafe_allow_html=True)
                        charts = report_data.get("charts", {})
                        
                        if charts.get("issue_history"):
                            st.plotly_chart(charts["issue_history"], use_container_width=True)
                        
                        if charts.get("ga4_impressions"):
                            st.plotly_chart(charts["ga4_impressions"], use_container_width=True)
                        
                        # Channel Totals
                        if report_data.get("channel_totals"):
                            st.markdown("<div class='section-title'>Channel Session Totals</div>", unsafe_allow_html=True)
                            ch_cols = st.columns(5)
                            for i, (channel, sessions) in enumerate(report_data["channel_totals"].items()):
                                with ch_cols[i % 5]:
                                    st.markdown(f"""
                                    <div class='kpi-card'>
                                        <div class='kpi-label'>{channel}</div>
                                        <div class='kpi-value' style='font-size:20px;'>{sessions:,.0f}</div>
                                    </div>
                                    """, unsafe_allow_html=True)
                        
                        if charts.get("channel_trend"):
                            st.plotly_chart(charts["channel_trend"], use_container_width=True)
                        
                        st.markdown("<br>", unsafe_allow_html=True)
                        
                        # Problem Tables
                        st.markdown("<div class='section-title'>Detected Problems</div>", unsafe_allow_html=True)
                        
                        ptab1, ptab2 = st.tabs(["📈 Spikes", "❌ Missing Data"])
                        
                        with ptab1:
                            spike_table = report_data.get("spike_table")
                            if spike_table is not None and not spike_table.empty:
                                st.dataframe(spike_table.head(50), use_container_width=True, hide_index=True)
                            else:
                                st.success("No sudden spikes detected!")
                        
                        with ptab2:
                            missing_table = report_data.get("missing_table")
                            if missing_table is not None and not missing_table.empty:
                                st.dataframe(missing_table.head(50), use_container_width=True, hide_index=True)
                            else:
                                st.success("No missing floodlight data detected!")
                
                else:
                    # Placeholder when not running (matching friend's style)
                    st.markdown("""
                    <div class='anomaly-card'>
                        <div style="font-size:18px; font-weight:700;">👆 Select an advertiser and click 'Analyze Data Anomalies'</div>
                        <div style="margin-top:12px; color:rgba(255,255,255,0.75);">
                            This tab analyzes Floodlight conversion data for anomalies:
                        </div>
                        <div style="margin-top:16px; display:grid; grid-template-columns: repeat(3, 1fr); gap:12px;">
                            <div class='kpi-card'>
                                <div style="font-size:20px;">📈</div>
                                <div style="font-weight:700; margin-top:4px;">Spike Detection</div>
                                <div style="font-size:12px; color:rgba(255,255,255,0.65); margin-top:4px;">Sudden surges in Floodlight impressions</div>
                            </div>
                            <div class='kpi-card'>
                                <div style="font-size:20px;">❌</div>
                                <div style="font-weight:700; margin-top:4px;">Missing Data</div>
                                <div style="font-size:12px; color:rgba(255,255,255,0.65); margin-top:4px;">Floodlight activities that stopped firing</div>
                            </div>
                            <div class='kpi-card'>
                                <div style="font-size:20px;">🤖</div>
                                <div style="font-weight:700; margin-top:4px;">AI Insights</div>
                                <div style="font-size:12px; color:rgba(255,255,255,0.65); margin-top:4px;">Root cause analysis via Groq LLM</div>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
        
        except Exception as e:
            st.error(f"Error loading anomaly detection: {e}")
            st.info("Make sure the data files exist in data/anomalies/")


if __name__ == "__main__":
    main()

