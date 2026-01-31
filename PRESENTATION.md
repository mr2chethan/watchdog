# Watchdog: Account Health & Readiness Agent

## Hackathon Submission - Atomic Ads AI Agent Challenge

---

# Slide 1: The Problem

## "The Invisible $214/Hour Leak"

**Every ad account has hidden configuration drift that silently wastes budget**

- Floodlight pixels break silently → Bidding optimizes on ghost conversions
- GTM misconfigurations go unnoticed → Attribution becomes unreliable  
- GA4 settings drift over time → Compliance risks accumulate
- No one monitors these 24/7 → Problems compound

**Real Impact:** A single broken pixel can waste $5,000+/day while appearing "fine" in dashboards.

---

# Slide 2: Our Solution - Watchdog

## An Agentic AI System That Watches Your Accounts Overnight

**Three specialized agents working as a "Tribunal":**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   TECHNICIAN    │───▶│    AUDITOR      │───▶│      CFO        │
│   (Detective)   │    │ (Risk Assessor) │    │   (Narrator)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
      │                        │                      │
   Walks the              Checks GA4              Generates
   Decision Tree         Governance Rules       "Scary Story"
   (NO LLM)               (NO LLM)              (LLM-Powered)
```

**Key Insight:** LLM is used for reasoning and explanation, NOT for detection logic.

---

# Slide 3: Agent Behavior - The Observe-Reason-Recommend Loop

## What Each Agent Observes

| Agent | Data Observed | Frequency |
|-------|--------------|-----------|
| **Technician** | DV360 Floodlight configs, GTM tags, Website network calls | Continuous batches |
| **Auditor** | GA4 property settings, URL parameters, Campaign names | Continuous batches |
| **CFO** | Aggregated findings from both agents | After each batch |
| **Anomaly** | Historical Floodlight data, GA4 channel sessions | On-demand |

## When Agent Acts vs Stays Silent

| Condition | Action |
|-----------|--------|
| Floodlight Activity ID missing | **ACT** - P0 Critical Issue |
| Last conversion > 7 days ago | **ACT** - P0 Dead Pixel |
| Cookie consented = 0, unconsented > 0 | **ACT** - P1 Consent Blocking |
| GTM ID matches DV360 ID | **SILENT** - Configuration correct |
| Campaign name follows snake_case | **SILENT** - Governance passed |
| Health score > 80% | **SILENT** - Account healthy |

---

# Slide 4: The Decision Tree - Tribal Knowledge Encoded

## Media Buyer's Debugging Workflow → Agent Logic

```json
{
  "node": "Pixel firing",
  "instruction": "Check DV360 Reports → Floodlight → Impressions",
  "branches": [
    {
      "condition": "Cookie consented == 0 AND unconsented == 0",
      "next_step": "Pixel not placed or not placed properly"
    },
    {
      "condition": "Cookie consented > 0 AND unconsented == 0", 
      "next_step": "Check consent/CMP blocking"
    },
    {
      "condition": "GTM linked",
      "next_step": "Match Advertiser ID on GTM & Floodlight ID on DV360"
    }
  ]
}
```

**The Decision Tree IS the Law. The Agent IS the Lawyer.**

Agent interprets each branch with business context and calculates impact.

---

# Slide 5: Reasoning - How We Connect Signals to Outcomes

## Example: Dead Pixel Detection

**Signal Observed:**
```
Advertiser: ACME Corp
Floodlight_Activity_ID: FL_12345
Last_Conversion_Date: 2025-12-15 (47 days ago)
Daily_Spend: $2,450
```

**Agent Reasoning Chain:**
1. Loaded DV360 record for ACME Corp
2. Decision Tree Node: "Pixel firing" → Check last conversion date
3. Last conversion was 47 days ago (threshold: 7 days) → **FAIL**
4. Cookie_Consented_Count > 0 → Pixel WAS working
5. Cookie_Unconsented_Count = 0 → No consent blocking
6. Conclusion: **Pixel died silently**

**Why This Matters:**
- Daily spend: $2,450 × 47 days = **$115,150 wasted**
- Bidding algorithm optimizing on stale signals
- Learning phase reset when fixed = more waste

---

# Slide 6: Prioritization - The P0/P1/P2 Triage System

## How Agent Prioritizes Findings

| Priority | Criteria | Example | Action Urgency |
|----------|----------|---------|----------------|
| **P0 (Critical)** | Revenue directly at risk | Dead pixel, No Floodlight ID | Fix within hours |
| **P1 (High)** | Data quality compromised | ID mismatch, Consent blocking | Fix within days |
| **P2 (Medium)** | Best practice violations | Wrong counting method, Bad naming | Fix when convenient |

## What Gets Ignored

The agent deliberately stays silent on:
- ✅ Correctly configured pixels
- ✅ Properties with healthy data retention
- ✅ Campaigns following naming conventions
- ✅ Accounts with health score > 80

**Principle:** Only surface actionable issues. Never cry wolf.

---

# Slide 7: LLM Usage - The CFO's "Scary Story"

## How AI is Used for Reasoning, Not Just Summarization

**Input to LLM:**
```python
{
  "findings": [62 P0 issues, 94 P1 issues],
  "daily_spend_at_risk": "$73,343.15",
  "monthly_risk": "$2,200,294.50",
  "worst_issues": ["Dead pixels on 15 advertisers", "GTM ID mismatches on 23 tags"]
}
```

**LLM Prompt (Reasoning-Focused):**
```
You are a CFO presenting to the executive team.
Generate a 2-3 sentence URGENT narrative that:
1. Quantifies the TOTAL business impact
2. Explains WHY this is happening (root cause)
3. Creates urgency without being alarmist
4. Recommends the SINGLE most important action
```

**LLM Output (Not Template):**
> "This account is hemorrhaging money due to invisible tracking failures. 
> Our audit identified 62 critical issues actively degrading campaign performance.
> With $73,343 in daily spend at risk, every hour of inaction compounds the waste.
> **Immediate action required:** Fix the 15 dead Floodlight pixels to restore signal quality."

---

# Slide 8: Explainable Recommendations

## Each Finding Answers: What? Why? Why Now? Risk?

**Example Finding:**

```
┌────────────────────────────────────────────────────────────────┐
│ 🔴 P0 CRITICAL: Counting Method Mismatch                       │
├────────────────────────────────────────────────────────────────┤
│ WHAT:  GTM tag "FL_Purchase" configured as "Standard"          │
│        DV360 Floodlight activity expects "Unique"               │
│                                                                │
│ WHY:   DV360 sees 1 conversion per user per day                │
│        GTM fires on EVERY purchase → Inflated ROAS             │
│        Bidding algorithm over-values this conversion           │
│                                                                │
│ WHY NOW: Daily spend on this activity: $4,200                  │
│          Running for 23 days → $96,600 potentially misattributed│
│                                                                │
│ RISK:  If you fix this, short-term ROAS will DROP (looks bad)  │
│        But actual performance will improve (reality)            │
│                                                                │
│ RECOMMENDATION: Update GTM tag counting to "Unique"             │
│                 Expect 2-3 day learning phase after fix        │
└────────────────────────────────────────────────────────────────┘
```

---

# Slide 9: Live Demo Walkthrough

## What You'll See in the Demo

**1. Agent Starts Monitoring**
- Loads 1000 DV360 records in batches
- Live reasoning log shows step-by-step thinking
- Batch status updates in sidebar

**2. Issues Detected in Real-Time**
- P0/P1 findings appear as they're discovered
- Each finding shows full reasoning chain
- Technical proof (actual data values)

**3. CFO Analysis Every 3rd Batch**
- Generates fresh narrative each time
- Quantifies cumulative risk
- Prioritizes recommendations

**4. Anomaly Detection Tab**
- Spike/Missing data visualization
- GA4 channel attribution for spikes
- AI-powered root cause analysis

---

# Slide 10: Architecture - Glass Box, Not Black Box

## Full Transparency in Agent Reasoning

```
┌─────────────────────────────────────────────────────────────────┐
│                        WATCHDOG UI                              │
│  ┌─────────────────┐  ┌─────────────────────────────────────┐   │
│  │ Agent Reasoning │  │ Live Findings                       │   │
│  │ (Live Stream)   │  │ ┌───────────────────────────────┐   │   │
│  │                 │  │ │ 🔴 P0: Dead Pixel - FL_12345  │   │   │
│  │ [10:23:45]      │  │ │    Reasoning: [1,2,3,4,5]     │   │   │
│  │ Technician:     │  │ │    Proof: Last conv 47d ago   │   │   │
│  │ Checking pixel  │  │ │    Daily Spend: $2,450        │   │   │
│  │ FL_12345...     │  │ └───────────────────────────────┘   │   │
│  │                 │  │ ┌───────────────────────────────┐   │   │
│  │ [10:23:46]      │  │ │ 🟡 P1: ID Mismatch            │   │   │
│  │ Last conversion │  │ │    ...                        │   │   │
│  │ 47 days ago     │  │ └───────────────────────────────┘   │   │
│  └─────────────────┘  └─────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 💰 CFO Analysis: "This account is hemorrhaging..."      │   │
│  │    Health Score: 34/100 | Monthly Risk: $2.2M           │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

# Slide 11: Trust & Explainability

## Would a Real Marketer Trust This Agent?

**Why YES:**

1. **Deterministic Detection** - No hallucinations in finding issues
   - Technician and Auditor use pure Python logic
   - Decision tree encodes real media buyer knowledge
   
2. **Transparent Reasoning** - Every finding shows its work
   - Step-by-step logic chain visible
   - Actual data values as proof
   
3. **Quantified Impact** - Speaks in dollars, not just errors
   - Daily spend at risk calculated
   - Monthly exposure projected
   
4. **Conservative Prioritization** - Doesn't cry wolf
   - Only surfaces actionable issues
   - Stays silent on healthy configurations

5. **LLM for Narrative, Not Detection** - AI generates the story
   - Hard logic catches the issues
   - AI makes it understandable

---

# Slide 12: Relevance to Real Atomic Ads Workflows

## This Solves Real, Recognizable Problems

**Current Reality:**
- Media buyers manually check configurations monthly (if ever)
- Issues discovered reactively when campaigns underperform
- Debugging is time-consuming detective work
- Knowledge lives in experts' heads

**With Watchdog:**
- Continuous automated monitoring (no manual checks)
- Proactive detection before budget is wasted
- Decision tree encodes expert knowledge
- Any team member can understand issues

**Time Saved:**
- Manual audit: 4-8 hours per account
- Watchdog: Continuous, automatic, prioritized

---

# Slide 13: What We Built in 24 Hours

## Prototype-First, Not Production

**Built:**
- ✅ 4 specialized agents (Technician, Auditor, CFO, Anomaly)
- ✅ Decision tree walker (tribal knowledge encoded)
- ✅ 8 GA4 governance checks (PII, consent, naming, etc.)
- ✅ LLM-powered executive narrative generation
- ✅ Real-time streaming UI with glass-box reasoning
- ✅ Priority-based triage system (P0/P1/P2)
- ✅ Financial impact quantification
- ✅ Anomaly detection with Plotly charts

**Assumptions Made:**
- Mock data simulates real DV360/GTM/GA4 exports
- 1000 rows to demonstrate scale
- Planted "traps" to show detection capability
- Groq/Gemini API for LLM (free tier)

**Not Built (by design):**
- Production integrations with DV360/GA4 APIs
- Real-time webhook listeners
- Historical trend analysis
- User management / auth

---

# Slide 14: Key Takeaways

## Simple. Thoughtful. Explainable.

1. **Agent Loop is Clear**
   - Observe → Reason → Recommend (continuously)
   - Each agent has defined scope and triggers

2. **AI is Used Meaningfully**
   - LLM generates narrative, not detections
   - Decision tree = deterministic, trustworthy
   - AI explains WHY issues matter

3. **Recommendations are Actionable**
   - What to do, why, why now, trade-offs
   - Prioritized by business impact
   - Quantified in dollars at risk

4. **Marketers Would Trust This**
   - Glass-box reasoning (not black box)
   - Stays silent when things are fine
   - Speaks their language (not tech jargon)

---

# Slide 15: Demo Time

## "What would I want to see if this agent were running on my account?"

**Opening Watchdog...**

1. Start continuous audit → Watch agents work
2. See P0 issues surface with reasoning chains
3. Watch CFO generate executive narrative
4. Switch to Anomaly tab → Analyze Floodlight data
5. View charts, AI summaries, recommendations

---

# Appendix: Technical Details

## Stack
- **Frontend:** Streamlit (Python)
- **Backend:** Pure Python agents (no framework)
- **LLM:** Groq (Llama 3.1) / Google Gemini
- **Charts:** Plotly
- **Data:** Pandas

## Agent Design
- **Technician:** Walks JSON decision tree programmatically
- **Auditor:** Regex + Pandas validation rules
- **CFO:** LLM prompt engineering for narrative
- **Anomaly:** Statistical spike/missing detection + LLM analysis

## Data Sources (Mock)
- `mock_dv360_audit_ready.csv` - 1000 Floodlight configs
- `mock_gtm_audit_ready.csv` - 1000 GTM tags
- `mock_ga4_audit_ready.csv` - 1000 GA4 properties
- `mock_website_scan_audit_ready.csv` - 1000 network scans

---

# Thank You

## Questions?

**Watchdog** - Account Health & Readiness Agent

*"Build an agent you would trust to watch your accounts overnight."*
