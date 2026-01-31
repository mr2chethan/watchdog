# Watchdog - 10 Minute Demo Script

## Slide 1: The Hook (30 sec)

**Title:** "The Invisible $214/Hour Leak"

**Say:**
> "Every ad account has hidden configuration drift that silently wastes budget. 
> A single broken Floodlight pixel can waste $5,000 per day while your dashboards show everything is 'fine'.
> Watchdog is an agent that catches these invisible leaks before they drain your budget."

---

## Slide 2: What Watchdog Is (1 min)

**Title:** "Three Agents Working as a Tribunal"

**Show diagram:**
```
TECHNICIAN → AUDITOR → CFO
(Detective)   (Risk)    (Narrator)
```

**Say:**
> "Watchdog is three specialized agents:
> 1. **Technician** - Walks a decision tree encoded from media buyer knowledge. Pure logic, no AI.
> 2. **Auditor** - Checks GA4 governance rules. Regex and pandas, no AI.
> 3. **CFO** - Uses LLM to generate the 'scary story' - translates errors into dollars at risk.
>
> **Key insight:** AI is used for explanation and reasoning, NOT for detection. Detection is deterministic."

---

## Slide 3: Agent Behavior (1 min)

**Title:** "When to Act vs Stay Silent"

**Show table:**
| Condition | Agent Action |
|-----------|--------------|
| Dead pixel (no conversions 7+ days) | **ACT** - P0 Critical |
| GTM ID ≠ DV360 ID | **ACT** - P1 High |
| Everything configured correctly | **STAY SILENT** |

**Say:**
> "The agent continuously observes your account data.
> It only speaks when something is wrong.
> It stays silent when everything is healthy - no alert fatigue.
> This is what makes it trustworthy."

---

## Slide 4: The Decision Tree (1 min)

**Title:** "Tribal Knowledge → Agent Logic"

**Show JSON snippet:**
```json
{
  "node": "Pixel firing",
  "branches": [
    {"condition": "Cookie consented == 0", "next": "Check consent blocking"},
    {"condition": "GTM linked", "next": "Match IDs"}
  ]
}
```

**Say:**
> "This decision tree came from our media buyer's debugging workflow.
> It's the exact same steps they would take manually.
> The agent walks this tree programmatically - no hallucinations, no guessing.
> **The decision tree IS the law. The agent IS the lawyer.**"

---

## Slide 5: Reasoning Chain Example (1.5 min)

**Title:** "How Agent Connects Signals to Outcomes"

**Show finding:**
```
🔴 P0: Dead Pixel Detected

Reasoning:
1. Loaded DV360 record for ACME Corp
2. Floodlight Activity: FL_12345
3. Last conversion: 47 days ago (threshold: 7 days) → FAIL
4. Cookie counts show it WAS working before
5. Conclusion: Pixel died silently

Impact: $2,450/day × 47 days = $115,150 wasted
```

**Say:**
> "Every finding shows its work. Full transparency.
> You can see exactly WHY the agent flagged this.
> The daily spend is calculated - this isn't abstract, it's real money at risk."

---

## Slide 6: Prioritization (1 min)

**Title:** "P0 / P1 / P2 Triage"

**Show:**
- **P0 Critical:** Revenue directly at risk → Fix in hours
- **P1 High:** Data quality compromised → Fix in days  
- **P2 Medium:** Best practice violations → Fix when convenient

**Say:**
> "Not all issues are equal. The agent prioritizes by business impact.
> P0 issues appear first because they're burning money right now.
> P2 issues are noted but won't wake you up at night."

---

## Slide 7: The LLM's Job (1 min)

**Title:** "CFO Agent - The Scary Story"

**Show example narrative:**
> "This account is hemorrhaging money due to invisible tracking failures. 
> With $73,343 in daily spend at risk, every hour of inaction compounds the waste."

**Say:**
> "The CFO agent takes all the technical findings and translates them into executive language.
> It quantifies the TOTAL impact - not individual errors, but cumulative risk.
> This is where the LLM adds value - making technical issues understandable to stakeholders."

---

## Slide 8: Live Demo (3 min)

**[SWITCH TO LIVE APP]**

**Demo flow:**
1. Open Watchdog → Show the two tabs
2. Click "Run Account Audit" → Watch live reasoning stream
3. Point out: "See the agent thinking out loud"
4. Show P0/P1 findings appearing with reasoning chains
5. Show CFO narrative appearing after batch
6. Switch to Tab 2 → Select advertiser → Run anomaly detection
7. Show charts, AI summaries, recommendations

**Say during demo:**
> "Watch the agent work in real-time...
> See the reasoning chain for each finding...
> Notice how it only surfaces actionable issues...
> The CFO translates this into business impact..."

---

## Slide 9: Why Trust This Agent? (30 sec)

**Title:** "Glass Box, Not Black Box"

**Bullet points:**
- Deterministic detection (no hallucinations)
- Transparent reasoning (shows its work)
- Quantified impact (speaks in dollars)
- Conservative (doesn't cry wolf)
- LLM for narrative, not detection

**Say:**
> "A marketer can trust this because they can SEE exactly why each issue was flagged.
> There's no magic. The logic is visible. The AI just makes it readable."

---

## Slide 10: Summary (30 sec)

**Title:** "An Agent You'd Trust Overnight"

**Key points:**
- Observe → Reason → Recommend (continuously)
- AI used meaningfully (narrative, not detection)
- Recommendations are actionable with quantified impact
- Simple. Thoughtful. Explainable.

**Close with:**
> "Watchdog is an agent you'd trust to watch your accounts overnight.
> Questions?"

---

# Q&A Prep

**Likely questions and answers:**

**Q: How would this integrate with real DV360/GA4?**
> "The agents are designed to consume CSV exports. Real integration would be API calls to DV360/GA4 reporting endpoints, but the agent logic stays the same."

**Q: Why not use LLM for detection too?**
> "Deterministic logic is trustworthy - no hallucinations. LLMs are great for explanation and reasoning, but for detection you want consistent, auditable rules."

**Q: How often would this run in production?**
> "It could run continuously, but practically - hourly or daily batches would catch drift without overwhelming users with alerts."

**Q: What if the decision tree is wrong?**
> "Great question - the tree is editable JSON. Media buyers can update it as their knowledge evolves. The agent just follows whatever logic they define."

**Q: How do you handle false positives?**
> "The prioritization system helps - P2 issues won't alarm anyone. And because reasoning is visible, users can quickly dismiss false positives and potentially update the rules."
