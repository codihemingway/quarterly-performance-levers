# Quarterly Performance Levers — System Overview

## What This System Does

Each week, this automation:
1. Reads a Mode dashboard CSV export with current enrollment data
2. Calculates the weekly and quarterly OKR gap
3. Recommends the appropriate lever (if action is needed)
4. Generates a formatted playbook
5. Creates a Google Doc with the playbook
6. Posts a summary + doc link to Slack (`#codis-claude-test`)

---

## Data Inputs

### Source: Mode Dashboard CSV

The CSV is exported from the Mode dashboard and contains one row per reporting period. Fields:

| Field | Description |
|---|---|
| `quarter` | The quarter being tracked (e.g. "Q2 2026") |
| `week` | Week number within the quarter (1–13) |
| `q2_okr` | The full-quarter OKR enrollment target (e.g. 351,000) |
| `q2_outlook` | Current projected full-quarter enrollment total (see calculation below) |
| `cumulative_enrollments` | Total enrollments recorded through the current week |
| `okrs_enrollments` | The weekly OKR enrollment target for this week |
| `target_enrollments` | Internal weekly target (may differ slightly from OKR) |
| `actual_enrollments` | Actual enrollments recorded this week |
| `forecast_enrollments` | Forecasted weekly enrollment rate for remaining weeks |
| `remaining_weeks` | Number of weeks left in the quarter |
| `marketing_reactivation_allowed` | Whether reactivation marketing is permitted (`yes`/`no`) |
| `marketing_status` | Marketing approval status (`approved`/`pending`/etc.) |
| `client_tier` | Client tier (1, 2, or 3) — affects lever eligibility |
| `sender` | Who sends communications (`Hinge Health` or partner name) |
| `reaction_eligibility_touchpoints` | Number of members eligible for reactivation outreach |

---

## How the Q2 Outlook Is Calculated

The Q2 Outlook is a forward projection of total quarterly enrollments:

```
Q2 Outlook = cumulative_enrollments + (remaining_weeks × forecast_enrollments)
```

**Example (Week 5, Q2 2026):**
- Cumulative enrollments through Week 5: **194,500**
- Remaining weeks: **7**
- Weekly forecast rate: **22,500/week**
- Q2 Outlook: 194,500 + (7 × 22,500) = **352,000**

This number is populated in the Mode dashboard and exported in the CSV. It reflects the most current forecast based on actuals to date plus the projected run rate for the remainder of the quarter.

---

## Decision Logic — How Levers Are Recommended

The script evaluates the current data against three lever conditions in priority order:

### Step 1: Calculate the Weekly Gap

```
gap = (actual_enrollments − okrs_enrollments) / okrs_enrollments × 100
```

A negative gap means underperformance; positive means overperformance.

### Step 2: Evaluate Lever Conditions

**Lever 1: Email Reactivation** (underperformance with time to act)
- Condition: gap ≤ −2% AND remaining weeks ≥ 4
- Additional eligibility: `marketing_reactivation_allowed = yes`, `marketing_status = approved`, `client_tier` in {1, 2, 3}, `sender = Hinge Health`, `reaction_eligibility_touchpoints > 0`
- Action: Defer 70% of email volume from the next eligible window
- Impact: Estimated shift = `reaction_eligibility_touchpoints × 0.70` enrollments from Q2 → Q3
- Workback: 7 days

**Lever 2: Mailer Flat Deferment** (overperformance at end of quarter)
- Condition: gap ≥ +2% AND remaining weeks ≤ 2
- Action: Defer 100% of qualifying mail volume into next quarter
- Workback: 30 days

**Lever 3: Throttle Pause** (underperformance with little time remaining)
- Condition: gap ≤ −2% AND remaining weeks < 4
- Action: Unthrottle planned communications for the next available window
- Workback: 6 days

**No action** is recommended if gap is within ±2% (within tolerance).

---

## Output: Weekly Playbook

The playbook is generated from a Jinja2 template (`templates/playbook.md.j2`) and includes:

1. **Q2 At-a-Glance** — quarterly OKR, outlook, gap, and the calculation behind the outlook
2. **Weekly OKR Status** — weekly target vs. actual vs. forecast, and the weekly % gap
3. **Recommendation** — lever name, rationale, estimated impact, workback window, and qualifying touchpoints
4. **Execution Plan** — standard four-step checklist for GROMO, CS, and EM
5. **FAQ** — standard Q&A covering why this lever, what deferment means, and who owns execution

---

## Integrations

### Google Docs (via Hinge Health Agent Gateway)
- The playbook is uploaded as a new Google Doc via the `gworkspace` MCP server at `https://agentgateway.security.hingehealth.net/mcp/gworkspace`
- Auth: Okta SSO + Google OAuth through the Agent Gateway (approved Hinge Health pattern — no service accounts)
- Each weekly run creates a new doc titled `"Week X · Month Day, Year — Lever Playbook"`
- The doc URL is included in the Slack notification

### Slack (via Slack MCP)
- Notifications post to `#codis-claude-test` via the official Slack MCP (`plugin:slack`)
- The message includes the Q2 at-a-glance summary, weekly gap, recommendation, and a link to the Google Doc

---

## How to Run

```bash
cd quarterly-performance-levers
source .venv/bin/activate
python -m agent.main --input sample_data/mode_dashboard_sample.csv
```

The `--output` flag is optional (defaults to `outputs/weekly_playbook.md`).

After the script generates the playbook, Claude Code handles the Google Doc creation and Slack post via MCP tools.

---

## File Structure

```
quarterly-performance-levers/
├── agent/
│   ├── data.py          # CSV parsing and ModeRow dataclass
│   ├── decision.py      # Gap calculation and lever recommendation logic
│   ├── playbook.py      # Jinja2 template rendering
│   ├── gdocs.py         # (Legacy) Google Docs service account code — superseded by gworkspace MCP
│   ├── slack_notify.py  # (Legacy) Slack webhook code — superseded by Slack MCP
│   └── main.py          # Entry point — orchestrates data → decision → playbook
├── templates/
│   └── playbook.md.j2   # Jinja2 playbook template
├── sample_data/
│   └── mode_dashboard_sample.csv
└── outputs/
    └── weekly_playbook.md   # Generated output (not committed)
```
