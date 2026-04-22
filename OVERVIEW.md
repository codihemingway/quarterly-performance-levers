# Quarterly Performance Levers — How It Works

**Audience:** CS, EM, Growth Leadership, anyone who uses this dashboard
**Last updated:** April 2026

---

## What Is This?

This is a weekly dashboard that answers one question: **Is our enrollment on track, and if not, what lever should we pull?**

Every week, the Growth / Acquisition team runs a quick script that pulls the latest data and updates the webpage at:

> **https://codihemingway.github.io/quarterly-performance-levers/**

The page shows the current quarter's performance, channel activity, and Y2 renewals — and automatically recommends which of four levers to activate (or confirms no action is needed).

---

## Where Does the Data Come From?

### 1. Q2 Enrollment Metrics (OKR, Outlook, Gap)
- **Source:** Weekly CSV export from our Mode analytics dashboard
- **What it is:** The Q2 OKR (our enrollment target), the current enrollment outlook (projected total based on pacing), and the gap between them
- **How it's updated:** Manually exported each Monday and run through the script

### 2. Channel Touchpoints (Email & Direct Mail)
- **Source:** Databricks — queried live from our data warehouse each Monday
- **What it is:** How many email and direct mail touchpoints are in the upcoming send window, and how many actual enrollments they're projected to drive
- **SQL file:** `queries/channel_touchpoints.sql`
- **Updated automatically** every time the script runs

### 3. Y2 Renewals (Year 2 Subscription Renewals)
- **Source:** Databricks — queried live from our data warehouse each Monday
- **What it is:** How many members who completed Year 1 have started Year 2 subscriptions, broken out by program type (Enso, WPH/Chronic, Unmarketed) and channel (paid, unpaid, organic) — compared against our renewal OKR forecast
- **SQL file:** `queries/y2_renewals.sql`
- **Updated automatically** every time the script runs

---

## What Are the Four Levers?

| Lever | When to Use | Lead Time |
|---|---|---|
| **No Action Required** | Q2 outlook is at or above OKR | — |
| **Email Reactivation** | Q2 outlook is below OKR, email volume can be shifted | 7 days |
| **Mailer Flat Deferment** | Q2 outlook is above OKR, DM volume needs to move to Q3 | 30 days |
| **Throttle Pause** | Q2 outlook is below OKR and time is short | 6 days |

The script automatically recommends the right lever based on the gap between outlook and OKR, time remaining in the quarter, and channel volumes. The recommendation is pre-selected on the page but any lever can be selected manually.

---

## The Playbook Builder

The bottom of the page has a **Playbook Builder** — a form that pre-fills from the current data and generates the full CS/EM playbook document.

Fill in the Owner, any specific dates, and toggle the action items (EM/CS action needed, client notification, SFDC update), then click **Copy Full Playbook**. The formatted playbook is ready to paste directly into Slack or a Google Doc.

---

## How the Weekly Update Works

```
Every Monday morning:
  1. Export the Mode dashboard CSV
  2. Run:  python3 -m agent.main --input path/to/csv
  3. Script pulls live Databricks data (touchpoints + Y2 renewals)
  4. Script writes  docs/data.json  with all updated values
  5. Run:  git add docs/data.json && git commit -m "Week X update" && git push
  6. Webpage auto-updates (GitHub Pages picks up the new file)
```

No login required to view the page. No special software. Anyone with the link can open it.

---

## Key Files (For the Technical Team)

| File | Purpose |
|---|---|
| `docs/index.html` | The webpage — all HTML, CSS, and JavaScript in one file |
| `docs/data.json` | The data file the webpage reads — updated weekly by the script |
| `agent/main.py` | The Python script that generates data.json |
| `agent/databricks.py` | Connects to Databricks to pull live channel and Y2 renewal data |
| `queries/channel_touchpoints.sql` | SQL for email/DM touchpoint modeling |
| `queries/y2_renewals.sql` | SQL for Y2 renewal actuals vs OKR |

---

## Questions?

Reach out to **Codi Hemingway** (GROMO / Acquisition) for questions about the dashboard, data sources, or lever recommendations.
