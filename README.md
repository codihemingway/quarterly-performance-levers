# Quarterly Performance Lever Automation

This repository contains a prototype automation agent for weekly quarterly performance lever monitoring and CS/EM playbook generation.

## What it does

- Parses Mode dashboard exports or sample performance data
- Compares actual enrollments with quarterly OKR targets
- Detects enrollment gaps >2%
- Applies lever selection rules for email reactivation, deferment, or throttle pause
- Generates a production-ready CS/EM playbook document
- Outputs a Slack-ready summary

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python -m agent.main --input sample_data/mode_dashboard_sample.csv --output outputs/weekly_playbook.md
```

To send a Slack notification, set `SLACK_WEBHOOK_URL` in your environment.

## Structure

- `agent/` — core workflow modules
- `templates/` — playbook templates
- `sample_data/` — example Mode export input
- `outputs/` — generated summaries and playbooks

## Notes

This is a scaffold for a Claude-powered automation agent. The decision engine and playbook template are intentionally modular so you can connect them to real Mode exports, Glean docs, and Slack.
