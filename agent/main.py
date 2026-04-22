import argparse
import json
import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from .data import parse_mode_dashboard
from .decision import recommend_lever, calculate_gap, calculate_q2_gap
from .playbook import render_playbook
from .databricks import fetch_touchpoints, fetch_y2_renewals, fetch_q2_metrics


def build_summary(row, recommendation):
    q2_gap = (row.q2_outlook - row.q2_okr) / row.q2_okr * 100 if row.q2_okr else 0
    return (
        f"Q2 Lever Check - Week {row.week}, {row.quarter}\n\n"
        f"Q2 Outlook: {row.q2_outlook:,} vs OKR {row.q2_okr:,} ({q2_gap:+.1f}%)\n"
        f"Channel Touchpoints — Email: {row.email_touchpoints:,} | Direct Mail: {row.direct_mail_touchpoints:,}\n\n"
        f"Recommendation: {recommendation.name}\n"
        f"• {recommendation.estimated_impact}\n"
    )


def build_doc_values(row, recommendation, today: date) -> dict:
    """Snapshot of all formatted values as they appear in the rendered doc."""
    q2_gap = (row.q2_outlook - row.q2_okr) / row.q2_okr * 100 if row.q2_okr else 0
    total_touchpoints = row.email_touchpoints + row.direct_mail_touchpoints
    return {
        "doc_title": f"Week {row.week} · {today.strftime('%b %-d, %Y')}",
        "week_label": f"Week {row.week}",
        "week_num": str(row.week),
        "date_label": today.strftime("%b %-d, %Y"),
        "quarter": row.quarter,
        "q2_okr": f"{row.q2_okr:,}",
        "q2_outlook": f"{row.q2_outlook:,}",
        "gap_to_okr": f"{row.q2_outlook - row.q2_okr:+,}",
        "q2_gap_pct": f"{q2_gap:+.1f}%",
        "cumulative": f"{row.cumulative_enrollments:,}",
        "remaining_weeks": str(row.remaining_weeks),
        "forecast_rate": f"{row.forecast_enrollments:,}",
        "email_touchpoints": f"{row.email_touchpoints:,}",
        "dm_touchpoints": f"{row.direct_mail_touchpoints:,}",
        "total_touchpoints": f"{total_touchpoints:,}",
        "recommendation_name": recommendation.name,
        "rationale": recommendation.rationale,
        "estimated_impact": recommendation.estimated_impact,
        "action": recommendation.action,
    }


def _lever_id(recommendation) -> str:
    name = recommendation.name.lower()
    if "no action" in name:
        return "no_action"
    if "email" in name:
        return "email_reactivation"
    if "mailer" in name or "flat" in name:
        return "mailer_deferment"
    return "throttle_pause"


def _parse_y2_deferred(y2_table_text: str) -> list:
    rows = []
    for line in y2_table_text.strip().splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) == 2:
            try:
                rows.append({"week": parts[0], "volume": int(parts[1].replace(",", ""))})
            except ValueError:
                pass
    return rows


def generate_web_data(row, recommendation, y2_table_text: str, docs_dir: Path) -> Path:
    """Write docs/data.json for the GitHub Pages interactive playbook."""

    # Pull live Q2 OKR + Outlook from Databricks working_forecast.sql; fall back to CSV row
    try:
        _metrics = fetch_q2_metrics()
        q2_okr = _metrics["q2_okr"]
        q2_outlook = _metrics["q2_outlook"]
        q2_cumulative = _metrics["cumulative"]
        q2_gap = _metrics["gap_pct"]
        print(f"Q2 metrics sourced from Databricks: OKR={q2_okr:,} Outlook={q2_outlook:,}")
    except Exception as e:
        print(f"Databricks unavailable for Q2 metrics ({e}); using CSV values.")
        q2_okr = row.q2_okr
        q2_outlook = row.q2_outlook
        q2_cumulative = row.cumulative_enrollments
        q2_gap = calculate_q2_gap(row)

    # Pull live touchpoints from Databricks; fall back to CSV row values
    try:
        _touchpoints = fetch_touchpoints()
        print("Touchpoints sourced from Databricks.")
    except Exception as e:
        print(f"Databricks unavailable ({e}); using CSV touchpoint values.")
        _touchpoints = {
            "email": {
                "touchpoints": row.email_touchpoints,
                "shifted": int(row.email_touchpoints * 0.70),
            },
            "dm": {
                "touchpoints": row.direct_mail_touchpoints,
                "shifted": int(row.direct_mail_touchpoints * 0.60),
            },
        }

    # Pull live Y2 renewals from Databricks; fall back to parsed handoff text
    try:
        _y2_deferred = fetch_y2_renewals()
        print("Y2 renewals sourced from Databricks.")
    except Exception as e:
        print(f"Databricks unavailable for Y2 renewals ({e}); using handoff text.")
        _y2_deferred = _parse_y2_deferred(y2_table_text)

    email_defer = int(_touchpoints["email"]["touchpoints"] * 0.70)

    levers = [
        {
            "id": "no_action",
            "name": "No Action Required",
            "subtitle": "DM Pipeline Supporting Q2",
            "workback_days": 0,
            "urgency_label": "No action needed",
            "rationale": (
                f"Q2 outlook ({q2_outlook:,}) is {q2_gap:+.1f}% vs the Q2 OKR ({q2_okr:,}). "
                "Gap is within the 2% action threshold — no lever needed. "
                "Prior direct mail campaigns are driving enrollment pipeline into Q2."
            ),
            "estimated_impact": (
                f"Q2 outlook is tracking {q2_outlook - q2_okr:+,} vs OKR. "
                "No lever action needed; monitor quarterly pacing."
            ),
            "action": "Hold all channel volumes as planned. Reassess if Q2 outlook drops more than 2% below OKR.",
            "execution_steps": [
                "No lever action required this week.",
                "Continue monitoring quarterly pacing against OKR.",
                "Reassess if Q2 outlook drops more than 2% below OKR.",
                "Publish updated playbook to the CS/EM channel.",
            ],
        },
        {
            "id": "email_reactivation",
            "name": "Email Reactivation",
            "subtitle": f"Defer 70% of email volume ({email_defer:,}) to Q3",
            "workback_days": 7,
            "urgency_label": "Decision needed within 7 days",
            "rationale": (
                f"Q2 outlook ({q2_outlook:,}) is below OKR ({q2_okr:,}) "
                f"with {row.remaining_weeks} weeks remaining. "
                "Email reactivation is eligible under marketing/reactivation criteria."
            ),
            "estimated_impact": (
                f"Defer 70% of email volume from the next eligible window. "
                f"Estimated movement: -{email_defer:,} Q2 enrollments to Q3."
            ),
            "action": "Defer reactivation volume and update campaign calendar.",
            "execution_steps": [
                "Confirm decision by the next working day.",
                "Coordinate with GROMO, CS, and EM to update campaign schedules.",
                "Apply eligibility filters and freeze affected audience segments.",
                "Publish updated playbook to the CS/EM channel.",
            ],
        },
        {
            "id": "mailer_deferment",
            "name": "Mailer Flat Deferment",
            "subtitle": f"Defer 100% of DM volume ({row.direct_mail_touchpoints:,}) to Q3",
            "workback_days": 30,
            "urgency_label": "Decision needed within 30 days",
            "rationale": (
                f"Q2 outlook is above OKR with {row.remaining_weeks} weeks remaining. "
                "Use standard mail deferment to shift volume into next quarter."
            ),
            "estimated_impact": "Defer 100% of qualifying mail volume into next quarter.",
            "action": "Pause outbound mailer shipments and update the mail calendar.",
            "execution_steps": [
                "Confirm decision by the next working day.",
                "Coordinate with GROMO, CS, and EM to update campaign schedules.",
                "Apply eligibility filters and freeze affected audience segments.",
                "Publish updated playbook to the CS/EM channel.",
            ],
        },
        {
            "id": "throttle_pause",
            "name": "Throttle Pause",
            "subtitle": "Unthrottle next available send window",
            "workback_days": 6,
            "urgency_label": "Decision needed within 6 days",
            "rationale": (
                f"Q2 outlook is below OKR with only {row.remaining_weeks} weeks remaining. "
                "Throttle pause can free up volume with a short workback."
            ),
            "estimated_impact": "Unthrottle planned communications for the next available window.",
            "action": "Pause planned sends and align with media calendar.",
            "execution_steps": [
                "Confirm decision by the next working day.",
                "Coordinate with GROMO, CS, and EM to update campaign schedules.",
                "Apply eligibility filters and freeze affected audience segments.",
                "Publish updated playbook to the CS/EM channel.",
            ],
        },
    ]

    data = {
        "generated_at": date.today().strftime("%b %-d, %Y"),
        "week": row.week,
        "quarter": row.quarter,
        "metrics": {
            "q2_okr": q2_okr,
            "q2_outlook": q2_outlook,
            "gap": q2_outlook - q2_okr,
            "gap_pct": round(q2_gap, 1),
            "cumulative": q2_cumulative,
            "remaining_weeks": row.remaining_weeks,
            "forecast_rate": row.forecast_enrollments,
        },
        "touchpoints": _touchpoints,
        "y2_deferred": _y2_deferred,
        "recommended_lever_id": _lever_id(recommendation),
        "levers": levers,
    }

    docs_dir.mkdir(exist_ok=True)
    out = docs_dir / "data.json"
    out.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return out


def build_replacements(prev_values: dict, new_values: dict) -> list:
    """
    Generate find/replace pairs from prev → new doc_values.
    Order matters: replace longer/more-specific strings first.
    """
    keys_in_order = [
        "doc_title", "week_label", "date_label", "quarter",
        "rationale", "estimated_impact", "action", "recommendation_name",
        "q2_outlook", "q2_okr", "gap_to_okr", "q2_gap_pct",
        "cumulative", "forecast_rate", "remaining_weeks",
        "total_touchpoints", "email_touchpoints", "dm_touchpoints",
        "week_num",
    ]
    replacements = []
    for key in keys_in_order:
        old = prev_values.get(key, "")
        new = new_values.get(key, "")
        if old and old != new:
            replacements.append({"find": old, "replace": new})
    return replacements


def main():
    parser = argparse.ArgumentParser(description="Quarterly performance lever automation agent.")
    parser.add_argument("--input", required=True, help="Path to Mode dashboard CSV export.")
    parser.add_argument("--output", default="outputs/weekly_playbook.md", help="Generated playbook path.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    rows = parse_mode_dashboard(input_path)
    if not rows:
        raise SystemExit("No rows found in Mode dashboard export.")

    row = rows[0]
    recommendation = recommend_lever(row)

    if not recommendation:
        print("No intervention needed this week. Enrollment pacing is within tolerance.")
        return

    playbook_path = render_playbook(row, recommendation, output_path)
    summary = build_summary(row, recommendation)

    print(summary)
    print(f"Generated playbook: {playbook_path}")

    today = date.today()
    doc_id = os.environ.get("GOOGLE_DOC_ID", "")
    new_doc_values = build_doc_values(row, recommendation, today)

    # Read previous handoff to compute find/replace pairs for the persistent doc
    handoff_path = output_path.parent / "mcp_handoff.json"
    prev_doc_values = {}
    Y2_PLACEHOLDER = "| — | — |"
    prev_y2_table_text = Y2_PLACEHOLDER
    if doc_id and handoff_path.exists():
        try:
            prev = json.loads(handoff_path.read_text(encoding="utf-8"))
            prev_doc_values = prev.get("doc_values", {})
            prev_y2_table_text = prev.get("y2_table_text", Y2_PLACEHOLDER)
        except (json.JSONDecodeError, KeyError):
            pass

    replacements = build_replacements(prev_doc_values, new_doc_values) if prev_doc_values else []

    handoff = {
        "doc_title": new_doc_values["doc_title"],
        "summary": summary,
        "playbook_content": playbook_path.read_text(encoding="utf-8"),
        "doc_id": doc_id,
        "doc_values": new_doc_values,
        "replacements": replacements,
        "y2_table_text": prev_y2_table_text,
        "slack_channel": "#codis-claude-test",
    }
    handoff_path.write_text(json.dumps(handoff, indent=2), encoding="utf-8")
    print(f"\nHandoff file written: {handoff_path}")

    docs_dir = Path(__file__).resolve().parent.parent / "docs"
    web_data_path = generate_web_data(row, recommendation, prev_y2_table_text, docs_dir)
    print(f"Web data written:     {web_data_path}")
    if doc_id and replacements:
        print(f"Update mode: {len(replacements)} replacements queued for doc {doc_id}")
        print("Claude will apply replacements to the persistent doc and post to Slack.")
    elif doc_id:
        print("No changes detected vs last run — doc is already up to date.")
    else:
        print("First-run mode: Claude will create a new Google Doc and post to Slack.")


if __name__ == "__main__":
    main()
