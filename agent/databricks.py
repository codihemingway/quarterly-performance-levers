"""
Databricks SQL API client for fetching channel touchpoints.

Required env vars:
  DATABRICKS_HOST        e.g. https://dbc-abc123.cloud.databricks.com
  DATABRICKS_TOKEN       personal access token
  DATABRICKS_WAREHOUSE_ID  SQL warehouse ID (HTTP path segment)
"""

import os
import time
from pathlib import Path

import requests


_SQL_PATH = Path(__file__).resolve().parent.parent / "queries" / "channel_touchpoints.sql"

# Columns from the query used to populate data.json touchpoints
_EMAIL_TOUCHPOINTS_COL = "email_qualified_volume"
_EMAIL_ENROLLMENTS_COL = "email_enrollments_deferred"
_DM_TOUCHPOINTS_COL = "dm_qualified_volume"
_DM_ENROLLMENTS_COL = "dm_enrollments_deferred"


def _get_current_week_row(rows: list[dict]) -> dict | None:
    """Return the row whose decision_date is closest to today (most recent past Monday)."""
    from datetime import date, timedelta
    today = date.today()
    # Find the most recent Monday on or before today
    monday = today - timedelta(days=today.weekday())
    monday_str = monday.isoformat()

    # Try exact match first, then fall back to the latest row
    for row in rows:
        if str(row.get("decision_date", "")).startswith(monday_str):
            return row
    # Fall back to last row
    return rows[-1] if rows else None


def fetch_touchpoints(host: str = None, token: str = None, warehouse_id: str = None) -> dict:
    """
    Run channel_touchpoints.sql against Databricks and return:
      {
        "email": {"touchpoints": int, "shifted": int},
        "dm":    {"touchpoints": int, "shifted": int},
      }
    Raises RuntimeError if credentials are missing or the query fails.
    """
    host = host or os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = token or os.environ.get("DATABRICKS_TOKEN", "")
    warehouse_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

    if not all([host, token, warehouse_id]):
        raise RuntimeError(
            "Missing Databricks credentials. Set DATABRICKS_HOST, DATABRICKS_TOKEN, "
            "and DATABRICKS_WAREHOUSE_ID in your .env file."
        )

    sql = _SQL_PATH.read_text(encoding="utf-8")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    base = f"{host}/api/2.0/sql/statements"

    # Submit statement
    resp = requests.post(
        base,
        headers=headers,
        json={
            "warehouse_id": warehouse_id,
            "statement": sql,
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
        },
        timeout=60,
    )
    resp.raise_for_status()
    result = resp.json()
    statement_id = result["statement_id"]

    # Poll until done
    for _ in range(30):
        state = result.get("status", {}).get("state", "")
        if state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            break
        time.sleep(3)
        poll = requests.get(f"{base}/{statement_id}", headers=headers, timeout=30)
        poll.raise_for_status()
        result = poll.json()

    state = result.get("status", {}).get("state", "")
    if state != "SUCCEEDED":
        err = result.get("status", {}).get("error", {}).get("message", state)
        raise RuntimeError(f"Databricks query failed: {err}")

    # Parse result into list of dicts
    manifest = result.get("manifest", {})
    columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
    data_array = result.get("result", {}).get("data_array", [])
    rows = [dict(zip(columns, row)) for row in data_array]

    if not rows:
        raise RuntimeError("Databricks query returned no rows.")

    row = _get_current_week_row(rows)

    def _int(val):
        try:
            return int(float(val or 0))
        except (TypeError, ValueError):
            return 0

    return {
        "email": {
            "touchpoints": _int(row.get(_EMAIL_TOUCHPOINTS_COL)),
            "shifted": _int(row.get(_EMAIL_ENROLLMENTS_COL)),
        },
        "dm": {
            "touchpoints": _int(row.get(_DM_TOUCHPOINTS_COL)),
            "shifted": _int(row.get(_DM_ENROLLMENTS_COL)),
        },
    }
