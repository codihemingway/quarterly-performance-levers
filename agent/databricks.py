"""
Databricks SQL API client for fetching channel touchpoints and Y2 renewals.

Required env vars:
  DATABRICKS_HOST        e.g. https://dbc-abc123.cloud.databricks.com
  DATABRICKS_TOKEN       personal access token
  DATABRICKS_WAREHOUSE_ID  SQL warehouse ID (HTTP path segment)
"""

import json
import os
import time
from pathlib import Path

import requests


_SQL_PATH = Path(__file__).resolve().parent.parent / "queries" / "channel_touchpoints.sql"
_Y2_SQL_PATH = Path(__file__).resolve().parent.parent / "queries" / "y2_renewals.sql"
_FORECAST_SQL_PATH = Path(__file__).resolve().parent.parent / "queries" / "working_forecast.sql"

# Columns from the query used to populate data.json touchpoints
_EMAIL_TOUCHPOINTS_COL = "email_qualified_volume"
_EMAIL_ENROLLMENTS_COL = "email_enrollments_deferred"
_DM_TOUCHPOINTS_COL = "dm_qualified_volume"
_DM_ENROLLMENTS_COL = "dm_enrollments_deferred"


def _get_current_week_row(rows: list[dict]) -> dict | None:
    """Return the row for the current or next upcoming decision date."""
    from datetime import date
    today = date.today().isoformat()
    for row in rows:
        dd = str(row.get("decision_date", ""))[:10]
        if dd >= today:
            return row
    return rows[-1] if rows else None


def _fmt_date(val) -> str | None:
    """Format a date value as 'Apr 6, 2026'."""
    if not val:
        return None
    s = str(val)[:10]
    try:
        from datetime import date as _date
        d = _date.fromisoformat(s)
        return d.strftime("%b %-d, %Y")
    except ValueError:
        return s


def fetch_touchpoints(host: str = None, token: str = None, warehouse_id: str = None) -> dict:
    """
    Run channel_touchpoints.sql against Databricks and return:
      {
        "email": {"touchpoints": int, "shifted": int},  # current decision week summary
        "dm":    {"touchpoints": int, "shifted": int},
        "rows":  [                                       # full weekly schedule
          {
            "decision_date":       "Apr 14, 2026",
            "email_inhome_week":   "Apr 20, 2026",
            "email_reactivation":  45.3,
            "email_flat_deferment": None,
            "dm_inhome_week":      "May 11, 2026",
            "dm_reactivation":     32.1,
            "dm_flat_deferment":   None,
            "dm_note":             None,
            "email_total_sends":   180000,
            "email_qualified_volume": 120000,
            "email_pct_deferrable":   0.667,
            "email_qualified_am":     384.0,
            "email_days_to_qtr_end":  71,
            "email_decay_pct":        0.0,
            "dm_total_sends":         90000,
            "dm_qualified_volume":    60000,
            "dm_pct_deferrable":      0.5,
            "dm_qualified_am":        300.0,
            "dm_days_to_qtr_end":     50,
            "dm_decay_pct":           0.11,
          }, ...
        ]
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
    rows = _run_sql(sql, host, token, warehouse_id)

    if not rows:
        raise RuntimeError("Databricks query returned no rows.")

    def _int(val):
        try:
            return int(float(val or 0))
        except (TypeError, ValueError):
            return 0

    def _num(val):
        if val is None:
            return None
        try:
            return round(float(val), 2)
        except (TypeError, ValueError):
            return None

    schedule_rows = []
    for r in rows:
        schedule_rows.append({
            "decision_date":          _fmt_date(r.get("decision_date")),
            "email_inhome_week":      _fmt_date(r.get("email_inhome_week")),
            "email_reactivation":     _num(r.get("email_enrollments_deferred")),
            "email_flat_deferment":   _num(r.get("email_flat_deferment")),
            "dm_inhome_week":         _fmt_date(r.get("dm_inhome_week")),
            "dm_reactivation":        _num(r.get("dm_enrollments_deferred")),
            "dm_flat_deferment":      _num(r.get("dm_flat_deferment")),
            "dm_note":                r.get("dm_note"),
            "email_total_sends":      _int(r.get("email_total_sends")),
            "email_qualified_volume": _int(r.get("email_qualified_volume")),
            "email_pct_deferrable":   _num(r.get("email_pct_deferrable")),
            "email_qualified_am":     _num(r.get("email_qualified_am")),
            "email_days_to_qtr_end":  _int(r.get("email_days_to_qtr_end")),
            "email_decay_pct":        _num(r.get("email_decay_pct")),
            "dm_total_sends":         _int(r.get("dm_total_sends")),
            "dm_qualified_volume":    _int(r.get("dm_qualified_volume")),
            "dm_pct_deferrable":      _num(r.get("dm_pct_deferrable")),
            "dm_qualified_am":        _num(r.get("dm_qualified_am")),
            "dm_days_to_qtr_end":     _int(r.get("dm_days_to_qtr_end")),
            "dm_decay_pct":           _num(r.get("dm_decay_pct")),
        })

    # Summary row for lever subtitle computation (current/next decision week)
    current_row = _get_current_week_row(rows)

    return {
        "email": {
            "touchpoints": _int(current_row.get(_EMAIL_TOUCHPOINTS_COL)) if current_row else 0,
            "shifted":     _int(current_row.get(_EMAIL_ENROLLMENTS_COL)) if current_row else 0,
        },
        "dm": {
            "touchpoints": _int(current_row.get(_DM_TOUCHPOINTS_COL)) if current_row else 0,
            "shifted":     _int(current_row.get(_DM_ENROLLMENTS_COL)) if current_row else 0,
        },
        "rows": schedule_rows,
    }


def _extract_token(token: str) -> str:
    """Handle both raw tokens and JSON output from `databricks auth token`."""
    token = token.strip()
    if token.startswith("{"):
        try:
            return json.loads(token)["access_token"]
        except (json.JSONDecodeError, KeyError):
            pass
    return token


def _run_sql(sql: str, host: str, token: str, warehouse_id: str) -> list[dict]:
    """Execute a SQL statement on Databricks and return rows as list of dicts."""
    headers = {"Authorization": f"Bearer {_extract_token(token)}", "Content-Type": "application/json"}
    base = f"{host}/api/2.0/sql/statements"

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

    manifest = result.get("manifest", {})
    columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
    data_array = result.get("result", {}).get("data_array", [])
    return [dict(zip(columns, row)) for row in data_array]


def fetch_q2_metrics(host: str = None, token: str = None, warehouse_id: str = None) -> dict:
    """
    Run working_forecast.sql against Databricks and return aggregated Q2 metrics:
      {
        "q2_okr":      int,   # sum of okr_goal for current quarter
        "q2_outlook":  int,   # sum of outlook for current quarter
        "cumulative":  int,   # sum of actuals_till_date for current quarter
        "gap":         int,   # q2_outlook - q2_okr
        "gap_pct":     float, # gap as % of q2_okr
      }
    Raises RuntimeError if credentials are missing or the query fails.
    """
    from datetime import date

    host = host or os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = token or os.environ.get("DATABRICKS_TOKEN", "")
    warehouse_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

    if not all([host, token, warehouse_id]):
        raise RuntimeError(
            "Missing Databricks credentials. Set DATABRICKS_HOST, DATABRICKS_TOKEN, "
            "and DATABRICKS_WAREHOUSE_ID in your .env file."
        )

    sql = _FORECAST_SQL_PATH.read_text(encoding="utf-8")
    rows = _run_sql(sql, host, token, warehouse_id)

    if not rows:
        raise RuntimeError("Working forecast query returned no rows.")

    # Current quarter start (first day of current quarter)
    today = date.today()
    q_month = ((today.month - 1) // 3) * 3 + 1
    q_start = date(today.year, q_month, 1).isoformat()

    def _float(val):
        try:
            return float(val or 0)
        except (TypeError, ValueError):
            return 0.0

    q2_okr = 0.0
    q2_outlook = 0.0
    q2_cumulative = 0.0

    for row in rows:
        qs = str(row.get("quarter_start", ""))[:10]
        if qs != q_start:
            continue
        q2_okr += _float(row.get("okr_goal"))
        q2_outlook += _float(row.get("outlook"))
        q2_cumulative += _float(row.get("actuals_till_date"))

    q2_okr_int = round(q2_okr)
    q2_outlook_int = round(q2_outlook)
    gap = q2_outlook_int - q2_okr_int
    gap_pct = round((gap / q2_okr_int * 100), 1) if q2_okr_int else 0.0

    return {
        "q2_okr": q2_okr_int,
        "q2_outlook": q2_outlook_int,
        "cumulative": round(q2_cumulative),
        "gap": gap,
        "gap_pct": gap_pct,
    }


def fetch_y2_renewals(host: str = None, token: str = None, warehouse_id: str = None) -> list[dict]:
    """
    Run y2_renewals.sql against Databricks and return weekly renewal data:
      [
        {"week": "Apr 6, 2026", "volume": 210000, "okr": 215000},
        ...
      ]
    Rows are aggregated by week (Monday), summing enso + wph_or_chronic + unmarketed renewals.
    Raises RuntimeError if credentials are missing or the query fails.
    """
    from datetime import date, timedelta

    host = host or os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = token or os.environ.get("DATABRICKS_TOKEN", "")
    warehouse_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

    if not all([host, token, warehouse_id]):
        raise RuntimeError(
            "Missing Databricks credentials. Set DATABRICKS_HOST, DATABRICKS_TOKEN, "
            "and DATABRICKS_WAREHOUSE_ID in your .env file."
        )

    sql = _Y2_SQL_PATH.read_text(encoding="utf-8")
    rows = _run_sql(sql, host, token, warehouse_id)

    if not rows:
        raise RuntimeError("Y2 renewals query returned no rows.")

    def _int(val):
        try:
            return int(float(val or 0))
        except (TypeError, ValueError):
            return 0

    def _float(val):
        try:
            return float(val or 0)
        except (TypeError, ValueError):
            return 0.0

    def _week_start(date_str: str):
        """Return the Monday of the week containing date_str."""
        try:
            d = date.fromisoformat(str(date_str)[:10])
            return d - timedelta(days=d.weekday())
        except (ValueError, TypeError):
            return None

    # Aggregate daily rows into weekly buckets
    from collections import defaultdict
    weekly: dict = defaultdict(lambda: {"volume": 0, "okr": 0.0})

    for row in rows:
        day_str = row.get("starts_at_max", "")
        monday = _week_start(day_str)
        if monday is None:
            continue
        total_renewals = (
            _int(row.get("enso_renewals"))
            + _int(row.get("wph_or_chronic_renewals"))
            + _int(row.get("unmarketed_renewals"))
        )
        weekly[monday]["volume"] += total_renewals
        weekly[monday]["okr"] += _float(row.get("okr_forecast", 0))

    result = []
    for monday in sorted(weekly.keys()):
        result.append({
            "week": monday.strftime("%b %-d, %Y"),
            "volume": weekly[monday]["volume"],
            "okr": round(weekly[monday]["okr"]),
        })

    return result
