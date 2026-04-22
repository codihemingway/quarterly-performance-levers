import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List

@dataclass
class ModeRow:
    quarter: str
    week: int
    q2_okr: int
    q2_outlook: int
    cumulative_enrollments: int
    okrs_enrollments: int
    target_enrollments: int
    actual_enrollments: int
    forecast_enrollments: int
    remaining_weeks: int
    marketing_reactivation_allowed: str
    marketing_status: str
    client_tier: int
    sender: str
    email_touchpoints: int
    direct_mail_touchpoints: int


def parse_mode_dashboard(path: Path) -> List[ModeRow]:
    rows: List[ModeRow] = []
    with path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for record in reader:
            rows.append(
                ModeRow(
                    quarter=record["quarter"].strip(),
                    week=int(record["week"]),
                    q2_okr=int(record["q2_okr"]),
                    q2_outlook=int(record["q2_outlook"]),
                    cumulative_enrollments=int(record["cumulative_enrollments"]),
                    okrs_enrollments=int(record["okrs_enrollments"]),
                    target_enrollments=int(record["target_enrollments"]),
                    actual_enrollments=int(record["actual_enrollments"]),
                    forecast_enrollments=int(record["forecast_enrollments"]),
                    remaining_weeks=int(record["remaining_weeks"]),
                    marketing_reactivation_allowed=record["marketing_reactivation_allowed"].strip().lower(),
                    marketing_status=record["marketing_status"].strip().lower(),
                    client_tier=int(record["client_tier"]),
                    sender=record["sender"].strip(),
                    email_touchpoints=int(record["email_touchpoints"]),
                    direct_mail_touchpoints=int(record["direct_mail_touchpoints"]),
                )
            )
    return rows
