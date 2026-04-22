from dataclasses import asdict
from pathlib import Path
from typing import Dict

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .decision import LeverRecommendation
from .data import ModeRow


def render_playbook(row: ModeRow, recommendation: LeverRecommendation, output_path: Path) -> Path:
    env = Environment(
        loader=FileSystemLoader(searchpath=Path(__file__).resolve().parent.parent / "templates"),
        autoescape=select_autoescape([]),
    )
    template = env.get_template("playbook.md.j2")
    content = template.render(
        quarter=row.quarter,
        week=row.week,
        q2_okr=row.q2_okr,
        q2_outlook=row.q2_outlook,
        cumulative_enrollments=row.cumulative_enrollments,
        okrs_enrollments=row.okrs_enrollments,
        actual_enrollments=row.actual_enrollments,
        forecast_enrollments=row.forecast_enrollments,
        remaining_weeks=row.remaining_weeks,
        gap_percent=(row.actual_enrollments - row.okrs_enrollments) / row.okrs_enrollments * 100,
        q2_gap_percent=(row.q2_outlook - row.q2_okr) / row.q2_okr * 100 if row.q2_okr else 0,
        recommendation=asdict(recommendation),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    return output_path
