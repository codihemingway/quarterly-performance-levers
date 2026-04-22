from dataclasses import dataclass
from typing import Optional

from .data import ModeRow

@dataclass
class LeverRecommendation:
    name: str
    rationale: str
    estimated_impact: str
    workback_days: int
    action: str
    email_touchpoints: int
    direct_mail_touchpoints: int
    email_shifted_enrollments: int
    dm_shifted_enrollments: int


def calculate_q2_gap(row: ModeRow) -> float:
    if row.q2_okr == 0:
        return 0.0
    return (row.q2_outlook - row.q2_okr) / row.q2_okr * 100


def calculate_gap(row: ModeRow) -> float:
    """Alias kept for backward compatibility."""
    return calculate_q2_gap(row)


def qualifies_for_email_reactivation(row: ModeRow) -> bool:
    return (
        row.marketing_reactivation_allowed == "yes"
        and row.marketing_status == "approved"
        and row.client_tier in {1, 2, 3}
        and row.sender.lower() == "hinge health"
        and row.email_touchpoints > 0
    )


def recommend_lever(row: ModeRow) -> Optional[LeverRecommendation]:
    q2_gap = calculate_q2_gap(row)
    weeks_left = row.remaining_weeks

    # Q2 on track — no lever needed; DM pipeline is supporting pacing
    if q2_gap >= 0:
        return LeverRecommendation(
            name="No Action Required — DM Pipeline Supporting Q2",
            rationale=(
                f"Q2 outlook ({row.q2_outlook:,}) is {q2_gap:+.1f}% above the Q2 OKR ({row.q2_okr:,}). "
                "Prior direct mail campaigns are driving enrollment pipeline into Q2. "
                "Email reactivation would defer Q2 enrollments to Q3 — the wrong direction."
            ),
            estimated_impact=(
                f"Q2 outlook is tracking +{row.q2_outlook - row.q2_okr:,} vs OKR. "
                "No lever action needed; monitor quarterly pacing."
            ),
            workback_days=0,
            action="Hold all channel volumes as planned. Reassess if Q2 outlook drops below OKR.",
            email_touchpoints=row.email_touchpoints,
            direct_mail_touchpoints=row.direct_mail_touchpoints,
            email_shifted_enrollments=int(row.email_touchpoints * 0.70),
            dm_shifted_enrollments=int(row.direct_mail_touchpoints * 0.60),
        )

    # Q2 under-pacing with time to act — email reactivation
    if q2_gap < 0 and weeks_left >= 4:
        if qualifies_for_email_reactivation(row):
            volume_pct = 70
            impact = int(row.email_touchpoints * 0.7)
            return LeverRecommendation(
                name="Email Reactivation",
                rationale=(
                    f"Q2 outlook ({row.q2_outlook:,}) is {q2_gap:.1f}% below OKR ({row.q2_okr:,}) "
                    f"with {weeks_left} weeks remaining. "
                    "Email reactivation is eligible under marketing/reactivation criteria."
                ),
                estimated_impact=(
                    f"Defer {volume_pct}% of email volume from the next eligible window. "
                    f"Estimated movement: -{impact:,} Q2 enrollments to Q3."
                ),
                workback_days=7,
                action="Defer reactivation volume and update campaign calendar.",
                email_touchpoints=row.email_touchpoints,
                direct_mail_touchpoints=row.direct_mail_touchpoints,
            )

    # Q2 over-pacing near end of quarter — mailer deferment
    if q2_gap >= 2.0 and weeks_left <= 2:
        return LeverRecommendation(
            name="Mailer Flat Deferment",
            rationale=(
                f"Q2 outlook is {q2_gap:.1f}% above OKR with only {weeks_left} weeks remaining. "
                "Use standard mail deferment to shift volume into next quarter."
            ),
            estimated_impact="Defer 100% of qualifying mail volume into next quarter.",
            workback_days=30,
            action="Pause outbound mailer shipments and update the mail calendar.",
            email_touchpoints=row.email_touchpoints,
            direct_mail_touchpoints=row.direct_mail_touchpoints,
            email_shifted_enrollments=int(row.email_touchpoints * 0.70),
            dm_shifted_enrollments=int(row.direct_mail_touchpoints * 0.60),
        )

    # Q2 under-pacing, too late for email reactivation
    if q2_gap < 0 and weeks_left < 4:
        return LeverRecommendation(
            name="Throttle Pause",
            rationale=(
                f"Q2 outlook is {q2_gap:.1f}% below OKR with only {weeks_left} weeks remaining. "
                "Throttle pause can free up volume with a short workback."
            ),
            estimated_impact="Unthrottle planned communications for the next available window.",
            workback_days=6,
            action="Pause planned sends and align with media calendar.",
            email_touchpoints=row.email_touchpoints,
            direct_mail_touchpoints=row.direct_mail_touchpoints,
            email_shifted_enrollments=int(row.email_touchpoints * 0.70),
            dm_shifted_enrollments=int(row.direct_mail_touchpoints * 0.60),
        )

    return None
