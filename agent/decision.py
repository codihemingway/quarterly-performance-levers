from dataclasses import dataclass
from typing import Optional

from .data import ModeRow

GLEAN_LEVERS_DOC = "https://docs.google.com/document/d/1dOhc55Qyxg-14HZvCxzo-FAiws49B91tPABXgtgKtDM"

@dataclass
class LeverRecommendation:
    name: str
    rationale: str
    estimated_impact: str
    workback_days: int
    action: str
    email_touchpoints: int
    direct_mail_touchpoints: int
    email_shifted_enrollments: int = 0
    dm_shifted_enrollments: int = 0


def calculate_q2_gap(row: ModeRow) -> float:
    if row.q2_okr == 0:
        return 0.0
    return (row.q2_outlook - row.q2_okr) / row.q2_okr * 100


def calculate_gap(row: ModeRow) -> float:
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

    # Over-pacing by more than 2% — defer volume to Q3
    if q2_gap > 2.0:
        return LeverRecommendation(
            name="Mailer Flat Deferment",
            rationale=(
                f"Q2 outlook ({row.q2_outlook:,}) is {q2_gap:+.1f}% above OKR ({row.q2_okr:,}) — "
                f"over the 2% action threshold with {weeks_left} weeks remaining. "
                "Defer DM volume to Q3 to avoid over-delivering this quarter. "
                f"Reference: {GLEAN_LEVERS_DOC}"
            ),
            estimated_impact=(
                f"Defer qualifying mail volume into Q3. "
                f"Estimated movement: -{row.direct_mail_touchpoints:,} DM touchpoints shifted."
            ),
            workback_days=30,
            action="Pause outbound mailer shipments and update the mail calendar.",
            email_touchpoints=row.email_touchpoints,
            direct_mail_touchpoints=row.direct_mail_touchpoints,
            email_shifted_enrollments=int(row.email_touchpoints * 0.70),
            dm_shifted_enrollments=int(row.direct_mail_touchpoints * 0.60),
        )

    # Within ±2% — no action needed, reference Glean for context
    if q2_gap >= -2.0:
        return LeverRecommendation(
            name="No Action Required — DM Pipeline Supporting Q2",
            rationale=(
                f"Q2 outlook ({row.q2_outlook:,}) is {q2_gap:+.1f}% vs the Q2 OKR ({row.q2_okr:,}). "
                "Gap is within the ±2% action threshold — no lever needed. "
                "Prior direct mail campaigns are driving enrollment pipeline into Q2. "
                f"Reference: {GLEAN_LEVERS_DOC}"
            ),
            estimated_impact=(
                f"Q2 outlook is tracking {row.q2_outlook - row.q2_okr:+,} vs OKR. "
                "No lever action needed; monitor quarterly pacing."
            ),
            workback_days=0,
            action="Hold all channel volumes as planned. Reassess if gap moves outside ±2%.",
            email_touchpoints=row.email_touchpoints,
            direct_mail_touchpoints=row.direct_mail_touchpoints,
            email_shifted_enrollments=int(row.email_touchpoints * 0.70),
            dm_shifted_enrollments=int(row.direct_mail_touchpoints * 0.60),
        )

    # Under-pacing by more than 2% with time to act — email reactivation
    if q2_gap < -2.0 and weeks_left >= 4:
        if qualifies_for_email_reactivation(row):
            impact = int(row.email_touchpoints * 0.7)
            return LeverRecommendation(
                name="Email Reactivation",
                rationale=(
                    f"Q2 outlook ({row.q2_outlook:,}) is {q2_gap:.1f}% below OKR ({row.q2_okr:,}) — "
                    f"over the 2% action threshold with {weeks_left} weeks remaining. "
                    "Email reactivation is eligible under marketing/reactivation criteria. "
                    f"Reference: {GLEAN_LEVERS_DOC}"
                ),
                estimated_impact=(
                    f"Defer 70% of email volume from the next eligible window. "
                    f"Estimated movement: -{impact:,} Q2 enrollments to Q3."
                ),
                workback_days=7,
                action="Defer reactivation volume and update campaign calendar.",
                email_touchpoints=row.email_touchpoints,
                direct_mail_touchpoints=row.direct_mail_touchpoints,
                email_shifted_enrollments=impact,
                dm_shifted_enrollments=int(row.direct_mail_touchpoints * 0.60),
            )

    # Under-pacing by more than 2%, too late for email reactivation
    if q2_gap < -2.0 and weeks_left < 4:
        return LeverRecommendation(
            name="Throttle Pause",
            rationale=(
                f"Q2 outlook is {q2_gap:.1f}% below OKR with only {weeks_left} weeks remaining — "
                "over the 2% action threshold. Throttle pause can free up volume with a short workback. "
                f"Reference: {GLEAN_LEVERS_DOC}"
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
