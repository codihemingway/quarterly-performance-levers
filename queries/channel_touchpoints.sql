-- =============================================================================
-- DEFERMENT & REACTIVATION MODELING DASHBOARD
-- =============================================================================
-- Automates the manual "2026 Possible Deferred Enrollments" spreadsheet.
--
-- Output matches the manual format:
--   Decision Date | Email In-Home Week | Email Reactivation | Email Flat Defer
--                 | DM In-Home Week    | DM Reactivation    | DM Flat Defer
--
-- Data Sources:
--   - dbt_prod.rpt_marketing_touchpoints_consolidated (scheduled sends)
--   - rollups.sf_accounts (client details, reactivation eligibility)
--   - rollups.sf_marketing_activities (campaign_template_type)
--
-- For Mode: replace hardcoded values in `params` with
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- CONFIGURABLE PARAMETERS
-- ─────────────────────────────────────────────────────────────────────────────
WITH params AS (
  SELECT
    DATE_TRUNC('quarter', CURRENT_DATE)                        AS quarter_start,
    DATE_SUB(ADD_MONTHS(DATE_TRUNC('quarter', CURRENT_DATE), 3), 1)
                                                              AS quarter_end,
    CASE
      WHEN CAST(REPLACE('60', '%', '') AS DOUBLE) > 1
      THEN CAST(REPLACE('60', '%', '') AS DOUBLE) / 100.0
      ELSE CAST(REPLACE('60', '%', '') AS DOUBLE)
    END AS deferment_pct,
    0.0032             AS email_am_rate,         -- quarterly OKR AM rate for email
    0.0050             AS mailer_am_rate,         -- quarterly OKR AM rate for mailer
    10                 AS email_decision_lead_days,  -- days before in-home to decide
    21                 AS dm_inhome_offset_days       -- DM in-home = email in-home + N days
),

-- ─────────────────────────────────────────────────────────────────────────────
-- DECAY CURVES
-- pct_in_next_quarter = fraction of conversions that would naturally land
-- in the NEXT quarter given N days remaining before quarter end.
-- Source: Email dashboard (Q4 2025) / Mailer time-to-decay view
-- ─────────────────────────────────────────────────────────────────────────────
email_decay AS (
  SELECT * FROM (VALUES
    (0,  2,  0.40),   -- 2-day: 60% converted, 40% bleeds to next Q
    (3,  7,  0.15),   -- 7-day
    (8,  14, 0.09),   -- 14-day
    (15, 21, 0.07),   -- 21-day
    (22, 28, 0.05),   -- 28-day
    (29, 999, 0.00)   -- 29+ days: all conversions land in current Q
  ) AS t(min_days, max_days, pct_in_next_quarter)
),

mailer_decay AS (
  SELECT * FROM (VALUES
    (0,  2,  0.96),   -- 2-day: only 4% converted, 96% bleeds
    (3,  7,  0.64),   -- 7-day
    (8,  14, 0.38),   -- 14-day
    (15, 21, 0.27),   -- 21-day
    (22, 28, 0.21),   -- 28-day
    (29, 60, 0.11),   -- 60-day
    (61, 999, 0.00)   -- 61+ days
  ) AS t(min_days, max_days, pct_in_next_quarter)
),

-- ─────────────────────────────────────────────────────────────────────────────
-- REFERENCE LISTS
-- ─────────────────────────────────────────────────────────────────────────────
excluded_partners AS (
  SELECT * FROM (VALUES
    ('BlueCross BlueShield of Tennessee'),
    ('Express Scripts (Partner)'),
    ('UnitedHealth Group')
  ) AS t(partner_name)
),

excluded_templates_email AS (
  SELECT * FROM (VALUES
    ('Highlights: EM1 HingeSelect Baseline'),
    ('Highlights: EM2 HingeSelect Baseline')
  ) AS t(template_type)
),

excluded_templates_mailer AS (
  SELECT * FROM (VALUES
    ('Highlights: DM HingeSelect Baseline')
  ) AS t(template_type)
),

-- ─────────────────────────────────────────────────────────────────────────────
-- BASE TOUCHPOINTS: join all three data sources, apply AM rate
-- ─────────────────────────────────────────────────────────────────────────────
base_touchpoints AS (
  SELECT
    t.touchpoint_id,
    t.account_id,
    t.customer_id,
    t.deployment_id,
    t.touchpoint_name,
    t.touchpoint_number,
    t.touchpoint_medium,
    t.touchpoint_marketing_activity_status,
    t.touchpoint_sender,
    t.content_type,
    t.customer_name,
    t.customer_tier,
    t.population_type,
    t.touchpoint_sent_at,
    COALESCE(t.scheduled_sends, 0)                                  AS scheduled_sends,
    COALESCE(t.forecasted_conversions, 0)                           AS forecasted_conversions,

    -- Estimated AM using quarterly OKR rate (matches manual methodology)
    COALESCE(t.scheduled_sends, 0) * CASE t.touchpoint_medium
      WHEN 'Email'  THEN p.email_am_rate
      WHEN 'Mailer' THEN p.mailer_am_rate
      ELSE 0
    END                                                             AS estimated_am,

    a.allows_marketing_reactivation,
    a.contracting_partner,
    a.experimentation_participation,
    a.cs_lead_division,
    a.name                                                          AS sf_account_name,
    CAST(a.sf_client_id AS INT)                                     AS client_id,

    ma.campaign_template_type,
    ma.automation_approved,

    DATE_TRUNC('WEEK', t.touchpoint_sent_at)                        AS week_of,
    DATEDIFF(p.quarter_end, t.touchpoint_sent_at)                   AS days_until_quarter_end

  FROM dbt_prod.rpt_marketing_touchpoints_consolidated t
  INNER JOIN rollups.sf_accounts a
    ON t.account_id = a.account_id
  LEFT JOIN rollups.sf_marketing_activities ma
    ON t.touchpoint_id = ma.marketing_activity_id
  CROSS JOIN params p
  WHERE t.touchpoint_sent_at BETWEEN p.quarter_start AND p.quarter_end
    AND t.is_throttled = false
    AND t.touchpoint_medium IN ('Email', 'Mailer')
),

-- ─────────────────────────────────────────────────────────────────────────────
-- QUALIFICATION FLAGS
-- ─────────────────────────────────────────────────────────────────────────────
qualified_touchpoints AS (
  SELECT
    bt.*,

    -- EMAIL REACTIVATION
    CASE
      WHEN bt.touchpoint_medium = 'Email'
        AND bt.allows_marketing_reactivation IN ('Yes - all channels', 'Yes - email only')
        AND bt.touchpoint_marketing_activity_status IN ('Approved', 'Awaiting Approval-High Likelihood')
        AND bt.customer_tier IN ('Tier 1', 'Tier 2', 'Tier 3')
        AND bt.touchpoint_sender = 'Hinge Health'
        AND COALESCE(bt.contracting_partner, '') NOT IN (SELECT partner_name FROM excluded_partners)
        AND (
              bt.campaign_template_type IS NULL
              OR (
                bt.campaign_template_type NOT LIKE 'Non-Standard%'
                AND bt.campaign_template_type NOT IN (SELECT template_type FROM excluded_templates_email)
              )
            )
      THEN TRUE ELSE FALSE
    END AS is_email_reactivation_qualified,

    -- MAILER REACTIVATION
    CASE
      WHEN bt.touchpoint_medium = 'Mailer'
        AND bt.allows_marketing_reactivation IN ('Yes - all channels', 'Yes - mailer only')
        AND bt.touchpoint_marketing_activity_status IN ('Approved', 'Awaiting Approval-High Likelihood')
        AND bt.customer_tier IN ('Tier 1', 'Tier 2', 'Tier 3')
        AND bt.touchpoint_sender = 'Hinge Health'
        AND COALESCE(bt.contracting_partner, '') NOT IN (SELECT partner_name FROM excluded_partners)
        AND (
              bt.campaign_template_type IS NULL
              OR (
                bt.campaign_template_type NOT LIKE 'Non-Standard%'
                AND bt.campaign_template_type NOT IN (SELECT template_type FROM excluded_templates_mailer)
              )
            )
      THEN TRUE ELSE FALSE
    END AS is_mailer_reactivation_qualified,

    -- EMAIL FLAT DEFERMENT: last 7 days of quarter, sender = HH, non-custom
    CASE
      WHEN bt.touchpoint_medium = 'Email'
        AND bt.touchpoint_sender = 'Hinge Health'
        AND bt.touchpoint_marketing_activity_status IN ('Approved', 'Awaiting Approval-High Likelihood')
        AND bt.days_until_quarter_end BETWEEN 0 AND 6
      THEN TRUE ELSE FALSE
    END AS is_email_flat_defer_qualified,

    -- MAILER FLAT DEFERMENT: last ~3 weeks of quarter (21 days), sender = HH
    CASE
      WHEN bt.touchpoint_medium = 'Mailer'
        AND bt.touchpoint_sender = 'Hinge Health'
        AND bt.touchpoint_marketing_activity_status IN ('Approved', 'Awaiting Approval-High Likelihood')
        AND bt.days_until_quarter_end BETWEEN 0 AND 21
      THEN TRUE ELSE FALSE
    END AS is_mailer_flat_defer_qualified

  FROM base_touchpoints bt
),

-- ─────────────────────────────────────────────────────────────────────────────
-- PER-TOUCHPOINT DECAY: join decay curves using each touchpoint's actual
-- days_until_quarter_end so aggregates match touchpoint-level detail query
-- ─────────────────────────────────────────────────────────────────────────────
touchpoint_with_decay AS (
  SELECT
    qt.*,

    COALESCE(
      CASE qt.touchpoint_medium
        WHEN 'Email'  THEN ed.pct_in_next_quarter
        WHEN 'Mailer' THEN md.pct_in_next_quarter
      END, 0)                                                              AS pct_naturally_in_next_quarter,

    CASE WHEN qt.is_email_reactivation_qualified OR qt.is_mailer_reactivation_qualified
      THEN qt.estimated_am * (SELECT deferment_pct FROM params)
           * (1 - COALESCE(
                CASE qt.touchpoint_medium
                  WHEN 'Email'  THEN ed.pct_in_next_quarter
                  WHEN 'Mailer' THEN md.pct_in_next_quarter
                END, 0))
      ELSE 0
    END                                                                    AS tp_reactivation_deferred_am,

    CASE WHEN qt.is_email_flat_defer_qualified OR qt.is_mailer_flat_defer_qualified
      THEN qt.estimated_am
           * (1 - COALESCE(
                CASE qt.touchpoint_medium
                  WHEN 'Email'  THEN ed.pct_in_next_quarter
                  WHEN 'Mailer' THEN md.pct_in_next_quarter
                END, 0))
      ELSE 0
    END                                                                    AS tp_flat_deferred_am

  FROM qualified_touchpoints qt
  LEFT JOIN email_decay ed
    ON qt.touchpoint_medium = 'Email'
    AND qt.days_until_quarter_end BETWEEN ed.min_days AND ed.max_days
  LEFT JOIN mailer_decay md
    ON qt.touchpoint_medium = 'Mailer'
    AND qt.days_until_quarter_end BETWEEN md.min_days AND md.max_days
),

-- ─────────────────────────────────────────────────────────────────────────────
-- WEEKLY AGGREGATION BY CHANNEL (with pre-computed decay-adjusted deferments)
-- ─────────────────────────────────────────────────────────────────────────────
weekly_agg AS (
  SELECT
    week_of,
    touchpoint_medium                                                       AS channel,

    COUNT(*)                                                                AS touchpoint_count,

    SUM(scheduled_sends)                                                    AS total_scheduled_sends,

    SUM(CASE WHEN touchpoint_sender IN ('Hinge Health', 'Custom Hinge Health')
             THEN scheduled_sends ELSE 0 END)                               AS hh_sent_volume,

    SUM(CASE WHEN is_email_reactivation_qualified OR is_mailer_reactivation_qualified
             THEN scheduled_sends ELSE 0 END)                               AS reactivation_qualified_volume,

    SUM(CASE WHEN is_email_reactivation_qualified OR is_mailer_reactivation_qualified
             THEN estimated_am ELSE 0 END)                                  AS reactivation_qualified_am,

    SUM(CASE WHEN touchpoint_marketing_activity_status IN ('Approved', 'Awaiting Approval-High Likelihood')
             THEN estimated_am ELSE 0 END)                                  AS total_approved_am,

    SUM(CASE WHEN is_email_flat_defer_qualified OR is_mailer_flat_defer_qualified
             THEN scheduled_sends ELSE 0 END)                               AS flat_defer_volume,

    DATEDIFF((SELECT quarter_end FROM params), week_of)                     AS days_until_quarter_end,

    ROUND(SUM(CASE WHEN is_email_reactivation_qualified OR is_mailer_reactivation_qualified
                   THEN estimated_am * pct_naturally_in_next_quarter ELSE 0 END)
          / NULLIF(SUM(CASE WHEN is_email_reactivation_qualified OR is_mailer_reactivation_qualified
                            THEN estimated_am ELSE 0 END), 0),
          4)                                                                AS avg_decay_pct,

    ROUND(SUM(tp_reactivation_deferred_am), 2)                             AS reactivation_deferred_am_final,
    ROUND(SUM(tp_flat_deferred_am), 2)                                     AS flat_deferred_am

  FROM touchpoint_with_decay
  GROUP BY week_of, touchpoint_medium
),

-- ─────────────────────────────────────────────────────────────────────────────
-- DEFERMENT CALCULATIONS (ratios and reference metrics)
-- ─────────────────────────────────────────────────────────────────────────────
deferment_calc AS (
  SELECT
    w.week_of,
    w.channel,
    w.touchpoint_count,
    w.total_scheduled_sends,
    w.hh_sent_volume,
    w.reactivation_qualified_volume,
    w.reactivation_qualified_am,
    w.total_approved_am,
    w.days_until_quarter_end,

    ROUND(w.reactivation_qualified_volume
          / NULLIF(w.hh_sent_volume, 0), 4)                                AS pct_deferrable_of_hh,
    ROUND(w.reactivation_qualified_volume
          / NULLIF(w.total_scheduled_sends, 0), 4)                         AS pct_deferrable_of_total,

    ROUND(w.reactivation_qualified_volume
          * (SELECT deferment_pct FROM params), 0)                         AS reactivation_deferred_volume,

    w.avg_decay_pct                                                        AS pct_naturally_in_next_quarter,

    w.reactivation_deferred_am_final,
    w.flat_deferred_am

  FROM weekly_agg w
),

-- ─────────────────────────────────────────────────────────────────────────────
-- SEPARATE EMAIL & MAILER weekly summaries
-- ─────────────────────────────────────────────────────────────────────────────
email_weekly AS (
  SELECT * FROM deferment_calc WHERE channel = 'Email'
),

mailer_weekly AS (
  SELECT * FROM deferment_calc WHERE channel = 'Mailer'
),

-- ─────────────────────────────────────────────────────────────────────────────
-- BUILD DECISION DATE SCHEDULE
-- Decision date = email in-home week - lead days
-- DM in-home week = email in-home week + offset days
-- ─────────────────────────────────────────────────────────────────────────────
decision_schedule AS (
  SELECT
    DATE_SUB(e.week_of, (SELECT email_decision_lead_days FROM params))      AS decision_date,
    e.week_of                                                              AS email_inhome_week,
    DATE_ADD(e.week_of, (SELECT dm_inhome_offset_days FROM params))        AS dm_inhome_week
  FROM email_weekly e
)

-- =============================================================================
-- FINAL OUTPUT: Pivoted by decision date (matches manual spreadsheet format)
-- =============================================================================
SELECT
  ds.decision_date,

  -- EMAIL
  ds.email_inhome_week,
  e.reactivation_deferred_am_final                                         AS email_enrollments_deferred,
  CASE
    WHEN e.flat_deferred_am > 0 THEN e.flat_deferred_am
    ELSE NULL
  END                                                                      AS email_flat_deferment,

  -- DIRECT MAIL
  ds.dm_inhome_week,
  CASE
    WHEN ds.dm_inhome_week > DATE_SUB((SELECT quarter_end FROM params), 7)
      THEN NULL
    ELSE m.reactivation_deferred_am_final
  END                                                                      AS dm_enrollments_deferred,
  CASE
    WHEN ds.dm_inhome_week > DATE_SUB((SELECT quarter_end FROM params), 7)
      THEN NULL
    WHEN m.flat_deferred_am > 0 THEN m.flat_deferred_am
    ELSE NULL
  END                                                                      AS dm_flat_deferment,
  CASE
    WHEN ds.dm_inhome_week > DATE_SUB((SELECT quarter_end FROM params), 7)
      THEN 'Already in Q2'
    ELSE NULL
  END                                                                      AS dm_note,

  -- DETAIL COLUMNS (for Mode charts)
  e.total_scheduled_sends                                                  AS email_total_sends,
  e.reactivation_qualified_volume                                          AS email_qualified_volume,
  e.pct_deferrable_of_hh                                                   AS email_pct_deferrable,
  e.reactivation_qualified_am                                              AS email_qualified_am,
  e.days_until_quarter_end                                                 AS email_days_to_qtr_end,
  e.pct_naturally_in_next_quarter                                          AS email_decay_pct,

  m.total_scheduled_sends                                                  AS dm_total_sends,
  m.reactivation_qualified_volume                                          AS dm_qualified_volume,
  m.pct_deferrable_of_hh                                                   AS dm_pct_deferrable,
  m.reactivation_qualified_am                                              AS dm_qualified_am,
  COALESCE(m.days_until_quarter_end, 0)                                    AS dm_days_to_qtr_end,
  COALESCE(m.pct_naturally_in_next_quarter, 0)                             AS dm_decay_pct

FROM decision_schedule ds
LEFT JOIN email_weekly e
  ON ds.email_inhome_week = e.week_of
LEFT JOIN mailer_weekly m
  ON ds.dm_inhome_week = m.week_of
ORDER BY ds.decision_date
