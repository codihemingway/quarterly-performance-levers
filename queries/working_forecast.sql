/*
Creating Working Forecast, Baseline, Outlook, Adjusted Outlook for various cohorts across all quarters starting Q3 2025

SETUP INSTRUCTIONS:
1. Update the seed file: seeds/core/ref_okr_multipliers_by_quarter.csv
   - Populate with actual OKR multiplier values from your spreadsheet
   - Ensure quarter_start dates match the quarters CTE below
   - Update okr_segment values: legacy_client, new_client, intra_year, y2, non-aso
   - The non-aso segment applies to medicare, fully_insured, and federal forecast_types

2. Run dbt seed to load the CSV into your database:
   dbt seed --select ref_okr_multipliers_by_quarter

3. Update the quarters CTE to include all quarters you want to analyze (currently includes Q3 2025 through Q4 2026)

4. Update the table reference for okr_multipliers if your seeds are in a different schema than dbt_prod

5. The query will generate results for each quarter, showing baseline forecast, working forecast,
   actuals, outlook, and OKR goals with variance calculations
*/

-- Generate quarters starting Q3 2025
with quarters AS (
  SELECT
    DATE '2025-07-01' as quarter_start,
    DATE '2025-09-30' as quarter_end
  UNION ALL
  SELECT
    DATE '2025-10-01' as quarter_start,
    DATE '2025-12-31' as quarter_end
  UNION ALL
  SELECT
    DATE '2026-01-01' as quarter_start,
    DATE '2026-03-31' as quarter_end
  UNION ALL
  SELECT
    DATE '2026-04-01' as quarter_start,
    DATE '2026-06-30' as quarter_end
  UNION ALL
  SELECT
    DATE '2026-07-01' as quarter_start,
    DATE '2026-09-30' as quarter_end
  UNION ALL
  SELECT
    DATE '2026-10-01' as quarter_start,
    DATE '2026-12-31' as quarter_end
  -- Add more quarters as needed
),

client_attributes AS (
select
  distinct
  customer_id as client_id,
  client_cohort,
  line_of_business,
  is_intra_year
  from dbt_prod.dim_accounts
  where
  customer_id is not null
),
-- Load OKR multipliers from seed
-- Note: Update this reference based on your database/schema where seeds are loaded
-- If using dbt, seeds are typically in the same schema as models
-- If seeds are in a different location, update the table reference accordingly
okr_multipliers AS (
  SELECT
    quarter_start,
    okr_segment,
    okr_acceptances_multiplier as okr_multiplier,
    board_acceptances_multiplier as board_multiplier
  FROM prod.gsheets.okr_acceptances
  -- Alternative if seeds are in a different schema:
  -- FROM your_schema.ref_okr_multipliers_by_quarter
),

-- Map forecast_type to okr_segment
forecast_type_mapping AS (
  SELECT DISTINCT
    q.quarter_start,
    q.quarter_end,
    forecast_type
  FROM quarters q
  CROSS JOIN (
    SELECT 'legacy_client' as forecast_type
    UNION ALL SELECT 'new_client'
    UNION ALL SELECT 'intra_year'
    UNION ALL SELECT 'Y2+'
    UNION ALL SELECT 'medicare'
    UNION ALL SELECT 'fully_insured'
    UNION ALL SELECT 'federal'
  ) forecast_types
),

-- Join multipliers to mapping
okr_multipliers_mapped AS (
  SELECT
    ftm.quarter_start,
    ftm.quarter_end,
    ftm.forecast_type,
    COALESCE(om.okr_multiplier, 1.0) AS okr_multiplier,
    COALESCE(om.board_multiplier, 1.0) AS board_multiplier
  FROM forecast_type_mapping ftm
  LEFT JOIN okr_multipliers om
    ON ftm.quarter_start = om.quarter_start
    AND ftm.forecast_type = om.okr_segment
),

ffc_baseline AS (
  select * FROM (
    select
      q.quarter_start,
      q.quarter_end,
      ffc.customer_id,
      ffc.day_at,
      ffc.client_type,
      ffc.estimate_type,
      ffc.estimate,
      COALESCE(ffc.prop_acute,0) as prop_acute,
      ffct.client_forecast_throttled as is_throttled,
      ROW_NUMBER() OVER (PARTITION BY q.quarter_start, ffc.customer_id, ffc.day_at, ffc.client_type, ffc.estimate_type ORDER BY ffc.forecast_at ASC) as row_num
    FROM quarters q
    CROSS JOIN dbt_prod.fct_forecasts_clients as ffc
    LEFT JOIN dbt_prod.fct_forecasts_clients_throttling as ffct ON
      ffc.customer_id = ffct.customer_id AND
      ffc.date_at = ffct.date_at AND
      ffc.estimate_type = 'attributed'
    WHERE
      ffc.day_at BETWEEN q.quarter_start AND q.quarter_end
      AND ffc.forecast_at >= q.quarter_start - INTERVAL '7 DAYS'
      AND ffc.forecast_at <= q.quarter_start
  )
  where row_num=1 AND not ifnull(is_throttled, false)
),

ffc_program_baseline AS (
  select
    quarter_start,
    quarter_end,
    customer_id,
    day_at,
    client_type,
    estimate_type,
    'chronic' as program,
    estimate*(1-prop_acute) as forecast
  from ffc_baseline
  UNION ALL
  select
    quarter_start,
    quarter_end,
    customer_id,
    day_at,
    client_type,
    estimate_type,
    'acute' as program,
    estimate*prop_acute as forecast
  from ffc_baseline
),

baseline_fct AS (
  SELECT
    fpb.quarter_start,
    fpb.quarter_end,
    CASE
      WHEN fpb.estimate_type IN ('y2', 'y3+') THEN 'Y2+'
      WHEN dc.client_cohort=YEAR(fpb.day_at) AND dc.is_intra_year='true' THEN 'intra_year'
      WHEN lower(dc.line_of_business) IN ('federal') THEN 'federal'
      WHEN lower(dc.line_of_business) IN ('fully insured') THEN 'fully_insured'
      WHEN lower(dc.line_of_business) IN ('medicare', 'medicaid') THEN 'medicare'
      WHEN dc.client_cohort<YEAR(fpb.day_at) THEN 'legacy_client'
      WHEN dc.client_cohort=YEAR(fpb.day_at) THEN 'new_client'
      else null
    END AS forecast_type,
    fpb.program,
    SUM(forecast) as baseline_forecast
  FROM ffc_program_baseline fpb
  INNER JOIN client_attributes as dc ON fpb.customer_id = dc.client_id
  GROUP BY ALL
),

ffc AS (
  select * FROM (
    select
      q.quarter_start,
      q.quarter_end,
      ffc.customer_id,
      ffc.day_at,
      ffc.client_type,
      ffc.estimate_type,
      ffc.estimate,
      COALESCE(ffc.prop_acute,0) as prop_acute,
      ffct.client_forecast_throttled as is_throttled,
      DENSE_RANK() OVER (PARTITION BY q.quarter_start, DATE(ffc.day_at) ORDER BY ffc.forecast_at DESC) as row_num
    FROM quarters q
    CROSS JOIN dbt_prod.fct_forecasts_clients as ffc
    LEFT JOIN dbt_prod.fct_forecasts_clients_throttling as ffct ON
      ffc.customer_id = ffct.customer_id AND
      ffc.date_at = ffct.date_at AND
      ffc.estimate_type = 'attributed'
    WHERE
      ffc.day_at BETWEEN q.quarter_start AND q.quarter_end
      AND ffc.forecast_at >= q.quarter_start - INTERVAL '7 DAYS'
      AND DATE(ffc.forecast_at) <= DATE(ffc.day_at)
  )
  where row_num=1 AND not ifnull(is_throttled, false)
),

ffc_program AS (
  select
    quarter_start,
    quarter_end,
    customer_id,
    day_at,
    client_type,
    estimate_type,
    'chronic' as program,
    estimate*(1-prop_acute) as forecast
  from ffc
  UNION ALL
  select
    quarter_start,
    quarter_end,
    customer_id,
    day_at,
    client_type,
    estimate_type,
    'acute' as program,
    estimate*prop_acute as forecast
  from ffc
),

working_fct AS (
  SELECT
    fp.quarter_start,
    fp.quarter_end,
    CASE
      WHEN fp.estimate_type IN ('y2', 'y3+') THEN 'Y2+'
      WHEN dc.client_cohort=YEAR(fp.day_at) AND dc.is_intra_year='true' THEN 'intra_year'
      WHEN lower(dc.line_of_business) IN ('federal') THEN 'federal'
      WHEN lower(dc.line_of_business) IN ('fully insured') THEN 'fully_insured'
      WHEN lower(dc.line_of_business) IN ('medicare', 'medicaid') THEN 'medicare'
      WHEN dc.client_cohort<YEAR(fp.day_at) THEN 'legacy_client'
      WHEN dc.client_cohort=YEAR(fp.day_at) THEN 'new_client'
      else null
    END AS forecast_type,
    fp.program,
    SUM(forecast) as working_fct
  FROM ffc_program fp
  INNER JOIN client_attributes as dc ON fp.customer_id = dc.client_id
  GROUP BY ALL
),

actuals_till_date AS (
  SELECT
    q.quarter_start,
    q.quarter_end,
    CASE
      WHEN a.enrollment_year_descriptor='Y1' AND c.line_of_business='Federal' THEN 'federal'
      WHEN a.enrollment_year_descriptor='Y1' AND b.forecast_type='new_client' AND c.is_intra_year='true' THEN 'intra_year'
      WHEN a.enrollment_year_descriptor='Y1' THEN b.forecast_type
      ELSE 'Y2+'
    END AS forecast_type,
    a.program,
    count(distinct a.user_id) as actuals_till_date
  FROM quarters q
  CROSS JOIN dbt_prod.fct_users_enrollments AS a
  LEFT JOIN dbt_prod.rpt_campaigns_utms_activities as b
    on a.campaigns_utms_activities_key = b.campaigns_utms_activities_key
  LEFT JOIN client_attributes as c ON a.client_id = c.client_id
  WHERE
    a.enrollment_date >= q.quarter_start
    AND a.enrollment_date <= LEAST(DATE_TRUNC('WEEK', current_date()) - INTERVAL '1 DAYS', q.quarter_end)
  GROUP BY ALL
),

working_fct_future AS (
  SELECT
    fp.quarter_start,
    fp.quarter_end,
    CASE
      WHEN fp.estimate_type IN ('y2', 'y3+') THEN 'Y2+'
      WHEN dc.client_cohort=YEAR(fp.day_at) AND dc.is_intra_year='true' THEN 'intra_year'
      WHEN lower(dc.line_of_business) IN ('federal') THEN 'federal'
      WHEN lower(dc.line_of_business) IN ('fully insured') THEN 'fully_insured'
      WHEN lower(dc.line_of_business) IN ('medicare', 'medicaid') THEN 'medicare'
      WHEN dc.client_cohort<YEAR(fp.day_at) THEN 'legacy_client'
      WHEN dc.client_cohort=YEAR(fp.day_at) THEN 'new_client'
      else null
    END AS forecast_type,
    fp.program,
    SUM(forecast) as working_fct_future
  FROM ffc_program fp
  INNER JOIN client_attributes as dc ON fp.customer_id = dc.client_id
  WHERE
    fp.quarter_start = DATE_TRUNC('QUARTER', current_date()) -- Only current quarter
    AND fp.day_at BETWEEN DATE_TRUNC('WEEK', current_date()) AND fp.quarter_end
  GROUP BY ALL
),

baseline_fct_future AS (
  SELECT
    fpb.quarter_start,
    fpb.quarter_end,
    CASE
      WHEN fpb.estimate_type IN ('y2', 'y3+') THEN 'Y2+'
      WHEN dc.client_cohort=YEAR(fpb.day_at) AND dc.is_intra_year='true' THEN 'intra_year'
      WHEN lower(dc.line_of_business) IN ('federal') THEN 'federal'
      WHEN lower(dc.line_of_business) IN ('fully insured') THEN 'fully_insured'
      WHEN lower(dc.line_of_business) IN ('medicare', 'medicaid') THEN 'medicare'
      WHEN dc.client_cohort<YEAR(fpb.day_at) THEN 'legacy_client'
      WHEN dc.client_cohort=YEAR(fpb.day_at) THEN 'new_client'
      else null
    END AS forecast_type,
    fpb.program,
    SUM(forecast) as baseline_fct_future
  FROM ffc_program_baseline fpb
  INNER JOIN client_attributes as dc ON fpb.customer_id = dc.client_id
  WHERE
    fpb.quarter_start = DATE_TRUNC('QUARTER', current_date()) -- Only current quarter
    AND fpb.day_at BETWEEN DATE_TRUNC('WEEK', current_date()) AND fpb.quarter_end
  GROUP BY ALL
),

outlook AS (
  -- Outlook = actuals_till_date + working_fct_future
  -- For current quarter: shows actuals + remaining forecast
  -- For past quarters: working_fct_future will be NULL, so just shows actuals_till_date (full quarter actuals)
  -- For future quarters: actuals_till_date will be NULL, so shows working_fct_future (which will also be NULL for future quarters)
  --   In this case, we'll use working_fct as fallback in the final SELECT
  select
    COALESCE(a.quarter_start, b.quarter_start) as quarter_start,
    COALESCE(a.quarter_end, b.quarter_end) as quarter_end,
    COALESCE(a.forecast_type, b.forecast_type) as forecast_type,
    COALESCE(a.program, b.program) as program,
    (COALESCE(a.actuals_till_date,0) + COALESCE(b.working_fct_future,0)) as outlook
  from
    actuals_till_date AS a
    FULL OUTER JOIN working_fct_future AS b
      ON a.quarter_start = b.quarter_start
      AND a.forecast_type = b.forecast_type
      AND a.program = b.program
),

working_fct_till_date AS (
  SELECT
    fp.quarter_start,
    fp.quarter_end,
    CASE
      WHEN fp.estimate_type IN ('y2', 'y3+') THEN 'Y2+'
      WHEN dc.client_cohort=YEAR(fp.day_at) AND dc.is_intra_year='true' THEN 'intra_year'
      WHEN lower(dc.line_of_business) IN ('federal') THEN 'federal'
      WHEN lower(dc.line_of_business) IN ('fully insured') THEN 'fully_insured'
      WHEN lower(dc.line_of_business) IN ('medicare', 'medicaid') THEN 'medicare'
      WHEN dc.client_cohort<YEAR(fp.day_at) THEN 'legacy_client'
      WHEN dc.client_cohort=YEAR(fp.day_at) THEN 'new_client'
      else null
    END AS forecast_type,
    fp.program,
    SUM(forecast) as working_fct_till_date
  FROM ffc_program fp
  INNER JOIN client_attributes as dc ON fp.customer_id = dc.client_id
  WHERE
    fp.day_at >= fp.quarter_start
    AND fp.day_at <= LEAST(DATE_TRUNC('WEEK', current_date()) - INTERVAL '1 DAYS', fp.quarter_end)
  GROUP BY ALL
),

variance_till_date_wf AS (
  select
    a.quarter_start,
    a.quarter_end,
    a.forecast_type,
    a.program,
    (b.actuals_till_date - a.working_fct_till_date)/a.working_fct_till_date as variance
  from working_fct_till_date as a
  LEFT JOIN actuals_till_date as b
    ON a.quarter_start = b.quarter_start
    AND a.forecast_type = b.forecast_type
    AND a.program = b.program
),

adjusted_outlook_wf AS (
  select
    a.quarter_start,
    a.quarter_end,
    a.forecast_type,
    a.program,
    (a.actuals_till_date + b.working_fct_future * (1+COALESCE(c.variance, 0))) as adjusted_outlook_wf
  from
    actuals_till_date AS a
    LEFT JOIN working_fct_future AS b
      ON a.quarter_start = b.quarter_start
      AND a.forecast_type = b.forecast_type
      AND a.program = b.program
    LEFT JOIN variance_till_date_wf AS c
      ON a.quarter_start = c.quarter_start
      AND a.forecast_type = c.forecast_type
      AND a.program = c.program
),

baseline_fct_till_date AS (
  SELECT
    fpb.quarter_start,
    fpb.quarter_end,
    CASE
      WHEN fpb.estimate_type IN ('y2', 'y3+') THEN 'Y2+'
      WHEN dc.client_cohort=YEAR(fpb.day_at) AND dc.is_intra_year='true' THEN 'intra_year'
      WHEN lower(dc.line_of_business) IN ('federal') THEN 'federal'
      WHEN lower(dc.line_of_business) IN ('fully insured') THEN 'fully_insured'
      WHEN lower(dc.line_of_business) IN ('medicare', 'medicaid') THEN 'medicare'
      WHEN dc.client_cohort<YEAR(fpb.day_at) THEN 'legacy_client'
      WHEN dc.client_cohort=YEAR(fpb.day_at) THEN 'new_client'
      else null
    END AS forecast_type,
    fpb.program,
    SUM(forecast) as baseline_fct_till_date
  FROM ffc_program_baseline fpb
  INNER JOIN client_attributes as dc ON fpb.customer_id = dc.client_id
  WHERE
    fpb.day_at >= fpb.quarter_start
    AND fpb.day_at <= LEAST(DATE_TRUNC('WEEK', current_date()) - INTERVAL '1 DAYS', fpb.quarter_end)
  GROUP BY ALL
),

variance_till_date_baseline AS (
  select
    a.quarter_start,
    a.quarter_end,
    a.forecast_type,
    a.program,
    (b.actuals_till_date - a.baseline_fct_till_date)/a.baseline_fct_till_date as variance
  from baseline_fct_till_date as a
  LEFT JOIN actuals_till_date as b
    ON a.quarter_start = b.quarter_start
    AND a.forecast_type = b.forecast_type
    AND a.program = b.program
),

adjusted_outlook_baseline AS (
  select
    a.quarter_start,
    a.quarter_end,
    a.forecast_type,
    a.program,
    (a.actuals_till_date + b.baseline_fct_future * (1+COALESCE(c.variance, 0))) as adjusted_outlook_baseline
  from
    actuals_till_date AS a
    LEFT JOIN baseline_fct_future AS b
      ON a.quarter_start = b.quarter_start
      AND a.forecast_type = b.forecast_type
      AND a.program = b.program
    LEFT JOIN variance_till_date_baseline AS c
      ON a.quarter_start = c.quarter_start
      AND a.forecast_type = c.forecast_type
      AND a.program = c.program
),

okr AS (
  select
    bf.quarter_start,
    bf.quarter_end,
    bf.forecast_type,
    bf.program,
    bf.baseline_forecast,
    bf.baseline_forecast * COALESCE(omm.okr_multiplier, 1.0) AS okr_goal
  FROM baseline_fct bf
  LEFT JOIN okr_multipliers_mapped omm
    ON bf.quarter_start = omm.quarter_start
    AND lower(trim(bf.forecast_type)) = lower(trim(omm.forecast_type))
),

okr_till_date AS (
  select
    bftd.quarter_start,
    bftd.quarter_end,
    bftd.forecast_type,
    bftd.program,
    bftd.baseline_fct_till_date * COALESCE(omm.okr_multiplier, 1.0) AS okr_goal_till_date
  FROM baseline_fct_till_date bftd
  LEFT JOIN okr_multipliers_mapped omm
    ON bftd.quarter_start = omm.quarter_start
    AND lower(trim(bftd.forecast_type)) = lower(trim(omm.forecast_type))
)

select
  COALESCE(a.quarter_start, b.quarter_start, e.quarter_start) as quarter_start,
  COALESCE(a.quarter_end, b.quarter_end, e.quarter_end) as quarter_end,
  COALESCE(a.forecast_type, b.forecast_type, e.forecast_type) as forecast_type,
  COALESCE(a.program, b.program, e.program) as program,
  COALESCE(a.baseline_forecast,0) as baseline_forecast,
  COALESCE(b.working_fct,0) as working_fct,
  -- For future quarters, use working_fct if outlook is NULL
  COALESCE(c.outlook,
    CASE
      WHEN COALESCE(a.quarter_start, b.quarter_start) > DATE_TRUNC('QUARTER', current_date())
        THEN COALESCE(b.working_fct, 0)
      ELSE 0
    END
  ) as outlook,
  CASE
    WHEN b.working_fct > 0 THEN (c.outlook - b.working_fct)/b.working_fct
    ELSE NULL
  END AS variance_wf,
  d.adjusted_outlook_wf,
  g.adjusted_outlook_baseline,
  COALESCE(e.actuals_till_date,0) as actuals_till_date,
  f.working_fct_till_date,
  i.baseline_fct_till_date,
  CASE
    WHEN f.working_fct_till_date > 0 THEN (e.actuals_till_date - f.working_fct_till_date) / f.working_fct_till_date
    ELSE NULL
  END as variance_till_date_wf,
  CASE
    WHEN i.baseline_fct_till_date > 0 THEN (e.actuals_till_date - i.baseline_fct_till_date) / i.baseline_fct_till_date
    ELSE NULL
  END AS variance_till_date_baseline,
  h.okr_goal,
  CASE
    WHEN h.okr_goal > 0 THEN (c.outlook - h.okr_goal)/h.okr_goal
    ELSE NULL
  END as variance_okr,
  j.okr_goal_till_date,
  (e.actuals_till_date - j.okr_goal_till_date) as actuals_okr_delta,
  CASE
    WHEN j.okr_goal_till_date > 0 THEN (e.actuals_till_date - j.okr_goal_till_date)/j.okr_goal_till_date
    ELSE NULL
  END as perc_delta
FROM baseline_fct as a
FULL OUTER JOIN working_fct as b
  ON a.quarter_start = b.quarter_start
  AND a.forecast_type = b.forecast_type
  AND a.program = b.program
FULL OUTER JOIN outlook as c
  ON COALESCE(a.quarter_start, b.quarter_start) = c.quarter_start
  AND COALESCE(a.forecast_type, b.forecast_type) = c.forecast_type
  AND COALESCE(a.program, b.program) = c.program
LEFT JOIN adjusted_outlook_wf as d
  ON COALESCE(a.quarter_start, b.quarter_start) = d.quarter_start
  AND COALESCE(a.forecast_type, b.forecast_type) = d.forecast_type
  AND COALESCE(a.program, b.program) = d.program
LEFT JOIN adjusted_outlook_baseline AS g
  ON COALESCE(a.quarter_start, b.quarter_start) = g.quarter_start
  AND COALESCE(a.forecast_type, b.forecast_type) = g.forecast_type
  AND COALESCE(a.program, b.program) = g.program
FULL OUTER JOIN actuals_till_date AS e
  ON COALESCE(a.quarter_start, b.quarter_start) = e.quarter_start
  AND COALESCE(a.forecast_type, b.forecast_type) = e.forecast_type
  AND COALESCE(a.program, b.program) = e.program
LEFT JOIN working_fct_till_date AS f
  ON COALESCE(a.quarter_start, b.quarter_start) = f.quarter_start
  AND COALESCE(a.forecast_type, b.forecast_type) = f.forecast_type
  AND COALESCE(a.program, b.program) = f.program
LEFT JOIN okr AS h
  ON COALESCE(a.quarter_start, b.quarter_start) = h.quarter_start
  AND COALESCE(a.forecast_type, b.forecast_type) = h.forecast_type
  AND COALESCE(a.program, b.program) = h.program
LEFT JOIN baseline_fct_till_date AS i
  ON COALESCE(a.quarter_start, b.quarter_start) = i.quarter_start
  AND COALESCE(a.forecast_type, b.forecast_type) = i.forecast_type
  AND COALESCE(a.program, b.program) = i.program
LEFT JOIN okr_till_date as j
  ON COALESCE(a.quarter_start, b.quarter_start) = j.quarter_start
  AND COALESCE(a.forecast_type, b.forecast_type) = j.forecast_type
  AND COALESCE(a.program, b.program) = j.program
WHERE COALESCE(a.quarter_start, b.quarter_start, e.quarter_start) is not null
ORDER BY quarter_start, forecast_type, program DESC
