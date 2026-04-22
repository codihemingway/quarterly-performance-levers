-- =============================================================================
-- Y2 RENEWALS BY SUBSCRIPTION START DATE
-- =============================================================================
-- Returns daily renewal counts vs OKR forecast for the current year.
-- Used to populate the Y2 Renewals table on the performance levers dashboard.
--
-- Key output columns:
--   starts_at_max          - subscription start date (daily)
--   enso_renewals          - Enso-driven renewals
--   wph_or_chronic_renewals- WPH / chronic renewals
--   unmarketed_renewals    - unmarketed renewals
--   paid_renewals          - paid channel renewals
--   unpaid_renewals        - unpaid renewals
--   organic_renewals       - organic renewals
--   okr_forecast           - daily OKR forecast
-- =============================================================================

with renewals_okr_by_quarter as (
    select *
    from values
        (make_date(year(current_date()), 1, 1), 67000)
        , (make_date(year(current_date()), 4, 1), 86000)
        , (make_date(year(current_date()), 7, 1), 65000)
        , (make_date(year(current_date()), 10, 1), 63000)
    as renewal_data(quarter_start_date, renewals_okr)

), ffc_baseline_current as (
    select *
    from (
        select customer_id
            , day_at
            , client_type
            , estimate_type
            , estimate
            , coalesce(prop_acute,0) as prop_acute
            , row_number() over (partition by customer_id, day_at, client_type, estimate_type order by forecast_at asc) as row_num
        from dbt_prod.fct_forecasts_clients
        where true
            and estimate_type in ('y2', 'y3+')
            and day_at between make_date(year(current_date()), 1, 1) and make_date(year(current_date()), 3, 31)
            and forecast_at between make_date(year(current_date()), 1, 1) - interval '7 days' and make_date(year(current_date()), 1, 1)

        union
        select customer_id
            , day_at
            , client_type
            , estimate_type
            , estimate
            , coalesce(prop_acute,0) as prop_acute
            , row_number() over (partition by customer_id, day_at, client_type, estimate_type order by forecast_at asc) as row_num
        from dbt_prod.fct_forecasts_clients
        where true
            and estimate_type in ('y2', 'y3+')
            and day_at between make_date(year(current_date()), 4, 1) and make_date(year(current_date()), 6, 30)
            and forecast_at between make_date(year(current_date()), 4, 1) - interval '7 days' and make_date(year(current_date()), 4, 1)

        union
        select customer_id
            , day_at
            , client_type
            , estimate_type
            , estimate
            , coalesce(prop_acute,0) as prop_acute
            , row_number() over (partition by customer_id, day_at, client_type, estimate_type order by forecast_at asc) as row_num
        from dbt_prod.fct_forecasts_clients
        where true
            and estimate_type in ('y2', 'y3+')
            and day_at between make_date(year(current_date()), 7, 1) and make_date(year(current_date()), 9, 30)
            and forecast_at between make_date(year(current_date()), 7, 1) - interval '7 days' and make_date(year(current_date()), 7, 1)

        union
        select customer_id
            , day_at
            , client_type
            , estimate_type
            , estimate
            , coalesce(prop_acute,0) as prop_acute
            , row_number() over (partition by customer_id, day_at, client_type, estimate_type order by forecast_at asc) as row_num
        from dbt_prod.fct_forecasts_clients
        where true
            and estimate_type in ('y2', 'y3+')
            and day_at between make_date(year(current_date()), 10, 1) and make_date(year(current_date()), 12, 31)
            and forecast_at between make_date(year(current_date()), 10, 1) - interval '7 days' and make_date(year(current_date()), 10, 1)
    )
    where true
        and row_num = 1

), ffc_baseline_future as (
    select *
    from (
        select customer_id
            , day_at
            , client_type
            , estimate_type
            , estimate
            , coalesce(prop_acute,0) as prop_acute
            , row_number() over (partition by customer_id, day_at, client_type, estimate_type order by forecast_at asc) as row_num
        from dbt_prod.fct_forecasts_clients
        where true
            and estimate_type in ('y2', 'y3+')
            and day_at between (select max(day_at) + interval '1 day' from ffc_baseline_current) and make_date(year(current_date()), 12, 31)
            and forecast_at = (select max(forecast_at) from dbt_prod.fct_forecasts_clients)
        )
    where true
        and row_num = 1

), ffc_baseline as (
    select * from ffc_baseline_current
    union
    select * from ffc_baseline_future

), ffc_program_baseline as (
    select customer_id
        , day_at
        , client_type
        , estimate_type
        , 'chronic' as program
        , estimate * (1-prop_acute) as forecast
    from ffc_baseline
    union all
    select customer_id
        , day_at
        , client_type
        , estimate_type
        , 'acute' as program
        , estimate * prop_acute as forecast
    from ffc_baseline

), baseline_fct_daily as (
    select 'Y2+' AS forecast_type
        , dd.date_day as day_at
        , (s.`.mean`)/7 as baseline_forecast
    from prod_sandbox.dbt_tbalani.simulated_q1_baseline_y2_20260212 as s
    left join dbt_prod.dim_dates as dd
        on s.date_at = dd.week_start_date
    where true
        and s.client_type = '<aggregated>'
        and dd.date_day between '2026-01-01' and '2026-03-31'

    union

    select 'Y2+' AS forecast_type
        , day_at
        , sum(forecast) as baseline_forecast
    from ffc_program_baseline
    where true
        and day_at between '2026-04-01' and '2026-12-31'
    group by all

), quarterly_baseline as (
    select date_trunc('quarter', day_at) as quarter_start_date
        , sum(baseline_forecast) as baseline_forecast
    from baseline_fct_daily
    group by all

), quarterly_multiplier as (
    select qb.*
        , okr.renewals_okr
        , okr.renewals_okr / qb.baseline_forecast as multiplier
    from quarterly_baseline as qb
    left join renewals_okr_by_quarter as okr
        on qb.quarter_start_date = okr.quarter_start_date

), baseline_okr_daily as (
    select bd.day_at
        , bd.baseline_forecast
        , bd.baseline_forecast * qm.multiplier as okr_forecast
    from baseline_fct_daily as bd
    left join quarterly_multiplier as qm
        on date_trunc('quarter', bd.day_at) = qm.quarter_start_date

), baseline_okr_monthly as (
    select date_trunc('day', day_at) as month_start_date
        , sum(baseline_forecast) as baseline_forecast
        , sum(okr_forecast) as okr_forecast
    from baseline_okr_daily
    group by all

), monthly_okrs as (
    select *
        , okr_forecast * 0.55 as enso_okr
        , okr_forecast * 0.12 as chronic_wph_okr
        , okr_forecast * 0.33 as unmarketed_expected
        , okr_forecast * 0.45 as paid_okr
        , okr_forecast * 0.05 as unpaid_okr
        , okr_forecast * 0.50 as organic_okr
    from baseline_okr_monthly

), subs_base as (
    select distinct user_id
        , subscription_start_date as subscription_starts_at
        , subscription_end_date as subscription_ends_at
        , enrollment_year_number as subscription_year_count
    from dbt_prod.fct_users_enrollments as bs
    where true
        and subscription_start_date is not null

), subs as (
    select s.*
        , y1.subscription_year_count as subscription_year_count_prev_sub
        , y1.subscription_starts_at as subscription_starts_at_prev_sub
        , y1.subscription_ends_at as subscription_ends_at_prev_sub
        , y2.subscription_year_count as subscription_year_count_next_sub
        , y2.subscription_starts_at as subscription_starts_at_next_sub
        , y2.subscription_ends_at as subscription_ends_at_next_sub
    from subs_base as s
    left join subs_base as y1
        on s.user_id = y1.user_id
            and y1.subscription_year_count = s.subscription_year_count - 1
    left join subs_base as y2
        on s.user_id = y2.user_id
            and y2.subscription_year_count = s.subscription_year_count + 1

), enso_tag_date as (
    select distinct tg.taggable_id as user_id
        , min(date_trunc('day', tg.created_at)) as tag_date
    from public.public_taggings as tg
    inner join public.public_tags as t
        on t.id = tg.tag_id
    where taggable_type = 'User'
        and (t.name like '%enable_enso%')
    group by tg.taggable_id

), enso_eligible_clients as (
    select distinct client_id
    from public.public_client_configurations
    where configuration_id = 2
        and client_id != 25

), fct_y2_outreached_users as (
    select *
        , concat_ws('x_x', outreach_type, campaign, template_name) as outreach_identifier
    from dbt_prod.fct_y2_outreached_users

), renewals as (
    select distinct uv.user_id
        , max(case when uv.client_id in (select distinct client_id from enso_eligible_clients) then 1 else 0 end) as is_enso_client
        , etd.tag_date
        , max(case when lower(uv.tags) ilike '%enable_enso%' then 1 else 0 end) as is_enso_user

        , max(case when (fyo.outreach_identifier ilike '%enso%' or fyo.outreach_identifier ilike '%mg%' or fyo.outreach_identifier ilike '%inc%'
                              or fyo.outreach_identifier ilike '%y3_open_enrollment%' or fyo.outreach_identifier ilike '%inactive%' or fyo.outreach_identifier ilike '%challenge dm%')
                          and s.subscription_ends_at_prev_sub < fyo.sent_date
                          and s.subscription_starts_at between fyo.sent_date and fyo.sent_date + interval '6 months' then 1 else 0 end) as is_paid_renewal
        , max(case when fyo.outreach_identifier ilike '%enso%'
                          and s.subscription_ends_at_prev_sub < fyo.sent_date
                          and s.subscription_starts_at between fyo.sent_date and fyo.sent_date + interval '6 months' then 1 else 0 end) as is_paid_renewal_enso

        , max(case when not (fyo.outreach_identifier ilike '%enso%' or fyo.outreach_identifier ilike '%mg%' or fyo.outreach_identifier ilike '%inc%'
                              or fyo.outreach_identifier ilike '%y3_open_enrollment%' or fyo.outreach_identifier ilike '%inactive%' or fyo.outreach_identifier ilike '%challenge dm%')
                          and s.subscription_ends_at_prev_sub < fyo.sent_date
                          and s.subscription_starts_at between fyo.sent_date and fyo.sent_date + interval '6 months' then 1 else 0 end) as is_unpaid_renewal

        , max(case when ea.activity_event_type = 'enso_session'
                      and (ea.activity_date_at >= s.subscription_starts_at - interval '1 month')
                      then 1 else 0 end) as is_organic_renewal_enso

        , max(case when s.subscription_starts_at > '2022-04-01' and etd.tag_date is not null then 1 else 0 end) as is_enso_renewal
        , max(case when s.subscription_starts_at > '2023-04-01' and pv.program_indication_identifier ilike '%pelvic%' then 1 else 0 end) as is_wph_renewal
        , max(case when fyo.user_id is not null
                          and s.subscription_ends_at_prev_sub < fyo.sent_date
                          and (s.subscription_starts_at between fyo.sent_date and fyo.sent_date + interval '6 months') then 1 else 0 end) as is_chronic_renewal

        , s.subscription_starts_at
        , s.subscription_ends_at
        , s.subscription_year_count
        , max(case when s.subscription_year_count > 1 then 1 else 0 end) as is_y2_user
    from rollups.users_view as uv
    left join enso_tag_date as etd
        on cast(uv.user_id as varchar(100)) = cast(etd.user_id as varchar(100))
    left join subs as s
        on cast(uv.user_id as varchar(100)) = cast(s.user_id as varchar(100))
    left join rollups.pathways_view as pv
        on uv.application_pathway_id = pv.pathway_id
    left join fct_y2_outreached_users as fyo
        on uv.user_id = fyo.user_id
    left join dbt_prod.fct_user_engagement_activities as ea
        on uv.user_id = ea.user_id
            and ea.activity_event_type != 'article_read'
    group by all

), data as (
    select distinct user_id
        , is_enso_client
        , tag_date
        , is_enso_user
        , case when is_paid_renewal = 1 then 'paid_renewal'
                when is_paid_renewal = 0 and is_unpaid_renewal = 1 then 'unpaid_renewal'
                when is_paid_renewal = 0 and is_unpaid_renewal = 0 then 'organic_renewal'
                else null end as y2_renewal_type
        , case when is_paid_renewal = 1 and is_paid_renewal_enso = 1 then 'paid_renewal_enso'
                when is_paid_renewal = 1 and is_paid_renewal_enso = 0 then 'paid_renewal_non_enso'
                when is_paid_renewal = 0 and is_unpaid_renewal = 1 then 'unpaid_renewal'
                when is_paid_renewal = 0 and is_unpaid_renewal = 0 and is_organic_renewal_enso = 1 then 'organic_renewal_enso'
                when is_paid_renewal = 0 and is_unpaid_renewal = 0 and is_organic_renewal_enso = 0 then 'organic_renewal_et'
                else null end as y2_renewal_sub_type
        , case when is_enso_renewal = 1 then 'enso_renewal'
                when is_enso_renewal = 0 and is_wph_renewal = 1 then 'wph_or_chronic_renewal'
                when is_enso_renewal = 0 and is_wph_renewal = 0 and is_chronic_renewal = 1 then 'wph_or_chronic_renewal'
                when is_enso_renewal = 0 and is_wph_renewal = 0 and is_chronic_renewal = 0 then 'unmarketed'
                else null end as y2_user_type
        , subscription_starts_at
        , subscription_ends_at
        , subscription_year_count
        , is_y2_user
    from renewals
    where true
        and is_y2_user = 1

) select date_trunc('day', o.month_start_date) as starts_at_max
    , count(distinct case when d.y2_user_type = 'enso_renewal' then d.user_id else null end) as enso_renewals
    , count(distinct case when d.y2_user_type = 'wph_or_chronic_renewal' then d.user_id else null end) as wph_or_chronic_renewals
    , count(distinct case when d.y2_user_type = 'unmarketed' then d.user_id else null end) as unmarketed_renewals
    , o.enso_okr
    , o.chronic_wph_okr
    , o.unmarketed_expected

    , count(distinct case when d.y2_renewal_type = 'paid_renewal' then d.user_id else null end) as paid_renewals
    , count(distinct case when d.y2_renewal_type = 'unpaid_renewal' then d.user_id else null end) as unpaid_renewals
    , count(distinct case when d.y2_renewal_type = 'organic_renewal' then d.user_id else null end) as organic_renewals
    , o.paid_okr
    , o.unpaid_okr
    , o.organic_okr

    , o.baseline_forecast
    , o.okr_forecast
from monthly_okrs as o
left join data as d
    on date_trunc('day', o.month_start_date) = date_trunc('day', d.subscription_starts_at)
        and d.is_y2_user = 1
        and d.subscription_starts_at >= date_trunc('year', current_date())
group by all
order by 1
