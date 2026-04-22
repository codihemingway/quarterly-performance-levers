with user_min_subs as (
    select distinct user_id
        , min(subscription_start_date) as first_sub_starts_at
        , min(subscription_end_date) as first_sub_ends_at
    from dbt_prod.fct_users_enrollments as bs
    where true
        and subscription_start_date is not null
    group by all

), user_subs as (
    select distinct s.user_id
        , s.subscription_start_date as starts_at
        , s.subscription_end_date as ends_at
        , s.subscription_start_date as created_at
        , s.enrollment_year_number as year_count_sub
        , s.program
        , s.primary_indication
        , ums.first_sub_starts_at
        , case when s.subscription_start_date = ums.first_sub_starts_at then 1 
                else ceil(date_diff(s.subscription_start_date, ums.first_sub_starts_at)/365) end as year_count_rel_first_sub
    from dbt_prod.fct_users_enrollments as s
    left join user_min_subs as ums
        on s.user_id = ums.user_id
    where true
        and subscription_start_date is not null

), termed_client_users as (
    select user_id
        , min(term_date) as term_date
    from (
        select distinct du.user_id
            , ci.term_date_at as term_date
        from dbt_prod.dim_users as du
        inner join dbt_prod.dim_clients_insurers as ci
            on du.client_id = ci.client_id
                and du.insurer_id = ci.insurer_id
                and ci.term_date_at < now()
        
        union 
        select distinct du.user_id
            , dc.cancelled_at as term_date
        from dbt_prod.dim_users as du
        inner join dbt_prod.dim_clients as dc
            on du.client_id = dc.client_id
                and dc.cancelled_at < now()
        ) as a
    group by all

), client_exclusions as (
    select DISTINCT uv.user_id
    FROM dbt_prod.dim_users AS uv
    left join dbt_prod.dim_clients as dc
        on uv.client_id = dc.client_id
            and (dc.client_identifier in ('nrgenergy','verisk','mypath','richproducts','team','signode','metrohospital','exeter','axient','bhi','centralsquaretechnologies','amerigas','putnaminvestments','mohawk','centralusd','gainwelltechnologies','tyson','mobilitymaterials','owensminor','anduril','lifestance','smiledirectclub','ardaghgroup','nyp','dioceseofstpetersburg','cvshealth','florida','918','scentsy','newjersey','nustarenergy','trstx','envoyair','healthselect','erstexas','sleepnumber','signode','foundationforseniorliving','endousa','tangoe','rathgibson','corellebrands','comoto','dccpropane','communityhealthok','hostessbrands','buckeyeohio','illumina','quantlab','regencycenters','granges','missionhealthcommunities','corporatetravel','costarrealty','weldcountyco','hoshizaki','independentfinancial','gdenergyproducts','hachettebookgroup','armacell','amys','akfgroup','westwindsorplainsboro','sterlingcheck','mauworkforce','fredhaasmotors','glenbrook','kroger','iuoecoalition','wyndenstark','thrivepediatric','willscot','boarshead','rumbleon','gulfdistributing','lewisbakeries','norco','hapaglloyd','mcdermott','enlinkmidstream','kehe','sunlandasphalt','reliableroofingcompany','vikingcocacola','hoovercs','waterservicecorp','silverbow','bsra','akorn','amgen','athene','avantax','avoncompany','axientcorp','bcbsvtem','bdsi','bedbathbeyond','benefitfocus','birchstoneresidential','bluekcma','bryancaveleightonpaisnerllp','bsra','ccps','celebrityhomeloans','celgene','centralsteel','changehealthcare','chubb','cincinnatichildrens','cityoftaylorville','cognosante','cornerstone','crown','cuninghamarchitecture','cwa142wvfrontier','dcp','diamondlinedelivery','dominos','ecuhealth','esi','firstrepublicbank','greatexpressionsdental','hillrom','homedepot','hydranautics','ipalco','jcpenney','jenkinscountyboc','jmac','jshelwig','level10','marriott','maximus','menningerclinic','nationalinstruments','networkhealth','oerlikon','parpacific','partycityholdings','sehbp','sfhss','shawneemilling','soleraholdings','southwestchristian','sparksmarketing','sprint','townshipofraritan','usoncology','witmerpublicsafety','xilinx','yorkvillecusd115','zappos','zynga','bsra','superstoreindustries','thrivepediatric','opcmiav100','uts','trinityhealth','sedol','crowtherroofing','viantmedical','animalsupply','forumenergy','asante','teaneckschools','texasschoolshealthbenefits','petersonholding')
                or dc.client_identifier in ('autozone','mcw','uts','animalsupply','jwaluminum','utcare','wai','healthselect','rei'))
    WHERE uv.client_id in (24, 53, 55) -- vip client_ids
        OR uv.client_id in (478, 214, 311, 610, 634, 652, 683) -- exclusion client_ids
        OR uv.client_id in (27,30,43,240,63,182,267,164,85,214,326,34,253,610,634,2024,386,243,339,280,328
                        ,332,345,291,256,245,294,226,307,343,36,7,105,63,302,198,76,90,351,69,34,326,227
                        ,179,299,197,2024,279,908)  -- growth marketing exclusion client list
        or dc.client_id is not null

), stop_users_tags AS ( -- pull users who can't be contacted via marketing
    SELECT DISTINCT uv.user_id
    FROM dbt_prod.dim_users AS uv
    LEFT JOIN public.public_enrollment_engagement_statuses AS es
        ON uv.user_id = es.user_id
            AND es.active = true
            and es.status != 'opt_in'
    WHERE lower(uv.user_tags) ILIKE '%no_sms%'
        OR lower(uv.user_tags) ILIKE '%no_marketing%'
        OR lower(uv.user_tags) ILIKE '%opt_out%'
        OR lower(uv.user_tags) ILIKE '%spanish%'
        OR lower(uv.user_tags) ILIKE '%no sms%'
        OR lower(uv.user_tags) ILIKE '%no marketing%'
        OR lower(uv.user_tags) ILIKE '%no-ensomarketing%'
        OR uv.is_vip != FALSE
        OR es.user_id IS NOT NULL

), m360_ineligibility_date as (
    -- Find when users became ineligible (first false after true, or first ineligible record)
    SELECT hhuuid
        , MIN(published_date) as became_ineligible_date
    FROM (
        SELECT hhuuid
            , is_eligible
            , published_date
            , LAG(is_eligible) OVER (PARTITION BY hhuuid ORDER BY published_date) as prev_is_eligible
        FROM member360.eligibility_staging
    ) as a
    WHERE is_eligible = false 
        AND (prev_is_eligible = true OR prev_is_eligible IS NULL)
    GROUP BY hhuuid

), user_contract_expiry as (
    -- Find contract end date for each user based on their client_insurer
    -- NULL end_date means open-ended contract (never expires)
    SELECT u.user_id
        , MAX(co.end_date) as contract_end_date
        , MAX(CASE WHEN co.end_date IS NULL AND co.id IS NOT NULL THEN 1 ELSE 0 END) as has_open_ended_contract
    FROM dbt_prod.dim_users AS u
    LEFT JOIN dbt_prod.dim_clients_insurers AS ci
        ON u.client_id = ci.client_id
            AND u.insurer_id = ci.insurer_id
    LEFT JOIN contract.contract_contract AS co
        ON ci.client_insurer_id = co.clients_insurer_id
    GROUP BY u.user_id

), subscriptions_y2 as (
    select s.user_id
        , uv.user_uuid
        , s.first_sub_starts_at
        , s.year_count_rel_first_sub
        , s.year_count_sub -- as year_count_cur_sub
        , s.starts_at -- as starts_at_cur_sub
        , s.ends_at -- as ends_at_cur_sub
        , s.program
        , s.primary_indication
        , uv.client_id
        , uv.client_name as client
        , uv.billing_partnership_id
        , uv.billing_partnership_name
        , tcu.term_date as client_term_date
        , max(case when s.ends_at >= tcu.term_date then 1 else 0 end) as is_termed_client
        , mid.became_ineligible_date as insurance_ineligible_date
        , max(case when s.ends_at >= mid.became_ineligible_date then 1 else 0 end) as is_insurance_ineligible
        , uce.contract_end_date
        , uce.has_open_ended_contract
        , max(case when uce.has_open_ended_contract = 1 then 0  -- open-ended contract = never expires
                   when uce.contract_end_date IS NULL then 1     -- no contract found = treat as expired
                   when s.ends_at >= uce.contract_end_date then 1 
                   else 0 end) as is_contract_expired
        , max(case when ce.user_id is not null then 1 else 0 end) as is_client_exclusion
        , max(case when sut.user_id is not null then 1 else 0 end) as is_stop_user_tag
        , max(date_trunc('day', ea.activity_date_at)) as last_activity --_cur_sub
        , max(case when s.starts_at > now() - interval '28 day' then null
              when date_trunc('day', ea.activity_date_at) between s.starts_at::date + 22 and s.starts_at::date + 28 then 1 
              else 0 end) as is_active_4th_wk
        , max(case when s.ends_at > now() then null
              when date_trunc('day', ea.activity_date_at) between s.ends_at - interval '4 weeks' and s.ends_at then 1 
              else 0 end) as is_active_last_4_wk_end
        , y1.year_count_sub as year_count_prev_sub
        , y1.starts_at as starts_at_prev_sub
        , y1.ends_at as ends_at_prev_sub
        , y1.program as program_prev_sub
        , y1.primary_indication as primary_indication_prev_sub
        , max(date_trunc('day', ea1.activity_date_at)) as last_activity_prev_sub --_cur_sub
        , max(case when y1.starts_at > now() - interval '28 day' then null
              when date_trunc('day', ea1.activity_date_at) between y1.starts_at::date + 22 and y1.starts_at::date + 28 then 1 
              else 0 end) as is_active_4th_wk_prev_sub
        , max(case when y1.ends_at > now() then null
              when date_trunc('day', ea1.activity_date_at) between y1.ends_at - interval '4 weeks' and y1.ends_at then 1 
              else 0 end) as is_active_last_4_wk_prev_sub
        , y2.year_count_sub as year_count_next_sub
        , y2.starts_at as starts_at_next_sub
        , y2.ends_at as ends_at_next_sub
        , y2.program as program_next_sub
        , y2.primary_indication as primary_indication_next_sub
        , floor(date_diff(s.starts_at::date, y1.ends_at::date)/7) as weeks_to_resub
        , case when y1.user_id is not null then 1 else 0 end as is_y2_renewed
    from user_subs as s 
    inner join dbt_prod.dim_users as uv
        on s.user_id = uv.user_id 
    left join user_subs as y1
        on s.user_id = y1.user_id
            and y1.year_count_sub = s.year_count_sub - 1
    left join user_subs as y2
        on s.user_id = y2.user_id
            and y2.year_count_sub = s.year_count_sub + 1
    left join dbt_prod.fct_user_engagement_activities as ea
        on s.user_id = ea.user_id
            and date_trunc('day', ea.activity_date_at) between s.starts_at and s.ends_at
            and ea.activity_event_type != 'article_read'
    left join dbt_prod.fct_user_engagement_activities as ea1 
        on y1.user_id = ea1.user_id
            and date_trunc('day', ea1.activity_date_at) between y1.starts_at and y1.ends_at
            and ea1.activity_event_type != 'article_read'
    left join termed_client_users as tcu
        on s.user_id = tcu.user_id
    left join client_exclusions as ce
        on s.user_id = ce.user_id
    left join stop_users_tags as sut
        on s.user_id = sut.user_id
    left join m360_ineligibility_date as mid
        on uv.user_uuid = mid.hhuuid
    left join user_contract_expiry as uce
        on s.user_id = uce.user_id
    where 1=1
        and s.starts_at >= make_date(year(current_date()), 1, 1)
        and s.year_count_sub >= 2
    group by all

), enso_tag_date as (
    select distinct tg.taggable_id as user_id
        --, t.name as tag_name
        , min(date_trunc('day', tg.created_at)) as tag_date
    from public.public_taggings as tg  
    inner join public.public_tags as t 
        on t.id = tg.tag_id
    where taggable_type = 'User'
        and (t.name like '%enable_enso%')
    group by tg.taggable_id

), fct_y2_outreached_users as (
    select *
        , concat_ws('x_x', outreach_type, campaign, template_name) as outreach_identifier
    from dbt_prod.fct_y2_outreached_users

), mixpanel_events as (
    -- Get distinct_id to uuid mapping for better match rates
    with distinct_uuid as (
        select distinct distinct_id
            , uuid
        from rollups.mixpanel_onboarding_prod_utms
    ),
    cte1 AS (
      select 
        mp.user_id,
        mp.distinct_id,
        mp.insert_id,
        mp.occurred_at,
        mp.name,
        mp.properties,
        mp.current_url,
        mp.initial_referrer,
        mp.referring_domain,
        mp.initial_referring_domain,
        NULLIF(get_json_object(mp.properties, '$.referrer'),'') AS referrer,
        NULLIF( mp.referrer,'') AS referrer_dollar,
        NULLIF(get_json_object(mp.properties, '$.utm_medium'),'') as utm_medium,
        NULLIF(get_json_object(mp.properties, '$.utm_source'),'') as utm_source,
        NULLIF(get_json_object(mp.properties, '$.utm_campaign'),'') as utm_campaign,
        NULLIF(get_json_object(mp.properties, '$.utm_template'),'') as utm_template,
        NULLIF(get_json_object(mp.properties, '$.utm_content'),'') as utm_content,
        NULLIF(get_json_object(mp.properties, '$.utm_term'),'') as utm_term,
        NULLIF(get_json_object(replace(mp.properties, 'utm_medium [last touch]', 'utm_medium_last_touch'), '$.utm_medium_last_touch'),'') as utm_medium_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'utm_source [last touch]', 'utm_source_last_touch'), '$.utm_source_last_touch'),'') as utm_source_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'utm_campaign [last touch]', 'utm_campaign_last_touch'), '$.utm_campaign_last_touch'),'') as utm_campaign_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'utm_template [last touch]', 'utm_template_last_touch'), '$.utm_template_last_touch'),'') as utm_template_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'utm_content [last touch]', 'utm_content_last_touch'), '$.utm_content_last_touch'),'') as utm_content_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'utm_term [last touch]', 'utm_term_last_touch'), '$.utm_term_last_touch'),'') as utm_term_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'sequence [last touch]', 'sequence_last_touch'), '$.sequence_last_touch'),'') as sequence_no_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'exp_name [last touch]', 'exp_name_last_touch'), '$.exp_name_last_touch'),'') as exp_name_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'exp_variant [last touch]', 'exp_variant_last_touch'), '$.exp_variant_last_touch'),'') as exp_variant_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'marketing_id [last touch]', 'marketing_id_last_touch'), '$.marketing_id_last_touch'),'') as marketing_id_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'mailformat [last touch]', 'mailformat_last_touch'), '$.mailformat_last_touch'),'') as mailformat_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'aud_segment [last touch]', 'aud_segment_last_touch'), '$.aud_segment_last_touch'),'') as aud_segment_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'template_name [last touch]', 'template_name_last_touch'), '$.template_name_last_touch'),'') as template_name_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'template_id [last touch]', 'template_id_last_touch'), '$.template_id_last_touch'),'') as template_id_last_touch,
        NULLIF(get_json_object(replace(mp.properties, 'experience [last touch]', 'experience_last_touch'), '$.experience_last_touch'),'') as experience_last_touch,
        NULLIF(get_json_object(mp.properties, '$.sequence'),'') as sequence_no,
        NULLIF(get_json_object(mp.properties, '$.Client'),'') as client,
        NULLIF(get_json_object(mp.properties, '$.exp_name'),'') as exp_name,
        NULLIF(get_json_object(mp.properties, '$.exp_variant'),'') as exp_variant,
        NULLIF(get_json_object(mp.properties, '$.marketing_id'),'') as marketing_id,
        NULLIF(get_json_object(mp.properties, '$.mailformat'),'') as mailformat,
        NULLIF(get_json_object(mp.properties, '$.aud_segment'),'') as aud_segment,
        NULLIF(get_json_object(mp.properties, '$.template_name'),'') as template_name,
        NULLIF(get_json_object(mp.properties, '$.template_id'),'') as template_id,
        NULLIF(get_json_object(mp.properties, '$.experience'),'') as experience,
        NULLIF(get_json_object(mp.properties, '$.Current URL'),'') AS page_url,
        -- Coalesce uuid from distinct_id mapping with original user_id for better match rates
        coalesce(d.uuid, mp.user_id) as user_uuid
      FROM mixpanel_onboarding.event_view mp
      LEFT JOIN distinct_uuid d
        ON mp.distinct_id = d.distinct_id
      WHERE mp.occurred_at >= '2023-01-01'  -- Date filter for performance
    ),
    cte2 AS (
        select *, COALESCE(SUBSTRING(REGEXP_SUBSTR(referrer_dollar, 'utm_medium=([^&?/]*)'), 12),
                    SUBSTRING(REGEXP_SUBSTR(referrer, 'utm_medium=([^&?/]*)'), 12)) as referrer_medium from cte1
    ),
    cte3 AS (
        select *
            , LOWER(COALESCE(utm_medium_last_touch, referrer_medium, utm_medium)) as utm_medium_final
            , COALESCE(NULLIF(get_json_object(replace(properties, 'utm_source [last touch]', 'utm_source_last_touch'), '$.utm_source_last_touch'),''), COALESCE(SUBSTRING(REGEXP_SUBSTR(NULLIF(referrer,''), 'utm_source=([^&?/]*)'), 12),SUBSTRING(REGEXP_SUBSTR(NULLIF(get_json_object(properties, '$.referrer'),''), 'utm_source=([^&?/]*)'), 12))
                ,NULLIF(get_json_object(properties, '$.utm_source'),'')) as utm_source_final
            , COALESCE(NULLIF(get_json_object(replace(properties, 'utm_campaign [last touch]', 'utm_campaign_last_touch'), '$.utm_campaign_last_touch'),''), COALESCE(SUBSTRING(REGEXP_SUBSTR(NULLIF(referrer,''), 'utm_campaign=([^&?/]*)'), 14),SUBSTRING(REGEXP_SUBSTR(NULLIF(get_json_object(properties, '$.referrer'),''), 'utm_campaign=([^&?/]*)'), 14))
                ,NULLIF(get_json_object(properties, '$.utm_campaign'),'')) as utm_campaign_final
            , COALESCE(NULLIF(get_json_object(replace(properties, 'template_name [last touch]', 'template_name_last_touch'), '$.template_name_last_touch'),''), COALESCE(SUBSTRING(REGEXP_SUBSTR(NULLIF(referrer,''), 'template_name=([^&?/]*)'), 15),SUBSTRING(REGEXP_SUBSTR(NULLIF(get_json_object(properties, '$.referrer'),''), 'template_name=([^&?/]*)'), 15))
                ,NULLIF(get_json_object(properties, '$.template_name'),'')) as template_name_final
            , COALESCE(NULLIF(get_json_object(replace(properties, 'aud_segment [last touch]', 'aud_segment_last_touch'), '$.aud_segment_last_touch'),''), COALESCE(SUBSTRING(REGEXP_SUBSTR(NULLIF(referrer,''), 'aud_segment=([^&?/]*)'), 13),SUBSTRING(REGEXP_SUBSTR(NULLIF(get_json_object(properties, '$.referrer'),''), 'aud_segment=([^&?/]*)'), 13))
                ,NULLIF(get_json_object(properties, '$.aud_segment'),'')) as aud_segment_final
            , date_trunc('day', occurred_at) as event_date
        from cte2
        -- Only keep events with at least one non-null UTM/marketing value
        WHERE COALESCE(utm_medium_last_touch, referrer_medium, utm_medium) is not null
           OR utm_source is not null OR utm_source_last_touch is not null
           OR utm_campaign is not null OR utm_campaign_last_touch is not null
           OR template_name is not null OR template_name_last_touch is not null
           OR aud_segment is not null OR aud_segment_last_touch is not null
           OR name is not null 
    )
    -- Aggregate to last event per user per day for performance
    select 
        user_uuid,
        event_date,
        max(occurred_at) as occurred_at,
        max_by(utm_medium_final, case when utm_medium_final is not null then occurred_at end) as utm_medium_final,
        max_by(utm_source_final, case when utm_source_final is not null then occurred_at end) as utm_source_final,
        max_by(utm_campaign_final, case when utm_campaign_final is not null then occurred_at end) as utm_campaign_final,
        max_by(template_name_final, case when template_name_final is not null then occurred_at end) as template_name_final,
        max_by(aud_segment_final, case when aud_segment_final is not null then occurred_at end) as aud_segment_final,
        max_by(name, case when name is not null then occurred_at end) as name
    from cte3
    group by user_uuid, event_date

), final_renewal_outreach as (
    select distinct s.*
        , etd.tag_date as enso_optin_date
        
        -- renewal types
        , max(case when (fyo.outreach_identifier ilike '%enso%' or fyo.outreach_identifier ilike '%mg%' or fyo.outreach_identifier ilike '%inc%' 
                              or fyo.outreach_identifier ilike '%y3_open_enrollment%' or fyo.outreach_identifier ilike '%inactive%' or fyo.outreach_identifier ilike '%challenge dm%')
                          and s.ends_at_prev_sub < fyo.sent_date
                          and s.starts_at between fyo.sent_date and fyo.sent_date + interval '6 months' then 1 else 0 end) as is_paid_renewal
        , max(case when fyo.outreach_identifier ilike '%enso%'
                          and s.ends_at_prev_sub < fyo.sent_date
                          and s.starts_at between fyo.sent_date and fyo.sent_date + interval '6 months' then 1 else 0 end) as is_paid_renewal_enso
                          
        , max(case when not (fyo.outreach_identifier ilike '%enso%' or fyo.outreach_identifier ilike '%mg%' or fyo.outreach_identifier ilike '%inc%' 
                              or fyo.outreach_identifier ilike '%y3_open_enrollment%' or fyo.outreach_identifier ilike '%inactive%' or fyo.outreach_identifier ilike '%challenge dm%')
                          and s.ends_at_prev_sub < fyo.sent_date
                          and s.starts_at between fyo.sent_date and fyo.sent_date + interval '6 months' then 1 else 0 end) as is_unpaid_renewal
                          
        , max(case when ea.activity_event_type = 'enso_session'
                      and (ea.activity_date_at >= s.starts_at - interval '1 month') 
                      then 1 else 0 end) as is_organic_renewal_enso
                      
        , max(case when s.starts_at > '2022-04-01' and etd.tag_date is not null then 1 else 0 end) as is_enso_renewal
        , max(case when s.starts_at > '2023-04-01' and pv.program_indication_identifier ilike '%pelvic%' then 1 else 0 end) as is_wph_renewal
        , max(case when fyo.user_id is not null 
                          and s.ends_at_prev_sub < fyo.sent_date
                          and (s.starts_at between fyo.sent_date and fyo.sent_date + interval '6 months') then 1 else 0 end) as is_chronic_renewal
        
        -- last marketing asset before renewal
        , max_by(
            fyo.outreach_medium, 
            case when fyo.sent_date >= s.starts_at - interval '6 months' 
                      and fyo.sent_date <= s.starts_at
                      and fyo.sent_date >= s.ends_at_prev_sub
                 then fyo.sent_date 
                 else null 
            end
        ) as last_marketing_medium
        
        , max_by(
            fyo.outreach_type, 
            case when fyo.sent_date >= s.starts_at - interval '6 months' 
                      and fyo.sent_date <= s.starts_at
                      and fyo.sent_date >= s.ends_at_prev_sub
                 then fyo.sent_date 
                 else null 
            end
        ) as last_marketing_outreach_type
        
        , max_by(
            fyo.template_name, 
            case when fyo.sent_date >= s.starts_at - interval '6 months' 
                      and fyo.sent_date <= s.starts_at
                      and fyo.sent_date >= s.ends_at_prev_sub
                 then fyo.sent_date 
                 else null 
            end
        ) as last_marketing_template_name
        
        , max(case when fyo.sent_date >= s.starts_at - interval '6 months' 
                        and fyo.sent_date <= s.starts_at
                        and fyo.sent_date >= s.ends_at_prev_sub
                   then fyo.sent_date 
                   else null 
              end) as last_marketing_asset_date
        
        -- last non-null mixpanel event before renewal
        , max_by(
            mxp.utm_medium_final, 
            case when mxp.event_date >= s.starts_at - interval '6 months' 
                      and mxp.event_date <= s.starts_at
                      and mxp.event_date >= s.ends_at_prev_sub
                      and mxp.utm_medium_final is not null
                 then mxp.occurred_at 
                 else null 
            end
        ) as last_mixpanel_utm_medium
        
        , max_by(
            mxp.utm_source_final, 
            case when mxp.event_date >= s.starts_at - interval '6 months' 
                      and mxp.event_date <= s.starts_at
                      and mxp.event_date >= s.ends_at_prev_sub
                      and mxp.utm_source_final is not null
                 then mxp.occurred_at 
                 else null 
            end
        ) as last_mixpanel_utm_source
        
        , max_by(
            mxp.utm_campaign_final, 
            case when mxp.event_date >= s.starts_at - interval '6 months' 
                      and mxp.event_date <= s.starts_at
                      and mxp.event_date >= s.ends_at_prev_sub
                      and mxp.utm_campaign_final is not null
                 then mxp.occurred_at 
                 else null 
            end
        ) as last_mixpanel_utm_campaign
        
        , max_by(
            mxp.template_name_final, 
            case when mxp.event_date >= s.starts_at - interval '6 months' 
                      and mxp.event_date <= s.starts_at
                      and mxp.event_date >= s.ends_at_prev_sub
                      and mxp.template_name_final is not null
                 then mxp.occurred_at 
                 else null 
            end
        ) as last_mixpanel_template_name
        
        , max_by(
            mxp.aud_segment_final, 
            case when mxp.event_date >= s.starts_at - interval '6 months' 
                      and mxp.event_date <= s.starts_at
                      and mxp.event_date >= s.ends_at_prev_sub
                      and mxp.aud_segment_final is not null
                 then mxp.occurred_at 
                 else null 
            end
        ) as last_mixpanel_aud_segment
        
        , max_by(
            mxp.name, 
            case when mxp.event_date >= s.starts_at - interval '6 months' 
                      and mxp.event_date <= s.starts_at
                      and mxp.event_date >= s.ends_at_prev_sub
                      and mxp.name is not null
                 then mxp.occurred_at 
                 else null 
            end
        ) as last_mixpanel_event_name
        
        , max(case when mxp.event_date >= s.starts_at - interval '6 months' 
                        and mxp.event_date <= s.starts_at
                        and mxp.event_date >= s.ends_at_prev_sub
                   then mxp.event_date 
                   else null 
              end) as last_mixpanel_event_date
        
        -- outreach types (6-month bound applied to both renewed and non-renewed users)
        , max(case when (fyo.outreach_identifier ilike '%enso%' or fyo.outreach_identifier ilike '%mg%' or fyo.outreach_identifier ilike '%inc%'
                              or fyo.outreach_identifier ilike '%y3_open_enrollment%' or fyo.outreach_identifier ilike '%inactive%' or fyo.outreach_identifier ilike '%challenge dm%')
                          and (s.starts_at_next_sub is not null and s.ends_at < fyo.sent_date and s.starts_at_next_sub between fyo.sent_date and fyo.sent_date + interval '6 months'
                              or s.starts_at_next_sub is null and s.ends_at < fyo.sent_date and fyo.sent_date <= s.ends_at + interval '6 months') then 1 else 0 end) as is_paid_outreached
        , max(case when (fyo.outreach_identifier ilike '%enso%')
                          and (s.starts_at_next_sub is not null and s.ends_at < fyo.sent_date and s.starts_at_next_sub between fyo.sent_date and fyo.sent_date + interval '6 months'
                              or s.starts_at_next_sub is null and s.ends_at < fyo.sent_date and fyo.sent_date <= s.ends_at + interval '6 months') then 1 else 0 end) as is_paid_outreached_enso

        , max(case when not (fyo.outreach_identifier ilike '%enso%' or fyo.outreach_identifier ilike '%mg%' or fyo.outreach_identifier ilike '%inc%'
                              or fyo.outreach_identifier ilike '%y3_open_enrollment%' or fyo.outreach_identifier ilike '%inactive%' or fyo.outreach_identifier ilike '%challenge dm%')
                          and (s.starts_at_next_sub is not null and s.ends_at < fyo.sent_date and s.starts_at_next_sub between fyo.sent_date and fyo.sent_date + interval '6 months'
                              or s.starts_at_next_sub is null and s.ends_at < fyo.sent_date and fyo.sent_date <= s.ends_at + interval '6 months') then 1 else 0 end) as is_unpaid_outreached

        , max(case when (fyo.outreach_identifier ilike '%enso%')
                          and (s.starts_at_next_sub is not null and s.ends_at < fyo.sent_date and s.starts_at_next_sub between fyo.sent_date and fyo.sent_date + interval '6 months'
                              or s.starts_at_next_sub is null and s.ends_at < fyo.sent_date and fyo.sent_date <= s.ends_at + interval '6 months') then 1 else 0 end) as is_enso_outreached
        , max(case when (fyo.outreach_identifier ilike '%wph%' and fyo.outreach_identifier not ilike '%mg%')
                          and (s.starts_at_next_sub is not null and s.ends_at < fyo.sent_date and s.starts_at_next_sub between fyo.sent_date and fyo.sent_date + interval '6 months'
                              or s.starts_at_next_sub is null and s.ends_at < fyo.sent_date and fyo.sent_date <= s.ends_at + interval '6 months') then 1 else 0 end) as is_wph_outreached
        , max(case when (fyo.outreach_identifier ilike '%chronic%' or fyo.outreach_identifier ilike '%mg%' or fyo.outreach_identifier ilike '%inc%'
                              or fyo.outreach_identifier ilike '%y3_open_enrollment%' or fyo.outreach_identifier ilike '%inactive%' or fyo.outreach_identifier ilike '%challenge dm%')
                          and (s.starts_at_next_sub is not null and s.ends_at < fyo.sent_date and s.starts_at_next_sub between fyo.sent_date and fyo.sent_date + interval '6 months'
                              or s.starts_at_next_sub is null and s.ends_at < fyo.sent_date and fyo.sent_date <= s.ends_at + interval '6 months') then 1 else 0 end) as is_chronic_outreached
        
    from subscriptions_y2 as s
    left join rollups.users_view as uv
        on s.user_id = uv.user_id
    left join rollups.pathways_view as pv
        on uv.application_pathway_id = pv.pathway_id
    left join enso_tag_date as etd
        on s.user_id = etd.user_id
    left join fct_y2_outreached_users as fyo
        on s.user_id = fyo.user_id
    left join mixpanel_events as mxp
        on uv.uuid = mxp.user_uuid
    left join dbt_prod.fct_user_engagement_activities as ea 
        on s.user_id = ea.user_id
            and ea.activity_event_type != 'article_read'
    group by all
    
), final as (
    select distinct * except(is_enso_renewal, is_wph_renewal, is_chronic_renewal, is_enso_outreached, is_wph_outreached, is_chronic_outreached
                            , is_paid_renewal, is_paid_renewal_enso, is_unpaid_renewal, is_organic_renewal_enso
                            , is_paid_outreached, is_paid_outreached_enso, is_unpaid_outreached)
        , case when is_enso_renewal = 1 then 'enso_renewal'
                when is_enso_renewal = 0 and is_wph_renewal = 1 then 'wph_renewal'
                when is_enso_renewal = 0 and is_wph_renewal = 0 and is_chronic_renewal = 1 then 'chronic_renewal'
                when is_enso_renewal = 0 and is_wph_renewal = 0 and is_chronic_renewal = 0 then 'unmarketed'
                else null end as outreach_type_attribution
        , case when is_enso_outreached = 1 then 'enso_outreached'
                when is_enso_outreached = 0 and is_wph_outreached = 1 then 'wph_outreached'
                when is_enso_outreached = 0 and is_wph_outreached = 0 and is_chronic_outreached = 1 then 'chronic_outreached'
                when is_enso_outreached = 0 and is_wph_outreached = 0 and is_chronic_outreached = 0 then 'unmarketed'
                else null end as outreach_type
                
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
            
    , case when is_paid_outreached = 1 then 'paid_outreached'
            when is_paid_outreached = 0 and is_unpaid_outreached = 1 then 'unpaid_outreached'
            when is_paid_outreached = 0 and is_unpaid_outreached = 0 then 'organic_outreached'
            else null end as y2_renewal_type_outreach
    , case when is_paid_outreached = 1 and is_paid_outreached_enso = 1 then 'paid_outreached_enso'
            when is_paid_outreached = 1 and is_paid_outreached_enso = 0 then 'paid_outreached_non_enso'
            when is_paid_outreached = 0 and is_unpaid_outreached = 1 then 'unpaid_outreached'
            when is_paid_outreached = 0 and is_unpaid_outreached = 0 then 'organic_outreached'
            else null end as y2_renewal_sub_type_outreach
    from final_renewal_outreach

), m360_eligibility_staging as (
    select distinct hhuuid
        , is_eligible
        , published_date
    from (
        select *
            , rank() over(partition by hhuuid order by published_date desc) as rank_hhuuid
        from member360.eligibility_staging
        ) as a
    where true
        and rank_hhuuid = 1

-- =============================================================================
-- Aggregate by subscription start week for the dashboard
-- =============================================================================
) select
    date_trunc('week', f.starts_at)::date                              as starts_at
    , count(distinct case when f.outreach_type_attribution = 'enso_renewal'
                          then f.user_id end)                          as enso_renewals
    , count(distinct case when f.outreach_type_attribution in ('wph_renewal', 'chronic_renewal')
                          then f.user_id end)                          as wph_or_chronic_renewals
    , count(distinct case when f.outreach_type_attribution = 'unmarketed'
                          then f.user_id end)                          as unmarketed_renewals
    , count(distinct case when f.y2_renewal_type = 'paid_renewal'
                          then f.user_id end)                          as paid_renewals
    , count(distinct case when f.y2_renewal_type = 'unpaid_renewal'
                          then f.user_id end)                          as unpaid_renewals
    , count(distinct case when f.y2_renewal_type = 'organic_renewal'
                          then f.user_id end)                          as organic_renewals
    , count(distinct f.user_id)                                        as total_renewals
from final as f
group by 1
order by 1