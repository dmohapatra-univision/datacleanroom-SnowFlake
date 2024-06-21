
WITH vendor_test_data AS (
    SELECT
        -- one-way hashed email with an agreed upon hashing, ideally SHA2
        HEM as vendor_user_email
        ,  date(exposure_dtm_est) as impression_date
        -- ideally the timestamp in the device's local time
        -- or a timezone identifier could be supplied alongside timestamp
        -- trunc'd to 1 hour
        , exposure_dtm_est as impression_timestamp
        -- union all audience data to audience-level data, to get an aggregated picture
        -- alongside the audience-level data. Users often fall into multiple audiences
        , 'All Audiences' AS audience_name
        -- campaign identifier, often SMB or Jobseeker
        , 'Jobseeker' as campaign
        -- for a suppression list, all ad logs users are test group
        , 'Test' AS experiment_group
    FROM
        {{ app_data | sqlsafe }}.cleanroom.INDEED_Q4_BRAND_CAMPAIGN_EXPOSURE_vw
    WHERE
        impression_date >= TO_DATE('{{ campaign_start | sqlsafe }}')
        AND impression_date <= TO_DATE('{{ campaign_end | sqlsafe }}'))

    --UNION ALL

    --SELECT
    --    vendor_user_email_sha2
    --    , impression_date
    --    , impression_timestamp
        -- identifier on what audience this impression was targetted at usually extra code 
        -- is required to identify audiences from creative_names with regular expressions
    --    , audience_name
        -- campaign identifier, often SMB or Jobseeker
    --    , campaign
        -- for a suppression list, all ad logs users are test group
    --    , 'Test' AS experiment_group
    --FROM
    --    vendor_ad_logs
    --WHERE
    --    impression_date >= TO_DATE('<campaign_start>')
    --    AND impression_date <= TO_DATE('<campaign_end>')
--)

-- required if using a suppression list rather than house ads
, vendor_control_data AS (
    SELECT
        identifier as vendor_user_email
        ,'All Audiences' AS  audience_name
        , 'Jobseeker' as campaign
        -- suppression list users are control group
        , 'Control' AS experiment_group
    FROM
        {{ app_data | sqlsafe }}.cleanroom.INDEED_Q4_CONTROL_SEGMENT_VW
        where id_subtype = 'HEM'
)

, vendor_data_cleaned AS (
    SELECT
        vendor_user_email
        , audience_name
        , campaign
        , experiment_group
    FROM
        vendor_test_data
    
    UNION ALL
    
    SELECT
        vendor_user_email
        , audience_name
        , campaign
        , experiment_group
    FROM
        vendor_control_data
)

-- clean, filter and join the Indeed conversion data - SMB & JS
, indeed_data_cleaned AS (
    SELECT
        USER_ACCOUNT_EMAIL_SHA2 AS indeed_user_email
        , conversion_date
        , 'SMB NSAs' AS conversion_metric
    FROM
        {{ consumer_db | sqlsafe }}.{{ consumer_schema | sqlsafe }}.{{ consumer_smb_tbl | sqlsafe }}
    WHERE
        -- rough campaign conversions window, trimmed by campaign in the aggregated_conversion_data CTE
        conversion_date >= TO_DATE('{{ campaign_start | sqlsafe }}')
        AND conversion_date <= TO_DATE(dateadd(day, 60, '{{ campaign_end | sqlsafe }}')) -- dateadd(day, 60, '2023-12-31')

    UNION ALL

    SELECT
        USER_ACCOUNT_EMAIL_SHA2 AS indeed_user_email
        , conversion_date
        , 'JS Acct Creates' AS conversion_metric
    FROM
        {{ consumer_db | sqlsafe }}.{{ consumer_schema | sqlsafe }}.{{ consumer_js_tbl | sqlsafe }}
    WHERE
        -- rough campaign conversions window, trimmed by campaign in the aggregated_conversion_data CTE
        conversion_date >= TO_DATE('{{ campaign_start | sqlsafe }}')
        AND conversion_date <= TO_DATE(dateadd(day, 60, '{{ campaign_end | sqlsafe }}'))  --dateadd(day, 60, campaign_end)
)

, vendor_indeed_data_joined AS (
    SELECT
        v.vendor_user_email
        , v.audience_name
        --, v.impression_timestamp -- this field isn't carried thru
        --, v.impression_date
        , i.indeed_user_email
        --, i.conversion_date
        , v.campaign
        , v.experiment_group
        , i.conversion_metric
    FROM
        vendor_data_cleaned v
    LEFT JOIN
        indeed_data_cleaned i
        ON v.vendor_user_email = i.indeed_user_email
)

-- aggregate campaign data:
-- impressions population and users population
, aggregated_campaign_data AS (
    SELECT
        campaign
        , audience_name
        , experiment_group
        , COUNT(vendor_user_email) AS impressions_population
        , COUNT(DISTINCT vendor_user_email) AS vendor_users_population
    FROM
        vendor_indeed_data_joined
    GROUP BY
        campaign
        , audience_name
        , experiment_group
)

-- aggregate conversion data:
-- conversion types by group
, aggregated_conversion_data AS (
    SELECT
        campaign
        , audience_name
        , conversion_metric
        , experiment_group
        , COUNT(DISTINCT indeed_user_email) AS indeed_conversions
    FROM
        vendor_indeed_data_joined
    WHERE
        conversion_metric IS NOT NULL
    GROUP BY
        campaign
        , audience_name
        , conversion_metric
        , experiment_group
)
-- the aggregations above happen in separate CTEs as the
-- 'conversion_metric' field only exists for the conversion data

-- finally, select all the fields from each aggregated CTE
SELECT
    camp.campaign
    , camp.audience_name
    , conv.conversion_metric
    , camp.experiment_group
    , camp.impressions_population
    , camp.vendor_users_population
    , conv.indeed_conversions
FROM
    aggregated_campaign_data camp
LEFT JOIN
    aggregated_conversion_data conv
    ON camp.campaign = conv.campaign
    AND camp.audience_name = conv.audience_name
    AND camp.experiment_group = conv.experiment_group;