
WITH vendor_test_data AS (
    SELECT 
        -- one-way hashed email with an agreed upon hashing, ideally SHA2
        HEM as vendor_user_email,
        date(exposure_dtm_est) as impression_date
        -- ideally the timestamp in the device's local time
        -- or a timezone identifier could be supplied alongside timestamp
        -- trunc'd to 1 hour
        ,exposure_dtm_est as  impression_timestamp
        -- campaign identifier, often SMB or Jobseeker
        ,'Job Seeker Campaign' as campaign
        -- for a suppression list, all ad logs users are test group
        , 'Test' AS experiment_group
    FROM
        {{ app_data | sqlsafe }}.cleanroom.INDEED_Q4_BRAND_CAMPAIGN_EXPOSURE_vw --indeed_q4_brand_campaign_exposure
    WHERE
        impression_date >= TO_DATE('{{ campaign_start | sqlsafe }}')  -- set as a parameter
        AND impression_date <= TO_DATE('{{ campaign_end | sqlsafe }}') -- set as a parameter
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
, user_impression_frequency AS (
    SELECT
        vendor_user_email
        , COUNT(vendor_user_email) AS impressions_count
    FROM
        vendor_test_data
    GROUP BY
        1
)
--Below Query doesn't make any sense. I think its should be joining the first 2 tables.
, vendor_indeed_data_joined AS (
    SELECT
        v.vendor_user_email --HEM
        ,f.impressions_count -- this field might not make any sense
        , v.impression_timestamp
        , v.impression_date
        , DATE_PART('HOUR', v.impression_timestamp) AS impression_hour_of_day
        , i.indeed_user_email
        , i.conversion_date
        , v.campaign
        , v.experiment_group
        , i.conversion_metric
    FROM
        vendor_test_data v --I think it should be on 'vendor_test_data' table
    LEFT OUTER JOIN  -- added outer join 
        indeed_data_cleaned i
        on -- needs to be joined on a field (probably HEM)
        v.vendor_user_email = i.indeed_user_email
        LEFT JOIN
        user_impression_frequency f
        ON v.vendor_user_email = f.vendor_user_email

)
-- aggregate campaign data:
-- impressions population and users population
, aggregated_campaign_data AS (
    SELECT
        campaign
        , impressions_count
        , COUNT(vendor_user_email) AS impressions_population
        , COUNT(DISTINCT vendor_user_email) AS vendor_users_population
    FROM
        vendor_indeed_data_joined
    GROUP BY
      1,2
)
-- aggregate conversion data:
-- conversion types by group
, aggregated_conversion_data AS (
    SELECT
        campaign
        , impressions_count
        , conversion_metric
        , COUNT(DISTINCT indeed_user_email) AS indeed_conversions
    FROM
        vendor_indeed_data_joined
    WHERE
        conversion_metric IS NOT NULL
    GROUP BY
        1,2,3
)
-- the aggregations above happen in separate CTEs as the
-- 'conversion_metric' field only exists for the conversion data
-- finally, select all the fields from each aggregated CTE
SELECT
    camp.impressions_count
    , camp.campaign
    , conv.conversion_metric
    , camp.impressions_population
    , camp.vendor_users_population
    , conv.indeed_conversions
FROM
    aggregated_campaign_data camp
LEFT JOIN
    aggregated_conversion_data conv
    ON camp.campaign = conv.campaign
    AND camp.impressions_count = conv.impressions_count;