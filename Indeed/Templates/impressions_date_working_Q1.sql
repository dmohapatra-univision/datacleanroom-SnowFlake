
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
        ,CAMPAIGN_STARTEGY as campaign
        -- for a suppression list, all ad logs users are test group
        , 'Test' AS experiment_group
    FROM
        {{ app_data | sqlsafe }}.cleanroom.Indeed_2024_SMB_Job_Seeker_Exposure_vw --indeed_q4_brand_campaign_exposure
    WHERE
        impression_date >= TO_DATE('{{ campaign_start | sqlsafe }}')  -- set as a parameter
        AND impression_date <= TO_DATE('{{ campaign_end | sqlsafe }}') -- set as a parameter
)
SELECT
    campaign
    , impression_date
    , impression_timestamp
    , COUNT(vendor_user_email) AS impression_volume
FROM
    vendor_test_data
GROUP BY
    campaign
    , impression_date
    , impression_timestamp;