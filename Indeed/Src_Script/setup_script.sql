CREATE PROCEDURE create_indeed_campaign_fires(
    start_date DATE,
    end_date DATE,
    line_item_ids ARRAY<STRING>,
    project_name STRING,
    dataset_name STRING
)
BEGIN
    DECLARE error_message STRING;
    TRY
        CREATE TEMP TABLE temp_indeed_campaign_fires AS
        SELECT
            timestamp_utc,
            ip,
            device_id,
            regexp_extract(url, r'line_item_id=([\d.]+)') line_item_id,
            regexp_extract(url, r'advertiser_id=([\d.]+)') advertiser_id,
            regexp_extract(url, r'creative_id=([\d.]+)') creative_id
        FROM `${project_name}.${dataset_name}.madhive_pixel_fires`
        WHERE
            upper(beacon_name) LIKE '%JIFFY%'
            AND timestamp_utc BETWEEN start_date AND end_date
            AND beacon_name = 'Jiffy_Pixel'
            AND regexp_extract(url, r'line_item_id=([\d.]+)') IN UNNEST(line_item_ids);

        CREATE OR REPLACE TABLE `${project_name}.${dataset_name}.indeed_campaign_fires_unique` AS
        SELECT GENERATE_UUID() AS impression_Id, * FROM temp_indeed_campaign_fires;

        CREATE OR REPLACE TABLE `${project_name}.${dataset_name}.indeed_campaign_fires_HEM` AS
        SELECT
            a.*,
            b.identifier AS HEM
        FROM `${project_name}.${dataset_name}.indeed_campaign_fires_unique` a
        LEFT OUTER JOIN (
            SELECT DISTINCT
                ip_address,
                identifier
            FROM dl-datalake-gold-prd.datascience_share_gold.univision_hhg2_latest
            WHERE id_subtype = 'HEM'
        ) b ON a.ip = b.ip_address;

        CREATE OR REPLACE TABLE `${project_name}.${dataset_name}.Indeed_Q4_Brand_Campaign_Exposure` AS
        SELECT
            impression_id,
            DATETIME(timestamp_utc, "America/New_York") exposure_dtm_est,
            SHA256(ip) AS hashed_ip,
            device_id,
            line_item_id,
            advertiser_id,
            creative_id,
            HEM
        FROM `${project_name}.${dataset_name}.indeed_campaign_fires_HEM`;

        CREATE OR REPLACE TABLE `${project_name}.${dataset_name}.indeed_test_control_segment` AS
        SELECT
            segment_name,
            identifier,
            id_subtype
        FROM `dev-hh-graph.sergey_scratch.hhg_custom_segments`
        WHERE
            segment_name IN ('HHG_ABtestQ4_Indeed_Treatment', 'HHG_ABtestQ4_Indeed_Control')
            AND id_subtype = 'HEM';

        CREATE OR REPLACE TABLE `${project_name}.${dataset_name}.indeed_Q4_control_segment` AS
        SELECT
            segment_name,
            identifier,
            id_subtype
        FROM `dev-hh-graph.sergey_scratch.hhg_custom_segments`
        WHERE segment_name IN ('HHG_ABtestQ4_Indeed_Control');

        CREATE OR REPLACE TABLE `${project_name}.${dataset_name}.indeed_Q4_control_segment_ip` AS
        SELECT DISTINCT
            a.segment_name,
            SHA256(b.ip_address) AS hashed_ip
        FROM `${project_name}.${dataset_name}.indeed_Q4_control_segment` a
        JOIN dl-datalake-gold-prd.datascience_share_gold.univision_hhg2_latest b
            ON a.identifier = b.identifier
        WHERE ip_address IS NOT NULL;
    CATCH e
        SET error_message = e.message;
        EXECUTE IMMEDIATE """
            INSERT INTO `${project_name}.${dataset_name}.indeed_campaign_fires_error_log`
            VALUES (CURRENT_TIMESTAMP(), @error_message)
        """;
    END TRY;
END;
