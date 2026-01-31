CREATE OR ALTER VIEW dbo.vw_salesforce_customer_weekly_export AS
SELECT
    external_customer_id,
    external_signal_id,
    batch_week_start,
    batch_week_end,
    total_spend_week,
    activity_count_week,
    last_activity_time,
    customer_tier
FROM dbo.vw_reverse_etl_customer_weekly;
